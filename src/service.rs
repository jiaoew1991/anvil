// Copyright 2025 nurion team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// gRPC Anvil Service Implementation (Protocol v2)
//
// Hot-path RPCs unified:
//   Claim:            queue + group claims
//   Complete:         ack + nack + forward + scatter
//   Push:             single + batch
//   ClaimAndComplete: combined claim + complete (halves round trips)

use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

#[allow(unused_imports)]
use crate::state::AnvilState;
use crate::storage::{
    self, AnvilStorage, NackOptions, Rejection, StorageError, DEAD_LETTERS_PER_CALL_MAX,
};
use crate::types::{ClaimedMessage, Message};

// Generated protobuf types. Generated code is not ours to lint: each Rust
// release adds clippy lints that tonic's output trips.
#[allow(clippy::all)]
pub mod proto {
    tonic::include_proto!("anvil");
}

use proto::anvil_server::Anvil;
use proto::*;

/// The one place a storage error becomes a gRPC status. A `Rejection` gets
/// the code its caller acts on, so a worker can tell `FAILED_PRECONDITION`
/// (its claim is lost: drop the message) from `INTERNAL` (the broker failed:
/// retry) and from `UNAVAILABLE` (this broker's database is closed for good,
/// typically fenced by a newer broker: fail over instead of retrying here).
fn storage_status(rpc: &str, error: StorageError) -> Status {
    let Some(rejection) = error.downcast_ref::<Rejection>() else {
        tracing::error!("{rpc} failed: {error}");
        if storage::is_closed(&error) {
            return Status::unavailable(format!("Storage closed: {error}"));
        }
        return Status::internal(format!("Storage error: {error}"));
    };
    tracing::warn!("{rpc} rejected: {rejection}");
    match rejection {
        Rejection::Fenced(message) => Status::failed_precondition(message.as_str()),
        Rejection::NotFound(message) => Status::not_found(message.as_str()),
        Rejection::InvalidArgument(message) => Status::invalid_argument(message.as_str()),
        Rejection::QueueFull(message) => Status::resource_exhausted(message.as_str()),
        Rejection::QueueFinished(message) => Status::failed_precondition(message.as_str()),
        Rejection::PartitionOwned(message) => Status::aborted(message.as_str()),
    }
}

/// Refuses the empty queue name, which GetStats reads as "every queue" and
/// MarkQueueFinished refuses: a queue named "" could be filled but never
/// inspected or finished.
fn check_queue_name(queue: &str, role: &str) -> Result<(), Status> {
    if queue.is_empty() {
        return Err(Status::invalid_argument(format!("{role} is required")));
    }
    Ok(())
}

/// Refuses claims that do not pair up one token per msg_id, or that name a
/// msg_id twice: the second entry of a repeated id would be fenced by the
/// first and fail the request as FAILED_PRECONDITION, telling the worker it
/// lost a claim it still holds.
fn check_claims(msg_ids: &[String], claim_tokens: &[String]) -> Result<(), Status> {
    if claim_tokens.len() != msg_ids.len() {
        return Err(Status::invalid_argument(format!(
            "claim_tokens length must match msg_ids (msg_ids={}, claim_tokens={})",
            msg_ids.len(),
            claim_tokens.len()
        )));
    }
    check_distinct(msg_ids)
}

fn check_distinct(msg_ids: &[String]) -> Result<(), Status> {
    let mut seen = HashSet::with_capacity(msg_ids.len());
    if let Some(repeated) = msg_ids.iter().find(|id| !seen.insert(id.as_str())) {
        return Err(Status::invalid_argument(format!(
            "duplicate msg_id {repeated}"
        )));
    }
    Ok(())
}

/// Refuses a redrive or purge naming no dead letter, more than one call may
/// handle, or one msg_id twice, which would count one dead letter twice.
fn check_dead_letter_ids(msg_ids: &[String]) -> Result<(), Status> {
    if msg_ids.is_empty() || msg_ids.len() > DEAD_LETTERS_PER_CALL_MAX {
        return Err(Status::invalid_argument(format!(
            "msg_ids must name 1 to {DEAD_LETTERS_PER_CALL_MAX} dead letters, got {}",
            msg_ids.len()
        )));
    }
    check_distinct(msg_ids)
}

/// Anvil gRPC service implementation
pub struct AnvilService {
    state: Arc<AnvilState>,
    storage: Arc<AnvilStorage>,
    /// How long a heartbeat stream may stay silent before it is closed:
    /// `lease_timeout_secs`, after which its lease is dead anyway.
    lease_timeout: Duration,
}

impl AnvilService {
    pub fn new(
        state: Arc<AnvilState>,
        storage: Arc<AnvilStorage>,
        lease_timeout_secs: f64,
    ) -> Self {
        // An infinite lease timeout never closes a silent stream.
        let lease_timeout =
            Duration::try_from_secs_f64(lease_timeout_secs).unwrap_or(Duration::MAX);
        Self {
            state,
            storage,
            lease_timeout,
        }
    }

    #[cfg(test)]
    pub(crate) fn state(&self) -> &AnvilState {
        &self.state
    }

    /// Convert internal Message to slim ClaimMessage (no queue, no created_at)
    fn to_claim_message(claimed: &ClaimedMessage) -> ClaimMessage {
        ClaimMessage {
            msg_id: claimed.message.msg_id.clone(),
            payload: claimed.message.payload.clone(),
            metadata: claimed.message.metadata.clone(),
            claim_token: claimed.claim_token.clone(),
            delivery_count: claimed.delivery_count,
        }
    }

    /// Execute a Complete operation (shared by `complete` and `claim_and_complete`)
    async fn execute_complete(&self, req: CompleteRequest) -> Result<CompleteResponse, Status> {
        check_claims(&req.msg_ids, &req.claim_tokens)?;

        // A state update the broker would not write fails the whole Complete:
        // acking without it would lose the update the worker counts on.
        let has_state = req
            .state
            .as_ref()
            .is_some_and(|s| !s.puts.is_empty() || !s.deletes.is_empty());
        if has_state {
            let namespace = req.state.as_ref().map_or("", |s| s.namespace.as_str());
            storage::check_state_namespace(namespace)
                .map_err(|rejection| Status::invalid_argument(rejection.to_string()))?;
        }
        let state_ns = req
            .state
            .as_ref()
            .map(|s| s.namespace.as_str())
            .unwrap_or("");
        let state_puts: HashMap<String, Vec<u8>> = req
            .state
            .as_ref()
            .map(|s| s.puts.clone())
            .unwrap_or_default();
        let state_deletes: Vec<String> = req
            .state
            .as_ref()
            .map(|s| s.deletes.clone())
            .unwrap_or_default();

        match req.action {
            // --- Ack: just acknowledge, no downstream ---
            Some(complete_request::Action::Ack(_)) => {
                let result = if has_state {
                    self.storage
                        .ack_with_state(
                            &req.upstream_queue,
                            &req.msg_ids,
                            &req.claim_tokens,
                            &req.worker_id,
                            &req.lease_id,
                            state_ns,
                            &state_puts,
                            &state_deletes,
                        )
                        .await
                } else {
                    self.storage
                        .ack_messages(
                            &req.upstream_queue,
                            &req.msg_ids,
                            &req.claim_tokens,
                            &req.worker_id,
                            &req.lease_id,
                        )
                        .await
                };
                match result {
                    Ok(()) => Ok(CompleteResponse {
                        success: true,
                        processed_count: req.msg_ids.len() as i32,
                        new_msg_ids: vec![],
                    }),
                    Err(e) => Err(storage_status("Complete(ack)", e)),
                }
            }

            // --- Nack: return messages to queue ---
            Some(complete_request::Action::Nack(nack)) => {
                let nack = NackOptions {
                    dead_letter: nack.dead_letter,
                    reason: &nack.reason,
                };
                let result = if has_state {
                    self.storage
                        .nack_messages_with_state(
                            &req.upstream_queue,
                            &req.msg_ids,
                            &req.claim_tokens,
                            &req.worker_id,
                            &req.lease_id,
                            nack,
                            state_ns,
                            &state_puts,
                            &state_deletes,
                        )
                        .await
                } else {
                    self.storage
                        .nack_messages(
                            &req.upstream_queue,
                            &req.msg_ids,
                            &req.claim_tokens,
                            &req.worker_id,
                            &req.lease_id,
                            nack,
                        )
                        .await
                };
                match result {
                    Ok(()) => Ok(CompleteResponse {
                        success: true,
                        processed_count: req.msg_ids.len() as i32,
                        new_msg_ids: vec![],
                    }),
                    Err(e) => Err(storage_status("Complete(nack)", e)),
                }
            }

            // --- Forward: ack upstream + push to single downstream queue ---
            Some(complete_request::Action::Forward(fwd)) => {
                check_queue_name(&fwd.downstream_queue, "forward downstream_queue")?;
                let downstream_messages: Vec<Message> = fwd
                    .payloads
                    .iter()
                    .map(|payload| Message::new(payload.clone()))
                    .collect();
                let new_msg_ids: Vec<String> = downstream_messages
                    .iter()
                    .map(|m| m.msg_id.clone())
                    .collect();

                let result = if has_state {
                    self.storage
                        .ack_forward_with_state(
                            &req.upstream_queue,
                            &req.msg_ids,
                            &req.claim_tokens,
                            &req.worker_id,
                            &req.lease_id,
                            &fwd.downstream_queue,
                            &downstream_messages,
                            state_ns,
                            &state_puts,
                            &state_deletes,
                        )
                        .await
                } else {
                    self.storage
                        .ack_and_forward(
                            &req.upstream_queue,
                            &req.msg_ids,
                            &req.claim_tokens,
                            &req.worker_id,
                            &req.lease_id,
                            &fwd.downstream_queue,
                            &downstream_messages,
                        )
                        .await
                };
                match result {
                    Ok(()) => Ok(CompleteResponse {
                        success: true,
                        processed_count: req.msg_ids.len() as i32,
                        new_msg_ids,
                    }),
                    Err(e) => Err(storage_status("Complete(forward)", e)),
                }
            }

            // --- Scatter: ack upstream + push to partition group ---
            Some(complete_request::Action::Scatter(sct)) => {
                if sct.group_name.is_empty() {
                    return Err(Status::invalid_argument("scatter group_name is required"));
                }

                let mut partition_msgs: Vec<(u32, Vec<Message>)> = Vec::new();
                for pp in &sct.partitions {
                    let messages: Vec<Message> = pp
                        .payloads
                        .iter()
                        .map(|payload| Message::new(payload.clone()))
                        .collect();
                    partition_msgs.push((pp.partition_id as u32, messages));
                }

                match self
                    .storage
                    .ack_and_scatter(
                        &req.upstream_queue,
                        &req.msg_ids,
                        &req.claim_tokens,
                        &req.worker_id,
                        &req.lease_id,
                        &sct.group_name,
                        &partition_msgs,
                        if has_state { Some(state_ns) } else { None },
                        if has_state { Some(&state_puts) } else { None },
                        if has_state {
                            Some(&state_deletes)
                        } else {
                            None
                        },
                    )
                    .await
                {
                    Ok(new_msg_ids) => Ok(CompleteResponse {
                        success: true,
                        processed_count: req.msg_ids.len() as i32,
                        new_msg_ids,
                    }),
                    Err(e) => Err(storage_status("Complete(scatter)", e)),
                }
            }

            None => Err(Status::invalid_argument(
                "action is required (ack, nack, forward, or scatter)",
            )),
        }
    }

    /// Execute a Claim operation (shared by `claim` and `claim_and_complete`)
    async fn execute_claim(&self, req: ClaimRequest) -> Result<ClaimResponse, Status> {
        let batch_size = if req.batch_size > 0 {
            req.batch_size as usize
        } else {
            1
        };

        match req.source {
            // --- Plain queue claim ---
            Some(claim_request::Source::Queue(queue)) => {
                check_queue_name(&queue, "claim queue")?;
                let claimed = self
                    .storage
                    .claim_messages(&queue, batch_size, &req.worker_id, &req.lease_id)
                    .await
                    .map_err(|e| storage_status("Claim", e))?;

                let messages: Vec<ClaimMessage> =
                    claimed.iter().map(Self::to_claim_message).collect();

                let has_more = match self.storage.get_queue_stats(&queue).await {
                    Ok(meta) => meta.claim_seq < meta.push_seq,
                    Err(_) => false,
                };

                // When empty, check if upstream is finished + fully drained
                let (upstream_drained, dead_count) = if messages.is_empty() {
                    match self.storage.check_queue_completion(&queue).await {
                        Ok(completion) => {
                            if completion.drained {
                                tracing::info!(
                                    "upstream_drained=true for queue={}, worker={}",
                                    queue,
                                    req.worker_id
                                );
                            }
                            (completion.drained, completion.dead_count as i64)
                        }
                        Err(_) => (false, 0),
                    }
                } else {
                    (false, 0)
                };

                Ok(ClaimResponse {
                    messages,
                    has_more,
                    source_queue: String::new(),
                    source_partition: 0,
                    upstream_drained,
                    dead_count,
                })
            }

            // --- Group claim ---
            Some(claim_request::Source::Group(group)) => {
                if group.group_name.is_empty() {
                    return Err(Status::invalid_argument("group_name is required"));
                }
                // Cast to u32, a negative partition becomes one past any group's
                // end, which the storage rejects.
                let assigned: Vec<u32> = group
                    .assigned_partitions
                    .iter()
                    .map(|&p| p as u32)
                    .collect();

                let partitions = self
                    .storage
                    .group_partitions(&group.group_name, &assigned)
                    .await
                    .map_err(|e| storage_status("Claim(group)", e))?;
                self.state
                    .own_partitions(
                        partitions.iter().map(|(_, queue)| queue.as_str()),
                        &req.lease_id,
                    )
                    .map_err(|e| storage_status("Claim(group)", Box::new(e)))?;

                let (claimed, source_queue, source_partition) = self
                    .storage
                    .claim_from_group(
                        &group.group_name,
                        &partitions,
                        batch_size,
                        &req.worker_id,
                        &req.lease_id,
                    )
                    .await
                    .map_err(|e| storage_status("Claim(group)", e))?;

                let messages: Vec<ClaimMessage> =
                    claimed.iter().map(Self::to_claim_message).collect();

                // The worker's signal to exit covers the partitions it
                // consumes: the rest of the group is other workers' to drain.
                let (upstream_drained, dead_count) = if messages.is_empty() {
                    let mut drained = true;
                    let mut dead_count = 0;
                    for (_, queue) in &partitions {
                        let completion = self
                            .storage
                            .check_queue_completion(queue)
                            .await
                            .map_err(|e| storage_status("Claim(group)", e))?;
                        drained &= completion.drained;
                        dead_count += completion.dead_count as i64;
                    }
                    if drained {
                        tracing::info!(
                            "upstream_drained=true for group={}, partitions={:?}, worker={}",
                            group.group_name,
                            assigned,
                            req.worker_id
                        );
                    }
                    (drained, dead_count)
                } else {
                    (false, 0)
                };

                Ok(ClaimResponse {
                    messages,
                    has_more: false,
                    source_queue,
                    source_partition: source_partition as i32,
                    upstream_drained,
                    dead_count,
                })
            }

            None => Err(Status::invalid_argument(
                "source is required (queue or group)",
            )),
        }
    }
}

#[tonic::async_trait]
impl Anvil for AnvilService {
    // =========================================================================
    // Unified Hot Path
    // =========================================================================

    async fn claim(
        &self,
        request: Request<ClaimRequest>,
    ) -> Result<Response<ClaimResponse>, Status> {
        self.execute_claim(request.into_inner())
            .await
            .map(Response::new)
    }

    async fn complete(
        &self,
        request: Request<CompleteRequest>,
    ) -> Result<Response<CompleteResponse>, Status> {
        self.execute_complete(request.into_inner())
            .await
            .map(Response::new)
    }

    async fn renew_claims(
        &self,
        request: Request<RenewClaimsRequest>,
    ) -> Result<Response<RenewClaimsResponse>, Status> {
        let req = request.into_inner();
        check_claims(&req.msg_ids, &req.claim_tokens)?;
        self.storage
            .renew_claims(
                &req.queue,
                &req.msg_ids,
                &req.claim_tokens,
                &req.worker_id,
                &req.lease_id,
            )
            .await
            .map(|()| Response::new(RenewClaimsResponse {}))
            .map_err(|e| storage_status("RenewClaims", e))
    }

    async fn push(&self, request: Request<PushRequest>) -> Result<Response<PushResponse>, Status> {
        let req = request.into_inner();

        check_queue_name(&req.queue, "push queue")?;
        if req.payloads.is_empty() {
            return Err(Status::invalid_argument("at least one payload is required"));
        }

        let messages: Vec<Message> = req
            .payloads
            .iter()
            .map(|payload| {
                if req.metadata.is_empty() {
                    Message::new(payload.clone())
                } else {
                    Message::with_metadata(payload.clone(), req.metadata.clone())
                }
            })
            .collect();

        let msg_ids: Vec<String> = messages.iter().map(|m| m.msg_id.clone()).collect();

        match self.storage.push_messages(&req.queue, &messages).await {
            Ok(()) => Ok(Response::new(PushResponse { msg_ids })),
            Err(e) => Err(storage_status("Push", e)),
        }
    }

    async fn claim_and_complete(
        &self,
        request: Request<ClaimAndCompleteRequest>,
    ) -> Result<Response<ClaimAndCompleteResponse>, Status> {
        let req = request.into_inner();

        // Execute complete first (if provided)
        let complete_result = if let Some(complete_req) = req.complete {
            Some(self.execute_complete(complete_req).await?)
        } else {
            None
        };

        // Then claim
        let claim_result = if let Some(claim_req) = req.claim {
            Some(self.execute_claim(claim_req).await?)
        } else {
            None
        };

        Ok(Response::new(ClaimAndCompleteResponse {
            complete_result,
            claim_result,
        }))
    }

    // =========================================================================
    // State API (unchanged)
    // =========================================================================

    async fn state_get(
        &self,
        request: Request<StateGetRequest>,
    ) -> Result<Response<StateGetResponse>, Status> {
        let req = request.into_inner();
        match self
            .storage
            .state_get_batch(&req.namespace, &req.keys)
            .await
        {
            Ok(values) => Ok(Response::new(StateGetResponse { values })),
            Err(e) => Err(storage_status("StateGet", e)),
        }
    }

    async fn state_put(
        &self,
        request: Request<StatePutRequest>,
    ) -> Result<Response<StatePutResponse>, Status> {
        let req = request.into_inner();
        let puts: HashMap<String, Vec<u8>> = req.puts.into_iter().collect();

        match self
            .storage
            .state_put_batch(&req.namespace, &puts, &req.deletes)
            .await
        {
            Ok((puts_count, deletes_count)) => Ok(Response::new(StatePutResponse {
                puts_count: puts_count as i32,
                deletes_count: deletes_count as i32,
            })),
            Err(e) => Err(storage_status("StatePut", e)),
        }
    }

    // =========================================================================
    // Heartbeat
    // =========================================================================

    type HeartbeatStreamStream = Pin<Box<dyn Stream<Item = Result<HeartbeatPong, Status>> + Send>>;

    /// A stream's first ping opens its lease: the lease it presents if the
    /// broker still holds it, so a worker whose stream broke keeps its claims
    /// when it reconnects, else a new one. A stream lives only as long as its
    /// lease could: it closes once silent for `lease_timeout_secs`, or once
    /// its lease has been pruned, and the worker's next stream opens anew.
    /// Neither the stream nor its connection is part of the lease, so the
    /// transport may drop either one sooner without costing the worker its
    /// claims.
    async fn heartbeat_stream(
        &self,
        request: Request<Streaming<HeartbeatPing>>,
    ) -> Result<Response<Self::HeartbeatStreamStream>, Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(16);
        let state = self.state.clone();
        let lease_timeout = self.lease_timeout;

        tokio::spawn(async move {
            let Ok(Some(Ok(first))) = timeout(lease_timeout, stream.next()).await else {
                return;
            };
            let pong = HeartbeatPong {
                lease_id: state.open_lease(&first.lease_id),
                ok: true,
                next_ping_ms: 5000,
            };
            if !first.lease_id.is_empty() && first.lease_id != pong.lease_id {
                tracing::info!(
                    "Worker {} presented unknown lease {}, opened lease {}",
                    first.worker_id,
                    first.lease_id,
                    pong.lease_id
                );
            }

            loop {
                if tx.send(Ok(pong.clone())).await.is_err() {
                    break;
                }
                match timeout(lease_timeout, stream.next()).await {
                    Ok(Some(Ok(_ping))) => {
                        if !state.renew_lease(&pong.lease_id) {
                            tracing::debug!(
                                "Heartbeat lease {} pruned, closing stream",
                                pong.lease_id
                            );
                            break;
                        }
                    }
                    Ok(Some(Err(e))) => {
                        tracing::warn!("Heartbeat stream error: {}", e);
                        break;
                    }
                    Ok(None) => break,
                    Err(_) => {
                        tracing::debug!("Heartbeat silent for the lease timeout, closing stream");
                        break;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    // =========================================================================
    // Admin API (unchanged)
    // =========================================================================

    async fn create_queue(
        &self,
        request: Request<CreateQueueRequest>,
    ) -> Result<Response<CreateQueueResponse>, Status> {
        let req = request.into_inner();
        check_queue_name(&req.queue, "queue")?;
        match self
            .storage
            .create_queue(&req.queue, req.max_pending, req.max_deliveries)
            .await
        {
            Ok(()) => {
                self.state.get_or_create_queue(&req.queue);
                Ok(Response::new(CreateQueueResponse { created: true }))
            }
            Err(e) => Err(storage_status("CreateQueue", e)),
        }
    }

    async fn delete_queue(
        &self,
        request: Request<DeleteQueueRequest>,
    ) -> Result<Response<DeleteQueueResponse>, Status> {
        let req = request.into_inner();
        match self.storage.delete_queue(&req.queue).await {
            Ok(deleted) => {
                self.state.delete_queue(&req.queue);
                Ok(Response::new(DeleteQueueResponse {
                    deleted: deleted > 0,
                    messages_deleted: deleted as i32,
                }))
            }
            Err(e) => Err(storage_status("DeleteQueue", e)),
        }
    }

    async fn get_stats(
        &self,
        request: Request<GetStatsRequest>,
    ) -> Result<Response<GetStatsResponse>, Status> {
        let req = request.into_inner();

        let queues_to_check: Vec<String> = if req.queue.is_empty() {
            self.state.list_queues()
        } else {
            vec![req.queue.clone()]
        };

        let mut queues: HashMap<String, QueueStats> = HashMap::new();
        for queue in queues_to_check {
            match self.storage.get_queue_stats(&queue).await {
                Ok(meta) => {
                    let pending_count = meta.push_seq.saturating_sub(meta.claim_seq);
                    queues.insert(
                        queue.clone(),
                        QueueStats {
                            queue: queue.clone(),
                            pending_count: pending_count as i64,
                            claimed_count: meta.claimed_count as i64,
                            total_pushed: meta.total_pushed as i64,
                            total_acked: meta.total_acked as i64,
                            dead_count: meta.dead_count as i64,
                            total_purged: meta.total_purged as i64,
                            max_deliveries: meta.max_deliveries,
                            effective_max_deliveries: meta.effective_max_deliveries,
                        },
                    );
                }
                Err(e) => {
                    // A request for one queue reports why it failed, such as
                    // NOT_FOUND; a listing skips a queue deleted since it was
                    // listed.
                    if !req.queue.is_empty() {
                        return Err(storage_status("GetStats", e));
                    }
                    tracing::warn!("Failed to get stats for queue {}: {}", queue, e);
                }
            }
        }

        Ok(Response::new(GetStatsResponse {
            queues,
            total_workers: 0,
            uptime_secs: 0,
        }))
    }

    // =========================================================================
    // Dead Letter API
    // =========================================================================

    async fn list_dead_letters(
        &self,
        request: Request<ListDeadLettersRequest>,
    ) -> Result<Response<ListDeadLettersResponse>, Status> {
        let req = request.into_inner();
        check_queue_name(&req.queue, "queue")?;
        let limit = req.limit as usize;
        if !(1..=DEAD_LETTERS_PER_CALL_MAX).contains(&limit) {
            return Err(Status::invalid_argument(format!(
                "limit must be 1 to {DEAD_LETTERS_PER_CALL_MAX}, got {limit}"
            )));
        }
        let start_after = Some(req.start_after.as_str()).filter(|id| !id.is_empty());
        let (letters, more) = self
            .storage
            .list_dead_letters(&req.queue, start_after, limit, req.ids_only)
            .await
            .map_err(|e| storage_status("ListDeadLetters", e))?;
        let next_start_after = match letters.last() {
            Some(last) if more => last.message.msg_id.clone(),
            _ => String::new(),
        };
        let dead_letters = letters
            .into_iter()
            .map(|letter| DeadLetter {
                msg_id: letter.message.msg_id,
                payload: letter.message.payload,
                metadata: letter.message.metadata,
                delivery_count: letter.delivery_count,
                cause: letter.cause.as_str().to_string(),
                reason: letter.reason,
                worker_id: letter.worker_id,
                dead_at_ms: letter.dead_at_ms,
            })
            .collect();
        Ok(Response::new(ListDeadLettersResponse {
            dead_letters,
            next_start_after,
        }))
    }

    async fn redrive_dead_letters(
        &self,
        request: Request<RedriveDeadLettersRequest>,
    ) -> Result<Response<RedriveDeadLettersResponse>, Status> {
        let req = request.into_inner();
        check_queue_name(&req.queue, "queue")?;
        check_dead_letter_ids(&req.msg_ids)?;
        let (redriven, missing) = self
            .storage
            .redrive_dead_letters(&req.queue, &req.msg_ids, req.reopen)
            .await
            .map_err(|e| storage_status("RedriveDeadLetters", e))?;
        Ok(Response::new(RedriveDeadLettersResponse {
            redriven,
            missing,
        }))
    }

    async fn purge_dead_letters(
        &self,
        request: Request<PurgeDeadLettersRequest>,
    ) -> Result<Response<PurgeDeadLettersResponse>, Status> {
        let req = request.into_inner();
        check_queue_name(&req.queue, "queue")?;
        check_dead_letter_ids(&req.msg_ids)?;
        let (purged, missing) = self
            .storage
            .purge_dead_letters(&req.queue, &req.msg_ids)
            .await
            .map_err(|e| storage_status("PurgeDeadLetters", e))?;
        Ok(Response::new(PurgeDeadLettersResponse { purged, missing }))
    }

    // =========================================================================
    // Queue Completion API (unchanged)
    // =========================================================================

    async fn mark_queue_finished(
        &self,
        request: Request<MarkQueueFinishedRequest>,
    ) -> Result<Response<MarkQueueFinishedResponse>, Status> {
        let req = request.into_inner();
        if req.queue.is_empty() {
            return Err(Status::invalid_argument("queue is required"));
        }
        match self.storage.mark_queue_finished(&req.queue).await {
            Ok(()) => Ok(Response::new(MarkQueueFinishedResponse { success: true })),
            Err(e) => Err(storage_status("MarkQueueFinished", e)),
        }
    }

    async fn is_queue_finished(
        &self,
        request: Request<IsQueueFinishedRequest>,
    ) -> Result<Response<IsQueueFinishedResponse>, Status> {
        let req = request.into_inner();
        if req.queue.is_empty() {
            return Err(Status::invalid_argument("queue is required"));
        }
        match self.storage.check_queue_completion(&req.queue).await {
            Ok(completion) => Ok(Response::new(IsQueueFinishedResponse {
                finished: completion.finished,
                drained: completion.drained,
                safe_to_exit: completion.drained,
                pending_count: completion.pending_count as i64,
                claimed_count: completion.claimed_count as i64,
                dead_count: completion.dead_count as i64,
            })),
            Err(e) => Err(storage_status("IsQueueFinished", e)),
        }
    }

    // =========================================================================
    // QueueGroup API (unchanged)
    // =========================================================================

    async fn create_queue_group(
        &self,
        request: Request<CreateQueueGroupRequest>,
    ) -> Result<Response<CreateQueueGroupResponse>, Status> {
        let req = request.into_inner();
        if req.group_name.is_empty() {
            return Err(Status::invalid_argument("group_name is required"));
        }
        if req.num_partitions <= 0 {
            return Err(Status::invalid_argument("num_partitions must be positive"));
        }

        match self
            .storage
            .create_queue_group(
                &req.group_name,
                req.num_partitions as u32,
                req.max_pending_per_partition,
                req.max_deliveries,
            )
            .await
        {
            Ok((meta, created)) => {
                for queue_name in &meta.partition_queues {
                    self.state.get_or_create_queue(queue_name);
                }
                Ok(Response::new(CreateQueueGroupResponse {
                    queue_names: meta.partition_queues,
                    version: meta.version as i32,
                    created,
                }))
            }
            Err(e) => Err(storage_status("CreateQueueGroup", e)),
        }
    }

    async fn is_group_finished(
        &self,
        request: Request<IsGroupFinishedRequest>,
    ) -> Result<Response<IsGroupFinishedResponse>, Status> {
        let req = request.into_inner();
        if req.group_name.is_empty() {
            return Err(Status::invalid_argument("group_name is required"));
        }

        match self.storage.check_group_completion(&req.group_name).await {
            Ok((all_finished, all_drained, completions)) => {
                let partitions = completions
                    .iter()
                    .enumerate()
                    .map(|(pid, completion)| PartitionStatus {
                        partition_id: pid as i32,
                        pending_count: completion.pending_count as i64,
                        claimed_count: completion.claimed_count as i64,
                        finished: completion.finished,
                        dead_count: completion.dead_count as i64,
                    })
                    .collect();
                Ok(Response::new(IsGroupFinishedResponse {
                    all_finished,
                    all_drained,
                    safe_to_exit: all_finished && all_drained,
                    partitions,
                    dead_count: completions.iter().map(|c| c.dead_count as i64).sum(),
                }))
            }
            Err(e) => Err(storage_status("IsGroupFinished", e)),
        }
    }

    async fn get_group_stats(
        &self,
        request: Request<GetGroupStatsRequest>,
    ) -> Result<Response<GetGroupStatsResponse>, Status> {
        let req = request.into_inner();
        if req.group_name.is_empty() {
            return Err(Status::invalid_argument("group_name is required"));
        }

        match self.storage.get_group_stats(&req.group_name).await {
            Ok((group_meta, stats)) => {
                let mut total_pending: i64 = 0;
                let mut total_claimed: i64 = 0;
                let mut max_pending: i64 = 0;
                let mut pending_values: Vec<i64> = Vec::new();

                let partitions: Vec<PartitionStats> = stats
                    .iter()
                    .map(|(pid, meta)| {
                        let pending = meta.push_seq.saturating_sub(meta.claim_seq) as i64;
                        let claimed = meta.claimed_count as i64;
                        total_pending += pending;
                        total_claimed += claimed;
                        if pending > max_pending {
                            max_pending = pending;
                        }
                        pending_values.push(pending);
                        PartitionStats {
                            partition_id: *pid as i32,
                            pending_count: pending,
                            claimed_count: claimed,
                            total_pushed: meta.total_pushed as i64,
                            total_acked: meta.total_acked as i64,
                            dead_count: meta.dead_count as i64,
                            total_purged: meta.total_purged as i64,
                        }
                    })
                    .collect();

                pending_values.sort();
                let median = if pending_values.is_empty() {
                    0
                } else {
                    pending_values[pending_values.len() / 2]
                };
                let skew_ratio = if median > 0 {
                    max_pending as f32 / median as f32
                } else {
                    0.0
                };
                let hot_partitions: Vec<i32> = if median > 0 {
                    stats
                        .iter()
                        .filter_map(|(pid, meta)| {
                            let pending = meta.push_seq.saturating_sub(meta.claim_seq) as i64;
                            if pending > median * 5 {
                                Some(*pid as i32)
                            } else {
                                None
                            }
                        })
                        .collect()
                } else {
                    vec![]
                };

                Ok(Response::new(GetGroupStatsResponse {
                    partitions,
                    total_pending,
                    total_claimed,
                    max_partition_pending: max_pending,
                    median_partition_pending: median,
                    skew_ratio,
                    hot_partitions,
                    version: group_meta.version as i32,
                }))
            }
            Err(e) => Err(storage_status("GetGroupStats", e)),
        }
    }

    async fn mark_group_finished(
        &self,
        request: Request<MarkGroupFinishedRequest>,
    ) -> Result<Response<MarkGroupFinishedResponse>, Status> {
        let req = request.into_inner();
        if req.group_name.is_empty() {
            return Err(Status::invalid_argument("group_name is required"));
        }
        match self.storage.mark_group_finished(&req.group_name).await {
            Ok(count) => Ok(Response::new(MarkGroupFinishedResponse {
                success: true,
                queues_marked: count as i32,
            })),
            Err(e) => Err(storage_status("MarkGroupFinished", e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recovery::RecoveryTask;
    use crate::types::{AnvilConfig, SimClock};
    use tonic::Code;

    async fn create_service() -> (AnvilService, Arc<AnvilStorage>) {
        let storage = Arc::new(AnvilStorage::new("memory://").await.unwrap());
        let service = AnvilService::new(
            Arc::new(AnvilState::new()),
            storage.clone(),
            AnvilConfig::default().lease_timeout_secs,
        );
        (service, storage)
    }

    /// Pushes one message to `queue` and claims it as worker `w` on lease `l`.
    async fn push_and_claim(service: &AnvilService, queue: &str) -> ClaimMessage {
        service
            .push(Request::new(PushRequest {
                queue: queue.to_string(),
                payloads: vec![b"x".to_vec()],
                metadata: HashMap::new(),
            }))
            .await
            .unwrap();
        let claimed = service
            .claim(Request::new(ClaimRequest {
                source: Some(claim_request::Source::Queue(queue.to_string())),
                worker_id: "w".to_string(),
                lease_id: "l".to_string(),
                batch_size: 1,
                timeout_ms: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(claimed.messages.len(), 1);
        claimed.messages.into_iter().next().unwrap()
    }

    fn assert_status(status: &Status, code: Code, reason: &str) {
        assert_eq!(status.code(), code, "{status:?}");
        assert!(status.message().contains(reason), "{status:?}");
    }

    /// Every way a worker can lose its claim reaches it as FAILED_PRECONDITION,
    /// whichever action settles the claim: each request below presents one
    /// stale credential, or settles a message already settled.
    #[tokio::test]
    async fn test_fencing_failures_are_failed_precondition() {
        let (service, _storage) = create_service().await;
        let queue = "fenced";
        service
            .create_queue_group(Request::new(CreateQueueGroupRequest {
                group_name: "group".to_string(),
                num_partitions: 2,
                max_pending_per_partition: 0,
                max_deliveries: None,
            }))
            .await
            .unwrap();
        let message = push_and_claim(&service, queue).await;

        let ack = CompleteRequest {
            upstream_queue: queue.to_string(),
            msg_ids: vec![message.msg_id.clone()],
            claim_tokens: vec![message.claim_token.clone()],
            worker_id: "w".to_string(),
            lease_id: "l".to_string(),
            action: Some(complete_request::Action::Ack(AckAction {})),
            state: None,
        };
        let stale_token = vec!["stale".to_string()];
        let forward = complete_request::Action::Forward(ForwardAction {
            downstream_queue: "downstream".to_string(),
            payloads: vec![b"y".to_vec()],
        });
        let scatter = complete_request::Action::Scatter(ScatterAction {
            group_name: "group".to_string(),
            partitions: vec![PartitionPayload {
                partition_id: 0,
                payloads: vec![b"y".to_vec()],
            }],
        });
        let fenced = [
            (
                "claim_token mismatch",
                CompleteRequest {
                    claim_tokens: stale_token.clone(),
                    ..ack.clone()
                },
            ),
            (
                "lease_id mismatch",
                CompleteRequest {
                    lease_id: "other".to_string(),
                    ..ack.clone()
                },
            ),
            (
                "worker_id mismatch",
                CompleteRequest {
                    worker_id: "other".to_string(),
                    action: Some(complete_request::Action::Nack(NackAction::default())),
                    ..ack.clone()
                },
            ),
            (
                "claim_token mismatch",
                CompleteRequest {
                    claim_tokens: stale_token.clone(),
                    action: Some(forward),
                    ..ack.clone()
                },
            ),
            (
                "claim_token mismatch",
                CompleteRequest {
                    claim_tokens: stale_token.clone(),
                    action: Some(scatter),
                    ..ack.clone()
                },
            ),
        ];
        for (reason, request) in fenced {
            let status = service.complete(Request::new(request)).await.unwrap_err();
            assert_status(&status, Code::FailedPrecondition, reason);
        }

        let renew = RenewClaimsRequest {
            queue: queue.to_string(),
            msg_ids: vec![message.msg_id.clone()],
            claim_tokens: vec![message.claim_token.clone()],
            worker_id: "w".to_string(),
            lease_id: "l".to_string(),
        };
        let status = service
            .renew_claims(Request::new(RenewClaimsRequest {
                claim_tokens: stale_token,
                ..renew.clone()
            }))
            .await
            .unwrap_err();
        assert_status(&status, Code::FailedPrecondition, "claim_token mismatch");

        // None of the fenced requests settled the message.
        service.complete(Request::new(ack.clone())).await.unwrap();
        let status = service.complete(Request::new(ack)).await.unwrap_err();
        assert_status(&status, Code::FailedPrecondition, "message_not_claimed");
        let status = service.renew_claims(Request::new(renew)).await.unwrap_err();
        assert_status(&status, Code::FailedPrecondition, "message_not_claimed");
    }

    /// A renewal naming a claim twice and a group claim naming no partition
    /// are malformed: INVALID_ARGUMENT, and nothing renews.
    #[tokio::test]
    async fn test_malformed_requests_are_invalid_argument() {
        let (service, _storage) = create_service().await;
        let message = push_and_claim(&service, "q").await;
        let status = service
            .renew_claims(Request::new(RenewClaimsRequest {
                queue: "q".to_string(),
                msg_ids: vec![message.msg_id.clone(), message.msg_id.clone()],
                claim_tokens: vec![message.claim_token.clone(), message.claim_token],
                worker_id: "w".to_string(),
                lease_id: "l".to_string(),
            }))
            .await
            .unwrap_err();
        assert_status(&status, Code::InvalidArgument, "duplicate msg_id");

        let status = service
            .claim(Request::new(ClaimRequest {
                source: Some(claim_request::Source::Group(GroupClaimSource {
                    group_name: "group".to_string(),
                    assigned_partitions: Vec::new(),
                })),
                worker_id: "w".to_string(),
                lease_id: "l".to_string(),
                batch_size: 1,
                timeout_ms: 0,
            }))
            .await
            .unwrap_err();
        assert_status(
            &status,
            Code::InvalidArgument,
            "assigned_partitions is required",
        );
    }

    /// Claims one message of partition `partition` of group `g` as worker
    /// and lease `lease`.
    async fn group_claim(
        service: &AnvilService,
        lease: &str,
        partition: i32,
    ) -> Result<ClaimResponse, Status> {
        service
            .claim(Request::new(ClaimRequest {
                source: Some(claim_request::Source::Group(GroupClaimSource {
                    group_name: "g".to_string(),
                    assigned_partitions: vec![partition],
                })),
                worker_id: lease.to_string(),
                lease_id: lease.to_string(),
                batch_size: 1,
                timeout_ms: 0,
            }))
            .await
            .map(Response::into_inner)
    }

    /// Goal: a partition has one consumer at a time, and a consumer drains
    /// with its own partitions. Method: two leases claim from a finished
    /// two-partition group with one message left in partition 1. A second
    /// lease naming an owned partition is ABORTED, while its owner is live
    /// and, once it is dead, until a recovery pass frees the partition; a
    /// lease the broker does not hold is FAILED_PRECONDITION.
    #[tokio::test]
    async fn test_group_partition_ownership_follows_the_lease() {
        let (service, storage) = create_service().await;
        storage.create_queue_group("g", 2, 0, None).await.unwrap();
        storage
            .push_messages("g_p1", &[Message::new(b"x".to_vec())])
            .await
            .unwrap();
        storage.mark_group_finished("g").await.unwrap();
        let claim = |lease: &'static str, partition: i32| group_claim(&service, lease, partition);
        let state = service.state();
        state.update_lease("a");
        state.update_lease("b");

        let status = claim("unknown", 0).await.unwrap_err();
        assert_status(&status, Code::FailedPrecondition, "lease_not_live");
        // Partition 0 is drained whatever partition 1 still holds.
        let own = claim("a", 0).await.unwrap();
        assert!(own.upstream_drained);
        let status = claim("b", 0).await.unwrap_err();
        assert_status(&status, Code::Aborted, "partition_owned");
        let other = claim("b", 1).await.unwrap();
        assert_eq!((other.messages.len(), other.source_partition), (1, 1));
        assert!(!other.upstream_drained);

        // Both leases die and b's worker comes back: partition 0 stays a's
        // until recovery, which reclaims a's claims first, has run.
        state.prune_leases(-1.0);
        state.update_lease("b");
        let status = claim("b", 0).await.unwrap_err();
        assert_status(&status, Code::Aborted, "partition_owned");
        state.release_dead_owners(&state.lease_snapshot());
        assert!(claim("b", 0).await.unwrap().upstream_drained);
    }

    /// Goal: a dead worker's partition passes to its successor only with
    /// its claims. Method: under a running recovery task, lease a claims
    /// the one message of partition 0 and never heartbeats; lease b, kept
    /// alive, claims the partition until it is no longer refused. Its first
    /// claim that succeeds returns the message a held, redelivered.
    #[tokio::test]
    async fn test_recovery_hands_a_dead_owners_partition_over() {
        const HANDOVER_DEADLINE: Duration = Duration::from_secs(10);
        // Lease a must expire in real time.
        let _clock = SimClock::acquire();
        let (service, storage) = create_service().await;
        let config = AnvilConfig {
            lease_timeout_secs: 0.2,
            recovery_interval_secs: 0.05,
            ..AnvilConfig::default()
        };
        let mut recovery = RecoveryTask::new(storage.clone(), service.state.clone(), config);
        recovery.start();
        storage.create_queue_group("g", 1, 0, None).await.unwrap();
        storage
            .push_messages("g_p0", &[Message::new(b"x".to_vec())])
            .await
            .unwrap();

        service.state.update_lease("a");
        let held = group_claim(&service, "a", 0).await.unwrap();
        assert_eq!(held.messages.len(), 1);
        let started = std::time::Instant::now();
        let taken = loop {
            service.state.update_lease("b");
            match group_claim(&service, "b", 0).await {
                Ok(response) => break response,
                Err(status) => {
                    assert_status(&status, Code::Aborted, "partition_owned");
                    assert!(started.elapsed() < HANDOVER_DEADLINE, "never handed over");
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        };
        recovery.stop_async().await;
        assert_eq!(taken.messages.len(), 1);
        assert_eq!(taken.messages[0].msg_id, held.messages[0].msg_id);
        assert_eq!(taken.messages[0].delivery_count, 2);
    }

    /// A push past a bounded queue's capacity is RESOURCE_EXHAUSTED, and the
    /// message still names QueueFull, which clients match on.
    #[tokio::test]
    async fn test_full_queue_is_resource_exhausted() {
        let (service, _storage) = create_service().await;
        service
            .create_queue(Request::new(CreateQueueRequest {
                queue: "bounded".to_string(),
                max_pending: 1,
                max_deliveries: None,
            }))
            .await
            .unwrap();
        let push = PushRequest {
            queue: "bounded".to_string(),
            payloads: vec![b"x".to_vec()],
            metadata: HashMap::new(),
        };
        service.push(Request::new(push.clone())).await.unwrap();
        let status = service.push(Request::new(push)).await.unwrap_err();
        assert_status(&status, Code::ResourceExhausted, "QueueFull");
    }

    /// A closed database is UNAVAILABLE, not INTERNAL: it never reopens, so
    /// a worker must fail over instead of retrying this broker.
    #[tokio::test]
    async fn test_closed_storage_is_unavailable() {
        let (service, storage) = create_service().await;
        storage.close().await.unwrap();
        let status = service
            .push(Request::new(PushRequest {
                queue: "closed".to_string(),
                payloads: vec![b"x".to_vec()],
                metadata: HashMap::new(),
            }))
            .await
            .unwrap_err();
        assert_status(&status, Code::Unavailable, "Storage closed");
    }

    /// Reading the stats of a queue that does not exist reports NOT_FOUND
    /// and creates nothing: neither the stored queues nor a later listing
    /// gain it.
    #[tokio::test]
    async fn test_stats_of_unknown_queue_is_not_found() {
        let (service, storage) = create_service().await;
        service
            .create_queue(Request::new(CreateQueueRequest {
                queue: "known".to_string(),
                max_pending: 0,
                max_deliveries: None,
            }))
            .await
            .unwrap();
        let queues_before = storage.list_queues().await.unwrap();
        assert_eq!(queues_before, vec!["known".to_string()]);

        for _ in 0..2 {
            let status = service
                .get_stats(Request::new(GetStatsRequest {
                    queue: "missing".to_string(),
                }))
                .await
                .unwrap_err();
            assert_status(&status, Code::NotFound, "missing");
        }
        assert_eq!(storage.list_queues().await.unwrap(), queues_before);

        let stats = service
            .get_stats(Request::new(GetStatsRequest {
                queue: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(stats.queues.len(), 1);
        assert!(stats.queues.contains_key("known"));
    }
}
