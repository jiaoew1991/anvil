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

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

#[allow(unused_imports)]
use crate::state::AnvilState;
use crate::storage::AnvilStorage;
use crate::types::Message;

// Generated protobuf types
pub mod proto {
    tonic::include_proto!("anvil");
}

use proto::anvil_server::Anvil;
use proto::*;

/// Anvil gRPC service implementation
pub struct AnvilService {
    state: Arc<AnvilState>,
    storage: Arc<AnvilStorage>,
}

impl AnvilService {
    pub fn new(state: Arc<AnvilState>, storage: Arc<AnvilStorage>) -> Self {
        Self { state, storage }
    }

    /// Convert internal Message to slim ClaimMessage (no queue, no created_at)
    fn to_claim_message(msg: &Message, claim_token: String) -> ClaimMessage {
        ClaimMessage {
            msg_id: msg.msg_id.clone(),
            payload: msg.payload.clone(),
            metadata: msg.metadata.clone(),
            claim_token,
        }
    }

    /// Execute a Complete operation (shared by `complete` and `claim_and_complete`)
    async fn execute_complete(&self, req: CompleteRequest) -> Result<CompleteResponse, Status> {
        if !req.msg_ids.is_empty() && req.claim_tokens.len() != req.msg_ids.len() {
            return Err(Status::invalid_argument(
                "claim_tokens length must match msg_ids",
            ));
        }

        let has_state = req.state.as_ref().is_some_and(|s| {
            !s.namespace.is_empty() && (!s.puts.is_empty() || !s.deletes.is_empty())
        });
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
                    Err(e) => {
                        tracing::error!("Complete(ack) failed: {}", e);
                        Err(Status::internal(format!("Storage error: {e}")))
                    }
                }
            }

            // --- Nack: return messages to queue ---
            Some(complete_request::Action::Nack(_)) => {
                let result = if has_state {
                    self.storage
                        .nack_messages_with_state(
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
                        .nack_messages(
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
                    Err(e) => {
                        tracing::error!("Complete(nack) failed: {}", e);
                        Err(Status::internal(format!("Storage error: {e}")))
                    }
                }
            }

            // --- Forward: ack upstream + push to single downstream queue ---
            Some(complete_request::Action::Forward(fwd)) => {
                let downstream_messages: Vec<Message> = fwd
                    .payloads
                    .iter()
                    .map(|payload| Message::new(fwd.downstream_queue.clone(), payload.clone()))
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
                    Err(e) => {
                        if e.to_string().contains("QueueFull") {
                            Err(Status::resource_exhausted("QueueFull"))
                        } else {
                            tracing::error!("Complete(forward) failed: {}", e);
                            Ok(CompleteResponse {
                                success: false,
                                processed_count: 0,
                                new_msg_ids: vec![],
                            })
                        }
                    }
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
                        .map(|payload| {
                            let queue_name = format!("{}_p{}", sct.group_name, pp.partition_id);
                            Message::new(queue_name, payload.clone())
                        })
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
                    Err(e) => {
                        if e.to_string().contains("QueueFull") {
                            Err(Status::resource_exhausted("QueueFull"))
                        } else {
                            tracing::error!("Complete(scatter) failed: {}", e);
                            Ok(CompleteResponse {
                                success: false,
                                processed_count: 0,
                                new_msg_ids: vec![],
                            })
                        }
                    }
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
                let claimed = self
                    .storage
                    .claim_messages(&queue, batch_size, &req.worker_id, &req.lease_id)
                    .await
                    .map_err(|e| {
                        tracing::error!("Claim failed: {}", e);
                        Status::internal(format!("Storage error: {e}"))
                    })?;

                let messages: Vec<ClaimMessage> = claimed
                    .iter()
                    .map(|c| Self::to_claim_message(&c.message, c.claim_token.clone()))
                    .collect();

                let has_more = match self.storage.get_queue_stats(&queue).await {
                    Ok(meta) => meta.claim_seq < meta.push_seq,
                    Err(_) => false,
                };

                Ok(ClaimResponse {
                    messages,
                    has_more,
                    source_queue: String::new(),
                    source_partition: 0,
                })
            }

            // --- Group claim ---
            Some(claim_request::Source::Group(group)) => {
                if group.group_name.is_empty() {
                    return Err(Status::invalid_argument("group_name is required"));
                }

                let assigned: Vec<u32> = group
                    .assigned_partitions
                    .iter()
                    .map(|&p| p as u32)
                    .collect();

                let (claimed, source_queue, source_partition) = self
                    .storage
                    .claim_from_group(
                        &group.group_name,
                        batch_size,
                        &req.worker_id,
                        &req.lease_id,
                        &assigned,
                        group.allow_steal,
                        group.steal_pending_threshold as u64,
                    )
                    .await
                    .map_err(|e| {
                        tracing::error!("ClaimFromGroup failed: {}", e);
                        Status::internal(format!("Storage error: {e}"))
                    })?;

                let messages: Vec<ClaimMessage> = claimed
                    .iter()
                    .map(|c| Self::to_claim_message(&c.message, c.claim_token.clone()))
                    .collect();

                Ok(ClaimResponse {
                    messages,
                    has_more: false,
                    source_queue,
                    source_partition: source_partition as i32,
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

    async fn push(&self, request: Request<PushRequest>) -> Result<Response<PushResponse>, Status> {
        let req = request.into_inner();

        if req.payloads.is_empty() {
            return Err(Status::invalid_argument("at least one payload is required"));
        }

        let messages: Vec<Message> = req
            .payloads
            .iter()
            .map(|payload| {
                if req.metadata.is_empty() {
                    Message::new(req.queue.clone(), payload.clone())
                } else {
                    Message::with_metadata(req.queue.clone(), payload.clone(), req.metadata.clone())
                }
            })
            .collect();

        let msg_ids: Vec<String> = messages.iter().map(|m| m.msg_id.clone()).collect();

        match self.storage.push_messages(&req.queue, &messages).await {
            Ok(()) => Ok(Response::new(PushResponse { msg_ids })),
            Err(e) => {
                if e.to_string().contains("QueueFull") {
                    Err(Status::resource_exhausted("QueueFull"))
                } else {
                    tracing::error!("Push failed: {}", e);
                    Err(Status::internal(format!("Storage error: {e}")))
                }
            }
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

        if req.namespace.is_empty() {
            return Err(Status::invalid_argument("namespace is required"));
        }

        match self
            .storage
            .state_get_batch(&req.namespace, &req.keys)
            .await
        {
            Ok(values) => Ok(Response::new(StateGetResponse { values })),
            Err(e) => {
                tracing::error!("Failed to get state: {}", e);
                Err(Status::internal(format!("Storage error: {e}")))
            }
        }
    }

    async fn state_put(
        &self,
        request: Request<StatePutRequest>,
    ) -> Result<Response<StatePutResponse>, Status> {
        let req = request.into_inner();

        if req.namespace.is_empty() {
            return Err(Status::invalid_argument("namespace is required"));
        }

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
            Err(e) => {
                tracing::error!("Failed to put state: {}", e);
                Err(Status::internal(format!("Storage error: {e}")))
            }
        }
    }

    // =========================================================================
    // Heartbeat (unchanged)
    // =========================================================================

    type HeartbeatStreamStream = Pin<Box<dyn Stream<Item = Result<HeartbeatPong, Status>> + Send>>;

    async fn heartbeat_stream(
        &self,
        request: Request<Streaming<HeartbeatPing>>,
    ) -> Result<Response<Self::HeartbeatStreamStream>, Status> {
        let mut stream = request.into_inner();
        let (tx, rx) = mpsc::channel(16);
        let state = self.state.clone();

        const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(30);

        tokio::spawn(async move {
            let lease_id = uuid::Uuid::now_v7().to_string();

            loop {
                match timeout(HEARTBEAT_TIMEOUT, stream.next()).await {
                    Ok(Some(Ok(_ping))) => {
                        state.update_lease(&lease_id);
                        let pong = HeartbeatPong {
                            lease_id: lease_id.clone(),
                            ok: true,
                            next_ping_ms: 5000,
                        };
                        if tx.send(Ok(pong)).await.is_err() {
                            break;
                        }
                    }
                    Ok(Some(Err(e))) => {
                        tracing::warn!("Heartbeat stream error: {}", e);
                        break;
                    }
                    Ok(None) => break,
                    Err(_) => {
                        tracing::debug!("Heartbeat timeout, closing connection");
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
        match self.storage.create_queue(&req.queue, req.max_pending).await {
            Ok(()) => {
                self.state.get_or_create_queue(&req.queue);
                Ok(Response::new(CreateQueueResponse { created: true }))
            }
            Err(e) => {
                tracing::error!("Failed to create queue: {}", e);
                Err(Status::internal(format!("Storage error: {e}")))
            }
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
            Err(e) => {
                tracing::error!("Failed to delete queue: {}", e);
                Err(Status::internal(format!("Storage error: {e}")))
            }
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
                        },
                    );
                }
                Err(e) => {
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
            Err(e) => {
                tracing::error!("Failed to mark queue finished: {}", e);
                Err(Status::internal(format!("Storage error: {e}")))
            }
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
            Ok((finished, drained, pending_count, claimed_count)) => {
                Ok(Response::new(IsQueueFinishedResponse {
                    finished,
                    drained,
                    safe_to_exit: finished && drained,
                    pending_count: pending_count as i64,
                    claimed_count: claimed_count as i64,
                }))
            }
            Err(e) => {
                tracing::error!("Failed to check queue finished: {}", e);
                Err(Status::internal(format!("Storage error: {e}")))
            }
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

        let existed = self
            .storage
            .get_group_meta(&req.group_name)
            .await
            .map_err(|e| {
                tracing::error!("Failed to check group: {}", e);
                Status::internal(format!("Storage error: {e}"))
            })?
            .is_some();

        match self
            .storage
            .create_queue_group(
                &req.group_name,
                req.num_partitions as u32,
                req.max_pending_per_partition,
            )
            .await
        {
            Ok(meta) => {
                for queue_name in &meta.partition_queues {
                    self.state.get_or_create_queue(queue_name);
                }
                Ok(Response::new(CreateQueueGroupResponse {
                    queue_names: meta.partition_queues,
                    version: meta.version as i32,
                    created: !existed,
                }))
            }
            Err(e) => {
                tracing::error!("Failed to create queue group: {}", e);
                Err(Status::internal(format!("Storage error: {e}")))
            }
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
            Ok((all_finished, all_drained, partition_statuses)) => {
                let partitions = partition_statuses
                    .iter()
                    .map(|(pid, pending, claimed, finished)| PartitionStatus {
                        partition_id: *pid as i32,
                        pending_count: *pending as i64,
                        claimed_count: *claimed as i64,
                        finished: *finished,
                    })
                    .collect();
                Ok(Response::new(IsGroupFinishedResponse {
                    all_finished,
                    all_drained,
                    safe_to_exit: all_finished && all_drained,
                    partitions,
                }))
            }
            Err(e) => {
                tracing::error!("Failed is_group_finished: {}", e);
                Err(Status::internal(format!("Storage error: {e}")))
            }
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
            Err(e) => {
                tracing::error!("Failed get_group_stats: {}", e);
                Err(Status::internal(format!("Storage error: {e}")))
            }
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
            Err(e) => {
                tracing::error!("Failed mark_group_finished: {}", e);
                Err(Status::internal(format!("Storage error: {e}")))
            }
        }
    }
}
