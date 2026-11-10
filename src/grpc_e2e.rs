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

// End-to-end tests of the gRPC surface: a real broker, with its recovery
// task, bound to a loopback socket and driven by the generated tonic client,
// so leases come from real `HeartbeatStream`s and expire on the real clock.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::{Duration, Instant};

    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::transport::Channel;
    use tonic::{Response, Status};

    use crate::server::AnvilBrokerInner;
    use crate::service::proto::anvil_client::AnvilClient;
    use crate::service::proto::*;
    use crate::types::{AnvilConfig, SimClock};

    /// Short enough that a test waits a fraction of a second for a lease or
    /// a claim to expire, long enough that a heartbeat or renewal every
    /// `KEEPALIVE_INTERVAL` never lets one lapse on a loaded runner.
    const SHORT_TIMEOUT_SECS: f64 = 0.4;
    /// Never reached within a test, for the timeout a test does not exercise.
    const LONG_TIMEOUT_SECS: f64 = 600.0;
    const RECOVERY_INTERVAL: Duration = Duration::from_millis(25);
    const KEEPALIVE_INTERVAL: Duration = Duration::from_millis(40);
    /// Bounds every wait, so a broker that never reclaims fails the test
    /// instead of hanging it.
    const WAIT_DEADLINE: Duration = Duration::from_secs(10);
    const POLL_INTERVAL: Duration = Duration::from_millis(10);

    /// Starts a broker on an ephemeral loopback port. The broker is returned
    /// because dropping it stops its recovery task.
    async fn start_broker(
        lease_timeout_secs: f64,
        claim_age_timeout_secs: f64,
    ) -> (AnvilBrokerInner, AnvilClient<Channel>) {
        let config = AnvilConfig {
            host: "127.0.0.1".to_string(),
            db_path: "memory://".to_string(),
            lease_timeout_secs,
            claim_age_timeout_secs,
            recovery_interval_secs: RECOVERY_INTERVAL.as_secs_f64(),
            ..AnvilConfig::default()
        };
        let mut broker = AnvilBrokerInner::new(config).await.unwrap();
        let port = broker.start().await.unwrap();
        let client = AnvilClient::connect(format!("http://127.0.0.1:{port}"))
            .await
            .unwrap();
        (broker, client)
    }

    /// An open `HeartbeatStream`, pinged every `KEEPALIVE_INTERVAL` until it
    /// is dropped, which closes the stream the way a dead worker would.
    struct Heartbeat {
        lease_id: String,
        pinger: JoinHandle<()>,
    }

    impl Heartbeat {
        async fn open(client: &AnvilClient<Channel>, worker_id: &str) -> Self {
            let ping = HeartbeatPing {
                worker_id: worker_id.to_string(),
                lease_id: String::new(),
                timestamp: 0,
            };
            let (tx, rx) = mpsc::channel(1);
            tx.send(ping.clone()).await.unwrap();
            let mut pongs = client
                .clone()
                .heartbeat_stream(ReceiverStream::new(rx))
                .await
                .unwrap()
                .into_inner();
            let lease_id = pongs.message().await.unwrap().unwrap().lease_id;
            let pinger = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(KEEPALIVE_INTERVAL).await;
                    if tx.send(ping.clone()).await.is_err() {
                        return;
                    }
                    if !matches!(pongs.message().await, Ok(Some(_))) {
                        return;
                    }
                }
            });
            Self { lease_id, pinger }
        }
    }

    impl Drop for Heartbeat {
        fn drop(&mut self) {
            self.pinger.abort();
        }
    }

    async fn create_queue(client: &AnvilClient<Channel>, queue: &str) {
        client
            .clone()
            .create_queue(CreateQueueRequest {
                queue: queue.to_string(),
                max_pending: 0,
            })
            .await
            .unwrap();
    }

    async fn push(client: &AnvilClient<Channel>, queue: &str, payloads: &[&[u8]]) -> Vec<String> {
        client
            .clone()
            .push(PushRequest {
                queue: queue.to_string(),
                payloads: payloads.iter().map(|p| p.to_vec()).collect(),
                metadata: HashMap::new(),
            })
            .await
            .unwrap()
            .into_inner()
            .msg_ids
    }

    async fn claim(
        client: &AnvilClient<Channel>,
        queue: &str,
        worker_id: &str,
        lease_id: &str,
        batch_size: i32,
    ) -> Vec<ClaimMessage> {
        client
            .clone()
            .claim(ClaimRequest {
                source: Some(claim_request::Source::Queue(queue.to_string())),
                worker_id: worker_id.to_string(),
                lease_id: lease_id.to_string(),
                batch_size,
                timeout_ms: 0,
            })
            .await
            .unwrap()
            .into_inner()
            .messages
    }

    async fn complete(
        client: &AnvilClient<Channel>,
        queue: &str,
        claims: &[&ClaimMessage],
        worker_id: &str,
        lease_id: &str,
        action: complete_request::Action,
        state: Option<StateUpdate>,
    ) -> Result<Response<CompleteResponse>, Status> {
        client
            .clone()
            .complete(CompleteRequest {
                upstream_queue: queue.to_string(),
                msg_ids: claims.iter().map(|c| c.msg_id.clone()).collect(),
                claim_tokens: claims.iter().map(|c| c.claim_token.clone()).collect(),
                worker_id: worker_id.to_string(),
                lease_id: lease_id.to_string(),
                action: Some(action),
                state,
            })
            .await
    }

    async fn ack(
        client: &AnvilClient<Channel>,
        queue: &str,
        claims: &[&ClaimMessage],
        worker_id: &str,
        lease_id: &str,
    ) -> Result<Response<CompleteResponse>, Status> {
        let action = complete_request::Action::Ack(AckAction {});
        complete(client, queue, claims, worker_id, lease_id, action, None).await
    }

    /// A fenced Complete is refused either with an error status or with
    /// `success: false`, depending on the action; which one is not what these
    /// tests pin down.
    fn refused(result: &Result<Response<CompleteResponse>, Status>) -> bool {
        result.as_ref().map_or(true, |r| !r.get_ref().success)
    }

    async fn stats(client: &AnvilClient<Channel>, queue: &str) -> QueueStats {
        client
            .clone()
            .get_stats(GetStatsRequest {
                queue: queue.to_string(),
            })
            .await
            .unwrap()
            .into_inner()
            .queues
            .remove(queue)
            .unwrap()
    }

    async fn wait_for_stats(
        client: &AnvilClient<Channel>,
        queue: &str,
        done: impl Fn(&QueueStats) -> bool,
    ) -> QueueStats {
        tokio::time::timeout(WAIT_DEADLINE, async {
            loop {
                let s = stats(client, queue).await;
                if done(&s) {
                    return s;
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("[{queue}] stats did not settle within {WAIT_DEADLINE:?}"))
    }

    /// Goal: the basic worker cycle works over the wire. Method: push three
    /// payloads, claim them under a heartbeat lease, ack them with their
    /// tokens, and check the claim order, the delivery counts and the stats.
    #[tokio::test]
    async fn test_grpc_push_claim_ack() {
        let _clock = SimClock::acquire();
        let (_broker, client) = start_broker(LONG_TIMEOUT_SECS, LONG_TIMEOUT_SECS).await;
        let queue = "e2e";
        create_queue(&client, queue).await;
        let pushed = push(&client, queue, &[b"a", b"b", b"c"]).await;
        let lease = Heartbeat::open(&client, "worker").await;

        let claimed = claim(&client, queue, "worker", &lease.lease_id, 10).await;
        let claimed_ids: Vec<&str> = claimed.iter().map(|c| c.msg_id.as_str()).collect();
        assert_eq!(claimed_ids, pushed);
        let payloads: Vec<&[u8]> = claimed.iter().map(|c| c.payload.as_slice()).collect();
        assert_eq!(payloads, [b"a", b"b", b"c"]);
        assert!(claimed.iter().all(|c| c.delivery_count == 1));

        let all: Vec<&ClaimMessage> = claimed.iter().collect();
        let acked = ack(&client, queue, &all, "worker", &lease.lease_id)
            .await
            .unwrap();
        assert!(acked.get_ref().success);
        assert_eq!(acked.get_ref().processed_count, 3);

        let s = stats(&client, queue).await;
        assert_eq!(
            (
                s.pending_count,
                s.claimed_count,
                s.total_pushed,
                s.total_acked
            ),
            (0, 0, 3, 3)
        );
        assert!(claim(&client, queue, "worker", &lease.lease_id, 10)
            .await
            .is_empty());
    }

    /// Goal: a worker whose `HeartbeatStream` stays open keeps its claims for
    /// as long as it pings, however long past `lease_timeout_secs`. Method:
    /// claim under a live stream, let several lease timeouts and many
    /// recovery ticks pass, then check the claim is still held and its token
    /// still acks.
    #[tokio::test]
    async fn test_grpc_open_heartbeat_keeps_claims() {
        let _clock = SimClock::acquire();
        let (_broker, client) = start_broker(SHORT_TIMEOUT_SECS, LONG_TIMEOUT_SECS).await;
        let queue = "e2e";
        create_queue(&client, queue).await;
        push(&client, queue, &[b"work"]).await;
        let lease = Heartbeat::open(&client, "worker").await;
        let claimed = claim(&client, queue, "worker", &lease.lease_id, 1).await;
        assert_eq!(claimed.len(), 1);

        tokio::time::sleep(Duration::from_secs_f64(SHORT_TIMEOUT_SECS * 4.0)).await;

        let s = stats(&client, queue).await;
        assert_eq!((s.pending_count, s.claimed_count), (0, 1));
        let acked = ack(&client, queue, &[&claimed[0]], "worker", &lease.lease_id).await;
        assert!(acked.unwrap().get_ref().success);
    }

    /// Goal: when a worker's `HeartbeatStream` goes away, its claims return to
    /// pending once `lease_timeout_secs` has passed, are redelivered with
    /// `delivery_count == 2` and a fresh token, and the dead worker's old
    /// token can no longer ack. Method: claim, drop the stream, wait for the
    /// broker's own recovery task to requeue the claim, reclaim it under a
    /// second worker's lease, then ack with the old and the new token.
    #[tokio::test]
    async fn test_grpc_dropped_heartbeat_reclaims_claims() {
        let _clock = SimClock::acquire();
        let (_broker, client) = start_broker(SHORT_TIMEOUT_SECS, LONG_TIMEOUT_SECS).await;
        let queue = "e2e";
        create_queue(&client, queue).await;
        push(&client, queue, &[b"work"]).await;
        let dead = Heartbeat::open(&client, "dead").await;
        let first = claim(&client, queue, "dead", &dead.lease_id, 1).await;
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].delivery_count, 1);

        let dead_lease_id = dead.lease_id.clone();
        drop(dead);
        let dropped_at = Instant::now();
        wait_for_stats(&client, queue, |s| {
            (s.pending_count, s.claimed_count) == (1, 0)
        })
        .await;
        // The lease was last seen at most one keepalive before the drop.
        let lease_timeout = Duration::from_secs_f64(SHORT_TIMEOUT_SECS);
        assert!(
            dropped_at.elapsed() + KEEPALIVE_INTERVAL >= lease_timeout,
            "claim reclaimed {:?} after its lease went silent, before the {lease_timeout:?} lease timeout",
            dropped_at.elapsed()
        );

        let live = Heartbeat::open(&client, "live").await;
        let second = claim(&client, queue, "live", &live.lease_id, 1).await;
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].msg_id, first[0].msg_id);
        assert_eq!(second[0].payload, b"work");
        assert_eq!(second[0].delivery_count, 2);
        assert_ne!(second[0].claim_token, first[0].claim_token);

        let stale = ack(&client, queue, &[&first[0]], "dead", &dead_lease_id).await;
        assert!(refused(&stale), "the dead worker acked a reclaimed claim");
        // Only the token tells this ack apart from the current holder's.
        let stale = ack(&client, queue, &[&first[0]], "live", &live.lease_id).await;
        assert!(refused(&stale), "an ack with a reclaimed token succeeded");
        let s = stats(&client, queue).await;
        assert_eq!((s.claimed_count, s.total_acked), (1, 0));

        let fresh = ack(&client, queue, &[&second[0]], "live", &live.lease_id).await;
        assert!(fresh.unwrap().get_ref().success);
        let s = stats(&client, queue).await;
        assert_eq!((s.claimed_count, s.total_acked), (0, 1));
    }

    /// Goal: `RenewClaims` over RPC keeps a claim alive past
    /// `claim_age_timeout_secs` under a live lease. Method: claim two
    /// messages under one live heartbeat, renew only one of them in a loop,
    /// and wait for the broker to reclaim the other as stuck; the renewed one
    /// must stay claimed through several more claim-age windows and then ack.
    #[tokio::test]
    async fn test_grpc_renew_claims_keeps_claim_past_claim_age() {
        let _clock = SimClock::acquire();
        let (_broker, client) = start_broker(LONG_TIMEOUT_SECS, SHORT_TIMEOUT_SECS).await;
        let queue = "e2e";
        create_queue(&client, queue).await;
        push(&client, queue, &[b"renewed", b"stuck"]).await;
        let lease = Heartbeat::open(&client, "worker").await;
        let claimed = claim(&client, queue, "worker", &lease.lease_id, 2).await;
        assert_eq!(claimed.len(), 2);
        let renewed = claimed[0].clone();
        assert_eq!(renewed.payload, b"renewed");

        let renewer = {
            let client = client.clone();
            let request = RenewClaimsRequest {
                queue: queue.to_string(),
                msg_ids: vec![renewed.msg_id.clone()],
                claim_tokens: vec![renewed.claim_token.clone()],
                worker_id: "worker".to_string(),
                lease_id: lease.lease_id.clone(),
            };
            tokio::spawn(async move {
                loop {
                    client.clone().renew_claims(request.clone()).await.unwrap();
                    tokio::time::sleep(KEEPALIVE_INTERVAL).await;
                }
            })
        };

        wait_for_stats(&client, queue, |s| {
            (s.pending_count, s.claimed_count) == (1, 1)
        })
        .await;
        tokio::time::sleep(Duration::from_secs_f64(SHORT_TIMEOUT_SECS * 3.0)).await;
        renewer.abort();
        // A panic in the renewer (a refused renewal) would surface here.
        assert!(renewer.await.unwrap_err().is_cancelled());

        let s = stats(&client, queue).await;
        assert_eq!((s.pending_count, s.claimed_count), (1, 1));
        let acked = ack(&client, queue, &[&renewed], "worker", &lease.lease_id).await;
        assert!(acked.unwrap().get_ref().success);
    }

    /// Goal: one scatter Complete acks the upstream message, pushes each
    /// payload to the partition it names and writes state together, and a
    /// retry of it with the spent token changes nothing. Method: scatter one
    /// claim to partitions 0 and 2 of a three-partition group with a state
    /// put, read every effect back over RPC, then replay the Complete with
    /// different payloads and state and check none of them landed.
    #[tokio::test]
    async fn test_grpc_scatter_complete_writes_all_effects_once() {
        let _clock = SimClock::acquire();
        let (_broker, client) = start_broker(LONG_TIMEOUT_SECS, LONG_TIMEOUT_SECS).await;
        let (upstream, group) = ("src", "fanout");
        create_queue(&client, upstream).await;
        let partitions = client
            .clone()
            .create_queue_group(CreateQueueGroupRequest {
                group_name: group.to_string(),
                num_partitions: 3,
                max_pending_per_partition: 0,
            })
            .await
            .unwrap()
            .into_inner()
            .queue_names;
        push(&client, upstream, &[b"input"]).await;
        let lease = Heartbeat::open(&client, "worker").await;
        let claimed = claim(&client, upstream, "worker", &lease.lease_id, 1).await;
        let upstream_msg = &claimed[0];

        let scatter = |payloads: [&[&[u8]]; 3]| {
            complete_request::Action::Scatter(ScatterAction {
                group_name: group.to_string(),
                partitions: payloads
                    .iter()
                    .enumerate()
                    .map(|(pid, p)| PartitionPayload {
                        partition_id: pid as i32,
                        payloads: p.iter().map(|b| b.to_vec()).collect(),
                    })
                    .collect(),
            })
        };
        let state = |value: &[u8]| StateUpdate {
            namespace: "progress".to_string(),
            puts: HashMap::from([(upstream_msg.msg_id.clone(), value.to_vec())]),
            deletes: vec![],
        };
        let done = complete(
            &client,
            upstream,
            &[upstream_msg],
            "worker",
            &lease.lease_id,
            scatter([&[b"p0-a", b"p0-b"], &[], &[b"p2-a"]]),
            Some(state(b"first")),
        )
        .await
        .unwrap();
        assert!(done.get_ref().success);
        assert_eq!(done.get_ref().new_msg_ids.len(), 3);

        let replay = complete(
            &client,
            upstream,
            &[upstream_msg],
            "worker",
            &lease.lease_id,
            scatter([&[b"replay"], &[b"replay"], &[b"replay"]]),
            Some(state(b"replay")),
        )
        .await;
        assert!(refused(&replay), "a replayed Complete succeeded");

        let s = stats(&client, upstream).await;
        assert_eq!((s.claimed_count, s.total_acked), (0, 1));
        let expected: [&[&[u8]]; 3] = [&[b"p0-a", b"p0-b"], &[], &[b"p2-a"]];
        for (pid, (queue, want)) in partitions.iter().zip(expected).enumerate() {
            assert_eq!(stats(&client, queue).await.total_pushed, want.len() as i64);
            let got = client
                .clone()
                .claim(ClaimRequest {
                    source: Some(claim_request::Source::Group(GroupClaimSource {
                        group_name: group.to_string(),
                        assigned_partitions: vec![pid as i32],
                        allow_steal: false,
                        steal_pending_threshold: 0,
                    })),
                    worker_id: "worker".to_string(),
                    lease_id: lease.lease_id.clone(),
                    batch_size: 10,
                    timeout_ms: 0,
                })
                .await
                .unwrap()
                .into_inner();
            let payloads: Vec<&[u8]> = got.messages.iter().map(|m| m.payload.as_slice()).collect();
            assert_eq!(payloads, want, "partition {pid}");
            if !want.is_empty() {
                assert_eq!(got.source_partition, pid as i32);
                assert_eq!(&got.source_queue, queue);
            }
        }

        let stored = client
            .clone()
            .state_get(StateGetRequest {
                namespace: "progress".to_string(),
                keys: vec![upstream_msg.msg_id.clone()],
            })
            .await
            .unwrap()
            .into_inner()
            .values;
        assert_eq!(stored[&upstream_msg.msg_id], b"first");
    }
}
