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

// Chaos tests for the worker protocol: workers that die at every step of
// claim, renew and complete, zombies that come back with spent tokens or
// leases, broker restarts under running workers, poison messages, and steps
// of the clock that leases are measured with.
//
// The gRPC tests run a real broker on loopback with its recovery task, so
// leases come from real `HeartbeatStream`s; the storage tests drive recovery
// by hand on simulated time, which makes them exact at the timeout
// boundaries. A broker crash tears down the runtime the broker runs on
// without closing its database (`dst::tests::crash`) and reopens the same
// `file://` path, as a restarted process would.

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::future::Future;
    use std::task::Poll;
    use std::time::Duration;

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use tokio::runtime::Runtime;
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::transport::Channel;
    use tonic::{Code, Response, Status};

    use crate::dst::tests::{check_invariants, crash, crash_test_db_path};
    use crate::server::AnvilBrokerInner;
    use crate::service::proto::anvil_client::AnvilClient;
    use crate::service::proto::*;
    use crate::state::AnvilState;
    use crate::storage::tests::split_claims;
    use crate::storage::{AnvilStorage, NackOptions, Rejection, StorageError};
    use crate::types::{advance_sim_time_secs, set_sim_time_nanos, AnvilConfig, Message};
    use crate::types::{ClaimedMessage, SimClock};

    /// A lease timeout a test waits out: a fraction of a second, yet many
    /// heartbeat intervals, so a live worker never lapses on a loaded runner.
    const SHORT_TIMEOUT_SECS: f64 = 0.4;
    /// Never reached within a test.
    const LONG_TIMEOUT_SECS: f64 = 600.0;
    const RECOVERY_INTERVAL_SECS: f64 = 0.025;
    const KEEPALIVE_INTERVAL: Duration = Duration::from_millis(40);
    /// Bounds every wait, so a broker that never settles fails the test
    /// instead of hanging it.
    const WAIT_DEADLINE: Duration = Duration::from_secs(10);
    const POLL_INTERVAL: Duration = Duration::from_millis(10);
    /// Simulated epoch for the storage tests: whole seconds, so the timeout
    /// boundaries below are exact in `f64`.
    const SIM_EPOCH_NANOS: u64 = 1_735_689_600_000_000_000;

    // =====================================================================
    // gRPC harness
    // =====================================================================

    fn broker_config(db_path: &str, lease_timeout_secs: f64) -> AnvilConfig {
        AnvilConfig {
            host: "127.0.0.1".to_string(),
            db_path: db_path.to_string(),
            lease_timeout_secs,
            claim_age_timeout_secs: LONG_TIMEOUT_SECS,
            recovery_interval_secs: RECOVERY_INTERVAL_SECS,
            ..AnvilConfig::default()
        }
    }

    /// Starts a broker on an ephemeral loopback port and connects to it. The
    /// broker is returned because dropping it stops its recovery task.
    async fn start_broker(config: AnvilConfig) -> (AnvilBrokerInner, AnvilClient<Channel>) {
        let mut broker = AnvilBrokerInner::new(config).await.unwrap();
        let port = broker.start().await.unwrap();
        (broker, connect(port).await)
    }

    async fn connect(port: u16) -> AnvilClient<Channel> {
        AnvilClient::connect(format!("http://127.0.0.1:{port}"))
            .await
            .unwrap()
    }

    /// An open `HeartbeatStream`, pinged until it is dropped, which closes
    /// the stream the way a dead worker would. Every ping carries
    /// `ping_lease_id`, as the client sends the lease it last held.
    struct Heartbeat {
        lease_id: String,
        pinger: JoinHandle<()>,
    }

    impl Heartbeat {
        async fn open(client: &AnvilClient<Channel>, worker_id: &str) -> Self {
            Self::open_with_lease(client, worker_id, "").await
        }

        async fn open_with_lease(
            client: &AnvilClient<Channel>,
            worker_id: &str,
            ping_lease_id: &str,
        ) -> Self {
            let ping = HeartbeatPing {
                worker_id: worker_id.to_string(),
                lease_id: ping_lease_id.to_string(),
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

        fn worker<'a>(&'a self, id: &'a str) -> Worker<'a> {
            Worker {
                id,
                lease_id: &self.lease_id,
            }
        }
    }

    impl Drop for Heartbeat {
        fn drop(&mut self) {
            self.pinger.abort();
        }
    }

    /// The identity a worker presents on every request.
    struct Worker<'a> {
        id: &'a str,
        lease_id: &'a str,
    }

    async fn push(client: &AnvilClient<Channel>, queue: &str, count: usize) -> Vec<String> {
        client
            .clone()
            .push(PushRequest {
                queue: queue.to_string(),
                payloads: payloads(count),
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
        worker: &Worker<'_>,
        batch_size: usize,
    ) -> ClaimResponse {
        client
            .clone()
            .claim(ClaimRequest {
                source: Some(claim_request::Source::Queue(queue.to_string())),
                worker_id: worker.id.to_string(),
                lease_id: worker.lease_id.to_string(),
                batch_size: batch_size as i32,
                timeout_ms: 0,
            })
            .await
            .unwrap()
            .into_inner()
    }

    async fn complete(
        client: &AnvilClient<Channel>,
        queue: &str,
        claims: &[ClaimMessage],
        worker: &Worker<'_>,
        action: complete_request::Action,
    ) -> Result<Response<CompleteResponse>, Status> {
        client
            .clone()
            .complete(CompleteRequest {
                upstream_queue: queue.to_string(),
                msg_ids: claims.iter().map(|c| c.msg_id.clone()).collect(),
                claim_tokens: claims.iter().map(|c| c.claim_token.clone()).collect(),
                worker_id: worker.id.to_string(),
                lease_id: worker.lease_id.to_string(),
                action: Some(action),
                state: None,
            })
            .await
    }

    fn ack_action() -> complete_request::Action {
        complete_request::Action::Ack(AckAction {})
    }

    async fn renew(
        client: &AnvilClient<Channel>,
        queue: &str,
        claims: &[ClaimMessage],
        worker: &Worker<'_>,
    ) -> Result<Response<RenewClaimsResponse>, Status> {
        client
            .clone()
            .renew_claims(RenewClaimsRequest {
                queue: queue.to_string(),
                msg_ids: claims.iter().map(|c| c.msg_id.clone()).collect(),
                claim_tokens: claims.iter().map(|c| c.claim_token.clone()).collect(),
                worker_id: worker.id.to_string(),
                lease_id: worker.lease_id.to_string(),
            })
            .await
    }

    fn assert_fenced<T: std::fmt::Debug>(result: Result<T, Status>, what: &str) {
        let status = result.expect_err(what);
        assert_eq!(
            status.code(),
            Code::FailedPrecondition,
            "{what}: {status:?}"
        );
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

    /// Waits until `queue` holds exactly `pending` messages and none claimed.
    async fn wait_for_pending(client: &AnvilClient<Channel>, queue: &str, pending: usize) {
        tokio::time::timeout(WAIT_DEADLINE, async {
            loop {
                let s = stats(client, queue).await;
                if s.pending_count == pending as i64 && s.claimed_count == 0 {
                    return;
                }
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("[{queue}] {pending} pending not reached in {WAIT_DEADLINE:?}"))
    }

    fn payloads(count: usize) -> Vec<Vec<u8>> {
        (0..count).map(|i| format!("m{i}").into_bytes()).collect()
    }

    fn ids_of(claims: &[ClaimMessage]) -> HashSet<String> {
        claims.iter().map(|c| c.msg_id.clone()).collect()
    }

    // =====================================================================
    // Workers dying at each step of the protocol
    // =====================================================================

    /// Goal: a worker that dies anywhere in the protocol loses exactly the
    /// claims it had not completed: each comes back once with
    /// `delivery_count == 2`, and an ack that landed is never redelivered,
    /// even when the worker never saw its response and retried it. Method:
    /// three workers each claim one message; one dies right after claiming,
    /// one dies with a renewal in flight, one acks, retries the ack as if the
    /// response were lost, then dies. A survivor drains the queue.
    #[tokio::test]
    async fn test_worker_death_at_each_protocol_step() {
        let _clock = SimClock::acquire();
        let (_broker, client) = start_broker(broker_config("memory://", SHORT_TIMEOUT_SECS)).await;
        let queue = "steps";
        push(&client, queue, 3).await;
        let mut lost = HashSet::new();

        let heartbeat = Heartbeat::open(&client, "after-claim").await;
        let held = claim(&client, queue, &heartbeat.worker("after-claim"), 1).await;
        assert_eq!(held.messages[0].delivery_count, 1);
        lost.extend(ids_of(&held.messages));
        drop(heartbeat);

        let heartbeat = Heartbeat::open(&client, "mid-renew").await;
        let lease_id = heartbeat.lease_id.clone();
        let worker = Worker {
            id: "mid-renew",
            lease_id: &lease_id,
        };
        let held = claim(&client, queue, &worker, 1).await.messages;
        lost.extend(ids_of(&held));
        renew(&client, queue, &held, &worker).await.unwrap();
        let renewal = renew(&client, queue, &held, &worker);
        drop(heartbeat);
        // However the dying renewal ends, it must not keep the claim.
        let _ = renewal.await;

        let heartbeat = Heartbeat::open(&client, "after-ack").await;
        let worker = heartbeat.worker("after-ack");
        let held = claim(&client, queue, &worker, 1).await.messages;
        complete(&client, queue, &held, &worker, ack_action())
            .await
            .unwrap();
        assert_fenced(
            complete(&client, queue, &held, &worker, ack_action()).await,
            "retried ack of a settled message",
        );
        let acked = ids_of(&held);
        drop(heartbeat);

        wait_for_pending(&client, queue, 2).await;
        let survivor = Heartbeat::open(&client, "survivor").await;
        let worker = survivor.worker("survivor");
        let redelivered = claim(&client, queue, &worker, 10).await.messages;
        assert_eq!(ids_of(&redelivered), lost);
        assert!(ids_of(&redelivered).is_disjoint(&acked));
        assert!(redelivered.iter().all(|c| c.delivery_count == 2));
        complete(&client, queue, &redelivered, &worker, ack_action())
            .await
            .unwrap();

        let s = stats(&client, queue).await;
        assert_eq!(
            (
                s.pending_count,
                s.claimed_count,
                s.total_acked,
                s.total_pushed
            ),
            (0, 0, 3, 3)
        );
    }

    /// Goal: when every worker dies at once, including one holding a large
    /// batch, every message comes back exactly once with
    /// `delivery_count == 2`, and none is lost or duplicated. Method: 31
    /// workers claim 8 messages each and one claims 200, all under their own
    /// heartbeats; all streams close together; a survivor claims everything
    /// back in one batch and acks it in one Complete.
    #[tokio::test]
    async fn test_mass_worker_death_redelivers_every_message_once() {
        const SMALL_WORKERS: usize = 31;
        const SMALL_BATCH: usize = 8;
        const LARGE_BATCH: usize = 200;
        const MESSAGES: usize = SMALL_WORKERS * SMALL_BATCH + LARGE_BATCH;

        let _clock = SimClock::acquire();
        let (_broker, client) = start_broker(broker_config("memory://", SHORT_TIMEOUT_SECS)).await;
        let queue = "mass";
        let pushed: HashSet<String> = push(&client, queue, MESSAGES).await.into_iter().collect();

        let mut workers = Vec::new();
        for i in 0..=SMALL_WORKERS {
            let batch = if i == SMALL_WORKERS {
                LARGE_BATCH
            } else {
                SMALL_BATCH
            };
            let client = client.clone();
            workers.push(tokio::spawn(async move {
                let id = format!("worker-{i}");
                let heartbeat = Heartbeat::open(&client, &id).await;
                let held = claim(&client, queue, &heartbeat.worker(&id), batch).await;
                assert_eq!(held.messages.len(), batch);
                (heartbeat, held.messages)
            }));
        }
        let mut heartbeats = Vec::new();
        let mut first_deliveries = HashSet::new();
        for worker in workers {
            let (heartbeat, held) = worker.await.unwrap();
            heartbeats.push(heartbeat);
            for message in held {
                assert_eq!(message.delivery_count, 1);
                assert!(first_deliveries.insert(message.msg_id));
            }
        }
        assert_eq!(first_deliveries, pushed);
        drop(heartbeats);

        wait_for_pending(&client, queue, MESSAGES).await;
        let survivor = Heartbeat::open(&client, "survivor").await;
        let worker = survivor.worker("survivor");
        let redelivered = claim(&client, queue, &worker, MESSAGES + 1).await.messages;
        assert_eq!(redelivered.len(), MESSAGES);
        assert!(redelivered.iter().all(|c| c.delivery_count == 2));
        assert_eq!(ids_of(&redelivered), pushed);
        complete(&client, queue, &redelivered, &worker, ack_action())
            .await
            .unwrap();
        let s = stats(&client, queue).await;
        assert_eq!(
            (s.pending_count, s.claimed_count, s.total_acked),
            (0, 0, MESSAGES as i64)
        );
    }

    // =====================================================================
    // Zombies
    // =====================================================================

    /// Goal: a worker presumed dead that comes back can neither settle,
    /// renew nor revive the claim it lost. Method: a zombie claims, its
    /// heartbeat stops, and recovery hands the message to a second worker.
    /// The zombie then acks, nacks, forwards and renews with its old token,
    /// under its old lease, under a fresh one and under the new owner's
    /// identity; it reopens a heartbeat
    /// that pings its old lease id, which must not bring that lease back;
    /// and a claim it takes under the old lease must be reclaimed.
    #[tokio::test]
    async fn test_zombie_cannot_settle_renew_or_revive_a_lost_claim() {
        let _clock = SimClock::acquire();
        let (_broker, client) = start_broker(broker_config("memory://", SHORT_TIMEOUT_SECS)).await;
        let queue = "zombie";
        push(&client, queue, 1).await;

        let heartbeat = Heartbeat::open(&client, "zombie").await;
        let old_lease = heartbeat.lease_id.clone();
        let zombie = Worker {
            id: "zombie",
            lease_id: &old_lease,
        };
        let stale = claim(&client, queue, &zombie, 1).await.messages;
        drop(heartbeat);
        wait_for_pending(&client, queue, 1).await;

        let owner_heartbeat = Heartbeat::open(&client, "owner").await;
        let owner = owner_heartbeat.worker("owner");
        let owned = claim(&client, queue, &owner, 1).await.messages;
        assert_eq!(ids_of(&owned), ids_of(&stale));
        assert_eq!(owned[0].delivery_count, 2);

        let revived = Heartbeat::open_with_lease(&client, "zombie", &old_lease).await;
        assert_ne!(
            revived.lease_id, old_lease,
            "a new stream resumed a dead lease"
        );
        // The owner's identity covers a zombie that shares its worker id
        // and lease: then only the token fences it.
        for worker in [&zombie, &revived.worker("zombie"), &owner] {
            let forward = complete_request::Action::Forward(ForwardAction {
                downstream_queue: "zombie-out".to_string(),
                payloads: vec![b"zombie output".to_vec()],
            });
            let nack = complete_request::Action::Nack(NackAction::default());
            for (action, what) in [(ack_action(), "ack"), (nack, "nack"), (forward, "forward")] {
                assert_fenced(complete(&client, queue, &stale, worker, action).await, what);
            }
            assert_fenced(renew(&client, queue, &stale, worker).await, "renew");
        }

        complete(&client, queue, &owned, &owner, ack_action())
            .await
            .unwrap();
        let s = stats(&client, queue).await;
        assert_eq!((s.claimed_count, s.total_acked), (0, 1));

        // The old lease stays dead while the zombie heartbeats again on a
        // new one: a claim taken under it is reclaimed.
        push(&client, queue, 1).await;
        assert_eq!(claim(&client, queue, &zombie, 1).await.messages.len(), 1);
        wait_for_pending(&client, queue, 1).await;
    }

    /// Goal: a forward refused because its claim is lost leaves the
    /// downstream queue as it found it, so a zombie's refused output never
    /// shows as work, and a finished, empty downstream queue reports
    /// drained. Method: settle a claim, replay `ack_and_forward` with its
    /// spent token, mark the downstream queue finished and read its
    /// completion. The forward reserves a downstream seq before it checks
    /// the claim, and the reservation is closed with no key behind it.
    #[tokio::test]
    async fn test_fenced_forward_leaves_downstream_untouched() {
        let _clock = SimClock::acquire();
        let storage = AnvilStorage::new("memory://").await.unwrap();
        let (queue, downstream) = ("upstream", "downstream");
        storage.create_queue(queue, 0, None).await.unwrap();
        storage.create_queue(downstream, 0, None).await.unwrap();
        push_batch(&storage, queue, 1).await;
        let claimed = storage
            .claim_messages(queue, 1, "worker", "lease")
            .await
            .unwrap();
        let (ids, tokens) = split_claims(&claimed);
        storage
            .ack_messages(queue, &ids, &tokens, "worker", "lease")
            .await
            .unwrap();

        let output = Message::new(b"late".to_vec());
        assert_storage_fenced(
            storage
                .ack_and_forward(
                    queue,
                    &ids,
                    &tokens,
                    "worker",
                    "lease",
                    downstream,
                    &[output],
                )
                .await,
            "forward with a spent token",
        );
        storage.mark_queue_finished(downstream).await.unwrap();
        let completion = storage.check_queue_completion(downstream).await.unwrap();
        assert!(completion.drained);
        assert_eq!(completion.dead_count, 0);
    }

    /// Goal: a worker whose heartbeat stream breaks and reconnects within
    /// `lease_timeout_secs` keeps the claims it holds, as the lease contract
    /// promises ("claims held by a lease that misses heartbeats for
    /// lease_timeout_secs return to pending"). Method: claim under one
    /// stream, drop it, reopen at once pinging the old lease id (what the
    /// client sends), keep the new stream alive for several lease timeouts,
    /// then ack with the lease the new stream returned, which the client
    /// sends with every later request.
    #[tokio::test]
    async fn test_reconnected_heartbeat_keeps_in_flight_claims() {
        let _clock = SimClock::acquire();
        let (_broker, client) = start_broker(broker_config("memory://", SHORT_TIMEOUT_SECS)).await;
        let queue = "reconnect";
        push(&client, queue, 1).await;

        let first = Heartbeat::open(&client, "worker").await;
        let held = claim(&client, queue, &first.worker("worker"), 1)
            .await
            .messages;
        let old_lease = first.lease_id.clone();
        drop(first);
        let second = Heartbeat::open_with_lease(&client, "worker", &old_lease).await;

        tokio::time::sleep(Duration::from_secs_f64(SHORT_TIMEOUT_SECS * 4.0)).await;
        let s = stats(&client, queue).await;
        assert_eq!(s.claimed_count, 1, "a live worker's claim was reclaimed");
        complete(
            &client,
            queue,
            &held,
            &second.worker("worker"),
            ack_action(),
        )
        .await
        .expect("a live worker's ack was fenced after its stream reconnected");
    }

    /// Goal: the lease table forgets a lease once it is dead, so
    /// `anvil.leases.active` counts live workers and each recovery tick does
    /// not copy every lease the broker ever issued. Method: open and close
    /// several heartbeat streams, wait out many lease timeouts, and read the
    /// broker's lease table.
    #[tokio::test]
    async fn test_dead_leases_leave_the_lease_table() {
        let _clock = SimClock::acquire();
        let (broker, client) = start_broker(broker_config("memory://", SHORT_TIMEOUT_SECS)).await;
        for i in 0..4 {
            drop(Heartbeat::open(&client, &format!("worker-{i}")).await);
        }
        tokio::time::sleep(Duration::from_secs_f64(SHORT_TIMEOUT_SECS * 5.0)).await;
        assert_eq!(broker.state.lease_snapshot().len(), 0);
    }

    // =====================================================================
    // Broker restarts under running workers
    // =====================================================================

    /// A broker on its own runtime, so a test can kill it like a process.
    struct BrokerProcess {
        runtime: Runtime,
        broker: AnvilBrokerInner,
        port: u16,
    }

    impl BrokerProcess {
        fn start(config: AnvilConfig) -> Self {
            let runtime = Runtime::new().unwrap();
            let (broker, port) = runtime.block_on(async {
                let mut broker = AnvilBrokerInner::new(config).await.unwrap();
                let port = broker.start().await.unwrap();
                (broker, port)
            });
            Self {
                runtime,
                broker,
                port,
            }
        }

        fn kill(self) {
            crash(self.runtime, self.broker);
        }
    }

    /// Goal: across a broker crash, an acked message stays acked, a claim in
    /// flight comes back once with its delivery count raised and its old
    /// token fenced, and a queue marked finished and drained still reports
    /// drained. Method: on a file:// store, a worker claims three messages
    /// and acks one, and a second queue is finished and drained; the broker
    /// is killed while the worker's heartbeat runs, restarted on the same
    /// path, and the worker reconnects and carries on.
    #[test]
    fn test_broker_restart_under_running_workers() {
        let _clock = SimClock::acquire();
        let (dir, db_path) = crash_test_db_path("chaos_restart");
        let config = broker_config(&db_path, LONG_TIMEOUT_SECS);
        let (queue, finished) = ("work", "finished");
        let workers = Runtime::new().unwrap();

        let before = BrokerProcess::start(config.clone());
        let (old_heartbeat, held, acked) = workers.block_on(async {
            let client = connect(before.port).await;
            push(&client, queue, 3).await;
            push(&client, finished, 1).await;
            let heartbeat = Heartbeat::open(&client, "worker").await;
            let worker = heartbeat.worker("worker");
            let done = claim(&client, finished, &worker, 1).await.messages;
            complete(&client, finished, &done, &worker, ack_action())
                .await
                .unwrap();
            client
                .clone()
                .mark_queue_finished(MarkQueueFinishedRequest {
                    queue: finished.to_string(),
                })
                .await
                .unwrap();
            let mut held = claim(&client, queue, &worker, 3).await.messages;
            let acked = vec![held.remove(0)];
            complete(&client, queue, &acked, &worker, ack_action())
                .await
                .unwrap();
            (heartbeat, held, acked)
        });
        before.kill();

        let after = BrokerProcess::start(config);
        workers.block_on(async {
            drop(old_heartbeat);
            let client = connect(after.port).await;
            let heartbeat = Heartbeat::open(&client, "worker").await;
            let worker = heartbeat.worker("worker");
            wait_for_pending(&client, queue, 2).await;
            assert_fenced(
                complete(&client, queue, &held, &worker, ack_action()).await,
                "ack with a token from before the restart",
            );

            let redelivered = claim(&client, queue, &worker, 10).await.messages;
            assert_eq!(ids_of(&redelivered), ids_of(&held));
            assert!(ids_of(&redelivered).is_disjoint(&ids_of(&acked)));
            assert!(redelivered.iter().all(|c| c.delivery_count == 1));
            complete(&client, queue, &redelivered, &worker, ack_action())
                .await
                .unwrap();
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

            let drained = claim(&client, finished, &worker, 1).await;
            assert!(drained.messages.is_empty());
            assert!(drained.upstream_drained, "the drain signal did not survive");
        });
        after.kill();
        drop(workers);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Goal: a broker restart returns every claim in flight to pending, as
    /// the README documents, even for a worker that reconnects at once
    /// presenting its old lease: the lease table is in memory, so the
    /// restarted broker knows no lease from before. The claim is redelivered
    /// with the delivery count it had, the restart taking no delivery from
    /// it, and an ack of the claim made before the
    /// restart fails with `FAILED_PRECONDITION`. Method: claim under a long
    /// lease timeout, kill and restart the broker, reconnect the worker's
    /// heartbeat with its old lease id, then check the lease it gets, the
    /// redelivery and the old claim's ack.
    #[test]
    fn test_restart_reclaims_claims_of_workers_that_reconnect() {
        let _clock = SimClock::acquire();
        let (dir, db_path) = crash_test_db_path("chaos_restart_lease");
        let config = broker_config(&db_path, LONG_TIMEOUT_SECS);
        let queue = "work";
        let workers = Runtime::new().unwrap();

        let before = BrokerProcess::start(config.clone());
        let (old_lease, held) = workers.block_on(async {
            let client = connect(before.port).await;
            push(&client, queue, 1).await;
            let heartbeat = Heartbeat::open(&client, "worker").await;
            let held = claim(&client, queue, &heartbeat.worker("worker"), 1).await;
            // A durable write after the claim makes the claim durable too.
            push(&client, "barrier", 1).await;
            (heartbeat.lease_id.clone(), held.messages)
        });
        before.kill();

        let after = BrokerProcess::start(config);
        workers.block_on(async {
            let client = connect(after.port).await;
            let heartbeat = Heartbeat::open_with_lease(&client, "worker", &old_lease).await;
            assert_ne!(
                heartbeat.lease_id, old_lease,
                "a restarted broker honoured a lease from before the restart"
            );
            wait_for_pending(&client, queue, 1).await;
            let worker = heartbeat.worker("worker");
            assert_fenced(
                complete(&client, queue, &held, &worker, ack_action()).await,
                "ack of a claim made before the restart",
            );
            let redelivered = claim(&client, queue, &worker, 1).await.messages;
            assert_eq!(ids_of(&redelivered), ids_of(&held));
            assert!(redelivered.iter().all(|c| c.delivery_count == 1));
            complete(&client, queue, &redelivered, &worker, ack_action())
                .await
                .unwrap();
        });
        after.kill();
        drop(workers);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // =====================================================================
    // Configuration edges
    // =====================================================================

    /// Goal: a recovery interval of zero is refused up front; it must not
    /// leave a broker serving with no recovery at all. Method: start a broker
    /// with `recovery_interval_secs = 0`. Refusing beats running recovery
    /// back to back: a zero interval is a configuration mistake, not a wish
    /// to scan every claim in a busy loop, and a broker that refuses to start
    /// says so where the operator looks, while one that panics only its
    /// recovery task serves on and never reclaims a dead worker's claims.
    #[tokio::test]
    async fn test_zero_recovery_interval_is_refused_at_start() {
        let config = AnvilConfig {
            recovery_interval_secs: 0.0,
            ..broker_config("memory://", SHORT_TIMEOUT_SECS)
        };
        let mut broker = AnvilBrokerInner::new(config).await.unwrap();
        let error = broker.start().await.unwrap_err();
        assert!(
            error.to_string().contains("recovery_interval_secs"),
            "{error}"
        );
    }

    // =====================================================================
    // Storage-level harness, on simulated time
    // =====================================================================

    fn rejection(error: &StorageError) -> &Rejection {
        error
            .downcast_ref::<Rejection>()
            .unwrap_or_else(|| panic!("not a rejection: {error}"))
    }

    fn assert_storage_fenced<T>(result: Result<T, StorageError>, what: &str) {
        let Err(error) = result else {
            panic!("{what} succeeded");
        };
        assert!(
            matches!(rejection(&error), Rejection::Fenced(_)),
            "{what}: {error}"
        );
    }

    async fn push_batch(storage: &AnvilStorage, queue: &str, count: usize) -> Vec<String> {
        let messages: Vec<Message> = payloads(count).into_iter().map(Message::new).collect();
        storage.push_messages(queue, &messages).await.unwrap();
        messages.into_iter().map(|m| m.msg_id).collect()
    }

    /// Recovery as a restarted broker, or one whose workers all died, runs
    /// it: no lease is alive.
    async fn recover_all_dead(storage: &AnvilStorage) -> usize {
        storage
            .recover_expired_claims(LONG_TIMEOUT_SECS, LONG_TIMEOUT_SECS, Some(&HashMap::new()))
            .await
            .unwrap()
    }

    // =====================================================================
    // Poison messages and redelivery storms
    // =====================================================================

    /// Goal: a message that kills every worker that takes it gets a
    /// `delivery_count` one higher on each delivery, so a worker can park it
    /// as the README advises; the messages around it, including those in
    /// its batch that die with it, are all completed, none twice. Method:
    /// one poison message among normal ones, claimed in batches by three
    /// workers per round. A worker acks the messages it processed before
    /// the poison and dies holding the rest, unless the poison has reached
    /// `PARK_AT` deliveries, in which case it forwards it to a dead-letter
    /// queue. Recovery runs between rounds with every lease dead.
    #[tokio::test]
    async fn test_poison_message_is_parked_without_starving_others() {
        const NORMAL: usize = 23;
        const BATCH: usize = 4;
        const WORKERS: usize = 3;
        const PARK_AT: u32 = 4;
        const ROUNDS_MAX: usize = 30;

        let _clock = SimClock::acquire();
        let storage = AnvilStorage::new("memory://").await.unwrap();
        let (queue, dlq) = ("poison", "poison-dlq");
        storage.create_queue(queue, 0, None).await.unwrap();
        let mut normal: HashSet<String> =
            push_batch(&storage, queue, 1).await.into_iter().collect();
        let poison_id = push_batch(&storage, queue, 1).await.remove(0);
        normal.extend(push_batch(&storage, queue, NORMAL - 1).await);

        let mut deliveries: HashMap<String, Vec<u32>> = HashMap::new();
        let mut acked: HashSet<String> = HashSet::new();
        let mut parked = false;
        for round in 0..ROUNDS_MAX {
            if parked && acked.len() == NORMAL {
                break;
            }
            for w in 0..WORKERS {
                let lease = format!("lease-{round}-{w}");
                let claimed = storage
                    .claim_messages(queue, BATCH, "worker", &lease)
                    .await
                    .unwrap();
                for message in &claimed {
                    deliveries
                        .entry(message.message.msg_id.clone())
                        .or_default()
                        .push(message.delivery_count);
                }
                let mut done: Vec<ClaimedMessage> = Vec::new();
                let mut park = None;
                for message in claimed {
                    if message.message.msg_id != poison_id {
                        done.push(message);
                    } else if message.delivery_count >= PARK_AT {
                        park = Some(message);
                    } else {
                        break;
                    }
                }
                if !done.is_empty() {
                    let (ids, tokens) = split_claims(&done);
                    storage
                        .ack_messages(queue, &ids, &tokens, "worker", &lease)
                        .await
                        .unwrap();
                    for id in ids {
                        assert!(acked.insert(id), "a message was acked twice");
                    }
                }
                if let Some(poison) = park {
                    let (ids, tokens) = split_claims(std::slice::from_ref(&poison));
                    let letter = Message::new(poison.message.payload);
                    storage
                        .ack_and_forward(queue, &ids, &tokens, "worker", &lease, dlq, &[letter])
                        .await
                        .unwrap();
                    parked = true;
                }
            }
            recover_all_dead(&storage).await;
            check_invariants(&storage, &[queue.to_string()]).await;
        }

        assert!(parked, "the poison message was never parked");
        assert_eq!(acked, normal, "normal messages starved behind the poison");
        assert_eq!(deliveries[&poison_id], (1..=PARK_AT).collect::<Vec<_>>());
        for (id, counts) in &deliveries {
            let expected: Vec<u32> = (1..=counts.len() as u32).collect();
            assert_eq!(
                *counts, expected,
                "{id}: delivery counts skipped or repeated"
            );
        }
        let meta = storage.get_queue_stats(queue).await.unwrap();
        assert_eq!(
            (
                meta.total_acked,
                meta.claimed_count,
                meta.push_seq - meta.claim_seq
            ),
            (NORMAL as u64 + 1, 0, 0)
        );
        assert_eq!(storage.get_queue_stats(dlq).await.unwrap().total_pushed, 1);
    }

    /// Goal: when many workers keep dying on the same messages, each
    /// redelivery hands every message out exactly once more, with its
    /// `delivery_count` raised by one, and the counters stay exact. Method:
    /// eight workers claim the whole queue between them and all die, five
    /// rounds in a row; then one worker acks everything.
    #[tokio::test]
    async fn test_repeated_mass_deaths_count_every_delivery() {
        const WORKERS: usize = 8;
        const BATCH: usize = 8;
        const ROUNDS: u32 = 5;

        let _clock = SimClock::acquire();
        let storage = AnvilStorage::new("memory://").await.unwrap();
        let queue = "storm";
        let queues = [queue.to_string()];
        storage.create_queue(queue, 0, None).await.unwrap();
        let pushed: HashSet<String> = push_batch(&storage, queue, WORKERS * BATCH)
            .await
            .into_iter()
            .collect();

        for round in 1..=ROUNDS {
            let mut seen = HashSet::new();
            for w in 0..WORKERS {
                let lease = format!("lease-{round}-{w}");
                let claimed = storage
                    .claim_messages(queue, BATCH, "worker", &lease)
                    .await
                    .unwrap();
                assert_eq!(claimed.len(), BATCH);
                for message in claimed {
                    assert_eq!(message.delivery_count, round);
                    assert!(seen.insert(message.message.msg_id));
                }
            }
            assert_eq!(seen, pushed, "round {round}");
            check_invariants(&storage, &queues).await;
            assert_eq!(recover_all_dead(&storage).await, WORKERS * BATCH);
        }

        let claimed = storage
            .claim_messages(queue, WORKERS * BATCH, "last", "last")
            .await
            .unwrap();
        assert!(claimed.iter().all(|m| m.delivery_count == ROUNDS + 1));
        let (ids, tokens) = split_claims(&claimed);
        storage
            .ack_messages(queue, &ids, &tokens, "last", "last")
            .await
            .unwrap();
        check_invariants(&storage, &queues).await;
        let meta = storage.get_queue_stats(queue).await.unwrap();
        assert_eq!(
            (
                meta.total_acked,
                meta.claimed_count,
                meta.push_seq - meta.claim_seq
            ),
            ((WORKERS * BATCH) as u64, 0, 0)
        );
    }

    // =====================================================================
    // Shared identities and late renewals, on simulated time
    // =====================================================================

    /// Goal: two workers that share one worker id and one lease live and die
    /// together while their tokens keep each other's claims apart, and a
    /// renewal sent after the lease died cannot save a claim. Method: on
    /// simulated time, two workers claim under one lease and each tries to
    /// settle the other's message; the lease heartbeats, then lapses; one
    /// worker renews before recovery runs; recovery must reclaim both claims
    /// and fence the renewal that follows.
    #[tokio::test]
    async fn test_shared_lease_lives_and_dies_as_one() {
        const LEASE_TIMEOUT_SECS: f64 = 5.0;

        let _clock = SimClock::acquire();
        set_sim_time_nanos(SIM_EPOCH_NANOS);
        let storage = AnvilStorage::new("memory://").await.unwrap();
        let leases = AnvilState::new();
        let queue = "shared";
        storage.create_queue(queue, 0, None).await.unwrap();
        push_batch(&storage, queue, 2).await;
        let recover = || {
            let snapshot = leases.lease_snapshot();
            let storage = &storage;
            async move {
                storage
                    .recover_expired_claims(LEASE_TIMEOUT_SECS, LONG_TIMEOUT_SECS, Some(&snapshot))
                    .await
                    .unwrap()
            }
        };

        leases.update_lease("lease");
        let a = storage
            .claim_messages(queue, 1, "worker", "lease")
            .await
            .unwrap();
        let b = storage
            .claim_messages(queue, 1, "worker", "lease")
            .await
            .unwrap();
        let (a_ids, a_tokens) = split_claims(&a);
        let (b_ids, b_tokens) = split_claims(&b);
        assert_storage_fenced(
            storage
                .ack_messages(queue, &a_ids, &b_tokens, "worker", "lease")
                .await,
            "ack with the twin's token",
        );
        assert_storage_fenced(
            storage
                .nack_messages(
                    queue,
                    &b_ids,
                    &a_tokens,
                    "worker",
                    "lease",
                    NackOptions::default(),
                )
                .await,
            "nack with the twin's token",
        );

        advance_sim_time_secs(LEASE_TIMEOUT_SECS / 2.0);
        leases.update_lease("lease");
        assert_eq!(recover().await, 0, "a live shared lease lost its claims");

        advance_sim_time_secs(LEASE_TIMEOUT_SECS + 1.0);
        storage
            .renew_claims(queue, &a_ids, &a_tokens, "worker", "lease")
            .await
            .unwrap();
        advance_sim_time_secs(0.001);
        assert_eq!(
            recover().await,
            2,
            "a late renewal kept a dead lease's claim"
        );
        assert_storage_fenced(
            storage
                .renew_claims(queue, &a_ids, &a_tokens, "worker", "lease")
                .await,
            "renewal after the reclaim",
        );
        check_invariants(&storage, &[queue.to_string()]).await;
    }

    // =====================================================================
    // Time edges
    // =====================================================================

    /// Goal: both timeouts hold at their boundary and expire just past it; a
    /// renewal exactly at the claim-age boundary buys a full new window;
    /// zero timeouts reclaim as soon as any time passes; infinite timeouts
    /// never reclaim a live lease's claims but still reclaim a dead lease's.
    /// Method: on simulated time, claim under one lease and run recovery at,
    /// and one millisecond past, each boundary.
    #[tokio::test]
    async fn test_timeout_boundaries() {
        const TIMEOUT_SECS: f64 = 5.0;
        const PAST_SECS: f64 = 0.001;

        let _clock = SimClock::acquire();
        set_sim_time_nanos(SIM_EPOCH_NANOS);
        let storage = AnvilStorage::new("memory://").await.unwrap();
        let leases = AnvilState::new();
        let queue = "edges";
        storage.create_queue(queue, 0, None).await.unwrap();
        push_batch(&storage, queue, 1).await;
        let recover = |lease_timeout: f64, claim_age: f64| {
            let snapshot = leases.lease_snapshot();
            let storage = &storage;
            async move {
                storage
                    .recover_expired_claims(lease_timeout, claim_age, Some(&snapshot))
                    .await
                    .unwrap()
            }
        };
        let claim_one = |lease: &'static str| storage.claim_messages(queue, 1, "worker", lease);

        // Lease timeout: alive at the boundary, dead just past it.
        leases.update_lease("lease");
        assert_eq!(claim_one("lease").await.unwrap().len(), 1);
        advance_sim_time_secs(TIMEOUT_SECS);
        assert_eq!(recover(TIMEOUT_SECS, LONG_TIMEOUT_SECS).await, 0);
        advance_sim_time_secs(PAST_SECS);
        assert_eq!(recover(TIMEOUT_SECS, LONG_TIMEOUT_SECS).await, 1);

        // Claim age: held at the boundary; a renewal there restarts it.
        leases.update_lease("lease");
        let claimed = claim_one("lease").await.unwrap();
        let (ids, tokens) = split_claims(&claimed);
        advance_sim_time_secs(TIMEOUT_SECS);
        leases.update_lease("lease");
        assert_eq!(recover(LONG_TIMEOUT_SECS, TIMEOUT_SECS).await, 0);
        storage
            .renew_claims(queue, &ids, &tokens, "worker", "lease")
            .await
            .unwrap();
        advance_sim_time_secs(TIMEOUT_SECS);
        leases.update_lease("lease");
        assert_eq!(recover(LONG_TIMEOUT_SECS, TIMEOUT_SECS).await, 0);
        advance_sim_time_secs(PAST_SECS);
        leases.update_lease("lease");
        assert_eq!(recover(LONG_TIMEOUT_SECS, TIMEOUT_SECS).await, 1);

        // Zero timeouts: nothing expires in the instant of the claim, and
        // everything once any time has passed.
        leases.update_lease("lease");
        assert_eq!(claim_one("lease").await.unwrap().len(), 1);
        assert_eq!(recover(0.0, 0.0).await, 0);
        advance_sim_time_secs(PAST_SECS);
        assert_eq!(recover(0.0, 0.0).await, 1);

        // Infinite timeouts: a live lease, however old, keeps its claim; a
        // lease the broker never heard from is dead regardless.
        leases.update_lease("lease");
        assert_eq!(claim_one("lease").await.unwrap().len(), 1);
        push_batch(&storage, queue, 1).await;
        assert_eq!(claim_one("unknown-lease").await.unwrap().len(), 1);
        advance_sim_time_secs(1_000_000.0);
        assert_eq!(recover(f64::INFINITY, f64::INFINITY).await, 1);
        let meta = storage.get_queue_stats(queue).await.unwrap();
        assert_eq!((meta.claimed_count, meta.push_seq - meta.claim_seq), (1, 1));
        check_invariants(&storage, &[queue.to_string()]).await;
    }

    /// Goal: a dead worker's claims return once its lease has gone
    /// `lease_timeout` without a heartbeat, even if the wall clock stepped
    /// backwards meanwhile, as an NTP correction does. Method: on simulated
    /// time, a lease heartbeats, claims and dies; the wall clock steps back
    /// an hour; then two lease timeouts pass. Were leases stamped with the
    /// wall clock, the dead lease would look alive for the hour it lost.
    #[tokio::test]
    async fn test_backward_clock_step_does_not_extend_a_dead_lease() {
        const LEASE_TIMEOUT_SECS: f64 = 5.0;

        let _clock = SimClock::acquire();
        set_sim_time_nanos(SIM_EPOCH_NANOS);
        let storage = AnvilStorage::new("memory://").await.unwrap();
        let leases = AnvilState::new();
        let queue = "clock-back";
        storage.create_queue(queue, 0, None).await.unwrap();
        push_batch(&storage, queue, 1).await;
        leases.update_lease("lease");
        storage
            .claim_messages(queue, 1, "worker", "lease")
            .await
            .unwrap();

        set_sim_time_nanos(SIM_EPOCH_NANOS - 3_600_000_000_000);
        advance_sim_time_secs(2.0 * LEASE_TIMEOUT_SECS);
        let recovered = storage
            .recover_expired_claims(
                LEASE_TIMEOUT_SECS,
                LONG_TIMEOUT_SECS,
                Some(&leases.lease_snapshot()),
            )
            .await
            .unwrap();
        assert_eq!(recovered, 1, "a dead lease outlived its timeout");
    }

    /// Goal: a live worker keeps its claims when the wall clock steps
    /// forwards. Method: on simulated time, a lease heartbeats every second
    /// and claims; the wall clock jumps an hour ahead and recovery runs
    /// before the next heartbeat, as it does when its tick lands first. Were
    /// lease age or claim age read off the wall clock, the live worker would
    /// look dead and its claim would be taken from it.
    #[tokio::test]
    async fn test_forward_clock_step_keeps_a_live_lease() {
        const LEASE_TIMEOUT_SECS: f64 = 5.0;

        let _clock = SimClock::acquire();
        set_sim_time_nanos(SIM_EPOCH_NANOS);
        let storage = AnvilStorage::new("memory://").await.unwrap();
        let leases = AnvilState::new();
        let queue = "clock-forward";
        storage.create_queue(queue, 0, None).await.unwrap();
        push_batch(&storage, queue, 1).await;
        leases.update_lease("lease");
        storage
            .claim_messages(queue, 1, "worker", "lease")
            .await
            .unwrap();
        for _ in 0..3 {
            advance_sim_time_secs(1.0);
            leases.update_lease("lease");
        }

        set_sim_time_nanos(SIM_EPOCH_NANOS + 3_600_000_000_000);
        let recovered = storage
            .recover_expired_claims(
                LEASE_TIMEOUT_SECS,
                LONG_TIMEOUT_SECS,
                Some(&leases.lease_snapshot()),
            )
            .await
            .unwrap();
        assert_eq!(recovered, 0, "a live worker's claim was reclaimed");
    }

    // =====================================================================
    // Broker crash loops, on a file:// store
    // =====================================================================

    /// Goal: counters stay exact, nothing is lost or acked twice, every
    /// redelivery after a nack raises `delivery_count` by exactly one while
    /// one after a crash gives the abandoned delivery back, and the finished
    /// and drained state survives, across many crash and restart cycles
    /// with work in flight at each crash. Method: seeded cycles on one
    /// file:// store; each reopens it, checks the counters against what was
    /// acknowledged, reclaims as a restarted broker does, pushes, claims,
    /// acks part (which makes the claims durable), nacks part, leaves the
    /// rest claimed and crashes. The last cycle finishes and drains the
    /// queue; one more restart must still report it drained.
    #[test]
    fn test_counters_exact_across_crash_restart_loops() {
        const CYCLES: usize = 5;

        let _clock = SimClock::acquire();
        for seed in [7_u64, 8] {
            let (dir, db_path) = crash_test_db_path("chaos_loops");
            let queue = "loop";
            let queues = [queue.to_string()];
            let mut rng = StdRng::seed_from_u64(seed);
            let mut pushed: HashSet<String> = HashSet::new();
            let mut acked: HashSet<String> = HashSet::new();
            let mut last_delivery: HashMap<String, u32> = HashMap::new();

            for cycle in 0..=CYCLES {
                let last = cycle == CYCLES;
                let lease = format!("lease-{cycle}");
                let runtime = Runtime::new().unwrap();
                let storage = runtime.block_on(async {
                    let storage = AnvilStorage::new(&db_path).await.unwrap();
                    if cycle == 0 {
                        storage.create_queue(queue, 0, None).await.unwrap();
                    } else {
                        check_invariants(&storage, &queues).await;
                        let meta = storage.get_queue_stats(queue).await.unwrap();
                        assert_eq!(meta.total_pushed, pushed.len() as u64, "seed {seed}");
                        assert_eq!(meta.total_acked, acked.len() as u64, "seed {seed}");
                        recover_all_dead(&storage).await;
                    }
                    let batch = if last {
                        storage.mark_queue_finished(queue).await.unwrap();
                        pushed.len()
                    } else {
                        let count = rng.random_range(3..8);
                        pushed.extend(push_batch(&storage, queue, count).await);
                        rng.random_range(3..10)
                    };

                    let claimed = storage
                        .claim_messages(queue, batch, "worker", &lease)
                        .await
                        .unwrap();
                    for message in &claimed {
                        let id = &message.message.msg_id;
                        let expected = last_delivery.get(id).map_or(1, |n| n + 1);
                        assert_eq!(message.delivery_count, expected, "seed {seed} {id}");
                        last_delivery.insert(id.clone(), message.delivery_count);
                    }

                    let ack_count = if last {
                        claimed.len()
                    } else {
                        rng.random_range(1..=claimed.len())
                    };
                    let (to_ack, rest) = claimed.split_at(ack_count);
                    let (to_nack, left_claimed) = rest.split_at(rng.random_range(0..=rest.len()));
                    // The crash, not the worker, ends these deliveries.
                    for message in left_claimed {
                        *last_delivery.get_mut(&message.message.msg_id).unwrap() -= 1;
                    }
                    let (ids, tokens) = split_claims(to_ack);
                    storage
                        .ack_messages(queue, &ids, &tokens, "worker", &lease)
                        .await
                        .unwrap();
                    acked.extend(ids);
                    if !to_nack.is_empty() {
                        let (ids, tokens) = split_claims(to_nack);
                        storage
                            .nack_messages(
                                queue,
                                &ids,
                                &tokens,
                                "worker",
                                &lease,
                                NackOptions::default(),
                            )
                            .await
                            .unwrap();
                    }
                    storage
                });
                crash(runtime, storage);
            }
            assert_eq!(
                acked, pushed,
                "seed {seed}: the last cycle drains the queue"
            );

            let runtime = Runtime::new().unwrap();
            runtime.block_on(async {
                let storage = AnvilStorage::new(&db_path).await.unwrap();
                check_invariants(&storage, &queues).await;
                let completion = storage.check_queue_completion(queue).await.unwrap();
                assert!(completion.drained, "seed {seed}");
                assert_eq!(completion.dead_count, 0, "seed {seed}");
                let meta = storage.get_queue_stats(queue).await.unwrap();
                assert_eq!(meta.total_acked, pushed.len() as u64, "seed {seed}");
            });
            drop(runtime);
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// Goal: a bounded queue stays bounded after a broker restart. Method:
    /// fill a queue created with `max_pending = 2`, crash, reopen and push
    /// once more. A limit kept only in memory, set by `create_queue`, would
    /// be lost: nothing calls it again after a restart.
    #[test]
    fn test_bounded_queue_stays_bounded_across_restart() {
        let _clock = SimClock::acquire();
        let (dir, db_path) = crash_test_db_path("chaos_bounded");
        let queue = "bounded";
        let over = || Message::new(b"over".to_vec());

        let runtime = Runtime::new().unwrap();
        let storage = runtime.block_on(async {
            let storage = AnvilStorage::new(&db_path).await.unwrap();
            storage.create_queue(queue, 2, None).await.unwrap();
            push_batch(&storage, queue, 2).await;
            let error = storage.push_message(queue, &over()).await.unwrap_err();
            assert!(matches!(rejection(&error), Rejection::QueueFull(_)));
            storage
        });
        crash(runtime, storage);

        let runtime = Runtime::new().unwrap();
        let result = runtime.block_on(async {
            let storage = AnvilStorage::new(&db_path).await.unwrap();
            storage.push_message(queue, &over()).await
        });
        drop(runtime);
        let _ = std::fs::remove_dir_all(&dir);
        let error = result.expect_err("a full bounded queue took a push after a restart");
        assert!(matches!(rejection(&error), Rejection::QueueFull(_)));
    }

    // =====================================================================
    // Completes abandoned by their worker
    // =====================================================================

    /// Polls `future` up to `polls` times, yielding to the runtime between
    /// polls so the database's writer moves on, then drops it: the way tonic
    /// drops a handler when its worker disconnects mid-call.
    async fn poll_then_drop<F: Future>(future: F, polls: usize) -> Option<F::Output> {
        let mut future = std::pin::pin!(future);
        for _ in 0..polls {
            if let Poll::Ready(output) = futures::poll!(future.as_mut()) {
                return Some(output);
            }
            tokio::task::yield_now().await;
        }
        None
    }

    /// Goal: a forwarding Complete abandoned mid-call by a dying worker
    /// settles at most once: either it took effect with its output, or it
    /// did not and the retry does, and the counters agree either way.
    /// Method: for each number of polls before the drop, claim one message,
    /// poll its `ack_and_forward` that many times and drop it, then retry it
    /// with the same token: the retry must be fenced exactly when the first
    /// attempt landed.
    #[tokio::test]
    async fn test_abandoned_complete_settles_once() {
        let _clock = SimClock::acquire();
        let storage = AnvilStorage::new("memory://").await.unwrap();
        for polls in 1..=3 {
            let queue = format!("abandoned-{polls}");
            let downstream = format!("abandoned-{polls}-out");
            storage.create_queue(&queue, 0, None).await.unwrap();
            push_batch(&storage, &queue, 1).await;
            let claimed = storage
                .claim_messages(&queue, 1, "worker", "lease")
                .await
                .unwrap();
            let (ids, tokens) = split_claims(&claimed);
            let forward = || {
                let output = Message::new(b"out".to_vec());
                let (storage, queue, ids, tokens) = (&storage, &queue, &ids, &tokens);
                let downstream = &downstream;
                async move {
                    storage
                        .ack_and_forward(
                            queue,
                            ids,
                            tokens,
                            "worker",
                            "lease",
                            downstream,
                            &[output],
                        )
                        .await
                }
            };
            if let Some(result) = poll_then_drop(forward(), polls).await {
                result.unwrap();
            }
            // Let a write the dropped call submitted land before the retry.
            tokio::time::sleep(Duration::from_millis(300)).await;
            let landed = storage.get_queue_stats(&queue).await.unwrap().total_acked == 1;
            let retry = forward().await;
            if landed {
                assert_storage_fenced(retry, "retry of a Complete that landed");
            } else {
                retry.unwrap();
            }

            check_invariants(&storage, &[queue.clone(), downstream.clone()]).await;
            let up = storage.get_queue_stats(&queue).await.unwrap();
            let down = storage.get_queue_stats(&downstream).await.unwrap();
            assert_eq!((up.total_acked, up.claimed_count), (1, 0), "polls {polls}");
            assert_eq!(down.total_pushed, 1, "polls {polls}");
        }
    }
}
