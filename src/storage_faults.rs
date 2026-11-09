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

// The broker over a slow and unreliable object store (`fault_store`): what
// an RPC reports must match what is durable, counters must match the keys,
// and the broker must come back once the store does.
//
// A cancelled future stands in for a client deadline: when a gRPC client
// gives up on a call, tonic drops the handler's future at its next await,
// exactly as `tokio::time::timeout` does here.

#[cfg(test)]
#[allow(clippy::await_holding_lock)] // SIM_TIME_LOCK is intentionally held across awaits to serialize time-sensitive tests
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use slatedb::object_store::memory::InMemory;
    use slatedb::object_store::ObjectStore;
    use tonic::{Code, Request};

    use crate::dst::tests::{check_invariants, check_recovered, crash, DstSimulator};
    use crate::fault_store::{FaultStore, Faults, OpFaults};
    use crate::recovery::{GcTask, RecoveryTask};
    use crate::service::proto::anvil_server::Anvil;
    use crate::service::proto::*;
    use crate::service::AnvilService;
    use crate::state::AnvilState;
    use crate::storage::tests::split_claims;
    use crate::storage::AnvilStorage;
    use crate::types::{set_sim_time_nanos, AnvilConfig, Message, SIM_TIME_LOCK};

    /// Bounds every wait, so a wedged broker fails the test instead of
    /// hanging it. Generous: SlateDB backs off up to a second per retry.
    const WAIT_DEADLINE: Duration = Duration::from_secs(30);
    const POLL_INTERVAL: Duration = Duration::from_millis(20);

    fn memory() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    async fn open(store: &FaultStore) -> AnvilStorage {
        tokio::time::timeout(
            WAIT_DEADLINE,
            AnvilStorage::open("anvil".to_string(), Arc::new(store.clone())),
        )
        .await
        .expect("open timed out")
        .unwrap()
    }

    /// Faults of a store behind a congested network: requests fail, time
    /// out and lose the response to a put that landed.
    fn flaky() -> Faults {
        let op = OpFaults {
            latency: Duration::from_millis(2),
            fail_rate: 0.15,
            timeout_rate: 0.05,
            timeout: Duration::from_millis(200),
            land_then_fail_rate: 0.1,
        };
        Faults {
            put: op,
            get: op,
            list: op,
        }
    }

    /// Every request fails, as when the store answers only 503s.
    fn down() -> Faults {
        let op = OpFaults {
            fail_rate: 1.0,
            ..OpFaults::default()
        };
        Faults {
            put: op,
            get: op,
            list: op,
        }
    }

    fn slow_puts(latency: Duration) -> Faults {
        Faults {
            put: OpFaults {
                latency,
                ..OpFaults::default()
            },
            ..Faults::default()
        }
    }

    /// Claims everything claimable from `queue` and acks it.
    async fn drain(storage: &AnvilStorage, queue: &str) -> Vec<String> {
        let mut drained = Vec::new();
        loop {
            let claimed = storage
                .claim_messages(queue, 64, "drainer", "drainer-lease")
                .await
                .unwrap();
            if claimed.is_empty() {
                return drained;
            }
            let (ids, tokens) = split_claims(&claimed);
            storage
                .ack_messages(queue, &ids, &tokens, "drainer", "drainer-lease")
                .await
                .unwrap();
            drained.extend(ids);
        }
    }

    /// Goal: whatever the DST workload saw succeed over a flaky store
    /// survives a crash, and a broker can load it back over the same flaky
    /// store. Method: run the workload with transient failures, timeouts and
    /// lost put responses on every request, kill the runtime, reopen with the
    /// faults still on, and check every acknowledged push and ack.
    #[test]
    fn test_flaky_store_keeps_every_acknowledged_effect() {
        let _guard = SIM_TIME_LOCK.lock().unwrap();
        for seed in [11u64, 12] {
            let inner = memory();
            let store = FaultStore::new(inner.clone(), seed);
            store.set_faults(flaky());

            let crashed = tokio::runtime::Runtime::new().unwrap();
            let sim = crashed.block_on(async {
                let storage = open(&store).await;
                let mut sim = DstSimulator::with_storage(storage, seed, 2, 3).await;
                let ops = sim.generate_ops(50);
                for op in ops {
                    tokio::time::timeout(WAIT_DEADLINE, sim.execute(op))
                        .await
                        .expect("operation wedged on the flaky store");
                }
                sim
            });
            let queues = sim.queues.clone();
            let pushed = sim.pushed.clone();
            let acked = sim.acked.clone();
            crash(crashed, sim);
            let counts = store.counts();
            assert!(counts.failed.load(Ordering::Relaxed) > 0);
            assert!(counts.landed_then_failed.load(Ordering::Relaxed) > 0);

            let reopened = FaultStore::new(inner, seed + 1000);
            reopened.set_faults(flaky());
            let recovered = tokio::runtime::Runtime::new().unwrap();
            recovered.block_on(async {
                let storage = open(&reopened).await;
                tokio::time::timeout(
                    WAIT_DEADLINE,
                    check_recovered(&storage, &queues, &pushed, &acked),
                )
                .await
                .expect("recovery check wedged on the flaky store");
            });
        }
        set_sim_time_nanos(0);
    }

    /// Goal: a claim cancelled mid-flight, as a client deadline cancels it
    /// when the store is slow, leaves its messages claimable and the claimed
    /// count true. Method: restart the broker so the message bodies live only
    /// in the store, make every read slow, cancel a claim while it reads
    /// them, then lift the latency, drain the queue and mark it finished.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_claim_cancelled_mid_read_leaves_messages_claimable() {
        const MESSAGES: usize = 16;
        const BATCH: usize = 4;
        let store = FaultStore::new(memory(), 1);
        let queue = "q";
        let storage = open(&store).await;
        // Bodies span several blocks, so reading one is not served by the
        // blocks the reload of the queue's counters caches.
        let messages: Vec<Message> = (0..MESSAGES)
            .map(|_| Message::new(queue.to_string(), vec![7u8; 8 * 1024]))
            .collect();
        storage.push_messages(queue, &messages).await.unwrap();
        storage.close().await.unwrap();

        let storage = open(&store).await;
        storage.get_queue_stats(queue).await.unwrap();
        store.set_faults(Faults {
            get: OpFaults {
                latency: Duration::from_millis(500),
                ..OpFaults::default()
            },
            ..Faults::default()
        });
        let cancelled = tokio::time::timeout(
            Duration::from_millis(150),
            storage.claim_messages(queue, BATCH, "w", "l"),
        )
        .await;
        assert!(cancelled.is_err(), "the claim beat its deadline");
        store.set_faults(Faults::default());

        let drained = drain(&storage, queue).await;
        storage.mark_queue_finished(queue).await.unwrap();
        let meta = storage.get_queue_stats(queue).await.unwrap();
        let claimed_keys = storage.scan_claimed(Some(queue)).await.unwrap().len();
        assert_eq!(
            meta.claimed_count as usize, claimed_keys,
            "claimed count drifted from the keys: {meta:?}"
        );
        assert_eq!(
            drained.len(),
            MESSAGES,
            "messages stranded behind the claim cursor: {meta:?}"
        );
        let (_, drained_flag, _, _) = storage.check_queue_completion(queue).await.unwrap();
        assert!(drained_flag, "a finished, empty queue never drains");
    }

    /// Goal: a forward cut off by its deadline after its batch was submitted
    /// lands exactly once, and its retry is fenced rather than forwarding
    /// again. Method: slow every put, cancel the forward while it waits for
    /// durability, retry it, then crash and reopen.
    #[test]
    fn test_forward_cut_off_by_deadline_lands_once() {
        let inner = memory();
        let store = FaultStore::new(inner.clone(), 2);
        let (up, down) = ("up", "down");

        let crashed = tokio::runtime::Runtime::new().unwrap();
        let storage = crashed.block_on(async {
            let storage = open(&store).await;
            storage
                .push_message(up, &Message::new(up.to_string(), b"in".to_vec()))
                .await
                .unwrap();
            storage.create_queue(down, 0).await.unwrap();
            let claimed = storage.claim_messages(up, 1, "w", "l").await.unwrap();
            let (ids, tokens) = split_claims(&claimed);
            let output = [Message::new(down.to_string(), b"out".to_vec())];

            store.set_faults(slow_puts(Duration::from_millis(400)));
            let cut_off = tokio::time::timeout(
                Duration::from_millis(100),
                storage.ack_and_forward(up, &ids, &tokens, "w", "l", down, &output),
            )
            .await;
            assert!(cut_off.is_err(), "the forward beat its deadline");

            let retry = storage
                .ack_and_forward(up, &ids, &tokens, "w", "l", down, &output)
                .await;
            let error = retry.expect_err("a retried forward forwarded again");
            assert!(error.to_string().contains("message_not_claimed"), "{error}");
            check_invariants(&storage, &[up.to_string(), down.to_string()]).await;

            // Any later durable write is ordered after the forward's.
            storage
                .state_put_batch("ns", &HashMap::from([("k".to_string(), vec![1])]), &[])
                .await
                .unwrap();
            store.set_faults(Faults::default());
            storage
        });
        crash(crashed, storage);

        let recovered = tokio::runtime::Runtime::new().unwrap();
        recovered.block_on(async {
            let storage = open(&FaultStore::new(inner, 3)).await;
            assert_eq!(storage.scan_acked(Some(up)).await.unwrap().len(), 1);
            assert_eq!(drain(&storage, down).await.len(), 1);
            assert!(drain(&storage, up).await.is_empty());
        });
    }

    /// Goal: a push cut off by its deadline after its batch was submitted
    /// still lands, so a caller must not read the failure as "nothing
    /// happened"; and the counters stay true. Method: slow every put, cancel
    /// a push while it waits for durability, and check what is stored.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_push_cut_off_by_deadline_still_lands() {
        let store = FaultStore::new(memory(), 4);
        let queue = "q";
        let storage = open(&store).await;
        storage.create_queue(queue, 0).await.unwrap();

        store.set_faults(slow_puts(Duration::from_millis(400)));
        let message = Message::new(queue.to_string(), b"x".to_vec());
        let cut_off = tokio::time::timeout(
            Duration::from_millis(100),
            storage.push_message(queue, &message),
        )
        .await;
        assert!(cut_off.is_err(), "the push beat its deadline");
        store.set_faults(Faults::default());

        check_invariants(&storage, &[queue.to_string()]).await;
        assert_eq!(drain(&storage, queue).await, vec![message.msg_id]);
        let meta = storage.get_queue_stats(queue).await.unwrap();
        assert_eq!((meta.total_pushed, meta.total_acked), (1, 1));
    }

    /// Goal: heartbeats keep leases alive while every write is slow, so no
    /// live worker loses a claim and nothing is delivered twice. Method:
    /// workers claim, work past the lease timeout while heartbeating, and ack
    /// through the service while each put takes longer than the lease
    /// timeout, with recovery running often.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_slow_writes_do_not_expire_live_leases() {
        let _guard = SIM_TIME_LOCK.lock().unwrap();
        const WORKERS: usize = 3;
        const MESSAGES: usize = 3;
        const LEASE_TIMEOUT: Duration = Duration::from_secs(2);
        let store = FaultStore::new(memory(), 5);
        let storage = Arc::new(open(&store).await);
        let state = Arc::new(AnvilState::new());
        let service = Arc::new(AnvilService::new(
            state.clone(),
            storage.clone(),
            LEASE_TIMEOUT.as_secs_f64(),
        ));
        let config = AnvilConfig {
            lease_timeout_secs: LEASE_TIMEOUT.as_secs_f64(),
            claim_age_timeout_secs: 600.0,
            recovery_interval_secs: 0.05,
            ..AnvilConfig::default()
        };
        let mut recovery = RecoveryTask::new(storage.clone(), state.clone(), config);
        let queue = "q";
        service
            .push(Request::new(PushRequest {
                queue: queue.to_string(),
                payloads: (0..MESSAGES).map(|i| vec![i as u8]).collect(),
                metadata: HashMap::new(),
            }))
            .await
            .unwrap();

        store.set_faults(slow_puts(LEASE_TIMEOUT + Duration::from_millis(200)));
        recovery.start();
        let mut workers = Vec::new();
        for w in 0..WORKERS {
            let service = service.clone();
            let state = state.clone();
            workers.push(tokio::spawn(async move {
                let lease = format!("lease-{w}");
                let worker = format!("worker-{w}");
                state.update_lease(&lease);
                let heartbeat = {
                    let state = state.clone();
                    let lease = lease.clone();
                    tokio::spawn(async move {
                        loop {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            state.update_lease(&lease);
                        }
                    })
                };
                let mut done = Vec::new();
                loop {
                    let claimed = service
                        .claim(Request::new(ClaimRequest {
                            source: Some(claim_request::Source::Queue(queue.to_string())),
                            worker_id: worker.clone(),
                            lease_id: lease.clone(),
                            batch_size: 1,
                            timeout_ms: 0,
                        }))
                        .await
                        .unwrap()
                        .into_inner();
                    let Some(message) = claimed.messages.into_iter().next() else {
                        break;
                    };
                    tokio::time::sleep(LEASE_TIMEOUT + Duration::from_millis(200)).await;
                    service
                        .complete(Request::new(CompleteRequest {
                            upstream_queue: queue.to_string(),
                            msg_ids: vec![message.msg_id.clone()],
                            claim_tokens: vec![message.claim_token],
                            worker_id: worker.clone(),
                            lease_id: lease.clone(),
                            action: Some(complete_request::Action::Ack(AckAction {})),
                            state: None,
                        }))
                        .await
                        .expect("a live worker lost its claim to slow writes");
                    done.push((message.msg_id, message.delivery_count));
                }
                heartbeat.abort();
                done
            }));
        }
        let mut seen = HashSet::new();
        for worker in workers {
            let done = tokio::time::timeout(WAIT_DEADLINE, worker)
                .await
                .expect("workers wedged on slow writes")
                .unwrap();
            for (msg_id, delivery_count) in done {
                assert_eq!(delivery_count, 1, "{msg_id} redelivered");
                assert!(seen.insert(msg_id));
            }
        }
        recovery.stop_async().await;
        store.set_faults(Faults::default());
        assert_eq!(seen.len(), MESSAGES);
        check_invariants(&storage, &[queue.to_string()]).await;
    }

    /// Goal: a full store outage only stalls the broker: background loops do
    /// not spin, a write in flight completes once the store is back, a claim
    /// reclaimed during the outage is redelivered once, and nothing is lost
    /// across a crash. Method: fail every request for a while with recovery
    /// and GC running hot, count the requests, then restore the store.
    #[test]
    fn test_outage_stalls_then_broker_recovers() {
        let _guard = SIM_TIME_LOCK.lock().unwrap();
        const OUTAGE: Duration = Duration::from_secs(2);
        let inner = memory();
        let store = FaultStore::new(inner.clone(), 6);
        let queue = "q";

        let crashed = tokio::runtime::Runtime::new().unwrap();
        let (storage, tasks, pushed_id) = crashed.block_on(async {
            let storage = Arc::new(open(&store).await);
            // Claimed under a lease nobody heartbeats, so recovery reclaims
            // it during the outage.
            let orphan = Message::new(queue.to_string(), b"orphan".to_vec());
            storage.push_message(queue, &orphan).await.unwrap();
            let claimed = storage
                .claim_messages(queue, 1, "dead", "dead")
                .await
                .unwrap();
            let (ids, tokens) = split_claims(&claimed);

            let config = AnvilConfig {
                lease_timeout_secs: 0.1,
                claim_age_timeout_secs: 0.1,
                recovery_interval_secs: 0.025,
                gc_interval_secs: 0.025,
                acked_retention_secs: 0.0,
                ..AnvilConfig::default()
            };
            let state = Arc::new(AnvilState::new());
            let mut recovery = RecoveryTask::new(storage.clone(), state, config.clone());
            let mut gc = GcTask::new(storage.clone(), config);

            store.set_faults(down());
            let requests_before = store.counts().requests.load(Ordering::Relaxed);
            recovery.start();
            gc.start();
            let pushed = Message::new(queue.to_string(), b"during".to_vec());
            let pushed_id = pushed.msg_id.clone();
            let in_flight = {
                let storage = storage.clone();
                tokio::spawn(async move { storage.push_message(queue, &pushed).await })
            };
            tokio::time::sleep(OUTAGE).await;
            let requests = store.counts().requests.load(Ordering::Relaxed) - requests_before;
            assert!(
                requests < 100,
                "{requests} requests in {OUTAGE:?} of outage: a loop spins"
            );
            let stale_ack = storage
                .ack_messages(queue, &ids, &tokens, "dead", "dead")
                .await;
            assert!(stale_ack.is_err(), "acked a claim recovery took");

            store.set_faults(Faults::default());
            tokio::time::timeout(WAIT_DEADLINE, in_flight)
                .await
                .expect("a push begun in the outage never finished")
                .unwrap()
                .expect("a push begun in the outage failed");
            let deadline = Instant::now() + WAIT_DEADLINE;
            while storage.get_queue_stats(queue).await.unwrap().push_seq < 3 {
                assert!(Instant::now() < deadline, "the reclaim never landed");
                tokio::time::sleep(POLL_INTERVAL).await;
            }
            recovery.stop_async().await;
            gc.stop_async().await;
            check_invariants(&storage, &[queue.to_string()]).await;
            let fresh = Message::new(queue.to_string(), b"after".to_vec());
            storage.push_message(queue, &fresh).await.unwrap();
            (
                storage,
                (recovery, gc),
                (orphan.msg_id, pushed_id, fresh.msg_id),
            )
        });
        crash(crashed, (storage, tasks));

        let recovered = tokio::runtime::Runtime::new().unwrap();
        recovered.block_on(async {
            let storage = open(&FaultStore::new(inner, 7)).await;
            let drained: HashSet<String> = drain(&storage, queue).await.into_iter().collect();
            let (orphan, during, after) = pushed_id;
            assert_eq!(drained, HashSet::from([orphan, during, after]));
        });
    }

    /// Goal: once a second broker opens the same database and fences this
    /// one, this broker must answer UNAVAILABLE, not INTERNAL, the code a
    /// worker retries, since no retry against it can succeed. Method: open two
    /// brokers on one store and push through the fenced one's service.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_fenced_broker_stops_inviting_retries() {
        let store = FaultStore::new(memory(), 8);
        let queue = "q";
        let fenced = Arc::new(open(&store).await);
        let service = AnvilService::new(
            Arc::new(AnvilState::new()),
            fenced.clone(),
            AnvilConfig::default().lease_timeout_secs,
        );
        let push = || PushRequest {
            queue: queue.to_string(),
            payloads: vec![b"x".to_vec()],
            metadata: HashMap::new(),
        };
        service.push(Request::new(push())).await.unwrap();

        let successor = open(&store).await;
        for _ in 0..3 {
            let status = tokio::time::timeout(WAIT_DEADLINE, service.push(Request::new(push())))
                .await
                .expect("push to the fenced broker hung")
                .expect_err("the fenced broker acknowledged a push");
            assert_eq!(status.code(), Code::Unavailable, "{status:?}");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert_eq!(drain(&successor, queue).await.len(), 1);
    }
}
