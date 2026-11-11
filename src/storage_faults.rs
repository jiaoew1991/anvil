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
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
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
    use crate::storage::tests::{dead_counts, split_claims};
    use crate::storage::{AnvilStorage, NackOptions, Rejection};
    use crate::types::{AnvilConfig, Message, SimClock};

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

    /// Pushes `count` messages to `queue` and dead-letters them all, as a
    /// worker that knows no retry can succeed does. Returns their msg_ids.
    async fn dead_letter(storage: &AnvilStorage, queue: &str, count: usize) -> Vec<String> {
        let messages: Vec<Message> = (0..count).map(|i| Message::new(vec![i as u8])).collect();
        storage.push_messages(queue, &messages).await.unwrap();
        let claimed = storage
            .claim_messages(queue, count, "w", "l")
            .await
            .unwrap();
        assert_eq!(claimed.len(), count);
        let (ids, tokens) = split_claims(&claimed);
        let reject = NackOptions {
            dead_letter: true,
            reason: "rejected",
        };
        storage
            .nack_messages(queue, &ids, &tokens, "w", "l", reject)
            .await
            .unwrap();
        ids
    }

    /// Goal: whatever the DST workload saw succeed over a flaky store
    /// survives a crash, and a broker can load it back over the same flaky
    /// store. Method: run the workload with transient failures, timeouts and
    /// lost put responses on every request, kill the runtime, reopen with the
    /// faults still on, and check every acknowledged push and ack.
    #[test]
    fn test_flaky_store_keeps_every_acknowledged_effect() {
        let _clock = SimClock::acquire();
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
    }

    /// Asserts that the counters of `queue` account for every message
    /// pushed to it, and returns them as `dead_counts` does, which checks
    /// the dead count against the dead letter keys.
    async fn accounted(
        storage: &AnvilStorage,
        queue: &str,
        pushed: usize,
        when: &str,
    ) -> (u64, u64, u64, u64, u64) {
        let counts = dead_counts(storage, queue).await;
        let (pending, claimed, acked, dead, purged) = counts;
        let total_pushed = storage.get_queue_stats(queue).await.unwrap().total_pushed;
        assert_eq!(total_pushed, pushed as u64, "{when}");
        assert_eq!(
            total_pushed,
            pending + claimed + acked + dead + purged,
            "{when}: pushed = pending + claimed + acked + dead + purged, got {counts:?}"
        );
        counts
    }

    /// Goal: over a flaky store, a queue that dead-letters, redrives and
    /// purges accounts for every message pushed to it: pushed = pending +
    /// claimed + acked + dead + purged, by the counters and by the keys,
    /// when the workload stops and again after a crash and a reload.
    /// Method: with a delivery limit of 2 and transient failures, timeouts
    /// and lost put responses on every request, nack four messages to their
    /// limit, then run a producer, workers that ack, nack, reject or abandon
    /// what they claim, a reclaimer that takes claims from under them, and
    /// an operator that purges and redrives the dead letters it lists; leave
    /// a message in every state, kill the runtime, and reopen with the
    /// faults still on.
    #[test]
    fn test_flaky_store_accounts_for_dead_letters() {
        const QUEUE: &str = "q";
        const MAX_DELIVERIES: u32 = 2;
        const WORKERS: u64 = 3;
        const BATCHES: usize = 8;
        const BATCH: usize = 4;
        /// Claims each worker still makes once the producer is done.
        const CLAIMS_AFTER_PRODUCER: usize = 6;
        /// Bounds the whole workload, every operation of which may back off
        /// on the flaky store.
        const WORKLOAD_DEADLINE: Duration = Duration::from_secs(120);

        for seed in [21u64, 22] {
            let inner = memory();
            let store = FaultStore::new(inner.clone(), seed);
            store.set_faults(flaky());

            let crashed = tokio::runtime::Runtime::new().unwrap();
            let workload = crashed.block_on(async {
                let storage = Arc::new(open(&store).await);
                storage
                    .create_queue(QUEUE, 0, Some(MAX_DELIVERIES))
                    .await
                    .unwrap();
                let nack = |dead_letter| NackOptions {
                    dead_letter,
                    reason: "flaky",
                };

                let at_limit: Vec<Message> = (0..4).map(|i| Message::new(vec![i as u8])).collect();
                storage.push_messages(QUEUE, &at_limit).await.unwrap();
                let mut pushed: Vec<String> = at_limit.into_iter().map(|m| m.msg_id).collect();
                for delivery in 1..=MAX_DELIVERIES {
                    let claimed = storage.claim_messages(QUEUE, 4, "w", "l").await.unwrap();
                    assert!(claimed.iter().all(|c| c.delivery_count == delivery));
                    let (ids, tokens) = split_claims(&claimed);
                    storage
                        .nack_messages(QUEUE, &ids, &tokens, "w", "l", nack(false))
                        .await
                        .unwrap();
                }
                assert_eq!(accounted(&storage, QUEUE, 4, "at the limit").await.3, 4);

                let producer_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let workers_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let producer = {
                    let (storage, done) = (storage.clone(), producer_done.clone());
                    tokio::spawn(async move {
                        let mut pushed = Vec::new();
                        for _ in 0..BATCHES {
                            let messages: Vec<Message> =
                                (0..BATCH).map(|_| Message::new(b"work".to_vec())).collect();
                            storage.push_messages(QUEUE, &messages).await.unwrap();
                            pushed.extend(messages.into_iter().map(|m| m.msg_id));
                        }
                        done.store(true, Ordering::Release);
                        pushed
                    })
                };
                let workers: Vec<_> = (0..WORKERS)
                    .map(|w| {
                        let (storage, producer_done) = (storage.clone(), producer_done.clone());
                        tokio::spawn(async move {
                            let mut rng = StdRng::seed_from_u64(seed * 100 + w);
                            let (worker, lease) = (format!("w{w}"), format!("l{w}"));
                            let mut acked = Vec::new();
                            let mut claims_left = CLAIMS_AFTER_PRODUCER;
                            while claims_left > 0 {
                                if producer_done.load(Ordering::Acquire) {
                                    claims_left -= 1;
                                }
                                let claimed = storage
                                    .claim_messages(QUEUE, 3, &worker, &lease)
                                    .await
                                    .unwrap();
                                if claimed.is_empty() {
                                    tokio::time::sleep(POLL_INTERVAL).await;
                                }
                                for message in claimed {
                                    let ids = [message.message.msg_id];
                                    let tokens = [message.claim_token];
                                    let settled = match rng.random_range(0..10) {
                                        0..3 => storage
                                            .ack_messages(QUEUE, &ids, &tokens, &worker, &lease)
                                            .await
                                            .map(|()| acked.extend(ids)),
                                        // Abandoned: the reclaimer takes it.
                                        3 => continue,
                                        roll => {
                                            storage
                                                .nack_messages(
                                                    QUEUE,
                                                    &ids,
                                                    &tokens,
                                                    &worker,
                                                    &lease,
                                                    nack(roll == 4),
                                                )
                                                .await
                                        }
                                    };
                                    // The reclaimer took the claim first.
                                    if let Err(error) = settled {
                                        let fenced = error.downcast_ref::<Rejection>();
                                        assert!(
                                            matches!(fenced, Some(Rejection::Fenced(_))),
                                            "{error}"
                                        );
                                    }
                                }
                            }
                            acked
                        })
                    })
                    .collect();
                let reclaimer = {
                    let (storage, done) = (storage.clone(), workers_done.clone());
                    tokio::spawn(async move {
                        let no_live_leases = HashMap::new();
                        while !done.load(Ordering::Acquire) {
                            tokio::time::sleep(POLL_INTERVAL * 5).await;
                            storage
                                .recover_expired_claims(0.0, 0.0, Some(&no_live_leases))
                                .await
                                .unwrap();
                        }
                    })
                };
                let operator = {
                    let (storage, done) = (storage.clone(), workers_done.clone());
                    tokio::spawn(async move {
                        let mut purged = Vec::new();
                        let mut redriven = 0;
                        loop {
                            let last_pass = done.load(Ordering::Acquire);
                            let (letters, _) = storage
                                .list_dead_letters(QUEUE, None, 4, true)
                                .await
                                .unwrap();
                            let ids: Vec<String> =
                                letters.into_iter().map(|l| l.message.msg_id).collect();
                            // No one else takes a dead letter out, so none
                            // listed is missing.
                            let (to_purge, to_redrive) = ids.split_at(ids.len() / 2);
                            if !to_purge.is_empty() {
                                let result =
                                    storage.purge_dead_letters(QUEUE, to_purge).await.unwrap();
                                assert_eq!(result, (to_purge.to_vec(), Vec::new()));
                                purged.extend_from_slice(to_purge);
                            }
                            if !to_redrive.is_empty() {
                                let result = storage
                                    .redrive_dead_letters(QUEUE, to_redrive, false)
                                    .await
                                    .unwrap();
                                assert_eq!(result, (to_redrive.to_vec(), Vec::new()));
                                redriven += to_redrive.len();
                            }
                            if last_pass {
                                return (purged, redriven);
                            }
                            tokio::time::sleep(POLL_INTERVAL).await;
                        }
                    })
                };

                let (acked, purged, redriven) = tokio::time::timeout(WORKLOAD_DEADLINE, async {
                    pushed.extend(producer.await.unwrap());
                    let mut acked = Vec::new();
                    for worker in workers {
                        acked.extend(worker.await.unwrap());
                    }
                    workers_done.store(true, Ordering::Release);
                    reclaimer.await.unwrap();
                    let (purged, redriven) = operator.await.unwrap();
                    (acked, purged, redriven)
                })
                .await
                .expect("the workload wedged on the flaky store");
                assert!(purged.len() >= 2, "the letters at the limit were purged");
                assert!(redriven >= 2, "the letters at the limit were redriven");

                // A message in every state, whatever the workload left.
                pushed.extend(dead_letter(&storage, QUEUE, 2).await);
                let rest: Vec<Message> = (0..3).map(|i| Message::new(vec![i as u8])).collect();
                storage.push_messages(QUEUE, &rest).await.unwrap();
                pushed.extend(rest.into_iter().map(|m| m.msg_id));
                let claimed = storage.claim_messages(QUEUE, 2, "w", "l").await.unwrap();
                let (ids, tokens) = split_claims(&claimed);
                storage
                    .ack_messages(QUEUE, &ids[..1], &tokens[..1], "w", "l")
                    .await
                    .unwrap();
                let mut acked: HashSet<String> = acked.into_iter().collect();
                acked.insert(ids[0].clone());

                let counts = accounted(&storage, QUEUE, pushed.len(), "after the workload").await;
                let (pending, claimed, _, dead, _) = counts;
                assert!(pending > 0 && claimed > 0 && dead > 0, "{counts:?}");
                assert_eq!(counts.2, acked.len() as u64, "every ack counted once");
                assert_eq!(counts.4, purged.len() as u64, "every purge counted once");
                check_invariants(&storage, &[QUEUE.to_string()]).await;
                (storage, pushed, acked, purged, counts)
            });
            let (storage, pushed, acked, purged, counts) = workload;
            crash(crashed, storage);
            let faults = store.counts();
            assert!(faults.failed.load(Ordering::Relaxed) > 0);
            assert!(faults.landed_then_failed.load(Ordering::Relaxed) > 0);

            let reopened = FaultStore::new(inner, seed + 1000);
            reopened.set_faults(flaky());
            let recovered = tokio::runtime::Runtime::new().unwrap();
            recovered.block_on(async {
                let storage = open(&reopened).await;
                let reloaded = tokio::time::timeout(
                    WAIT_DEADLINE,
                    accounted(&storage, QUEUE, pushed.len(), "after the reload"),
                )
                .await
                .expect("the reload wedged on the flaky store");
                assert_eq!(reloaded, counts, "the reload changed the counters");

                // A purged message is gone for good, so it is not expected
                // back, and the totals leave no room for one that came back.
                let on_queue = |ids: &mut dyn Iterator<Item = &String>| {
                    ids.map(|id| (QUEUE.to_string(), id.clone()))
                        .collect::<HashSet<_>>()
                };
                let purged: HashSet<&String> = purged.iter().collect();
                let kept = on_queue(&mut pushed.iter().filter(|id| !purged.contains(id)));
                let acked = on_queue(&mut acked.iter());
                tokio::time::timeout(
                    WAIT_DEADLINE,
                    check_recovered(&storage, &[QUEUE.to_string()], &kept, &acked),
                )
                .await
                .expect("recovery check wedged on the flaky store");
                let (pending, ..) = accounted(&storage, QUEUE, pushed.len(), "drained").await;
                assert_eq!(pending, 0);
            });
        }
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
            .map(|_| Message::new(vec![7u8; 8 * 1024]))
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
        let drained_flag = storage.check_queue_completion(queue).await.unwrap().drained;
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
                .push_message(up, &Message::new(b"in".to_vec()))
                .await
                .unwrap();
            storage.create_queue(down, 0, None).await.unwrap();
            let claimed = storage.claim_messages(up, 1, "w", "l").await.unwrap();
            let (ids, tokens) = split_claims(&claimed);
            let output = [Message::new(b"out".to_vec())];

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
        storage.create_queue(queue, 0, None).await.unwrap();

        store.set_faults(slow_puts(Duration::from_millis(400)));
        let message = Message::new(b"x".to_vec());
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

    /// Goal: a redrive cut off by its deadline after its batch was submitted
    /// lands exactly once: its retry finds no dead letter under those ids,
    /// the counters match the keys, and the queue's dead letter lock is
    /// free for the next redrive and purge. Method: slow every put, cancel a
    /// redrive while it waits for durability, retry it with the puts still
    /// slow, redrive and purge other letters, then crash and reopen.
    #[test]
    fn test_redrive_cut_off_by_deadline_lands_once() {
        let inner = memory();
        let store = FaultStore::new(inner.clone(), 9);
        let queue = "q";

        let crashed = tokio::runtime::Runtime::new().unwrap();
        let (storage, redriven_ids) = crashed.block_on(async {
            let storage = open(&store).await;
            let dead = dead_letter(&storage, queue, 4).await;
            assert_eq!(dead_counts(&storage, queue).await, (0, 0, 0, 4, 0));

            store.set_faults(slow_puts(Duration::from_millis(400)));
            let cut_off = tokio::time::timeout(
                Duration::from_millis(100),
                storage.redrive_dead_letters(queue, &dead[..2], false),
            )
            .await;
            assert!(cut_off.is_err(), "the redrive beat its deadline");
            assert_eq!(dead_counts(&storage, queue).await, (2, 0, 0, 2, 0));

            let retry = tokio::time::timeout(
                WAIT_DEADLINE,
                storage.redrive_dead_letters(queue, &dead[..2], false),
            )
            .await
            .expect("the cancelled redrive kept the dead letter lock")
            .unwrap();
            assert_eq!(retry, (Vec::new(), dead[..2].to_vec()));
            assert_eq!(dead_counts(&storage, queue).await, (2, 0, 0, 2, 0));
            store.set_faults(Faults::default());

            let later = tokio::time::timeout(WAIT_DEADLINE, async {
                let redriven = storage
                    .redrive_dead_letters(queue, &dead[2..3], false)
                    .await
                    .unwrap();
                let purged = storage.purge_dead_letters(queue, &dead[3..]).await.unwrap();
                (redriven, purged)
            })
            .await
            .expect("the cancelled redrive kept the dead letter lock");
            assert_eq!(later.0, (dead[2..3].to_vec(), Vec::new()));
            assert_eq!(later.1, (dead[3..].to_vec(), Vec::new()));
            assert_eq!(dead_counts(&storage, queue).await, (3, 0, 0, 0, 1));
            check_invariants(&storage, &[queue.to_string()]).await;
            (storage, dead[..3].to_vec())
        });
        crash(crashed, storage);

        let recovered = tokio::runtime::Runtime::new().unwrap();
        recovered.block_on(async {
            let storage = open(&FaultStore::new(inner, 10)).await;
            assert_eq!(dead_counts(&storage, queue).await, (3, 0, 0, 0, 1));
            let claimed = storage.claim_messages(queue, 64, "w", "l").await.unwrap();
            let delivered: Vec<(String, u32)> = claimed
                .iter()
                .map(|c| (c.message.msg_id.clone(), c.delivery_count))
                .collect();
            let expected: Vec<(String, u32)> = redriven_ids.into_iter().map(|id| (id, 1)).collect();
            assert_eq!(delivered, expected, "each redriven message once, anew");
            assert_eq!(dead_counts(&storage, queue).await, (0, 3, 0, 0, 1));
        });
    }

    /// Goal: a redrive or purge cancelled before it submits its batch does
    /// nothing and gives back the queue's dead letter lock. Method: restart
    /// the broker so the dead letters live only in the store, make every
    /// read slow, cancel a redrive and a purge while they read their
    /// letters, then lift the latency and run both.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_redrive_and_purge_cancelled_mid_read_do_nothing() {
        let store = FaultStore::new(memory(), 13);
        let queue = "q";
        let storage = open(&store).await;
        // Letters span several blocks, so reading one is not served by the
        // blocks an earlier read cached.
        let messages: Vec<Message> = (0..4).map(|_| Message::new(vec![7u8; 8 * 1024])).collect();
        storage.push_messages(queue, &messages).await.unwrap();
        let claimed = storage.claim_messages(queue, 4, "w", "l").await.unwrap();
        let (dead, tokens) = split_claims(&claimed);
        let reject = NackOptions {
            dead_letter: true,
            reason: "rejected",
        };
        storage
            .nack_messages(queue, &dead, &tokens, "w", "l", reject)
            .await
            .unwrap();
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
        let deadline = Duration::from_millis(150);
        let redrive = storage.redrive_dead_letters(queue, &dead[..2], false);
        let cancelled = tokio::time::timeout(deadline, redrive).await;
        assert!(cancelled.is_err(), "the redrive beat its deadline");
        let purge = storage.purge_dead_letters(queue, &dead[2..]);
        let cancelled = tokio::time::timeout(deadline, purge).await;
        assert!(cancelled.is_err(), "the purge beat its deadline");
        store.set_faults(Faults::default());

        assert_eq!(dead_counts(&storage, queue).await, (0, 0, 0, 4, 0));
        let (redriven, purged) = tokio::time::timeout(WAIT_DEADLINE, async {
            let redriven = storage
                .redrive_dead_letters(queue, &dead[..2], false)
                .await
                .unwrap();
            let purged = storage.purge_dead_letters(queue, &dead[2..]).await.unwrap();
            (redriven, purged)
        })
        .await
        .expect("a cancelled redrive or purge kept the dead letter lock");
        assert_eq!(redriven, (dead[..2].to_vec(), Vec::new()));
        assert_eq!(purged, (dead[2..].to_vec(), Vec::new()));
        assert_eq!(dead_counts(&storage, queue).await, (2, 0, 0, 0, 2));
        check_invariants(&storage, &[queue.to_string()]).await;
    }

    /// Goal: a purge cut off by its deadline after its batch was submitted
    /// lands exactly once: its retry finds no dead letter under those ids,
    /// `total_purged` counts them once, and the queue's dead letter lock is
    /// free for the next purge and redrive. Method: slow every put, cancel a
    /// purge while it waits for durability, retry it with the puts still
    /// slow, purge and redrive other letters, then crash and reopen.
    #[test]
    fn test_purge_cut_off_by_deadline_lands_once() {
        let inner = memory();
        let store = FaultStore::new(inner.clone(), 11);
        let queue = "q";

        let crashed = tokio::runtime::Runtime::new().unwrap();
        let (storage, redriven_id) = crashed.block_on(async {
            let storage = open(&store).await;
            let dead = dead_letter(&storage, queue, 4).await;

            store.set_faults(slow_puts(Duration::from_millis(400)));
            let cut_off = tokio::time::timeout(
                Duration::from_millis(100),
                storage.purge_dead_letters(queue, &dead[..2]),
            )
            .await;
            assert!(cut_off.is_err(), "the purge beat its deadline");
            assert_eq!(dead_counts(&storage, queue).await, (0, 0, 0, 2, 2));

            let retry =
                tokio::time::timeout(WAIT_DEADLINE, storage.purge_dead_letters(queue, &dead[..2]))
                    .await
                    .expect("the cancelled purge kept the dead letter lock")
                    .unwrap();
            assert_eq!(retry, (Vec::new(), dead[..2].to_vec()));
            assert_eq!(dead_counts(&storage, queue).await, (0, 0, 0, 2, 2));
            store.set_faults(Faults::default());

            let later = tokio::time::timeout(WAIT_DEADLINE, async {
                let purged = storage
                    .purge_dead_letters(queue, &dead[2..3])
                    .await
                    .unwrap();
                let redriven = storage
                    .redrive_dead_letters(queue, &dead[3..], false)
                    .await
                    .unwrap();
                (purged, redriven)
            })
            .await
            .expect("the cancelled purge kept the dead letter lock");
            assert_eq!(later.0, (dead[2..3].to_vec(), Vec::new()));
            assert_eq!(later.1, (dead[3..].to_vec(), Vec::new()));
            assert_eq!(dead_counts(&storage, queue).await, (1, 0, 0, 0, 3));
            check_invariants(&storage, &[queue.to_string()]).await;
            (storage, dead[3].clone())
        });
        crash(crashed, storage);

        let recovered = tokio::runtime::Runtime::new().unwrap();
        recovered.block_on(async {
            let storage = open(&FaultStore::new(inner, 12)).await;
            assert_eq!(dead_counts(&storage, queue).await, (1, 0, 0, 0, 3));
            assert_eq!(drain(&storage, queue).await, vec![redriven_id]);
            assert_eq!(dead_counts(&storage, queue).await, (0, 0, 1, 0, 3));
        });
    }

    /// Goal: heartbeats keep leases alive while every write is slow, so no
    /// live worker loses a claim and nothing is delivered twice. Method:
    /// workers claim, work past the lease timeout while heartbeating, and ack
    /// through the service while each put takes longer than the lease
    /// timeout, with recovery running often.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_slow_writes_do_not_expire_live_leases() {
        let _clock = SimClock::acquire();
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
        let _clock = SimClock::acquire();
        const OUTAGE: Duration = Duration::from_secs(2);
        let inner = memory();
        let store = FaultStore::new(inner.clone(), 6);
        let queue = "q";

        let crashed = tokio::runtime::Runtime::new().unwrap();
        let (storage, tasks, pushed_id) = crashed.block_on(async {
            let storage = Arc::new(open(&store).await);
            // Claimed under a lease nobody heartbeats, so recovery reclaims
            // it during the outage.
            let orphan = Message::new(b"orphan".to_vec());
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
            let pushed = Message::new(b"during".to_vec());
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
            let fresh = Message::new(b"after".to_vec());
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
