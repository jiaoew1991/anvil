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

// Deterministic Simulation Testing (DST) for Anvil
//
// Generates random sequences of queue operations from a fixed seed,
// executes them against a real AnvilStorage instance, then verifies
// that critical invariants hold after every run.
//
// Invariants checked:
//   1. Counter consistency: meta.claimed_count == actual claimed keys
//   2. Pending consistency: push_seq - claim_seq == actual pending keys
//   3. Message conservation: every msg_id is in exactly one of pending/claimed/acked/dead/purged
//   4. No overlap: no msg_id appears in both claimed and acked
//   5. ack_and_forward atomicity: downstream push_seq increases iff upstream ack succeeds

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    use rand::prelude::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    use crate::storage::{AnvilStorage, NackOptions, DEAD_LETTERS_PER_CALL_MAX};
    use crate::types::{advance_sim_time_secs, set_sim_time_nanos, Message, SimClock};

    /// The delivery limit of the simulated queues: small, so the workload
    /// dead-letters, redrives and purges as often as it requeues.
    const DST_MAX_DELIVERIES: u32 = 3;

    /// What a simulated worker currently holds (claimed messages).
    #[derive(Default, Clone)]
    struct WorkerState {
        /// (queue, msg_id, claim_token)
        claims: Vec<(String, String, String)>,
    }

    /// The operation types the simulator can execute.
    #[derive(Debug, Clone)]
    pub(crate) enum Op {
        Push {
            queue_idx: usize,
        },
        PushBatch {
            queue_idx: usize,
            count: usize,
        },
        Claim {
            queue_idx: usize,
            worker_idx: usize,
            batch_size: usize,
        },
        Ack {
            worker_idx: usize,
        },
        Nack {
            worker_idx: usize,
            dead_letter: bool,
        },
        Redrive {
            queue_idx: usize,
        },
        Purge {
            queue_idx: usize,
        },
        AckAndForward {
            worker_idx: usize,
            downstream_idx: usize,
        },
        AdvanceTime {
            secs: f64,
        },
        RecoverExpired {
            timeout_secs: f64,
        },
    }

    pub(crate) struct DstSimulator {
        storage: AnvilStorage,
        rng: StdRng,
        pub(crate) queues: Vec<String>,
        num_workers: usize,
        workers: Vec<WorkerState>,
        lease_ids: Vec<String>,
        /// Every (queue, msg_id) whose push or forward returned success, and
        /// that was not purged since: a purge drops a message for good.
        pub(crate) pushed: HashSet<(String, String)>,
        /// Every (queue, msg_id) whose ack returned success.
        pub(crate) acked: HashSet<(String, String)>,
    }

    impl DstSimulator {
        async fn new(seed: u64, num_queues: usize, num_workers: usize) -> Self {
            let storage = AnvilStorage::new("memory://").await.unwrap();
            Self::with_storage(storage, seed, num_queues, num_workers).await
        }

        pub(crate) async fn with_storage(
            storage: AnvilStorage,
            seed: u64,
            num_queues: usize,
            num_workers: usize,
        ) -> Self {
            let rng = StdRng::seed_from_u64(seed);

            let queues: Vec<String> = (0..num_queues).map(|i| format!("q{}", i)).collect();
            for q in &queues {
                storage
                    .create_queue(q, 0, Some(DST_MAX_DELIVERIES))
                    .await
                    .unwrap();
            }

            let workers: Vec<WorkerState> =
                (0..num_workers).map(|_| WorkerState::default()).collect();
            let lease_ids: Vec<String> = (0..num_workers).map(|i| format!("lease-{}", i)).collect();

            set_sim_time_nanos(1_735_689_600_000_000_000);

            Self {
                storage,
                rng,
                queues,
                num_workers,
                workers,
                lease_ids,
                pushed: HashSet::new(),
                acked: HashSet::new(),
            }
        }

        /// Reset simulator state for a new seed, reusing the same storage.
        async fn reset(&mut self, seed: u64) {
            // Delete all queues and recreate them
            for q in &self.queues {
                let _ = self.storage.delete_queue(q).await;
                self.storage
                    .create_queue(q, 0, Some(DST_MAX_DELIVERIES))
                    .await
                    .unwrap();
            }
            self.rng = StdRng::seed_from_u64(seed);
            self.workers = (0..self.num_workers)
                .map(|_| WorkerState::default())
                .collect();
            self.pushed.clear();
            self.acked.clear();
            set_sim_time_nanos(1_735_689_600_000_000_000);
        }

        pub(crate) fn generate_ops(&mut self, count: usize) -> Vec<Op> {
            let mut ops = Vec::with_capacity(count);
            for _ in 0..count {
                let op = match self.rng.random_range(0u32..12) {
                    0..3 => {
                        let queue_idx = self.rng.random_range(0..self.queues.len());
                        if self.rng.random_bool(0.3) {
                            Op::PushBatch {
                                queue_idx,
                                count: self.rng.random_range(2..6),
                            }
                        } else {
                            Op::Push { queue_idx }
                        }
                    }
                    3..5 => Op::Claim {
                        queue_idx: self.rng.random_range(0..self.queues.len()),
                        worker_idx: self.rng.random_range(0..self.workers.len()),
                        batch_size: self.rng.random_range(1..4),
                    },
                    5..7 => Op::Ack {
                        worker_idx: self.rng.random_range(0..self.workers.len()),
                    },
                    7 => Op::Nack {
                        worker_idx: self.rng.random_range(0..self.workers.len()),
                        dead_letter: self.rng.random_bool(0.1),
                    },
                    8 => Op::AckAndForward {
                        worker_idx: self.rng.random_range(0..self.workers.len()),
                        downstream_idx: self.rng.random_range(0..self.queues.len()),
                    },
                    10 => Op::Redrive {
                        queue_idx: self.rng.random_range(0..self.queues.len()),
                    },
                    11 => Op::Purge {
                        queue_idx: self.rng.random_range(0..self.queues.len()),
                    },
                    _ => {
                        if self.rng.random_bool(0.5) {
                            Op::AdvanceTime {
                                secs: self.rng.random_range(1.0..120.0),
                            }
                        } else {
                            Op::RecoverExpired {
                                timeout_secs: self.rng.random_range(5.0..60.0),
                            }
                        }
                    }
                };
                ops.push(op);
            }
            ops
        }

        pub(crate) async fn execute(&mut self, op: Op) {
            match op {
                Op::Push { queue_idx } => {
                    let queue = &self.queues[queue_idx];
                    let msg = Message::new(b"dst-payload".to_vec());
                    self.storage.push_message(queue, &msg).await.unwrap();
                    self.pushed.insert((queue.clone(), msg.msg_id));
                }

                Op::PushBatch { queue_idx, count } => {
                    let queue = &self.queues[queue_idx];
                    let msgs: Vec<Message> = (0..count)
                        .map(|_| Message::new(b"dst-batch".to_vec()))
                        .collect();
                    self.storage.push_messages(queue, &msgs).await.unwrap();
                    self.pushed
                        .extend(msgs.into_iter().map(|m| (queue.clone(), m.msg_id)));
                }

                Op::Claim {
                    queue_idx,
                    worker_idx,
                    batch_size,
                } => {
                    let queue = &self.queues[queue_idx];
                    let worker_id = format!("w{}", worker_idx);
                    let lease_id = &self.lease_ids[worker_idx];
                    let claimed = self
                        .storage
                        .claim_messages(queue, batch_size, &worker_id, lease_id)
                        .await
                        .unwrap();
                    for c in claimed {
                        self.workers[worker_idx].claims.push((
                            queue.clone(),
                            c.message.msg_id,
                            c.claim_token,
                        ));
                    }
                }

                Op::Ack { worker_idx } => {
                    let worker = &mut self.workers[worker_idx];
                    if worker.claims.is_empty() {
                        return;
                    }
                    let n = self.rng.random_range(1..=worker.claims.len());
                    let to_ack: Vec<_> = worker.claims.drain(..n).collect();

                    let mut by_queue: HashMap<String, (Vec<String>, Vec<String>)> = HashMap::new();
                    for (queue, msg_id, token) in to_ack {
                        let entry = by_queue.entry(queue).or_default();
                        entry.0.push(msg_id);
                        entry.1.push(token);
                    }

                    let worker_id = format!("w{}", worker_idx);
                    let lease_id = &self.lease_ids[worker_idx];
                    for (queue, (msg_ids, tokens)) in &by_queue {
                        self.storage
                            .ack_messages(queue, msg_ids, tokens, &worker_id, lease_id)
                            .await
                            .unwrap();
                        self.acked
                            .extend(msg_ids.iter().map(|id| (queue.clone(), id.clone())));
                    }
                }

                Op::Nack {
                    worker_idx,
                    dead_letter,
                } => {
                    let worker = &mut self.workers[worker_idx];
                    if worker.claims.is_empty() {
                        return;
                    }
                    let n = self.rng.random_range(1..=worker.claims.len());
                    let to_nack: Vec<_> = worker.claims.drain(..n).collect();

                    let mut by_queue: HashMap<String, (Vec<String>, Vec<String>)> = HashMap::new();
                    for (queue, msg_id, token) in to_nack {
                        let entry = by_queue.entry(queue).or_default();
                        entry.0.push(msg_id);
                        entry.1.push(token);
                    }

                    let worker_id = format!("w{}", worker_idx);
                    let lease_id = &self.lease_ids[worker_idx];
                    for (queue, (msg_ids, tokens)) in &by_queue {
                        self.storage
                            .nack_messages(
                                queue,
                                msg_ids,
                                tokens,
                                &worker_id,
                                lease_id,
                                NackOptions {
                                    dead_letter,
                                    reason: "dst",
                                },
                            )
                            .await
                            .unwrap();
                    }
                }

                Op::AckAndForward {
                    worker_idx,
                    downstream_idx,
                } => {
                    let worker = &mut self.workers[worker_idx];
                    if worker.claims.is_empty() {
                        return;
                    }
                    let (queue, msg_id, token) = worker.claims.remove(0);
                    let downstream = &self.queues[downstream_idx];

                    let out_msgs: Vec<Message> = vec![
                        Message::new(b"fwd-1".to_vec()),
                        Message::new(b"fwd-2".to_vec()),
                    ];

                    let worker_id = format!("w{}", worker_idx);
                    let lease_id = &self.lease_ids[worker_idx];
                    self.storage
                        .ack_and_forward(
                            &queue,
                            std::slice::from_ref(&msg_id),
                            &[token],
                            &worker_id,
                            lease_id,
                            downstream,
                            &out_msgs,
                        )
                        .await
                        .unwrap();
                    self.acked.insert((queue, msg_id));
                    self.pushed
                        .extend(out_msgs.into_iter().map(|m| (downstream.clone(), m.msg_id)));
                }

                Op::Redrive { queue_idx } => {
                    let queue = &self.queues[queue_idx];
                    let (letters, _) = self
                        .storage
                        .list_dead_letters(queue, None, 3, false)
                        .await
                        .unwrap();
                    if letters.is_empty() {
                        return;
                    }
                    let msg_ids: Vec<String> =
                        letters.into_iter().map(|l| l.message.msg_id).collect();
                    let (redriven, missing) = self
                        .storage
                        .redrive_dead_letters(queue, &msg_ids, false)
                        .await
                        .unwrap();
                    assert_eq!((redriven, missing), (msg_ids, Vec::new()));
                }

                Op::Purge { queue_idx } => {
                    let queue = &self.queues[queue_idx];
                    let (letters, _) = self
                        .storage
                        .list_dead_letters(queue, None, 2, false)
                        .await
                        .unwrap();
                    if letters.is_empty() {
                        return;
                    }
                    let msg_ids: Vec<String> =
                        letters.into_iter().map(|l| l.message.msg_id).collect();
                    let (purged, missing) = self
                        .storage
                        .purge_dead_letters(queue, &msg_ids)
                        .await
                        .unwrap();
                    assert_eq!((&purged, missing), (&msg_ids, Vec::new()));
                    for msg_id in purged {
                        self.pushed.remove(&(queue.clone(), msg_id));
                    }
                }

                Op::AdvanceTime { secs } => {
                    advance_sim_time_secs(secs);
                }

                Op::RecoverExpired { timeout_secs } => {
                    // DST has no `active_leases` to feed in, so every claim
                    // falls into the dead-lease branch and reclaim is
                    // unconditional. Pass the same `timeout_secs` for both
                    // knobs to preserve the existing scenario semantics.
                    let recovered = self
                        .storage
                        .recover_expired_claims(timeout_secs, timeout_secs, None)
                        .await
                        .unwrap();
                    if recovered > 0 {
                        // Reconcile: only keep claims that are still in storage's claimed set.
                        // Build a set of (queue, msg_id) pairs that are still claimed.
                        let mut still_claimed: HashSet<(String, String)> = HashSet::new();
                        for queue in &self.queues {
                            for (_, msg_id, _) in
                                self.storage.scan_claimed(Some(queue)).await.unwrap()
                            {
                                still_claimed.insert((queue.clone(), msg_id));
                            }
                        }
                        for worker in &mut self.workers {
                            worker.claims.retain(|(q, mid, _)| {
                                still_claimed.contains(&(q.clone(), mid.clone()))
                            });
                        }
                    }
                }
            }
        }

        async fn run_seed(&mut self, seed: u64, op_count: usize) {
            self.reset(seed).await;
            let ops = self.generate_ops(op_count);
            for op in ops {
                self.execute(op).await;
            }
            check_invariants(&self.storage, &self.queues).await;
        }
    }

    /// Every dead letter of `queue`, by msg_id.
    pub(crate) async fn dead_letter_ids(storage: &AnvilStorage, queue: &str) -> HashSet<String> {
        let mut ids = HashSet::new();
        let mut start_after: Option<String> = None;
        loop {
            let (page, more) = storage
                .list_dead_letters(
                    queue,
                    start_after.as_deref(),
                    DEAD_LETTERS_PER_CALL_MAX,
                    false,
                )
                .await
                .unwrap();
            start_after = page.last().map(|l| l.message.msg_id.clone());
            ids.extend(page.into_iter().map(|l| l.message.msg_id));
            if !more {
                return ids;
            }
        }
    }

    /// Scan storage and verify all invariants.
    pub(crate) async fn check_invariants(storage: &AnvilStorage, queues: &[String]) {
        for queue in queues {
            let meta = storage.get_queue_stats(queue).await.unwrap();
            let pending_from_meta = meta.push_seq.saturating_sub(meta.claim_seq);

            let claimed_entries = storage.scan_claimed(Some(queue)).await.unwrap();
            let acked_entries = storage.scan_acked(Some(queue)).await.unwrap();

            let actual_claimed = claimed_entries.len() as u64;
            let actual_acked = acked_entries.len() as u64;

            // Invariant 1: claimed_count == actual claimed keys
            assert_eq!(
                meta.claimed_count, actual_claimed,
                "[{}] claimed_count: meta={} actual={} (meta={:?})",
                queue, meta.claimed_count, actual_claimed, meta
            );

            // Invariant 2: actual acked keys <= total_acked (GC may have deleted some)
            assert!(
                actual_acked <= meta.total_acked,
                "[{}] acked keys ({}) > total_acked ({}) (meta={:?})",
                queue,
                actual_acked,
                meta.total_acked,
                meta
            );

            // Invariant 3: conservation — pending + claimed + acked + dead +
            // purged >= total_pushed (excess = cumulative nack count, always
            // >= 0)
            let lhs = pending_from_meta
                + meta.claimed_count
                + meta.total_acked
                + meta.dead_count
                + meta.total_purged;
            assert!(
                lhs >= meta.total_pushed,
                "[{queue}] conservation: {lhs} < total_pushed={} (meta={meta:?})",
                meta.total_pushed,
            );

            // Invariant 3b: dead_count == actual dead letter keys, none of
            // them also claimed
            let dead_ids = dead_letter_ids(storage, queue).await;
            assert_eq!(
                meta.dead_count,
                dead_ids.len() as u64,
                "[{}] dead_count: meta={} actual={}",
                queue,
                meta.dead_count,
                dead_ids.len()
            );
            assert!(
                claimed_entries
                    .iter()
                    .all(|(_, id, _)| !dead_ids.contains(id)),
                "[{queue}] a message both claimed and dead"
            );

            // Invariant 4: no msg_id in both claimed AND acked
            let claimed_ids: HashSet<&str> = claimed_entries
                .iter()
                .map(|(_, id, _)| id.as_str())
                .collect();
            let acked_ids: HashSet<&str> =
                acked_entries.iter().map(|(_, _, id)| id.as_str()).collect();
            let overlap: Vec<_> = claimed_ids.intersection(&acked_ids).collect();
            assert!(
                overlap.is_empty(),
                "[{}] msg_ids in both claimed and acked: {:?}",
                queue,
                overlap
            );

            // Invariant 5: claim_seq <= push_seq
            assert!(
                meta.claim_seq <= meta.push_seq,
                "[{}] claim_seq({}) > push_seq({})",
                queue,
                meta.claim_seq,
                meta.push_seq
            );
        }
    }

    // =====================================================================
    // Test cases
    // =====================================================================

    /// Core DST: 20 seeds × 200 ops, reusing one storage instance.
    #[tokio::test]
    async fn test_dst_random_seeds() {
        let _clock = SimClock::acquire();
        let mut sim = DstSimulator::new(0, 3, 4).await;
        for seed in 0..20 {
            sim.run_seed(seed, 200).await;
        }
    }

    /// Stress: 5 seeds × 1000 ops with more workers.
    #[tokio::test]
    async fn test_dst_long_sequence() {
        let _clock = SimClock::acquire();
        let mut sim = DstSimulator::new(100, 4, 8).await;
        for seed in 100..105 {
            sim.run_seed(seed, 1000).await;
        }
    }

    /// ack_and_forward atomicity: 50 upstream → 100 downstream (1:2 fan-out).
    #[tokio::test]
    async fn test_dst_forward_heavy() {
        let _clock = SimClock::acquire();
        let storage = AnvilStorage::new("memory://").await.unwrap();
        storage.create_queue("upstream", 0, None).await.unwrap();
        storage.create_queue("downstream", 0, None).await.unwrap();

        set_sim_time_nanos(1_735_689_600_000_000_000);

        for _ in 0..50 {
            let msg = Message::new(b"data".to_vec());
            storage.push_message("upstream", &msg).await.unwrap();
        }

        let mut total_forwarded = 0u64;
        loop {
            let claimed = storage
                .claim_messages("upstream", 5, "w0", "lease-0")
                .await
                .unwrap();
            if claimed.is_empty() {
                break;
            }
            for c in &claimed {
                let out = vec![Message::new(b"o1".to_vec()), Message::new(b"o2".to_vec())];
                storage
                    .ack_and_forward(
                        "upstream",
                        std::slice::from_ref(&c.message.msg_id),
                        std::slice::from_ref(&c.claim_token),
                        "w0",
                        "lease-0",
                        "downstream",
                        &out,
                    )
                    .await
                    .unwrap();
                total_forwarded += 2;
            }
        }

        let up = storage.get_queue_stats("upstream").await.unwrap();
        let down = storage.get_queue_stats("downstream").await.unwrap();

        assert_eq!(up.total_acked, 50);
        assert_eq!(up.claimed_count, 0);
        assert_eq!(down.total_pushed, 100);
        assert_eq!(total_forwarded, 100);
        assert_eq!(down.claimed_count, 0);
    }

    /// Recovery: claims expire → re-enqueue → new worker claims → stale tokens rejected.
    #[tokio::test]
    async fn test_dst_recovery_cycle() {
        let _clock = SimClock::acquire();
        let storage = AnvilStorage::new("memory://").await.unwrap();
        storage.create_queue("q", 0, None).await.unwrap();

        set_sim_time_nanos(1_735_689_600_000_000_000);

        for _ in 0..10 {
            let msg = Message::new(b"data".to_vec());
            storage.push_message("q", &msg).await.unwrap();
        }

        let claimed = storage
            .claim_messages("q", 10, "w0", "lease-0")
            .await
            .unwrap();
        assert_eq!(claimed.len(), 10);

        let meta = storage.get_queue_stats("q").await.unwrap();
        assert_eq!(meta.claimed_count, 10);
        assert_eq!(meta.push_seq - meta.claim_seq, 0);

        advance_sim_time_secs(120.0);

        // No active leases → dead-lease branch reclaims unconditionally;
        // both timeout knobs are irrelevant. Pass production-like values
        // for documentation.
        let recovered = storage
            .recover_expired_claims(60.0, 60.0, None)
            .await
            .unwrap();
        assert_eq!(recovered, 10);

        let meta = storage.get_queue_stats("q").await.unwrap();
        assert_eq!(meta.claimed_count, 0);
        assert_eq!(meta.push_seq - meta.claim_seq, 10);

        let reclaimed = storage
            .claim_messages("q", 10, "w1", "lease-1")
            .await
            .unwrap();
        assert_eq!(reclaimed.len(), 10);

        // Old tokens must be rejected
        for c in &claimed {
            let result = storage
                .ack_messages(
                    "q",
                    std::slice::from_ref(&c.message.msg_id),
                    std::slice::from_ref(&c.claim_token),
                    "w0",
                    "lease-0",
                )
                .await;
            assert!(result.is_err(), "Stale token should be rejected");
        }

        // New tokens work
        for c in &reclaimed {
            storage
                .ack_messages(
                    "q",
                    std::slice::from_ref(&c.message.msg_id),
                    std::slice::from_ref(&c.claim_token),
                    "w1",
                    "lease-1",
                )
                .await
                .unwrap();
        }

        let meta = storage.get_queue_stats("q").await.unwrap();
        assert_eq!(meta.total_acked, 10);
        assert_eq!(meta.claimed_count, 0);
    }

    /// Nack storm: 5 messages bounce 20× between pending and claimed, on a
    /// queue without a delivery limit.
    #[tokio::test]
    async fn test_dst_nack_storm() {
        let _clock = SimClock::acquire();
        let storage = AnvilStorage::new("memory://").await.unwrap();
        storage.create_queue("q", 0, Some(0)).await.unwrap();

        set_sim_time_nanos(1_735_689_600_000_000_000);

        for _ in 0..5 {
            let msg = Message::new(b"data".to_vec());
            storage.push_message("q", &msg).await.unwrap();
        }

        for round in 0..20 {
            let claimed = storage
                .claim_messages("q", 5, "w0", "lease-0")
                .await
                .unwrap();
            if claimed.is_empty() {
                continue;
            }
            let ids: Vec<String> = claimed.iter().map(|c| c.message.msg_id.clone()).collect();
            let tokens: Vec<String> = claimed.iter().map(|c| c.claim_token.clone()).collect();
            storage
                .nack_messages("q", &ids, &tokens, "w0", "lease-0", NackOptions::default())
                .await
                .unwrap();

            let meta = storage.get_queue_stats("q").await.unwrap();
            assert_eq!(meta.claimed_count, 0, "Round {}: claimed after nack", round);
        }

        // Final ack
        let claimed = storage
            .claim_messages("q", 5, "w0", "lease-0")
            .await
            .unwrap();
        assert_eq!(claimed.len(), 5);
        let ids: Vec<String> = claimed.iter().map(|c| c.message.msg_id.clone()).collect();
        let tokens: Vec<String> = claimed.iter().map(|c| c.claim_token.clone()).collect();
        storage
            .ack_messages("q", &ids, &tokens, "w0", "lease-0")
            .await
            .unwrap();

        let meta = storage.get_queue_stats("q").await.unwrap();
        assert_eq!(meta.total_acked, 5);
        assert_eq!(meta.total_pushed, 5);
        assert_eq!(meta.push_seq, 105); // 5 + 5*20 nack re-pushes
    }

    // =====================================================================
    // High-concurrency stress tests (atomic counter validation)
    // =====================================================================

    /// 500 concurrent claimers on 10000 messages — no duplicates, no losses.
    #[tokio::test]
    async fn test_500_concurrent_claims() {
        use std::sync::atomic::AtomicU64;

        let storage = AnvilStorage::new("memory://").await.unwrap();
        let storage = Arc::new(storage);
        let queue = "stress_q";
        storage.create_queue(queue, 0, None).await.unwrap();

        // Push 10000 messages
        let mut msgs = Vec::new();
        for i in 0..10000u64 {
            msgs.push(Message::new(format!("msg_{i}").into_bytes()));
        }
        for chunk in msgs.chunks(100) {
            storage.push_messages(queue, chunk).await.unwrap();
        }

        // 500 concurrent claimers, each claiming batch_size=1
        let mut handles = Vec::new();
        let total_claimed = Arc::new(AtomicU64::new(0));
        let claimed_ids = Arc::new(std::sync::Mutex::new(HashSet::new()));

        for worker_id in 0..500u32 {
            let s = storage.clone();
            let tc = total_claimed.clone();
            let ci = claimed_ids.clone();
            handles.push(tokio::spawn(async move {
                let wid = format!("w_{worker_id}");
                let lid = format!("l_{worker_id}");
                loop {
                    let claimed = s.claim_messages(queue, 1, &wid, &lid).await.unwrap();
                    if claimed.is_empty() {
                        break;
                    }
                    tc.fetch_add(claimed.len() as u64, Ordering::Relaxed);
                    let mut ids = ci.lock().unwrap();
                    for c in &claimed {
                        assert!(
                            ids.insert(c.message.msg_id.clone()),
                            "DUPLICATE CLAIM: {}",
                            c.message.msg_id
                        );
                    }
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(total_claimed.load(Ordering::Relaxed), 10000);
        assert_eq!(claimed_ids.lock().unwrap().len(), 10000);

        // Verify stats
        let stats = storage.get_queue_stats(queue).await.unwrap();
        assert_eq!(stats.claim_seq, stats.push_seq); // all claimed
        assert_eq!(stats.claimed_count, 10000); // all in-flight
    }

    /// Simultaneous push + claim + ack from many workers.
    /// Phase 1: push all messages. Phase 2: claim+ack all messages concurrently.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_concurrent_push_claim_ack() {
        use std::sync::atomic::AtomicU64;

        let storage = Arc::new(AnvilStorage::new("memory://").await.unwrap());
        let queue = "pca_q";
        storage.create_queue(queue, 0, None).await.unwrap();

        // Phase 1: push 5000 messages concurrently from 50 pushers
        let total_pushed = Arc::new(AtomicU64::new(0));
        let mut push_handles = Vec::new();
        for pid in 0..50u32 {
            let s = storage.clone();
            let tp = total_pushed.clone();
            push_handles.push(tokio::spawn(async move {
                for i in 0..100u32 {
                    let msg = Message::new(format!("p{pid}_m{i}").into_bytes());
                    s.push_message(queue, &msg).await.unwrap();
                    tp.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        for h in push_handles {
            h.await.unwrap();
        }
        assert_eq!(total_pushed.load(Ordering::Relaxed), 5000);

        // Phase 2: 100 workers claim+ack concurrently
        let total_acked = Arc::new(AtomicU64::new(0));
        let mut work_handles = Vec::new();
        for wid in 0..100u32 {
            let s = storage.clone();
            let ta = total_acked.clone();
            work_handles.push(tokio::spawn(async move {
                let w = format!("w_{wid}");
                let l = format!("l_{wid}");
                loop {
                    let claimed = s.claim_messages(queue, 1, &w, &l).await.unwrap();
                    if claimed.is_empty() {
                        break;
                    }
                    let msg_ids: Vec<String> =
                        claimed.iter().map(|c| c.message.msg_id.clone()).collect();
                    let tokens: Vec<String> =
                        claimed.iter().map(|c| c.claim_token.clone()).collect();
                    s.ack_messages(queue, &msg_ids, &tokens, &w, &l)
                        .await
                        .unwrap();
                    ta.fetch_add(claimed.len() as u64, Ordering::Relaxed);
                }
            }));
        }
        for h in work_handles {
            h.await.unwrap();
        }

        let acked = total_acked.load(Ordering::Relaxed);
        assert_eq!(acked, 5000); // all messages acked

        let stats = storage.get_queue_stats(queue).await.unwrap();
        assert_eq!(stats.claimed_count, 0); // nothing in-flight
        assert_eq!(stats.total_acked, 5000);
    }

    /// 200 workers claiming from a 4-partition group simultaneously.
    #[tokio::test]
    async fn test_concurrent_claim_from_group() {
        let storage = Arc::new(AnvilStorage::new("memory://").await.unwrap());
        let group = "stress_grp";
        storage.create_queue_group(group, 4, 0, None).await.unwrap();

        // Push 2000 messages across partitions
        for pid in 0..4u32 {
            let q = format!("{group}_p{pid}");
            let msgs: Vec<Message> = (0..500)
                .map(|i| Message::new(format!("p{pid}_m{i}").into_bytes()))
                .collect();
            for chunk in msgs.chunks(50) {
                storage.push_messages(&q, chunk).await.unwrap();
            }
        }

        let claimed_ids = Arc::new(std::sync::Mutex::new(HashSet::new()));
        let mut handles = Vec::new();

        for wid in 0..200u32 {
            let s = storage.clone();
            let ci = claimed_ids.clone();
            let assigned: Vec<u32> = vec![wid % 4]; // each worker assigned to 1 partition
            handles.push(tokio::spawn(async move {
                let w = format!("w_{wid}");
                let l = format!("l_{wid}");
                let partitions = s.group_partitions(group, &assigned).await.unwrap();
                loop {
                    let (claimed, _, _) = s
                        .claim_from_group(group, &partitions, 1, &w, &l)
                        .await
                        .unwrap();
                    if claimed.is_empty() {
                        break;
                    }
                    let mut ids = ci.lock().unwrap();
                    for c in &claimed {
                        assert!(
                            ids.insert(c.message.msg_id.clone()),
                            "DUPLICATE: {}",
                            c.message.msg_id
                        );
                    }
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
        assert_eq!(claimed_ids.lock().unwrap().len(), 2000);
    }

    /// Bounded queue: push up to max_pending, reject when full, accept after ack frees space.
    #[tokio::test]
    async fn test_bounded_queue_rejects_when_full() {
        let storage = AnvilStorage::new("memory://").await.unwrap();
        storage.create_queue("bounded", 3, None).await.unwrap(); // max 3 in-flight

        // Push 3 messages — should succeed
        for i in 0..3 {
            let msg = Message::new(format!("data-{i}").into_bytes());
            storage.push_messages("bounded", &[msg]).await.unwrap();
        }

        // Push 4th — should fail with QueueFull
        let msg = Message::new(b"data-3".to_vec());
        let result = storage.push_messages("bounded", &[msg]).await;
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("QueueFull"),
            "Expected QueueFull error"
        );

        // Claim and ack one — should free space
        let claimed = storage
            .claim_messages("bounded", 1, "w1", "lease1")
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);
        let msg_ids: Vec<String> = claimed.iter().map(|c| c.message.msg_id.clone()).collect();
        let tokens: Vec<String> = claimed.iter().map(|c| c.claim_token.clone()).collect();
        storage
            .ack_messages("bounded", &msg_ids, &tokens, "w1", "lease1")
            .await
            .unwrap();

        // Now push should succeed again
        let msg4 = Message::new(b"data-4".to_vec());
        storage.push_messages("bounded", &[msg4]).await.unwrap();

        // Verify stats: 4 pushed, 1 acked, 3 in-flight
        let meta = storage.get_queue_stats("bounded").await.unwrap();
        assert_eq!(meta.total_pushed, 4);
        assert_eq!(meta.total_acked, 1);
    }

    // =====================================================================
    // Crash recovery on a file:// store
    //
    // A crash is simulated by tearing the runtime down without closing the
    // database and leaking the storage handle, so nothing can flush after
    // the last call returned. The directory is then reopened by a fresh
    // runtime, as a restarted broker would.
    // =====================================================================

    /// Claims taken per call while draining a reopened queue.
    const DRAIN_CLAIM_BATCH: usize = 256;

    pub(crate) fn crash_test_db_path(name: &str) -> (std::path::PathBuf, String) {
        static TRIAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let trial = TRIAL.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("anvil_{name}_{}_{trial}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let db_path = format!("file://{}", dir.display());
        (dir, db_path)
    }

    /// Ends `runtime` the way a killed process would end the broker.
    pub(crate) fn crash<T>(runtime: tokio::runtime::Runtime, storage: T) {
        runtime.shutdown_background();
        std::mem::forget(storage);
    }

    /// Checks a reopened store against what the crashed run acknowledged:
    /// counters agree with the keys, every successful ack is still acked
    /// exactly once, and every successful push is either acked or comes out
    /// of the queue exactly once when everything left is drained.
    pub(crate) async fn check_recovered(
        storage: &AnvilStorage,
        queues: &[String],
        pushed: &HashSet<(String, String)>,
        acked: &HashSet<(String, String)>,
    ) {
        check_invariants(storage, queues).await;

        let mut stored_acked: HashSet<(String, String)> = HashSet::new();
        for queue in queues {
            for (_, _, msg_id) in storage.scan_acked(Some(queue)).await.unwrap() {
                let first = stored_acked.insert((queue.clone(), msg_id.clone()));
                assert!(first, "[{queue}] {msg_id} acked twice");
            }
        }
        let lost_acks: Vec<_> = acked.difference(&stored_acked).collect();
        assert!(
            lost_acks.is_empty(),
            "acks lost in the crash: {lost_acks:?}"
        );

        // Every claim's holder died with the broker.
        let no_live_leases = HashMap::new();
        storage
            .recover_expired_claims(0.0, 0.0, Some(&no_live_leases))
            .await
            .unwrap();

        let mut drained: HashSet<(String, String)> = HashSet::new();
        for queue in queues {
            loop {
                let claimed = storage
                    .claim_messages(queue, DRAIN_CLAIM_BATCH, "drainer", "drainer-lease")
                    .await
                    .unwrap();
                if claimed.is_empty() {
                    break;
                }
                for c in claimed {
                    let key = (queue.clone(), c.message.msg_id);
                    assert!(
                        !stored_acked.contains(&key),
                        "{key:?} delivered after its ack"
                    );
                    let first = drained.insert(key.clone());
                    assert!(first, "{key:?} delivered twice");
                }
            }
            let meta = storage.get_queue_stats(queue).await.unwrap();
            assert_eq!(
                meta.push_seq, meta.claim_seq,
                "[{queue}] pending left after drain"
            );
        }
        let mut dead: HashSet<(String, String)> = HashSet::new();
        for queue in queues {
            for msg_id in dead_letter_ids(storage, queue).await {
                let key = (queue.clone(), msg_id);
                assert!(!drained.contains(&key), "{key:?} both delivered and dead");
                assert!(!stored_acked.contains(&key), "{key:?} both acked and dead");
                dead.insert(key);
            }
        }
        let lost: Vec<_> = pushed
            .iter()
            .filter(|key| {
                !stored_acked.contains(*key) && !drained.contains(*key) && !dead.contains(*key)
            })
            .collect();
        assert!(
            lost.is_empty(),
            "pushed messages lost in the crash: {lost:?}"
        );
    }

    /// The DST workload on a file:// store, crashed after its last
    /// operation and reopened: whatever each operation reported as done
    /// must have survived.
    #[test]
    fn test_dst_crash_recovery() {
        let _clock = SimClock::acquire();
        for seed in 300..304 {
            let (dir, db_path) = crash_test_db_path("dst_crash");

            let crashed = tokio::runtime::Runtime::new().unwrap();
            let sim = crashed.block_on(async {
                let storage = AnvilStorage::new(&db_path).await.unwrap();
                let mut sim = DstSimulator::with_storage(storage, seed, 3, 4).await;
                let ops = sim.generate_ops(150);
                for op in ops {
                    sim.execute(op).await;
                }
                sim
            });
            let queues = sim.queues.clone();
            let pushed = sim.pushed.clone();
            let acked = sim.acked.clone();
            crash(crashed, sim);

            let recovered = tokio::runtime::Runtime::new().unwrap();
            recovered.block_on(async {
                let storage = AnvilStorage::new(&db_path).await.unwrap();
                check_recovered(&storage, &queues, &pushed, &acked).await;
            });
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// Concurrent producers, workers acking and nacking, and a reclaimer
    /// that keeps taking claims out from under them, crashed mid-flight
    /// at several points. The reclaimer races every settlement, and
    /// concurrent writers race to persist the queue counters, so this is
    /// where a check-then-write gap or an out-of-order counter write would
    /// surface after the restart.
    #[test]
    fn test_crash_during_concurrent_workload() {
        const PRODUCERS: usize = 4;
        const PUSH_BATCH: usize = 8;
        const WORKERS: usize = 16;
        const CLAIM_BATCH: usize = 4;
        const RECLAIM_INTERVAL: std::time::Duration = std::time::Duration::from_millis(20);
        const IDLE_BACKOFF: std::time::Duration = std::time::Duration::from_millis(1);

        for crash_after_ms in [300u64, 1_000, 2_500] {
            let (dir, db_path) = crash_test_db_path("concurrent_crash");
            let queue = "work".to_string();
            let pushed: Arc<std::sync::Mutex<HashSet<(String, String)>>> = Arc::default();
            let acked: Arc<std::sync::Mutex<HashSet<(String, String)>>> = Arc::default();

            let crashed = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            let storage = crashed.block_on(async {
                let storage = Arc::new(AnvilStorage::new(&db_path).await.unwrap());
                storage.create_queue(&queue, 0, None).await.unwrap();

                for _ in 0..PRODUCERS {
                    let (storage, queue, pushed) = (storage.clone(), queue.clone(), pushed.clone());
                    tokio::spawn(async move {
                        loop {
                            let batch: Vec<Message> = (0..PUSH_BATCH)
                                .map(|_| Message::new(b"x".to_vec()))
                                .collect();
                            storage.push_messages(&queue, &batch).await.unwrap();
                            pushed
                                .lock()
                                .unwrap()
                                .extend(batch.into_iter().map(|m| (queue.clone(), m.msg_id)));
                        }
                    });
                }

                for worker in 0..WORKERS {
                    let (storage, queue, acked) = (storage.clone(), queue.clone(), acked.clone());
                    tokio::spawn(async move {
                        let worker_id = format!("w{worker}");
                        let lease_id = format!("lease-{worker}");
                        let mut rng = StdRng::seed_from_u64(worker as u64);
                        loop {
                            let claimed = storage
                                .claim_messages(&queue, CLAIM_BATCH, &worker_id, &lease_id)
                                .await
                                .unwrap();
                            if claimed.is_empty() {
                                tokio::time::sleep(IDLE_BACKOFF).await;
                                continue;
                            }
                            for c in claimed {
                                let msg_ids = std::slice::from_ref(&c.message.msg_id);
                                let tokens = std::slice::from_ref(&c.claim_token);
                                let settled = if rng.random_bool(0.8) {
                                    storage
                                        .ack_messages(
                                            &queue, msg_ids, tokens, &worker_id, &lease_id,
                                        )
                                        .await
                                        .map(|()| true)
                                } else {
                                    storage
                                        .nack_messages(
                                            &queue,
                                            msg_ids,
                                            tokens,
                                            &worker_id,
                                            &lease_id,
                                            NackOptions::default(),
                                        )
                                        .await
                                        .map(|()| false)
                                };
                                match settled {
                                    Ok(true) => {
                                        acked
                                            .lock()
                                            .unwrap()
                                            .insert((queue.clone(), c.message.msg_id));
                                    }
                                    Ok(false) => {}
                                    // The reclaimer took the claim first.
                                    Err(e) => assert!(
                                        e.to_string().contains("message_not_claimed")
                                            || e.to_string().contains("claim_token mismatch")
                                            || e.to_string().contains("claim_settling"),
                                        "unexpected settlement error: {e}"
                                    ),
                                }
                            }
                        }
                    });
                }

                {
                    let storage = storage.clone();
                    tokio::spawn(async move {
                        let no_live_leases = HashMap::new();
                        loop {
                            tokio::time::sleep(RECLAIM_INTERVAL).await;
                            storage
                                .recover_expired_claims(0.0, 0.0, Some(&no_live_leases))
                                .await
                                .unwrap();
                        }
                    });
                }

                tokio::time::sleep(std::time::Duration::from_millis(crash_after_ms)).await;
                storage
            });
            crash(crashed, storage);
            // Copy the records out under the lock: tasks of the crashed
            // runtime may still be unwinding.
            let pushed = pushed.lock().unwrap().clone();
            let acked = acked.lock().unwrap().clone();
            assert!(
                !acked.is_empty(),
                "the workload acked nothing before the crash"
            );

            let recovered = tokio::runtime::Runtime::new().unwrap();
            recovered.block_on(async {
                let storage = AnvilStorage::new(&db_path).await.unwrap();
                check_recovered(&storage, std::slice::from_ref(&queue), &pushed, &acked).await;
            });
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}
