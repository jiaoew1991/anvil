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

// Garbage collection of acked entries over many passes, on a file:// store
// that is crashed and reopened between and during passes: GC must retire
// exactly the entries past retention, touch nothing else, and leave every
// counter as it found it.

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Instant;

    use crate::dst::tests::{check_invariants, crash, crash_test_db_path};
    use crate::storage::tests::split_claims;
    use crate::storage::{AnvilStorage, GC_BATCH_KEYS_MAX};
    use crate::types::{now_nanos, set_sim_time_nanos, ClaimedMessage, Message, SimClock};

    const T0_NANOS: u64 = 1_900_000_000_000_000_000;
    const SEC_NANOS: u64 = 1_000_000_000;
    /// Messages claimed and never settled, and messages never claimed: GC
    /// must leave both alone.
    const HELD: usize = 8;
    const PENDING: usize = 8;

    /// An acked entry, ordered as its key orders it: (queue, ack timestamp,
    /// msg_id).
    type AckedEntry = (String, u64, String);

    /// Pushes, claims and acks `count` messages at the current sim time and
    /// returns their acked entries.
    async fn ack_new(storage: &AnvilStorage, queue: &str, count: usize) -> Vec<AckedEntry> {
        let messages: Vec<Message> = (0..count).map(|_| Message::new(b"done".to_vec())).collect();
        storage.push_messages(queue, &messages).await.unwrap();
        let claimed = storage
            .claim_messages(queue, count, "worker", "lease")
            .await
            .unwrap();
        assert_eq!(claimed.len(), count);
        let (ids, tokens) = split_claims(&claimed);
        storage
            .ack_messages(queue, &ids, &tokens, "worker", "lease")
            .await
            .unwrap();
        let acked_at = now_nanos();
        ids.into_iter()
            .map(|id| (queue.to_string(), acked_at, id))
            .collect()
    }

    /// Work in one queue that GC must never delete. A claim is not awaited
    /// for durability, so the held claims survive a crash only once a later
    /// write is durable. Acking new messages in the same queue would claim
    /// the pending ones first.
    struct Live {
        held: Vec<ClaimedMessage>,
        pending: Vec<String>,
    }

    async fn add_live(storage: &AnvilStorage, queue: &str) -> Live {
        let held: Vec<Message> = (0..HELD).map(|_| Message::new(b"held".to_vec())).collect();
        storage.push_messages(queue, &held).await.unwrap();
        let held = storage
            .claim_messages(queue, HELD, "holder", "holder-lease")
            .await
            .unwrap();
        assert_eq!(held.len(), HELD);
        let pending: Vec<Message> = (0..PENDING)
            .map(|_| Message::new(b"pending".to_vec()))
            .collect();
        storage.push_messages(queue, &pending).await.unwrap();
        Live {
            held,
            pending: pending.into_iter().map(|m| m.msg_id).collect(),
        }
    }

    async fn stored_acked(storage: &AnvilStorage) -> BTreeSet<AckedEntry> {
        storage
            .scan_acked(None)
            .await
            .unwrap()
            .into_iter()
            .collect()
    }

    /// What GC must not change in a queue: (pending, claimed, total pushed,
    /// total acked). The cursors themselves may restart on reopen.
    async fn counts(storage: &AnvilStorage, queues: &[String]) -> Vec<(u64, u64, u64, u64)> {
        let mut counts = Vec::new();
        for queue in queues {
            let meta = storage.get_meta(queue).await.unwrap();
            counts.push((
                meta.push_seq - meta.claim_seq,
                meta.claimed_count,
                meta.total_pushed,
                meta.total_acked,
            ));
        }
        counts
    }

    async fn check_untouched(
        storage: &AnvilStorage,
        queues: &[String],
        live: &HashMap<String, Live>,
        counts_before: &[(u64, u64, u64, u64)],
    ) {
        check_invariants(storage, queues).await;
        assert_eq!(counts(storage, queues).await, counts_before);
        for (queue, live) in live {
            let claimed: BTreeSet<String> = storage
                .scan_claimed(Some(queue))
                .await
                .unwrap()
                .into_iter()
                .map(|(_, id, _)| id)
                .collect();
            let held: BTreeSet<String> =
                live.held.iter().map(|c| c.message.msg_id.clone()).collect();
            assert_eq!(claimed, held, "[{queue}] held claims changed");
        }
    }

    /// Settles the live work: every pending message must still be claimable
    /// with its body, and every held claim must still ack with its token.
    async fn settle_live(storage: &AnvilStorage, live: &HashMap<String, Live>) {
        for (queue, live) in live {
            let claimed = storage
                .claim_messages(queue, PENDING + 1, "drainer", "drainer-lease")
                .await
                .unwrap();
            let ids: Vec<&str> = claimed.iter().map(|c| c.message.msg_id.as_str()).collect();
            assert_eq!(ids, live.pending, "[{queue}] pending messages changed");
            assert!(claimed.iter().all(|c| c.message.payload == b"pending"));
            let (ids, tokens) = split_claims(&live.held);
            storage
                .ack_messages(queue, &ids, &tokens, "holder", "holder-lease")
                .await
                .unwrap();
        }
    }

    /// Runs one GC pass at `now` and checks it deleted exactly the entries of
    /// `acked` older than the retention, leaving `acked` as what remains.
    async fn gc_pass(
        storage: &AnvilStorage,
        now: u64,
        retention_ns: u64,
        acked: &mut BTreeSet<AckedEntry>,
    ) {
        set_sim_time_nanos(now);
        let cutoff = now - retention_ns;
        let expired = acked.iter().filter(|(_, ts, _)| *ts < cutoff).count();
        let deleted = storage.gc_acked_messages(retention_ns).await.unwrap();
        assert_eq!(deleted, expired, "pass at cutoff {cutoff}");
        acked.retain(|(_, ts, _)| *ts >= cutoff);
        assert_eq!(
            &stored_acked(storage).await,
            acked,
            "pass at cutoff {cutoff}"
        );
    }

    /// Goal: repeated GC passes, as the clock advances, delete exactly the
    /// acked entries older than the retention (an entry exactly at the cutoff
    /// is kept), never an unexpired entry, a held claim or a pending message,
    /// and change no counter, including across a crash between passes.
    /// Method: in simulated time, ack waves of messages in two queues ten
    /// seconds apart, add held claims and pending messages, then run a pass
    /// every five seconds, so that every other cutoff falls exactly on a
    /// wave, checking the stored entries against the recorded ack times
    /// after each; the store is crashed and reopened halfway.
    #[test]
    fn test_gc_passes_delete_exactly_the_expired_acked_entries() {
        const WAVES: u64 = 6;
        const WAVE_SIZE: usize = 20;
        const WAVE_SPACING_SECS: u64 = 10;
        const RETENTION_SECS: u64 = 20;
        const PASS_SPACING_SECS: u64 = 5;
        const PASSES: u64 = 6;
        const PASSES_BEFORE_CRASH: u64 = 3;

        let _clock = SimClock::acquire();
        let (dir, db_path) = crash_test_db_path("gc_passes");
        let queues = ["gc-a".to_string(), "gc-b".to_string()];
        let retention_ns = RETENTION_SECS * SEC_NANOS;
        let last_wave = T0_NANOS + (WAVES - 1) * WAVE_SPACING_SECS * SEC_NANOS;
        let pass_at = |pass: u64| last_wave + pass * PASS_SPACING_SECS * SEC_NANOS;

        let crashed = tokio::runtime::Runtime::new().unwrap();
        let (storage, mut acked, live, counts_before) = crashed.block_on(async {
            let storage = AnvilStorage::new(&db_path).await.unwrap();
            for queue in &queues {
                storage.create_queue(queue, 0).await.unwrap();
            }
            let mut acked = BTreeSet::new();
            for wave in 0..WAVES {
                set_sim_time_nanos(T0_NANOS + wave * WAVE_SPACING_SECS * SEC_NANOS);
                for queue in &queues {
                    acked.extend(ack_new(&storage, queue, WAVE_SIZE).await);
                }
            }
            let live = HashMap::from([(queues[0].clone(), add_live(&storage, &queues[0]).await)]);
            // The durable write that makes the held claims durable.
            acked.extend(ack_new(&storage, &queues[1], WAVE_SIZE).await);
            let counts_before = counts(&storage, &queues).await;
            assert_eq!(stored_acked(&storage).await, acked);

            for pass in 0..PASSES_BEFORE_CRASH {
                gc_pass(&storage, pass_at(pass), retention_ns, &mut acked).await;
            }
            check_untouched(&storage, &queues, &live, &counts_before).await;
            (storage, acked, live, counts_before)
        });
        crash(crashed, storage);

        let recovered = tokio::runtime::Runtime::new().unwrap();
        recovered.block_on(async {
            let storage = AnvilStorage::new(&db_path).await.unwrap();
            assert_eq!(stored_acked(&storage).await, acked);
            check_untouched(&storage, &queues, &live, &counts_before).await;
            for pass in PASSES_BEFORE_CRASH..PASSES {
                gc_pass(&storage, pass_at(pass), retention_ns, &mut acked).await;
            }
            assert!(acked.is_empty(), "the last pass left entries to expire");
            check_untouched(&storage, &queues, &live, &counts_before).await;
            settle_live(&storage, &live).await;
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Goal: GC of one queue never deletes the unexpired entries of another
    /// queue whose name extends it past a ':', as the load path already
    /// guards for pending and claimed keys. Method: ack one entry in `gc:00`
    /// at the time of the pass and run a pass with `gc` also present; the
    /// entry, younger than the retention, must survive.
    #[tokio::test]
    async fn test_gc_spares_unexpired_entries_of_a_queue_sharing_the_prefix() {
        let _clock = SimClock::acquire();
        let storage = AnvilStorage::new("memory://").await.unwrap();
        let (queue, extended) = ("gc", "gc:00");
        storage.create_queue(queue, 0).await.unwrap();
        storage.create_queue(extended, 0).await.unwrap();
        set_sim_time_nanos(T0_NANOS);
        ack_new(&storage, extended, 1).await;

        storage.gc_acked_messages(SEC_NANOS).await.unwrap();

        let survivors = storage.scan_acked(Some(extended)).await.unwrap().len();
        assert_eq!(
            survivors, 1,
            "GC deleted an entry younger than the retention"
        );
    }

    fn copy_dir(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let target = to.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_dir(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).unwrap();
            }
        }
    }

    /// Goal: a crash in the middle of a GC pass that spans several write
    /// batches loses no unexpired entry, held claim, pending message or
    /// counter, leaves the deleted entries a prefix of the expired range, and
    /// the next pass deletes exactly the rest. Method: build once, in
    /// simulated time, a backlog of expired entries several
    /// `GC_BATCH_KEYS_MAX` batches long beside unexpired entries and live
    /// work, and close the store; time one uninterrupted pass on a copy, then
    /// crash a pass on fresh copies at fixed fractions of that time, and
    /// reopen and check each.
    #[test]
    fn test_crash_mid_gc_pass_keeps_counters_and_live_entries() {
        const BACKLOG_BATCHES: usize = 5;
        const BACKLOG_BATCH: usize = GC_BATCH_KEYS_MAX / 2;
        const RECENT: usize = 16;
        const RETENTION_SECS: u64 = 500;
        const CRASH_FRACTIONS: [f64; 4] = [0.2, 0.4, 0.6, 0.8];

        let _clock = SimClock::acquire();
        let queue = "gc".to_string();
        let queues = [queue.clone()];
        let retention_ns = RETENTION_SECS * SEC_NANOS;
        let gc_at = T0_NANOS + 2 * retention_ns;
        let (template_dir, template_path) = crash_test_db_path("gc_mid_pass_template");

        let setup = tokio::runtime::Runtime::new().unwrap();
        let (backlog, recent, live, counts_before) = setup.block_on(async {
            let storage = AnvilStorage::new(&template_path).await.unwrap();
            storage.create_queue(&queue, 0).await.unwrap();
            set_sim_time_nanos(T0_NANOS);
            let mut backlog = BTreeSet::new();
            for _ in 0..BACKLOG_BATCHES {
                backlog.extend(ack_new(&storage, &queue, BACKLOG_BATCH).await);
            }
            set_sim_time_nanos(gc_at);
            let recent: BTreeSet<AckedEntry> = ack_new(&storage, &queue, RECENT)
                .await
                .into_iter()
                .collect();
            let live = HashMap::from([(queue.clone(), add_live(&storage, &queue).await)]);
            let counts_before = counts(&storage, &queues).await;
            // Closing makes the held claims durable.
            storage.close().await.unwrap();
            (backlog, recent, live, counts_before)
        });
        drop(setup);
        assert!(backlog.len() > 2 * GC_BATCH_KEYS_MAX);

        let copy_template = |name: &str| {
            let (dir, db_path) = crash_test_db_path(name);
            copy_dir(&template_dir, &dir);
            (dir, db_path)
        };

        let (dir, db_path) = copy_template("gc_mid_pass_full");
        let full_pass = tokio::runtime::Runtime::new().unwrap().block_on(async {
            let storage = AnvilStorage::new(&db_path).await.unwrap();
            let started = Instant::now();
            let deleted = storage.gc_acked_messages(retention_ns).await.unwrap();
            assert_eq!(deleted, backlog.len());
            started.elapsed()
        });
        let _ = std::fs::remove_dir_all(&dir);

        for fraction in CRASH_FRACTIONS {
            let (dir, db_path) = copy_template("gc_mid_pass");
            let crashed = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            let storage = crashed.block_on(async {
                let storage = Arc::new(AnvilStorage::new(&db_path).await.unwrap());
                let gc = storage.clone();
                tokio::spawn(async move { gc.gc_acked_messages(retention_ns).await });
                tokio::time::sleep(full_pass.mul_f64(fraction)).await;
                storage
            });
            crash(crashed, storage);

            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let storage = AnvilStorage::new(&db_path).await.unwrap();
                let stored = stored_acked(&storage).await;
                assert!(stored.is_superset(&recent), "unexpired entries deleted");
                let survivors: BTreeSet<AckedEntry> = stored.difference(&recent).cloned().collect();
                let deleted = backlog.len() - survivors.len();
                assert!(
                    backlog.iter().skip(deleted).eq(survivors.iter()),
                    "a crash {fraction} into the pass left survivors that are not a suffix \
                     of the expired range"
                );
                check_untouched(&storage, &queues, &live, &counts_before).await;

                let rest = storage.gc_acked_messages(retention_ns).await.unwrap();
                assert_eq!(rest, survivors.len());
                assert_eq!(stored_acked(&storage).await, recent);
                check_untouched(&storage, &queues, &live, &counts_before).await;
                settle_live(&storage, &live).await;
            });
            let _ = std::fs::remove_dir_all(&dir);
        }
        let _ = std::fs::remove_dir_all(&template_dir);
    }
}
