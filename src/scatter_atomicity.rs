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

// Atomicity of the forwarding Completes (`ack_and_scatter`,
// `ack_forward_with_state`) on a file:// store across crashes: an upstream
// ack, its downstream pushes, their partition placement and the state
// written with them must survive or vanish together.

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use rand::prelude::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    use crate::dst::tests::{check_invariants, check_recovered, crash, crash_test_db_path};
    use crate::storage::tests::split_claims;
    use crate::storage::AnvilStorage;
    use crate::types::Message;

    const GROUP: &str = "out";
    const PARTITIONS: u32 = 3;
    const UPSTREAM: &str = "src";
    const FORWARD_QUEUE: &str = "fwd";
    /// Namespace of the state each Complete writes, keyed by upstream msg_id.
    const DONE_NAMESPACE: &str = "done";
    const DRAIN_BATCH: usize = 256;

    fn partition_queue(pid: u32) -> String {
        format!("{GROUP}_p{pid}")
    }

    fn downstream_queues() -> Vec<String> {
        let mut queues: Vec<String> = (0..PARTITIONS).map(partition_queue).collect();
        queues.push(FORWARD_QUEUE.to_string());
        queues
    }

    /// One downstream message, encoded into its payload so a reopened store
    /// says by itself which upstream message produced it, how many siblings
    /// it has and which queue it was sent to.
    #[derive(Debug)]
    struct Output {
        upstream_id: String,
        count: usize,
        index: usize,
        queue: String,
    }

    impl Output {
        fn message(&self) -> Message {
            let payload = format!(
                "{}/{}/{}/{}",
                self.upstream_id, self.count, self.index, self.queue
            );
            Message::new(payload.into_bytes())
        }

        fn decode(payload: &[u8]) -> Self {
            let text = std::str::from_utf8(payload).unwrap();
            let parts: Vec<&str> = text.split('/').collect();
            assert_eq!(parts.len(), 4, "malformed output payload {text}");
            Self {
                upstream_id: parts[0].to_string(),
                count: parts[1].parse().unwrap(),
                index: parts[2].parse().unwrap(),
                queue: parts[3].to_string(),
            }
        }
    }

    /// Claims every message left in `queue` and decodes its payload.
    async fn drain_outputs(storage: &AnvilStorage, queue: &str) -> Vec<Output> {
        let mut outputs = Vec::new();
        loop {
            let claimed = storage
                .claim_messages(queue, DRAIN_BATCH, "drainer", "drainer-lease")
                .await
                .unwrap();
            if claimed.is_empty() {
                return outputs;
            }
            outputs.extend(claimed.iter().map(|c| Output::decode(&c.message.payload)));
        }
    }

    /// Goal: one `ack_and_scatter` acks the upstream message, places each
    /// payload in the partition it names and applies its state puts and
    /// deletes as one durable commit, while a refused scatter leaves no trace.
    /// Method: on a file:// store, scatter one claim to partitions 0 and 2
    /// with state, attempt a scatter to an out-of-range partition and one with
    /// a spent token, crash without closing, reopen, and read every effect and
    /// counter back.
    #[test]
    fn test_ack_and_scatter_commits_every_effect_across_reopen() {
        let (dir, db_path) = crash_test_db_path("scatter_reopen");

        let crashed = tokio::runtime::Runtime::new().unwrap();
        let (storage, scattered_id, held_id) = crashed.block_on(async {
            let storage = AnvilStorage::new(&db_path).await.unwrap();
            storage.create_queue(UPSTREAM, 0, None).await.unwrap();
            storage.create_queue(FORWARD_QUEUE, 0, None).await.unwrap();
            storage
                .create_queue_group(GROUP, PARTITIONS, 0, None)
                .await
                .unwrap();
            let stale = HashMap::from([("stale".to_string(), b"x".to_vec())]);
            storage
                .state_put_batch(DONE_NAMESPACE, &stale, &[])
                .await
                .unwrap();
            let inputs: Vec<Message> = (0u8..2).map(|i| Message::new(vec![i])).collect();
            storage.push_messages(UPSTREAM, &inputs).await.unwrap();
            let claimed = storage
                .claim_messages(UPSTREAM, 2, "worker", "lease")
                .await
                .unwrap();
            let (ids, tokens) = split_claims(&claimed);

            let puts = HashMap::from([(ids[0].clone(), b"scattered".to_vec())]);
            let new_ids = storage
                .ack_and_scatter(
                    UPSTREAM,
                    &ids[..1],
                    &tokens[..1],
                    "worker",
                    "lease",
                    GROUP,
                    &[
                        (
                            0,
                            vec![
                                Message::new(b"p0-a".to_vec()),
                                Message::new(b"p0-b".to_vec()),
                            ],
                        ),
                        (2, vec![Message::new(b"p2-a".to_vec())]),
                    ],
                    Some(DONE_NAMESPACE),
                    Some(&puts),
                    Some(&["stale".to_string()]),
                )
                .await
                .unwrap();
            assert_eq!(new_ids.len(), 3);

            let refused_puts = HashMap::from([(ids[1].clone(), b"refused".to_vec())]);
            let out_of_range = storage
                .ack_and_scatter(
                    UPSTREAM,
                    &ids[1..],
                    &tokens[1..],
                    "worker",
                    "lease",
                    GROUP,
                    &[
                        (1, vec![Message::new(b"refused".to_vec())]),
                        (PARTITIONS, vec![Message::new(b"refused".to_vec())]),
                    ],
                    Some(DONE_NAMESPACE),
                    Some(&refused_puts),
                    None,
                )
                .await;
            assert!(out_of_range.is_err());
            let spent_token = storage
                .ack_and_scatter(
                    UPSTREAM,
                    &ids[..1],
                    &tokens[..1],
                    "worker",
                    "lease",
                    GROUP,
                    &[(1, vec![Message::new(b"refused".to_vec())])],
                    Some(DONE_NAMESPACE),
                    Some(&refused_puts),
                    None,
                )
                .await;
            assert!(spent_token.is_err());
            (storage, ids[0].clone(), ids[1].clone())
        });
        crash(crashed, storage);

        let recovered = tokio::runtime::Runtime::new().unwrap();
        recovered.block_on(async {
            let storage = AnvilStorage::new(&db_path).await.unwrap();
            let mut queues = downstream_queues();
            queues.push(UPSTREAM.to_string());
            check_invariants(&storage, &queues).await;

            let acked: Vec<String> = storage
                .scan_acked(Some(UPSTREAM))
                .await
                .unwrap()
                .into_iter()
                .map(|(_, _, id)| id)
                .collect();
            assert_eq!(acked, std::slice::from_ref(&scattered_id));
            let claimed: Vec<String> = storage
                .scan_claimed(Some(UPSTREAM))
                .await
                .unwrap()
                .into_iter()
                .map(|(_, id, _)| id)
                .collect();
            assert_eq!(claimed, std::slice::from_ref(&held_id));
            let upstream = storage.get_meta(UPSTREAM).await.unwrap();
            assert_eq!(
                (
                    upstream.total_pushed,
                    upstream.total_acked,
                    upstream.claimed_count
                ),
                (2, 1, 1)
            );

            let (_, partition_stats) = storage.get_group_stats(GROUP).await.unwrap();
            let pushed: Vec<u64> = partition_stats
                .iter()
                .map(|(_, meta)| meta.total_pushed)
                .collect();
            assert_eq!(pushed, [2, 0, 1]);
            let expected: [&[&[u8]]; 3] = [&[b"p0-a", b"p0-b"], &[], &[b"p2-a"]];
            for (pid, want) in (0..PARTITIONS).zip(expected) {
                let (got, source_queue, source_pid) = storage
                    .claim_from_group(GROUP, 10, "w", "l", &[pid], false, 0)
                    .await
                    .unwrap();
                let payloads: Vec<&[u8]> =
                    got.iter().map(|c| c.message.payload.as_slice()).collect();
                assert_eq!(payloads, want, "partition {pid}");
                if !want.is_empty() {
                    assert_eq!((source_queue, source_pid), (partition_queue(pid), pid));
                }
            }

            let state = storage
                .state_get_batch(
                    DONE_NAMESPACE,
                    &[scattered_id.clone(), held_id, "stale".to_string()],
                )
                .await
                .unwrap();
            assert_eq!(
                state,
                HashMap::from([(scattered_id, b"scattered".to_vec())])
            );
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Goal: under a concurrent workload, a crash at any point leaves every
    /// upstream message either acked with all of its outputs and its state
    /// present, or unacked with none of them: no partial fan-out, no output
    /// or state without its ack, no output in a queue it was not sent to,
    /// and downstream totals that count exactly the outputs present.
    /// Method: producers push upstream while workers scatter each claim to
    /// random partitions or forward it, both with state, and a reclaimer
    /// requeues claims under them so Completes race fencing; the runtime is
    /// killed at seeded points, and the reopened store is checked against the
    /// payloads, which name their upstream message and their target queue.
    #[test]
    fn test_crash_during_concurrent_complete_never_splits_ack_from_outputs() {
        const PRODUCERS: usize = 2;
        const PUSH_BATCH: usize = 8;
        const WORKERS: usize = 8;
        const CLAIM_BATCH: usize = 2;
        const MAX_OUTPUTS: usize = 4;
        const RECLAIM_INTERVAL: Duration = Duration::from_millis(15);
        const IDLE_BACKOFF: Duration = Duration::from_millis(1);
        const TRIALS: u64 = 4;
        const CRASH_AFTER_MS: std::ops::Range<u64> = 100..1_200;

        let mut seeds = StdRng::seed_from_u64(0x5ca7);
        for trial in 0..TRIALS {
            let crash_after = Duration::from_millis(seeds.random_range(CRASH_AFTER_MS));
            let (dir, db_path) = crash_test_db_path("scatter_crash");
            let pushed: Arc<Mutex<HashSet<(String, String)>>> = Arc::default();
            let completed: Arc<Mutex<HashSet<(String, String)>>> = Arc::default();

            let crashed = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            let storage = crashed.block_on(async {
                let storage = Arc::new(AnvilStorage::new(&db_path).await.unwrap());
                storage.create_queue(UPSTREAM, 0, None).await.unwrap();
                storage.create_queue(FORWARD_QUEUE, 0, None).await.unwrap();
                storage
                    .create_queue_group(GROUP, PARTITIONS, 0, None)
                    .await
                    .unwrap();

                for _ in 0..PRODUCERS {
                    let (storage, pushed) = (storage.clone(), pushed.clone());
                    tokio::spawn(async move {
                        loop {
                            let batch: Vec<Message> = (0..PUSH_BATCH)
                                .map(|_| Message::new(b"in".to_vec()))
                                .collect();
                            storage.push_messages(UPSTREAM, &batch).await.unwrap();
                            pushed.lock().unwrap().extend(
                                batch.into_iter().map(|m| (UPSTREAM.to_string(), m.msg_id)),
                            );
                        }
                    });
                }

                for worker in 0..WORKERS {
                    let (storage, completed) = (storage.clone(), completed.clone());
                    let mut rng = StdRng::seed_from_u64(trial * 1_000 + worker as u64);
                    tokio::spawn(async move {
                        let worker_id = format!("w{worker}");
                        let lease_id = format!("lease-{worker}");
                        loop {
                            let claimed = storage
                                .claim_messages(UPSTREAM, CLAIM_BATCH, &worker_id, &lease_id)
                                .await
                                .unwrap();
                            if claimed.is_empty() {
                                tokio::time::sleep(IDLE_BACKOFF).await;
                                continue;
                            }
                            for c in claimed {
                                let upstream_id = c.message.msg_id;
                                let count = rng.random_range(1..=MAX_OUTPUTS);
                                // None forwards; Some(pid) scatters to pid.
                                let scatter = rng.random_bool(0.6);
                                let targets: Vec<Option<u32>> = (0..count)
                                    .map(|_| scatter.then(|| rng.random_range(0..PARTITIONS)))
                                    .collect();
                                let outputs: Vec<Message> = targets
                                    .iter()
                                    .enumerate()
                                    .map(|(index, target)| {
                                        Output {
                                            upstream_id: upstream_id.clone(),
                                            count,
                                            index,
                                            queue: target
                                                .map_or(FORWARD_QUEUE.to_string(), partition_queue),
                                        }
                                        .message()
                                    })
                                    .collect();
                                let ids = std::slice::from_ref(&upstream_id);
                                let tokens = std::slice::from_ref(&c.claim_token);
                                let puts = HashMap::from([(
                                    upstream_id.clone(),
                                    count.to_string().into_bytes(),
                                )]);
                                let result = if scatter {
                                    let mut by_partition: HashMap<u32, Vec<Message>> =
                                        HashMap::new();
                                    for (target, output) in targets.iter().zip(outputs) {
                                        by_partition
                                            .entry(target.unwrap())
                                            .or_default()
                                            .push(output);
                                    }
                                    let payloads: Vec<(u32, Vec<Message>)> =
                                        by_partition.into_iter().collect();
                                    storage
                                        .ack_and_scatter(
                                            UPSTREAM,
                                            ids,
                                            tokens,
                                            &worker_id,
                                            &lease_id,
                                            GROUP,
                                            &payloads,
                                            Some(DONE_NAMESPACE),
                                            Some(&puts),
                                            None,
                                        )
                                        .await
                                        .map(|_| ())
                                } else {
                                    storage
                                        .ack_forward_with_state(
                                            UPSTREAM,
                                            ids,
                                            tokens,
                                            &worker_id,
                                            &lease_id,
                                            FORWARD_QUEUE,
                                            &outputs,
                                            DONE_NAMESPACE,
                                            &puts,
                                            &[],
                                        )
                                        .await
                                };
                                // An error is the reclaimer having fenced
                                // this claim; the reopened store must show
                                // no trace of the refused Complete.
                                if result.is_ok() {
                                    completed
                                        .lock()
                                        .unwrap()
                                        .insert((UPSTREAM.to_string(), upstream_id));
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

                tokio::time::sleep(crash_after).await;
                storage
            });
            crash(crashed, storage);
            // Copy the records out under the lock: tasks of the crashed
            // runtime may still be unwinding.
            let pushed = pushed.lock().unwrap().clone();
            let completed = completed.lock().unwrap().clone();
            assert!(
                !completed.is_empty(),
                "trial {trial}: nothing completed in {crash_after:?} before the crash"
            );

            let recovered = tokio::runtime::Runtime::new().unwrap();
            recovered.block_on(async {
                let storage = AnvilStorage::new(&db_path).await.unwrap();
                let downstream = downstream_queues();
                check_invariants(&storage, &downstream).await;
                let acked: HashSet<String> = storage
                    .scan_acked(Some(UPSTREAM))
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|(_, _, id)| id)
                    .collect();
                assert_eq!(
                    storage.get_meta(UPSTREAM).await.unwrap().total_acked,
                    acked.len() as u64,
                    "trial {trial}: upstream total_acked disagrees with its acked keys"
                );
                // Every reported Complete is durable, and every unacked
                // upstream message is still deliverable.
                check_recovered(&storage, &[UPSTREAM.to_string()], &pushed, &completed).await;

                let mut outputs_of: HashMap<String, Vec<Output>> = HashMap::new();
                for queue in &downstream {
                    let outputs = drain_outputs(&storage, queue).await;
                    assert_eq!(
                        storage.get_meta(queue).await.unwrap().total_pushed,
                        outputs.len() as u64,
                        "trial {trial}: [{queue}] total_pushed disagrees with the outputs present"
                    );
                    for output in outputs {
                        assert_eq!(
                            &output.queue, queue,
                            "trial {trial}: output landed in the wrong queue"
                        );
                        outputs_of
                            .entry(output.upstream_id.clone())
                            .or_default()
                            .push(output);
                    }
                }

                for (upstream_id, outputs) in &mut outputs_of {
                    assert!(
                        acked.contains(upstream_id),
                        "trial {trial}: outputs of unacked {upstream_id} present: {outputs:?}"
                    );
                    outputs.sort_by_key(|o| o.index);
                    let count = outputs[0].count;
                    assert!(
                        outputs.iter().all(|o| o.count == count),
                        "trial {trial}: outputs of {upstream_id} from two Completes: {outputs:?}"
                    );
                    let indices: Vec<usize> = outputs.iter().map(|o| o.index).collect();
                    assert_eq!(
                        indices,
                        (0..count).collect::<Vec<_>>(),
                        "trial {trial}: partial or duplicated outputs of {upstream_id}"
                    );
                }
                for id in &acked {
                    assert!(
                        outputs_of.contains_key(id),
                        "trial {trial}: {id} acked without its outputs"
                    );
                }

                let upstream_ids: Vec<String> = pushed
                    .into_iter()
                    .map(|(_, id)| id)
                    .chain(acked)
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect();
                let state = storage
                    .state_get_batch(DONE_NAMESPACE, &upstream_ids)
                    .await
                    .unwrap();
                for id in &upstream_ids {
                    let want = outputs_of
                        .get(id)
                        .map(|outputs| outputs.len().to_string().into_bytes());
                    assert_eq!(
                        state.get(id),
                        want.as_ref(),
                        "trial {trial}: state of {id} disagrees with its outputs"
                    );
                }
            });
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}
