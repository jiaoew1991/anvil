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

// The cross-version format harness: a writer that leaves a file:// store in
// every state a queue can hold, and a verifier that opens such a store and
// checks its contents and behaviour. CI runs the writer of every release
// with a versioned format and the verifier of the current code against what
// each wrote (scripts/format-compat.sh), so a release that cannot read an
// earlier release's data fails before it ships.
//
// The writer of a release is frozen once that release is tagged: the
// verifier must keep accepting what it writes. Its test name, the
// ANVIL_FORMAT_COMPAT_DIR variable and the scenario below are therefore part
// of the format contract (docs/storage-format.md). A later writer may add to
// the scenario only under a new `SCENARIO` value, which the verifier checks
// for alongside every earlier one.

#[cfg(test)]
#[allow(clippy::await_holding_lock)] // SIM_TIME_LOCK is held across awaits to keep the clock this harness's own
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::path::Path;

    use crate::storage::tests::split_claims;
    use crate::storage::{AnvilStorage, Rejection};
    use crate::types::{now_nanos, set_sim_time_nanos, ClaimedMessage, Message, SIM_TIME_LOCK};

    /// Names the scenario a writer wrote, so the verifier knows what to
    /// expect. Bumped whenever the writer adds to the scenario.
    const SCENARIO: &[u8] = b"1";
    const COMPAT: &str = "compat";
    /// Namespaces and keys with ':' and NUL, which only a versioned format
    /// stores apart.
    const STATE_NAMESPACE: &str = "compat:ns";
    const STATE: [(&str, &[u8]); 3] = [("a", b"1"), ("a:b", b"\x00\xff"), ("nul\0key", b"")];
    const GROUP: &str = "grp";
    const PARTITIONS: u32 = 3;
    const BOUND: u64 = 2;
    const DAY_NANOS: u64 = 86_400 * 1_000_000_000;
    /// How long before the writer ran it acks the entries that are past
    /// retention by the time the verifier runs GC with a one-day retention.
    const EXPIRED_AGE_NANOS: u64 = 30 * DAY_NANOS;

    fn store_dir() -> String {
        std::env::var("ANVIL_FORMAT_COMPAT_DIR")
            .expect("ANVIL_FORMAT_COMPAT_DIR names the store directory")
    }

    async fn open(dir: &Path) -> AnvilStorage {
        AnvilStorage::new(&format!("file://{}", dir.display()))
            .await
            .unwrap()
    }

    fn payload(queue: &str, i: usize) -> Vec<u8> {
        format!("{queue}-{i}").into_bytes()
    }

    async fn push(storage: &AnvilStorage, queue: &str, count: usize) {
        let messages: Vec<Message> = (0..count)
            .map(|i| Message::new(payload(queue, i)))
            .collect();
        storage.push_messages(queue, &messages).await.unwrap();
    }

    async fn claim(storage: &AnvilStorage, queue: &str, count: usize) -> Vec<ClaimedMessage> {
        storage
            .claim_messages(queue, count, "worker", "lease")
            .await
            .unwrap()
    }

    async fn ack(storage: &AnvilStorage, queue: &str, claimed: &[ClaimedMessage]) {
        let (ids, tokens) = split_claims(claimed);
        storage
            .ack_messages(queue, &ids, &tokens, "worker", "lease")
            .await
            .unwrap();
    }

    /// Writes scenario 1 into an empty store at `dir`.
    async fn write(dir: &Path) {
        let _clock = SIM_TIME_LOCK.lock().unwrap();
        let storage = open(dir).await;

        // Redelivered: one message nacked and one reclaimed from a dead
        // lease, both pending again with one delivery behind them. First, so
        // that recovery finds no other claim to reclaim.
        push(&storage, "redelivered", 2).await;
        let nacked = claim(&storage, "redelivered", 1).await;
        let (ids, tokens) = split_claims(&nacked);
        storage
            .nack_messages("redelivered", &ids, &tokens, "worker", "lease")
            .await
            .unwrap();
        let reclaimed = claim(&storage, "redelivered", 1).await;
        assert_ne!(reclaimed[0].message.msg_id, nacked[0].message.msg_id);
        let recovered = storage
            .recover_expired_claims(1.0, 1.0, Some(&HashMap::new()))
            .await
            .unwrap();
        assert_eq!(recovered, 1);

        push(&storage, "pending", 4).await;
        let tagged = Message::with_metadata(
            payload("pending", 4),
            HashMap::from([("k".to_string(), "v".to_string())]),
        );
        storage.push_message("pending", &tagged).await.unwrap();

        // Claimed: two held by a worker, one still pending. The tokens are
        // kept so the verifier can settle the claims as their worker would.
        push(&storage, "claimed", 3).await;
        let held = claim(&storage, "claimed", 2).await;
        let tokens: String = held
            .iter()
            .map(|c| format!("{} {}\n", c.message.msg_id, c.claim_token))
            .collect();
        let compat = HashMap::from([
            ("scenario".to_string(), SCENARIO.to_vec()),
            ("claimed".to_string(), tokens.into_bytes()),
        ]);
        storage.state_put_batch(COMPAT, &compat, &[]).await.unwrap();

        push(&storage, "acked", 3).await;
        let claimed = claim(&storage, "acked", 3).await;
        ack(&storage, "acked", &claimed).await;

        push(&storage, "finished", 1).await;
        let claimed = claim(&storage, "finished", 1).await;
        ack(&storage, "finished", &claimed).await;
        storage.mark_queue_finished("finished").await.unwrap();

        storage
            .create_queue_group(GROUP, PARTITIONS, 0)
            .await
            .unwrap();
        push(&storage, "scatter-src", 1).await;
        let claimed = claim(&storage, "scatter-src", 1).await;
        let (ids, tokens) = split_claims(&claimed);
        let partitions = vec![
            (
                0,
                vec![
                    Message::new(payload(GROUP, 0)),
                    Message::new(payload(GROUP, 1)),
                ],
            ),
            (2, vec![Message::new(payload(GROUP, 2))]),
        ];
        storage
            .ack_and_scatter(
                "scatter-src",
                &ids,
                &tokens,
                "worker",
                "lease",
                GROUP,
                &partitions,
                None,
                None,
                None,
            )
            .await
            .unwrap();

        let state: HashMap<String, Vec<u8>> = STATE
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_vec()))
            .collect();
        storage
            .state_put_batch(STATE_NAMESPACE, &state, &[])
            .await
            .unwrap();

        storage.create_queue("bounded", BOUND).await.unwrap();
        push(&storage, "bounded", BOUND as usize).await;

        // Retention: two acks long past retention, two fresh ones.
        push(&storage, "retention", 4).await;
        let claimed = claim(&storage, "retention", 4).await;
        set_sim_time_nanos(now_nanos() - EXPIRED_AGE_NANOS);
        ack(&storage, "retention", &claimed[..2]).await;
        set_sim_time_nanos(0);
        ack(&storage, "retention", &claimed[2..]).await;

        storage.close().await.unwrap();
    }

    /// (pushed, acked, claimed, pending) of `queue`.
    async fn counts(storage: &AnvilStorage, queue: &str) -> (u64, u64, u64, u64) {
        let meta = storage.get_meta(queue).await.unwrap();
        (
            meta.total_pushed,
            meta.total_acked,
            meta.claimed_count,
            meta.push_seq - meta.claim_seq,
        )
    }

    fn sorted_payloads(claimed: &[ClaimedMessage]) -> Vec<Vec<u8>> {
        let mut payloads: Vec<Vec<u8>> =
            claimed.iter().map(|c| c.message.payload.clone()).collect();
        payloads.sort();
        payloads
    }

    /// Checks a store that `write`, of this release or an earlier one,
    /// wrote at `dir`, settling and collecting what it holds as it goes.
    async fn verify(dir: &Path) {
        // GC reads the wall clock, which a test elsewhere must not move.
        let _clock = SIM_TIME_LOCK.lock().unwrap();
        let storage = open(dir).await;
        let scenario = storage
            .state_get_batch(COMPAT, &["scenario".to_string()])
            .await
            .unwrap();
        assert_eq!(scenario["scenario"], SCENARIO, "an unknown scenario");

        // GC first, before the verifier acks anything: exactly the two
        // expired entries go.
        assert_eq!(storage.gc_acked_messages(DAY_NANOS).await.unwrap(), 2);
        assert_eq!(
            storage.scan_acked(Some("retention")).await.unwrap().len(),
            2
        );
        assert_eq!(counts(&storage, "retention").await, (4, 4, 0, 0));

        let queues: BTreeSet<String> = storage.list_queues().await.unwrap().into_iter().collect();
        let expected: BTreeSet<String> = [
            "acked",
            "bounded",
            "claimed",
            "finished",
            "grp_p0",
            "grp_p1",
            "grp_p2",
            "pending",
            "redelivered",
            "retention",
            "scatter-src",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        assert_eq!(queues, expected);

        assert_eq!(counts(&storage, "pending").await, (5, 0, 0, 5));
        let claimed = claim(&storage, "pending", 10).await;
        let expected: Vec<Vec<u8>> = (0..5).map(|i| payload("pending", i)).collect();
        assert_eq!(sorted_payloads(&claimed), expected);
        assert!(claimed.iter().all(|c| c.delivery_count == 1));
        let tagged = claimed
            .iter()
            .find(|c| c.message.payload == payload("pending", 4))
            .unwrap();
        assert_eq!(tagged.message.metadata["k"], "v");
        ack(&storage, "pending", &claimed).await;
        assert_eq!(counts(&storage, "pending").await, (5, 5, 0, 0));

        // The writer's claims, settled with the tokens it handed out: one
        // acked, the other reclaimed and redelivered.
        assert_eq!(counts(&storage, "claimed").await, (3, 0, 2, 1));
        let held = storage.scan_claimed(Some("claimed")).await.unwrap();
        assert_eq!(held.len(), 2);
        assert!(held
            .iter()
            .all(|(_, _, c)| (c.worker_id.as_str(), c.lease_id.as_str()) == ("worker", "lease")));
        let tokens = storage
            .state_get_batch(COMPAT, &["claimed".to_string()])
            .await
            .unwrap();
        let tokens = String::from_utf8(tokens["claimed"].clone()).unwrap();
        let (acked_id, acked_token) = tokens.lines().next().unwrap().split_once(' ').unwrap();
        storage
            .ack_messages(
                "claimed",
                &[acked_id.to_string()],
                &[acked_token.to_string()],
                "worker",
                "lease",
            )
            .await
            .unwrap();
        let recovered = storage
            .recover_expired_claims(1.0, 1.0, Some(&HashMap::new()))
            .await
            .unwrap();
        assert_eq!(recovered, 1);
        let redelivered = claim(&storage, "claimed", 10).await;
        let mut deliveries: Vec<u32> = redelivered.iter().map(|c| c.delivery_count).collect();
        deliveries.sort();
        assert_eq!(deliveries, [1, 2]);
        ack(&storage, "claimed", &redelivered).await;
        assert_eq!(counts(&storage, "claimed").await, (3, 3, 0, 0));

        assert_eq!(counts(&storage, "acked").await, (3, 3, 0, 0));
        assert_eq!(storage.scan_acked(Some("acked")).await.unwrap().len(), 3);

        assert_eq!(counts(&storage, "redelivered").await, (2, 0, 0, 2));
        let redelivered = claim(&storage, "redelivered", 10).await;
        assert!(redelivered.iter().all(|c| c.delivery_count == 2));
        assert_eq!(
            sorted_payloads(&redelivered),
            [payload("redelivered", 0), payload("redelivered", 1)]
        );
        ack(&storage, "redelivered", &redelivered).await;

        assert_eq!(
            storage.check_queue_completion("finished").await.unwrap(),
            (true, true, 0, 0)
        );

        let group = storage.get_group_meta(GROUP).await.unwrap().unwrap();
        assert_eq!(group.partition_queues, ["grp_p0", "grp_p1", "grp_p2"]);
        assert_eq!(counts(&storage, "scatter-src").await, (1, 1, 0, 0));
        let mut scattered = Vec::new();
        loop {
            let (claimed, queue, _) = storage
                .claim_from_group(GROUP, 10, "worker", "lease", &[0, 1, 2], false, 0)
                .await
                .unwrap();
            if claimed.is_empty() {
                break;
            }
            ack(&storage, &queue, &claimed).await;
            scattered.extend(claimed);
        }
        let expected: Vec<Vec<u8>> = (0..3).map(|i| payload(GROUP, i)).collect();
        assert_eq!(sorted_payloads(&scattered), expected);
        storage.mark_group_finished(GROUP).await.unwrap();
        let (finished, drained, _) = storage.check_group_completion(GROUP).await.unwrap();
        assert!(finished && drained);

        let keys: Vec<String> = STATE.iter().map(|(key, _)| key.to_string()).collect();
        let state = storage
            .state_get_batch(STATE_NAMESPACE, &keys)
            .await
            .unwrap();
        for (key, value) in STATE {
            assert_eq!(state[key], value, "state {key:?}");
        }
        let scanned = storage
            .state_scan_prefix(STATE_NAMESPACE, "a", 0)
            .await
            .unwrap();
        let scanned: Vec<&str> = scanned.iter().map(|(key, _)| key.as_str()).collect();
        assert_eq!(scanned, ["a", "a:b"]);

        assert_eq!(counts(&storage, "bounded").await, (2, 0, 0, 2));
        let full = storage
            .push_message("bounded", &Message::new(b"over".to_vec()))
            .await
            .unwrap_err();
        assert!(matches!(
            full.downcast_ref::<Rejection>(),
            Some(Rejection::QueueFull(_))
        ));
        let claimed = claim(&storage, "bounded", 1).await;
        ack(&storage, "bounded", &claimed).await;
        storage
            .push_message("bounded", &Message::new(b"room".to_vec()))
            .await
            .unwrap();

        storage.close().await.unwrap();
    }

    /// The writer, run by CI from each release: `ANVIL_FORMAT_COMPAT_DIR`
    /// names a directory that must not hold a store yet.
    #[tokio::test]
    #[ignore]
    async fn format_compat_write() {
        write(Path::new(&store_dir())).await;
    }

    /// The verifier, run by CI against the store each release's writer left
    /// in `ANVIL_FORMAT_COMPAT_DIR`.
    #[tokio::test]
    #[ignore]
    async fn format_compat_verify() {
        verify(Path::new(&store_dir())).await;
    }

    /// Goal: the verifier accepts what this release's writer writes, through
    /// a reopen, so the harness CI runs works. Method: write, then verify,
    /// in a fresh directory.
    #[tokio::test]
    async fn test_format_compat_round_trip() {
        let dir = std::env::temp_dir().join(format!("anvil_format_compat_{}", now_nanos()));
        write(&dir).await;
        verify(&dir).await;
        let _ = std::fs::remove_dir_all(&dir);
    }
}
