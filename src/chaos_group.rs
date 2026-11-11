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

// A queue group consumed by workers that keep state per partition, while
// workers die, stall and commit late, the store fails, and the broker
// restarts. Each worker sums the messages of its partition and writes the
// sum in the call that acks them, which is how a consumer whose result
// needs every message exactly once is meant to use a group. The sums must
// come out exact: a message counted twice or not at all shows in them.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use tokio::task::{JoinHandle, JoinSet};
    use tonic::{Code, Request, Status};

    use crate::dst::tests::check_invariants;
    use crate::fault_store::{FaultStore, Faults};
    use crate::recovery::RecoveryTask;
    use crate::service::proto::anvil_server::Anvil;
    use crate::service::proto::*;
    use crate::service::AnvilService;
    use crate::state::AnvilState;
    use crate::storage::AnvilStorage;
    use crate::storage_faults::tests::{down, flaky, memory, open};
    use crate::types::{AnvilConfig, Message, SimClock};

    const GROUP: &str = "g";
    const NAMESPACE: &str = "sums";
    const PARTITIONS: u32 = 4;
    const MESSAGES: u64 = 800;
    /// Messages a worker claims, and commits, at a time.
    const BATCH: i32 = 4;
    /// Workers racing for each partition: one consumes it, the rest stand by.
    const WORKERS_PER_PARTITION: usize = 2;
    const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(30);
    const RECOVERY_INTERVAL: Duration = Duration::from_millis(50);
    const IDLE_POLL: Duration = Duration::from_millis(10);
    /// What a worker spends on a batch before it commits it. It keeps a
    /// partition's consumer at work long enough for a late commit of an
    /// earlier one to land in between two of its own.
    const BATCH_WORK: Duration = Duration::from_millis(10);
    /// Bounds a whole run, so a wedged handover fails the test instead of
    /// hanging it.
    const RUN_DEADLINE: Duration = Duration::from_secs(180);

    /// How long the broker lets a lease go silent and a claim go unrenewed.
    #[derive(Clone, Copy)]
    struct Timeouts {
        lease: Duration,
        claim_age: Duration,
    }

    impl Timeouts {
        /// Long enough for the recovery task to act on a timeout that ran
        /// out.
        fn outlast(timeout: Duration) -> Duration {
            timeout + RECOVERY_INTERVAL * 4
        }
    }

    /// A broker without its gRPC transport: the service, the lease table and
    /// the recovery task over one store.
    struct Broker {
        storage: Arc<AnvilStorage>,
        state: Arc<AnvilState>,
        service: Arc<AnvilService>,
        recovery: RecoveryTask,
    }

    impl Broker {
        async fn start(store: &FaultStore, timeouts: Timeouts) -> Self {
            let storage = Arc::new(open(store).await);
            let state = Arc::new(AnvilState::new());
            let service = Arc::new(AnvilService::new(
                state.clone(),
                storage.clone(),
                timeouts.lease.as_secs_f64(),
            ));
            let config = AnvilConfig {
                lease_timeout_secs: timeouts.lease.as_secs_f64(),
                claim_age_timeout_secs: timeouts.claim_age.as_secs_f64(),
                recovery_interval_secs: RECOVERY_INTERVAL.as_secs_f64(),
                ..AnvilConfig::default()
            };
            let mut recovery = RecoveryTask::new(storage.clone(), state.clone(), config);
            recovery.start();
            Self {
                storage,
                state,
                service,
                recovery,
            }
        }
    }

    /// A lease kept alive by a heartbeat until it is dropped, or until the
    /// broker ends it, as a worker's `HeartbeatStream` keeps one.
    struct Lease {
        id: String,
        heartbeat: JoinHandle<()>,
    }

    impl Lease {
        fn open(state: &Arc<AnvilState>) -> Self {
            let id = state.open_lease("");
            let heartbeat = tokio::spawn({
                let (state, id) = (state.clone(), id.clone());
                async move {
                    while state.renew_lease(&id) {
                        tokio::time::sleep(HEARTBEAT_INTERVAL).await;
                    }
                }
            });
            Self { id, heartbeat }
        }
    }

    impl Drop for Lease {
        fn drop(&mut self) {
            self.heartbeat.abort();
        }
    }

    /// What becomes of a worker once it has claimed a batch.
    #[derive(Clone, Copy, Debug)]
    enum Fate {
        /// Commits the batch.
        Commit,
        /// Dies with the batch claimed.
        DieHolding,
        /// Stops heartbeating, and commits the batch once its lease is dead.
        DieThenCommit,
        /// Sits on the batch past the claim age timeout, heartbeating, then
        /// commits it.
        StallThenCommit,
        /// The same, having claimed a second batch that is still young when
        /// the first ages out, as a worker that claims ahead does; then
        /// commits both.
        StallHoldingMore,
    }

    /// The fates of a run's batches: each fault once, whichever worker gets
    /// there first, then a fault for one batch in fourteen.
    struct Fates {
        scripted: Vec<Fate>,
        rng: StdRng,
    }

    impl Fates {
        fn new(seed: u64) -> Self {
            Self {
                scripted: vec![
                    Fate::StallHoldingMore,
                    Fate::StallThenCommit,
                    Fate::DieThenCommit,
                    Fate::DieHolding,
                ],
                rng: StdRng::seed_from_u64(seed),
            }
        }

        fn draw(&mut self) -> Fate {
            if let Some(fate) = self.scripted.pop() {
                return fate;
            }
            match self.rng.random_range(0..100) {
                0..2 => Fate::DieHolding,
                2..4 => Fate::DieThenCommit,
                4..5 => Fate::StallThenCommit,
                5..7 => Fate::StallHoldingMore,
                _ => Fate::Commit,
            }
        }
    }

    fn encode_sums(count: u64, sum: u64) -> Vec<u8> {
        [count.to_le_bytes(), sum.to_le_bytes()].concat()
    }

    fn decode_sums(bytes: &[u8]) -> (u64, u64) {
        assert_eq!(bytes.len(), 16);
        (
            u64::from_le_bytes(bytes[..8].try_into().unwrap()),
            u64::from_le_bytes(bytes[8..].try_into().unwrap()),
        )
    }

    /// The count and sum committed for `partition`, zero before the first
    /// commit. Retries while the store fails the read.
    async fn committed(service: &AnvilService, partition: u32) -> (u64, u64) {
        let key = format!("p{partition}");
        loop {
            let request = StateGetRequest {
                namespace: NAMESPACE.to_string(),
                keys: vec![key.clone()],
            };
            match service.state_get(Request::new(request)).await {
                Ok(response) => {
                    return response
                        .into_inner()
                        .values
                        .get(&key)
                        .map_or((0, 0), |bytes| decode_sums(bytes));
                }
                Err(status) => {
                    assert_eq!(status.code(), Code::Internal, "{status:?}");
                    tokio::time::sleep(IDLE_POLL).await;
                }
            }
        }
    }

    /// A sum worker's identity and the partition it consumes.
    struct Worker {
        service: Arc<AnvilService>,
        name: String,
        partition: u32,
    }

    impl Worker {
        async fn claim(&self, lease: &Lease, batch_size: i32) -> Result<ClaimResponse, Status> {
            let request = ClaimRequest {
                source: Some(claim_request::Source::Group(GroupClaimSource {
                    group_name: GROUP.to_string(),
                    assigned_partitions: vec![self.partition as i32],
                })),
                worker_id: self.name.clone(),
                lease_id: lease.id.clone(),
                batch_size,
                timeout_ms: 0,
            };
            let response = self.service.claim(Request::new(request)).await?;
            Ok(response.into_inner())
        }

        /// Adds `messages` to the partition's sums, `known` if the worker
        /// holds them and read from the broker otherwise, and commits the
        /// result with the ack of the messages. Returns the sums committed,
        /// or `None` when the commit was refused or failed: it may have
        /// cost the worker its partition, or have landed all the same, so
        /// the sums are read again before the next commit.
        async fn commit(
            &self,
            lease: &Lease,
            messages: &[ClaimMessage],
            known: Option<(u64, u64)>,
        ) -> Option<(u64, u64)> {
            let (count, sum) = match known {
                Some(sums) => sums,
                None => committed(&self.service, self.partition).await,
            };
            let values = messages
                .iter()
                .map(|m| u64::from_le_bytes(m.payload.as_slice().try_into().unwrap()));
            let next = (count + messages.len() as u64, sum + values.sum::<u64>());
            let request = CompleteRequest {
                upstream_queue: format!("{GROUP}_p{}", self.partition),
                msg_ids: messages.iter().map(|m| m.msg_id.clone()).collect(),
                claim_tokens: messages.iter().map(|m| m.claim_token.clone()).collect(),
                worker_id: self.name.clone(),
                lease_id: lease.id.clone(),
                action: Some(complete_request::Action::Ack(AckAction {})),
                state: Some(StateUpdate {
                    namespace: NAMESPACE.to_string(),
                    puts: HashMap::from([(
                        format!("p{}", self.partition),
                        encode_sums(next.0, next.1),
                    )]),
                    deletes: Vec::new(),
                }),
            };
            match self.service.complete(Request::new(request)).await {
                Ok(_) => Some(next),
                Err(status) => {
                    assert!(
                        matches!(status.code(), Code::FailedPrecondition | Code::Internal),
                        "{}: a commit failed with {status:?}",
                        self.name
                    );
                    None
                }
            }
        }

        /// Sums the partition until it drains or the worker dies, and
        /// returns whether it died.
        ///
        /// The worker follows the rules a consumer with state must: it
        /// reads the committed sums only once a claim has made it the
        /// partition's consumer, commits them with the ack of the messages
        /// they add, and reads them again after any refusal or failure.
        async fn run(
            &self,
            state: &Arc<AnvilState>,
            fates: &Mutex<Fates>,
            timeouts: Timeouts,
        ) -> bool {
            let mut lease = Lease::open(state);
            let mut sums = None;
            loop {
                let claimed = match self.claim(&lease, BATCH).await {
                    Ok(claimed) => claimed,
                    Err(status) => {
                        sums = None;
                        match status.code() {
                            // Another worker consumes the partition, or the
                            // store failed.
                            Code::Aborted | Code::Internal => {}
                            // The broker ended the lease.
                            Code::FailedPrecondition => lease = Lease::open(state),
                            code => {
                                panic!("{}: a claim failed with {code:?}: {status:?}", self.name)
                            }
                        }
                        tokio::time::sleep(IDLE_POLL).await;
                        continue;
                    }
                };
                if claimed.upstream_drained {
                    assert_eq!(claimed.dead_count, 0, "{}: a dead letter", self.name);
                    return false;
                }
                if claimed.messages.is_empty() {
                    tokio::time::sleep(IDLE_POLL).await;
                    continue;
                }

                tokio::time::sleep(BATCH_WORK).await;
                let fate = fates.lock().unwrap().draw();
                let mut more = Vec::new();
                match fate {
                    Fate::Commit => {}
                    Fate::DieHolding => return true,
                    Fate::DieThenCommit => {
                        lease.heartbeat.abort();
                        tokio::time::sleep(Timeouts::outlast(timeouts.lease)).await;
                        // Refused, unless the broker has not noticed the death.
                        self.commit(&lease, &claimed.messages, sums).await;
                        return true;
                    }
                    Fate::StallThenCommit => {
                        tokio::time::sleep(Timeouts::outlast(timeouts.claim_age)).await;
                    }
                    Fate::StallHoldingMore => {
                        let young = timeouts.claim_age * 7 / 10;
                        tokio::time::sleep(young).await;
                        if let Ok(claimed) = self.claim(&lease, BATCH).await {
                            more = claimed.messages;
                        }
                        tokio::time::sleep(Timeouts::outlast(timeouts.claim_age) - young).await;
                    }
                }
                sums = self.commit(&lease, &claimed.messages, sums).await;
                if !more.is_empty() {
                    sums = self.commit(&lease, &more, sums).await;
                }
            }
        }
    }

    /// Runs the workload over `store` with `faults` on it from the first
    /// claim to the last commit, and checks the sums.
    async fn run(seed: u64, faults: Faults, timeouts: Timeouts) {
        let store = FaultStore::new(memory(), seed);
        let mut broker = Broker::start(&store, timeouts).await;
        // Unlimited deliveries: a batch a dying worker held is redelivered
        // however often its workers die.
        broker
            .storage
            .create_queue_group(GROUP, PARTITIONS, 0, Some(0))
            .await
            .unwrap();
        let mut expected = vec![(0u64, 0u64); PARTITIONS as usize];
        for partition in 0..PARTITIONS {
            let values: Vec<u64> = (1..=MESSAGES)
                .filter(|value| value % u64::from(PARTITIONS) == u64::from(partition))
                .collect();
            let messages: Vec<Message> = values
                .iter()
                .map(|value| Message::new(value.to_le_bytes().to_vec()))
                .collect();
            broker
                .storage
                .push_messages(&format!("{GROUP}_p{partition}"), &messages)
                .await
                .unwrap();
            expected[partition as usize] = (values.len() as u64, values.iter().sum());
        }
        broker.storage.mark_group_finished(GROUP).await.unwrap();
        store.set_faults(faults);

        let fates = Arc::new(Mutex::new(Fates::new(seed)));
        let mut workers = JoinSet::new();
        let mut spawned = 0;
        let mut spawn = |workers: &mut JoinSet<(u32, bool)>, partition: u32| {
            let worker = Worker {
                service: broker.service.clone(),
                name: format!("w{spawned}"),
                partition,
            };
            spawned += 1;
            let (state, fates) = (broker.state.clone(), fates.clone());
            workers.spawn(async move { (partition, worker.run(&state, &fates, timeouts).await) });
        };
        for partition in 0..PARTITIONS {
            for _ in 0..WORKERS_PER_PARTITION {
                spawn(&mut workers, partition);
            }
        }
        let mut deaths = 0;
        while let Some(exit) = workers.join_next().await {
            let (partition, died) = exit.unwrap();
            if died {
                deaths += 1;
                spawn(&mut workers, partition);
            }
        }
        assert!(deaths >= 2, "the scripted deaths did not happen");

        store.set_faults(Faults::default());
        broker.recovery.stop_async().await;
        for partition in 0..PARTITIONS {
            assert_eq!(
                committed(&broker.service, partition).await,
                expected[partition as usize],
                "seed {seed}: (count, sum) of partition {partition}"
            );
        }
        let queues: Vec<String> = (0..PARTITIONS)
            .map(|partition| format!("{GROUP}_p{partition}"))
            .collect();
        for (queue, (count, _)) in queues.iter().zip(&expected) {
            let stats = broker.storage.get_queue_stats(queue).await.unwrap();
            assert_eq!(
                (
                    stats.push_seq - stats.claim_seq,
                    stats.claimed_count,
                    stats.total_acked
                ),
                (0, 0, *count),
                "seed {seed}: (pending, claimed, acked) of {queue}"
            );
        }
        check_invariants(&broker.storage, &queues).await;
    }

    /// Goal: a store outage in the middle of a handover does not let a
    /// partition pass to its next consumer while the last one can still
    /// commit. Method: a worker commits one message, claims a second and
    /// dies; the store goes down past the lease timeout, which stalls the
    /// recovery pass on the write that reclaims the claim, and comes back.
    /// All along a second worker claims the partition: it gets no answer
    /// but a refusal or a store failure until a claim returns the second
    /// message, and from then on the dead worker's commit is refused. The
    /// sums end exact.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_outage_during_a_handover_keeps_the_sums_exact() {
        let _clock = SimClock::acquire();
        let timeouts = Timeouts {
            lease: Duration::from_millis(300),
            claim_age: Duration::from_secs(600),
        };
        let store = FaultStore::new(memory(), 7);
        let mut broker = Broker::start(&store, timeouts).await;
        broker
            .storage
            .create_queue_group(GROUP, 1, 0, Some(0))
            .await
            .unwrap();
        let messages: Vec<Message> = [5u64, 7]
            .iter()
            .map(|value| Message::new(value.to_le_bytes().to_vec()))
            .collect();
        for message in &messages {
            broker
                .storage
                .push_messages(&format!("{GROUP}_p0"), std::slice::from_ref(message))
                .await
                .unwrap();
        }
        let worker = |name: &str| Worker {
            service: broker.service.clone(),
            name: name.to_string(),
            partition: 0,
        };

        let (dead, dead_lease) = (worker("dead"), Lease::open(&broker.state));
        let first = dead.claim(&dead_lease, 1).await.unwrap().messages;
        assert_eq!(dead.commit(&dead_lease, &first, None).await, Some((1, 5)));
        let held = dead.claim(&dead_lease, 1).await.unwrap().messages;
        assert_eq!(held.len(), 1);
        dead_lease.heartbeat.abort();
        store.set_faults(down());

        let (next, next_lease) = (worker("next"), Lease::open(&broker.state));
        let outage_ends = tokio::time::Instant::now() + Timeouts::outlast(timeouts.lease) * 2;
        let taken = tokio::time::timeout(RUN_DEADLINE, async {
            loop {
                if tokio::time::Instant::now() >= outage_ends {
                    store.set_faults(Faults::default());
                }
                match next.claim(&next_lease, 1).await {
                    Ok(claimed) if !claimed.messages.is_empty() => return claimed.messages,
                    // The partition is free and empty: the dead worker's
                    // claim is still out, and can still be committed.
                    Ok(_) => panic!("the partition was handed over without its claim"),
                    // Refused, or the store failed the read of the group.
                    Err(status) => assert!(
                        matches!(status.code(), Code::Aborted | Code::Internal),
                        "{status:?}"
                    ),
                }
                tokio::time::sleep(IDLE_POLL).await;
            }
        })
        .await
        .expect("no handover after the outage");
        assert_eq!(taken[0].msg_id, held[0].msg_id);

        assert_eq!(dead.commit(&dead_lease, &held, Some((1, 5))).await, None);
        assert_eq!(next.commit(&next_lease, &taken, None).await, Some((2, 12)));
        broker.recovery.stop_async().await;
        assert_eq!(committed(&broker.service, 0).await, (2, 12));
    }

    /// Goal: a consumer learns how many of its partition's messages it
    /// never got. Method: of three messages of a partition that allows one
    /// delivery, the worker rejects one and commits two; the claim that
    /// reports the partition drained reports one dead letter, and the
    /// committed count and the dead letter add up to the messages pushed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_drained_partition_reports_its_dead_letters() {
        let _clock = SimClock::acquire();
        let timeouts = Timeouts {
            lease: Duration::from_secs(600),
            claim_age: Duration::from_secs(600),
        };
        let store = FaultStore::new(memory(), 11);
        let mut broker = Broker::start(&store, timeouts).await;
        // Two partitions: the dead letter of the other is not this
        // worker's to report.
        broker
            .storage
            .create_queue_group(GROUP, 2, 0, Some(1))
            .await
            .unwrap();
        let messages: Vec<Message> = [5u64, 7, 9]
            .iter()
            .map(|value| Message::new(value.to_le_bytes().to_vec()))
            .collect();
        for queue in [format!("{GROUP}_p0"), format!("{GROUP}_p1")] {
            broker
                .storage
                .push_messages(&queue, &messages)
                .await
                .unwrap();
        }
        broker.storage.mark_group_finished(GROUP).await.unwrap();

        let worker = Worker {
            service: broker.service.clone(),
            name: "w".to_string(),
            partition: 0,
        };
        let lease = Lease::open(&broker.state);
        let poison = worker.claim(&lease, 1).await.unwrap().messages;
        let reject = CompleteRequest {
            upstream_queue: format!("{GROUP}_p0"),
            msg_ids: vec![poison[0].msg_id.clone()],
            claim_tokens: vec![poison[0].claim_token.clone()],
            worker_id: worker.name.clone(),
            lease_id: lease.id.clone(),
            action: Some(complete_request::Action::Nack(NackAction {
                dead_letter: false,
                reason: "bad input".to_string(),
            })),
            state: None,
        };
        broker.service.complete(Request::new(reject)).await.unwrap();
        let rest = worker.claim(&lease, BATCH).await.unwrap().messages;
        assert_eq!(worker.commit(&lease, &rest, None).await, Some((2, 16)));

        let drained = worker.claim(&lease, BATCH).await.unwrap();
        assert!(drained.upstream_drained);
        assert_eq!(drained.dead_count, 1);
        assert_eq!(2 + drained.dead_count as usize, messages.len());
        broker.recovery.stop_async().await;
    }

    /// Goal: each partition's sum counts every message exactly once while
    /// its consumers die holding a batch, commit after their lease died,
    /// and stall past the claim age timeout. Method: two workers race for
    /// each of four partitions, a dead one is replaced, and every batch
    /// draws a fate; at the end the committed count and sum of each
    /// partition equal those of the messages pushed to it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_partition_sums_are_exact_under_worker_churn() {
        let _clock = SimClock::acquire();
        let timeouts = Timeouts {
            lease: Duration::from_millis(300),
            claim_age: Duration::from_millis(600),
        };
        for seed in [1, 2] {
            tokio::time::timeout(RUN_DEADLINE, run(seed, Faults::default(), timeouts))
                .await
                .unwrap_or_else(|_| panic!("seed {seed}: the run wedged"));
        }
    }

    /// Goal: the same, over a store whose requests fail, time out and lose
    /// the response to a put that landed, so that a claim or a commit may
    /// fail after it took effect. Method: as above, with the faults on
    /// from the first claim to the last commit.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_partition_sums_are_exact_over_a_flaky_store() {
        let _clock = SimClock::acquire();
        // A request the store times out takes 200 ms, and a claim makes
        // several: a claim age below that would end every worker's lease.
        let timeouts = Timeouts {
            lease: Duration::from_millis(300),
            claim_age: Duration::from_secs(3),
        };
        tokio::time::timeout(RUN_DEADLINE, run(3, flaky(), timeouts))
            .await
            .expect("the run wedged");
    }
}
