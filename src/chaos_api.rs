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

// Adversarial tests of the gRPC surface: hostile and edge inputs on every
// RPC, checked against the error contract of docs/semantics.md (section 2.4),
// and operations racing on one claim or one queue.

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use tonic::{Code, Request, Status};

    use crate::server::AnvilBrokerInner;
    use crate::service::proto::anvil_server::Anvil;
    use crate::service::proto::*;
    use crate::service::AnvilService;
    use crate::state::AnvilState;
    use crate::storage::AnvilStorage;
    use crate::types::{AnvilConfig, ClaimInfo, SimClock};

    /// Bounds every wait on a racing task, so a deadlock fails the test
    /// instead of hanging the suite.
    pub(crate) const TASK_DEADLINE: Duration = Duration::from_secs(20);

    /// Pause of a consumer that found nothing to claim, so idle consumers do
    /// not starve the producer they wait for.
    const IDLE_POLL: Duration = Duration::from_millis(1);

    pub(crate) async fn service() -> (Arc<AnvilService>, Arc<AnvilStorage>) {
        let storage = Arc::new(AnvilStorage::new("memory://").await.unwrap());
        let service = AnvilService::new(
            Arc::new(AnvilState::new()),
            storage.clone(),
            AnvilConfig::default().lease_timeout_secs,
        );
        (Arc::new(service), storage)
    }

    pub(crate) fn race_runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap()
    }

    pub(crate) async fn push(
        service: &AnvilService,
        queue: &str,
        payloads: Vec<Vec<u8>>,
    ) -> Result<Vec<String>, Status> {
        service
            .push(Request::new(PushRequest {
                queue: queue.to_string(),
                payloads,
                metadata: HashMap::new(),
            }))
            .await
            .map(|r| r.into_inner().msg_ids)
    }

    pub(crate) async fn claim_as(
        service: &AnvilService,
        queue: &str,
        batch_size: i32,
        worker: &str,
    ) -> Result<Vec<ClaimMessage>, Status> {
        service
            .claim(Request::new(ClaimRequest {
                source: Some(claim_request::Source::Queue(queue.to_string())),
                worker_id: worker.to_string(),
                lease_id: format!("lease-{worker}"),
                batch_size,
                timeout_ms: 0,
            }))
            .await
            .map(|r| r.into_inner().messages)
    }

    async fn claim(
        service: &AnvilService,
        queue: &str,
        batch_size: i32,
    ) -> Result<Vec<ClaimMessage>, Status> {
        claim_as(service, queue, batch_size, "w").await
    }

    async fn push_and_claim(service: &AnvilService, queue: &str) -> ClaimMessage {
        push(service, queue, vec![b"x".to_vec()]).await.unwrap();
        let mut claimed = claim(service, queue, 1).await.unwrap();
        assert_eq!(claimed.len(), 1);
        claimed.pop().unwrap()
    }

    pub(crate) fn complete_as(
        queue: &str,
        messages: &[ClaimMessage],
        action: complete_request::Action,
        worker: &str,
    ) -> CompleteRequest {
        CompleteRequest {
            upstream_queue: queue.to_string(),
            msg_ids: messages.iter().map(|m| m.msg_id.clone()).collect(),
            claim_tokens: messages.iter().map(|m| m.claim_token.clone()).collect(),
            worker_id: worker.to_string(),
            lease_id: format!("lease-{worker}"),
            action: Some(action),
            state: None,
        }
    }

    fn complete_request(
        queue: &str,
        messages: &[ClaimMessage],
        action: complete_request::Action,
    ) -> CompleteRequest {
        complete_as(queue, messages, action, "w")
    }

    pub(crate) async fn complete(
        service: &AnvilService,
        request: CompleteRequest,
    ) -> Result<(), Status> {
        service.complete(Request::new(request)).await.map(|_| ())
    }

    pub(crate) fn ack() -> complete_request::Action {
        complete_request::Action::Ack(AckAction {})
    }

    pub(crate) fn nack() -> complete_request::Action {
        complete_request::Action::Nack(NackAction::default())
    }

    pub(crate) fn forward(downstream: &str, count: usize) -> complete_request::Action {
        complete_request::Action::Forward(ForwardAction {
            downstream_queue: downstream.to_string(),
            payloads: vec![b"out".to_vec(); count],
        })
    }

    fn scatter(group: &str, partition_id: i32) -> complete_request::Action {
        complete_request::Action::Scatter(ScatterAction {
            group_name: group.to_string(),
            partitions: vec![PartitionPayload {
                partition_id,
                payloads: vec![b"out".to_vec()],
            }],
        })
    }

    /// (pending, claimed, total_pushed, total_acked) as GetStats reports them.
    pub(crate) async fn stats(
        service: &AnvilService,
        queue: &str,
    ) -> Result<(i64, i64, i64, i64), Status> {
        let mut response = service
            .get_stats(Request::new(GetStatsRequest {
                queue: queue.to_string(),
            }))
            .await?
            .into_inner();
        let s = response
            .queues
            .remove(queue)
            .ok_or_else(|| Status::unknown(format!("GetStats omits {queue:?}")))?;
        Ok((
            s.pending_count,
            s.claimed_count,
            s.total_pushed,
            s.total_acked,
        ))
    }

    async fn create_group(
        service: &AnvilService,
        group: &str,
        partitions: i32,
    ) -> Result<CreateQueueGroupResponse, Status> {
        service
            .create_queue_group(Request::new(CreateQueueGroupRequest {
                group_name: group.to_string(),
                num_partitions: partitions,
                max_pending_per_partition: 0,
                max_deliveries: None,
            }))
            .await
            .map(|r| r.into_inner())
    }

    async fn is_finished(service: &AnvilService, queue: &str) -> IsQueueFinishedResponse {
        service
            .is_queue_finished(Request::new(IsQueueFinishedRequest {
                queue: queue.to_string(),
            }))
            .await
            .unwrap()
            .into_inner()
    }

    async fn mark_finished(service: &AnvilService, queue: &str) {
        service
            .mark_queue_finished(Request::new(MarkQueueFinishedRequest {
                queue: queue.to_string(),
            }))
            .await
            .unwrap();
    }

    fn assert_code<T: std::fmt::Debug>(result: &Result<T, Status>, code: Code, case: &str) {
        match result {
            Err(status) => assert_eq!(status.code(), code, "{case}: {status:?}"),
            Ok(value) => panic!("{case}: expected {code:?}, got Ok({value:?})"),
        }
    }

    // =====================================================================
    // Hostile and edge inputs
    // =====================================================================

    /// Goal: no key family of one queue name reads another's keys.
    /// Method: names with separators other than ':', unicode, control bytes
    /// and a key past 64 KiB, several of them prefixes of one another, each
    /// make a push, claim, ack and nack round trip; each queue's stats and
    /// redelivery then count only its own messages.
    #[tokio::test]
    async fn test_hostile_queue_names_round_trip_in_isolation() {
        let (service, _storage) = service().await;
        let names = [
            "q".to_string(),
            "q;".to_string(),
            "q0".to_string(),
            "q\0".to_string(),
            "q\n;/\\%_p0".to_string(),
            "名前-🚀".to_string(),
            "x".repeat(1024),
            "y".repeat(70_000),
        ];
        for name in &names {
            push(&service, name, vec![b"a".to_vec(), Vec::new()])
                .await
                .unwrap();
        }
        for name in &names {
            let claimed = claim(&service, name, 2).await.unwrap();
            assert_eq!(claimed.len(), 2, "{name:?}");
            assert!(claimed[1].payload.is_empty(), "{name:?}: empty payload");
            complete(&service, complete_request(name, &claimed[..1], ack()))
                .await
                .unwrap();
            complete(&service, complete_request(name, &claimed[1..], nack()))
                .await
                .unwrap();
        }
        for name in &names {
            assert_eq!(
                stats(&service, name).await.unwrap(),
                (1, 0, 2, 1),
                "{name:?}"
            );
            let redelivered = claim(&service, name, 10).await.unwrap();
            assert_eq!(redelivered.len(), 1, "{name:?}");
            assert_eq!(redelivered[0].delivery_count, 2, "{name:?}");
        }
    }

    /// Goal: batch sizes at the edges of i32 neither fail nor overflow.
    /// Method: zero and negative sizes claim one message each; i32::MAX
    /// claims the rest; the stats account for all five.
    #[tokio::test]
    async fn test_edge_batch_sizes() {
        let (service, _storage) = service().await;
        push(&service, "batch", vec![b"x".to_vec(); 5])
            .await
            .unwrap();
        for batch_size in [0, -5, i32::MIN] {
            let claimed = claim(&service, "batch", batch_size).await.unwrap();
            assert_eq!(claimed.len(), 1, "{batch_size}");
        }
        assert_eq!(claim(&service, "batch", i32::MAX).await.unwrap().len(), 2);
        assert!(claim(&service, "batch", i32::MAX).await.unwrap().is_empty());
        assert_eq!(stats(&service, "batch").await.unwrap(), (0, 5, 5, 0));
    }

    /// Goal: each malformed or misdirected request gets its documented code
    /// and settles nothing. Method: send each one against two live claims,
    /// then ack both claims normally and check the stats.
    #[tokio::test]
    async fn test_rejected_requests_have_no_effect() {
        let (service, _storage) = service().await;
        create_group(&service, "group", 2).await.unwrap();
        let message = push_and_claim(&service, "up").await;
        let other = push_and_claim(&service, "other").await;
        let one = std::slice::from_ref(&message);

        let mut extra_token = complete_request("up", one, ack());
        extra_token.claim_tokens.push("extra".to_string());
        let no_action = CompleteRequest {
            action: None,
            ..complete_request("up", one, ack())
        };
        let cases = [
            ("extra token", extra_token, Code::InvalidArgument),
            ("no action", no_action, Code::InvalidArgument),
            (
                "partition past the end",
                complete_request("up", one, scatter("group", 2)),
                Code::InvalidArgument,
            ),
            (
                "negative partition",
                complete_request("up", one, scatter("group", -1)),
                Code::InvalidArgument,
            ),
            (
                "largest partition",
                complete_request("up", one, scatter("group", i32::MAX)),
                Code::InvalidArgument,
            ),
            (
                "unnamed group",
                complete_request("up", one, scatter("", 0)),
                Code::InvalidArgument,
            ),
            (
                "unknown group",
                complete_request("up", one, scatter("missing", 0)),
                Code::NotFound,
            ),
            (
                "message of another queue",
                complete_request("up", std::slice::from_ref(&other), ack()),
                Code::FailedPrecondition,
            ),
        ];
        for (case, request, code) in cases {
            assert_code(&complete(&service, request).await, code, case);
        }

        let group_claim = |group_name: &str| ClaimRequest {
            source: Some(claim_request::Source::Group(GroupClaimSource {
                group_name: group_name.to_string(),
                assigned_partitions: vec![-1, 7],
                allow_steal: false,
                steal_pending_threshold: 0,
            })),
            worker_id: "w".to_string(),
            lease_id: "lease-w".to_string(),
            batch_size: 1,
            timeout_ms: 0,
        };
        let no_source = ClaimRequest {
            source: None,
            ..group_claim("group")
        };
        let claims = [
            ("claim without source", no_source, Code::InvalidArgument),
            (
                "unnamed group claim",
                group_claim(""),
                Code::InvalidArgument,
            ),
            (
                "unknown group claim",
                group_claim("missing"),
                Code::NotFound,
            ),
        ];
        for (case, request, code) in claims {
            assert_code(&service.claim(Request::new(request)).await, code, case);
        }
        let out_of_range = service
            .claim(Request::new(group_claim("group")))
            .await
            .unwrap()
            .into_inner();
        assert!(out_of_range.messages.is_empty());
        for partitions in [0, -1, i32::MIN] {
            let created = create_group(&service, "bad", partitions).await;
            assert_code(&created, Code::InvalidArgument, "num_partitions");
        }
        let empty_push = push(&service, "up", Vec::new()).await;
        assert_code(&empty_push, Code::InvalidArgument, "push without payloads");

        assert_eq!(stats(&service, "group_p0").await.unwrap(), (0, 0, 0, 0));
        complete(&service, complete_request("up", one, ack()))
            .await
            .unwrap();
        complete(&service, complete_request("other", &[other], ack()))
            .await
            .unwrap();
        assert_eq!(stats(&service, "up").await.unwrap(), (0, 0, 1, 1));
    }

    /// Goal: a bounded queue admits exactly `max_pending` messages in flight.
    /// Method: a batch crossing the bound is refused whole, a forward into
    /// the full queue is refused without acking upstream, an ack frees room
    /// for the forward, and the largest bound does not overflow.
    #[tokio::test]
    async fn test_max_pending_boundaries() {
        let (service, _storage) = service().await;
        for (queue, max_pending) in [("one", 1), ("unbounded", u64::MAX)] {
            service
                .create_queue(Request::new(CreateQueueRequest {
                    queue: queue.to_string(),
                    max_pending,
                    max_deliveries: None,
                }))
                .await
                .unwrap();
        }
        let over = push(&service, "one", vec![b"x".to_vec(); 2]).await;
        assert_code(&over, Code::ResourceExhausted, "batch over the bound");
        assert_eq!(stats(&service, "one").await.unwrap(), (0, 0, 0, 0));
        push(&service, "one", vec![b"x".to_vec()]).await.unwrap();
        let full = push(&service, "one", vec![b"x".to_vec()]).await;
        assert_code(&full, Code::ResourceExhausted, "full");

        let upstream = push_and_claim(&service, "up").await;
        let into_full = complete_request("up", std::slice::from_ref(&upstream), forward("one", 1));
        assert_code(
            &complete(&service, into_full).await,
            Code::ResourceExhausted,
            "forward into a full queue",
        );
        assert_eq!(stats(&service, "up").await.unwrap(), (0, 1, 1, 0));

        let held = claim(&service, "one", 1).await.unwrap();
        complete(&service, complete_request("one", &held, ack()))
            .await
            .unwrap();
        complete(
            &service,
            complete_request("up", &[upstream], forward("one", 1)),
        )
        .await
        .unwrap();
        assert_eq!(stats(&service, "one").await.unwrap(), (1, 0, 2, 1));

        push(&service, "unbounded", vec![b"x".to_vec(); 3])
            .await
            .unwrap();
        assert_eq!(stats(&service, "unbounded").await.unwrap(), (3, 0, 3, 0));
    }

    /// Goal: one msg_id twice in a Complete is a malformed request,
    /// INVALID_ARGUMENT. Method: ack and nack a claim listed twice.
    /// Settled as is, the second entry fails `SettlementHold::take` against
    /// the first and reports FAILED_PRECONDITION, which tells the worker it
    /// lost a claim it still holds: it drops the message, which then waits
    /// out the claim-age timeout.
    #[tokio::test]
    async fn test_duplicate_msg_ids_in_one_complete_are_invalid_argument() {
        let (service, _storage) = service().await;
        let message = push_and_claim(&service, "dup").await;
        let twice = [message.clone(), message.clone()];
        for action in [ack(), nack()] {
            let result = complete(&service, complete_request("dup", &twice, action)).await;
            assert_code(&result, Code::InvalidArgument, "duplicate msg_id");
        }
        complete(&service, complete_request("dup", &[message], ack()))
            .await
            .unwrap();
    }

    /// Goal: claim tokens that pair with no msg_id are INVALID_ARGUMENT, the
    /// contract's own example. Method: a Complete with one token and no ids,
    /// which a length check skipping empty msg_ids lets through.
    #[tokio::test]
    async fn test_unpaired_claim_tokens_are_invalid_argument() {
        let (service, _storage) = service().await;
        let mut request = complete_request("q", &[], ack());
        request.claim_tokens = vec!["orphan".to_string()];
        let result = complete(&service, request).await;
        assert_code(&result, Code::InvalidArgument, "tokens without ids");
    }

    /// Goal: a Complete whose state update the broker will not write fails
    /// whole. Method: ack with puts but no namespace, which StatePut refuses
    /// as INVALID_ARGUMENT; dropping the puts and acking alone would lose the
    /// update.
    #[tokio::test]
    async fn test_complete_state_without_namespace_is_rejected() {
        let (service, _storage) = service().await;
        let message = push_and_claim(&service, "q").await;
        let mut request = complete_request("q", &[message], ack());
        request.state = Some(StateUpdate {
            namespace: String::new(),
            puts: HashMap::from([("seen".to_string(), b"1".to_vec())]),
            deletes: Vec::new(),
        });
        let result = complete(&service, request).await;
        assert_code(&result, Code::InvalidArgument, "state without namespace");
        assert_eq!(stats(&service, "q").await.unwrap(), (0, 1, 1, 0));
    }

    /// Goal: a key of one state namespace is invisible in another, whatever
    /// bytes either holds. Method: namespace "job:a" key "k" and namespace
    /// "job" key "a:k" would be one stored key if the two were joined with
    /// ':'; put both, the first through a Complete, and read each back
    /// alone, by key and by a scan of its namespace.
    #[tokio::test]
    async fn test_state_namespaces_do_not_collide() {
        let (service, storage) = service().await;
        let message = push_and_claim(&service, "q").await;
        let mut request = complete_request("q", &[message], ack());
        request.state = Some(StateUpdate {
            namespace: "job:a".to_string(),
            puts: HashMap::from([("k".to_string(), b"job:a".to_vec())]),
            deletes: Vec::new(),
        });
        complete(&service, request).await.unwrap();
        service
            .state_put(Request::new(StatePutRequest {
                namespace: "job".to_string(),
                puts: HashMap::from([("a:k".to_string(), b"job".to_vec())]),
                deletes: Vec::new(),
            }))
            .await
            .unwrap();
        for (namespace, key) in [("job:a", "k"), ("job", "a:k")] {
            let read = service
                .state_get(Request::new(StateGetRequest {
                    namespace: namespace.to_string(),
                    keys: vec![key.to_string(), "k".to_string(), "a:k".to_string()],
                }))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(
                read.values,
                HashMap::from([(key.to_string(), namespace.as_bytes().to_vec())]),
                "{namespace}"
            );
            let scanned = storage.state_scan_prefix(namespace, "", 0).await.unwrap();
            let keys: Vec<&str> = scanned.iter().map(|(key, _)| key.as_str()).collect();
            assert_eq!(keys, [key], "{namespace}");
        }
    }

    /// Goal: every RPC refuses the empty queue name. MarkQueueFinished and
    /// IsQueueFinished refuse it and GetStats reads it as "all queues", so a
    /// queue named "" can be filled but never finished or inspected. Method:
    /// push, claim, create and forward to "".
    #[tokio::test]
    async fn test_empty_queue_name_is_invalid_argument() {
        let (service, _storage) = service().await;
        let pushed = push(&service, "", vec![b"x".to_vec()]).await;
        assert_code(&pushed, Code::InvalidArgument, "push");
        assert_code(
            &claim(&service, "", 1).await,
            Code::InvalidArgument,
            "claim",
        );
        let created = service
            .create_queue(Request::new(CreateQueueRequest {
                queue: String::new(),
                max_pending: 0,
                max_deliveries: None,
            }))
            .await;
        assert_code(&created, Code::InvalidArgument, "create");
        let message = push_and_claim(&service, "up").await;
        let forwarded =
            complete(&service, complete_request("up", &[message], forward("", 1))).await;
        assert_code(&forwarded, Code::InvalidArgument, "forward");
    }

    /// Goal: a forward fenced by a stale token leaves downstream untouched.
    /// Method: forward three payloads with a stale token into a finished,
    /// empty queue, then read its stats and drain state. Today
    /// `ack_internal` reserves the downstream seq range before
    /// `validate_claims`; the guard then commits the unwritten range, so
    /// GetStats and IsQueueFinished count three phantom pending messages
    /// (not drained) until a claim walks past the hole.
    #[tokio::test]
    async fn test_fenced_forward_leaves_no_phantom_pending() {
        let (service, _storage) = service().await;
        let mut message = push_and_claim(&service, "up").await;
        message.claim_token = "stale".to_string();
        service
            .create_queue(Request::new(CreateQueueRequest {
                queue: "down".to_string(),
                max_pending: 0,
                max_deliveries: None,
            }))
            .await
            .unwrap();
        mark_finished(&service, "down").await;
        let fenced = complete(
            &service,
            complete_request("up", &[message], forward("down", 3)),
        )
        .await;
        assert_code(&fenced, Code::FailedPrecondition, "stale forward");
        assert_eq!(stats(&service, "down").await.unwrap(), (0, 0, 0, 0));
        let finished = is_finished(&service, "down").await;
        assert!(finished.drained, "{finished:?}");
    }

    /// Goal: a broker config with NaN timeouts or non-positive intervals is
    /// refused by `start` with an error. Method: start a broker per bad
    /// config. A NaN passes a `<= 0.0` check, and a zero or NaN recovery
    /// interval or a negative GC interval panics only the spawned background
    /// task: unrefused, the broker serves with no recovery (dead workers'
    /// claims are never reclaimed) or no GC.
    #[test]
    fn test_invalid_broker_config_is_refused() {
        let base = AnvilConfig {
            host: "127.0.0.1".to_string(),
            db_path: "memory://".to_string(),
            ..AnvilConfig::default()
        };
        let cases = [
            (
                "NaN lease timeout",
                AnvilConfig {
                    lease_timeout_secs: f64::NAN,
                    ..base.clone()
                },
            ),
            (
                "zero recovery interval",
                AnvilConfig {
                    recovery_interval_secs: 0.0,
                    ..base.clone()
                },
            ),
            (
                "NaN recovery interval",
                AnvilConfig {
                    recovery_interval_secs: f64::NAN,
                    ..base.clone()
                },
            ),
            (
                "negative GC interval",
                AnvilConfig {
                    gc_interval_secs: -1.0,
                    ..base.clone()
                },
            ),
        ];
        let runtime = race_runtime();
        // `tokio::spawn` runs before `block_on` enters the runtime. The
        // spawn turns a panic in `start` into a failed join, not a test abort.
        let _context = runtime.enter();
        for (case, config) in cases {
            let started = runtime.block_on(tokio::spawn(async move {
                let mut broker = AnvilBrokerInner::new(config).await.unwrap();
                broker.start().await.map(|_| ()).map_err(|e| e.to_string())
            }));
            assert!(matches!(started, Ok(Err(_))), "{case}: {started:?}");
        }
    }

    // =====================================================================
    // Races
    // =====================================================================

    /// The ways one claim can end.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Settlement {
        Ack,
        Nack,
        Forward,
        Reclaim,
    }

    /// Goal: a claim is settled exactly once, whatever races for it, and
    /// each kind of settlement can be the one.
    /// Method: two acks with the same token, a nack, a forward, a recovery
    /// reclaim and a renewal race on one claim, 150 times. Each trial
    /// rotates which settlement is spawned first, its leader. The runtime
    /// has one thread, so spawn order and await points alone decide the
    /// interleaving: the leader takes the settlement hold before it first
    /// suspends, and the others make their checks while it writes. A
    /// work-stealing runtime would leave the winner to chance: there too
    /// the first-spawned task runs first unless an idle worker steals a
    /// later one, which under load is rare. The leader wins every trial and
    /// its effect alone is visible: an ack counts once, a forward delivers
    /// one output, a nack or reclaim redelivers the input once with
    /// delivery_count 2. Downstream is checked by draining it, not by
    /// stats, which a fenced forward inflates
    /// (test_fenced_forward_leaves_no_phantom_pending).
    #[test]
    fn test_settlements_racing_on_one_claim_settle_it_once() {
        const TRIALS: usize = 150;
        const ATTEMPTS: [Settlement; 5] = [
            Settlement::Ack,
            Settlement::Ack,
            Settlement::Nack,
            Settlement::Forward,
            Settlement::Reclaim,
        ];
        // A reclaim skips a claim renewed at or after its read time, so a
        // test moving the simulated clock meanwhile would skew the race.
        let _clock = SimClock::acquire();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            // Trials run side by side: each durable write waits out a WAL
            // flush, so serial trials would spend the suite's time budget.
            let trials: Vec<_> = (0..TRIALS)
                .map(|trial| {
                    tokio::spawn(async move {
                        let (service, storage) = service().await;
                        // A forward to a queue not yet loaded suspends to
                        // load it before it reaches the hold.
                        storage.create_queue("down", 0, None).await.unwrap();
                        let message = push_and_claim(&service, "up").await;
                        let (_, msg_id, info) =
                            storage.scan_claimed(Some("up")).await.unwrap().remove(0);
                        let claim_info = ClaimInfo { msg_id, ..info };
                        let read_at = crate::types::mono_now();
                        let mut order = ATTEMPTS;
                        order.rotate_left(trial % ATTEMPTS.len());
                        let attempts: Vec<_> = order
                            .iter()
                            .map(|&settlement| {
                                let (service, storage) = (service.clone(), storage.clone());
                                let (message, claim_info) = (message.clone(), claim_info.clone());
                                tokio::spawn(async move {
                                    let one = std::slice::from_ref(&message);
                                    let action = match settlement {
                                        Settlement::Ack => ack(),
                                        Settlement::Nack => nack(),
                                        Settlement::Forward => forward("down", 1),
                                        Settlement::Reclaim => {
                                            let reclaimed = storage
                                                .reclaim_messages("up", &[claim_info], read_at)
                                                .await
                                                .unwrap();
                                            return (settlement, !reclaimed.is_empty());
                                        }
                                    };
                                    let result =
                                        complete(&service, complete_request("up", one, action))
                                            .await;
                                    if let Err(status) = &result {
                                        assert_eq!(
                                            status.code(),
                                            Code::FailedPrecondition,
                                            "{status:?}"
                                        );
                                    }
                                    (settlement, result.is_ok())
                                })
                            })
                            .collect();
                        let renewal = {
                            let service = service.clone();
                            let message = message.clone();
                            tokio::spawn(async move {
                                let request = RenewClaimsRequest {
                                    queue: "up".to_string(),
                                    msg_ids: vec![message.msg_id],
                                    claim_tokens: vec![message.claim_token],
                                    worker_id: "w".to_string(),
                                    lease_id: "lease-w".to_string(),
                                };
                                if let Err(status) =
                                    service.renew_claims(Request::new(request)).await
                                {
                                    assert_eq!(
                                        status.code(),
                                        Code::FailedPrecondition,
                                        "{status:?}"
                                    );
                                }
                            })
                        };
                        let mut won = Vec::new();
                        for attempt in attempts {
                            let (settlement, ok) = tokio::time::timeout(TASK_DEADLINE, attempt)
                                .await
                                .unwrap()
                                .unwrap();
                            if ok {
                                won.push(settlement);
                            }
                        }
                        tokio::time::timeout(TASK_DEADLINE, renewal)
                            .await
                            .unwrap()
                            .unwrap();
                        assert_eq!(won, [order[0]], "trial {trial}: settled by {won:?}");
                        let winner = won[0];

                        let acked =
                            u64::from(matches!(winner, Settlement::Ack | Settlement::Forward));
                        let meta = storage.get_meta("up").await.unwrap();
                        assert_eq!(
                            (meta.claimed_count, meta.total_acked),
                            (0, acked),
                            "trial {trial}: {winner:?}"
                        );
                        let redelivered = claim(&service, "up", 10).await.unwrap();
                        let forwarded = claim(&service, "down", 10).await.unwrap();
                        let expected = match winner {
                            Settlement::Ack => (0, 0),
                            Settlement::Forward => (0, 1),
                            Settlement::Nack | Settlement::Reclaim => (1, 0),
                        };
                        assert_eq!(
                            (redelivered.len(), forwarded.len()),
                            expected,
                            "trial {trial}: {winner:?}"
                        );
                        assert!(redelivered.iter().all(|m| m.delivery_count == 2));
                    })
                })
                .collect();
            for trial in trials {
                tokio::time::timeout(TASK_DEADLINE, trial)
                    .await
                    .unwrap()
                    .unwrap();
            }
        });
    }

    /// Goal: stealing never delivers a message twice. Method: a producer
    /// scatters 400 messages, ten per call, unevenly over four partitions while owners
    /// claim their assigned partition and thieves claim with stealing at
    /// thresholds 0 and 1; every consumer acks and stops on the group's
    /// drain signal. Each message is delivered exactly once.
    #[test]
    fn test_group_steal_racing_assigned_claims_delivers_once() {
        const PARTITIONS: i32 = 4;
        const MESSAGES: usize = 400;
        const SCATTER_BATCH: usize = 10;
        race_runtime().block_on(async {
            let (service, _storage) = service().await;
            create_group(&service, "g", PARTITIONS).await.unwrap();
            let producer = {
                let service = service.clone();
                tokio::spawn(async move {
                    for round in 0..MESSAGES / SCATTER_BATCH {
                        let partition = (round % 7 % PARTITIONS as usize) as i32;
                        let first = round * SCATTER_BATCH;
                        let request = CompleteRequest {
                            action: Some(complete_request::Action::Scatter(ScatterAction {
                                group_name: "g".to_string(),
                                partitions: vec![PartitionPayload {
                                    partition_id: partition,
                                    payloads: (first..first + SCATTER_BATCH)
                                        .map(|i| i.to_le_bytes().to_vec())
                                        .collect(),
                                }],
                            })),
                            ..complete_request("src", &[], ack())
                        };
                        complete(&service, request).await.unwrap();
                    }
                    service
                        .mark_group_finished(Request::new(MarkGroupFinishedRequest {
                            group_name: "g".to_string(),
                        }))
                        .await
                        .unwrap();
                })
            };
            let consumers: Vec<_> = (0..6)
                .map(|c: i32| {
                    let service = service.clone();
                    let worker = format!("w{c}");
                    tokio::spawn(async move {
                        let assigned = if c < PARTITIONS { vec![c] } else { Vec::new() };
                        let mut delivered = Vec::new();
                        loop {
                            let request = ClaimRequest {
                                source: Some(claim_request::Source::Group(GroupClaimSource {
                                    group_name: "g".to_string(),
                                    assigned_partitions: assigned.clone(),
                                    allow_steal: c % 2 == 0 || c >= PARTITIONS,
                                    steal_pending_threshold: i64::from(c % 2),
                                })),
                                worker_id: worker.clone(),
                                lease_id: format!("lease-{worker}"),
                                batch_size: 3,
                                timeout_ms: 0,
                            };
                            let response = service
                                .claim(Request::new(request))
                                .await
                                .unwrap()
                                .into_inner();
                            if response.upstream_drained {
                                return delivered;
                            }
                            if response.messages.is_empty() {
                                tokio::time::sleep(IDLE_POLL).await;
                                continue;
                            }
                            let settle = complete_as(
                                &response.source_queue,
                                &response.messages,
                                ack(),
                                &worker,
                            );
                            complete(&service, settle).await.unwrap();
                            delivered.extend(response.messages.into_iter().map(|m| m.payload));
                        }
                    })
                })
                .collect();
            tokio::time::timeout(TASK_DEADLINE, producer)
                .await
                .unwrap()
                .unwrap();
            let mut delivered = HashSet::new();
            for consumer in consumers {
                let payloads = tokio::time::timeout(TASK_DEADLINE, consumer)
                    .await
                    .unwrap()
                    .unwrap();
                for payload in payloads {
                    assert!(delivered.insert(payload.clone()), "{payload:?} twice");
                }
            }
            assert_eq!(delivered.len(), MESSAGES);
        });
    }

    /// Goal: a settlement with a stale token cannot fence the claim's
    /// owner. Method: the owner's ack races a stale-token ack (a zombie
    /// worker's late ack after redelivery) of the same message, sent by
    /// eight zombies in a loop, 20 trials side by side; the owner's ack must succeed every time. Today
    /// `validate_claims` takes the settlement hold before it checks the
    /// token, so while the stale ack holds the claim the owner's ack is
    /// FAILED_PRECONDITION (claim_settling): the owner drops a message it
    /// holds, and the message waits for the claim-age timeout and is
    /// processed again.
    #[test]
    fn test_stale_settlement_does_not_fence_the_owner() {
        const TRIALS: usize = 20;
        const ZOMBIES: usize = 8;
        race_runtime().block_on(async {
            let trials: Vec<_> = (0..TRIALS)
                .map(|trial| {
                    tokio::spawn(async move {
                        let (service, _storage) = service().await;
                        let message = push_and_claim(&service, "q").await;
                        let owner_done = Arc::new(AtomicBool::new(false));
                        let zombies: Vec<_> = (0..ZOMBIES)
                            .map(|_| {
                                let (service, owner_done) = (service.clone(), owner_done.clone());
                                let mut stale = message.clone();
                                stale.claim_token = "stale".to_string();
                                tokio::spawn(async move {
                                    while !owner_done.load(Ordering::SeqCst) {
                                        let request = complete_request(
                                            "q",
                                            std::slice::from_ref(&stale),
                                            ack(),
                                        );
                                        let zombie = complete(&service, request).await;
                                        assert_code(&zombie, Code::FailedPrecondition, "stale ack");
                                        tokio::task::yield_now().await;
                                    }
                                })
                            })
                            .collect();
                        tokio::time::sleep(IDLE_POLL).await;
                        let owner =
                            complete(&service, complete_request("q", &[message], ack())).await;
                        owner_done.store(true, Ordering::SeqCst);
                        for zombie in zombies {
                            zombie.await.unwrap();
                        }
                        assert!(owner.is_ok(), "trial {trial}: owner fenced: {owner:?}");
                    })
                })
                .collect();
            for trial in trials {
                tokio::time::timeout(TASK_DEADLINE, trial)
                    .await
                    .unwrap()
                    .unwrap();
            }
        });
    }

    /// Goal: state written by a Complete lands only with its ack. Method:
    /// 16 workers ack their own message with puts to a shared key and to a
    /// key of their own, while 16 fenced Completes (stale tokens) put their
    /// own value to the shared key and a poison key. The shared key ends as
    /// a successful writer's value, each successful writer's key is
    /// present, and no fenced Complete wrote anything.
    #[test]
    fn test_racing_state_writes_land_only_with_their_ack() {
        const WORKERS: usize = 16;
        race_runtime().block_on(async {
            let (service, _storage) = service().await;
            push(&service, "q", vec![b"x".to_vec(); 2 * WORKERS])
                .await
                .unwrap();
            let mut claimed = claim(&service, "q", 2 * WORKERS as i32).await.unwrap();
            // The fenced Completes name messages no genuine Complete settles:
            // a stale settlement of a claim its owner is settling can fence
            // the owner (test_stale_settlement_does_not_fence_the_owner).
            let fenced = claimed.split_off(WORKERS);
            let completes: Vec<_> = claimed
                .into_iter()
                .zip(fenced)
                .enumerate()
                .flat_map(|(i, (message, mut stale))| {
                    stale.claim_token = format!("stale-{i}");
                    [
                        (format!("own-{i}"), message),
                        (format!("poison-{i}"), stale),
                    ]
                })
                .map(|(key, message)| {
                    let service = service.clone();
                    tokio::spawn(async move {
                        let mut request = complete_request("q", &[message], ack());
                        request.state = Some(StateUpdate {
                            namespace: "ns".to_string(),
                            puts: HashMap::from([
                                ("shared".to_string(), key.clone().into_bytes()),
                                (key.clone(), b"1".to_vec()),
                            ]),
                            deletes: Vec::new(),
                        });
                        complete(&service, request).await.is_ok().then_some(key)
                    })
                })
                .collect();
            let mut written = HashSet::new();
            for complete in completes {
                let key = tokio::time::timeout(TASK_DEADLINE, complete)
                    .await
                    .unwrap()
                    .unwrap();
                written.extend(key);
            }
            let expected: HashSet<String> = (0..WORKERS).map(|i| format!("own-{i}")).collect();
            assert_eq!(written, expected);

            let mut keys: Vec<String> = (0..WORKERS)
                .flat_map(|i| [format!("own-{i}"), format!("poison-{i}")])
                .collect();
            keys.push("shared".to_string());
            let mut values = service
                .state_get(Request::new(StateGetRequest {
                    namespace: "ns".to_string(),
                    keys,
                }))
                .await
                .unwrap()
                .into_inner()
                .values;
            let shared = String::from_utf8(values.remove("shared").unwrap()).unwrap();
            assert!(written.contains(&shared), "shared = {shared}");
            assert_eq!(values.into_keys().collect::<HashSet<_>>(), written);
        });
    }

    /// Goal: drained is never reported while work is left. Method: a
    /// producer pushes in batches and then marks the queue finished, while
    /// four workers claim, check IsQueueFinished, and ack (one nacks first
    /// deliveries so requeues race the check); a worker exits only on
    /// drained with an empty claim. Every message must be acked whenever
    /// drained is seen, and nothing is claimable afterwards.
    #[test]
    fn test_drained_is_never_reported_with_work_left() {
        const MESSAGES: usize = 300;
        race_runtime().block_on(async {
            let (service, _storage) = service().await;
            let producer = {
                let service = service.clone();
                tokio::spawn(async move {
                    for chunk in 0..MESSAGES / 10 {
                        let payloads = vec![chunk.to_le_bytes().to_vec(); 10];
                        push(&service, "q", payloads).await.unwrap();
                    }
                    mark_finished(&service, "q").await;
                })
            };
            let workers: Vec<_> = (0..4)
                .map(|w| {
                    let service = service.clone();
                    tokio::spawn(async move {
                        let mut acked = 0;
                        loop {
                            let messages = claim(&service, "q", 4).await.unwrap();
                            let drained = is_finished(&service, "q").await.drained;
                            if messages.is_empty() {
                                if drained {
                                    // Acks only grow, so all must be in by now.
                                    let total_acked = stats(&service, "q").await.unwrap().3;
                                    assert_eq!(
                                        total_acked, MESSAGES as i64,
                                        "drained with work in flight"
                                    );
                                    return acked;
                                }
                                tokio::time::sleep(IDLE_POLL).await;
                                continue;
                            }
                            let requeue = w == 0 && messages[0].delivery_count == 1;
                            let action = if requeue { nack() } else { ack() };
                            complete(&service, complete_request("q", &messages, action))
                                .await
                                .unwrap();
                            if !requeue {
                                acked += messages.len();
                            }
                        }
                    })
                })
                .collect();
            tokio::time::timeout(TASK_DEADLINE, producer)
                .await
                .unwrap()
                .unwrap();
            let mut acked = 0;
            for worker in workers {
                acked += tokio::time::timeout(TASK_DEADLINE, worker)
                    .await
                    .unwrap()
                    .unwrap();
            }
            assert_eq!(acked, MESSAGES, "a worker exited on drained with work left");
            assert!(claim(&service, "q", 10).await.unwrap().is_empty());
            let total = MESSAGES as i64;
            assert_eq!(stats(&service, "q").await.unwrap(), (0, 0, total, total));
        });
    }

    /// Goal: after DeleteQueue races workers, the queue's counters agree
    /// with its keys. Method: delete while concurrent pushes, claims and acks
    /// are in flight, 30 times, then compare claimed_count with the claimed
    /// keys. The pushes and acks are issued at once rather than one after
    /// another: a durable write waits for a WAL flush, and the race is with
    /// the writes in flight when the delete runs, not with later ones.
    /// Unfenced, a claim that loaded the old counters writes its claimed key
    /// after the delete batch, and the next access recreates the queue with
    /// zero counters beside the orphan. The live broker reports
    /// claimed_count 0 over orphan claims, which recovery requeues into the
    /// new queue (pointing at deleted bodies) and a restart counts as
    /// claimed; orphan pending keys likewise move the push cursor on restart.
    #[test]
    fn test_delete_queue_racing_workers_leaves_counters_matching_keys() {
        const TRIALS: usize = 30;
        race_runtime().block_on(async {
            for trial in 0..TRIALS {
                let (service, storage) = service().await;
                push(&service, "q", vec![b"x".to_vec(); 20]).await.unwrap();
                let held = claim(&service, "q", 5).await.unwrap();
                let mut tasks: Vec<_> = (0..20)
                    .map(|_| {
                        let service = service.clone();
                        tokio::spawn(async move {
                            let _ = push(&service, "q", vec![b"y".to_vec()]).await;
                        })
                    })
                    .collect();
                tasks.extend(held.into_iter().map(|message| {
                    let service = service.clone();
                    tokio::spawn(async move {
                        let _ = complete(&service, complete_request("q", &[message], ack())).await;
                    })
                }));
                tasks.push({
                    let service = service.clone();
                    tokio::spawn(async move {
                        for _ in 0..20 {
                            let _ = claim(&service, "q", 2).await;
                        }
                    })
                });
                tasks.push({
                    let service = service.clone();
                    tokio::spawn(async move {
                        tokio::task::yield_now().await;
                        let request = DeleteQueueRequest {
                            queue: "q".to_string(),
                        };
                        service.delete_queue(Request::new(request)).await.unwrap();
                    })
                });
                for task in tasks {
                    tokio::time::timeout(TASK_DEADLINE, task)
                        .await
                        .unwrap()
                        .unwrap();
                }
                let claimed_keys = storage.scan_claimed(Some("q")).await.unwrap().len() as u64;
                let claimed_count = storage
                    .get_meta("q")
                    .await
                    .map_or(0, |meta| meta.claimed_count);
                assert_eq!(claimed_count, claimed_keys, "trial {trial}");
            }
        });
    }

    /// Goal: of concurrent CreateQueueGroup calls for one new group, exactly
    /// one reports `created`. Method: two creates race, 20 times. A create
    /// that reads the group meta outside the lock it writes under lets both
    /// report created and both write the partitions' zero counters.
    #[test]
    fn test_concurrent_group_creates_report_one_creation() {
        const TRIALS: usize = 20;
        race_runtime().block_on(async {
            for trial in 0..TRIALS {
                let (service, _storage) = service().await;
                let creates: Vec<_> = (0..2)
                    .map(|_| {
                        let service = service.clone();
                        tokio::spawn(async move {
                            create_group(&service, "g", 2).await.unwrap().created
                        })
                    })
                    .collect();
                let mut created = 0;
                for create in creates {
                    let was_created = tokio::time::timeout(TASK_DEADLINE, create)
                        .await
                        .unwrap()
                        .unwrap();
                    created += usize::from(was_created);
                }
                assert_eq!(created, 1, "trial {trial}");
            }
        });
    }
}
