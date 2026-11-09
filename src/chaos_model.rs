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

// Model-based test of the gRPC service: random operation sequences, awkward
// ones included, run against the real service and storage and against a
// reference model of the documented semantics. After every operation the
// service must agree with the model on what each call returned, which
// messages a claim hands out and in what order, delivery counts, stats,
// lifetime totals and the drain signal.

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, VecDeque};
    use std::sync::Arc;

    use rand::prelude::*;
    use rand::rngs::StdRng;
    use tonic::{Code, Request};

    use crate::chaos_api::tests::{
        ack, claim_as, complete, complete_as, forward, nack, push, race_runtime, service, stats,
        TASK_DEADLINE,
    };
    use crate::service::proto::anvil_server::Anvil;
    use crate::service::proto::*;
    use crate::service::AnvilService;
    use crate::storage::AnvilStorage;
    use crate::types::SimClock;

    /// Queue names with separators other than ':' and unicode, so the model
    /// also checks that queues sharing a name prefix stay apart.
    const QUEUES: [&str; 3] = ["q", "q;x", "名前"];
    const WORKERS: usize = 3;
    const SEEDS: u64 = 16;
    const OPS_PER_SEED: usize = 150;
    /// Recovery timeouts no live claim reaches, so only killed workers'
    /// claims are reclaimed.
    const NEVER_SECS: f64 = 1e9;

    /// A queue as the documented semantics describe it.
    #[derive(Default)]
    struct QueueModel {
        /// msg_id and how often it was delivered, in claim order.
        pending: VecDeque<(String, u32)>,
        /// msg_id to the delivery count of its live claim.
        claimed: BTreeMap<String, u32>,
        total_pushed: i64,
        total_acked: i64,
        finished: bool,
    }

    struct Model {
        queues: HashMap<&'static str, QueueModel>,
        /// The claims each worker holds, as (queue, message).
        held: Vec<Vec<(&'static str, ClaimMessage)>>,
        /// Claims settled since, for replays that must be fenced.
        settled: Vec<(&'static str, ClaimMessage)>,
    }

    fn worker(index: usize) -> String {
        format!("w{index}")
    }

    struct Run {
        service: Arc<AnvilService>,
        storage: Arc<AnvilStorage>,
        model: Model,
        rng: StdRng,
        fenced_forwards: bool,
        log: Vec<String>,
    }

    impl Run {
        async fn new(seed: u64, fenced_forwards: bool) -> Self {
            let (service, storage) = service().await;
            for queue in QUEUES {
                service
                    .create_queue(Request::new(CreateQueueRequest {
                        queue: queue.to_string(),
                        max_pending: 0,
                        max_deliveries: None,
                    }))
                    .await
                    .unwrap();
            }
            Self {
                service,
                storage,
                model: Model {
                    queues: QUEUES.iter().map(|&q| (q, QueueModel::default())).collect(),
                    held: vec![Vec::new(); WORKERS],
                    settled: Vec::new(),
                },
                rng: StdRng::seed_from_u64(seed),
                fenced_forwards,
                log: Vec::new(),
            }
        }

        fn queue(&mut self) -> &'static str {
            QUEUES[self.rng.random_range(0..QUEUES.len())]
        }

        /// Removes and returns up to `max` random claims of worker `w`, all
        /// of one queue.
        fn take_held(&mut self, w: usize, max: usize) -> Option<(&'static str, Vec<ClaimMessage>)> {
            let held = &mut self.model.held[w];
            let queue = held.choose(&mut self.rng)?.0;
            let mut taken = Vec::new();
            let mut i = 0;
            while i < held.len() && taken.len() < max {
                if held[i].0 == queue && self.rng.random_bool(0.7) {
                    taken.push(held.remove(i).1);
                } else {
                    i += 1;
                }
            }
            if taken.is_empty() {
                let i = held.iter().position(|(q, _)| *q == queue).unwrap();
                taken.push(held.remove(i).1);
            }
            Some((queue, taken))
        }

        fn context(&self) -> String {
            self.log.join("\n")
        }

        async fn step(&mut self) {
            let w = self.rng.random_range(0..WORKERS);
            match self.rng.random_range(0..100) {
                0..=19 => self.push().await,
                20..=39 => self.claim(w).await,
                40..=51 => self.settle(w, Settle::Ack).await,
                52..=59 => self.settle(w, Settle::Nack).await,
                60..=67 => self.settle(w, Settle::Forward).await,
                68..=75 => self.fenced(w).await,
                76..=81 => self.renew(w).await,
                82..=87 => self.replay_settled().await,
                88..=91 => self.kill(w).await,
                92..=95 => self.duplicate(w).await,
                _ => self.mark_finished().await,
            }
            self.check().await;
        }

        async fn push(&mut self) {
            let queue = self.queue();
            let count = self.rng.random_range(1..=4);
            let ids = push(&self.service, queue, vec![b"p".to_vec(); count])
                .await
                .unwrap();
            self.log.push(format!("push {queue} {count}"));
            let model = self.model.queues.get_mut(queue).unwrap();
            model.pending.extend(ids.into_iter().map(|id| (id, 0)));
            model.total_pushed += count as i64;
        }

        async fn claim(&mut self, w: usize) {
            let queue = self.queue();
            let batch_size = self.rng.random_range(-1..=4);
            let claimed = claim_as(&self.service, queue, batch_size, &worker(w))
                .await
                .unwrap();
            self.log.push(format!(
                "claim {queue} {batch_size} by {w}: {}",
                claimed.len()
            ));
            let model = self.model.queues.get_mut(queue).unwrap();
            let expected = (batch_size.max(1) as usize).min(model.pending.len());
            assert_eq!(claimed.len(), expected, "{}", self.context());
            for message in claimed {
                let (msg_id, delivered) = model.pending.pop_front().unwrap();
                assert_eq!(message.msg_id, msg_id, "claim order\n{}", self.context());
                assert_eq!(message.delivery_count, delivered + 1, "{}", self.context());
                model.claimed.insert(msg_id, message.delivery_count);
                self.model.held[w].push((queue, message));
            }
        }

        async fn settle(&mut self, w: usize, settle: Settle) {
            let Some((queue, messages)) = self.take_held(w, 3) else {
                return;
            };
            let target = self.queue();
            let outputs = self.rng.random_range(0..=2);
            let action = match settle {
                Settle::Ack => ack(),
                Settle::Nack => nack(),
                Settle::Forward => forward(target, outputs),
            };
            let request = complete_as(queue, &messages, action, &worker(w));
            let response = self.service.complete(Request::new(request)).await;
            self.log
                .push(format!("{settle:?} {} of {queue} by {w}", messages.len()));
            let response = response.unwrap().into_inner();
            assert!(response.success);
            assert_eq!(response.processed_count as usize, messages.len());

            let model = self.model.queues.get_mut(queue).unwrap();
            for message in &messages {
                let delivered = model.claimed.remove(&message.msg_id).unwrap();
                if settle == Settle::Nack {
                    model.pending.push_back((message.msg_id.clone(), delivered));
                }
            }
            if settle != Settle::Nack {
                model.total_acked += messages.len() as i64;
            }
            if settle == Settle::Forward {
                assert_eq!(response.new_msg_ids.len(), outputs);
                let target = self.model.queues.get_mut(target).unwrap();
                target
                    .pending
                    .extend(response.new_msg_ids.into_iter().map(|id| (id, 0)));
                target.total_pushed += outputs as i64;
            }
            self.model
                .settled
                .extend(messages.into_iter().map(|m| (queue, m)));
        }

        /// A batch with one stale token among live claims, from the holder
        /// or from another worker: all or none, so nothing is settled.
        async fn fenced(&mut self, w: usize) {
            let Some((queue, messages)) = self.take_held(w, 3) else {
                return;
            };
            let mut request_messages = messages.clone();
            let from = if self.rng.random_bool(0.5) {
                let stale = self.rng.random_range(0..request_messages.len());
                request_messages[stale].claim_token = "stale".to_string();
                worker(w)
            } else {
                worker((w + 1) % WORKERS)
            };
            let action = match self.rng.random_range(0..3) {
                0 => ack(),
                1 => nack(),
                _ if self.fenced_forwards => forward(self.queue(), 2),
                _ => ack(),
            };
            let result = complete(
                &self.service,
                complete_as(queue, &request_messages, action, &from),
            )
            .await;
            self.log
                .push(format!("fenced {} of {queue} by {from}", messages.len()));
            assert_eq!(
                result.unwrap_err().code(),
                Code::FailedPrecondition,
                "{}",
                self.context()
            );
            self.model.held[w].extend(messages.into_iter().map(|m| (queue, m)));
        }

        async fn renew(&mut self, w: usize) {
            let Some((queue, messages)) = self.take_held(w, 3) else {
                return;
            };
            let stale = self.rng.random_bool(0.3);
            let mut tokens: Vec<String> = messages.iter().map(|m| m.claim_token.clone()).collect();
            if stale {
                tokens[0] = "stale".to_string();
            }
            let request = RenewClaimsRequest {
                queue: queue.to_string(),
                msg_ids: messages.iter().map(|m| m.msg_id.clone()).collect(),
                claim_tokens: tokens,
                worker_id: worker(w),
                lease_id: format!("lease-{}", worker(w)),
            };
            let result = self.service.renew_claims(Request::new(request)).await;
            self.log.push(format!(
                "renew {} of {queue} by {w} stale={stale}",
                messages.len()
            ));
            match result {
                Ok(_) => assert!(!stale, "{}", self.context()),
                Err(status) => {
                    assert!(stale, "{status:?}\n{}", self.context());
                    assert_eq!(status.code(), Code::FailedPrecondition);
                }
            }
            self.model.held[w].extend(messages.into_iter().map(|m| (queue, m)));
        }

        /// Settling an already settled claim again (a retried ack, a late
        /// nack) is fenced, even after its message was claimed again.
        async fn replay_settled(&mut self) {
            let Some((queue, message)) = self.model.settled.choose(&mut self.rng).cloned() else {
                return;
            };
            let action = if self.rng.random_bool(0.5) {
                ack()
            } else {
                nack()
            };
            let result =
                complete(&self.service, complete_as(queue, &[message], action, "w0")).await;
            self.log.push(format!("replay settled of {queue}"));
            assert_eq!(
                result.unwrap_err().code(),
                Code::FailedPrecondition,
                "{}",
                self.context()
            );
        }

        /// Worker `w` dies: its lease drops out of the live set, so recovery
        /// requeues each of its claims, per queue in msg_id order, keeping
        /// their delivery counts.
        async fn kill(&mut self, w: usize) {
            let now = crate::types::mono_now();
            let live: HashMap<String, std::time::Instant> = (0..WORKERS)
                .filter(|&other| other != w)
                .map(|other| (format!("lease-{}", worker(other)), now))
                .collect();
            let reclaimed = self
                .storage
                .recover_expired_claims(NEVER_SECS, NEVER_SECS, Some(&live))
                .await
                .unwrap();
            let mut dead: Vec<(&'static str, ClaimMessage)> =
                std::mem::take(&mut self.model.held[w]);
            self.log
                .push(format!("kill {w}: {reclaimed} of {}", dead.len()));
            assert_eq!(reclaimed, dead.len(), "{}", self.context());
            dead.sort_by(|a, b| a.1.msg_id.cmp(&b.1.msg_id));
            for (queue, message) in dead {
                let model = self.model.queues.get_mut(queue).unwrap();
                let delivered = model.claimed.remove(&message.msg_id).unwrap();
                model.pending.push_back((message.msg_id.clone(), delivered));
                self.model.settled.push((queue, message));
            }
        }

        /// One claim named twice in a batch is refused with no effect.
        async fn duplicate(&mut self, w: usize) {
            let Some((queue, mut messages)) = self.take_held(w, 1) else {
                return;
            };
            messages.push(messages[0].clone());
            let action = if self.rng.random_bool(0.5) {
                ack()
            } else {
                nack()
            };
            let result = complete(
                &self.service,
                complete_as(queue, &messages, action, &worker(w)),
            )
            .await;
            self.log.push(format!("duplicate of {queue} by {w}"));
            assert!(result.is_err(), "{}", self.context());
            messages.pop();
            self.model.held[w].extend(messages.into_iter().map(|m| (queue, m)));
        }

        async fn mark_finished(&mut self) {
            let queue = self.queue();
            self.service
                .mark_queue_finished(Request::new(MarkQueueFinishedRequest {
                    queue: queue.to_string(),
                }))
                .await
                .unwrap();
            self.log.push(format!("finish {queue}"));
            self.model.queues.get_mut(queue).unwrap().finished = true;
        }

        async fn check(&self) {
            for queue in QUEUES {
                let model = &self.model.queues[queue];
                let expected = (
                    model.pending.len() as i64,
                    model.claimed.len() as i64,
                    model.total_pushed,
                    model.total_acked,
                );
                let actual = stats(&self.service, queue).await.unwrap();
                assert_eq!(actual, expected, "stats of {queue}\n{}", self.context());
                let finished = self
                    .service
                    .is_queue_finished(Request::new(IsQueueFinishedRequest {
                        queue: queue.to_string(),
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                let drained =
                    model.finished && model.pending.is_empty() && model.claimed.is_empty();
                assert_eq!(
                    finished.finished,
                    model.finished,
                    "{queue}\n{}",
                    self.context()
                );
                assert_eq!(finished.drained, drained, "{queue}\n{}", self.context());
            }
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Settle {
        Ack,
        Nack,
        Forward,
    }

    fn run_seeds(fenced_forwards: bool) {
        // Recovery compares claim and renewal times with its own clock, so a
        // test moving the simulated clock meanwhile would skew them.
        let _clock = SimClock::acquire();
        race_runtime().block_on(run_seeds_async(fenced_forwards));
    }

    async fn run_seeds_async(fenced_forwards: bool) {
        // Seeds run side by side, each on its own storage: every durable
        // write waits out a WAL flush, so serial seeds would take minutes.
        let seeds: Vec<_> = (0..SEEDS)
            .map(|seed| {
                tokio::spawn(async move {
                    let mut run = Run::new(seed, fenced_forwards).await;
                    for _ in 0..OPS_PER_SEED {
                        run.step().await;
                    }
                })
            })
            .collect();
        for (seed, handle) in seeds.into_iter().enumerate() {
            let result = tokio::time::timeout(TASK_DEADLINE * 3, handle).await;
            assert!(matches!(result, Ok(Ok(()))), "seed {seed}: {result:?}");
        }
    }

    /// Goal: the service implements the documented semantics under random
    /// operation sequences. Method: 16 seeded runs of 150 operations each
    /// (pushes, claims of edge batch sizes, acks, nacks, forwards, batches
    /// with a stale token or a foreign worker, renewals, replays of settled
    /// claims, worker deaths reclaimed by recovery, duplicate msg_ids and
    /// finish marks) on three queues, checked against the model after every
    /// operation.
    #[test]
    fn test_service_matches_model() {
        run_seeds(false);
    }

    /// The model run with fenced forwards among the fenced batches. A forward
    /// refused for a stale token commits its unwritten downstream range, so
    /// the downstream stats count phantom pending messages
    /// (chaos_api::test_fenced_forward_leaves_no_phantom_pending).
    #[test]
    fn test_service_matches_model_with_fenced_forwards() {
        run_seeds(true);
    }
}
