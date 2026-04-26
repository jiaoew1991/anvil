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

// SlateDB storage layer for Anvil — atomic counter model
//
// Key Schema:
//   seq_push:{queue}                -> u64 LE  (next push sequence)
//   seq_claim:{queue}               -> u64 LE  (next claim sequence)
//   cnt_total_pushed:{queue}        -> u64 LE  (lifetime push count)
//   cnt_total_claimed:{queue}       -> u64 LE  (monotonic claimed count)
//   cnt_total_unclaimed:{queue}     -> u64 LE  (monotonic unclaim count: ack + nack)
//   cnt_total_acked:{queue}         -> u64 LE  (lifetime ack count)
//   pending:{queue}:{seq:020d}      -> msg_id
//   msg:{queue}:{msg_id}            -> Message JSON
//   claimed:{queue}:{msg_id}        -> ClaimInfo JSON
//   acked:{queue}:{ts:020d}:{msg_id} -> ""
//   state:{namespace}:{key}         -> value bytes
//
// Hot paths (push, claim, ack, nack) use in-memory atomic counters
// with CAS loops instead of SlateDB SerializableSnapshot transactions.
// This eliminates transaction conflicts at high concurrency.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use dashmap::DashMap;
use slatedb::{Db, DbRead, Error as SlateError, WriteBatch};

use crate::types::{now_nanos, ClaimInfo, ClaimedMessage, Message, QueueGroupMeta};

pub type StorageError = Box<dyn std::error::Error + Send + Sync>;

/// Per-queue in-memory state. **Single source of truth** for everything
/// claimers and stage masters need to reason about queue progress.
///
/// Replaces the previous design of seven independent `AtomicU64`s + a
/// separate commit-log mutex. That design had a class of "torn snapshot"
/// bugs: writers modified counters in some order, readers consulted them
/// in some order, and getting the Acquire/Release pairings right required
/// memorizing every (writer-pair, reader-pair) combination. We tried —
/// it didn't survive contact with reality (PRs #84, #86, #87 each fixed
/// a different pair). Holding everything under one mutex eliminates the
/// entire bug class by construction.
///
/// **Lock scope**: every op acquires the mutex *only* for the in-memory
/// mutation step. `db.write` runs unlocked, so concurrent writers' batch
/// commits still execute in parallel. The critical section is sub-µs
/// (struct field bumps + maybe a `BTreeMap` insert).
#[derive(Debug, Default, Clone)]
pub struct QueueState {
    /// Committed push watermark — claimers' upper bound. Only advanced
    /// *after* a writer's `db.write(batch)` returns, so any seq below
    /// `push_seq_committed` has its `pending_key` durable.
    ///
    /// `commit_log` tracks in-flight reservations so out-of-order
    /// commits don't roll this watermark backward — it only moves
    /// through the contiguous-committed prefix.
    pub push_seq_committed: u64,
    /// Reservation cursor — bumped at the start of every push/nack/
    /// ack-with-downstream. NOT visible to claimers; used by stage
    /// masters' "is there work in flight?" check (drained must see
    /// in-flight reservations as work).
    pub push_seq_alloc: u64,
    /// Next seq to be claimed.
    pub claim_seq: u64,
    /// Total messages ever pushed.
    pub total_pushed: u64,
    /// Monotonic count of msgs that entered claimed state.
    pub total_claimed: u64,
    /// Monotonic count of msgs that left claimed state (ack OR nack).
    pub total_unclaimed: u64,
    /// Total messages ever acked.
    pub total_acked: u64,
    /// In-flight reservations keyed by `base_seq`. Each entry's `done`
    /// flips when the matching `PushReservationGuard` is dropped (post
    /// `db.write`, success or failure). The committer walks the front
    /// of the map and advances `push_seq_committed` through every
    /// contiguous-done entry.
    pub commit_log: BTreeMap<u64, PushReservation>,
}

impl QueueState {
    /// Pending count includes in-flight reservations. Use this for
    /// "is there work?" semantics (stage master, drained check).
    pub fn pending_count(&self) -> u64 {
        self.push_seq_alloc.saturating_sub(self.claim_seq)
    }

    /// Currently-claimed count.
    pub fn claimed_count(&self) -> u64 {
        self.total_claimed.saturating_sub(self.total_unclaimed)
    }
}

/// Per-queue counters / state holder. Just wraps the `QueueState` in a
/// mutex; everything else lives on the heap inside.
pub struct QueueCounters {
    pub state: std::sync::Mutex<QueueState>,
}

#[derive(Debug, Clone, Copy)]
pub struct PushReservation {
    pub count: u64,
    pub done: bool,
}

/// RAII guard for an in-flight push reservation. `Drop` flips the
/// matching `commit_log` entry to `done` and advances
/// `push_seq_committed` through the contiguous-done prefix — all
/// inside one mutex acquisition.
///
/// Drop runs on every exit path (Ok return, `?`-propagated error,
/// panic, async task cancellation), so a writer cannot leak a Pending
/// reservation and wedge the watermark.
pub struct PushReservationGuard {
    counters: Arc<QueueCounters>,
    base_seq: u64,
}

impl Drop for PushReservationGuard {
    fn drop(&mut self) {
        let mut s = match self.counters.state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(entry) = s.commit_log.get_mut(&self.base_seq) {
            entry.done = true;
        }
        // Walk the contiguous-done prefix from the front.
        loop {
            let front = s
                .commit_log
                .iter()
                .next()
                .map(|(&k, e)| (k, e.count, e.done));
            match front {
                Some((k, count, true)) if k == s.push_seq_committed => {
                    s.push_seq_committed = k + count;
                    s.commit_log.remove(&k);
                }
                _ => break,
            }
        }
    }
}

/// Queue metadata for O(1) operations (return type for get_queue_stats)
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct QueueMeta {
    pub claim_seq: u64,
    pub push_seq: u64,
    /// Number of currently claimed messages (O(1) stats)
    #[serde(default)]
    pub claimed_count: u64,
    /// Total messages ever pushed (lifetime counter)
    #[serde(default)]
    pub total_pushed: u64,
    /// Total messages ever acked (lifetime counter)
    #[serde(default)]
    pub total_acked: u64,
}

/// Options for ack operations
#[derive(Default)]
pub struct AckOptions<'a> {
    pub downstream_queue: Option<&'a str>,
    pub downstream_messages: Option<&'a [Message]>,
    pub state_namespace: Option<&'a str>,
    pub state_puts: Option<&'a HashMap<String, Vec<u8>>>,
    pub state_deletes: Option<&'a [String]>,
    pub claim_tokens: Option<&'a [String]>,
    pub lease_id: Option<&'a str>,
    pub worker_id: Option<&'a str>,
}

/// Anvil storage backed by SlateDB with in-memory atomic counters
pub struct AnvilStorage {
    db: Db,
    /// Per-queue atomic counters (in-memory cache, persisted to DB on each op)
    counters: DashMap<String, Arc<QueueCounters>>,
    /// Per-group round-robin counters for O(1) steal path in claim_from_group
    steal_rr: std::sync::Mutex<HashMap<String, u64>>,
    /// Per-queue max pending limits (0 = unlimited). Used for bounded queue support.
    max_pending_limits: DashMap<String, u64>,
}

impl AnvilStorage {
    pub async fn new(db_path: &str) -> Result<Self, StorageError> {
        let object_store = Db::resolve_object_store(db_path)?;
        let db = Db::open("/", object_store).await?;
        Ok(Self {
            db,
            counters: DashMap::new(),
            steal_rr: std::sync::Mutex::new(HashMap::new()),
            max_pending_limits: DashMap::new(),
        })
    }

    /// Close the storage gracefully.
    #[allow(dead_code)]
    pub async fn close(&self) -> Result<(), StorageError> {
        self.db.close().await?;
        Ok(())
    }

    // === Key Generation ===

    /// Legacy meta key (used for migration from old format)
    fn meta_key(queue: &str) -> Vec<u8> {
        format!("meta:{}", queue).into_bytes()
    }

    fn seq_push_key(queue: &str) -> Vec<u8> {
        format!("seq_push:{}", queue).into_bytes()
    }

    fn seq_claim_key(queue: &str) -> Vec<u8> {
        format!("seq_claim:{}", queue).into_bytes()
    }

    fn cnt_total_pushed_key(queue: &str) -> Vec<u8> {
        format!("cnt_total_pushed:{}", queue).into_bytes()
    }

    fn cnt_total_claimed_key(queue: &str) -> Vec<u8> {
        format!("cnt_total_claimed:{}", queue).into_bytes()
    }

    fn cnt_total_unclaimed_key(queue: &str) -> Vec<u8> {
        format!("cnt_total_unclaimed:{}", queue).into_bytes()
    }

    fn cnt_total_acked_key(queue: &str) -> Vec<u8> {
        format!("cnt_total_acked:{}", queue).into_bytes()
    }

    fn pending_key(queue: &str, seq: u64) -> Vec<u8> {
        format!("pending:{}:{:020}", queue, seq).into_bytes()
    }

    fn msg_key(queue: &str, msg_id: &str) -> Vec<u8> {
        format!("msg:{}:{}", queue, msg_id).into_bytes()
    }

    fn claimed_key(queue: &str, msg_id: &str) -> Vec<u8> {
        format!("claimed:{}:{}", queue, msg_id).into_bytes()
    }

    fn acked_key(queue: &str, timestamp_ns: u64, msg_id: &str) -> Vec<u8> {
        format!("acked:{}:{:020}:{}", queue, timestamp_ns, msg_id).into_bytes()
    }

    fn state_key(namespace: &str, key: &str) -> Vec<u8> {
        format!("state:{}:{}", namespace, key).into_bytes()
    }

    fn finished_key(queue: &str) -> Vec<u8> {
        format!("finished:{}", queue).into_bytes()
    }

    fn group_meta_key(group_name: &str) -> Vec<u8> {
        format!("group_meta:{}", group_name).into_bytes()
    }

    // === Counter helpers ===

    fn read_u64_le(data: &[u8]) -> u64 {
        if data.len() >= 8 {
            u64::from_le_bytes(data[..8].try_into().unwrap())
        } else {
            0
        }
    }

    /// Write 6 zero counter keys for a new queue into a WriteBatch.
    fn write_zero_counters(batch: &mut WriteBatch, queue: &str) {
        let zero = 0u64.to_le_bytes();
        batch.put(Self::seq_push_key(queue), zero);
        batch.put(Self::seq_claim_key(queue), zero);
        batch.put(Self::cnt_total_pushed_key(queue), zero);
        batch.put(Self::cnt_total_claimed_key(queue), zero);
        batch.put(Self::cnt_total_unclaimed_key(queue), zero);
        batch.put(Self::cnt_total_acked_key(queue), zero);
    }

    /// Validate claim tokens for a batch of message IDs.
    /// Reads claim info from DB and verifies token, lease, and worker identity.
    async fn validate_claims(
        &self,
        queue: &str,
        msg_ids: &[String],
        claim_tokens: &[String],
        expected_lease_id: Option<&str>,
        expected_worker_id: Option<&str>,
    ) -> Result<(), StorageError> {
        for (msg_id, token) in msg_ids.iter().zip(claim_tokens.iter()) {
            let claim_key = Self::claimed_key(queue, msg_id);
            let claim_data = self.db.get(&claim_key).await?.ok_or_else(|| {
                SlateError::invalid(format!(
                    "message_not_claimed: queue={queue}, msg_id={msg_id}"
                ))
            })?;
            let claim_info: ClaimInfo = serde_json::from_slice(&claim_data)?;

            if claim_info.claim_token != *token {
                return Err(Box::new(SlateError::invalid(format!(
                    "claim_token mismatch: queue={queue}, msg_id={msg_id}, \
                     expected={token}, actual={}",
                    claim_info.claim_token
                ))));
            }
            if let Some(expected) = expected_lease_id {
                if claim_info.lease_id != expected {
                    return Err(Box::new(SlateError::invalid(format!(
                        "lease_id mismatch: queue={queue}, msg_id={msg_id}, \
                         expected={expected}, actual={}",
                        claim_info.lease_id
                    ))));
                }
            }
            if let Some(expected) = expected_worker_id {
                if claim_info.worker_id != expected {
                    return Err(Box::new(SlateError::invalid(format!(
                        "worker_id mismatch: queue={queue}, msg_id={msg_id}, \
                         expected={expected}, actual={}",
                        claim_info.worker_id
                    ))));
                }
            }
        }
        Ok(())
    }

    /// Persist push counters into a WriteBatch (push_seq + total_pushed for a queue).
    fn persist_push_counters(
        batch: &mut WriteBatch,
        queue: &str,
        new_push_seq: u64,
        new_total_pushed: u64,
    ) {
        batch.put(Self::seq_push_key(queue), new_push_seq.to_le_bytes());
        batch.put(
            Self::cnt_total_pushed_key(queue),
            new_total_pushed.to_le_bytes(),
        );
    }

    /// Reserve a contiguous push range and register a pending entry in
    /// the queue's commit log. Returns a `PushReservationGuard` that
    /// closes the reservation on `Drop` (advancing the watermark through
    /// the contiguous-committed prefix).
    ///
    /// **Crucial**: callers MUST hold the guard for the entire span of
    /// "writing this push to the DB" — `Ok` returns, `?`-propagated
    /// errors, panics, and async-task cancellation all run the same
    /// `Drop` path. Without this, an early return between
    /// `reserve_push_range` and a separate explicit-commit call would
    /// leave a `Pending` entry in the log forever, wedging the watermark
    /// and orphaning every subsequent push to that queue. (That bug
    /// was real on this PR's first iteration — see
    /// `docs/lessons/anvil-publish-commit-race.md` for the trace.)
    ///
    /// All three push-side writers (`push_messages`,
    /// `nack_messages_internal`, the downstream-push branch of
    /// `ack_internal`) share this single bookkeeping path. The
    /// `push_commit_log` mutex is held only briefly here and inside
    /// `Drop` — never across `db.write` — so concurrent writers run
    /// their commits fully in parallel.
    fn reserve_push_range(c: &Arc<QueueCounters>, count: u64) -> PushReservationGuard {
        let base_seq = {
            let mut s = c.state.lock().expect("queue state poisoned");
            let base = s.push_seq_alloc;
            s.push_seq_alloc += count;
            s.commit_log
                .insert(base, PushReservation { count, done: false });
            base
        };
        PushReservationGuard {
            counters: c.clone(),
            base_seq,
        }
    }

    /// Load or initialize counters for a queue.
    /// 1. Check DashMap (fast path)
    /// 2. If missing, try to load from new counter keys
    /// 3. If new keys don't exist, fall back to old meta:{queue} JSON (migration)
    /// 4. Write new counter keys if migrating
    async fn load_or_init_counters(&self, queue: &str) -> Result<Arc<QueueCounters>, StorageError> {
        // Fast path: already in cache
        if let Some(c) = self.counters.get(queue) {
            return Ok(c.clone());
        }

        // Try loading from new counter keys first
        let push_seq_data = self.db.get(&Self::seq_push_key(queue)).await?;

        let counters = if let Some(ps_data) = push_seq_data {
            // New format exists — load all counter keys
            let push_seq = Self::read_u64_le(&ps_data);
            let claim_seq = self
                .db
                .get(&Self::seq_claim_key(queue))
                .await?
                .map(|d| Self::read_u64_le(&d))
                .unwrap_or(0);
            let total_pushed = self
                .db
                .get(&Self::cnt_total_pushed_key(queue))
                .await?
                .map(|d| Self::read_u64_le(&d))
                .unwrap_or(0);
            let total_claimed = self
                .db
                .get(&Self::cnt_total_claimed_key(queue))
                .await?
                .map(|d| Self::read_u64_le(&d))
                .unwrap_or(0);
            let total_unclaimed = self
                .db
                .get(&Self::cnt_total_unclaimed_key(queue))
                .await?
                .map(|d| Self::read_u64_le(&d))
                .unwrap_or(0);
            let total_acked = self
                .db
                .get(&Self::cnt_total_acked_key(queue))
                .await?
                .map(|d| Self::read_u64_le(&d))
                .unwrap_or(0);

            Arc::new(QueueCounters {
                state: std::sync::Mutex::new(QueueState {
                    push_seq_committed: push_seq,
                    push_seq_alloc: push_seq,
                    claim_seq,
                    total_pushed,
                    total_claimed,
                    total_unclaimed,
                    total_acked,
                    commit_log: BTreeMap::new(),
                }),
            })
        } else {
            // Try old meta:{queue} JSON (migration path)
            let old_meta: QueueMeta = match self.db.get(&Self::meta_key(queue)).await? {
                Some(data) => serde_json::from_slice(&data)?,
                None => QueueMeta::default(),
            };

            // Compute total_claimed and total_unclaimed from old format:
            // claimed_count = total_claimed - total_unclaimed
            // Set total_unclaimed = total_acked, total_claimed = claimed_count + total_acked
            // Then claimed_count = (claimed_count + total_acked) - total_acked = claimed_count. Correct.
            let migrated_total_claimed = old_meta.claimed_count + old_meta.total_acked;
            let migrated_total_unclaimed = old_meta.total_acked;

            let c = Arc::new(QueueCounters {
                state: std::sync::Mutex::new(QueueState {
                    push_seq_committed: old_meta.push_seq,
                    push_seq_alloc: old_meta.push_seq,
                    claim_seq: old_meta.claim_seq,
                    total_pushed: old_meta.total_pushed,
                    total_claimed: migrated_total_claimed,
                    total_unclaimed: migrated_total_unclaimed,
                    total_acked: old_meta.total_acked,
                    commit_log: BTreeMap::new(),
                }),
            });

            // Persist new counter keys
            let mut batch = WriteBatch::new();
            batch.put(Self::seq_push_key(queue), old_meta.push_seq.to_le_bytes());
            batch.put(Self::seq_claim_key(queue), old_meta.claim_seq.to_le_bytes());
            batch.put(
                Self::cnt_total_pushed_key(queue),
                old_meta.total_pushed.to_le_bytes(),
            );
            batch.put(
                Self::cnt_total_claimed_key(queue),
                migrated_total_claimed.to_le_bytes(),
            );
            batch.put(
                Self::cnt_total_unclaimed_key(queue),
                migrated_total_unclaimed.to_le_bytes(),
            );
            batch.put(
                Self::cnt_total_acked_key(queue),
                old_meta.total_acked.to_le_bytes(),
            );
            self.db.write(batch).await?;

            c
        };

        let _ = self.counters.entry(queue.to_string()).or_insert(counters);
        Ok(self.counters.get(queue).unwrap().clone())
    }

    // === Queue Metadata (legacy + new) ===

    /// Read QueueMeta from legacy meta:{queue} key.
    /// Kept for backward compat and migration path.
    #[allow(dead_code)]
    async fn get_meta_from_reader<R: DbRead + Sync + ?Sized>(
        reader: &R,
        queue: &str,
    ) -> Result<QueueMeta, StorageError> {
        match reader.get(Self::meta_key(queue)).await? {
            Some(data) => Ok(serde_json::from_slice(&data)?),
            None => Ok(QueueMeta::default()),
        }
    }

    /// Get queue metadata — single locked snapshot of in-memory state.
    pub async fn get_meta(&self, queue: &str) -> Result<QueueMeta, StorageError> {
        let c = self.load_or_init_counters(queue).await?;
        let s = c.state.lock().expect("queue state poisoned");
        Ok(QueueMeta {
            push_seq: s.push_seq_committed,
            claim_seq: s.claim_seq,
            claimed_count: s.claimed_count(),
            total_pushed: s.total_pushed,
            total_acked: s.total_acked,
        })
    }

    /// Create a queue — write the 6 counter keys (all zeros).
    /// `max_pending`: 0 = unlimited (default), >0 = bounded queue.
    pub async fn create_queue(&self, queue: &str, max_pending: u64) -> Result<(), StorageError> {
        // Always set in-memory limit (survives idempotent create on persistent DB).
        if max_pending > 0 {
            self.max_pending_limits
                .insert(queue.to_string(), max_pending);
        }

        // Check if already exists (either new or old format)
        if self.db.get(&Self::seq_push_key(queue)).await?.is_some() {
            return Ok(());
        }
        if self.db.get(&Self::meta_key(queue)).await?.is_some() {
            return Ok(());
        }

        let mut batch = WriteBatch::new();
        Self::write_zero_counters(&mut batch, queue);
        self.db.write(batch).await?;
        self.db.flush().await?;

        Ok(())
    }

    /// Check if a queue has capacity for `additional` messages.
    /// Uses atomic counter reads (O(1)), no scans.
    /// Slight over-admission is acceptable (Relaxed ordering).
    async fn check_queue_capacity(
        &self,
        queue: &str,
        additional: usize,
    ) -> Result<(), StorageError> {
        if let Some(limit) = self.max_pending_limits.get(queue) {
            let max = *limit;
            if max > 0 {
                // Always load counters — they may not be cached yet on first access.
                let counters = self.load_or_init_counters(queue).await?;
                let in_flight = {
                    let s = counters.state.lock().expect("queue state poisoned");
                    s.total_pushed.saturating_sub(s.total_acked)
                };
                if in_flight + additional as u64 > max {
                    return Err(Box::new(std::io::Error::other(format!(
                        "QueueFull: queue={queue}, in_flight={in_flight}, max_pending={max}, attempted={additional}"
                    ))));
                }
            }
        }
        Ok(())
    }

    // === Push Operations (NO TRANSACTION) ===

    /// Push messages to queue.
    ///
    /// Reserves a unique seq range via `reserve_push_range` (briefly
    /// touches the per-queue commit log mutex), runs `db.write` without
    /// holding any per-queue lock, and lets the returned guard's `Drop`
    /// advance the claimer-visible `push_seq` watermark through the
    /// contiguous-committed prefix on every exit path. Concurrent
    /// pushers / nacks proceed fully in parallel through their `db.write`s.
    pub async fn push_messages(
        &self,
        queue: &str,
        messages: &[Message],
    ) -> Result<(), StorageError> {
        if messages.is_empty() {
            return Ok(());
        }

        self.check_queue_capacity(queue, messages.len()).await?;

        let c = self.load_or_init_counters(queue).await?;
        let count = messages.len() as u64;

        // Reserve seq range + bump total_pushed in one locked critical
        // section. push_seq_committed does NOT advance here — claimers
        // can't see this range until the watermark catches up after this
        // guard drops (post-`db.write`).
        let _push_guard = Self::reserve_push_range(&c, count);
        let base_seq = _push_guard.base_seq;
        let (new_alloc, new_total_pushed) = {
            let mut s = c.state.lock().expect("queue state poisoned");
            s.total_pushed += count;
            (s.push_seq_alloc, s.total_pushed)
        };

        let mut batch = WriteBatch::new();
        for (i, msg) in messages.iter().enumerate() {
            let seq = base_seq + i as u64;
            batch.put(Self::msg_key(queue, &msg.msg_id), &serde_json::to_vec(msg)?);
            batch.put(Self::pending_key(queue, seq), msg.msg_id.as_bytes());
        }
        // Persist alloc cursor (monotonic across out-of-order commits) so
        // restart recovers all committed pending_keys.
        Self::persist_push_counters(&mut batch, queue, new_alloc, new_total_pushed);

        // Guard drops at end-of-scope — both on the Ok return below and on
        // the `?` propagation from `db.write` errors. No early-return path
        // can leak the reservation.
        self.db.write(batch).await?;
        Ok(())
    }

    /// Push a single message (convenience wrapper, used in tests)
    #[allow(dead_code)]
    pub async fn push_message(&self, queue: &str, msg: &Message) -> Result<(), StorageError> {
        self.push_messages(queue, std::slice::from_ref(msg)).await
    }

    // === Claim Operations (CAS loop, NO TRANSACTION) ===

    /// Claim messages from queue using CAS on claim_seq.
    ///
    /// Reserve a range of seqs and load their messages.
    ///
    /// **One critical section** for the reservation: lock the queue
    /// state, observe `(claim_seq, push_seq_committed)`, advance
    /// `claim_seq` and bump `total_claimed` by the reserved range,
    /// release. DB reads + batch build + `db.write` run unlocked.
    ///
    /// If some seqs in the reserved range had no `pending_key`
    /// committed (publish/nack writer in flight, or msg_data deleted),
    /// the over-bump on `total_claimed` is undone in a second brief
    /// lock. Brief inflation is the safe direction for `drained` checks.
    pub async fn claim_messages(
        &self,
        queue: &str,
        batch_size: usize,
        worker_id: &str,
        lease_id: &str,
    ) -> Result<Vec<ClaimedMessage>, StorageError> {
        let c = self.load_or_init_counters(queue).await?;

        // Reserve the seq range under one lock.
        let (start, end) = {
            let mut s = c.state.lock().expect("queue state poisoned");
            let cur = s.claim_seq;
            let lim = s.push_seq_committed;
            if cur >= lim {
                return Ok(Vec::new());
            }
            let target = std::cmp::min(cur + batch_size as u64, lim);
            let reserved = target - cur;
            s.claim_seq = target;
            // Bump total_claimed *together with* claim_seq so any reader
            // sees them consistently. Adjusted below if some seqs end up
            // empty (phantom claims).
            s.total_claimed += reserved;
            (cur, target)
        };

        // Read pending_keys + msg payloads (lock not held — DB reads).
        let mut claimed_items = Vec::new();
        for seq in start..end {
            let pending_key = Self::pending_key(queue, seq);
            if let Some(msg_id_bytes) = self.db.get(&pending_key).await? {
                let msg_id = String::from_utf8_lossy(&msg_id_bytes).to_string();
                if let Some(msg_data) = self.db.get(&Self::msg_key(queue, &msg_id)).await? {
                    let msg: Message = serde_json::from_slice(&msg_data)?;
                    let claim_info =
                        ClaimInfo::new(msg_id.clone(), worker_id.to_string(), lease_id.to_string());
                    claimed_items.push((pending_key, msg_id, msg, claim_info));
                }
            } else {
                tracing::warn!(
                    "claim: pending_key missing (publish-commit race, orphaned msg): \
                     queue={}, seq={}, claim_seq=[{},{}), push_seq_seen={}",
                    queue,
                    seq,
                    start,
                    end,
                    end
                );
            }
        }

        let actual_count = claimed_items.len() as u64;
        let reserved = end - start;

        // Persist the new total_claimed value computed under the reserve
        // lock, after correcting for any phantom seqs.
        let persisted_total_claimed = {
            let mut s = c.state.lock().expect("queue state poisoned");
            if actual_count < reserved {
                s.total_claimed -= reserved - actual_count;
            }
            s.total_claimed
        };

        if claimed_items.is_empty() {
            return Ok(Vec::new());
        }

        let mut batch = WriteBatch::new();
        let mut result = Vec::new();
        for (pending_key, msg_id, msg, claim_info) in claimed_items {
            batch.delete(&pending_key);
            batch.put(
                Self::claimed_key(queue, &msg_id),
                &serde_json::to_vec(&claim_info)?,
            );
            result.push(ClaimedMessage {
                message: msg,
                claim_token: claim_info.claim_token.clone(),
            });
        }
        batch.put(Self::seq_claim_key(queue), end.to_le_bytes());
        batch.put(
            Self::cnt_total_claimed_key(queue),
            persisted_total_claimed.to_le_bytes(),
        );
        self.db.write(batch).await?;

        Ok(result)
    }

    // === Ack Operations (NO TRANSACTION) ===

    /// Core ack operation with optional downstream push and state updates
    async fn ack_internal(
        &self,
        queue: &str,
        msg_ids: &[String],
        opts: AckOptions<'_>,
    ) -> Result<(), StorageError> {
        if msg_ids.is_empty() && opts.downstream_messages.is_none_or(|m| m.is_empty()) {
            return Ok(());
        }

        let claim_tokens = if msg_ids.is_empty() {
            None
        } else {
            let tokens = opts.claim_tokens.ok_or_else(|| {
                SlateError::invalid("claim_tokens is required for ack".to_string())
            })?;
            if tokens.len() != msg_ids.len() {
                return Err(Box::new(SlateError::invalid(format!(
                    "claim_tokens length mismatch (msg_ids={}, claim_tokens={})",
                    msg_ids.len(),
                    tokens.len()
                ))));
            }
            Some(tokens)
        };

        let expected_lease_id = opts.lease_id.filter(|v| !v.is_empty());
        let expected_worker_id = opts.worker_id.filter(|v| !v.is_empty());

        let now_ns = now_nanos();
        let ack_count = msg_ids.len() as u64;

        // 0. Check downstream capacity BEFORE any mutations (atomic counters
        //    are fetch_add — if we increment first and then fail on QueueFull,
        //    the counters are permanently corrupted).
        if let (Some(downstream_queue), Some(messages)) =
            (opts.downstream_queue, opts.downstream_messages)
        {
            if !messages.is_empty() {
                self.check_queue_capacity(downstream_queue, messages.len())
                    .await?;
            }
        }

        // For the downstream-push branch we need a reservation on the
        // downstream queue's seq range (same invariant as push_messages
        // and nack_messages_internal). The guard drops at end-of-scope so
        // the watermark advances even if any later step (validate_claims,
        // serde_json::to_vec, db.write) returns Err via `?`. This is the
        // bug that orphaned a transform_output batch in PR #84's CI run:
        // ack_and_forward had reserved the downstream range, validate_claims
        // returned a stale-token error, and the previous code never
        // closed the reservation — wedging the watermark behind it and
        // hiding every subsequent committed pending_key.
        let _downstream_push_guard: Option<PushReservationGuard> =
            match (opts.downstream_queue, opts.downstream_messages) {
                (Some(dq), Some(msgs)) if !msgs.is_empty() => {
                    let dc = self.load_or_init_counters(dq).await?;
                    Some(Self::reserve_push_range(&dc, msgs.len() as u64))
                }
                _ => None,
            };

        let mut batch = WriteBatch::new();

        // 1. Validate claims + move messages from claimed to acked
        if !msg_ids.is_empty() {
            self.validate_claims(
                queue,
                msg_ids,
                claim_tokens.unwrap(),
                expected_lease_id,
                expected_worker_id,
            )
            .await?;

            let c = self.load_or_init_counters(queue).await?;
            let (new_total_unclaimed, new_total_acked) = {
                let mut s = c.state.lock().expect("queue state poisoned");
                s.total_unclaimed += ack_count;
                s.total_acked += ack_count;
                (s.total_unclaimed, s.total_acked)
            };

            for msg_id in msg_ids {
                batch.delete(Self::claimed_key(queue, msg_id));
                batch.put(Self::acked_key(queue, now_ns, msg_id), []);
            }
            batch.put(
                Self::cnt_total_unclaimed_key(queue),
                new_total_unclaimed.to_le_bytes(),
            );
            batch.put(
                Self::cnt_total_acked_key(queue),
                new_total_acked.to_le_bytes(),
            );
        }

        // 2. Push downstream messages (capacity checked in step 0,
        //    seq range reserved via _downstream_push_guard above).
        if let (Some(downstream_queue), Some(messages)) =
            (opts.downstream_queue, opts.downstream_messages)
        {
            if !messages.is_empty() {
                let guard = _downstream_push_guard
                    .as_ref()
                    .expect("downstream guard set when we have messages");
                let dc = &guard.counters;
                let base_seq = guard.base_seq;
                let count = messages.len() as u64;
                let (new_alloc, new_total_pushed) = {
                    let mut s = dc.state.lock().expect("queue state poisoned");
                    s.total_pushed += count;
                    (s.push_seq_alloc, s.total_pushed)
                };

                for (i, msg) in messages.iter().enumerate() {
                    batch.put(
                        Self::msg_key(downstream_queue, &msg.msg_id),
                        &serde_json::to_vec(msg)?,
                    );
                    batch.put(
                        Self::pending_key(downstream_queue, base_seq + i as u64),
                        msg.msg_id.as_bytes(),
                    );
                }
                Self::persist_push_counters(
                    &mut batch,
                    downstream_queue,
                    new_alloc,
                    new_total_pushed,
                );
            }
        }

        // 3. Update state if provided
        if let Some(namespace) = opts.state_namespace {
            if let Some(puts) = opts.state_puts {
                for (key, value) in puts {
                    batch.put(Self::state_key(namespace, key), value);
                }
            }
            if let Some(deletes) = opts.state_deletes {
                for key in deletes {
                    batch.delete(Self::state_key(namespace, key));
                }
            }
        }

        // Guard drops at end-of-scope (on success or `?`-propagated error
        // from db.write), advancing the downstream watermark.
        self.db.write(batch).await?;
        Ok(())
    }

    /// Acknowledge messages (move to acked)
    pub async fn ack_messages(
        &self,
        queue: &str,
        msg_ids: &[String],
        claim_tokens: &[String],
        worker_id: &str,
        lease_id: &str,
    ) -> Result<(), StorageError> {
        self.ack_internal(
            queue,
            msg_ids,
            AckOptions {
                claim_tokens: Some(claim_tokens),
                worker_id: Some(worker_id),
                lease_id: Some(lease_id),
                ..Default::default()
            },
        )
        .await
    }

    /// Acknowledge with state updates
    #[allow(clippy::too_many_arguments)]
    pub async fn ack_with_state(
        &self,
        queue: &str,
        msg_ids: &[String],
        claim_tokens: &[String],
        worker_id: &str,
        lease_id: &str,
        namespace: &str,
        state_puts: &HashMap<String, Vec<u8>>,
        state_deletes: &[String],
    ) -> Result<(), StorageError> {
        self.ack_internal(
            queue,
            msg_ids,
            AckOptions {
                state_namespace: Some(namespace),
                state_puts: Some(state_puts),
                state_deletes: Some(state_deletes),
                claim_tokens: Some(claim_tokens),
                worker_id: Some(worker_id),
                lease_id: Some(lease_id),
                ..Default::default()
            },
        )
        .await
    }

    /// Acknowledge upstream and push to downstream
    #[allow(clippy::too_many_arguments)]
    pub async fn ack_and_forward(
        &self,
        upstream_queue: &str,
        upstream_msg_ids: &[String],
        upstream_claim_tokens: &[String],
        worker_id: &str,
        lease_id: &str,
        downstream_queue: &str,
        downstream_messages: &[Message],
    ) -> Result<(), StorageError> {
        self.ack_internal(
            upstream_queue,
            upstream_msg_ids,
            AckOptions {
                downstream_queue: Some(downstream_queue),
                downstream_messages: Some(downstream_messages),
                claim_tokens: Some(upstream_claim_tokens),
                worker_id: Some(worker_id),
                lease_id: Some(lease_id),
                ..Default::default()
            },
        )
        .await
    }

    /// Acknowledge upstream, push downstream, and update state
    #[allow(clippy::too_many_arguments)]
    pub async fn ack_forward_with_state(
        &self,
        upstream_queue: &str,
        upstream_msg_ids: &[String],
        upstream_claim_tokens: &[String],
        worker_id: &str,
        lease_id: &str,
        downstream_queue: &str,
        downstream_messages: &[Message],
        namespace: &str,
        state_puts: &HashMap<String, Vec<u8>>,
        state_deletes: &[String],
    ) -> Result<(), StorageError> {
        self.ack_internal(
            upstream_queue,
            upstream_msg_ids,
            AckOptions {
                downstream_queue: Some(downstream_queue),
                downstream_messages: Some(downstream_messages),
                state_namespace: Some(namespace),
                state_puts: Some(state_puts),
                state_deletes: Some(state_deletes),
                claim_tokens: Some(upstream_claim_tokens),
                worker_id: Some(worker_id),
                lease_id: Some(lease_id),
            },
        )
        .await
    }

    // === Nack Operations (NO TRANSACTION) ===

    /// Return messages to pending queue (at tail)
    pub async fn nack_messages(
        &self,
        queue: &str,
        msg_ids: &[String],
        claim_tokens: &[String],
        worker_id: &str,
        lease_id: &str,
    ) -> Result<(), StorageError> {
        self.nack_messages_internal(
            queue,
            msg_ids,
            Some(claim_tokens),
            Some(worker_id),
            Some(lease_id),
            None,
            None,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn nack_messages_with_state(
        &self,
        queue: &str,
        msg_ids: &[String],
        claim_tokens: &[String],
        worker_id: &str,
        lease_id: &str,
        state_namespace: &str,
        state_puts: &HashMap<String, Vec<u8>>,
        state_deletes: &[String],
    ) -> Result<(), StorageError> {
        self.nack_messages_internal(
            queue,
            msg_ids,
            Some(claim_tokens),
            Some(worker_id),
            Some(lease_id),
            Some(state_namespace),
            Some(state_puts),
            Some(state_deletes),
        )
        .await
    }

    /// Return messages to pending queue without claim validation (recovery path).
    pub async fn nack_messages_unchecked(
        &self,
        queue: &str,
        msg_ids: &[String],
    ) -> Result<(), StorageError> {
        self.nack_messages_internal(queue, msg_ids, None, None, None, None, None, None)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn nack_messages_internal(
        &self,
        queue: &str,
        msg_ids: &[String],
        claim_tokens: Option<&[String]>,
        worker_id: Option<&str>,
        lease_id: Option<&str>,
        state_namespace: Option<&str>,
        state_puts: Option<&HashMap<String, Vec<u8>>>,
        state_deletes: Option<&[String]>,
    ) -> Result<(), StorageError> {
        if msg_ids.is_empty() {
            return Ok(());
        }

        if let Some(tokens) = claim_tokens {
            if tokens.len() != msg_ids.len() {
                return Err(Box::new(SlateError::invalid(format!(
                    "claim_tokens length mismatch (msg_ids={}, claim_tokens={})",
                    msg_ids.len(),
                    tokens.len()
                ))));
            }
        }

        let expected_worker_id = worker_id.filter(|v| !v.is_empty());
        let expected_lease_id = lease_id.filter(|v| !v.is_empty());

        // Validate claim tokens via direct DB reads (no transaction needed)
        if let Some(tokens) = claim_tokens {
            self.validate_claims(
                queue,
                msg_ids,
                tokens,
                expected_lease_id,
                expected_worker_id,
            )
            .await?;
        }

        let c = self.load_or_init_counters(queue).await?;
        let nack_count = msg_ids.len() as u64;

        // Reserve seq range at the tail and register the pending commit.
        // Watermark stays put until this guard drops at end-of-scope.
        let _push_guard = Self::reserve_push_range(&c, nack_count);
        let base_seq = _push_guard.base_seq;
        let (new_alloc, new_unclaimed) = {
            let mut s = c.state.lock().expect("queue state poisoned");
            s.total_unclaimed += nack_count;
            (s.push_seq_alloc, s.total_unclaimed)
        };

        let mut batch = WriteBatch::new();
        for (i, msg_id) in msg_ids.iter().enumerate() {
            batch.delete(Self::claimed_key(queue, msg_id));
            batch.put(
                Self::pending_key(queue, base_seq + i as u64),
                msg_id.as_bytes(),
            );
        }
        // Persist alloc cursor so a recovery from disk picks up everything
        // committed (regardless of out-of-order commits).
        batch.put(Self::seq_push_key(queue), new_alloc.to_le_bytes());
        batch.put(
            Self::cnt_total_unclaimed_key(queue),
            new_unclaimed.to_le_bytes(),
        );

        // State updates
        let has_state_updates = state_namespace.is_some()
            && (state_puts.is_some_and(|puts| !puts.is_empty())
                || state_deletes.is_some_and(|deletes| !deletes.is_empty()));
        if has_state_updates {
            let namespace = state_namespace.unwrap();
            if let Some(puts) = state_puts {
                for (key, value) in puts {
                    batch.put(Self::state_key(namespace, key), value);
                }
            }
            if let Some(deletes) = state_deletes {
                for key in deletes {
                    batch.delete(Self::state_key(namespace, key));
                }
            }
        }

        // Guard drops at end-of-scope; on `?` propagation from db.write
        // the reservation is still closed out (just with no pending_key
        // committed for the missing batch — which is the correct
        // localization of a failed nack).
        self.db.write(batch).await?;
        Ok(())
    }

    // === Scan Operations ===

    /// Scan all claimed messages (optionally filtered by queue)
    pub async fn scan_claimed(
        &self,
        queue: Option<&str>,
    ) -> Result<Vec<(String, String, ClaimInfo)>, StorageError> {
        let prefix = match queue {
            Some(q) => format!("claimed:{}:", q).into_bytes(),
            None => b"claimed:".to_vec(),
        };

        let mut results = Vec::new();
        let mut iter = self.db.scan_prefix(&prefix).await?;

        while let Ok(Some(kv)) = iter.next().await {
            let key_str = String::from_utf8_lossy(&kv.key);
            let parts: Vec<&str> = key_str.split(':').collect();

            if parts.len() >= 3 {
                if let Ok(claim_info) = serde_json::from_slice::<ClaimInfo>(&kv.value) {
                    results.push((parts[1].to_string(), parts[2].to_string(), claim_info));
                }
            }
        }

        Ok(results)
    }

    /// Scan all acked messages (optionally filtered by queue)
    pub async fn scan_acked(
        &self,
        queue: Option<&str>,
    ) -> Result<Vec<(String, u64, String)>, StorageError> {
        let prefix = match queue {
            Some(q) => format!("acked:{}:", q).into_bytes(),
            None => b"acked:".to_vec(),
        };

        let mut results = Vec::new();
        let mut iter = self.db.scan_prefix(&prefix).await?;

        while let Ok(Some(kv)) = iter.next().await {
            let key_str = String::from_utf8_lossy(&kv.key);
            let parts: Vec<&str> = key_str.split(':').collect();

            if parts.len() >= 4 {
                if let Ok(timestamp_ns) = parts[2].parse::<u64>() {
                    results.push((parts[1].to_string(), timestamp_ns, parts[3].to_string()));
                }
            }
        }

        Ok(results)
    }

    // === Query Operations ===

    /// Get queue stats - pure in-memory from atomic counters
    pub async fn get_queue_stats(&self, queue: &str) -> Result<QueueMeta, StorageError> {
        self.get_meta(queue).await
    }

    // === Delete Operations ===

    pub async fn delete_queue(&self, queue: &str) -> Result<usize, StorageError> {
        let meta = self.get_meta(queue).await?;
        let claimed = self.scan_claimed(Some(queue)).await?;
        let acked = self.scan_acked(Some(queue)).await?;

        let mut batch = WriteBatch::new();
        let mut deleted = 0;

        // Delete new counter keys
        batch.delete(Self::seq_push_key(queue));
        batch.delete(Self::seq_claim_key(queue));
        batch.delete(Self::cnt_total_pushed_key(queue));
        batch.delete(Self::cnt_total_claimed_key(queue));
        batch.delete(Self::cnt_total_unclaimed_key(queue));
        batch.delete(Self::cnt_total_acked_key(queue));
        // Also delete old meta key (migration cleanup)
        batch.delete(Self::meta_key(queue));

        // Delete pending entries and messages
        for seq in meta.claim_seq..meta.push_seq {
            let pending_key = Self::pending_key(queue, seq);
            if let Some(msg_id_bytes) = self.db.get(&pending_key).await? {
                let msg_id = String::from_utf8_lossy(&msg_id_bytes).to_string();
                batch.delete(&pending_key);
                batch.delete(Self::msg_key(queue, &msg_id));
                deleted += 1;
            }
        }

        // Delete claimed entries and messages
        for (_, msg_id, _) in &claimed {
            batch.delete(Self::claimed_key(queue, msg_id));
            batch.delete(Self::msg_key(queue, msg_id));
            deleted += 1;
        }

        // Delete acked entries and messages
        for (_, ts, msg_id) in &acked {
            batch.delete(Self::acked_key(queue, *ts, msg_id));
            batch.delete(Self::msg_key(queue, msg_id));
            deleted += 1;
        }

        if deleted > 0 || !claimed.is_empty() || !acked.is_empty() {
            self.db.write(batch).await?;
        }

        // Remove from in-memory caches
        self.counters.remove(queue);
        self.max_pending_limits.remove(queue);

        Ok(deleted)
    }

    // === GC and Recovery ===

    pub async fn gc_acked_messages(&self, retention_ns: u64) -> Result<usize, StorageError> {
        let cutoff_ns = now_nanos().saturating_sub(retention_ns);
        let all_acked = self.scan_acked(None).await?;

        let mut batch = WriteBatch::new();
        let mut deleted = 0;

        for (queue, timestamp_ns, msg_id) in all_acked {
            if timestamp_ns < cutoff_ns {
                batch.delete(Self::acked_key(&queue, timestamp_ns, &msg_id));
                batch.delete(Self::msg_key(&queue, &msg_id));
                deleted += 1;
            }
        }

        if deleted > 0 {
            self.db.write(batch).await?;
            tracing::info!("GC deleted {} acked messages", deleted);
        }

        Ok(deleted)
    }

    /// Reclaim expired claims back to pending. Two independent timeouts:
    ///
    /// - `lease_timeout_secs`: how long a lease's heartbeat can lag before
    ///   the worker is considered dead. A lease is "alive" when its
    ///   `last_seen` is within this many seconds of `now`. Tunes
    ///   *worker-death detection*.
    /// - `claim_age_timeout_secs`: how long a single claim can be held
    ///   before being treated as stuck, even if the worker's lease still
    ///   appears alive. Tunes *task-duration SLA* and covers the lag
    ///   window between a Ray-level worker death and the broker noticing
    ///   the lease drop.
    ///
    /// Previously these were a single `timeout_secs` knob, which forced
    /// callers to pick one number that worked for both — e.g. the chaos
    /// tests had to use 10 s for both, even though task duration and
    /// dead-worker detection are completely different concerns. Splitting
    /// them lets each test (and production) tune them independently.
    pub async fn recover_expired_claims(
        &self,
        lease_timeout_secs: f64,
        claim_age_timeout_secs: f64,
        active_leases: Option<&HashMap<String, f64>>,
    ) -> Result<usize, StorageError> {
        let now = crate::types::now_secs();
        let all_claimed = self.scan_claimed(None).await?;

        // Group expired by queue
        let mut expired_by_queue: HashMap<String, Vec<ClaimInfo>> = HashMap::new();
        for (queue, msg_id, claim_info) in all_claimed {
            let lease_alive = if let Some(leases) = active_leases {
                if let Some(last_seen) = leases.get(&claim_info.lease_id) {
                    now - *last_seen <= lease_timeout_secs
                } else {
                    // Lease not in active set — worker is dead
                    false
                }
            } else {
                false
            };

            if lease_alive {
                // Live lease — only recover if the claim has been held longer
                // than `claim_age_timeout_secs`. This catches both a genuinely
                // stuck worker (alive but not making progress) AND the lag
                // window between a Ray-level worker death and the broker
                // noticing the lease drop.
                if now - claim_info.claimed_at > claim_age_timeout_secs {
                    let mut info = claim_info.clone();
                    info.msg_id = msg_id;
                    expired_by_queue.entry(queue).or_default().push(info);
                }
                continue;
            }

            // Dead lease: recover immediately regardless of claim age.
            // The old code also checked `now - claimed_at > timeout_secs` for dead
            // leases, creating a window where the message was stuck even though the
            // worker was definitely gone.  Waiting serves no purpose when the worker
            // is confirmed dead.
            tracing::info!(
                "Recovering dead-lease claim: queue={}, msg_id={}, worker={}, lease={}",
                queue,
                msg_id,
                claim_info.worker_id,
                claim_info.lease_id
            );
            let mut info = claim_info.clone();
            info.msg_id = msg_id;
            expired_by_queue.entry(queue).or_default().push(info);
        }

        let mut total = 0;
        for (queue, claims) in expired_by_queue {
            let msg_ids: Vec<String> = claims.iter().map(|c| c.msg_id.clone()).collect();
            self.nack_messages_unchecked(&queue, &msg_ids).await?;

            let mut puts = HashMap::new();
            let ts_ns = now_nanos();
            for claim in &claims {
                let key = format!("timeout:{}:{}:{}", queue, ts_ns, claim.msg_id);
                let event = serde_json::json!({
                    "event_type": "timeout",
                    "timestamp_ns": ts_ns,
                    "timestamp": now,
                    "queue": queue,
                    "msg_id": claim.msg_id,
                    "worker_id": claim.worker_id,
                    "lease_id": claim.lease_id,
                    "claimed_at": claim.claimed_at,
                    "input_rows": 0,
                    "input_bytes": 0,
                    "output_rows": 0,
                    "output_bytes": 0,
                    "processing_ms": 0,
                    "queue_wait_ms": 0,
                    "reason": "claim_timeout",
                });
                puts.insert(key, serde_json::to_vec(&event)?);
            }
            if !puts.is_empty() {
                let _ = self.state_put_batch("wq_events", &puts, &[]).await?;
            }

            total += msg_ids.len();
        }

        if total > 0 {
            tracing::info!("Recovered {} expired claims", total);
        }

        Ok(total)
    }

    // === State Operations ===

    pub async fn state_get_batch(
        &self,
        namespace: &str,
        keys: &[String],
    ) -> Result<HashMap<String, Vec<u8>>, StorageError> {
        let mut results = HashMap::new();
        for key in keys {
            if let Some(data) = self.db.get(&Self::state_key(namespace, key)).await? {
                results.insert(key.clone(), data.to_vec());
            }
        }
        Ok(results)
    }

    pub async fn state_put_batch(
        &self,
        namespace: &str,
        puts: &HashMap<String, Vec<u8>>,
        deletes: &[String],
    ) -> Result<(usize, usize), StorageError> {
        let mut batch = WriteBatch::new();

        for (key, value) in puts {
            batch.put(Self::state_key(namespace, key), value);
        }
        for key in deletes {
            batch.delete(Self::state_key(namespace, key));
        }

        self.db.write(batch).await?;
        Ok((puts.len(), deletes.len()))
    }

    pub async fn state_scan_prefix(
        &self,
        namespace: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let key_prefix = format!("state:{}:{}", namespace, prefix).into_bytes();
        let mut iter = self.db.scan_prefix(&key_prefix).await?;
        let mut results = Vec::new();
        let namespace_prefix = format!("state:{}:", namespace);

        while let Ok(Some(kv)) = iter.next().await {
            let key_str = String::from_utf8_lossy(&kv.key);
            if let Some(suffix) = key_str.strip_prefix(&namespace_prefix) {
                results.push((suffix.to_string(), kv.value.to_vec()));
                if limit > 0 && results.len() >= limit {
                    break;
                }
            }
        }

        Ok(results)
    }

    pub async fn list_queues(&self) -> Result<Vec<String>, StorageError> {
        // Scan new counter key prefix first
        let mut queues = Vec::new();
        let mut iter = self.db.scan_prefix(b"seq_push:").await?;
        while let Ok(Some(kv)) = iter.next().await {
            let key_str = String::from_utf8_lossy(&kv.key);
            if let Some(queue) = key_str.strip_prefix("seq_push:") {
                queues.push(queue.to_string());
            }
        }

        // Fallback: also scan old meta: prefix for migration
        let mut iter = self.db.scan_prefix(b"meta:").await?;
        let existing: std::collections::HashSet<String> = queues.iter().cloned().collect();
        while let Ok(Some(kv)) = iter.next().await {
            let key_str = String::from_utf8_lossy(&kv.key);
            if let Some(queue) = key_str.strip_prefix("meta:") {
                if !existing.contains(queue) {
                    queues.push(queue.to_string());
                }
            }
        }

        Ok(queues)
    }

    // === Queue Completion API ===

    /// Mark a queue as finished (no more messages will be pushed)
    pub async fn mark_queue_finished(&self, queue: &str) -> Result<(), StorageError> {
        self.db.put(&Self::finished_key(queue), b"1").await?;
        self.db.flush().await?;
        tracing::info!("Queue {} marked as finished", queue);
        Ok(())
    }

    /// Check if queue is finished (marked by upstream)
    pub async fn is_queue_finished(&self, queue: &str) -> Result<bool, StorageError> {
        Ok(self.db.get(&Self::finished_key(queue)).await?.is_some())
    }

    /// Check if queue is finished AND drained (safe for worker to exit)
    /// Returns (finished, drained, pending_count, claimed_count)
    pub async fn check_queue_completion(
        &self,
        queue: &str,
    ) -> Result<(bool, bool, u64, u64), StorageError> {
        let finished = self.is_queue_finished(queue).await?;
        let c = self.load_or_init_counters(queue).await?;

        // Single locked snapshot of all queue counters. Includes in-flight
        // push reservations (`push_seq_alloc`) so a writer mid-commit
        // doesn't transiently look drained.
        let (pending_count, claimed_count) = {
            let s = c.state.lock().expect("queue state poisoned");
            (s.pending_count(), s.claimed_count())
        };

        // A queue is drained only when explicitly marked finished AND fully empty.
        let drained = finished && pending_count == 0 && claimed_count == 0;

        Ok((finished, drained, pending_count, claimed_count))
    }

    /// Clear the finished flag (for queue reuse/testing)
    #[allow(dead_code)]
    pub async fn clear_queue_finished(&self, queue: &str) -> Result<(), StorageError> {
        self.db.delete(&Self::finished_key(queue)).await?;
        Ok(())
    }

    // =========================================================================
    // QueueGroup Operations
    // =========================================================================

    /// Get group metadata. Returns None if group doesn't exist.
    pub async fn get_group_meta(
        &self,
        group_name: &str,
    ) -> Result<Option<QueueGroupMeta>, StorageError> {
        match self.db.get(&Self::group_meta_key(group_name)).await? {
            Some(data) => Ok(Some(serde_json::from_slice(&data)?)),
            None => Ok(None),
        }
    }

    /// Create a queue group with N partition queues atomically.
    /// Idempotent: returns existing group if it already exists.
    /// `max_pending_per_partition`: 0 = unlimited (default), >0 = bounded partitions.
    pub async fn create_queue_group(
        &self,
        group_name: &str,
        num_partitions: u32,
        max_pending_per_partition: u64,
    ) -> Result<QueueGroupMeta, StorageError> {
        // Check for existing group
        if let Some(existing) = self.get_group_meta(group_name).await? {
            // Always set in-memory limits (survives idempotent create on persistent DB).
            if max_pending_per_partition > 0 {
                for queue_name in &existing.partition_queues {
                    self.max_pending_limits
                        .insert(queue_name.clone(), max_pending_per_partition);
                }
            }
            return Ok(existing);
        }

        let meta = QueueGroupMeta::new(group_name.to_string(), num_partitions);

        // Create group_meta + all partition queue counter keys in one WriteBatch
        let mut batch = WriteBatch::new();
        batch.put(
            Self::group_meta_key(group_name),
            &serde_json::to_vec(&meta)?,
        );
        for queue_name in &meta.partition_queues {
            Self::write_zero_counters(&mut batch, queue_name);
        }
        self.db.write(batch).await?;

        // Store max_pending limits for each partition queue
        if max_pending_per_partition > 0 {
            for queue_name in &meta.partition_queues {
                self.max_pending_limits
                    .insert(queue_name.clone(), max_pending_per_partition);
            }
        }

        tracing::info!(
            "Created queue group '{}' with {} partitions",
            group_name,
            num_partitions
        );
        Ok(meta)
    }

    /// Atomic ack upstream + push to multiple downstream partition queues.
    /// NO TRANSACTION — uses atomic counters + one big WriteBatch.
    #[allow(clippy::too_many_arguments)]
    pub async fn ack_and_scatter(
        &self,
        upstream_queue: &str,
        upstream_msg_ids: &[String],
        upstream_claim_tokens: &[String],
        worker_id: &str,
        lease_id: &str,
        group_name: &str,
        partition_payloads: &[(u32, Vec<Message>)],
        state_namespace: Option<&str>,
        state_puts: Option<&HashMap<String, Vec<u8>>>,
        state_deletes: Option<&[String]>,
    ) -> Result<Vec<String>, StorageError> {
        // Resolve group
        let group = self
            .get_group_meta(group_name)
            .await?
            .ok_or_else(|| SlateError::invalid(format!("Queue group not found: {}", group_name)))?;

        // Validate partition indices
        for (pid, _) in partition_payloads {
            if *pid >= group.num_partitions {
                return Err(Box::new(SlateError::invalid(format!(
                    "Partition {} out of range [0, {})",
                    pid, group.num_partitions
                ))));
            }
        }

        let expected_lease_id = if lease_id.is_empty() {
            None
        } else {
            Some(lease_id)
        };
        let expected_worker_id = if worker_id.is_empty() {
            None
        } else {
            Some(worker_id)
        };

        // 0. Check capacity for all downstream partition queues before any mutations
        for (pid, messages) in partition_payloads {
            if messages.is_empty() {
                continue;
            }
            let partition_queue = &group.partition_queues[*pid as usize];
            self.check_queue_capacity(partition_queue, messages.len())
                .await?;
        }

        let now_ns = now_nanos();
        let ack_count = upstream_msg_ids.len() as u64;
        let mut all_new_msg_ids = Vec::new();
        let mut batch = WriteBatch::new();

        // 1. Validate claims and ack upstream
        if !upstream_msg_ids.is_empty() {
            if upstream_claim_tokens.len() != upstream_msg_ids.len() {
                return Err(Box::new(SlateError::invalid(
                    "claim_tokens length must match msg_ids".to_string(),
                )));
            }

            self.validate_claims(
                upstream_queue,
                upstream_msg_ids,
                upstream_claim_tokens,
                expected_lease_id,
                expected_worker_id,
            )
            .await?;

            let uc = self.load_or_init_counters(upstream_queue).await?;
            let (new_total_unclaimed, new_total_acked) = {
                let mut s = uc.state.lock().expect("queue state poisoned");
                s.total_unclaimed += ack_count;
                s.total_acked += ack_count;
                (s.total_unclaimed, s.total_acked)
            };

            for msg_id in upstream_msg_ids {
                batch.delete(Self::claimed_key(upstream_queue, msg_id));
                batch.put(Self::acked_key(upstream_queue, now_ns, msg_id), []);
            }
            batch.put(
                Self::cnt_total_unclaimed_key(upstream_queue),
                new_total_unclaimed.to_le_bytes(),
            );
            batch.put(
                Self::cnt_total_acked_key(upstream_queue),
                new_total_acked.to_le_bytes(),
            );
        }

        // 2. Push to each partition queue. Each push goes through the
        //    reservation-guard pattern so the watermark advance happens
        //    post-commit, in lockstep with the other writers.
        let mut push_guards: Vec<PushReservationGuard> = Vec::new();
        for (pid, messages) in partition_payloads {
            if messages.is_empty() {
                continue;
            }
            let partition_queue = &group.partition_queues[*pid as usize];
            let dc = self.load_or_init_counters(partition_queue).await?;
            let msg_count = messages.len() as u64;

            let guard = Self::reserve_push_range(&dc, msg_count);
            let base_seq = guard.base_seq;
            let (new_alloc, new_total_pushed) = {
                let mut s = dc.state.lock().expect("queue state poisoned");
                s.total_pushed += msg_count;
                (s.push_seq_alloc, s.total_pushed)
            };
            push_guards.push(guard);

            for (i, msg) in messages.iter().enumerate() {
                let seq = base_seq + i as u64;
                batch.put(
                    Self::msg_key(partition_queue, &msg.msg_id),
                    &serde_json::to_vec(msg)?,
                );
                batch.put(
                    Self::pending_key(partition_queue, seq),
                    msg.msg_id.as_bytes(),
                );
                all_new_msg_ids.push(msg.msg_id.clone());
            }

            Self::persist_push_counters(&mut batch, partition_queue, new_alloc, new_total_pushed);
        }

        // 3. State updates
        if let Some(namespace) = state_namespace {
            if let Some(puts) = state_puts {
                for (key, value) in puts {
                    batch.put(Self::state_key(namespace, key), value);
                }
            }
            if let Some(deletes) = state_deletes {
                for key in deletes {
                    batch.delete(Self::state_key(namespace, key));
                }
            }
        }

        self.db.write(batch).await?;
        Ok(all_new_msg_ids)
    }

    /// Claim from a partition group (O(1) per call).
    #[allow(clippy::too_many_arguments)]
    pub async fn claim_from_group(
        &self,
        group_name: &str,
        batch_size: usize,
        worker_id: &str,
        lease_id: &str,
        assigned_partitions: &[u32],
        allow_steal: bool,
        steal_pending_threshold: u64,
    ) -> Result<(Vec<ClaimedMessage>, String, u32), StorageError> {
        let group = self
            .get_group_meta(group_name)
            .await?
            .ok_or_else(|| SlateError::invalid(format!("Queue group not found: {}", group_name)))?;

        // O(A) where A is small (2-4): try claim directly, no meta reads needed.
        if !assigned_partitions.is_empty() {
            let a_len = assigned_partitions.len() as u64;
            let rr_assigned = {
                let mut map = self.steal_rr.lock().unwrap();
                let key = format!("{}_assigned_{}", group_name, worker_id);
                let counter = map.entry(key).or_insert(0);
                let val = *counter;
                *counter = val.wrapping_add(1);
                val
            };
            for offset in 0..a_len {
                let idx = ((rr_assigned + offset) % a_len) as usize;
                let pid = assigned_partitions[idx];
                if (pid as usize) < group.partition_queues.len() {
                    let queue_name = &group.partition_queues[pid as usize];
                    let claimed = self
                        .claim_messages(queue_name, batch_size, worker_id, lease_id)
                        .await?;
                    if !claimed.is_empty() {
                        return Ok((claimed, queue_name.to_string(), pid));
                    }
                }
            }
        }

        // Steal: round-robin through unassigned partitions.
        if allow_steal {
            let total = group.partition_queues.len() as u64;
            if total > 0 {
                let rr = {
                    let mut map = self.steal_rr.lock().unwrap();
                    let counter = map.entry(group_name.to_string()).or_insert(0);
                    let val = *counter;
                    *counter = val.wrapping_add(1);
                    val
                };

                for offset in 0..total {
                    let pid = ((rr + offset) % total) as u32;
                    if assigned_partitions.contains(&pid) {
                        continue;
                    }
                    let queue_name = &group.partition_queues[pid as usize];
                    if steal_pending_threshold > 0 {
                        let meta = self.get_meta(queue_name).await?;
                        let pending = meta.push_seq.saturating_sub(meta.claim_seq);
                        if pending <= steal_pending_threshold {
                            continue;
                        }
                    }
                    let claimed = self
                        .claim_messages(queue_name, batch_size, worker_id, lease_id)
                        .await?;
                    if !claimed.is_empty() {
                        return Ok((claimed, queue_name.to_string(), pid));
                    }
                }
            }
        }

        // Nothing to claim
        Ok((Vec::new(), String::new(), 0))
    }

    /// Check if all queues in a group are finished and drained.
    pub async fn check_group_completion(
        &self,
        group_name: &str,
    ) -> Result<(bool, bool, Vec<(u32, u64, u64, bool)>), StorageError> {
        let group = self
            .get_group_meta(group_name)
            .await?
            .ok_or_else(|| SlateError::invalid(format!("Queue group not found: {}", group_name)))?;

        let mut all_finished = true;
        let mut all_drained = true;
        let mut partition_statuses = Vec::new();

        for (i, queue_name) in group.partition_queues.iter().enumerate() {
            let (finished, drained, pending, claimed) =
                self.check_queue_completion(queue_name).await?;
            if !finished {
                all_finished = false;
            }
            if !drained {
                all_drained = false;
            }
            partition_statuses.push((i as u32, pending, claimed, finished));
        }

        Ok((all_finished, all_drained, partition_statuses))
    }

    /// Get stats for all partitions in a group, with skew detection.
    pub async fn get_group_stats(
        &self,
        group_name: &str,
    ) -> Result<(QueueGroupMeta, Vec<(u32, QueueMeta)>), StorageError> {
        let group = self
            .get_group_meta(group_name)
            .await?
            .ok_or_else(|| SlateError::invalid(format!("Queue group not found: {}", group_name)))?;

        let mut stats = Vec::new();
        for (i, queue_name) in group.partition_queues.iter().enumerate() {
            let meta = self.get_meta(queue_name).await?;
            stats.push((i as u32, meta));
        }

        Ok((group, stats))
    }

    /// Mark all queues in a group as finished.
    pub async fn mark_group_finished(&self, group_name: &str) -> Result<u32, StorageError> {
        let group = self
            .get_group_meta(group_name)
            .await?
            .ok_or_else(|| SlateError::invalid(format!("Queue group not found: {}", group_name)))?;

        let mut batch = WriteBatch::new();
        for queue_name in &group.partition_queues {
            batch.put(Self::finished_key(queue_name), b"1");
        }
        self.db.write(batch).await?;

        // Clean up in-memory round-robin counters for this group
        let prefix = format!("{}_", group_name);
        {
            let mut map = self.steal_rr.lock().unwrap();
            map.retain(|k, _| !k.starts_with(&prefix) && k != group_name);
        }

        tracing::info!(
            "Marked all {} queues in group '{}' as finished",
            group.num_partitions,
            group_name
        );
        Ok(group.num_partitions)
    }
}

#[cfg(test)]
#[allow(clippy::await_holding_lock)] // SIM_TIME_LOCK is intentionally held across awaits to serialize time-sensitive tests
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    // ─────────────────────────────────────────────────────────────────────
    // Test timing constants — named values for what would otherwise be
    // magic numbers scattered across the concurrency tests.
    //
    // Two reasons to centralize:
    //   1. There used to be one `claim_timeout_secs` knob doing three jobs
    //      (lease freshness, claim age, derived heartbeat interval), and
    //      every test picked a different number trying to make the one
    //      knob fit its scenario. Splitting recover_expired_claims into
    //      `lease_timeout_secs` + `claim_age_timeout_secs` removed the
    //      conflation; these constants pin the conventions.
    //   2. The async deadlines in the concurrency tests are scenario-
    //      driven (how long should a finite test wait?) rather than
    //      semantic-driven; naming them makes it clear what's a
    //      production-meaningful number vs. a "give the runtime enough
    //      slack" number.
    // ─────────────────────────────────────────────────────────────────────

    /// Production default for `BrokerConfig::claim_timeout_secs` (60 s).
    /// The single-knob compatibility layer in `recovery.rs` uses this same
    /// value for both lease freshness and claim age — see the comment in
    /// `RecoveryTask::start`.
    const LEASE_TIMEOUT_SECS_PRODUCTION: f64 = 60.0;
    const CLAIM_AGE_TIMEOUT_SECS_PRODUCTION: f64 = 60.0;

    /// Aggressive lease-freshness timeout for fast-recovery tests:
    /// dead-lease branch fires immediately on missing leases, so this
    /// only matters for live-but-stale leases. In a unit test, leases are
    /// in-process so their `last_seen` is exactly accurate; nothing
    /// realistic ever falls in the live-but-stale band.
    const LEASE_TIMEOUT_SECS_TEST_FAST: f64 = 1.0;

    /// Claim-age timeout for live-lease claimers. Set high enough that a
    /// healthy claimer is *never* preempted, even on a slow CI runner
    /// under coverage instrumentation (where in-process claim/ack cycles
    /// were observed > 5 s in CI logs). The dead-worker test never relies
    /// on this knob: the dead lease is detected by absence from
    /// `active_leases`, which fires immediately regardless of claim age.
    /// So we can set this conservatively without blunting the test.
    const CLAIM_AGE_TIMEOUT_SECS_TEST_LIVE_SAFE: f64 = 60.0;

    /// Heartbeat offset to mark a test lease as "definitely fresh" — far
    /// future so any reasonable lease-freshness timeout passes.
    const LEASE_FAR_FUTURE_SECS: f64 = 1_000_000.0;

    /// Async deadlines for the concurrency tests. These bound how long
    /// the test waits for producers/claimers to drain; they are not
    /// modeling any production semantic.
    const CONCURRENCY_TEST_DRAIN_DEADLINE: Duration = Duration::from_secs(5);
    const HEAVY_CONCURRENCY_TEST_DRAIN_DEADLINE: Duration = Duration::from_secs(15);
    const RACE_REPRODUCER_TRIAL_DEADLINE: Duration = Duration::from_millis(500);

    /// Polling intervals inside the concurrency tests' inner loops.
    const TEST_BUSY_SLEEP: Duration = Duration::from_millis(1);
    const TEST_RECOVERY_TICK: Duration = Duration::from_millis(20);

    async fn create_temp_storage() -> AnvilStorage {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let temp_dir = std::env::temp_dir().join(format!("anvil_test_{}", counter));
        let _ = std::fs::remove_dir_all(&temp_dir);
        AnvilStorage::new(&format!("file://{}", temp_dir.display()))
            .await
            .unwrap()
    }

    fn split_claims(claimed: &[ClaimedMessage]) -> (Vec<String>, Vec<String>) {
        let msg_ids = claimed.iter().map(|c| c.message.msg_id.clone()).collect();
        let claim_tokens = claimed.iter().map(|c| c.claim_token.clone()).collect();
        (msg_ids, claim_tokens)
    }

    #[tokio::test]
    async fn test_push_and_claim() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue, 0).await.unwrap();

        let msg1 = Message::new(queue.to_string(), b"hello".to_vec());
        let msg2 = Message::new(queue.to_string(), b"world".to_vec());

        storage.push_message(queue, &msg1).await.unwrap();
        storage.push_message(queue, &msg2).await.unwrap();

        let meta = storage.get_meta(queue).await.unwrap();
        assert_eq!(meta.claim_seq, 0);
        assert_eq!(meta.push_seq, 2);

        let claimed = storage
            .claim_messages(queue, 2, "worker-1", "lease-1")
            .await
            .unwrap();
        assert_eq!(claimed.len(), 2);
        assert_eq!(claimed[0].message.payload, b"hello");
        assert_eq!(claimed[1].message.payload, b"world");

        let meta = storage.get_meta(queue).await.unwrap();
        assert_eq!(meta.claim_seq, 2);
        assert_eq!(meta.push_seq, 2);
    }

    #[tokio::test]
    async fn test_push_batch() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue, 0).await.unwrap();

        let messages: Vec<Message> = (0..5)
            .map(|i| Message::new(queue.to_string(), format!("msg{}", i).into_bytes()))
            .collect();

        storage.push_messages(queue, &messages).await.unwrap();

        let meta = storage.get_meta(queue).await.unwrap();
        assert_eq!(meta.push_seq, 5);

        let claimed = storage
            .claim_messages(queue, 5, "worker-1", "lease-1")
            .await
            .unwrap();
        assert_eq!(claimed.len(), 5);
    }

    #[tokio::test]
    async fn test_ack() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue, 0).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        storage.push_message(queue, &msg).await.unwrap();
        let claimed = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        let (msg_ids, claim_tokens) = split_claims(&claimed);
        storage
            .ack_messages(queue, &msg_ids, &claim_tokens, "worker-1", "lease-1")
            .await
            .unwrap();

        // Claim info should be gone, message in acked
        let claimed = storage.scan_claimed(Some(queue)).await.unwrap();
        assert!(claimed.is_empty());

        let acked = storage.scan_acked(Some(queue)).await.unwrap();
        assert_eq!(acked.len(), 1);
    }

    #[tokio::test]
    async fn test_nack() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue, 0).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        storage.push_message(queue, &msg).await.unwrap();
        let claimed = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        let (msg_ids, claim_tokens) = split_claims(&claimed);
        let msg_id = msg_ids[0].clone();

        let meta = storage.get_meta(queue).await.unwrap();
        assert_eq!(meta.claim_seq, 1);
        assert_eq!(meta.push_seq, 1);

        storage
            .nack_messages(queue, &msg_ids, &claim_tokens, "worker-1", "lease-1")
            .await
            .unwrap();

        let meta = storage.get_meta(queue).await.unwrap();
        assert_eq!(meta.claim_seq, 1);
        assert_eq!(meta.push_seq, 2);

        let claimed_again = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        assert_eq!(claimed_again.len(), 1);
        assert_eq!(claimed_again[0].message.msg_id, msg_id);
    }

    #[tokio::test]
    async fn test_ack_rejects_wrong_claim_token() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue, 0).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        storage.push_message(queue, &msg).await.unwrap();
        let claimed = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        let (msg_ids, _claim_tokens) = split_claims(&claimed);

        let result = storage
            .ack_messages(
                queue,
                &msg_ids,
                &[String::from("bad-token")],
                "worker-1",
                "lease-1",
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_ack_rejects_wrong_lease() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue, 0).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        storage.push_message(queue, &msg).await.unwrap();
        let claimed = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        let (msg_ids, claim_tokens) = split_claims(&claimed);

        let result = storage
            .ack_messages(queue, &msg_ids, &claim_tokens, "worker-1", "lease-bad")
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_nack_rejects_wrong_worker() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue, 0).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        storage.push_message(queue, &msg).await.unwrap();
        let claimed = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        let (msg_ids, claim_tokens) = split_claims(&claimed);

        let result = storage
            .nack_messages(queue, &msg_ids, &claim_tokens, "worker-2", "lease-1")
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_ack_and_forward_rejects_wrong_claim_token() {
        let storage = create_temp_storage().await;

        storage.create_queue("upstream", 0).await.unwrap();
        storage.create_queue("downstream", 0).await.unwrap();

        let upstream_msg = Message::new("upstream".to_string(), b"input".to_vec());
        storage
            .push_message("upstream", &upstream_msg)
            .await
            .unwrap();
        let claimed = storage
            .claim_messages("upstream", 1, "worker-1", "lease-1")
            .await
            .unwrap();

        let downstream_msgs: Vec<Message> =
            vec![Message::new("downstream".to_string(), b"out".to_vec())];

        let result = storage
            .ack_and_forward(
                "upstream",
                &[claimed[0].message.msg_id.clone()],
                &[String::from("bad-token")],
                "worker-1",
                "lease-1",
                "downstream",
                &downstream_msgs,
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_claim_token_changes_after_nack() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue, 0).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        storage.push_message(queue, &msg).await.unwrap();

        let claimed = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        let (msg_ids, claim_tokens) = split_claims(&claimed);

        storage
            .nack_messages(queue, &msg_ids, &claim_tokens, "worker-1", "lease-1")
            .await
            .unwrap();

        let claimed_again = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        assert_eq!(claimed_again.len(), 1);
        assert_ne!(claimed_again[0].claim_token, claim_tokens[0]);
    }

    #[tokio::test]
    async fn test_ack_rejects_stale_token_after_reclaim() {
        use crate::types::{advance_sim_time_secs, set_sim_time_nanos, SIM_TIME_LOCK};
        let _guard = SIM_TIME_LOCK.lock().unwrap();
        set_sim_time_nanos(1_900_000_000_000_000_000);

        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue, 0).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        storage.push_message(queue, &msg).await.unwrap();

        let claimed = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        let (msg_ids, claim_tokens) = split_claims(&claimed);

        // Advance sim time so claim is expired
        advance_sim_time_secs(1.0);
        // No active_leases → every claim is dead-lease → reclaimed regardless
        // of timeouts. Both timeouts at 0 to cover the no-grace-period case.
        let recovered = storage
            .recover_expired_claims(0.0, 0.0, None)
            .await
            .unwrap();
        assert_eq!(recovered, 1);

        let claimed_again = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        assert_eq!(claimed_again.len(), 1);

        let result = storage
            .ack_messages(queue, &msg_ids, &claim_tokens, "worker-1", "lease-1")
            .await;
        assert!(result.is_err());

        set_sim_time_nanos(0);
    }

    #[tokio::test]
    async fn test_recover_respects_active_leases() {
        use crate::types::{advance_sim_time_secs, set_sim_time_nanos, SIM_TIME_LOCK};
        let _guard = SIM_TIME_LOCK.lock().unwrap();
        set_sim_time_nanos(2_100_000_000_000_000_000);

        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue, 0).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        storage.push_message(queue, &msg).await.unwrap();
        let claimed = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();

        advance_sim_time_secs(1.0);

        let mut active = HashMap::new();
        active.insert("lease-1".to_string(), crate::types::now_secs());

        // Both timeouts comfortably above the 1 s elapsed: lease just
        // heartbeated AND claim is fresh, so recovery should leave it alone.
        let recovered = storage
            .recover_expired_claims(
                LEASE_TIMEOUT_SECS_PRODUCTION,
                CLAIM_AGE_TIMEOUT_SECS_PRODUCTION,
                Some(&active),
            )
            .await
            .unwrap();
        assert_eq!(recovered, 0);

        let still_claimed = storage.scan_claimed(Some(queue)).await.unwrap();
        assert_eq!(still_claimed.len(), 1);
        assert_eq!(still_claimed[0].1, claimed[0].message.msg_id);

        set_sim_time_nanos(0);
    }

    #[tokio::test]
    async fn test_queue_completion_empty_finished() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue, 0).await.unwrap();
        storage.mark_queue_finished(queue).await.unwrap();

        let (finished, drained, pending, claimed) =
            storage.check_queue_completion(queue).await.unwrap();
        assert!(finished);
        assert!(drained);
        assert_eq!(pending, 0);
        assert_eq!(claimed, 0);
    }

    #[tokio::test]
    async fn test_ack_and_forward() {
        let storage = create_temp_storage().await;

        storage.create_queue("upstream", 0).await.unwrap();
        storage.create_queue("downstream", 0).await.unwrap();

        let upstream_msg = Message::new("upstream".to_string(), b"input".to_vec());
        storage
            .push_message("upstream", &upstream_msg)
            .await
            .unwrap();
        let claimed = storage
            .claim_messages("upstream", 1, "worker-1", "lease-1")
            .await
            .unwrap();
        let (upstream_ids, upstream_tokens) = split_claims(&claimed);

        let downstream_msgs: Vec<Message> = (0..2)
            .map(|i| Message::new("downstream".to_string(), format!("out{}", i).into_bytes()))
            .collect();

        storage
            .ack_and_forward(
                "upstream",
                &upstream_ids,
                &upstream_tokens,
                "worker-1",
                "lease-1",
                "downstream",
                &downstream_msgs,
            )
            .await
            .unwrap();

        let acked = storage.scan_acked(Some("upstream")).await.unwrap();
        assert_eq!(acked.len(), 1);

        let meta = storage.get_meta("downstream").await.unwrap();
        assert_eq!(meta.push_seq, 2);

        let downstream_claimed = storage
            .claim_messages("downstream", 2, "worker-2", "lease-2")
            .await
            .unwrap();
        assert_eq!(downstream_claimed.len(), 2);
    }

    #[tokio::test]
    async fn test_gc_acked_messages() {
        use crate::types::{advance_sim_time_secs, set_sim_time_nanos, SIM_TIME_LOCK};
        let _guard = SIM_TIME_LOCK.lock().unwrap();
        set_sim_time_nanos(1_800_000_000_000_000_000);

        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue, 0).await.unwrap();

        for i in 0..5 {
            let msg = Message::new(queue.to_string(), format!("msg{}", i).into_bytes());
            storage.push_message(queue, &msg).await.unwrap();
            let claimed = storage
                .claim_messages(queue, 1, "worker-1", "lease-1")
                .await
                .unwrap();
            let (msg_ids, claim_tokens) = split_claims(&claimed);
            storage
                .ack_messages(queue, &msg_ids, &claim_tokens, "worker-1", "lease-1")
                .await
                .unwrap();
        }

        let acked = storage.scan_acked(Some(queue)).await.unwrap();
        assert_eq!(acked.len(), 5);

        // Advance time so messages are older than retention=0
        advance_sim_time_secs(1.0);

        let deleted = storage.gc_acked_messages(0).await.unwrap();
        assert_eq!(deleted, 5);

        let acked = storage.scan_acked(Some(queue)).await.unwrap();
        assert!(acked.is_empty());

        set_sim_time_nanos(0);
    }

    #[tokio::test]
    async fn test_queue_stats() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue, 0).await.unwrap();

        for i in 0..5 {
            let msg = Message::new(queue.to_string(), format!("msg{}", i).into_bytes());
            storage.push_message(queue, &msg).await.unwrap();
        }

        let meta = storage.get_queue_stats(queue).await.unwrap();
        let pending = meta.push_seq.saturating_sub(meta.claim_seq);
        assert_eq!(pending, 5);
        assert_eq!(meta.claimed_count, 0);
        assert_eq!(meta.total_pushed, 5);

        storage
            .claim_messages(queue, 3, "worker-1", "lease-1")
            .await
            .unwrap();

        let meta = storage.get_queue_stats(queue).await.unwrap();
        let pending = meta.push_seq.saturating_sub(meta.claim_seq);
        assert_eq!(pending, 2);
        assert_eq!(meta.claimed_count, 3);
    }

    #[tokio::test]
    async fn test_state_operations() {
        let storage = create_temp_storage().await;
        let namespace = "job1/stage1";

        let mut puts = HashMap::new();
        puts.insert("key1".to_string(), b"value1".to_vec());
        puts.insert("key2".to_string(), b"value2".to_vec());

        storage
            .state_put_batch(namespace, &puts, &[])
            .await
            .unwrap();

        let keys = vec!["key1".to_string(), "key2".to_string(), "key3".to_string()];
        let values = storage.state_get_batch(namespace, &keys).await.unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values.get("key1"), Some(&b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_ack_with_state() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";
        let namespace = "job1/stage1";

        storage.create_queue(queue, 0).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());

        storage.push_message(queue, &msg).await.unwrap();
        let claimed = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        let (msg_ids, claim_tokens) = split_claims(&claimed);

        let mut state_puts = HashMap::new();
        state_puts.insert("seen_key".to_string(), b"1".to_vec());

        storage
            .ack_with_state(
                queue,
                &msg_ids,
                &claim_tokens,
                "worker-1",
                "lease-1",
                namespace,
                &state_puts,
                &[],
            )
            .await
            .unwrap();

        let values = storage
            .state_get_batch(namespace, &["seen_key".to_string()])
            .await
            .unwrap();
        assert_eq!(values.get("seen_key"), Some(&b"1".to_vec()));
    }

    #[tokio::test]
    async fn test_delete_queue() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue, 0).await.unwrap();

        for i in 0..5 {
            let msg = Message::new(queue.to_string(), format!("msg{}", i).into_bytes());
            storage.push_message(queue, &msg).await.unwrap();
        }
        storage
            .claim_messages(queue, 2, "worker-1", "lease-1")
            .await
            .unwrap();

        let deleted = storage.delete_queue(queue).await.unwrap();
        assert!(deleted > 0);

        let meta = storage.get_meta(queue).await.unwrap();
        assert_eq!(meta.claim_seq, 0);
        assert_eq!(meta.push_seq, 0);
    }

    #[tokio::test]
    async fn test_recover_expired_claims() {
        use crate::types::{advance_sim_time_secs, set_sim_time_nanos, SIM_TIME_LOCK};
        let _guard = SIM_TIME_LOCK.lock().unwrap();
        set_sim_time_nanos(2_000_000_000_000_000_000);

        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue, 0).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        storage.push_message(queue, &msg).await.unwrap();
        storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();

        advance_sim_time_secs(1.0);
        // No active leases at all → every claim falls into the dead-lease
        // branch; both timeouts irrelevant.
        let recovered = storage
            .recover_expired_claims(0.0, 0.0, None)
            .await
            .unwrap();
        assert_eq!(recovered, 1);

        let claimed = storage
            .claim_messages(queue, 1, "worker-2", "lease-2")
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);

        set_sim_time_nanos(0);
    }

    #[tokio::test]
    async fn test_counter_correctness_full_lifecycle() {
        // Verify O(1) counters are maintained correctly through full message lifecycle
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue, 0).await.unwrap();

        // Initial state
        let meta = storage.get_queue_stats(queue).await.unwrap();
        assert_eq!(meta.claimed_count, 0);
        assert_eq!(meta.total_pushed, 0);
        assert_eq!(meta.total_acked, 0);

        // Push 10 messages
        let mut msg_ids = Vec::new();
        for i in 0..10 {
            let msg = Message::new(queue.to_string(), format!("msg{}", i).into_bytes());
            msg_ids.push(msg.msg_id.clone());
            storage.push_message(queue, &msg).await.unwrap();
        }

        let meta = storage.get_queue_stats(queue).await.unwrap();
        assert_eq!(meta.claimed_count, 0, "No messages claimed yet");
        assert_eq!(meta.total_pushed, 10, "10 messages pushed");
        assert_eq!(meta.total_acked, 0, "No messages acked yet");

        // Claim 5 messages
        let claimed = storage
            .claim_messages(queue, 5, "worker-1", "lease-1")
            .await
            .unwrap();
        assert_eq!(claimed.len(), 5);
        let (claimed_ids, claimed_tokens) = split_claims(&claimed);

        let meta = storage.get_queue_stats(queue).await.unwrap();
        assert_eq!(meta.claimed_count, 5, "5 messages claimed");
        assert_eq!(meta.total_pushed, 10);
        assert_eq!(meta.total_acked, 0);

        // Ack 3 messages
        let ack_ids: Vec<String> = claimed_ids[0..3].to_vec();
        let ack_tokens: Vec<String> = claimed_tokens[0..3].to_vec();
        storage
            .ack_messages(queue, &ack_ids, &ack_tokens, "worker-1", "lease-1")
            .await
            .unwrap();

        let meta = storage.get_queue_stats(queue).await.unwrap();
        assert_eq!(meta.claimed_count, 2, "5 - 3 = 2 claimed");
        assert_eq!(meta.total_pushed, 10);
        assert_eq!(meta.total_acked, 3, "3 messages acked");

        // Nack 2 messages (return to pending)
        let nack_ids: Vec<String> = claimed_ids[3..5].to_vec();
        let nack_tokens: Vec<String> = claimed_tokens[3..5].to_vec();
        storage
            .nack_messages(queue, &nack_ids, &nack_tokens, "worker-1", "lease-1")
            .await
            .unwrap();

        let meta = storage.get_queue_stats(queue).await.unwrap();
        assert_eq!(meta.claimed_count, 0, "All claimed messages handled");
        assert_eq!(meta.total_pushed, 10);
        assert_eq!(meta.total_acked, 3);

        // Verify pending count: 10 original - 5 claimed + 2 nacked back = 7 pending
        let pending = meta.push_seq.saturating_sub(meta.claim_seq);
        assert_eq!(pending, 7, "7 messages pending (5 unclaimed + 2 nacked)");
    }

    // =========================================================================
    // Concurrency tests — guard against the publish-commit race documented
    // in docs/lessons/anvil-publish-commit-race.md.
    //
    // Invariant: every message that has been push_messages'd (or
    // nack_messages_unchecked'd back to pending) must be claimable at some
    // point after the writer returns. The race arises because push_seq is
    // bumped via fetch_add *before* the WriteBatch containing the
    // pending_key commits; a concurrent claimer can observe the new
    // push_seq, CAS claim_seq past the reserved range, read an empty
    // pending_key, and silently skip — orphaning the msg forever.
    // =========================================================================

    /// Concurrency invariant: every msg passed to `push_messages` must
    /// eventually be claimable. Before the fix to the publish-commit
    /// race, this test reproduced ~8% loss reliably (8 pushers × 8
    /// claimers × 200 msgs → "claimed 1472 of 1600"). After serializing
    /// `fetch_add(push_seq) + db.write` under a per-queue write lock
    /// and gating the claim-side `read push_seq + CAS claim_seq` under
    /// the matching read lock, no message is orphaned. See
    /// `docs/lessons/anvil-publish-commit-race.md`.
    #[tokio::test]
    async fn test_concurrent_push_claim_accounts_for_every_message() {
        let storage = Arc::new(create_temp_storage().await);
        let queue = "concurrent-push-claim";
        storage.create_queue(queue, 0).await.unwrap();

        const PUSHERS: usize = 8;
        const CLAIMERS: usize = 8;
        const PER_PUSHER: u64 = 200;
        const TOTAL: u64 = (PUSHERS as u64) * PER_PUSHER;

        let claimed_ids: Arc<tokio::sync::Mutex<std::collections::HashSet<String>>> =
            Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::new()));

        // Pushers
        let mut push_handles = Vec::new();
        for p in 0..PUSHERS {
            let storage = storage.clone();
            push_handles.push(tokio::spawn(async move {
                let msgs: Vec<Message> = (0..PER_PUSHER)
                    .map(|i| Message::new(queue.to_string(), format!("p{p}-msg{i}").into_bytes()))
                    .collect();
                // Chunk a bit so we interleave with claimers rather than one big batch.
                for chunk in msgs.chunks(16) {
                    storage.push_messages(queue, chunk).await.unwrap();
                    tokio::task::yield_now().await;
                }
            }));
        }

        // Claimers — keep draining + acking until they've seen all TOTAL msgs.
        let mut claim_handles = Vec::new();
        for w in 0..CLAIMERS {
            let storage = storage.clone();
            let claimed_ids = claimed_ids.clone();
            let worker_id = format!("claimer-{w}");
            let lease_id = format!("claimer-{w}-lease");
            claim_handles.push(tokio::spawn(async move {
                let deadline = std::time::Instant::now() + CONCURRENCY_TEST_DRAIN_DEADLINE;
                while std::time::Instant::now() < deadline {
                    let batch = storage
                        .claim_messages(queue, 32, &worker_id, &lease_id)
                        .await
                        .unwrap();
                    if batch.is_empty() {
                        let seen = claimed_ids.lock().await.len() as u64;
                        if seen >= TOTAL {
                            break;
                        }
                        tokio::time::sleep(TEST_BUSY_SLEEP).await;
                        continue;
                    }
                    let (msg_ids, claim_tokens) = split_claims(&batch);
                    {
                        let mut s = claimed_ids.lock().await;
                        for id in &msg_ids {
                            s.insert(id.clone());
                        }
                    }
                    storage
                        .ack_messages(queue, &msg_ids, &claim_tokens, &worker_id, &lease_id)
                        .await
                        .unwrap();
                }
            }));
        }

        for h in push_handles {
            h.await.unwrap();
        }
        for h in claim_handles {
            h.await.unwrap();
        }

        let seen = claimed_ids.lock().await.len() as u64;
        assert_eq!(
            seen, TOTAL,
            "concurrent push/claim lost messages: claimed {seen} of {TOTAL} pushed"
        );

        let meta = storage.get_queue_stats(queue).await.unwrap();
        assert_eq!(meta.total_pushed, TOTAL, "counter: total_pushed");
        assert_eq!(meta.total_acked, TOTAL, "counter: total_acked");
    }

    /// Regression guard for the publish-commit race on the recovery path.
    /// Pre-push N, claim as a "dead worker" (no ack), then concurrently
    /// nack (mirrors `recover_expired_claims`) against 16 active claimers
    /// across 20 trials. Before the fix this reproduced 100% loss every
    /// trial (4000/4000 orphaned). After the fix all messages are
    /// guaranteed claimable.
    #[tokio::test]
    async fn test_nack_claim_race_no_orphaned_messages() {
        const TRIALS: usize = 20;
        const PER_TRIAL: u64 = 200;
        const CLAIMERS: usize = 16;

        let mut total_loss: u64 = 0;
        let mut trials_with_loss = 0usize;
        for trial in 0..TRIALS {
            let loss = run_nack_claim_trial(PER_TRIAL, CLAIMERS).await;
            if loss > 0 {
                eprintln!(
                    "trial {trial}: {loss} / {PER_TRIAL} messages orphaned by push-commit race"
                );
                trials_with_loss += 1;
            }
            total_loss += loss;
        }

        assert_eq!(
            total_loss, 0,
            "publish-commit race: {total_loss} messages orphaned across \
             {trials_with_loss}/{TRIALS} trials (expected 0 once the race is fixed; \
             see docs/lessons/anvil-publish-commit-race.md)",
        );
    }

    async fn run_nack_claim_trial(n: u64, claimers: usize) -> u64 {
        let storage = Arc::new(create_temp_storage().await);
        let queue = "nack-claim-race";
        storage.create_queue(queue, 0).await.unwrap();

        // Push N, claim all as a dead worker (no ack).
        let messages: Vec<Message> = (0..n)
            .map(|i| Message::new(queue.to_string(), format!("msg{i}").into_bytes()))
            .collect();
        storage.push_messages(queue, &messages).await.unwrap();

        let dead = storage
            .claim_messages(queue, n as usize, "dead", "dead-lease")
            .await
            .unwrap();
        assert_eq!(dead.len(), n as usize, "failed to claim all upfront");
        let dead_ids: Vec<String> = dead.iter().map(|c| c.message.msg_id.clone()).collect();
        let dead_set: std::collections::HashSet<String> = dead_ids.iter().cloned().collect();

        // Use a barrier so nack + claimers start near-simultaneously.
        let barrier = Arc::new(tokio::sync::Barrier::new(claimers + 1));
        let claimed: Arc<tokio::sync::Mutex<std::collections::HashSet<String>>> =
            Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::new()));

        let mut handles = Vec::new();
        for w in 0..claimers {
            let storage = storage.clone();
            let barrier = barrier.clone();
            let claimed = claimed.clone();
            let worker_id = format!("claimer-{w}");
            let lease_id = format!("claimer-{w}-lease");
            handles.push(tokio::spawn(async move {
                barrier.wait().await;
                let deadline = std::time::Instant::now() + RACE_REPRODUCER_TRIAL_DEADLINE;
                while std::time::Instant::now() < deadline {
                    let batch = storage
                        .claim_messages(queue, 16, &worker_id, &lease_id)
                        .await
                        .unwrap();
                    if !batch.is_empty() {
                        let mut s = claimed.lock().await;
                        for c in &batch {
                            s.insert(c.message.msg_id.clone());
                        }
                    }
                    tokio::task::yield_now().await;
                }
            }));
        }

        // Reclaim task: nack all the dead worker's claimed msgs unchecked,
        // exactly as recover_expired_claims would.
        let reclaim = {
            let storage = storage.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                storage
                    .nack_messages_unchecked(queue, &dead_ids)
                    .await
                    .unwrap();
            })
        };

        reclaim.await.unwrap();
        for h in handles {
            h.await.unwrap();
        }

        let claimed_set = claimed.lock().await;
        dead_set.difference(&claimed_set).count() as u64
    }

    /// Concurrent ack_and_forward: 1:1 transform stage. K workers each
    /// claim from upstream and atomically ack-upstream + push-downstream.
    /// Exercises the *third* push-side write path (ack_internal's
    /// downstream-push branch) which has the same publish-commit invariant
    /// as push_messages and nack_messages_internal.
    ///
    /// Invariant: every msg pushed to upstream lands in downstream exactly
    /// once. With the watermark fix, downstream claimers must observe a
    /// `push_seq` that always reflects committed `pending_key` entries.
    #[tokio::test]
    async fn test_concurrent_ack_and_forward_no_loss() {
        let storage = Arc::new(create_temp_storage().await);
        let upstream = "ack-fwd-upstream";
        let downstream = "ack-fwd-downstream";
        storage.create_queue(upstream, 0).await.unwrap();
        storage.create_queue(downstream, 0).await.unwrap();

        const PRODUCERS: usize = 4;
        const TRANSFORMERS: usize = 8;
        const DOWNSTREAM_CLAIMERS: usize = 4;
        const PER_PRODUCER: u64 = 200;
        const TOTAL: u64 = (PRODUCERS as u64) * PER_PRODUCER;

        // Pushers: produce TOTAL messages onto upstream.
        let mut producers = Vec::new();
        for p in 0..PRODUCERS {
            let storage = storage.clone();
            producers.push(tokio::spawn(async move {
                let msgs: Vec<Message> = (0..PER_PRODUCER)
                    .map(|i| {
                        Message::new(upstream.to_string(), format!("p{p}-msg{i}").into_bytes())
                    })
                    .collect();
                for chunk in msgs.chunks(8) {
                    storage.push_messages(upstream, chunk).await.unwrap();
                    tokio::task::yield_now().await;
                }
            }));
        }

        // Transformers: claim from upstream + ack_and_forward to downstream.
        // Track msg-id correspondences so we can verify 1:1 conservation.
        let forwarded: Arc<tokio::sync::Mutex<std::collections::HashSet<String>>> =
            Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::new()));

        let mut transformers = Vec::new();
        for t in 0..TRANSFORMERS {
            let storage = storage.clone();
            let forwarded = forwarded.clone();
            let worker_id = format!("xform-{t}");
            let lease_id = format!("xform-{t}-lease");
            transformers.push(tokio::spawn(async move {
                let deadline = std::time::Instant::now() + HEAVY_CONCURRENCY_TEST_DRAIN_DEADLINE;
                while std::time::Instant::now() < deadline {
                    let batch = storage
                        .claim_messages(upstream, 16, &worker_id, &lease_id)
                        .await
                        .unwrap();
                    if batch.is_empty() {
                        if forwarded.lock().await.len() as u64 >= TOTAL {
                            break;
                        }
                        tokio::time::sleep(TEST_BUSY_SLEEP).await;
                        continue;
                    }
                    let upstream_ids: Vec<String> =
                        batch.iter().map(|c| c.message.msg_id.clone()).collect();
                    let upstream_tokens: Vec<String> =
                        batch.iter().map(|c| c.claim_token.clone()).collect();
                    // 1:1 — wrap each upstream msg into a downstream msg.
                    let downstream_msgs: Vec<Message> = batch
                        .iter()
                        .map(|c| Message::new(downstream.to_string(), c.message.payload.clone()))
                        .collect();

                    storage
                        .ack_and_forward(
                            upstream,
                            &upstream_ids,
                            &upstream_tokens,
                            &worker_id,
                            &lease_id,
                            downstream,
                            &downstream_msgs,
                        )
                        .await
                        .unwrap();

                    let mut f = forwarded.lock().await;
                    for d in &downstream_msgs {
                        f.insert(d.msg_id.clone());
                    }
                }
            }));
        }

        // Downstream claimers: drain downstream and count.
        let downstream_seen: Arc<tokio::sync::Mutex<std::collections::HashSet<String>>> =
            Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::new()));
        let mut downstream_handles = Vec::new();
        for w in 0..DOWNSTREAM_CLAIMERS {
            let storage = storage.clone();
            let downstream_seen = downstream_seen.clone();
            let worker_id = format!("dn-{w}");
            let lease_id = format!("dn-{w}-lease");
            downstream_handles.push(tokio::spawn(async move {
                let deadline = std::time::Instant::now() + HEAVY_CONCURRENCY_TEST_DRAIN_DEADLINE;
                while std::time::Instant::now() < deadline {
                    let batch = storage
                        .claim_messages(downstream, 32, &worker_id, &lease_id)
                        .await
                        .unwrap();
                    if batch.is_empty() {
                        if downstream_seen.lock().await.len() as u64 >= TOTAL {
                            break;
                        }
                        tokio::time::sleep(TEST_BUSY_SLEEP).await;
                        continue;
                    }
                    let ids: Vec<String> = batch.iter().map(|c| c.message.msg_id.clone()).collect();
                    let tokens: Vec<String> = batch.iter().map(|c| c.claim_token.clone()).collect();
                    {
                        let mut s = downstream_seen.lock().await;
                        for id in &ids {
                            s.insert(id.clone());
                        }
                    }
                    storage
                        .ack_messages(downstream, &ids, &tokens, &worker_id, &lease_id)
                        .await
                        .unwrap();
                }
            }));
        }

        for p in producers {
            p.await.unwrap();
        }
        for t in transformers {
            t.await.unwrap();
        }
        for h in downstream_handles {
            h.await.unwrap();
        }

        let forwarded = forwarded.lock().await;
        let downstream_seen = downstream_seen.lock().await;
        assert_eq!(
            forwarded.len() as u64,
            TOTAL,
            "transform stage forwarded {} of {} upstream msgs",
            forwarded.len(),
            TOTAL,
        );
        assert_eq!(
            downstream_seen.len() as u64,
            TOTAL,
            "downstream claimers saw {} of {} forwarded msgs",
            downstream_seen.len(),
            TOTAL,
        );
        assert_eq!(
            *forwarded, *downstream_seen,
            "forwarded set must equal downstream-seen set (no msg lost or duplicated)",
        );
    }

    /// Realistic chaos scenario: producers stream msgs while one batch of
    /// "dead" workers claims and never acks. Background reclaim runs with
    /// `active_leases` *excluding* the dead workers (the production
    /// pattern from `recover_expired_claims`), so live workers' in-flight
    /// claims are respected and only the dead workers' claims are nacked.
    /// After everything settles, every msg must end up acked.
    ///
    /// This exercises three concurrent code paths simultaneously:
    /// `push_messages`, `nack_messages_internal` (via reclaim), and
    /// `claim_messages` — covering all of the publish-commit
    /// invariant's writer side.
    #[tokio::test]
    async fn test_dead_worker_recovery_under_concurrent_pushes_and_claims() {
        let storage = Arc::new(create_temp_storage().await);
        let queue = "dead-worker-race";
        storage.create_queue(queue, 0).await.unwrap();

        const PRODUCERS: usize = 4;
        const LIVE_CLAIMERS: usize = 6;
        const PER_PRODUCER: u64 = 200;
        const TOTAL: u64 = (PRODUCERS as u64) * PER_PRODUCER;

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let acked: Arc<tokio::sync::Mutex<std::collections::HashSet<String>>> =
            Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::new()));

        // Pre-claim as a "dead worker": grab a small batch and never ack.
        // Reclaim will need to nack these back to pending. We do this
        // before producers start so the dead claim is one of the very
        // first reservations on the queue.
        let mut producer_handles = Vec::new();
        for p in 0..PRODUCERS {
            let storage = storage.clone();
            producer_handles.push(tokio::spawn(async move {
                for batch_idx in 0..(PER_PRODUCER / 10) {
                    let msgs: Vec<Message> = (0..10)
                        .map(|i| {
                            Message::new(
                                queue.to_string(),
                                format!("p{p}-b{batch_idx}-m{i}").into_bytes(),
                            )
                        })
                        .collect();
                    storage.push_messages(queue, &msgs).await.unwrap();
                    tokio::task::yield_now().await;
                }
            }));
        }

        // Dead worker — claim something, never ack.
        let dead_claim_task = {
            let storage = storage.clone();
            tokio::spawn(async move {
                // Wait briefly for some msgs to be available.
                tokio::time::sleep(TEST_RECOVERY_TICK).await;
                let _ = storage
                    .claim_messages(queue, 50, "dead-worker", "dead-lease")
                    .await
                    .unwrap();
                // Never ack. The lease "dead-lease" will not be in
                // active_leases when reclaim runs, so reclaim treats this
                // worker as gone and nacks its claims.
            })
        };

        // Reclaim task: passes only live claimers' leases as active. The
        // dead worker's lease is absent → its claims are reclaimed.
        let reclaim_handle = {
            let storage = storage.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                let mut active = HashMap::<String, f64>::new();
                for w in 0..LIVE_CLAIMERS {
                    active.insert(
                        format!("claimer-{w}-lease"),
                        crate::types::now_secs() + LEASE_FAR_FUTURE_SECS,
                    );
                }
                while !stop.load(std::sync::atomic::Ordering::Acquire) {
                    // Lease-timeout: short, so a missing lease (= dead worker)
                    // is recovered immediately by the dead-lease branch.
                    // Claim-age timeout: long enough to never fire on a
                    // healthy live claimer (worst-case ack latency in this
                    // test is sub-millisecond), but small enough that an
                    // entire test run can fit comfortably inside it.
                    let _ = storage
                        .recover_expired_claims(
                            LEASE_TIMEOUT_SECS_TEST_FAST,
                            CLAIM_AGE_TIMEOUT_SECS_TEST_LIVE_SAFE,
                            Some(&active),
                        )
                        .await
                        .unwrap();
                    tokio::time::sleep(TEST_RECOVERY_TICK).await;
                }
            })
        };

        // Live claimers: claim, ack, repeat. Always succeed (no contention
        // with reclaim because reclaim respects their leases).
        let mut claim_handles = Vec::new();
        for w in 0..LIVE_CLAIMERS {
            let storage = storage.clone();
            let acked = acked.clone();
            let worker_id = format!("claimer-{w}");
            let lease_id = format!("claimer-{w}-lease");
            claim_handles.push(tokio::spawn(async move {
                let deadline = std::time::Instant::now() + HEAVY_CONCURRENCY_TEST_DRAIN_DEADLINE;
                while std::time::Instant::now() < deadline {
                    if acked.lock().await.len() as u64 >= TOTAL {
                        break;
                    }
                    let batch = storage
                        .claim_messages(queue, 16, &worker_id, &lease_id)
                        .await
                        .unwrap();
                    if batch.is_empty() {
                        tokio::time::sleep(TEST_BUSY_SLEEP).await;
                        continue;
                    }
                    let ids: Vec<String> = batch.iter().map(|c| c.message.msg_id.clone()).collect();
                    let tokens: Vec<String> = batch.iter().map(|c| c.claim_token.clone()).collect();
                    // Under heavy CI scheduling pressure (cargo-llvm-cov
                    // can stretch a sub-ms ack into a multi-second one),
                    // a recovery cycle may catch a healthy live claim
                    // whose age happened to cross the
                    // `claim_age_timeout_secs` line and reclaim it out
                    // from under us. Production workers handle this
                    // benign race by dropping the stale token and
                    // letting the next claimer pick the msg up; the
                    // test does the same. The msg isn't lost — it's
                    // just owned by someone else now, and the final
                    // assertion (every produced msg is in the acked
                    // set) covers that.
                    if storage
                        .ack_messages(queue, &ids, &tokens, &worker_id, &lease_id)
                        .await
                        .is_ok()
                    {
                        let mut s = acked.lock().await;
                        for id in &ids {
                            s.insert(id.clone());
                        }
                    }
                }
            }));
        }

        for p in producer_handles {
            p.await.unwrap();
        }
        dead_claim_task.await.unwrap();
        for h in claim_handles {
            h.await.unwrap();
        }
        stop.store(true, std::sync::atomic::Ordering::Release);
        reclaim_handle.await.unwrap();

        let acked_set = acked.lock().await;
        assert_eq!(
            acked_set.len() as u64,
            TOTAL,
            "dead-worker recovery race: {} of {} msgs acked — rest orphaned by \
             push/nack/claim publish-commit race",
            acked_set.len(),
            TOTAL,
        );
    }
}
