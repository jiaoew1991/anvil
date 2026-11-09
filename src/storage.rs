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
// The keys and values it stores are the storage format, specified in
// docs/storage-format.md and built by `keys` and `format` alone.
//
// Hot paths (push, claim, ack, nack) use in-memory atomic counters
// with CAS loops instead of SlateDB SerializableSnapshot transactions.
// This eliminates transaction conflicts at high concurrency.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use dashmap::{DashMap, DashSet};
use slatedb::{Db, Error as SlateError, MergeOperator, MergeOperatorError, WriteBatch};

use crate::format::{self, FormatError, FORMAT_VERSION};
use crate::keys;
use crate::state::lease_expired;
use crate::types::{mono_now, now_nanos, ClaimInfo, ClaimedMessage, Message, QueueGroupMeta};

/// Upper bound on keys one GC write deletes, so a large expired backlog is
/// retired in many small commits instead of one batch that stalls writers.
pub(crate) const GC_BATCH_KEYS_MAX: usize = 10_000;

/// Upper bound on keys one GC pass deletes; the next pass resumes the rest.
pub(crate) const GC_PASS_KEYS_MAX: usize = 1_000_000;

pub type StorageError = Box<dyn std::error::Error + Send + Sync>;

/// A request refused because of what the caller asked, not because storage
/// failed: retrying it unchanged fails the same way. It travels as a
/// `StorageError` and is recognized by type, so the service can tell it from
/// a failure of the broker itself, which the caller may retry.
#[derive(Debug)]
pub enum Rejection {
    /// The caller no longer holds the claim it settles or renews: the
    /// message was settled, reclaimed or redelivered, or another settlement
    /// holds it right now. The worker must drop the message.
    Fenced(String),
    /// The queue or queue group does not exist.
    NotFound(String),
    /// The request contradicts itself, such as claim tokens that do not pair
    /// up with message ids.
    InvalidArgument(String),
    /// A bounded queue has no room for the messages pushed.
    QueueFull(String),
}

impl std::fmt::Display for Rejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Rejection::Fenced(message)
            | Rejection::NotFound(message)
            | Rejection::InvalidArgument(message)
            | Rejection::QueueFull(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for Rejection {}

/// Whether `error` comes from a closed database: fenced by a newer writer,
/// shut down, or stopped by a failed background task. A closed SlateDB
/// instance never opens again, so nothing retried against it can succeed.
pub fn is_closed(error: &StorageError) -> bool {
    error
        .downcast_ref::<SlateError>()
        .is_some_and(|e| matches!(e.kind(), slatedb::ErrorKind::Closed(_)))
}

/// Refuses a state request without a namespace. Any other namespace,
/// ':' and NUL included, is stored apart from every other (see `keys`).
pub fn check_state_namespace(namespace: &str) -> Result<(), Rejection> {
    if namespace.is_empty() {
        return Err(Rejection::InvalidArgument(
            "state without namespace".to_string(),
        ));
    }
    Ok(())
}

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
///
/// Counters that add work (`push_seq_alloc`, `claim_seq`, `total_pushed`,
/// `total_claimed`) move when a write is reserved, and counters that retire
/// it move once its batch has landed (see `QueueWrite`), so a queue never
/// looks drained while a write is in flight.
#[derive(Debug, Default, Clone)]
pub struct QueueState {
    /// Committed push watermark — claimers' upper bound. Only advanced
    /// *after* a writer's `db.write(batch)` returns, so any seq below
    /// `push_seq_committed` has its `pending_key` applied.
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
    /// Seqs below `claim_seq` whose claim was abandoned before its write, by
    /// a cancelled request or a failed read: their pending keys still wait,
    /// so claims take them before advancing `claim_seq`.
    pub returned: BTreeSet<u64>,
    /// Total messages ever pushed.
    pub total_pushed: u64,
    /// Monotonic count of msgs that entered claimed state.
    pub total_claimed: u64,
    /// Monotonic count of msgs that left claimed state (ack OR nack).
    pub total_unclaimed: u64,
    /// Total messages ever acked.
    pub total_acked: u64,
    /// In-flight reservations keyed by `base_seq`. Each entry's `done`
    /// flips when the matching `QueueWrite` is dropped (after its write,
    /// landed or not). The committer walks the front of the map and
    /// advances `push_seq_committed` through every contiguous-done entry.
    pub commit_log: BTreeMap<u64, PushReservation>,
    /// Writes admitted and not yet finished (see `QueueWrite`).
    pub writers: u64,
    /// Set by `delete_queue`, which then waits for `writers` to reach zero:
    /// no further write is admitted.
    pub deleted: bool,
}

impl QueueState {
    /// Pending count includes in-flight reservations. Use this for
    /// "is there work?" semantics (stage master, drained check).
    pub fn pending_count(&self) -> u64 {
        self.push_seq_alloc.saturating_sub(self.claim_seq) + self.returned.len() as u64
    }

    /// Currently-claimed count.
    pub fn claimed_count(&self) -> u64 {
        self.total_claimed.saturating_sub(self.total_unclaimed)
    }
}

/// Per-queue counters / state holder: the `QueueState` behind its mutex.
#[derive(Default)]
pub struct QueueCounters {
    pub state: std::sync::Mutex<QueueState>,
    /// Notified when the last write admitted to a deleted queue finishes.
    idle: tokio::sync::Notify,
}

impl QueueCounters {
    /// Turns away every new write and waits for those admitted to finish.
    async fn close(&self) {
        loop {
            let idle = self.idle.notified();
            tokio::pin!(idle);
            // Registered before the check, so the last writer's notification
            // cannot slip in between.
            idle.as_mut().enable();
            {
                let mut s = self.state.lock().expect("queue state poisoned");
                s.deleted = true;
                if s.writers == 0 {
                    return;
                }
            }
            idle.await;
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct PushReservation {
    pub count: u64,
    pub done: bool,
}

/// One operation's write to one queue, from its admission to the end of its
/// batch's write. `delete_queue` waits until every admitted write finished,
/// so none lands after the delete and leaves keys under counters that no
/// longer describe them.
///
/// Dropping it finishes the write in one critical section, on every exit
/// path: an `Ok` return, a `?` error, a panic, or the request future
/// being cancelled. It closes its push reservation (the watermark must not
/// wedge behind it), returns a claim abandoned before its write, and, only if
/// the batch landed, retires the claims it settled.
struct QueueWrite {
    counters: Arc<QueueCounters>,
    /// Base seq of the range this write fills at the tail of pending.
    push_base: Option<u64>,
    /// Seqs this claim reserved and has not yet handed to its write.
    claimed: Vec<u64>,
    /// Claims this write acks, retired once it lands.
    acked: u64,
    /// Claims this write acks or requeues, retired once it lands.
    unclaimed: u64,
    /// Set by `commit` once the batch is applied.
    landed: bool,
}

impl QueueWrite {
    /// Admits a write to the queue of `counters`, unless the queue is being
    /// deleted.
    fn admit(counters: Arc<QueueCounters>) -> Option<Self> {
        {
            let mut s = counters.state.lock().expect("queue state poisoned");
            if s.deleted {
                return None;
            }
            s.writers += 1;
        }
        Some(Self {
            counters,
            push_base: None,
            claimed: Vec::new(),
            acked: 0,
            unclaimed: 0,
            landed: false,
        })
    }

    /// Reserves `count` seqs at the tail of pending for what `tail` puts
    /// there and returns the first. Claimers see the range once the write
    /// finishes.
    fn reserve(&mut self, count: u64, tail: Tail) -> u64 {
        assert!(self.push_base.is_none(), "one push reservation per write");
        assert!(count > 0);
        let mut s = self.counters.state.lock().expect("queue state poisoned");
        match tail {
            Tail::Pushed => s.total_pushed += count,
            Tail::Requeued => self.unclaimed += count,
        }
        let base = s.push_seq_alloc;
        s.push_seq_alloc += count;
        s.commit_log
            .insert(base, PushReservation { count, done: false });
        self.push_base = Some(base);
        base
    }

    /// Records `count` claims this write acks.
    fn ack(&mut self, count: u64) {
        self.acked += count;
        self.unclaimed += count;
    }

    /// Reserves up to `batch_size` pending seqs, abandoned ones first. They
    /// count as claimed at once, so the queue never looks drained while the
    /// claim reads them; dropping the write before `hand_over_claims`
    /// returns them.
    fn claim(&mut self, batch_size: usize) -> Vec<u64> {
        assert!(self.claimed.is_empty());
        let mut s = self.counters.state.lock().expect("queue state poisoned");
        while self.claimed.len() < batch_size {
            let Some(seq) = s.returned.pop_first() else {
                break;
            };
            self.claimed.push(seq);
        }
        assert!(s.claim_seq <= s.push_seq_committed);
        let fresh =
            ((batch_size - self.claimed.len()) as u64).min(s.push_seq_committed - s.claim_seq);
        self.claimed.extend(s.claim_seq..s.claim_seq + fresh);
        s.claim_seq += fresh;
        s.total_claimed += self.claimed.len() as u64;
        self.claimed.clone()
    }

    /// Hands the claim's `written` messages to its batch, which is about to
    /// be committed. The other reserved seqs held no message (a pending key
    /// or body gone), so they stop counting as claimed.
    fn hand_over_claims(&mut self, written: usize) {
        assert!(written <= self.claimed.len());
        let empty = (self.claimed.len() - written) as u64;
        if empty > 0 {
            self.counters
                .state
                .lock()
                .expect("queue state poisoned")
                .total_claimed -= empty;
        }
        self.claimed.clear();
    }
}

impl Drop for QueueWrite {
    fn drop(&mut self) {
        let mut s = match self.counters.state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        if self.landed {
            s.total_acked += self.acked;
            s.total_unclaimed += self.unclaimed;
        }
        let abandoned = self.claimed.len() as u64;
        assert!(s.claimed_count() >= abandoned);
        s.total_claimed -= abandoned;
        s.returned.extend(self.claimed.drain(..));
        if let Some(base) = self.push_base {
            s.commit_log
                .get_mut(&base)
                .expect("reservation in the commit log")
                .done = true;
            // Walk the contiguous-done prefix from the front.
            loop {
                let front = s.commit_log.first_key_value().map(|(&k, &e)| (k, e));
                match front {
                    Some((k, PushReservation { count, done: true }))
                        if k == s.push_seq_committed =>
                    {
                        s.push_seq_committed = k + count;
                        s.commit_log.remove(&k);
                    }
                    _ => break,
                }
            }
        }
        assert!(s.writers > 0);
        s.writers -= 1;
        if s.deleted && s.writers == 0 {
            self.counters.idle.notify_waiters();
        }
    }
}

/// What a batch releases once its write is over, applied or not. `commit`
/// owns it until then, so a caller dropped mid-write cannot release it early.
#[derive(Default)]
struct Landing {
    writes: Vec<QueueWrite>,
    settlement: Option<SettlementHold>,
    /// `delete_queue`'s hold on `counters_loading`.
    loading: Option<tokio::sync::OwnedMutexGuard<()>>,
}

/// What a push reservation puts at the tail of pending.
#[derive(Clone, Copy)]
enum Tail {
    /// New messages.
    Pushed,
    /// Claims the write returns to pending.
    Requeued,
}

/// How far `commit` waits for a batch.
#[derive(Clone, Copy)]
enum Wait {
    /// Until it would survive a crash: every RPC that reports success.
    Durable,
    /// Until it is applied, visible to every later read.
    Applied,
}

/// Queue metadata for O(1) operations (return type for get_queue_stats)
#[derive(Debug, Clone, Default)]
pub struct QueueMeta {
    /// The claim cursor less the seqs returned behind it, so that
    /// `push_seq - claim_seq` counts every claimable message.
    pub claim_seq: u64,
    pub push_seq: u64,
    /// Number of currently claimed messages (O(1) stats)
    pub claimed_count: u64,
    /// Total messages ever pushed (lifetime counter)
    pub total_pushed: u64,
    /// Total messages ever acked (lifetime counter)
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

/// Sums the u64 increments merged into a queue's lifetime totals
/// (`keys::total_pushed`, `keys::total_acked`). Each operation merges its own
/// increment in its own batch, so the durable total is exactly the sum of
/// the durable operations, whatever order concurrent writers reach the WAL
/// in; a written absolute value would let an older one land last.
struct TotalSumMergeOperator;

impl MergeOperator for TotalSumMergeOperator {
    fn merge(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        value: Bytes,
    ) -> Result<Bytes, MergeOperatorError> {
        self.merge_batch(key, existing_value, std::slice::from_ref(&value))
    }

    fn merge_batch(
        &self,
        key: &Bytes,
        existing_value: Option<Bytes>,
        operands: &[Bytes],
    ) -> Result<Bytes, MergeOperatorError> {
        // Any other value is no u64 to sum: a merge on one is a bug that
        // must fail the write rather than corrupt the value.
        if !keys::is_total(key) {
            return Err(MergeOperatorError::Callback {
                message: format!("merge on {:02x?}, which is not a total", key.as_ref()),
            });
        }
        let decode = |value: &Bytes| {
            format::decode_total(value).map_err(|e| MergeOperatorError::Callback {
                message: format!("total {:02x?}: {e}", key.as_ref()),
            })
        };
        let mut total = match &existing_value {
            Some(value) => decode(value)?,
            None => 0,
        };
        for operand in operands {
            total += decode(operand)?;
        }
        Ok(Bytes::copy_from_slice(&format::encode_total(total)))
    }
}

/// A migration of the store from one format to the next. It must be
/// idempotent and resumable after a crash at any point, and write the marker
/// of its target format as its last durable write; docs/storage-format.md
/// lays out the procedure.
type Migration =
    for<'a> fn(&'a AnvilStorage) -> futures::future::BoxFuture<'a, Result<(), StorageError>>;

/// `MIGRATIONS[i]` migrates format `i + 1` to format `i + 2`. Its length is
/// what makes the chain cover every version: bumping `FORMAT_VERSION`
/// without adding the migration to it does not compile.
const MIGRATIONS: [Migration; FORMAT_VERSION as usize - 1] = [];

/// Anvil storage backed by SlateDB with in-memory atomic counters
pub struct AnvilStorage {
    db: Db,
    /// Per-queue atomic counters (in-memory cache, persisted to DB on each op)
    counters: DashMap<String, Arc<QueueCounters>>,
    /// Held while counters missing from `counters` are loaded from, or
    /// created in, the database. Each queue then has a single set of
    /// counters, and nothing writes its keys while they are derived from
    /// them: a loader racing a writer could see a later push land before an
    /// earlier one and derive a claim cursor past the push cursor, and a late
    /// zero write could reset totals another writer had merged.
    counters_loading: Arc<tokio::sync::Mutex<()>>,
    /// Per-group round-robin counters for O(1) steal path in claim_from_group
    steal_rr: std::sync::Mutex<HashMap<String, u64>>,
    /// Bounds of bounded queues; an unbounded queue has no entry. Mirrors the
    /// persisted `max_pending:{queue}` keys of the queues loaded so far.
    max_pending_limits: DashMap<String, u64>,
    /// Claims, as claim key and the token presented for it, whose
    /// settlement (ack, nack or reclaim) is between its check and its
    /// write. Without the hold, two settlements of one message both
    /// pass the check: a retried ack counts twice, and a reclaim that read
    /// the claim before an ack requeues an acked message, which undercounts
    /// `claimed_count` until a queue reports drained with work still in
    /// flight. The token is part of the key so that a request presenting a
    /// stale one never holds the claim its owner settles: of the
    /// settlements of one message, only those presenting its current token
    /// can pass the check, and the token cannot change while one holds it.
    /// The broker is the database's only writer, so an in-memory set is
    /// enough.
    settling: Arc<DashSet<(Vec<u8>, String)>>,
    /// When each claim, by claim token, was taken, on the monotonic clock.
    /// Claim age is timed from here rather than from the persisted
    /// `claimed_at`, which is wall clock and would let a clock step age a
    /// claim at once or never. Kept in memory only: a restarted broker has
    /// no live leases, so it reclaims every earlier claim regardless of age.
    /// Recovery prunes tokens that no longer hold a claim.
    taken_at: DashMap<String, Instant>,
    /// When each claim, by claim token, was last renewed by its worker, on
    /// the monotonic clock. A claim under a live lease is stuck once neither
    /// its taking nor its last renewal is within `claim_age_timeout_secs`.
    /// In memory and pruned like `taken_at`.
    renewed_at: DashMap<String, Instant>,
}

/// Claims held for settlement, released when dropped: once the write is
/// applied, on an error, or when the request is cancelled before its write.
struct SettlementHold {
    settling: Arc<DashSet<(Vec<u8>, String)>>,
    claims: Vec<(Vec<u8>, String)>,
}

impl SettlementHold {
    fn new(settling: Arc<DashSet<(Vec<u8>, String)>>) -> Self {
        Self {
            settling,
            claims: Vec::new(),
        }
    }

    /// Takes the claim under `token` unless another settlement presenting
    /// the same token holds it.
    fn take(&mut self, claim_key: Vec<u8>, token: &str) -> bool {
        let claim = (claim_key, token.to_string());
        if !self.settling.insert(claim.clone()) {
            return false;
        }
        self.claims.push(claim);
        true
    }
}

impl Drop for SettlementHold {
    fn drop(&mut self) {
        for claim in &self.claims {
            let released = self.settling.remove(claim);
            assert!(released.is_some(), "settlement hold released twice");
        }
    }
}

/// The claims an operation settles, held and verified, and the write that
/// settles them.
struct Settlement {
    write: QueueWrite,
    hold: SettlementHold,
    claims: Vec<ClaimInfo>,
}

impl AnvilStorage {
    pub async fn new(db_path: &str) -> Result<Self, StorageError> {
        // SlateDB resolves only the store root (scheme and authority, e.g.
        // `s3://bucket`); the path inside it is given to the builder.
        let mut store_url = url::Url::parse(db_path)
            .map_err(|e| SlateError::invalid(format!("invalid db_path {db_path}: {e}")))?;
        let db_root = store_url.path().to_string();
        store_url.set_path("");
        let object_store = Db::resolve_object_store(store_url.as_str())?;
        Self::open(db_root, object_store).await
    }

    /// Opens the database at `db_root` inside `object_store`, refusing or
    /// migrating it as its format requires (see `open_format`). Tests reach
    /// the store through this to put a fault-injecting one under the broker.
    pub(crate) async fn open(
        db_root: String,
        object_store: Arc<dyn slatedb::object_store::ObjectStore>,
    ) -> Result<Self, StorageError> {
        let db = Db::builder(db_root, object_store)
            .with_merge_operator(Arc::new(TotalSumMergeOperator))
            .build()
            .await?;
        let storage = Self {
            db,
            counters: DashMap::new(),
            counters_loading: Arc::new(tokio::sync::Mutex::new(())),
            steal_rr: std::sync::Mutex::new(HashMap::new()),
            max_pending_limits: DashMap::new(),
            settling: Arc::new(DashSet::new()),
            taken_at: DashMap::new(),
            renewed_at: DashMap::new(),
        };
        if let Err(e) = storage.open_format().await {
            // A refused store is left as it was found, and closed, so the
            // caller can open it again with a release that reads it.
            if let Err(close) = storage.db.close().await {
                tracing::warn!("closing a store refused at open: {close}");
            }
            return Err(e);
        }
        Ok(storage)
    }

    /// Brings the store to `FORMAT_VERSION` before anything else reads it:
    /// stamps an empty store, migrates an older one, and refuses one written
    /// by a newer release or by an unversioned one. The marker is the
    /// smallest key there is, so the first key of the store tells all three
    /// apart.
    async fn open_format(&self) -> Result<(), StorageError> {
        let first = self.db.scan(..).await?.next().await?;
        let mut version = match first {
            None => {
                // One put, so a crash leaves the store empty or stamped.
                let mut batch = WriteBatch::new();
                batch.put(keys::FORMAT, format::encode_format(FORMAT_VERSION));
                return self.commit(batch, Wait::Durable, Landing::default()).await;
            }
            Some(kv) if kv.key == keys::FORMAT => format::decode_format(&kv.value)?,
            Some(_) => return Err(Box::new(FormatError::Unversioned)),
        };
        if version > FORMAT_VERSION {
            return Err(Box::new(FormatError::Newer { version }));
        }
        while version < FORMAT_VERSION {
            MIGRATIONS[version as usize - 1](self).await?;
            let migrated = match self.db.get(keys::FORMAT).await? {
                Some(marker) => format::decode_format(&marker)?,
                None => 0,
            };
            assert_eq!(
                migrated,
                version + 1,
                "the migration from format {version} must leave the marker at {}",
                version + 1
            );
            version = migrated;
        }
        Ok(())
    }

    /// Writes `batch` and then waits as `wait` says. SlateDB's `write`
    /// returns once the batch is applied: in the in-memory WAL and memtable,
    /// visible to every later read. Every anvil RPC must not report success
    /// before its effect would survive a crash, so all but a claim then wait
    /// for durability.
    ///
    /// The write runs in a task of its own, which drops `landing` once the
    /// batch is applied, after marking its queue writes landed. The task is
    /// what makes a request cancel-safe. A client deadline drops the
    /// handler's future at its next await, and `Db::write` may or may not
    /// apply a batch whose future is dropped; awaited in the caller, the
    /// write could be applied after the holds and reservations it relies on
    /// were released, or be cut off after they were spent. The task finishes
    /// both together whatever happens to the caller. What memory reflects is
    /// what every read sees, so a caller cut off while waiting for
    /// durability leaves nothing to undo.
    ///
    /// A failed write retires nothing (see `QueueState`): SlateDB retries
    /// object-store errors itself and fails a write only for a malformed
    /// batch or a Db closed or fenced, which serves nothing more; a new
    /// broker derives every counter from the keys.
    async fn commit(
        &self,
        batch: WriteBatch,
        wait: Wait,
        landing: Landing,
    ) -> Result<(), StorageError> {
        let db = self.db.clone();
        let handle = tokio::spawn(async move {
            let Landing {
                mut writes,
                settlement,
                loading,
            } = landing;
            #[allow(clippy::disallowed_methods)] // the one sanctioned write path
            let written = db.write(batch).await;
            if written.is_ok() {
                for write in &mut writes {
                    write.landed = true;
                }
            }
            // The counters first: a settlement or load let in by a release
            // must find them up to date.
            drop(writes);
            drop(settlement);
            drop(loading);
            written
        })
        .await??;
        match wait {
            Wait::Durable => handle.await_durable().await?,
            Wait::Applied => {}
        }
        Ok(())
    }

    /// Close the storage gracefully.
    #[allow(dead_code)]
    pub async fn close(&self) -> Result<(), StorageError> {
        self.db.close().await?;
        Ok(())
    }

    /// Admits a write to `queue`, creating the queue on first use. A write
    /// that finds the queue being deleted retries: `delete_queue` removed
    /// the counters it found before closing them, so the retry waits for
    /// `counters_loading` and sees the queue as the delete left it.
    async fn admit(&self, queue: &str) -> Result<QueueWrite, StorageError> {
        loop {
            if let Some(write) = QueueWrite::admit(self.load_or_init_counters(queue).await?) {
                return Ok(write);
            }
        }
    }

    /// Admits a write to `queue` like `admit`, or `None` if the queue does
    /// not exist, which this never creates.
    async fn admit_existing(&self, queue: &str) -> Result<Option<QueueWrite>, StorageError> {
        loop {
            let Some(counters) = self.load_counters(queue).await? else {
                return Ok(None);
            };
            if let Some(write) = QueueWrite::admit(counters) {
                return Ok(Some(write));
            }
        }
    }

    /// Admits a write to `queue`, then holds the claims of `msg_ids` and
    /// verifies each one's token, lease and worker. The caller settles them
    /// through the returned write while it keeps the hold, which must outlive
    /// its write: a claim that is already being settled under the
    /// same token fails the check like a stale token.
    async fn validate_claims(
        &self,
        queue: &str,
        msg_ids: &[String],
        claim_tokens: &[String],
        expected_lease_id: Option<&str>,
        expected_worker_id: Option<&str>,
    ) -> Result<Settlement, StorageError> {
        assert!(!msg_ids.is_empty());
        let write = self.admit_existing(queue).await?.ok_or_else(|| {
            Rejection::Fenced(format!(
                "message_not_claimed: queue={queue} does not exist, msg_id={}",
                msg_ids[0]
            ))
        })?;
        let mut hold = SettlementHold::new(self.settling.clone());
        let mut claims = Vec::with_capacity(msg_ids.len());
        for (msg_id, token) in msg_ids.iter().zip(claim_tokens.iter()) {
            if !hold.take(keys::claim(queue, msg_id), token) {
                return Err(Box::new(Rejection::Fenced(format!(
                    "claim_settling: queue={queue}, msg_id={msg_id} is being settled concurrently"
                ))));
            }
            claims.push(
                self.read_owned_claim(queue, msg_id, token, expected_lease_id, expected_worker_id)
                    .await?,
            );
        }
        assert_eq!(claims.len(), msg_ids.len());
        assert_eq!(hold.claims.len(), msg_ids.len());
        Ok(Settlement {
            write,
            hold,
            claims,
        })
    }

    /// Settles the claims of `msg_ids` in `queue` (none if it is empty) by
    /// `validate_claims` and admits a write to each queue of `downstream`.
    /// The downstream counters are loaded before any write is admitted:
    /// `delete_queue` waits for admitted writes while it holds
    /// `counters_loading`, so an admitted write must never wait for it.
    #[allow(clippy::too_many_arguments)]
    async fn admit_settlement(
        &self,
        queue: &str,
        msg_ids: &[String],
        claim_tokens: &[String],
        expected_lease_id: Option<&str>,
        expected_worker_id: Option<&str>,
        downstream: &[&str],
    ) -> Result<(Option<Settlement>, Vec<QueueWrite>), StorageError> {
        loop {
            let mut counters = Vec::with_capacity(downstream.len());
            for queue in downstream {
                counters.push(self.load_or_init_counters(queue).await?);
            }
            let settlement = if msg_ids.is_empty() {
                None
            } else {
                Some(
                    self.validate_claims(
                        queue,
                        msg_ids,
                        claim_tokens,
                        expected_lease_id,
                        expected_worker_id,
                    )
                    .await?,
                )
            };
            // A downstream queue deleted since it was loaded is loaded anew.
            if let Some(writes) = counters.into_iter().map(QueueWrite::admit).collect() {
                return Ok((settlement, writes));
            }
        }
    }

    /// Reads the claim on `msg_id` and checks that its holder presented the
    /// right token, lease and worker.
    async fn read_owned_claim(
        &self,
        queue: &str,
        msg_id: &str,
        token: &str,
        expected_lease_id: Option<&str>,
        expected_worker_id: Option<&str>,
    ) -> Result<ClaimInfo, StorageError> {
        let claim_data = self
            .db
            .get(keys::claim(queue, msg_id))
            .await?
            .ok_or_else(|| {
                Rejection::Fenced(format!(
                    "message_not_claimed: queue={queue}, msg_id={msg_id}"
                ))
            })?;
        let claim_info = format::decode_claim(msg_id, &claim_data)?;

        if claim_info.claim_token != token {
            return Err(Box::new(Rejection::Fenced(format!(
                "claim_token mismatch: queue={queue}, msg_id={msg_id}, \
                 expected={token}, actual={}",
                claim_info.claim_token
            ))));
        }
        if let Some(expected) = expected_lease_id {
            if claim_info.lease_id != expected {
                return Err(Box::new(Rejection::Fenced(format!(
                    "lease_id mismatch: queue={queue}, msg_id={msg_id}, \
                     expected={expected}, actual={}",
                    claim_info.lease_id
                ))));
            }
        }
        if let Some(expected) = expected_worker_id {
            if claim_info.worker_id != expected {
                return Err(Box::new(Rejection::Fenced(format!(
                    "worker_id mismatch: queue={queue}, msg_id={msg_id}, \
                     expected={expected}, actual={}",
                    claim_info.worker_id
                ))));
            }
        }
        Ok(claim_info)
    }

    /// Renews the claims of a worker that is still making progress on them,
    /// restarting their `claim_age_timeout_secs` window. Nothing is written:
    /// the renewal only has to outlive the broker process (see `renewed_at`).
    ///
    /// It does not take the settlement hold, so a worker can renew from one
    /// thread while acking from another. A renewal that lands just as
    /// recovery reclaims the claim is lost with it; the worker's next renewal
    /// or ack is then fenced like any stale claim.
    pub async fn renew_claims(
        &self,
        queue: &str,
        msg_ids: &[String],
        claim_tokens: &[String],
        worker_id: &str,
        lease_id: &str,
    ) -> Result<(), StorageError> {
        if claim_tokens.len() != msg_ids.len() {
            return Err(Box::new(Rejection::InvalidArgument(format!(
                "claim_tokens length mismatch (msg_ids={}, claim_tokens={})",
                msg_ids.len(),
                claim_tokens.len()
            ))));
        }
        let mut claims = Vec::with_capacity(msg_ids.len());
        for (msg_id, token) in msg_ids.iter().zip(claim_tokens) {
            claims.push(
                self.read_owned_claim(
                    queue,
                    msg_id,
                    token,
                    Some(lease_id).filter(|v| !v.is_empty()),
                    Some(worker_id).filter(|v| !v.is_empty()),
                )
                .await?,
            );
        }
        // All or none: one stale claim fails the call before any renews.
        let now = mono_now();
        for claim in claims {
            self.renewed_at.insert(claim.claim_token, now);
        }
        Ok(())
    }

    /// The claim and push cursors the pending keys of `queue` imply:
    /// claiming resumes at the oldest pending entry, and pushing continues
    /// past the newest one. Reads only those two entries.
    async fn derive_seq_range(&self, queue: &str) -> Result<(u64, u64), StorageError> {
        let newest = self
            .db
            .scan_with_options(
                keys::pending_of(queue),
                &slatedb::config::ScanOptions::default()
                    .with_order(slatedb::IterationOrder::Descending),
            )
            .await?
            .next()
            .await?;
        let Some(newest) = newest else {
            return Ok((0, 0));
        };
        let oldest = self
            .db
            .scan(keys::pending_of(queue))
            .await?
            .next()
            .await?
            .expect("the newest pending entry is there to scan");
        let (_, claim_seq) = keys::parse_pending(&oldest.key)?;
        let (_, newest_seq) = keys::parse_pending(&newest.key)?;
        assert!(claim_seq <= newest_seq);
        Ok((claim_seq, newest_seq + 1))
    }

    /// Number of messages of `queue` claimed and not yet settled.
    async fn count_claimed(&self, queue: &str) -> Result<u64, StorageError> {
        let mut iter = self.db.scan(keys::claims(Some(queue))).await?;
        let mut count = 0;
        while iter.next().await?.is_some() {
            count += 1;
        }
        Ok(count)
    }

    /// Counters of a created queue, or `None` for a queue that does not
    /// exist, which this never creates.
    async fn load_counters(&self, queue: &str) -> Result<Option<Arc<QueueCounters>>, StorageError> {
        // Fast path: already in cache
        if let Some(c) = self.counters.get(queue) {
            return Ok(Some(c.clone()));
        }
        let loading = self.counters_loading.clone().lock_owned().await;
        self.load_counters_locked(queue, &loading).await
    }

    /// The slow path of `load_counters`, for a caller holding
    /// `counters_loading`.
    async fn load_counters_locked(
        &self,
        queue: &str,
        _loading: &tokio::sync::OwnedMutexGuard<()>,
    ) -> Result<Option<Arc<QueueCounters>>, StorageError> {
        // Loaded by the previous holder of the lock.
        if let Some(c) = self.counters.get(queue) {
            return Ok(Some(c.clone()));
        }

        let Some(record) = self.db.get(keys::queue(queue)).await? else {
            return Ok(None);
        };
        let max_pending = format::decode_queue(&record)?;
        // Only the totals are taken as persisted. Cursors and the in-flight
        // count come from the keys, which every durable batch keeps exact,
        // so no cursor can lag behind its keys and hide a pending message.
        let (claim_seq, push_seq) = self.derive_seq_range(queue).await?;
        let claimed_count = self.count_claimed(queue).await?;
        let total_pushed = self.read_total(keys::total_pushed(queue)).await?;
        let total_acked = self.read_total(keys::total_acked(queue)).await?;
        let counters = Arc::new(QueueCounters {
            state: std::sync::Mutex::new(QueueState {
                push_seq_committed: push_seq,
                push_seq_alloc: push_seq,
                claim_seq,
                total_pushed,
                total_claimed: claimed_count,
                total_unclaimed: 0,
                total_acked,
                ..QueueState::default()
            }),
            ..QueueCounters::default()
        });
        self.set_max_pending(queue, max_pending);
        let previous = self.counters.insert(queue.to_string(), counters.clone());
        assert!(previous.is_none(), "counters of {queue} loaded twice");
        Ok(Some(counters))
    }

    /// The total at `key`; one never merged into is absent and reads as 0.
    async fn read_total(&self, key: Vec<u8>) -> Result<u64, StorageError> {
        match self.db.get(key).await? {
            Some(value) => Ok(format::decode_total(&value)?),
            None => Ok(0),
        }
    }

    /// Counters of `queue`, creating the queue if it does not exist: push,
    /// forward, scatter and claim create a queue on first use.
    async fn load_or_init_counters(&self, queue: &str) -> Result<Arc<QueueCounters>, StorageError> {
        if let Some(c) = self.counters.get(queue) {
            return Ok(c.clone());
        }
        let loading = self.counters_loading.clone().lock_owned().await;
        if let Some(counters) = self.load_counters_locked(queue, &loading).await? {
            return Ok(counters);
        }
        // A queue's totals start absent, which reads as 0.
        let mut batch = WriteBatch::new();
        batch.put(keys::queue(queue), format::encode_queue(0));
        self.commit(batch, Wait::Durable, Landing::default())
            .await?;
        let counters = Arc::new(QueueCounters::default());
        let previous = self.counters.insert(queue.to_string(), counters.clone());
        assert!(previous.is_none(), "counters of {queue} created twice");
        Ok(counters)
    }

    // === Queue Metadata ===

    /// Get queue metadata — single locked snapshot of in-memory state. A
    /// read must not create the queue it reads, so an unknown queue is
    /// `Rejection::NotFound`.
    pub async fn get_meta(&self, queue: &str) -> Result<QueueMeta, StorageError> {
        let counters = self
            .load_counters(queue)
            .await?
            .ok_or_else(|| Rejection::NotFound(format!("queue not found: {queue}")))?;
        Ok(Self::meta_of(&counters))
    }

    /// Metadata of every queue loaded since startup, without touching the
    /// database, for metrics callbacks that cannot await.
    pub fn queue_meta_snapshot(&self) -> Vec<(String, QueueMeta)> {
        self.counters
            .iter()
            .map(|entry| (entry.key().clone(), Self::meta_of(entry.value())))
            .collect()
    }

    fn meta_of(counters: &QueueCounters) -> QueueMeta {
        let s = counters.state.lock().expect("queue state poisoned");
        QueueMeta {
            push_seq: s.push_seq_committed,
            claim_seq: s.claim_seq - s.returned.len() as u64,
            claimed_count: s.claimed_count(),
            total_pushed: s.total_pushed,
            total_acked: s.total_acked,
        }
    }

    /// Create a queue: write its zero counter keys.
    /// `max_pending`: 0 = unlimited (default), >0 = bounded queue. Creating
    /// an existing queue with a bound replaces its bound; with 0 it keeps it.
    pub async fn create_queue(&self, queue: &str, max_pending: u64) -> Result<(), StorageError> {
        // Held from the check until the write is applied: see
        // `counters_loading`. It also orders this bound against a loader
        // reading the persisted one.
        let loading = self.counters_loading.clone().lock_owned().await;
        let exists = self.db.get(keys::queue(queue)).await?.is_some();
        if exists && max_pending == 0 {
            return Ok(());
        }

        let mut batch = WriteBatch::new();
        batch.put(keys::queue(queue), format::encode_queue(max_pending));
        self.set_max_pending(queue, max_pending);
        self.commit(
            batch,
            Wait::Durable,
            Landing {
                loading: Some(loading),
                ..Landing::default()
            },
        )
        .await?;
        if !exists {
            self.db.flush().await?;
        }
        Ok(())
    }

    /// Applies the bound of a queue record that is loaded, or about to be
    /// committed. The caller holds `counters_loading` until that batch is
    /// applied, so no loader overwrites it with the bound it read before.
    fn set_max_pending(&self, queue: &str, max_pending: u64) {
        if max_pending > 0 {
            self.max_pending_limits
                .insert(queue.to_string(), max_pending);
        }
    }

    /// Check if a queue has capacity for `additional` messages.
    /// Uses atomic counter reads (O(1)), no scans.
    /// Slight over-admission is acceptable (Relaxed ordering).
    async fn check_queue_capacity(
        &self,
        queue: &str,
        additional: usize,
    ) -> Result<(), StorageError> {
        // Loading the counters loads the persisted bound. A queue not created
        // yet has none, and checking it must not create it.
        let Some(counters) = self.load_counters(queue).await? else {
            return Ok(());
        };
        let Some(max) = self.max_pending_limits.get(queue).map(|limit| *limit) else {
            return Ok(());
        };
        assert!(max > 0, "unbounded queue {queue} has a bound");
        let in_flight = {
            let s = counters.state.lock().expect("queue state poisoned");
            s.total_pushed.saturating_sub(s.total_acked)
        };
        if in_flight + additional as u64 > max {
            return Err(Box::new(Rejection::QueueFull(format!(
                "QueueFull: queue={queue}, in_flight={in_flight}, max_pending={max}, attempted={additional}"
            ))));
        }
        Ok(())
    }

    // === Push Operations (NO TRANSACTION) ===

    /// Puts `messages` into `batch` as new pending messages of `queue`
    /// from `base_seq` on, with the increment of its lifetime push total.
    fn put_new_messages(batch: &mut WriteBatch, queue: &str, base_seq: u64, messages: &[Message]) {
        batch.merge(
            keys::total_pushed(queue),
            format::encode_total(messages.len() as u64),
        );
        for (i, msg) in messages.iter().enumerate() {
            batch.put(
                keys::message(queue, &msg.msg_id),
                format::encode_message(msg),
            );
            batch.put(
                keys::pending(queue, base_seq + i as u64),
                format::encode_pending(&msg.msg_id, 0),
            );
        }
    }

    /// Puts the ack of the claims `settlement` holds into `batch`, and the
    /// settlement into the `landing` of its write.
    fn ack_claims(
        batch: &mut WriteBatch,
        landing: &mut Landing,
        queue: &str,
        msg_ids: &[String],
        settlement: Settlement,
    ) {
        let Settlement {
            mut write, hold, ..
        } = settlement;
        assert_eq!(hold.claims.len(), msg_ids.len());
        let ack_count = msg_ids.len() as u64;
        write.ack(ack_count);
        batch.merge(keys::total_acked(queue), format::encode_total(ack_count));
        // The body is dead once acked: no reader of acked entries needs the
        // payload, and keeping it until GC multiplies the live data every
        // point read has to search.
        let now_ns = now_nanos();
        for msg_id in msg_ids {
            batch.delete(keys::claim(queue, msg_id));
            batch.delete(keys::message(queue, msg_id));
            batch.put(keys::acked(queue, now_ns, msg_id), []);
        }
        landing.writes.push(write);
        landing.settlement = Some(hold);
    }

    /// Puts the state updates of an operation into its `batch`.
    fn put_state(
        batch: &mut WriteBatch,
        namespace: &str,
        puts: Option<&HashMap<String, Vec<u8>>>,
        deletes: Option<&[String]>,
    ) {
        for (key, value) in puts.into_iter().flatten() {
            batch.put(keys::state(namespace, key), value);
        }
        for key in deletes.into_iter().flatten() {
            batch.delete(keys::state(namespace, key));
        }
    }

    /// Push messages to queue.
    ///
    /// Reserves a unique seq range at the tail; claimers see it once the
    /// write finishes and the watermark passes it. Concurrent pushers and
    /// nacks run their writes fully in parallel.
    pub async fn push_messages(
        &self,
        queue: &str,
        messages: &[Message],
    ) -> Result<(), StorageError> {
        if messages.is_empty() {
            return Ok(());
        }

        self.check_queue_capacity(queue, messages.len()).await?;

        let mut write = self.admit(queue).await?;
        let base_seq = write.reserve(messages.len() as u64, Tail::Pushed);
        let mut batch = WriteBatch::new();
        Self::put_new_messages(&mut batch, queue, base_seq, messages);
        self.commit(
            batch,
            Wait::Durable,
            Landing {
                writes: vec![write],
                ..Landing::default()
            },
        )
        .await
    }

    /// Push a single message (convenience wrapper, used in tests)
    #[allow(dead_code)]
    pub async fn push_message(&self, queue: &str, msg: &Message) -> Result<(), StorageError> {
        self.push_messages(queue, std::slice::from_ref(msg)).await
    }

    // === Claim Operations (CAS loop, NO TRANSACTION) ===

    /// Claims up to `batch_size` pending messages for `worker_id`.
    ///
    /// The seqs are reserved, and counted claimed, in one critical section
    /// (`QueueWrite::claim`); the reads and the write run unlocked. A claim
    /// cancelled or failed before its write returns the seqs, so another
    /// claim takes them.
    pub async fn claim_messages(
        &self,
        queue: &str,
        batch_size: usize,
        worker_id: &str,
        lease_id: &str,
    ) -> Result<Vec<ClaimedMessage>, StorageError> {
        let mut write = self.admit(queue).await?;
        let seqs = write.claim(batch_size);

        let mut batch = WriteBatch::new();
        let mut claimed = Vec::with_capacity(seqs.len());
        for seq in seqs {
            let pending_key = keys::pending(queue, seq);
            let Some(pending_bytes) = self.db.get(&pending_key).await? else {
                tracing::warn!("claim: pending key missing: queue={queue}, seq={seq}");
                continue;
            };
            let pending = format::decode_pending(&pending_bytes)?;
            let Some(msg_data) = self.db.get(keys::message(queue, &pending.msg_id)).await? else {
                continue;
            };
            let message = format::decode_message(&pending.msg_id, &msg_data)?;
            let claim_info = ClaimInfo::new(
                pending.msg_id,
                worker_id.to_string(),
                lease_id.to_string(),
                pending.delivery_count + 1,
            );
            batch.delete(&pending_key);
            batch.put(
                keys::claim(queue, &claim_info.msg_id),
                format::encode_claim(&claim_info),
            );
            claimed.push(ClaimedMessage {
                message,
                claim_token: claim_info.claim_token,
                delivery_count: claim_info.delivery_count,
            });
        }
        write.hand_over_claims(claimed.len());
        if claimed.is_empty() {
            return Ok(claimed);
        }

        let redelivered = claimed.iter().filter(|c| c.delivery_count > 1).count() as u64;
        if redelivered > 0 {
            crate::metrics::events().redelivered.add(
                redelivered,
                &[opentelemetry::KeyValue::new("queue", queue.to_string())],
            );
        }
        // The claim persists no cursor or count: both are derived from the
        // pending and claimed keys on load. A claim need not be durable
        // before the worker sees it. If the broker crashes first, recovery
        // reloads the older durable `pending` entry and redelivers the
        // message, which at-least-once already allows. The WAL is one
        // ordered log, so the durable ack that follows a claim makes the
        // claim durable with it. Skipping the wait halves a worker's per-key
        // broker latency.
        //
        // Stamped before the write so a claim applied after its caller was
        // cut off still has one; a stamp left by a failed write is pruned
        // by recovery with the tokens that hold no claim.
        let now = mono_now();
        for claimed in &claimed {
            self.taken_at.insert(claimed.claim_token.clone(), now);
        }
        self.commit(
            batch,
            Wait::Applied,
            Landing {
                writes: vec![write],
                ..Landing::default()
            },
        )
        .await?;
        Ok(claimed)
    }

    // === Ack Operations (NO TRANSACTION) ===

    /// Core ack operation with optional downstream push and state updates
    async fn ack_internal(
        &self,
        queue: &str,
        msg_ids: &[String],
        opts: AckOptions<'_>,
    ) -> Result<(), StorageError> {
        let downstream = match (opts.downstream_queue, opts.downstream_messages) {
            (Some(queue), Some(messages)) if !messages.is_empty() => Some((queue, messages)),
            _ => None,
        };
        if msg_ids.is_empty() && downstream.is_none() {
            return Ok(());
        }

        let claim_tokens = if msg_ids.is_empty() {
            &[][..]
        } else {
            let tokens = opts.claim_tokens.ok_or_else(|| {
                Rejection::InvalidArgument("claim_tokens is required for ack".to_string())
            })?;
            if tokens.len() != msg_ids.len() {
                return Err(Box::new(Rejection::InvalidArgument(format!(
                    "claim_tokens length mismatch (msg_ids={}, claim_tokens={})",
                    msg_ids.len(),
                    tokens.len()
                ))));
            }
            tokens
        };

        // Checked before anything is admitted or reserved, so a full queue
        // changes nothing.
        if let Some((downstream_queue, messages)) = downstream {
            self.check_queue_capacity(downstream_queue, messages.len())
                .await?;
        }

        // The downstream range is reserved only once the claims are verified:
        // a refused forward must leave no reservation, which would count as
        // pending downstream until a claim walked past its empty seqs.
        let downstream_queues: Vec<&str> = downstream.iter().map(|(queue, _)| *queue).collect();
        let (settlement, mut downstream_writes) = self
            .admit_settlement(
                queue,
                msg_ids,
                claim_tokens,
                opts.lease_id.filter(|v| !v.is_empty()),
                opts.worker_id.filter(|v| !v.is_empty()),
                &downstream_queues,
            )
            .await?;

        let mut batch = WriteBatch::new();
        let mut landing = Landing::default();
        if let Some(settlement) = settlement {
            Self::ack_claims(&mut batch, &mut landing, queue, msg_ids, settlement);
        }
        if let Some((downstream_queue, messages)) = downstream {
            assert_eq!(downstream_writes.len(), 1);
            let mut write = downstream_writes.pop().expect("the downstream write");
            let base_seq = write.reserve(messages.len() as u64, Tail::Pushed);
            Self::put_new_messages(&mut batch, downstream_queue, base_seq, messages);
            landing.writes.push(write);
        }
        if let Some(namespace) = opts.state_namespace {
            Self::put_state(&mut batch, namespace, opts.state_puts, opts.state_deletes);
        }
        self.commit(batch, Wait::Durable, landing).await
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
            claim_tokens,
            worker_id,
            lease_id,
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
            claim_tokens,
            worker_id,
            lease_id,
            Some(state_namespace),
            Some(state_puts),
            Some(state_deletes),
        )
        .await
    }

    /// Returns claims to pending after their worker died or stopped renewing
    /// them. It races the claimants' own acks, nacks and renewals, so each
    /// claim is settled only if it is not being settled already, still
    /// carries the token the caller read at `read_at` (from `mono_now`), and
    /// was not renewed since; the rest are skipped. Returns the claims
    /// requeued.
    pub async fn reclaim_messages(
        &self,
        queue: &str,
        claims: &[ClaimInfo],
        read_at: Instant,
    ) -> Result<Vec<ClaimInfo>, StorageError> {
        // Claims of a queue that no longer exists are no one's to requeue.
        let Some(write) = self.admit_existing(queue).await? else {
            return Ok(Vec::new());
        };
        let mut hold = SettlementHold::new(self.settling.clone());
        let mut reclaimed = Vec::with_capacity(claims.len());
        for claim in claims {
            let claim_key = keys::claim(queue, &claim.msg_id);
            if !hold.take(claim_key.clone(), &claim.claim_token) {
                continue;
            }
            let Some(data) = self.db.get(&claim_key).await? else {
                continue;
            };
            let current = format::decode_claim(&claim.msg_id, &data)?;
            let renewed_since_read = self
                .renewed_at
                .get(&current.claim_token)
                .is_some_and(|renewed_at| *renewed_at >= read_at);
            if current.claim_token == claim.claim_token && !renewed_since_read {
                reclaimed.push(current);
            }
        }
        assert!(reclaimed.len() <= claims.len());
        if reclaimed.is_empty() {
            return Ok(reclaimed);
        }
        let msg_ids: Vec<String> = reclaimed.iter().map(|c| c.msg_id.clone()).collect();
        let settlement = Settlement {
            write,
            hold,
            claims: reclaimed.clone(),
        };
        self.requeue_claims(queue, &msg_ids, settlement, None, None, None)
            .await?;
        Ok(reclaimed)
    }

    #[allow(clippy::too_many_arguments)]
    async fn nack_messages_internal(
        &self,
        queue: &str,
        msg_ids: &[String],
        claim_tokens: &[String],
        worker_id: &str,
        lease_id: &str,
        state_namespace: Option<&str>,
        state_puts: Option<&HashMap<String, Vec<u8>>>,
        state_deletes: Option<&[String]>,
    ) -> Result<(), StorageError> {
        if msg_ids.is_empty() {
            return Ok(());
        }
        if claim_tokens.len() != msg_ids.len() {
            return Err(Box::new(Rejection::InvalidArgument(format!(
                "claim_tokens length mismatch (msg_ids={}, claim_tokens={})",
                msg_ids.len(),
                claim_tokens.len()
            ))));
        }

        let settlement = self
            .validate_claims(
                queue,
                msg_ids,
                claim_tokens,
                Some(lease_id).filter(|v| !v.is_empty()),
                Some(worker_id).filter(|v| !v.is_empty()),
            )
            .await?;
        self.requeue_claims(
            queue,
            msg_ids,
            settlement,
            state_namespace,
            state_puts,
            state_deletes,
        )
        .await
    }

    /// Moves the claims `settlement` holds back to the tail of pending, each
    /// keeping the delivery count its claim carried, with any state updates
    /// in the same batch.
    async fn requeue_claims(
        &self,
        queue: &str,
        msg_ids: &[String],
        settlement: Settlement,
        state_namespace: Option<&str>,
        state_puts: Option<&HashMap<String, Vec<u8>>>,
        state_deletes: Option<&[String]>,
    ) -> Result<(), StorageError> {
        let Settlement {
            mut write,
            hold,
            claims,
        } = settlement;
        assert!(!msg_ids.is_empty());
        assert_eq!(claims.len(), msg_ids.len());

        let base_seq = write.reserve(msg_ids.len() as u64, Tail::Requeued);
        let mut batch = WriteBatch::new();
        for (i, (msg_id, claim)) in msg_ids.iter().zip(&claims).enumerate() {
            batch.delete(keys::claim(queue, msg_id));
            batch.put(
                keys::pending(queue, base_seq + i as u64),
                format::encode_pending(msg_id, claim.delivery_count),
            );
        }
        if let Some(namespace) = state_namespace {
            Self::put_state(&mut batch, namespace, state_puts, state_deletes);
        }
        self.commit(
            batch,
            Wait::Durable,
            Landing {
                writes: vec![write],
                settlement: Some(hold),
                ..Landing::default()
            },
        )
        .await
    }

    // === Scan Operations ===

    /// Scan all claimed messages (optionally filtered by queue)
    pub async fn scan_claimed(
        &self,
        queue: Option<&str>,
    ) -> Result<Vec<(String, String, ClaimInfo)>, StorageError> {
        let mut results = Vec::new();
        let mut iter = self.db.scan(keys::claims(queue)).await?;
        while let Some(kv) = iter.next().await? {
            let (key_queue, msg_id) = keys::parse_claim(&kv.key)?;
            assert!(queue.is_none_or(|queue| queue == key_queue));
            let claim_info = format::decode_claim(&msg_id, &kv.value)?;
            results.push((key_queue, msg_id, claim_info));
        }
        Ok(results)
    }

    /// Scan all acked messages (optionally filtered by queue)
    pub async fn scan_acked(
        &self,
        queue: Option<&str>,
    ) -> Result<Vec<(String, u64, String)>, StorageError> {
        let mut results = Vec::new();
        let mut iter = self.db.scan(keys::acked_all(queue)).await?;
        while let Some(kv) = iter.next().await? {
            let acked = keys::parse_acked(&kv.key)?;
            assert!(queue.is_none_or(|queue| queue == acked.0));
            results.push(acked);
        }
        Ok(results)
    }

    // === Query Operations ===

    /// Get queue stats - pure in-memory from atomic counters
    pub async fn get_queue_stats(&self, queue: &str) -> Result<QueueMeta, StorageError> {
        self.get_meta(queue).await
    }

    // === Delete Operations ===

    /// Deletes a queue and every message in it. Deleting a queue that does
    /// not exist deletes nothing and is not an error, so a retried delete
    /// succeeds.
    ///
    /// Fenced against every operation on the queue: writes admitted before
    /// it finish first, and later ones wait for `counters_loading`, which it
    /// holds until its batch has landed, and then see the queue as it left
    /// it. Without the fence, a claim or push holding the old counters
    /// writes keys after the delete, beside the fresh zero counters of the
    /// queue's next use.
    pub async fn delete_queue(&self, queue: &str) -> Result<usize, StorageError> {
        let loading = self.counters_loading.clone().lock_owned().await;
        let Some(counters) = self.load_counters_locked(queue, &loading).await? else {
            return Ok(0);
        };
        // Removed before closing, so a write turned away loads anew, with
        // its bound, under `counters_loading`.
        self.counters.remove(queue);
        self.max_pending_limits.remove(queue);
        counters.close().await;
        let claimed = self.scan_claimed(Some(queue)).await?;
        let acked = self.scan_acked(Some(queue)).await?;

        let mut batch = WriteBatch::new();
        let mut deleted = 0;

        batch.delete(keys::queue(queue));
        batch.delete(keys::total_pushed(queue));
        batch.delete(keys::total_acked(queue));

        // Every pending key, including those a returned claim left behind
        // the claim cursor.
        let mut pending = self.db.scan(keys::pending_of(queue)).await?;
        while let Some(kv) = pending.next().await? {
            batch.delete(&kv.key);
            let msg_id = format::decode_pending(&kv.value)?.msg_id;
            batch.delete(keys::message(queue, &msg_id));
            deleted += 1;
        }

        for (_, msg_id, _) in &claimed {
            batch.delete(keys::claim(queue, msg_id));
            batch.delete(keys::message(queue, msg_id));
            deleted += 1;
        }

        // An acked message's body went with its ack.
        for (_, ts, msg_id) in &acked {
            batch.delete(keys::acked(queue, *ts, msg_id));
            deleted += 1;
        }

        // Always written: a queue has its record.
        self.commit(
            batch,
            Wait::Durable,
            Landing {
                loading: Some(loading),
                ..Landing::default()
            },
        )
        .await?;
        Ok(deleted)
    }

    // === GC and Recovery ===

    pub async fn gc_acked_messages(&self, retention_ns: u64) -> Result<usize, StorageError> {
        let cutoff_ns = now_nanos().saturating_sub(retention_ns);
        let mut deleted = 0;
        // Acked keys sort by timestamp within a queue, so each queue's
        // expired entries are one contiguous range ending at the cutoff:
        // the pass never touches live entries, unlike a full prefix scan.
        for queue in self.list_queues().await? {
            let mut iter = self.db.scan(keys::acked_before(&queue, cutoff_ns)).await?;
            let mut batch = WriteBatch::new();
            let mut batch_keys = 0;
            while deleted < GC_PASS_KEYS_MAX {
                let Some(kv) = iter.next().await? else {
                    break;
                };
                let (key_queue, timestamp_ns, _) = keys::parse_acked(&kv.key)?;
                assert!(key_queue == queue && timestamp_ns < cutoff_ns);
                batch.delete(&kv.key);
                batch_keys += 1;
                deleted += 1;
                if batch_keys == GC_BATCH_KEYS_MAX {
                    self.commit(
                        std::mem::take(&mut batch),
                        Wait::Durable,
                        Landing::default(),
                    )
                    .await?;
                    batch_keys = 0;
                }
            }
            if batch_keys > 0 {
                self.commit(batch, Wait::Durable, Landing::default())
                    .await?;
            }
        }
        if deleted > 0 {
            tracing::info!("GC deleted {} acked entries", deleted);
            crate::metrics::events().gc_deleted.add(deleted as u64, &[]);
        }
        Ok(deleted)
    }

    /// Reclaim expired claims back to pending. Two independent timeouts:
    ///
    /// - `lease_timeout_secs`: how long a lease's heartbeat can lag before
    ///   the worker is considered dead. A lease is "alive" when its
    ///   `last_seen` is within this many seconds of `now`, on the
    ///   monotonic clock. Tunes *worker-death detection*. A lease absent
    ///   from `active_leases` is dead.
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
        active_leases: Option<&HashMap<String, Instant>>,
    ) -> Result<usize, StorageError> {
        let now = mono_now();
        let all_claimed = self.scan_claimed(None).await?;
        let live_tokens: std::collections::HashSet<&str> = all_claimed
            .iter()
            .map(|(_, _, claim_info)| claim_info.claim_token.as_str())
            .collect();
        self.taken_at
            .retain(|token, _| live_tokens.contains(token.as_str()));
        self.renewed_at
            .retain(|token, _| live_tokens.contains(token.as_str()));

        // Group expired by queue
        let mut expired_by_queue: HashMap<String, Vec<ClaimInfo>> = HashMap::new();
        for (queue, msg_id, claim_info) in all_claimed {
            let lease_alive = if let Some(leases) = active_leases {
                if let Some(last_seen) = leases.get(&claim_info.lease_id) {
                    !lease_expired(*last_seen, now, lease_timeout_secs)
                } else {
                    // Lease not in active set — worker is dead
                    false
                }
            } else {
                false
            };

            if lease_alive {
                // Live lease — only recover if neither the claim nor its last
                // renewal is within `claim_age_timeout_secs`. This catches
                // both a genuinely stuck worker (alive but not making
                // progress) AND the lag window between a Ray-level worker
                // death and the broker noticing the lease drop.
                //
                // A claim with no `taken_at` was taken by an earlier broker
                // process, or its stamp was pruned in a race with this scan;
                // its age is timed from now rather than from the wall-clock
                // `claimed_at`.
                let taken_at = *self
                    .taken_at
                    .entry(claim_info.claim_token.clone())
                    .or_insert(now);
                let held_since = self
                    .renewed_at
                    .get(&claim_info.claim_token)
                    .map_or(taken_at, |renewed_at| taken_at.max(*renewed_at));
                if now.saturating_duration_since(held_since).as_secs_f64() > claim_age_timeout_secs
                {
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
        for (queue, expired) in expired_by_queue {
            let claims = self.reclaim_messages(&queue, &expired, now).await?;
            if claims.is_empty() {
                continue;
            }
            crate::metrics::events().reclaimed.add(
                claims.len() as u64,
                &[opentelemetry::KeyValue::new("queue", queue.clone())],
            );

            let ts_ns = now_nanos();
            let events: HashMap<String, Vec<u8>> = claims
                .iter()
                .map(|claim| {
                    (
                        keys::reclaim_event(&queue, ts_ns, &claim.msg_id),
                        format::encode_reclaim_event(&queue, claim, ts_ns),
                    )
                })
                .collect();
            self.state_put_batch(keys::RECLAIM_EVENTS, &events, &[])
                .await?;

            total += claims.len();
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
        check_state_namespace(namespace)?;
        let mut results = HashMap::new();
        for key in keys {
            if let Some(data) = self.db.get(keys::state(namespace, key)).await? {
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
        check_state_namespace(namespace)?;
        let mut batch = WriteBatch::new();

        Self::put_state(&mut batch, namespace, Some(puts), Some(deletes));

        self.commit(batch, Wait::Durable, Landing::default())
            .await?;
        Ok((puts.len(), deletes.len()))
    }

    pub async fn state_scan_prefix(
        &self,
        namespace: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        check_state_namespace(namespace)?;
        let mut iter = self
            .db
            .scan_prefix(keys::state_prefix(namespace, prefix), ..)
            .await?;
        let mut results = Vec::new();
        while let Some(kv) = iter.next().await? {
            let (key_namespace, key) = keys::parse_state(&kv.key)?;
            assert_eq!(key_namespace, namespace);
            results.push((key, kv.value.to_vec()));
            if limit > 0 && results.len() >= limit {
                break;
            }
        }
        Ok(results)
    }

    pub async fn list_queues(&self) -> Result<Vec<String>, StorageError> {
        let mut queues = Vec::new();
        let mut iter = self.db.scan(keys::queues()).await?;
        while let Some(kv) = iter.next().await? {
            queues.push(keys::parse_queue(&kv.key)?);
        }
        Ok(queues)
    }

    // === Queue Completion API ===

    /// Mark a queue as finished (no more messages will be pushed)
    pub async fn mark_queue_finished(&self, queue: &str) -> Result<(), StorageError> {
        let mut batch = WriteBatch::new();
        batch.put(keys::finished(queue), []);
        self.commit(batch, Wait::Durable, Landing::default())
            .await?;
        self.db.flush().await?;
        tracing::info!("Queue {} marked as finished", queue);
        Ok(())
    }

    /// Check if queue is finished (marked by upstream)
    pub async fn is_queue_finished(&self, queue: &str) -> Result<bool, StorageError> {
        Ok(self.db.get(keys::finished(queue)).await?.is_some())
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
        let mut batch = WriteBatch::new();
        batch.delete(keys::finished(queue));
        self.commit(batch, Wait::Durable, Landing::default())
            .await?;
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
        match self.db.get(keys::group(group_name)).await? {
            Some(data) => Ok(Some(format::decode_group(group_name, &data)?)),
            None => Ok(None),
        }
    }

    /// Create a queue group with N partition queues atomically, and report
    /// whether this call created it. Idempotent: returns the existing group
    /// if it already exists, with `false`.
    /// `max_pending_per_partition`: 0 = unlimited (default), >0 = bounded
    /// partitions, replacing their bounds if the group exists.
    pub async fn create_queue_group(
        &self,
        group_name: &str,
        num_partitions: u32,
        max_pending_per_partition: u64,
    ) -> Result<(QueueGroupMeta, bool), StorageError> {
        // Held from the check until the write is applied: see
        // `counters_loading`. Of concurrent creates of one group, the first
        // to take it creates the group and the others find it.
        let loading = self.counters_loading.clone().lock_owned().await;
        let mut batch = WriteBatch::new();
        let existing = self.get_group_meta(group_name).await?;
        let created = existing.is_none();
        let meta = match existing {
            Some(existing) => existing,
            None => {
                let meta = QueueGroupMeta::new(group_name, num_partitions);
                batch.put(keys::group(group_name), format::encode_group(&meta));
                meta
            }
        };
        if created || max_pending_per_partition > 0 {
            for queue_name in &meta.partition_queues {
                batch.put(
                    keys::queue(queue_name),
                    format::encode_queue(max_pending_per_partition),
                );
                self.set_max_pending(queue_name, max_pending_per_partition);
            }
        }
        if created || max_pending_per_partition > 0 {
            self.commit(
                batch,
                Wait::Durable,
                Landing {
                    loading: Some(loading),
                    ..Landing::default()
                },
            )
            .await?;
        }

        if created {
            tracing::info!(
                "Created queue group '{}' with {} partitions",
                group_name,
                num_partitions
            );
        }
        Ok((meta, created))
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
            .ok_or_else(|| Rejection::NotFound(format!("Queue group not found: {}", group_name)))?;

        // Validate partition indices
        for (pid, _) in partition_payloads {
            if *pid >= group.num_partitions {
                return Err(Box::new(Rejection::InvalidArgument(format!(
                    "Partition {} out of range [0, {})",
                    pid, group.num_partitions
                ))));
            }
        }

        // 0. Check capacity for all downstream partition queues before any mutations
        let payloads: Vec<(&str, &[Message])> = partition_payloads
            .iter()
            .filter(|(_, messages)| !messages.is_empty())
            .map(|(pid, messages)| {
                (
                    group.partition_queues[*pid as usize].as_str(),
                    messages.as_slice(),
                )
            })
            .collect();
        for (partition_queue, messages) in &payloads {
            self.check_queue_capacity(partition_queue, messages.len())
                .await?;
        }
        if !upstream_msg_ids.is_empty() && upstream_claim_tokens.len() != upstream_msg_ids.len() {
            return Err(Box::new(Rejection::InvalidArgument(
                "claim_tokens length must match msg_ids".to_string(),
            )));
        }

        // As in a forward, the partition ranges are reserved only once the
        // claims are verified.
        let partition_queues: Vec<&str> = payloads.iter().map(|(queue, _)| *queue).collect();
        let (settlement, partition_writes) = self
            .admit_settlement(
                upstream_queue,
                upstream_msg_ids,
                upstream_claim_tokens,
                Some(lease_id).filter(|v| !v.is_empty()),
                Some(worker_id).filter(|v| !v.is_empty()),
                &partition_queues,
            )
            .await?;

        let mut batch = WriteBatch::new();
        let mut landing = Landing::default();
        if let Some(settlement) = settlement {
            Self::ack_claims(
                &mut batch,
                &mut landing,
                upstream_queue,
                upstream_msg_ids,
                settlement,
            );
        }
        let mut all_new_msg_ids = Vec::new();
        assert_eq!(partition_writes.len(), payloads.len());
        for ((partition_queue, messages), mut write) in payloads.into_iter().zip(partition_writes) {
            let base_seq = write.reserve(messages.len() as u64, Tail::Pushed);
            Self::put_new_messages(&mut batch, partition_queue, base_seq, messages);
            all_new_msg_ids.extend(messages.iter().map(|msg| msg.msg_id.clone()));
            landing.writes.push(write);
        }
        if let Some(namespace) = state_namespace {
            Self::put_state(&mut batch, namespace, state_puts, state_deletes);
        }
        self.commit(batch, Wait::Durable, landing).await?;
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
            .ok_or_else(|| Rejection::NotFound(format!("Queue group not found: {}", group_name)))?;

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
            .ok_or_else(|| Rejection::NotFound(format!("Queue group not found: {}", group_name)))?;

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
            .ok_or_else(|| Rejection::NotFound(format!("Queue group not found: {}", group_name)))?;

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
            .ok_or_else(|| Rejection::NotFound(format!("Queue group not found: {}", group_name)))?;

        let mut batch = WriteBatch::new();
        for queue_name in &group.partition_queues {
            batch.put(keys::finished(queue_name), []);
        }
        self.commit(batch, Wait::Durable, Landing::default())
            .await?;

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
pub(crate) mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    /// Goal: the first accesses to a queue, arriving together, agree on one
    /// set of counters. A loader that derives the cursors from the keys while
    /// another caller already pushes through counters it loaded can see a
    /// later push land before an earlier one and derive a claim cursor past
    /// the push cursor. Method: on fresh stores, let many tasks push to a
    /// queue no one has touched since it was created, then check every
    /// message is pending exactly once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_concurrent_first_access_loads_counters_once() {
        const ROUNDS: usize = 60;
        const PUSHERS: usize = 32;
        const PUSHES: usize = 2;
        for _ in 0..ROUNDS {
            let storage = Arc::new(AnvilStorage::new("memory://").await.unwrap());
            let queue = "first_access";
            storage.create_queue(queue, 0).await.unwrap();
            let pushers: Vec<_> = (0..PUSHERS)
                .map(|_| {
                    let storage = storage.clone();
                    tokio::spawn(async move {
                        for _ in 0..PUSHES {
                            let msg = Message::new(b"x".to_vec());
                            storage.push_message(queue, &msg).await.unwrap();
                        }
                    })
                })
                .collect();
            for pusher in pushers {
                pusher.await.unwrap();
            }
            let meta = storage.get_meta(queue).await.unwrap();
            assert_eq!(meta.claim_seq, 0);
            assert_eq!(meta.push_seq, (PUSHERS * PUSHES) as u64);
        }
    }

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

    /// Production defaults for `BrokerConfig::lease_timeout_secs` and
    /// `BrokerConfig::claim_age_timeout_secs` (60 s each).
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

    pub(crate) fn split_claims(claimed: &[ClaimedMessage]) -> (Vec<String>, Vec<String>) {
        let msg_ids = claimed.iter().map(|c| c.message.msg_id.clone()).collect();
        let claim_tokens = claimed.iter().map(|c| c.claim_token.clone()).collect();
        (msg_ids, claim_tokens)
    }

    /// Asserts that `error` is a fencing rejection, which the service maps to
    /// FAILED_PRECONDITION, for `reason`.
    fn assert_fenced(error: &StorageError, reason: &str) {
        assert!(
            matches!(
                error.downcast_ref::<Rejection>(),
                Some(Rejection::Fenced(_))
            ),
            "{error}"
        );
        assert!(error.to_string().contains(reason), "{error}");
    }

    #[tokio::test]
    async fn test_push_and_claim() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue, 0).await.unwrap();

        let msg1 = Message::new(b"hello".to_vec());
        let msg2 = Message::new(b"world".to_vec());

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
            .map(|i| Message::new(format!("msg{}", i).into_bytes()))
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

        let msg = Message::new(b"hello".to_vec());
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

    /// Goal: a queue whose name extends another's with ':' is parsed whole,
    /// so scans, recovery and the other queue's operations each see only
    /// their own entries. Method: claim one message in `colon` and one in
    /// `colon:00`, recover both with every lease dead, then ack one of them
    /// and check each scan by queue and across all queues.
    #[tokio::test]
    async fn test_queue_name_containing_colon_is_parsed_whole() {
        let storage = AnvilStorage::new("memory://").await.unwrap();
        let queues = ["colon", "colon:00"];
        for queue in queues {
            storage.create_queue(queue, 0).await.unwrap();
            let msg = Message::new(b"hello".to_vec());
            storage.push_message(queue, &msg).await.unwrap();
            storage
                .claim_messages(queue, 1, "worker-1", "lease-1")
                .await
                .unwrap();
        }
        for queue in queues {
            let claimed = storage.scan_claimed(Some(queue)).await.unwrap();
            assert_eq!(claimed.len(), 1, "claims of {queue}");
            assert_eq!(claimed[0].0, queue);
        }

        let reclaimed = storage
            .recover_expired_claims(30.0, 60.0, None)
            .await
            .unwrap();
        assert_eq!(reclaimed, 2, "a claim in `colon:00` was not reclaimed");
        assert!(storage.scan_claimed(None).await.unwrap().is_empty());

        let (extended, base) = (queues[1], queues[0]);
        let claimed = storage
            .claim_messages(extended, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        assert_eq!(
            claimed.len(),
            1,
            "the reclaimed message was not redelivered"
        );
        let (msg_ids, claim_tokens) = split_claims(&claimed);
        storage
            .ack_messages(extended, &msg_ids, &claim_tokens, "worker-1", "lease-1")
            .await
            .unwrap();
        assert!(storage.scan_acked(Some(base)).await.unwrap().is_empty());
        let acked = storage.scan_acked(None).await.unwrap();
        assert_eq!(acked.len(), 1);
        assert_eq!(acked[0].0, extended);
        assert_eq!(acked[0].2, msg_ids[0]);
    }

    /// A claim is not awaited for durability, so a crash right after it may
    /// forget it; at-least-once then requires the message to be claimable
    /// again after recovery rather than lost between pending and claimed.
    #[test]
    fn test_claim_lost_in_crash_is_redelivered() {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("anvil_crash_test_{counter}"));
        let _ = std::fs::remove_dir_all(&dir);
        let db_path = format!("file://{}", dir.display());
        let queue = "crash";

        let crashed = tokio::runtime::Runtime::new().unwrap();
        let (storage, msg_id) = crashed.block_on(async {
            let storage = AnvilStorage::new(&db_path).await.unwrap();
            storage.create_queue(queue, 0).await.unwrap();
            let msg = Message::new(b"payload".to_vec());
            storage.push_message(queue, &msg).await.unwrap();
            let claimed = storage
                .claim_messages(queue, 1, "worker", "lease")
                .await
                .unwrap();
            assert_eq!(claimed.len(), 1);
            (storage, msg.msg_id)
        });
        crashed.shutdown_background();
        std::mem::forget(storage);

        let recovered = tokio::runtime::Runtime::new().unwrap();
        recovered.block_on(async {
            let storage = AnvilStorage::new(&db_path).await.unwrap();
            let mut delivered = storage
                .claim_messages(queue, 1, "worker", "lease-2")
                .await
                .unwrap();
            if delivered.is_empty() {
                // The claim did reach the WAL before the crash: then lease
                // recovery is what hands the message out again.
                let recovered_count = storage
                    .recover_expired_claims(
                        LEASE_TIMEOUT_SECS_TEST_FAST,
                        CLAIM_AGE_TIMEOUT_SECS_TEST_LIVE_SAFE,
                        Some(&HashMap::new()),
                    )
                    .await
                    .unwrap();
                assert_eq!(recovered_count, 1);
                delivered = storage
                    .claim_messages(queue, 1, "worker", "lease-2")
                    .await
                    .unwrap();
            }
            assert_eq!(delivered.len(), 1);
            assert_eq!(delivered[0].message.msg_id, msg_id);
            assert_eq!(delivered[0].message.payload, b"payload");
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Every mutation that returned success must survive a crash. Phase one
    /// runs on its own runtime, which is torn down without closing the
    /// database, so no flush can sneak in after the calls return; phase two
    /// reopens the same directory and checks the acked and pending states.
    #[test]
    fn test_acknowledged_writes_survive_crash() {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("anvil_crash_test_{counter}"));
        let _ = std::fs::remove_dir_all(&dir);
        let db_path = format!("file://{}", dir.display());
        let queue = "crash";

        let crashed = tokio::runtime::Runtime::new().unwrap();
        let (storage, pushed_ids, acked_id) = crashed.block_on(async {
            let storage = AnvilStorage::new(&db_path).await.unwrap();
            storage.create_queue(queue, 0).await.unwrap();
            let messages: Vec<Message> = (0u8..3).map(|i| Message::new(vec![i])).collect();
            storage.push_messages(queue, &messages).await.unwrap();
            let claimed = storage
                .claim_messages(queue, 1, "worker", "lease")
                .await
                .unwrap();
            let (msg_ids, claim_tokens) = split_claims(&claimed);
            storage
                .ack_messages(queue, &msg_ids, &claim_tokens, "worker", "lease")
                .await
                .unwrap();
            let pushed_ids: Vec<String> = messages.into_iter().map(|m| m.msg_id).collect();
            (storage, pushed_ids, msg_ids[0].clone())
        });
        crashed.shutdown_background();
        std::mem::forget(storage);

        let recovered = tokio::runtime::Runtime::new().unwrap();
        recovered.block_on(async {
            let storage = AnvilStorage::new(&db_path).await.unwrap();
            let acked = storage.scan_acked(Some(queue)).await.unwrap();
            assert_eq!(acked.len(), 1);
            assert_eq!(acked[0].2, acked_id);

            let claimed = storage
                .claim_messages(queue, 10, "worker", "lease-2")
                .await
                .unwrap();
            let mut pending_ids: Vec<String> =
                claimed.iter().map(|c| c.message.msg_id.clone()).collect();
            pending_ids.sort();
            let mut expected: Vec<String> = pushed_ids
                .into_iter()
                .filter(|id| *id != acked_id)
                .collect();
            expected.sort();
            assert_eq!(pending_ids, expected);
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The delivery count starts at 1, survives a nack and a reclaimed lease
    /// (each adds one), and restarts at 1 for a message forwarded downstream,
    /// which is a new message.
    #[tokio::test]
    async fn test_delivery_count_across_redeliveries() {
        let storage = create_temp_storage().await;
        let queue = "count";
        storage.create_queue(queue, 0).await.unwrap();
        storage.create_queue("count-down", 0).await.unwrap();
        storage
            .push_message(queue, &Message::new(b"x".to_vec()))
            .await
            .unwrap();

        let first = storage
            .claim_messages(queue, 1, "w1", "lease-1")
            .await
            .unwrap();
        assert_eq!(first[0].delivery_count, 1);
        let (msg_ids, claim_tokens) = split_claims(&first);
        storage
            .nack_messages(queue, &msg_ids, &claim_tokens, "w1", "lease-1")
            .await
            .unwrap();

        let second = storage
            .claim_messages(queue, 1, "w2", "lease-2")
            .await
            .unwrap();
        assert_eq!(second[0].message.msg_id, msg_ids[0]);
        assert_eq!(second[0].delivery_count, 2);
        let no_live_leases = HashMap::new();
        let reclaimed = storage
            .recover_expired_claims(
                LEASE_TIMEOUT_SECS_TEST_FAST,
                CLAIM_AGE_TIMEOUT_SECS_TEST_LIVE_SAFE,
                Some(&no_live_leases),
            )
            .await
            .unwrap();
        assert_eq!(reclaimed, 1);

        let third = storage
            .claim_messages(queue, 1, "w3", "lease-3")
            .await
            .unwrap();
        assert_eq!(third[0].delivery_count, 3);
        let (msg_ids, claim_tokens) = split_claims(&third);
        storage
            .ack_and_forward(
                queue,
                &msg_ids,
                &claim_tokens,
                "w3",
                "lease-3",
                "count-down",
                &[Message::new(b"y".to_vec())],
            )
            .await
            .unwrap();
        let forwarded = storage
            .claim_messages("count-down", 1, "w4", "lease-4")
            .await
            .unwrap();
        assert_eq!(forwarded[0].delivery_count, 1);
    }

    #[tokio::test]
    async fn test_nack() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue, 0).await.unwrap();

        let msg = Message::new(b"hello".to_vec());
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

        let msg = Message::new(b"hello".to_vec());
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

        let msg = Message::new(b"hello".to_vec());
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

        let msg = Message::new(b"hello".to_vec());
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

        let upstream_msg = Message::new(b"input".to_vec());
        storage
            .push_message("upstream", &upstream_msg)
            .await
            .unwrap();
        let claimed = storage
            .claim_messages("upstream", 1, "worker-1", "lease-1")
            .await
            .unwrap();

        let downstream_msgs: Vec<Message> = vec![Message::new(b"out".to_vec())];

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

        let msg = Message::new(b"hello".to_vec());
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

    /// A retried ack racing the original must settle the message once: one
    /// call succeeds, the other is fenced, and the totals count it once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_duplicate_ack_settles_once() {
        let storage = Arc::new(create_temp_storage().await);
        let queue = "dup-ack";
        storage.create_queue(queue, 0).await.unwrap();
        let messages: Vec<Message> = (0..100).map(|_| Message::new(b"x".to_vec())).collect();
        storage.push_messages(queue, &messages).await.unwrap();
        let claimed = storage
            .claim_messages(queue, messages.len(), "w", "l")
            .await
            .unwrap();
        assert_eq!(claimed.len(), messages.len());

        for c in &claimed {
            let attempts: Vec<_> = (0..2)
                .map(|_| {
                    let storage = storage.clone();
                    let msg_ids = vec![c.message.msg_id.clone()];
                    let tokens = vec![c.claim_token.clone()];
                    tokio::spawn(async move {
                        storage
                            .ack_messages(queue, &msg_ids, &tokens, "w", "l")
                            .await
                            .is_ok()
                    })
                })
                .collect();
            let mut succeeded = 0;
            for attempt in attempts {
                succeeded += usize::from(attempt.await.unwrap());
            }
            assert_eq!(
                succeeded, 1,
                "{} settled {succeeded} times",
                c.message.msg_id
            );
        }

        let meta = storage.get_meta(queue).await.unwrap();
        assert_eq!(meta.total_acked, messages.len() as u64);
        assert_eq!(meta.claimed_count, 0);
        let acked = storage.scan_acked(Some(queue)).await.unwrap();
        assert_eq!(acked.len(), messages.len());
        assert!(storage.settling.is_empty());
    }

    /// Recovery reads claims, then reclaims them; a claim settled in
    /// between (acked, or nacked and claimed again under a new token) must
    /// be skipped. Requeueing it would deliver an acked message again and
    /// undercount `claimed_count`, so the queue would report drained while
    /// another message is still being processed.
    #[tokio::test]
    async fn test_reclaim_skips_claims_settled_since_read() {
        let storage = create_temp_storage().await;
        let queue = "reclaim-race";
        storage.create_queue(queue, 0).await.unwrap();
        let messages: Vec<Message> = (0..3).map(|_| Message::new(b"x".to_vec())).collect();
        storage.push_messages(queue, &messages).await.unwrap();
        let claimed = storage.claim_messages(queue, 3, "w", "l").await.unwrap();
        let read_at = crate::types::mono_now();
        let read_by_recovery: Vec<ClaimInfo> = storage
            .scan_claimed(Some(queue))
            .await
            .unwrap()
            .into_iter()
            .map(|(_, msg_id, info)| ClaimInfo { msg_id, ..info })
            .collect();
        let token_of = |msg_id: &str| {
            claimed
                .iter()
                .find(|c| c.message.msg_id == msg_id)
                .unwrap()
                .claim_token
                .clone()
        };
        let (acked_id, renewed_id, still_held_id) = (
            read_by_recovery[0].msg_id.clone(),
            read_by_recovery[1].msg_id.clone(),
            read_by_recovery[2].msg_id.clone(),
        );

        storage
            .ack_messages(
                queue,
                std::slice::from_ref(&acked_id),
                &[token_of(&acked_id)],
                "w",
                "l",
            )
            .await
            .unwrap();
        storage
            .nack_messages(
                queue,
                std::slice::from_ref(&renewed_id),
                &[token_of(&renewed_id)],
                "w",
                "l",
            )
            .await
            .unwrap();
        let renewed = storage.claim_messages(queue, 1, "w2", "l2").await.unwrap();
        assert_eq!(renewed[0].message.msg_id, renewed_id);

        let reclaimed = storage
            .reclaim_messages(queue, &read_by_recovery[..2], read_at)
            .await
            .unwrap();
        assert!(
            reclaimed.is_empty(),
            "reclaimed settled claims: {reclaimed:?}"
        );

        let meta = storage.get_meta(queue).await.unwrap();
        assert_eq!(meta.push_seq, meta.claim_seq, "nothing may be requeued");
        assert_eq!(meta.claimed_count, 2, "renewed and still-held claims");
        storage.mark_queue_finished(queue).await.unwrap();
        let (_, drained, _, _) = storage.check_queue_completion(queue).await.unwrap();
        assert!(
            !drained,
            "drained while {still_held_id} and {renewed_id} are in flight"
        );

        // A claim still holding the token recovery read is reclaimed.
        let reclaimed = storage
            .reclaim_messages(queue, &read_by_recovery[2..], read_at)
            .await
            .unwrap();
        assert_eq!(reclaimed.len(), 1);
        assert_eq!(reclaimed[0].msg_id, still_held_id);
    }

    /// While one settlement holds a claim, every other settlement of it is
    /// turned away, and the hold is released whether the settlement
    /// succeeds or fails.
    #[tokio::test]
    async fn test_settlement_hold_excludes_other_settlements() {
        let storage = create_temp_storage().await;
        let queue = "hold";
        storage.create_queue(queue, 0).await.unwrap();
        storage
            .push_message(queue, &Message::new(b"x".to_vec()))
            .await
            .unwrap();
        let claimed = storage.claim_messages(queue, 1, "w", "l").await.unwrap();
        let (msg_ids, tokens) = split_claims(&claimed);
        let read_at = crate::types::mono_now();
        let read_by_recovery: Vec<ClaimInfo> = storage
            .scan_claimed(Some(queue))
            .await
            .unwrap()
            .into_iter()
            .map(|(_, msg_id, info)| ClaimInfo { msg_id, ..info })
            .collect();

        {
            let mut held = SettlementHold::new(storage.settling.clone());
            assert!(held.take(keys::claim(queue, &msg_ids[0]), &tokens[0]));
            let err = storage
                .ack_messages(queue, &msg_ids, &tokens, "w", "l")
                .await
                .unwrap_err();
            assert_fenced(&err, "claim_settling");
            let err = storage
                .nack_messages(queue, &msg_ids, &tokens, "w", "l")
                .await
                .unwrap_err();
            assert_fenced(&err, "claim_settling");
            let reclaimed = storage
                .reclaim_messages(queue, &read_by_recovery, read_at)
                .await
                .unwrap();
            assert!(reclaimed.is_empty());
        }
        assert!(storage.settling.is_empty());

        let stale = vec!["stale-token".to_string()];
        assert!(storage
            .ack_messages(queue, &msg_ids, &stale, "w", "l")
            .await
            .is_err());
        assert!(
            storage.settling.is_empty(),
            "a failed check released its hold"
        );

        // The same message twice in one batch would settle it twice. The
        // service refuses such a batch as INVALID_ARGUMENT before it gets
        // here; the hold still settles the message once for any caller.
        let twice = vec![msg_ids[0].clone(), msg_ids[0].clone()];
        let err = storage
            .ack_messages(
                queue,
                &twice,
                &[tokens[0].clone(), tokens[0].clone()],
                "w",
                "l",
            )
            .await
            .unwrap_err();
        assert_fenced(&err, "claim_settling");

        storage
            .ack_messages(queue, &msg_ids, &tokens, "w", "l")
            .await
            .unwrap();
        assert!(storage.settling.is_empty());
        assert_eq!(storage.get_meta(queue).await.unwrap().total_acked, 1);
    }

    /// Reopening derives the claim and push cursors and the in-flight count
    /// from the keys, none of which is persisted: one written by concurrent
    /// writers in the wrong order could lag its keys, hiding pending
    /// messages or letting a new push overwrite one. A nack leaves a gap
    /// between the oldest and the newest pending entry, which the cursors
    /// must span.
    #[tokio::test]
    async fn test_reopen_derives_cursors_from_keys() {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("anvil_reopen_test_{counter}"));
        let _ = std::fs::remove_dir_all(&dir);
        let db_path = format!("file://{}", dir.display());
        let queue = "reopen";

        let storage = AnvilStorage::new(&db_path).await.unwrap();
        storage.create_queue(queue, 0).await.unwrap();
        let messages: Vec<Message> = (0u8..10).map(|i| Message::new(vec![i])).collect();
        storage.push_messages(queue, &messages).await.unwrap();
        let claimed = storage.claim_messages(queue, 4, "w", "l").await.unwrap();
        assert_eq!(claimed.len(), 4);
        let (msg_ids, claim_tokens) = split_claims(&claimed[..1]);
        storage
            .nack_messages(queue, &msg_ids, &claim_tokens, "w", "l")
            .await
            .unwrap();
        storage.close().await.unwrap();

        let storage = AnvilStorage::new(&db_path).await.unwrap();
        let meta = storage.get_meta(queue).await.unwrap();
        assert_eq!(meta.claim_seq, 4);
        assert_eq!(meta.push_seq, 11);
        assert_eq!(meta.claimed_count, 3);

        let late = Message::new(vec![10]);
        storage.push_message(queue, &late).await.unwrap();
        let rest = storage.claim_messages(queue, 100, "w", "l").await.unwrap();
        let mut payloads: Vec<u8> = rest.iter().map(|c| c.message.payload[0]).collect();
        payloads.sort();
        let mut expected: Vec<u8> = (4u8..=10).collect();
        expected.push(claimed[0].message.payload[0]);
        expected.sort();
        assert_eq!(payloads, expected);
        storage.close().await.unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn memory_store() -> Arc<dyn slatedb::object_store::ObjectStore> {
        Arc::new(slatedb::object_store::memory::InMemory::new())
    }

    fn format_error(result: Result<AnvilStorage, StorageError>) -> FormatError {
        match result {
            Ok(_) => panic!("the store opened"),
            Err(e) => match e.downcast::<FormatError>() {
                Ok(e) => *e,
                Err(e) => panic!("not a format error: {e}"),
            },
        }
    }

    /// The marker of the store in `object_store`, read without opening it as
    /// anvil does, so the read changes nothing.
    async fn read_marker(object_store: Arc<dyn slatedb::object_store::ObjectStore>) -> Option<u32> {
        let db = Db::builder("db", object_store).build().await.unwrap();
        let marker = db.get(keys::FORMAT).await.unwrap();
        db.close().await.unwrap();
        marker.map(|marker| format::decode_format(&marker).unwrap())
    }

    /// Goal: data written by an unversioned release (0.3.0 or earlier) is
    /// refused, not misread, and left as it was. Method: write a key of the
    /// old layout with SlateDB alone, open the store, and check the error
    /// and that no marker was stamped.
    #[tokio::test]
    async fn test_open_refuses_unversioned_store() {
        let object_store = memory_store();
        let db = Db::builder("db", object_store.clone())
            .build()
            .await
            .unwrap();
        #[allow(clippy::disallowed_methods)] // writes data no anvil release writes now
        db.put(b"seq_push:q", 0u64.to_le_bytes()).await.unwrap();
        db.close().await.unwrap();

        let refused = AnvilStorage::open("db".to_string(), object_store.clone()).await;
        assert_eq!(format_error(refused), FormatError::Unversioned);
        assert_eq!(read_marker(object_store).await, None);
    }

    /// Goal: a store written by a release with a newer format is refused,
    /// and an empty one is stamped with this release's format. Method: open
    /// an empty store, move its marker past `FORMAT_VERSION`, and reopen.
    #[tokio::test]
    async fn test_open_refuses_newer_format() {
        let object_store = memory_store();
        let storage = AnvilStorage::open("db".to_string(), object_store.clone())
            .await
            .unwrap();
        storage.close().await.unwrap();
        assert_eq!(
            read_marker(object_store.clone()).await,
            Some(FORMAT_VERSION)
        );

        let storage = AnvilStorage::open("db".to_string(), object_store.clone())
            .await
            .unwrap();
        let mut batch = WriteBatch::new();
        batch.put(keys::FORMAT, format::encode_format(FORMAT_VERSION + 1));
        storage
            .commit(batch, Wait::Durable, Landing::default())
            .await
            .unwrap();
        storage.close().await.unwrap();

        let refused = AnvilStorage::open("db".to_string(), object_store.clone()).await;
        assert_eq!(
            format_error(refused),
            FormatError::Newer {
                version: FORMAT_VERSION + 1
            }
        );
        assert_eq!(read_marker(object_store).await, Some(FORMAT_VERSION + 1));
    }

    /// Goal: a crash while the first open stamps the marker leaves a store
    /// that opens, stamped once, and serves. Method: open over a store whose
    /// writes are slow, tear the runtime down at points before, during and
    /// after the stamp's write, then reopen over the same objects and push
    /// and claim a message.
    #[test]
    fn test_crash_during_format_stamp() {
        use crate::fault_store::{FaultStore, Faults, OpFaults};
        let mut stamped_before_reopen = Vec::new();
        for (seed, cut_ms) in [0u64, 1, 5, 20, 50, 100, 300, 1000].into_iter().enumerate() {
            let inner = memory_store();
            let store = FaultStore::new(inner.clone(), seed as u64);
            store.set_faults(Faults {
                put: OpFaults {
                    latency: Duration::from_millis(20),
                    ..OpFaults::default()
                },
                ..Faults::default()
            });
            let crashed = tokio::runtime::Runtime::new().unwrap();
            crashed.spawn(async move {
                if let Ok(storage) = AnvilStorage::open("db".to_string(), Arc::new(store)).await {
                    // Never closed: the crash takes it down.
                    std::mem::forget(storage);
                }
            });
            std::thread::sleep(Duration::from_millis(cut_ms));
            crashed.shutdown_background();

            let recovered = tokio::runtime::Runtime::new().unwrap();
            recovered.block_on(async {
                stamped_before_reopen.push(read_marker(inner.clone()).await.is_some());
                let storage = AnvilStorage::open("db".to_string(), inner.clone())
                    .await
                    .unwrap_or_else(|e| panic!("cut at {cut_ms}ms: {e}"));
                let msg = Message::new(b"x".to_vec());
                storage.push_message("q", &msg).await.unwrap();
                let claimed = storage.claim_messages("q", 1, "w", "l").await.unwrap();
                assert_eq!(claimed.len(), 1, "cut at {cut_ms}ms");
                storage.close().await.unwrap();
                assert_eq!(read_marker(inner).await, Some(FORMAT_VERSION));
            });
        }
        // The cuts must straddle the stamp, or the test proves nothing.
        assert!(!stamped_before_reopen[0], "{stamped_before_reopen:?}");
        assert!(
            stamped_before_reopen.last().unwrap(),
            "{stamped_before_reopen:?}"
        );
    }

    #[tokio::test]
    async fn test_ack_rejects_stale_token_after_reclaim() {
        use crate::types::{advance_sim_time_secs, set_sim_time_nanos, SIM_TIME_LOCK};
        let _guard = SIM_TIME_LOCK.lock().unwrap();
        set_sim_time_nanos(1_900_000_000_000_000_000);

        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue, 0).await.unwrap();

        let msg = Message::new(b"hello".to_vec());
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

        let msg = Message::new(b"hello".to_vec());
        storage.push_message(queue, &msg).await.unwrap();
        let claimed = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();

        advance_sim_time_secs(1.0);

        let mut active = HashMap::new();
        active.insert("lease-1".to_string(), crate::types::mono_now());

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

    /// A claim under a live lease survives `claim_age_timeout_secs` for as
    /// long as its worker keeps renewing it, and is reclaimed one window
    /// after the renewals stop; an unrenewed claim beside it is reclaimed
    /// on the usual schedule.
    #[tokio::test]
    async fn test_renewal_keeps_claim_past_claim_age() {
        use crate::types::{advance_sim_time_secs, set_sim_time_nanos, SIM_TIME_LOCK};
        let _guard = SIM_TIME_LOCK.lock().unwrap();
        set_sim_time_nanos(2_200_000_000_000_000_000);

        let storage = create_temp_storage().await;
        let queue = "renew";
        storage.create_queue(queue, 0).await.unwrap();
        let messages: Vec<Message> = (0..2).map(|_| Message::new(b"x".to_vec())).collect();
        storage.push_messages(queue, &messages).await.unwrap();
        let claimed = storage.claim_messages(queue, 2, "w", "l").await.unwrap();
        let (renewed, idle) = (&claimed[0], &claimed[1]);
        let storage = &storage;
        let renew = || {
            storage.renew_claims(
                queue,
                std::slice::from_ref(&renewed.message.msg_id),
                std::slice::from_ref(&renewed.claim_token),
                "w",
                "l",
            )
        };
        let recover = || {
            let live = HashMap::from([("l".to_string(), crate::types::mono_now())]);
            async move {
                storage
                    .recover_expired_claims(
                        LEASE_TIMEOUT_SECS_PRODUCTION,
                        CLAIM_AGE_TIMEOUT_SECS_PRODUCTION,
                        Some(&live),
                    )
                    .await
                    .unwrap()
            }
        };
        let window = CLAIM_AGE_TIMEOUT_SECS_PRODUCTION;

        // Renewed every two thirds of a window, for three windows; recovery
        // runs a moment after the last renewal, so only expiry keeps it.
        for _ in 0..4 {
            advance_sim_time_secs(window * 2.0 / 3.0);
            renew().await.unwrap();
        }
        advance_sim_time_secs(1.0);
        assert_eq!(recover().await, 1, "only the unrenewed claim is stuck");
        let held: Vec<String> = storage
            .scan_claimed(Some(queue))
            .await
            .unwrap()
            .into_iter()
            .map(|(_, msg_id, _)| msg_id)
            .collect();
        assert_eq!(held, vec![renewed.message.msg_id.clone()]);
        assert_ne!(held[0], idle.message.msg_id);

        advance_sim_time_secs(window + 1.0);
        assert_eq!(recover().await, 1, "renewals stopped a window ago");
        assert!(storage.scan_claimed(Some(queue)).await.unwrap().is_empty());
        assert!(
            renew().await.is_err(),
            "a reclaimed claim cannot be renewed"
        );

        set_sim_time_nanos(0);
    }

    /// Renewal is fenced like an ack, and all or none: a stale token, or
    /// another worker's claim, fails the whole call and renews nothing.
    #[tokio::test]
    async fn test_renewal_rejects_claims_not_held() {
        let storage = create_temp_storage().await;
        let queue = "renew-fenced";
        storage.create_queue(queue, 0).await.unwrap();
        let messages: Vec<Message> = (0..2).map(|_| Message::new(b"x".to_vec())).collect();
        storage.push_messages(queue, &messages).await.unwrap();
        let claimed = storage.claim_messages(queue, 2, "w", "l").await.unwrap();
        let (msg_ids, tokens) = split_claims(&claimed);

        let stale = vec![tokens[0].clone(), "stale".to_string()];
        let err = storage
            .renew_claims(queue, &msg_ids, &stale, "w", "l")
            .await
            .unwrap_err();
        assert_fenced(&err, "claim_token mismatch");
        assert!(storage.renewed_at.is_empty(), "nothing renews on failure");

        let err = storage
            .renew_claims(queue, &msg_ids, &tokens, "w", "other-lease")
            .await
            .unwrap_err();
        assert_fenced(&err, "lease_id mismatch");
        assert!(storage.renewed_at.is_empty());

        storage
            .renew_claims(queue, &msg_ids, &tokens, "w", "l")
            .await
            .unwrap();
        assert_eq!(storage.renewed_at.len(), 2);
    }

    /// Recovery reads a claim as expired, then its worker renews it before
    /// the reclaim commits: the renewal wins. Settled claims' renewal
    /// records are pruned by the next recovery pass.
    #[tokio::test]
    async fn test_reclaim_skips_claim_renewed_since_read() {
        let storage = create_temp_storage().await;
        let queue = "renew-race";
        storage.create_queue(queue, 0).await.unwrap();
        storage
            .push_message(queue, &Message::new(b"x".to_vec()))
            .await
            .unwrap();
        let claimed = storage.claim_messages(queue, 1, "w", "l").await.unwrap();
        let (msg_ids, tokens) = split_claims(&claimed);
        let read_at = crate::types::mono_now();
        let read_by_recovery: Vec<ClaimInfo> = storage
            .scan_claimed(Some(queue))
            .await
            .unwrap()
            .into_iter()
            .map(|(_, msg_id, info)| ClaimInfo { msg_id, ..info })
            .collect();

        storage
            .renew_claims(queue, &msg_ids, &tokens, "w", "l")
            .await
            .unwrap();
        let reclaimed = storage
            .reclaim_messages(queue, &read_by_recovery, read_at)
            .await
            .unwrap();
        assert!(reclaimed.is_empty(), "reclaimed a renewed claim");

        storage
            .ack_messages(queue, &msg_ids, &tokens, "w", "l")
            .await
            .unwrap();
        assert_eq!(storage.renewed_at.len(), 1);
        let live = HashMap::from([("l".to_string(), crate::types::mono_now())]);
        storage
            .recover_expired_claims(
                LEASE_TIMEOUT_SECS_PRODUCTION,
                CLAIM_AGE_TIMEOUT_SECS_PRODUCTION,
                Some(&live),
            )
            .await
            .unwrap();
        assert!(
            storage.renewed_at.is_empty(),
            "settled claim's renewal pruned"
        );
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

        let upstream_msg = Message::new(b"input".to_vec());
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
            .map(|i| Message::new(format!("out{}", i).into_bytes()))
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
            let msg = Message::new(format!("msg{}", i).into_bytes());
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

    /// Stopping GC must not wait out the pass in flight. A full pass takes
    /// seconds, so a stop that waited for it, or gave up and left it running,
    /// let the pass outlive the broker and still be reading the storage when
    /// the runtime shut down under it.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_gc_stop_interrupts_an_in_flight_pass() {
        use crate::recovery::GcTask;
        use crate::types::AnvilConfig;

        const STOP_DEADLINE: Duration = Duration::from_secs(1);
        let storage = Arc::new(create_temp_storage().await);
        let queue = "gc-stop";
        storage.create_queue(queue, 0).await.unwrap();
        // A full pass of expired entries, so the pass is still running long
        // after its first commit.
        let entry_key = |i: usize| keys::acked(queue, 1, &format!("{i:012}"));
        for first in (0..GC_PASS_KEYS_MAX).step_by(GC_BATCH_KEYS_MAX) {
            let mut batch = WriteBatch::new();
            for i in first..first + GC_BATCH_KEYS_MAX {
                batch.put(entry_key(i), b"");
            }
            storage
                .commit(batch, Wait::Durable, Landing::default())
                .await
                .unwrap();
        }

        let config = AnvilConfig {
            acked_retention_secs: 0.0,
            ..AnvilConfig::default()
        };
        let mut gc = GcTask::new(storage.clone(), config);
        gc.start();
        // The first tick fires at once; the pass is in flight once its first
        // batch of deletes has landed.
        tokio::time::timeout(CONCURRENCY_TEST_DRAIN_DEADLINE, async {
            while storage.db.get(entry_key(0)).await.unwrap().is_some() {
                tokio::time::sleep(TEST_BUSY_SLEEP).await;
            }
        })
        .await
        .expect("GC pass did not start");
        assert!(storage
            .db
            .get(entry_key(GC_PASS_KEYS_MAX - 1))
            .await
            .unwrap()
            .is_some());

        tokio::time::timeout(STOP_DEADLINE, gc.stop_async())
            .await
            .expect("stop waited for the GC pass in flight");
        drop(gc);
        assert_eq!(
            Arc::strong_count(&storage),
            1,
            "the GC task outlived its stop"
        );
    }

    #[tokio::test]
    async fn test_queue_stats() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue, 0).await.unwrap();

        for i in 0..5 {
            let msg = Message::new(format!("msg{}", i).into_bytes());
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

        let msg = Message::new(b"hello".to_vec());

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
            let msg = Message::new(format!("msg{}", i).into_bytes());
            storage.push_message(queue, &msg).await.unwrap();
        }
        storage
            .claim_messages(queue, 2, "worker-1", "lease-1")
            .await
            .unwrap();

        let deleted = storage.delete_queue(queue).await.unwrap();
        assert!(deleted > 0);
        assert!(!storage
            .list_queues()
            .await
            .unwrap()
            .contains(&queue.to_string()));

        // Reading a deleted queue does not bring it back.
        let err = storage.get_meta(queue).await.unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<Rejection>(),
                Some(Rejection::NotFound(_))
            ),
            "{err}"
        );
        assert!(!storage
            .list_queues()
            .await
            .unwrap()
            .contains(&queue.to_string()));

        // Deleting it again deletes nothing and creates nothing.
        assert_eq!(storage.delete_queue(queue).await.unwrap(), 0);
        assert!(storage.get_meta(queue).await.is_err());

        // A queue that never held a message is deleted too.
        storage.create_queue("empty", 0).await.unwrap();
        assert_eq!(storage.delete_queue("empty").await.unwrap(), 0);
        assert!(!storage
            .list_queues()
            .await
            .unwrap()
            .contains(&"empty".to_string()));
    }

    #[tokio::test]
    async fn test_recover_expired_claims() {
        use crate::types::{advance_sim_time_secs, set_sim_time_nanos, SIM_TIME_LOCK};
        let _guard = SIM_TIME_LOCK.lock().unwrap();
        set_sim_time_nanos(2_000_000_000_000_000_000);

        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue, 0).await.unwrap();

        let msg = Message::new(b"hello".to_vec());
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
        let events = storage
            .state_scan_prefix(keys::RECLAIM_EVENTS, "", 0)
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        let event = format::decode_reclaim_event(&events[0].1).unwrap();
        assert_eq!(
            (event.queue.as_str(), event.msg_id.as_str()),
            (queue, msg.msg_id.as_str())
        );
        assert_eq!(
            (event.worker_id.as_str(), event.delivery_count),
            ("worker-1", 1)
        );

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
            let msg = Message::new(format!("msg{}", i).into_bytes());
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
    // nacked back to pending) must be claimable at some
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
                    .map(|i| Message::new(format!("p{p}-msg{i}").into_bytes()))
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
            .map(|i| Message::new(format!("msg{i}").into_bytes()))
            .collect();
        storage.push_messages(queue, &messages).await.unwrap();

        let dead = storage
            .claim_messages(queue, n as usize, "dead", "dead-lease")
            .await
            .unwrap();
        assert_eq!(dead.len(), n as usize, "failed to claim all upfront");
        let (dead_ids, dead_tokens) = split_claims(&dead);
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

        // Requeue task: nack all the dead worker's claimed msgs in one batch.
        let reclaim = {
            let storage = storage.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                storage
                    .nack_messages(queue, &dead_ids, &dead_tokens, "dead", "dead-lease")
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
                    .map(|i| Message::new(format!("p{p}-msg{i}").into_bytes()))
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
                        .map(|c| Message::new(c.message.payload.clone()))
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
                        .map(|i| Message::new(format!("p{p}-b{batch_idx}-m{i}").into_bytes()))
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
                let mut active = HashMap::<String, std::time::Instant>::new();
                for w in 0..LIVE_CLAIMERS {
                    active.insert(
                        format!("claimer-{w}-lease"),
                        crate::types::mono_now()
                            + std::time::Duration::from_secs_f64(LEASE_FAR_FUTURE_SECS),
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
