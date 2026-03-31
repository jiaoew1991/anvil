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

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use slatedb::{Db, DbRead, Error as SlateError, WriteBatch};

use crate::types::{now_nanos, ClaimInfo, ClaimedMessage, Message, QueueGroupMeta};

pub type StorageError = Box<dyn std::error::Error + Send + Sync>;

/// Per-queue atomic counters — in-memory fast path.
/// Each counter has a small set of writer classes, and AtomicU64 with CAS/fetch_add suffices.
pub struct QueueCounters {
    /// Next sequence to assign on push (written by: push, nack)
    pub push_seq: AtomicU64,
    /// Next sequence to claim (written by: claim via CAS)
    pub claim_seq: AtomicU64,
    /// Total messages ever pushed (written by: push)
    pub total_pushed: AtomicU64,
    /// Monotonic count of messages that entered claimed state (written by: claim)
    pub total_claimed: AtomicU64,
    /// Monotonic count of messages that left claimed state (written by: ack, nack)
    pub total_unclaimed: AtomicU64,
    /// Total messages ever acked (written by: ack)
    pub total_acked: AtomicU64,
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
}

impl AnvilStorage {
    pub async fn new(db_path: &str) -> Result<Self, StorageError> {
        let object_store = Db::resolve_object_store(db_path)?;
        let db = Db::open("/", object_store).await?;
        Ok(Self {
            db,
            counters: DashMap::new(),
            steal_rr: std::sync::Mutex::new(HashMap::new()),
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
            let claim_data =
                self.db.get(&claim_key).await?.ok_or_else(|| {
                    SlateError::invalid(format!("Message not claimed: {}", msg_id))
                })?;
            let claim_info: ClaimInfo = serde_json::from_slice(&claim_data)?;

            if claim_info.claim_token != *token {
                return Err(Box::new(SlateError::invalid(format!(
                    "claim_token mismatch for msg_id {}",
                    msg_id
                ))));
            }
            if let Some(expected) = expected_lease_id {
                if claim_info.lease_id != expected {
                    return Err(Box::new(SlateError::invalid(format!(
                        "lease_id mismatch for msg_id {}",
                        msg_id
                    ))));
                }
            }
            if let Some(expected) = expected_worker_id {
                if claim_info.worker_id != expected {
                    return Err(Box::new(SlateError::invalid(format!(
                        "worker_id mismatch for msg_id {}",
                        msg_id
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
                push_seq: AtomicU64::new(push_seq),
                claim_seq: AtomicU64::new(claim_seq),
                total_pushed: AtomicU64::new(total_pushed),
                total_claimed: AtomicU64::new(total_claimed),
                total_unclaimed: AtomicU64::new(total_unclaimed),
                total_acked: AtomicU64::new(total_acked),
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
                push_seq: AtomicU64::new(old_meta.push_seq),
                claim_seq: AtomicU64::new(old_meta.claim_seq),
                total_pushed: AtomicU64::new(old_meta.total_pushed),
                total_claimed: AtomicU64::new(migrated_total_claimed),
                total_unclaimed: AtomicU64::new(migrated_total_unclaimed),
                total_acked: AtomicU64::new(old_meta.total_acked),
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

    /// Get queue metadata — reads from atomic counters (pure in-memory).
    pub async fn get_meta(&self, queue: &str) -> Result<QueueMeta, StorageError> {
        let c = self.load_or_init_counters(queue).await?;
        let total_claimed = c.total_claimed.load(Ordering::Relaxed);
        let total_unclaimed = c.total_unclaimed.load(Ordering::Relaxed);
        Ok(QueueMeta {
            push_seq: c.push_seq.load(Ordering::Relaxed),
            claim_seq: c.claim_seq.load(Ordering::Relaxed),
            claimed_count: total_claimed.saturating_sub(total_unclaimed),
            total_pushed: c.total_pushed.load(Ordering::Relaxed),
            total_acked: c.total_acked.load(Ordering::Relaxed),
        })
    }

    /// Create a queue — write the 6 counter keys (all zeros).
    pub async fn create_queue(&self, queue: &str) -> Result<(), StorageError> {
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

    // === Push Operations (NO TRANSACTION) ===

    /// Push messages to queue using atomic counter + WriteBatch
    pub async fn push_messages(
        &self,
        queue: &str,
        messages: &[Message],
    ) -> Result<(), StorageError> {
        if messages.is_empty() {
            return Ok(());
        }

        let c = self.load_or_init_counters(queue).await?;
        let count = messages.len() as u64;

        // Reserve sequence range atomically
        let base_seq = c.push_seq.fetch_add(count, Ordering::Relaxed);
        let new_total_pushed = c.total_pushed.fetch_add(count, Ordering::Relaxed) + count;

        let mut batch = WriteBatch::new();
        for (i, msg) in messages.iter().enumerate() {
            let seq = base_seq + i as u64;
            batch.put(Self::msg_key(queue, &msg.msg_id), &serde_json::to_vec(msg)?);
            batch.put(Self::pending_key(queue, seq), msg.msg_id.as_bytes());
        }
        // Persist counters
        Self::persist_push_counters(&mut batch, queue, base_seq + count, new_total_pushed);
        self.db.write(batch).await?;
        Ok(())
    }

    /// Push a single message (convenience wrapper, used in tests)
    #[allow(dead_code)]
    pub async fn push_message(&self, queue: &str, msg: &Message) -> Result<(), StorageError> {
        self.push_messages(queue, std::slice::from_ref(msg)).await
    }

    // === Claim Operations (CAS loop, NO TRANSACTION) ===

    /// Claim messages from queue using CAS on claim_seq
    pub async fn claim_messages(
        &self,
        queue: &str,
        batch_size: usize,
        worker_id: &str,
        lease_id: &str,
    ) -> Result<Vec<ClaimedMessage>, StorageError> {
        let c = self.load_or_init_counters(queue).await?;

        // CAS loop to reserve a range of sequences
        let (start, end) = loop {
            let cur = c.claim_seq.load(Ordering::Acquire);
            let lim = c.push_seq.load(Ordering::Acquire);
            if cur >= lim {
                return Ok(Vec::new());
            }
            let target = std::cmp::min(cur + batch_size as u64, lim);
            if c.claim_seq
                .compare_exchange_weak(cur, target, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break (cur, target);
            }
            // CAS failed — another claimer won. Spin retry (nanosecond cost).
        };

        // Read messages (we own [start, end), no contention)
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
            }
            // If pending key is missing (gap from crashed push), skip silently
        }

        if claimed_items.is_empty() {
            return Ok(Vec::new());
        }

        let actual_count = claimed_items.len() as u64;
        let new_total_claimed =
            c.total_claimed.fetch_add(actual_count, Ordering::Relaxed) + actual_count;

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
            new_total_claimed.to_le_bytes(),
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
            let new_total_unclaimed =
                c.total_unclaimed.fetch_add(ack_count, Ordering::Relaxed) + ack_count;
            let new_total_acked = c.total_acked.fetch_add(ack_count, Ordering::Relaxed) + ack_count;

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

        // 2. Push downstream messages if provided
        if let (Some(downstream_queue), Some(messages)) =
            (opts.downstream_queue, opts.downstream_messages)
        {
            if !messages.is_empty() {
                let dc = self.load_or_init_counters(downstream_queue).await?;
                let count = messages.len() as u64;
                let base_seq = dc.push_seq.fetch_add(count, Ordering::Relaxed);
                let new_dp = dc.total_pushed.fetch_add(count, Ordering::Relaxed) + count;

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
                Self::persist_push_counters(&mut batch, downstream_queue, base_seq + count, new_dp);
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

        // Reserve new pending sequences at the tail
        let base_seq = c.push_seq.fetch_add(nack_count, Ordering::Relaxed);
        let new_unclaimed = c.total_unclaimed.fetch_add(nack_count, Ordering::Relaxed) + nack_count;

        let mut batch = WriteBatch::new();
        for (i, msg_id) in msg_ids.iter().enumerate() {
            batch.delete(Self::claimed_key(queue, msg_id));
            batch.put(
                Self::pending_key(queue, base_seq + i as u64),
                msg_id.as_bytes(),
            );
        }
        batch.put(
            Self::seq_push_key(queue),
            (base_seq + nack_count).to_le_bytes(),
        );
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

        // Remove from in-memory counter cache
        self.counters.remove(queue);

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

    pub async fn recover_expired_claims(
        &self,
        timeout_secs: f64,
        active_leases: Option<&HashMap<String, f64>>,
    ) -> Result<usize, StorageError> {
        let now = crate::types::now_secs();
        let all_claimed = self.scan_claimed(None).await?;

        // Group expired by queue
        let mut expired_by_queue: HashMap<String, Vec<ClaimInfo>> = HashMap::new();
        for (queue, msg_id, claim_info) in all_claimed {
            if let Some(leases) = active_leases {
                if let Some(last_seen) = leases.get(&claim_info.lease_id) {
                    if now - *last_seen <= timeout_secs {
                        continue;
                    }
                }
            }

            if now - claim_info.claimed_at > timeout_secs {
                let mut info = claim_info.clone();
                info.msg_id = msg_id;
                expired_by_queue.entry(queue).or_default().push(info);
            }
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
        let meta = self.get_meta(queue).await?;

        let pending_count = meta.push_seq.saturating_sub(meta.claim_seq);
        let claimed_count = meta.claimed_count;

        let drained =
            pending_count == 0 && claimed_count == 0 && (meta.total_pushed > 0 || finished);

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
    pub async fn create_queue_group(
        &self,
        group_name: &str,
        num_partitions: u32,
    ) -> Result<QueueGroupMeta, StorageError> {
        // Check for existing group
        if let Some(existing) = self.get_group_meta(group_name).await? {
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
            let new_total_unclaimed =
                uc.total_unclaimed.fetch_add(ack_count, Ordering::Relaxed) + ack_count;
            let new_total_acked =
                uc.total_acked.fetch_add(ack_count, Ordering::Relaxed) + ack_count;

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

        // 2. Push to each partition queue
        for (pid, messages) in partition_payloads {
            if messages.is_empty() {
                continue;
            }
            let partition_queue = &group.partition_queues[*pid as usize];
            let dc = self.load_or_init_counters(partition_queue).await?;
            let msg_count = messages.len() as u64;

            let base_seq = dc.push_seq.fetch_add(msg_count, Ordering::Relaxed);
            let new_dp = dc.total_pushed.fetch_add(msg_count, Ordering::Relaxed) + msg_count;

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

            Self::persist_push_counters(&mut batch, partition_queue, base_seq + msg_count, new_dp);
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

    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

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

        storage.create_queue(queue).await.unwrap();

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

        storage.create_queue(queue).await.unwrap();

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

        storage.create_queue(queue).await.unwrap();

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

        storage.create_queue(queue).await.unwrap();

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

        storage.create_queue(queue).await.unwrap();

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

        storage.create_queue(queue).await.unwrap();

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

        storage.create_queue(queue).await.unwrap();

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

        storage.create_queue("upstream").await.unwrap();
        storage.create_queue("downstream").await.unwrap();

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

        storage.create_queue(queue).await.unwrap();

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

        storage.create_queue(queue).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        storage.push_message(queue, &msg).await.unwrap();

        let claimed = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        let (msg_ids, claim_tokens) = split_claims(&claimed);

        // Advance sim time so claim is expired
        advance_sim_time_secs(1.0);
        let recovered = storage.recover_expired_claims(0.0, None).await.unwrap();
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

        storage.create_queue(queue).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        storage.push_message(queue, &msg).await.unwrap();
        let claimed = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();

        advance_sim_time_secs(1.0);

        let mut active = HashMap::new();
        active.insert("lease-1".to_string(), crate::types::now_secs());

        let recovered = storage
            .recover_expired_claims(0.0, Some(&active))
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

        storage.create_queue(queue).await.unwrap();
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

        storage.create_queue("upstream").await.unwrap();
        storage.create_queue("downstream").await.unwrap();

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

        storage.create_queue(queue).await.unwrap();

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

        storage.create_queue(queue).await.unwrap();

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

        storage.create_queue(queue).await.unwrap();

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

        storage.create_queue(queue).await.unwrap();

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

        storage.create_queue(queue).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        storage.push_message(queue, &msg).await.unwrap();
        storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();

        advance_sim_time_secs(1.0);
        let recovered = storage.recover_expired_claims(0.0, None).await.unwrap();
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

        storage.create_queue(queue).await.unwrap();

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
}
