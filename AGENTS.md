# Anvil-RS Development Guide

This document provides critical design guidelines for developing and maintaining the Anvil Rust implementation.

---

## 1. Core Principle: O(1) I/O Complexity

**All hot-path operations MUST have O(1) I/O complexity.**

Scan operations (`scan_prefix`, `iter`) are extremely expensive in LSM-tree based storage (SlateDB). They should:
- NEVER be used in hot paths (claim, ack, push)
- ONLY be used for background maintenance tasks (GC, recovery) with rate limiting
- Be replaced with O(1) alternatives when possible

### Why This Matters

| Operation | O(1) Latency | O(n) Scan Latency |
|-----------|-------------|-------------------|
| 100 messages | ~1ms | ~10ms |
| 10,000 messages | ~1ms | ~1s |
| 1,000,000 messages | ~1ms | ~100s |

A single `scan_claimed()` call with 1M claimed messages can block the server for minutes.

---

## 2. Current Data Model

### Storage Format

[docs/storage-format.md](docs/storage-format.md) is the normative spec of
every key and value in the store: the format version and the open-time
rules, the key families and their byte layout, the value schemas, which
changes are compatible, and how a breaking one is made. `src/keys.rs` is the
only code that builds or parses a key, and `src/format.rs` with
`proto/storage.proto` the only code that encodes a value; a change to either
is a format change and follows the spec.

### Counters and restart

Queue counters live in memory (`QueueState`) and are authoritative while the
broker runs. On load, `load_or_init_counters` derives what must be exact from
the keys: `claim_seq` is the oldest pending key, `push_seq` is past the
newest one, and the in-flight count is the number of claimed keys. The dead
count is dead-lettered − redriven − purged, three merged totals, so a load
never scans dead letters. Only the lifetime totals are read as persisted.

Never persist a counter as an absolute value. Concurrent writers compute their
values under a lock but reach the WAL in any order, so an older value can land
last: a claim cursor persisted that way once hid pending messages forever after
a crash. A total that must survive is merged as an increment in the batch of
the operation it counts (`TotalSumMergeOperator`), which makes it exact.

### Settling a claim

Ack, nack and reclaim each read a claim and then write its outcome. Every one
must hold the claim between the two (`validate_claims` returns the
`SettlementHold`; `reclaim_messages` takes it and re-reads the token), so a
second settlement of the same message is fenced instead of acking it twice or
requeueing it after its ack. The hold is keyed by the claim and the token
presented: only settlements presenting the current token can pass the check,
so they are the ones that must exclude each other, and a stale token never
holds the claim its owner is settling.

A forward or scatter reserves its downstream seqs only after the claims it
settles are verified: a refused one must leave the downstream queue as it was.

### Cancellation and deletion

A client deadline drops the request's future at any await, so no operation may
leave memory and keys apart when it stops at one. Every write goes through
`commit`, which runs `Db::write` in a task of its own together with what the
write releases (`Landing`): the settlement hold, the push reservation, and the
retirement of what it settled, which happens only once the batch is applied
(visible to every read; the caller then waits for durability).
Work-adding counters move when the work is reserved, so a queue never looks
drained while a write is in flight. A claim cancelled before its write returns
its seqs (`QueueState::returned`) for the next claim to take.

Every write to a queue is admitted first (`QueueWrite`), and `delete_queue`
waits for the admitted ones while it holds `counters_loading`, so no write of
an operation that loaded the old counters lands after the delete. An admitted
write must therefore never wait for `counters_loading`: an operation that
writes several queues loads all their counters before admitting any write.

### Errors

A failure the caller caused is a `storage::Rejection`, never a
`SlateError::invalid` string: `Fenced` for a claim the caller no longer holds
(token, lease or worker mismatch, not claimed, settling), `NotFound`,
`InvalidArgument` and `QueueFull`. `service::storage_status` is the one place a
storage error becomes a gRPC status; it recognizes a `Rejection` by type, maps
a closed database (`storage::is_closed`: fenced by a newer broker, or stopped
by a failed background task) to `UNAVAILABLE`, and everything else to
`INTERNAL`. A worker drops a message on `FAILED_PRECONDITION`, fails over on
`UNAVAILABLE` and retries on `INTERNAL`, so a new fencing check that returns
anything but `Rejection::Fenced` makes it retry what cannot succeed.

A request malformed on its face (empty queue name, unpaired or repeated
msg_ids, a bad state namespace) is refused in `service.rs` before it reaches
storage. `AnvilConfig::validate` is the one check of a broker config; the
Python `BrokerConfig`, `AnvilBroker.start` and `AnvilBrokerInner::start` all
call it, so a bad duration is refused before any background task starts.

### Queue creation

`load_counters` reads a queue and never creates it; `load_or_init_counters`
creates it on first use, which push, forward, scatter and claim rely on.
`get_meta` and `delete_queue` use `load_counters`, so asking about or deleting
an unknown queue cannot bring it into existence; a new read path must too.

### O(1) Operations (Good ✅)

| Operation | How | Complexity |
|-----------|-----|------------|
| `push` | Increment `push_seq`, write the pending key at `seq` | O(1) |
| `claim` | Read from `claim_seq` to `claim_seq + batch_size`, increment `claim_seq` | O(batch_size) |
| `ack` | Delete the claim and the body, write the acked key at `ts` | O(batch_size) |
| `nack` | Delete the claim, append a pending key at `push_seq` | O(batch_size) |
| `state_get` | Direct key lookup | O(keys) |
| `state_put` | Direct key write | O(keys) |

### O(n) Operations

| Operation | Current Implementation | Frequency | Status |
|-----------|----------------------|-----------|--------|
| `get_queue_stats` | `QueueMeta` counters | **Hot path** (every stats call) | ✅ Fixed - O(1) |
| `recover_expired_claims` | `scan_claimed(None)` | Background (every 10s default) | ✅ Acceptable |
| `gc_acked_messages` | `scan_acked(None)` | Background (every 60s default) | ✅ Acceptable |
| `delete_queue` | Multiple scans | Admin (rare, on job cleanup) | ✅ Acceptable |
| Queue load | Counts the claim keys | Once per queue per broker process | ✅ Acceptable |
| `list_dead_letters` | Bounded range scan from a cursor, at most 1000 | Admin | ✅ Acceptable |

**Design Decision**: Low-frequency background/admin operations can use scan. Only hot-path operations (claim, ack, push, stats) must be O(1).

---

## 3. Fixing O(n) Operations

### 3.1 Claimed Count: Use Counter ✅ IMPLEMENTED

**Problem**: `get_queue_stats` calls `scan_claimed` to count claimed messages.

**Solution**: `get_queue_stats` reads the in-memory `QueueState` counters
(O(1), no I/O). The claimed keys are counted once per queue, when the broker
loads it (see "Counters and restart").

### 3.2 Background Tasks (Acceptable O(n))

The following operations use scan but run infrequently, so O(n) is acceptable:

| Operation | Frequency | Scan Scope | Why Acceptable |
|-----------|-----------|------------|----------------|
| `recover_expired_claims` | Every 10s | All claimed | Claimed count bounded by active workers |
| `gc_acked_messages` | Every 60s | All acked | Runs in background, doesn't block hot path |
| `delete_queue` | On job cleanup | One queue | Rare admin operation |

**Future optimization** (if needed): These could be optimized with time-indexed secondary keys or in-memory tracking, but current implementation is sufficient for expected workloads.

---

## 4. Performance Guidelines

### DO ✅

1. **Use direct key access** for all hot-path operations
2. **Maintain counters** instead of counting via scan
3. **Use time-ordered keys** for time-based queries
4. **Batch operations** using `WriteBatch` for atomicity and performance
5. **Add secondary indexes** when you need to query by different dimensions
6. **Use in-memory caches** for frequently accessed metadata

### DON'T ❌

1. **Never scan in hot paths** - claim, ack, push must be O(1)
2. **Never scan without bounds** - always limit scan range
3. **Never count by scanning** - use pre-computed counters
4. **Never assume scan is fast** - even 1000 entries can be slow under load
5. **Never block on GC/recovery** - run them in background with rate limiting

### Scan Usage Rules

| Scan Type | Allowed Context | Required Safeguards |
|-----------|-----------------|---------------------|
| Prefix scan | Background GC only | Rate limit, batch size limit |
| Range scan | Time-bounded queries | Upper bound on range |
| Full scan | Never in production | Only for admin/debug tools |

---

## 5. Key Schema Design Principles

Keys below are written as `family(field, ...)`, the tuples of
[docs/storage-format.md](docs/storage-format.md).

### Principle 1: Hot Path Keys Support Direct Access

```
# Good: Direct lookup by known key
message(queue, msg_id) → StoredMessage

# Bad: Requires scan to find
message(queue, timestamp, msg_id) → StoredMessage  # Can't lookup by msg_id directly
```

### Principle 2: Secondary Indexes for Query Patterns

If you need to query by multiple dimensions, add secondary indexes:

```
# Primary: lookup by msg_id
claim(queue, msg_id) → ClaimRecord

# Secondary: query by expiry time (for recovery)
claim_exp(queue, expires_at, msg_id) → empty
```

### Principle 3: Time-Ordered Keys Enable Range Deletion

```
# Good: can range-delete old entries
acked(queue, timestamp, msg_id) → empty

# Bad: can't efficiently delete old entries
acked(queue, msg_id) → {timestamp, ...}
```

### Principle 4: Counters for Cardinality

```
# Instead of: SELECT COUNT(*) FROM claimed WHERE queue = ? on every call
# Use: an in-memory counter, derived from the keys once when the queue loads
```

---

## 6. Implementation Checklist

When adding new features, verify:

- [ ] All hot-path operations are O(1) or O(batch_size)
- [ ] No unbounded scans in request handlers
- [ ] Counters updated atomically with state changes
- [ ] Secondary indexes added for new query patterns
- [ ] Background tasks have rate limiting
- [ ] Tests verify O(1) behavior (not just correctness)

### Performance Test Template

```rust
#[tokio::test]
async fn test_claim_performance_scales_constant() {
    let storage = create_temp_storage().await;
    let queue = "test-queue";
    storage.create_queue(queue).await.unwrap();

    // Push many messages
    for i in 0..10000 {
        let msg = Message::new(queue.to_string(), format!("msg{}", i).into_bytes());
        storage.push_message(queue, &msg).await.unwrap();
    }

    // Claim should be O(1), not O(total_messages)
    let start = std::time::Instant::now();
    let claimed = storage.claim_messages(queue, 10, "worker-1", "lease-1").await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(claimed.len(), 10);
    assert!(elapsed.as_millis() < 100, "Claim took too long: {:?}", elapsed);
}
```

---

## 7. Technical Debt & Design Decisions

### Hot Path: O(1) Required ✅

1. **`get_queue_stats`** - FIXED
   - Reads the in-memory queue counters (O(1), no I/O)

### Background Tasks: O(n) Acceptable ✅

Low-frequency operations can use scan without performance concerns:

2. **`recover_expired_claims`** - Acceptable
   - Runs every `recovery_interval_secs` (default 10s)
   - Scans claimed messages to find expired ones
   - Typical claimed count is low (bounded by worker count × batch size)

3. **`gc_acked_messages`** - Acceptable
   - Runs every `gc_interval_secs` (default 60s)
   - Scans acked messages older than retention period
   - Acked messages are retained for `acked_retention_secs` (default 1 hour)

### Admin Operations: O(n) Acceptable ✅

4. **`delete_queue`** - Acceptable
   - Only called on job cleanup (rare)
   - Full scan is fine for admin operations

---

## 8. Future: push_with_dedup

A planned feature for exactly-once semantics at the Source level:

```rust
/// Push with deduplication based on business key
/// Returns (msg_id, deduplicated) where deduplicated=true means message was skipped
pub async fn push_with_dedup(
    &self,
    queue: &str,
    business_key: &str,
    msg: &Message,
) -> Result<(Option<String>, bool), StorageError> {
    let dedup_key = keys::dedup(queue, business_key);

    // O(1) check: does this business_key already exist?
    if self.db.get(&dedup_key).await?.is_some() {
        return Ok((None, true));  // Deduplicated
    }

    // Atomic: write dedup marker + push message
    let mut batch = WriteBatch::new();
    batch.put(&dedup_key, msg.msg_id.as_bytes());
    // ... normal push logic ...

    self.db.write(batch).await?;
    Ok((Some(msg.msg_id.clone()), false))
}

// In src/keys.rs, as a new family (a compatible format change):
// a fresh tag, then str(queue) str(business_key).
```

**Design considerations:**
- Dedup keys need TTL/cleanup (job-scoped or time-based)
- Business key must be deterministic from source data
- See `anvil-semantics.md` Section 9 for full design discussion

---

_Last updated: 2026-02-02_
