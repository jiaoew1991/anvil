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

### Key Schema

```
meta:{queue}                          → QueueMeta {claim_seq, push_seq}
pending:{queue}:{seq:020d}            → msg_id
msg:{queue}:{msg_id}                  → Message JSON
claimed:{queue}:{msg_id}              → ClaimInfo JSON
acked:{queue}:{ts:020d}:{msg_id}      → ""
state:{namespace}:{key}               → value bytes
```

### O(1) Operations (Good ✅)

| Operation | How | Complexity |
|-----------|-----|------------|
| `push` | Increment `push_seq`, write to `pending:{queue}:{seq}` | O(1) |
| `claim` | Read from `claim_seq` to `claim_seq + batch_size`, increment `claim_seq` | O(batch_size) |
| `ack` | Delete `claimed:{queue}:{msg_id}`, write `acked:{queue}:{ts}:{msg_id}` | O(batch_size) |
| `nack` | Delete `claimed:{queue}:{msg_id}`, append to `pending:{queue}:{push_seq}` | O(batch_size) |
| `state_get` | Direct key lookup | O(keys) |
| `state_put` | Direct key write | O(keys) |

### O(n) Operations

| Operation | Current Implementation | Frequency | Status |
|-----------|----------------------|-----------|--------|
| `get_queue_stats` | `QueueMeta` counters | **Hot path** (every stats call) | ✅ Fixed - O(1) |
| `recover_expired_claims` | `scan_claimed(None)` | Background (every 10s default) | ✅ Acceptable |
| `gc_acked_messages` | `scan_acked(None)` | Background (every 60s default) | ✅ Acceptable |
| `delete_queue` | Multiple scans | Admin (rare, on job cleanup) | ✅ Acceptable |

**Design Decision**: Low-frequency background/admin operations can use scan. Only hot-path operations (claim, ack, push, stats) must be O(1).

---

## 3. Fixing O(n) Operations

### 3.1 Claimed Count: Use Counter ✅ IMPLEMENTED

**Problem**: `get_queue_stats` calls `scan_claimed` to count claimed messages.

**Solution**: Maintain counters in QueueMeta, updated atomically in each operation.

```rust
// storage.rs - QueueMeta now has counters
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QueueMeta {
    pub claim_seq: u64,
    pub push_seq: u64,
    #[serde(default)]
    pub claimed_count: u64,   // ✅ Updated in claim/ack/nack
    #[serde(default)]
    pub total_pushed: u64,    // ✅ Lifetime counter
    #[serde(default)]
    pub total_acked: u64,     // ✅ Lifetime counter
}

// get_queue_stats is now O(1)
pub async fn get_queue_stats(&self, queue: &str) -> Result<QueueMeta, StorageError> {
    self.get_meta(queue).await  // ✅ Single key lookup
}
```

Counter updates:
- `claim_messages`: `claimed_count += claimed.len()`
- `ack_internal`: `claimed_count -= ack_count`, `total_acked += ack_count`
- `nack_messages`: `claimed_count -= nack_count`
- `push_messages`: `total_pushed += msg_count`

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

### Principle 1: Hot Path Keys Support Direct Access

```
# Good: Direct lookup by known key
msg:{queue}:{msg_id} → Message

# Bad: Requires scan to find
msg:{queue}:{timestamp}:{msg_id} → Message  # Can't lookup by msg_id directly
```

### Principle 2: Secondary Indexes for Query Patterns

If you need to query by multiple dimensions, add secondary indexes:

```
# Primary: lookup by msg_id
claimed:{queue}:{msg_id} → ClaimInfo

# Secondary: query by expiry time (for recovery)
claim_exp:{queue}:{expires_at}:{msg_id} → ""
```

### Principle 3: Time-Ordered Keys Enable Range Deletion

```
# Good: can range-delete old entries
acked:{queue}:{timestamp}:{msg_id} → ""

# Bad: can't efficiently delete old entries
acked:{queue}:{msg_id} → {timestamp, ...}
```

### Principle 4: Counters for Cardinality

```
# Instead of: SELECT COUNT(*) FROM claimed WHERE queue = ?
# Use: meta:{queue} → {..., claimed_count: 42}
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
   - Now uses `QueueMeta` counters (O(1) single key lookup)
   - Counters updated atomically in claim/ack/nack operations

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
    let dedup_key = Self::dedup_key(queue, business_key);

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

fn dedup_key(queue: &str, business_key: &str) -> Vec<u8> {
    format!("dedup:{}:{}", queue, business_key).into_bytes()
}
```

**Design considerations:**
- Dedup keys need TTL/cleanup (job-scoped or time-based)
- Business key must be deterministic from source data
- See `anvil-semantics.md` Section 9 for full design discussion

---

_Last updated: 2026-02-02_
