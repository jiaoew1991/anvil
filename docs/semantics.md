# Anvil Semantics: Data Consistency, Fault Tolerance, and Recovery

_Design document - February 2026_

---

## Status

**Status**: 📝 READY FOR REVIEW
**Author**: AI Assistant
**Created**: 2026-02-02
**Last Discussion**: 2026-02-02 - Added trade-off analysis and design evolution

This document describes the semantic guarantees and recovery mechanisms for the new Anvil-based architecture introduced in PR #35. It supersedes:
- `deprecated/exactly-once-semantics.md` (deprecated)
- `deprecated/checkpoint-and-recovery.md` (deprecated)

---

## Table of Contents

1. [Background: Why New Design](#1-background-why-new-design)
2. [Anvil Model Overview](#2-anvil-model-overview)
3. [Semantic Guarantees](#3-semantic-guarantees)
4. [Fault Tolerance Mechanisms](#4-fault-tolerance-mechanisms)
5. [Recovery Scenarios](#5-recovery-scenarios)
6. [Design Decisions](#6-design-decisions)
7. [Implementation Roadmap](#7-implementation-roadmap)
8. [Open Questions](#8-open-questions)
9. [Design Evolution: Discussion Process and Trade-offs](#9-design-evolution-discussion-process-and-trade-offs)

---

## 1. Background: Why New Design

### 1.1 Old Model Problems

The previous Tansu/Kafka partition model had fundamental issues:

| Problem | Impact |
|---------|--------|
| Partition-worker coupling | Idle workers when `workers > partitions` |
| Offset-based recovery | Requires deterministic message ordering |
| Partition rebalancing | Complex coordinator logic, failure-prone |
| No true round-robin | Hot partitions cause load imbalance |

### 1.2 New Anvil Model

Anvil uses a **single-queue multi-consumer** model:

```
┌─────────────────────────────────────────────────────────────┐
│                   Anvil Server (Rust)                    │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐                 │
│  │ PENDING  │──▶│ CLAIMED  │──▶│  ACKED   │──▶ GC (delete)  │
│  │ (queue)  │   │(leased)  │   │(retained)│                 │
│  └──────────┘   └──────────┘   └──────────┘                 │
│       ▲                             │                        │
│       │         timeout             │                        │
│       └─────────(nack)──────────────┘                        │
└─────────────────────────────────────────────────────────────┘
        ▲                    ▲                    ▲
        │ claim              │ claim              │ claim
   ┌────┴────┐          ┌────┴────┐          ┌────┴────┐
   │Worker 1 │          │Worker 2 │          │Worker N │
   └─────────┘          └─────────┘          └─────────┘
```

Key differences:
- **No partitions**: Workers compete for any message
- **Claim-based**: Messages are "leased" to workers with timeout
- **Automatic recovery**: Timed-out claims return to pending queue
- **Work-stealing**: Natural load balancing

---

## 2. Anvil Model Overview

### 2.1 Message States

```
PENDING ──claim()──▶ CLAIMED ──ack()──▶ ACKED ──GC──▶ DELETED
    ▲                    │
    │                    │ timeout/nack()
    └────────────────────┘
```

| State | Description | Visibility |
|-------|-------------|------------|
| PENDING | Ready for processing | All workers can claim |
| CLAIMED | Being processed by a worker | Other workers cannot claim |
| ACKED | Processing confirmed | Retained for safety (GC-based) |
| DELETED | Garbage collected | Removed from storage |

### 2.2 Key Operations

| Operation | Atomicity | Description |
|-----------|-----------|-------------|
| `push(queue, payload)` | Atomic | Add message to pending queue |
| `claim(queue, batch_size)` | Atomic | Move messages from pending to claimed |
| `ack(queue, msg_ids)` | Atomic | Move messages from claimed to acked |
| `ack_and_forward(...)` | Atomic | Ack upstream + push downstream + update state |
| `nack(queue, msg_ids)` | Atomic | Return messages to pending (at tail) |

### 2.3 Storage Model (SlateDB)

```
Key Schema:
  meta:{queue}                          → {claim_seq, push_seq}
  pending:{queue}:{seq:020d}            → msg_id
  msg:{queue}:{msg_id}                  → Message JSON
  claimed:{queue}:{msg_id}              → ClaimInfo JSON
  acked:{queue}:{timestamp_ns}:{msg_id} → ""
  state:{namespace}:{key}               → value bytes
```

All operations use `WriteBatch` for atomicity.

---

## 3. Semantic Guarantees

### 3.1 Current Implementation: At-Least-Once

The current implementation provides **at-least-once** delivery:

```
Guarantee: Every message is processed at least once.
           Duplicates may occur on failure recovery.
```

**How it works:**
1. Worker claims message (PENDING → CLAIMED)
2. Worker processes message and produces output
3. Worker acks message (CLAIMED → ACKED)
4. If worker crashes before ack, message times out and returns to PENDING
5. Another worker claims and reprocesses the message

#### ✅ Atomic Ack + Forward (Fixed)

The `stage_worker.py` implementation uses `ack_and_forward` for atomic operations:

```python
# stage_worker.py - _run_claim_loop
output_bytes = await self._process_message(message, record, split_id)

if output_bytes and self.output_queue_name:
    # Atomic: ack upstream + push downstream
    self.upstream_queue.ack_and_forward(
        upstream_queue=self.upstream_queue_name,
        upstream_msg_ids=[record.msg_id],
        downstream_queue=self.output_queue_name,
        downstream_payloads=[output_bytes],
    )
else:
    # No output, just ack
    self.upstream_queue.ack(self.upstream_queue_name, [record.msg_id])
```

**No duplicate window**: Downstream only receives data after atomic commit succeeds.

#### With ack_and_forward: Remaining Edge Cases

Even with atomic `ack_and_forward`, one edge case remains:

**Payload Store Side Effects:**
```
t1: Worker A claims message M
t2: Worker A processes M (CPU/GPU work done)
t3: Worker A stores output in payload_store (Ray Object Store)
t4: [CRASH] before ack_and_forward()
t5: Processing work is lost, needs re-execution

This is NOT a duplicate issue, but a wasted computation issue.
The downstream only receives data after ack_and_forward succeeds.
```

**Mitigation**: Since `payload_key = split_id` (deterministic), re-processing
will overwrite the same key. No duplicate data, just wasted CPU/GPU cycles.

### 3.2 Achieving Exactly-Once

True exactly-once requires handling two aspects:

#### 3.2.1 Source Deduplication (Input Side)

**Option A: Message ID Deduplication**

Store processed message IDs in Anvil state:

```python
async def process_with_dedup(self, msg):
    # Check if already processed
    seen = self.queue_client.state_get(
        namespace=f"{self.job_id}/{self.stage_id}",
        keys=[msg.msg_id]
    )

    if msg.msg_id in seen:
        # Already processed, just ack
        self.queue_client.ack(queue, [msg.msg_id])
        return

    # Process message
    output = self.operator.process(msg)

    # Atomic: ack + mark as seen + forward output
    self.queue_client.ack_and_forward(
        upstream_queue=queue,
        upstream_msg_ids=[msg.msg_id],
        downstream_queue=output_queue,
        downstream_payloads=[output],
        state_namespace=f"{self.job_id}/{self.stage_id}",
        state_puts={msg.msg_id: b"1"}  # Mark as seen
    )
```

**Pros:**
- Works with any message ordering
- No offset tracking needed

**Cons:**
- State grows with message count (need TTL/cleanup)
- Extra storage overhead

**Option B: Idempotent Processing Key**

For messages with natural keys (e.g., document ID), use key-based deduplication:

```python
async def process_with_key_dedup(self, msg):
    doc_id = msg.metadata["doc_id"]
    version = msg.metadata["version"]

    # Check if newer version already processed
    stored = self.queue_client.state_get(
        namespace=f"{self.job_id}/{self.stage_id}",
        keys=[f"processed:{doc_id}"]
    )

    if stored.get(f"processed:{doc_id}"):
        stored_version = int(stored[f"processed:{doc_id}"])
        if version <= stored_version:
            # Older or same version, skip
            self.queue_client.ack(queue, [msg.msg_id])
            return

    # Process and update version
    ...
```

#### 3.2.2 Sink Deduplication (Output Side)

Most sinks can be made idempotent:

| Sink Type | Idempotency Strategy |
|-----------|---------------------|
| Object Storage (S3/GCS) | Use deterministic keys (e.g., `{split_id}.parquet`) |
| Lance/Iceberg Tables | Upsert with primary key |
| Database | Use `INSERT ... ON CONFLICT DO UPDATE` |
| API Calls | Include idempotency key in request |

### 3.3 Recommended Approach

```
┌─────────────────────────────────────────────────────────────┐
│              Exactly-Once = At-Least-Once +                  │
│              Deduplication at Boundaries                     │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Source ──▶ [Stage 1] ──▶ [Stage 2] ──▶ ... ──▶ Sink        │
│    │            │            │                    │          │
│    │            │            │                    │          │
│    ▼            ▼            ▼                    ▼          │
│  Replay     Atomic        Atomic            Idempotent       │
│  Capable   Ack+Forward   Ack+Forward         Writes          │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

**Implementation status:**

| Component | Status | Notes |
|-----------|--------|-------|
| Atomic ack_and_forward | ✅ Done | storage.rs |
| StageWorker uses ack_and_forward | ✅ Done | stage_worker.py |
| State API | ✅ Done | state_get/state_put |
| Message ID dedup in worker | ❌ Not done | Need to implement |
| Idempotent sinks | ⚠️ Partial | Lance/Iceberg OK, others need work |
| State cleanup (TTL) | ❌ Not done | Need GC for dedup state |

---

## 4. Fault Tolerance Mechanisms

### 4.1 Message-Level Recovery

**Automatic claim timeout recovery:**

```rust
// recovery.rs - runs periodically
pub async fn recover_expired_claims(&self, timeout_secs: f64) {
    let now = now_secs();
    let all_claimed = self.scan_claimed(None).await?;

    for (queue, msg_id, claim_info) in all_claimed {
        if now - claim_info.claimed_at > timeout_secs {
            // Return to pending queue (at tail)
            self.nack_messages(&queue, &[msg_id]).await?;
        }
    }
}
```

**Configuration:**
- `lease_timeout_secs`: Default 60s - how long a lease may miss heartbeats before its worker is considered dead and all its claims are reclaimed
- `claim_age_timeout_secs`: Default 60s - how long a claim under a live lease may go without being taken or renewed before it is considered stuck. A task that may run longer renews its claims (`RenewClaims`) from its processing loop; renewing from a background timer would keep a stuck task's claims alive
- `recovery_interval_secs`: Default 10s - how often to check for expired claims

### 4.2 Worker-Level Recovery

**RecoveryManager** handles worker failures:

```python
# recovery_manager.py
async def recover_failed_workers(self, failed_worker_ids):
    # 1. Track failures with sliding window
    self._tracker.record_failures(len(failed_worker_ids), current_worker_count)

    # 2. Check if should give up (failure rate too high)
    if self.should_give_up(current_worker_count):
        return RecoveryResult(should_give_up=True, reason="...")

    # 3. Apply exponential backoff
    delay = self.get_recovery_delay()

    # 4. Spawn replacement workers
    for _ in range(len(failed_worker_ids)):
        worker_id = await self._worker_manager.spawn_worker()
        # Notify of upstream completion if applicable
        await self._worker_manager.notify_worker_upstream_finished(worker_id)

    await asyncio.sleep(delay)
```

**Key behaviors:**
- Exponential backoff: Prevents rapid respawn loops
- Failure rate threshold: Give up if failures exceed threshold
- Upstream notification: New workers know if upstream finished

### 4.3 Stage-Level Recovery

**StageMaster** coordinates stage execution:

```python
# stage_master.py
async def run(self):
    while self._running and not self._finished:
        # Check if all workers done
        if self._worker_manager.worker_count == 0:
            if self._has_unprocessed_messages():
                # Spawn worker to process remaining
                await self._worker_manager.spawn_worker()
            else:
                self._finished = True
                break

        # Wait for worker completion (event-driven)
        completed, failed = await self._worker_manager.wait_for_completion()

        # Handle failures with recovery
        if failed:
            result = await self._recovery_manager.recover_failed_workers(failed)
            if result.should_give_up:
                self._failed = True
                break
```

### 4.4 Job-Level Recovery

**Current state: NOT IMPLEMENTED**

Job-level recovery (resume after driver crash) requires:
1. Persisting job state (stage progress, queue positions)
2. Reconstructing stage masters on restart
3. Reconnecting to existing Anvil broker

**Future design considerations:**
- Anvil data is persisted in SlateDB (survives restarts)
- Need to persist: job config, stage topology, completion status
- Option: Store job state in Anvil state API

---

## 5. Recovery Scenarios

### 5.1 Scenario: Worker Crash During Processing

```
Timeline:
  t1: Worker A claims message M (PENDING → CLAIMED)
  t2: Worker A processing M...
  t3: [Worker A CRASH]
  t4: lease_timeout expires (60s default)
  t5: RecoveryTask runs, M returns to PENDING
  t6: Worker B claims M
  t7: Worker B processes M successfully
  t8: Worker B acks M (CLAIMED → ACKED)

Result: Message processed (possibly twice if A produced partial output)
```

### 5.2 Scenario: Worker Crash After Processing, Before Ack

```
Timeline:
  t1: Worker A claims message M
  t2: Worker A processes M
  t3: Worker A produces output O to downstream queue
  t4: [Worker A CRASH before ack]
  t5: M times out, returns to PENDING
  t6: Worker B claims M
  t7: Worker B processes M again
  t8: Worker B produces output O' to downstream queue
  t9: Worker B acks M

Result: Downstream receives O and O' (duplicates)
        → Need sink deduplication or source dedup state
```

### 5.3 Scenario: StageMaster Crash

```
Timeline:
  t1: StageMaster running with 3 workers
  t2: [StageMaster CRASH]
  t3: Workers continue processing (Ray actors survive)
  t4: Workers eventually complete or timeout
  t5: Job fails (driver lost)

Current behavior: Job fails, need manual restart
Future: Job-level recovery could resume
```

### 5.4 Scenario: Anvil Broker Crash

```
Timeline:
  t1: Anvil broker running
  t2: [Broker CRASH]
  t3: All workers lose connection
  t4: Workers retry connection (grpc retry)
  t5: If broker restarts: SlateDB state recovered, processing resumes
  t6: If broker doesn't restart: Job fails

Data safety: SlateDB persists to disk/S3, no message loss
```

### 5.5 Scenario: Network Partition

```
Timeline:
  t1: Worker A claims message M
  t2: [Network partition - Worker A isolated]
  t3: Worker A processes M locally
  t4: Worker A cannot ack (network down)
  t5: claim_timeout expires on server
  t6: Server recovers M to PENDING
  t7: Worker B claims and processes M
  t8: Network heals
  t9: Worker A's late ack fails (message already acked by B)

Result: Message processed twice
        → Same as crash scenario, need deduplication
```

---

## 6. Design Decisions

### 6.1 Why GC-Based Ack (Not Immediate Delete)

**Decision**: Messages move to ACKED state on ack, deleted later by GC.

**Rationale:**
1. **Safer recovery**: If we need to inspect recent processing, messages are still there
2. **Debugging**: Can trace message flow post-hoc
3. **Auditability**: Know what was processed and when

**Trade-off:**
- More storage usage (retained messages)
- Mitigated by configurable retention (`acked_retention_secs`, default 1 hour)

### 6.2 Why No Offset-Based Deduplication

**Decision**: Use message ID or key-based deduplication instead of offsets.

**Rationale:**
1. **No global ordering**: Anvil doesn't guarantee message order
2. **Work-stealing**: Any worker can process any message
3. **Simpler model**: No need to track "last processed offset" per partition

**Trade-off:**
- Need to store processed message IDs (state growth)
- Mitigated by TTL-based state cleanup

### 6.3 Why Nack Returns to Tail (Not Head)

**Decision**: `nack()` appends message to end of pending queue, not front.

**Rationale:**
1. **Avoid poison messages**: If message repeatedly fails, other messages still progress
2. **Fairness**: Failed messages don't starve new messages
3. **Natural backoff**: Failed message waits for queue to drain before retry

**Trade-off:**
- Failed messages take longer to retry
- Mitigated by limited retry count (can configure max_retries)

### 6.4 Why Single Broker Per Job

**Decision**: Each job gets its own Anvil broker instance.

**Rationale:**
1. **Isolation**: Jobs don't interfere with each other
2. **Cleanup**: Easy to delete all state when job completes
3. **Scaling**: Can run broker on different nodes for different jobs

**Trade-off:**
- More processes to manage
- Mitigated by Ray actor management

---

## 7. Implementation Roadmap

### Phase 1: Core Semantics (Current)

- [x] Anvil broker with claim/ack/nack
- [x] Atomic ack_and_forward
- [x] State API (state_get/state_put)
- [x] Automatic claim timeout recovery
- [x] GC for acked messages

### Phase 2: Exactly-Once Support

- [ ] `push_with_dedup` API in Anvil server (business_key based dedup)
- [ ] Source-level dedup integration (Lance Source with rowid, Spark Source with user-specified key)
- [x] Fix `stage_worker.py` to use `ack_and_forward` instead of separate push+ack ✅
- [ ] Idempotent sink implementations
- [ ] State TTL and cleanup for dedup markers
- [ ] Configuration for semantic guarantee level

### Phase 3: Enhanced Recovery

- [ ] Job-level checkpoint (persist stage progress)
- [ ] Job resume after driver crash
- [ ] State snapshot/restore for debugging

### Phase 4: Observability

- [ ] Dedup metrics (messages skipped due to dedup)
- [ ] Recovery metrics (claims recovered, workers respawned)
- [ ] State size metrics (dedup state growth)

---

## 8. Open Questions

### 8.1 State Cleanup Strategy

**Question**: How to clean up dedup state after job completion?

**Options:**
1. **Job-scoped namespace**: Delete all state with prefix `{job_id}/` on completion
2. **TTL-based**: Each state entry has expiration, cleaned by GC
3. **Manual**: User explicitly calls cleanup API

**Recommendation**: Option 1 (job-scoped) for simplicity, with TTL as fallback for long-running jobs.

### 8.2 Max Retry Count

**Question**: Should we limit how many times a message can be nacked?

**Current**: No limit, message keeps retrying forever.

**Options:**
1. **Unlimited retries**: Simple, but poison messages never die
2. **Max retries with dead-letter queue**: Move to DLQ after N failures
3. **Max retries with skip**: Log and drop after N failures

**Recommendation**: Option 2 for production, Option 3 for dev/test.

### 8.3 Cross-Stage Transactions

**Question**: How to handle multi-stage atomic operations?

**Current**: Each stage is independent, no cross-stage transactions.

**Example scenario:**
```
Stage A produces to Stage B and Stage C
Want: Either both B and C receive, or neither
```

**Options:**
1. **Accept eventual consistency**: B and C may receive independently
2. **Two-phase commit**: Complex, performance impact
3. **Saga pattern**: Compensating transactions on failure

**Recommendation**: Option 1 for now, document limitation.

### 8.4 Performance of State-Based Dedup

**Question**: Will state_get for every message be a bottleneck?

**Analysis:**
- SlateDB read: ~100k ops/sec (mostly cache hits)
- Batch state_get: Can fetch multiple keys in one RPC
- Pre-fetch pattern: Claim returns messages + fetch dedup state together

**Mitigation:**
```python
# Batch dedup check
msgs = client.claim(queue, batch_size=100)
msg_ids = [m.msg_id for m in msgs]

# Single RPC for all dedup checks
seen = client.state_get(namespace, msg_ids)

# Filter already processed
new_msgs = [m for m in msgs if m.msg_id not in seen]
```

---

## 9. Design Evolution: Discussion Process and Trade-offs

This section documents the reasoning journey that led to our final design. The conclusion is important, but the process of elimination and trade-off analysis is equally valuable for understanding why alternatives were rejected.

### 9.1 Initial Complexity: Approaches Considered

When analyzing the exactly-once problem, several approaches were initially considered:

#### Approach 1: Anti-Join at Source (Rejected)

**Idea**: Before Source pushes a new batch, query the downstream (or state store) to find which records were already processed, then anti-join to skip them.

```python
# Pseudocode for anti-join approach
def push_batch(records):
    existing_ids = sink.query_processed_ids()  # Query downstream
    new_records = [r for r in records if r.id not in existing_ids]
    queue.push_batch(new_records)
```

**Why rejected**:
- Requires sink to expose a query API (not all sinks support this)
- In Spark DataFrame scenarios, there's no natural primary key to query
- High latency: additional query before every push
- Complexity: need to handle query failures, pagination, etc.

#### Approach 2: Record-Level Deduplication (Rejected)

**Idea**: Track every processed record ID in a deduplication store (Bloom filter, Redis set, etc.).

```python
# Pseudocode for record-level dedup
def process_record(record):
    if dedup_store.contains(record.id):
        return  # Skip duplicate
    result = transform(record)
    dedup_store.add(record.id)  # Mark as processed
    emit(result)
```

**Why rejected**:
- Unbounded state growth: every record ID must be stored
- Bloom filters have false positives (may incorrectly skip new records)
- Redis/external store adds operational complexity and latency
- For high-volume scenarios (billions of records), space becomes prohibitive

#### Approach 3: GroupBy Split ID for Dedup Key (Rejected)

**Idea**: Use GroupBy's group key as the deduplication key, ensuring all records with the same group key are deduplicated together.

**Why rejected**:
- Group keys can be highly skewed (e.g., 80% of records in one group)
- This is a correctness concern for parallelism, not deduplication
- GroupBy semantics are about aggregation, not identity
- Split ID and group key serve different purposes

### 9.2 Key Insight: split_id vs msg_id Alignment

A critical question arose: **Can split_id serve as the deduplication key? Is it aligned with msg_id?**

Investigation revealed:
```python
# models.py:561-575
def make_split_id(job_id: str, stage_id: str, msg_id: str) -> str:
    return f"{job_id}/{stage_id}/{msg_id}"
```

**Finding**: `msg_id` is Anvil's internal UUID (non-deterministic), while `split_id` is derived from it. This means:
- Each message push generates a NEW `msg_id` (UUID)
- If Source replays data, the SAME data gets a DIFFERENT `msg_id`
- Split-level dedup using `msg_id` is ineffective for Source replay

**Conclusion**: Deduplication must happen at the Source level with business-meaningful keys, not at the Queue level with internal UUIDs.

### 9.3 Finding the Essential Problem

After considering all approaches, we identified the **single essential problem**:

```
┌─────────────────────────────────────────────────────────────┐
│   The ONLY source of duplicates is Source replay after     │
│   job restart. All other scenarios are handled by          │
│   ack_and_forward atomicity + claim timeout recovery.      │
└─────────────────────────────────────────────────────────────┘
```

**Why other scenarios are NOT duplicate sources**:

| Scenario | Why NOT a Duplicate Source |
|----------|---------------------------|
| Worker crash before ack | With `ack_and_forward`, downstream receives data ONLY after atomic commit |
| Worker crash after ack | Message already acked, won't be redelivered |
| Network partition | Claim timeout returns message to pending; reprocessing is expected |
| Broker crash | SlateDB persists state; recovery resumes from persisted state |

**The real problem**: When a job restarts (not worker, but entire job), the Source may replay data that was already processed in a previous run.

### 9.4 The Simplified Solution

Based on the essential problem identification, the solution simplifies to:

```
┌─────────────────────────────────────────────────────────────┐
│                  Simplified Exactly-Once                     │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  1. Source: push_with_dedup(business_key, payload)          │
│     - Deterministic key from business data (e.g., rowid)    │
│     - Queue-level dedup rejects duplicates                  │
│                                                              │
│  2. Stages: ack_and_forward (already atomic)                │
│     - No additional dedup needed between stages             │
│     - Internal msg_id is sufficient for queue operations    │
│                                                              │
│  3. Sink: Idempotent writes                                 │
│     - Use deterministic output keys                         │
│     - Upsert semantics for databases                        │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### 9.5 What We Chose NOT to Do

| Decision | Rationale |
|----------|-----------|
| No anti-join at Source | Too complex, requires sink query capability |
| No record-level dedup | Unbounded state, overkill for the actual problem |
| No Bloom filters | False positives unacceptable for correctness |
| No cross-stage dedup state | `ack_and_forward` atomicity makes it unnecessary |
| No GroupBy-aware split_id | Correctness issue for parallelism, not dedup |

### 9.6 Source-Specific Deduplication Strategies

Since Source replay is the essential problem, different Source types need different strategies:

#### Lance Source (Easy)

```python
# Lance has natural row IDs
def push_lance_batch(fragments):
    for fragment in fragments:
        for row in fragment:
            business_key = f"{fragment.id}:{row.rowid}"
            queue.push_with_dedup(business_key, row.data)
```

#### Spark Source (Challenging)

Spark DataFrames don't have natural row IDs. Options:

1. **User-provided key column**: Require users to specify a unique key column
   ```python
   source = SparkSource(df, dedup_key_col="id")  # User specifies
   ```

2. **Computed hash key**: Hash row content as dedup key (non-deterministic if row order changes)
   ```python
   dedup_key = hash(row.as_dict())  # Fragile: depends on row serialization
   ```

3. **Accept at-least-once**: For Spark sources without natural keys, accept duplicates
   ```python
   source = SparkSource(df, semantic=AT_LEAST_ONCE)  # Explicit opt-out
   ```

**Recommendation**: Option 1 for production use cases with critical data, Option 3 for exploratory/batch scenarios.

### 9.7 Implementation: push_with_dedup

The Queue server needs a new operation:

```python
# Client API
def push_with_dedup(
    self,
    queue: str,
    business_key: str,  # Deterministic key from source
    payload: bytes
) -> PushResult:
    """
    Push message with deduplication.

    Returns:
        PushResult with:
        - msg_id: Queue's internal ID (UUID)
        - deduplicated: True if message was skipped as duplicate
    """
    return self._client.push_with_dedup(queue, business_key, payload)
```

Server-side implementation:
```rust
// storage.rs (conceptual)
fn push_with_dedup(&self, queue: &str, business_key: &str, payload: &[u8]) -> Result<PushResult> {
    let dedup_key = format!("dedup:{}:{}", queue, business_key);

    // Check if already pushed
    if self.db.get(&dedup_key)?.is_some() {
        return Ok(PushResult { msg_id: None, deduplicated: true });
    }

    // Atomic: insert dedup marker + push message
    let msg_id = Uuid::new_v4().to_string();
    let mut batch = WriteBatch::new();
    batch.put(&dedup_key, msg_id.as_bytes());
    batch.put(&format!("msg:{}:{}", queue, msg_id), payload);
    batch.put(&format!("pending:{}:{:020}", queue, next_seq), msg_id.as_bytes());
    self.db.write(batch)?;

    Ok(PushResult { msg_id: Some(msg_id), deduplicated: false })
}
```

### 9.8 Summary: The Trade-off Framework

When designing for exactly-once, consider this decision framework:

```
                  ┌─────────────────┐
                  │ Where do dupes  │
                  │    come from?   │
                  └────────┬────────┘
                           │
         ┌─────────────────┼─────────────────┐
         ▼                 ▼                 ▼
   ┌───────────┐    ┌───────────┐    ┌───────────┐
   │  Source   │    │  Stage    │    │   Sink    │
   │  replay   │    │  crash    │    │  retry    │
   └─────┬─────┘    └─────┬─────┘    └─────┬─────┘
         │                │                │
         ▼                ▼                ▼
   ┌───────────┐    ┌───────────┐    ┌───────────┐
   │push_dedup │    │ack_forward│    │idempotent │
   │(need impl)│    │(have it)  │    │(best prac)│
   └───────────┘    └───────────┘    └───────────┘
```

**Key takeaway**: Don't add complexity in the middle (stages). Handle deduplication at the boundaries where the problem actually originates.

---

## Appendix A: Configuration Reference

```python
# Anvil Broker Config
AnvilBrokerManager(
    db_path="file:///tmp/anvil",  # SlateDB path (or s3://...)
    claim_timeout_secs=60.0,           # Claimed message timeout
    recovery_interval_secs=10.0,       # How often to check for timeouts
    acked_retention_secs=3600.0,       # How long to keep acked messages
    gc_interval_secs=60.0,             # How often to run GC
)

# Stage Config
Stage(
    stage_id="my_stage",
    operator_config=...,
    min_parallelism=1,                 # Minimum workers
    max_parallelism=10,                # Maximum workers
    batch_size=100,                    # Messages per claim
)

# Job Config (future)
JobConfig(
    semantic_guarantee=SemanticGuarantee.EXACTLY_ONCE,  # or AT_LEAST_ONCE
    dedup_state_ttl_secs=86400,        # 1 day TTL for dedup state
)
```

---

## Appendix B: Migration from Old Model

If migrating from the old Tansu/Kafka partition model:

1. **Remove partition-related code**:
   - `PartitionManager` (deleted in PR #35)
   - Partition assignment logic
   - Offset commit/fetch

2. **Update operator code**:
   - Remove `last_offset` tracking
   - Remove `is_duplicate(offset)` checks
   - Add message ID dedup if needed

3. **Update sink code**:
   - Ensure idempotent writes
   - Use deterministic output keys

4. **Update tests**:
   - Remove partition-specific tests
   - Add claim/ack/nack tests
   - Add dedup tests

---

_Last updated: 2026-02-02_
