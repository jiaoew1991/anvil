# Anvil Storage Format

This document is normative. It specifies every key and value anvil writes to
its SlateDB store, the rules a release follows when it opens a store, which
changes to the format are compatible, and how a breaking one is made. The
code that implements it is `src/keys.rs` (keys), `src/format.rs` and
`proto/storage.proto` (values), and `AnvilStorage::open_format` in
`src/storage.rs` (the open-time rules); where this document and the code
disagree, one of them has a bug.

## 1. Policy

- **Every release opens the data of every release since format 1.** A store
  written by any release with a versioned format must open, read and behave
  correctly under every later release. A change that would break that is a
  breaking format change, made only as section 6 lays out.
- **Data of unversioned releases is refused.** Releases up to and including
  0.3.0 kept no format marker. Their stores are refused at open, not read and
  not migrated.
- **Rollback is not supported.** A store opened by a release is migrated to
  that release's format; an older release then refuses it as newer.
- The gRPC API, its error codes and the Python API are not part of the
  format and may change freely before 1.0.

## 2. Format version and open-time rules

The format version is a positive integer. This document specifies
**version 1**, the first versioned format, shipped from release 0.4.0 on.
A build supports exactly one version, `format::FORMAT_VERSION`.

The **format marker** is the key `0x00` (one byte). Its value is a
`StoreFormat` message whose `version` is the store's format version. No other
key starts with `0x00`, and it is the smallest possible key, so the first key
of a store tells every case apart. On open, before any other read or write,
a release reads the first key of the store and:

| First key | Meaning | Action |
|---|---|---|
| none | an empty store | write the marker of `FORMAT_VERSION`, durably, as a single put, then proceed |
| the marker, version equal to `FORMAT_VERSION` | this format | proceed |
| the marker, version lower | an older versioned format | run the migrations from that version up to `FORMAT_VERSION`, in order (section 6), then proceed |
| the marker, version higher | written by a newer release | refuse: `FormatError::Newer`, raised in Python as `StorageFormatError` |
| any other key | data of an unversioned release (0.3.0 or earlier) | refuse: `FormatError::Unversioned`, raised in Python as `StorageFormatError` |

A refused store is closed without a write to its keys, so a release that can
read it still finds it as it was. A marker that does not decode, or holds version 0,
is a corrupt store and fails the open.

Because the stamp is one put, a crash during the first open leaves either an
empty store, which the next open stamps, or a stamped one. The marker is
never deleted or rewritten, except by a migration as its last durable write.

## 3. Keys

### 3.1 Encoding

A key is a tuple, in the style of the FoundationDB tuple layer: one family
tag byte, then the family's fields in a fixed order. The tag fixes the field
types, so fields carry no type codes.

- **String** (UTF-8; any content, including `:` and NUL): the bytes, with
  every `0x00` written as `0x00 0xFF`, followed by the terminator
  `0x00 0x01`. The encoding is order-preserving (strings compare as their
  bytes do) and prefix-free (no encoded string is a prefix of another), so a
  key prefix that ends in an encoded string matches exactly the keys of that
  string.
- **u64**: 8 bytes, big-endian, so numeric order is byte order.

Family tags are below `0x20`. The keys of the unversioned releases began
with a printable ASCII byte, so none of them reads as a key of this format.
A tag, once assigned, is never reused for another layout.

### 3.2 Families

`str(x)` is an encoded string and `u64(x)` an encoded integer.

| Tag | Family | Key after the tag | Value |
|---|---|---|---|
| `0x00` | format marker | (nothing: the key is the one byte) | `StoreFormat` |
| `0x01` | queue | `str(queue)` | `QueueRecord` |
| `0x02` | total pushed | `str(queue)` | total (section 4.2) |
| `0x03` | total acked | `str(queue)` | total (section 4.2) |
| `0x04` | pending | `str(queue) u64(seq)` | `PendingEntry` |
| `0x05` | message | `str(queue) str(msg_id)` | `StoredMessage` |
| `0x06` | claim | `str(queue) str(msg_id)` | `ClaimRecord` |
| `0x07` | acked | `str(queue) u64(timestamp_ns) str(msg_id)` | empty |
| `0x08` | finished | `str(queue)` | empty |
| `0x09` | queue group | `str(group)` | `QueueGroup` |
| `0x0A` | state | `str(namespace) str(key)` | the caller's bytes |

For example, the pending key of seq 258 in queue `q:1` is
`04 71 3a 31 00 01 00 00 00 00 00 00 01 02`, and the state key of namespace
`ns:x`, key `k\0` is `0a 6e 73 3a 78 00 01 6b 00 ff 00 01`.

### 3.3 Ordering guarantees

Within a family, keys sort by their fields in order, each field by its own
order. Anvil relies on these ranges, each of which holds exactly the keys
named and no key of any other queue, group or namespace:

- **All queues** (`list_queues`): every key with tag `0x01`.
- **A queue's pending entries in seq order**: `0x04 str(queue)` as a prefix.
  The oldest entry is the claim cursor and the newest one bounds the push
  cursor when a queue is loaded.
- **A queue's claims, or all claims** (`count_claimed`, recovery,
  `delete_queue`): `0x06 str(queue)` as a prefix, or tag `0x06`.
- **A queue's acked entries in timestamp order**, and those acked before a
  cutoff (GC): from `0x07 str(queue)` up to, excluding,
  `0x07 str(queue) u64(cutoff_ns)`: one contiguous range.
- **A namespace's state keys starting with a prefix**:
  `0x0A str(namespace)` followed by the prefix escaped as a string, without
  the terminator. Escaping maps each byte on its own, so the escaped
  key starts with the escaped prefix exactly when the key starts with the
  prefix.

### 3.4 What each family records

- **Queue.** A queue exists exactly while its record does. Push, claim,
  forward and scatter create a queue on first use, `CreateQueue` creates it
  explicitly, and a queue group creates its partitions.
- **Totals.** Lifetime counts, merged as increments (section 4.2). A queue
  whose total was never merged into has no key, which reads as 0.
- **Pending.** One key per message waiting to be claimed, at its seq. A push
  or a requeue (nack, reclaim) appends at the tail; a claim removes the key.
- **Message.** The body of every message that is pending or claimed. An ack
  deletes it with the claim.
- **Claim.** One key per message claimed and not yet settled.
- **Acked.** One empty key per ack, until GC removes it after the retention
  period.
- **Finished.** Present once the queue is marked finished.
- **Queue group.** The group's partitions; its partition queues are ordinary
  queues.
- **State.** The caller's key-value state, by namespace.

No cursor or in-flight count is persisted: a loaded queue derives its claim
cursor and push cursor from its oldest and newest pending keys and its
claimed count from its claim keys. A persisted absolute value could be
overtaken by an older one written by a concurrent writer, so only totals,
which are merged, are persisted.

## 4. Values

### 4.1 Protobuf messages

Every structured value is a protobuf (proto3) message defined in
`proto/storage.proto`, which is part of this specification: its field
numbers, types and comments are normative. It is separate from the wire API
in `proto/anvil.proto`. The messages are:

| Message | Value of | Notes |
|---|---|---|
| `StoreFormat` | the format marker | `version` is at least 1 |
| `QueueRecord` | a queue | `max_pending` 0 is unbounded |
| `PendingEntry` | a pending key | `msg_id` is never empty; `delivery_count` counts earlier deliveries |
| `StoredMessage` | a message key | the queue and msg_id are the key's, not repeated |
| `ClaimRecord` | a claim key | the queue and msg_id are the key's; `claim_token` is never empty; `delivery_count` is at least 1 |
| `QueueGroup` | a queue group key | the name is the key's; `partition_queues` has `num_partitions` entries |
| `ReclaimEvent` | a state value (below) | written by recovery |

Protobuf leaves the order of map entries (`StoredMessage.metadata`) to the
encoder; a reader must accept any order.

**Reclaim events.** When recovery returns a claim to pending, it writes a
`ReclaimEvent` as a state value in namespace `wq_events`, under the state key
`timeout:{queue}:{timestamp_ns, 20 digits}:{msg_id}`. The key is a name, not
a tuple: the event's fields are in the value.

### 4.2 Totals

A total is a u64, 8 bytes **little-endian**, not protobuf, so that SlateDB's
merge operator can sum it. Every operation that changes a total merges its
increment (8 bytes little-endian) in the batch of the operation, never an
absolute value. The merge operator sums the existing value and every operand
and refuses, failing the write, a merge on any key whose tag is not one of
the totals (`0x02`, `0x03`) or an operand that is not 8 bytes.

### 4.3 Empty and opaque values

Acked and finished keys have an empty value; a reader must ignore any bytes
there. State values are the caller's bytes, stored as given.

## 5. Compatible and breaking changes

A change is **compatible** when every store a release of the current format
wrote still reads and behaves correctly under the new code, and the new code
writes nothing that an earlier release of the same format would misread.
Compatible changes keep the version:

- adding an optional field to a protobuf message, with a fresh field number,
  when its absence (the proto3 default) means what the old code did;
- adding a key family, under a fresh tag, when its absence has a defined
  meaning that matches every existing store (as an absent total reads as 0);
- adding a protobuf message for a new family;
- relaxing what a reader accepts.

Every other change to what is stored is **breaking**, among them:

- changing the layout of any key: a tag, a field, the field order or a
  field's encoding;
- changing a value's encoding, a field's number or type, or the encoding of a
  total;
- changing what an existing value means, such as a field's unit, a default's
  meaning, or the invariant that ties two families together;
- removing a family or a field that a reader still needs to interpret data;
- reusing a removed tag or field number.

The snapshot test (section 7) catches a changed encoding; a changed meaning
it cannot catch, so a reviewer must.

## 6. Making a breaking change

1. **Bump the version.** Increase `format::FORMAT_VERSION` by one and update
   this document to specify the new version. Keep the previous version's
   specification in the history of this file.
2. **Write the migration** from the previous version to the new one and add
   it to `MIGRATIONS` in `src/storage.rs`. `MIGRATIONS[i]` migrates version
   `i + 1` to `i + 2`; the array's length is `FORMAT_VERSION - 1`, so a bump
   without a migration does not compile, and the open-time check asserts that
   each migration left the marker at its target version. Every earlier
   versioned format then reaches the new one through the chain.
3. **Make the migration safe to interrupt.** It runs at open, before anything
   else, and a crash may stop it anywhere. It must be:
   - **idempotent**: running it again on data it already converted, in part
     or in full, changes nothing more;
   - **resumable**: a later open, which finds the old marker, finishes the
     work from wherever it stopped, by deriving what is left from the keys,
     not from progress kept in memory;
   - **flipped last**: its final durable write sets the marker to the new
     version, and nothing before it does. A store with the new marker is
     fully migrated.

   Large migrations write in bounded batches, each durable before the next.
4. **Test it** as section 7 requires, including a crash at arbitrary points.
5. **Update the snapshot**, the writer and the verifier for the new format,
   keeping the verifier accepting the stores of every earlier writer.

## 7. Testing rules

- **Format snapshot** (`format::tests::test_format_snapshot`). It encodes a
  fixed, representative key of every family and value of every message with
  every encoder and compares the bytes with hex committed in the test.
  Changing an encoding fails it. Updating an existing entry means a breaking
  change (section 6); a compatible change only adds entries.
- **Key ordering and exactness** (`keys::tests`). Strings with `:` and NUL
  keep their order and parse back whole, and every range of section 3.3 holds
  exactly its keys.
- **Open-time rules** (`storage::tests`). An unversioned store and a newer
  one are refused and left untouched; an empty one is stamped; a crash
  during the first open's stamp leaves a store that opens and serves.
- **Cross-version harness** (`src/format_compat.rs`,
  `scripts/format-compat.sh`, the `format-compat` CI job). The writer
  (`format_compat::tests::format_compat_write`, run with `--ignored`)
  populates the `file://` store named by `ANVIL_FORMAT_COMPAT_DIR` with every
  state a queue can hold: pending, claimed, acked, nacked and reclaimed
  messages, a finished queue, a queue group with scattered partitions, state
  with `:` and NUL, a bounded queue, and acked entries inside and past
  retention. The verifier (`format_compat_verify`) opens such a store and
  checks counts, contents and behaviour: claims, acks with the writer's
  tokens, redelivery, completion, GC and the bound. CI runs the writer of
  every release tag that ships `src/format_compat.rs`, and of the current
  code, and the current verifier against each store.

  A tagged release's writer never changes, so the writer's test name, the
  environment variable and the scenario it writes are part of this contract.
  A new writer that writes more records a new scenario number, and the
  verifier keeps checking every earlier scenario.
- Every test that needs a key builds it with `keys`; no code outside
  `src/keys.rs` builds or parses a key.
