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

//! The storage format version and the encoding of every persisted value.
//! docs/storage-format.md is the normative spec; `keys` lays out the keys.

use prost::Message as _;

use crate::types::{ClaimInfo, DeadLetter, DeadLetterCause, Message, QueueGroupMeta};

pub(crate) mod proto {
    tonic::include_proto!("anvil.storage");
}

/// The format this build writes. Opening a store of an earlier version
/// migrates it here (see `storage::MIGRATIONS`); a later one is refused.
pub(crate) const FORMAT_VERSION: u32 = 1;

/// A store this build will not open.
#[derive(Debug, PartialEq)]
pub enum FormatError {
    /// The store holds data but no format marker: it was written by a
    /// release before the format was versioned (0.3.0 or earlier).
    Unversioned,
    /// The store was written by a release with a newer format.
    Newer { version: u32 },
}

impl std::fmt::Display for FormatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FormatError::Unversioned => write!(
                f,
                "the store was written by an unversioned release (0.3.0 or earlier), \
                 whose data this release cannot read; start from an empty store"
            ),
            FormatError::Newer { version } => write!(
                f,
                "the store has storage format {version}, newer than format \
                 {FORMAT_VERSION} of this release; open it with a release that \
                 supports format {version}"
            ),
        }
    }
}

impl std::error::Error for FormatError {}

/// A value that does not decode as the schema of its key: a corrupt store.
#[derive(Debug)]
pub struct CorruptValue(pub(crate) String);

impl std::fmt::Display for CorruptValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for CorruptValue {}

fn decode<T: prost::Message + Default>(what: &str, bytes: &[u8]) -> Result<T, CorruptValue> {
    T::decode(bytes).map_err(|e| CorruptValue(format!("corrupt {what}: {e}")))
}

pub(crate) fn encode_format(version: u32) -> Vec<u8> {
    assert!(version >= 1);
    proto::StoreFormat { version }.encode_to_vec()
}

pub(crate) fn decode_format(bytes: &[u8]) -> Result<u32, CorruptValue> {
    let format: proto::StoreFormat = decode("format marker", bytes)?;
    if format.version == 0 {
        return Err(CorruptValue("format marker without a version".to_string()));
    }
    Ok(format.version)
}

pub(crate) fn encode_queue(max_pending: u64, max_deliveries: Option<u32>) -> Vec<u8> {
    proto::QueueRecord {
        max_pending,
        max_deliveries,
    }
    .encode_to_vec()
}

pub(crate) fn decode_queue(bytes: &[u8]) -> Result<proto::QueueRecord, CorruptValue> {
    decode("queue record", bytes)
}

/// A total, or an increment merged into one: 8 bytes little-endian, the
/// operand `TotalSumMergeOperator` sums.
pub(crate) fn encode_total(count: u64) -> [u8; 8] {
    count.to_le_bytes()
}

pub(crate) fn decode_total(bytes: &[u8]) -> Result<u64, CorruptValue> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| CorruptValue(format!("total of {} bytes, not 8", bytes.len())))?;
    Ok(u64::from_le_bytes(bytes))
}

pub(crate) fn encode_pending(msg_id: &str, delivery_count: u32) -> Vec<u8> {
    assert!(!msg_id.is_empty());
    proto::PendingEntry {
        msg_id: msg_id.to_string(),
        delivery_count,
    }
    .encode_to_vec()
}

pub(crate) fn decode_pending(bytes: &[u8]) -> Result<proto::PendingEntry, CorruptValue> {
    let entry: proto::PendingEntry = decode("pending entry", bytes)?;
    if entry.msg_id.is_empty() {
        return Err(CorruptValue("pending entry without a msg_id".to_string()));
    }
    Ok(entry)
}

/// The body of `msg`, as a message key and a dead letter both store it.
fn stored_message(msg: &Message) -> proto::StoredMessage {
    proto::StoredMessage {
        payload: msg.payload.clone(),
        created_at: msg.created_at,
        metadata: msg.metadata.clone(),
    }
}

/// The message `msg_id` whose body is `stored`.
fn message_of(msg_id: &str, stored: proto::StoredMessage) -> Message {
    Message {
        msg_id: msg_id.to_string(),
        payload: stored.payload,
        created_at: stored.created_at,
        metadata: stored.metadata,
    }
}

pub(crate) fn encode_message(msg: &Message) -> Vec<u8> {
    stored_message(msg).encode_to_vec()
}

/// The message `msg_id` whose body is `bytes`.
pub(crate) fn decode_message(msg_id: &str, bytes: &[u8]) -> Result<Message, CorruptValue> {
    Ok(message_of(msg_id, decode("message body", bytes)?))
}

pub(crate) fn encode_claim(claim: &ClaimInfo) -> Vec<u8> {
    assert!(claim.delivery_count >= 1);
    proto::ClaimRecord {
        worker_id: claim.worker_id.clone(),
        lease_id: claim.lease_id.clone(),
        claimed_at: claim.claimed_at,
        claim_token: claim.claim_token.clone(),
        delivery_count: claim.delivery_count,
        incarnation: claim.incarnation,
    }
    .encode_to_vec()
}

/// The claim on message `msg_id` whose record is `bytes`.
pub(crate) fn decode_claim(msg_id: &str, bytes: &[u8]) -> Result<ClaimInfo, CorruptValue> {
    let record: proto::ClaimRecord = decode("claim", bytes)?;
    if record.claim_token.is_empty() || record.delivery_count == 0 {
        return Err(CorruptValue(format!(
            "claim on {msg_id} without a token or a delivery"
        )));
    }
    Ok(ClaimInfo {
        msg_id: msg_id.to_string(),
        worker_id: record.worker_id,
        lease_id: record.lease_id,
        claimed_at: record.claimed_at,
        claim_token: record.claim_token,
        delivery_count: record.delivery_count,
        incarnation: record.incarnation,
    })
}

pub(crate) fn encode_dead_letter(letter: &DeadLetter) -> Vec<u8> {
    assert!(letter.delivery_count >= 1);
    let cause = match letter.cause {
        DeadLetterCause::NackLimit => proto::DeadLetterCause::NackLimit,
        DeadLetterCause::ReclaimLimit => proto::DeadLetterCause::ReclaimLimit,
        DeadLetterCause::Rejected => proto::DeadLetterCause::Rejected,
    };
    proto::DeadLetter {
        message: Some(stored_message(&letter.message)),
        delivery_count: letter.delivery_count,
        cause: cause.into(),
        reason: letter.reason.clone(),
        worker_id: letter.worker_id.clone(),
        dead_at_ms: letter.dead_at_ms,
    }
    .encode_to_vec()
}

/// The dead letter of message `msg_id` whose value is `bytes`.
pub(crate) fn decode_dead_letter(msg_id: &str, bytes: &[u8]) -> Result<DeadLetter, CorruptValue> {
    let record: proto::DeadLetter = decode("dead letter", bytes)?;
    let cause = match proto::DeadLetterCause::try_from(record.cause) {
        Ok(proto::DeadLetterCause::NackLimit) => DeadLetterCause::NackLimit,
        Ok(proto::DeadLetterCause::ReclaimLimit) => DeadLetterCause::ReclaimLimit,
        Ok(proto::DeadLetterCause::Rejected) => DeadLetterCause::Rejected,
        Ok(proto::DeadLetterCause::Unspecified) | Err(_) => {
            return Err(CorruptValue(format!(
                "dead letter {msg_id} with cause {}",
                record.cause
            )))
        }
    };
    let (Some(stored), 1..) = (record.message, record.delivery_count) else {
        return Err(CorruptValue(format!(
            "dead letter {msg_id} without a body or a delivery"
        )));
    };
    Ok(DeadLetter {
        message: message_of(msg_id, stored),
        delivery_count: record.delivery_count,
        cause,
        reason: record.reason,
        worker_id: record.worker_id,
        dead_at_ms: record.dead_at_ms,
    })
}

pub(crate) fn encode_group(group: &QueueGroupMeta) -> Vec<u8> {
    assert_eq!(group.partition_queues.len(), group.num_partitions as usize);
    proto::QueueGroup {
        num_partitions: group.num_partitions,
        version: group.version,
        partition_queues: group.partition_queues.clone(),
        created_at: group.created_at,
    }
    .encode_to_vec()
}

/// The group named `name` whose record is `bytes`.
pub(crate) fn decode_group(name: &str, bytes: &[u8]) -> Result<QueueGroupMeta, CorruptValue> {
    let record: proto::QueueGroup = decode("queue group", bytes)?;
    if record.partition_queues.len() != record.num_partitions as usize {
        return Err(CorruptValue(format!(
            "queue group {name} lists {} queues for {} partitions",
            record.partition_queues.len(),
            record.num_partitions
        )));
    }
    Ok(QueueGroupMeta {
        num_partitions: record.num_partitions,
        version: record.version,
        partition_queues: record.partition_queues,
        created_at: record.created_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::keys;
    use std::collections::HashMap;

    /// A message with every field set, for tests that pin or round-trip the
    /// encoding. One metadata entry: protobuf leaves the order of map entries
    /// to the encoder, so only a single entry encodes to fixed bytes.
    fn sample_message() -> Message {
        Message {
            msg_id: "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b".to_string(),
            payload: vec![0x00, 0x01, 0xff],
            created_at: 1_700_000_000.5,
            metadata: HashMap::from([("k".to_string(), "v".to_string())]),
        }
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn sample_dead_letter() -> DeadLetter {
        DeadLetter {
            message: sample_message(),
            delivery_count: 3,
            cause: DeadLetterCause::Rejected,
            reason: "r".to_string(),
            worker_id: "w".to_string(),
            dead_at_ms: 1_700_000_003_000,
        }
    }

    fn sample_claim() -> ClaimInfo {
        ClaimInfo {
            msg_id: sample_message().msg_id,
            worker_id: "w".to_string(),
            lease_id: "l".to_string(),
            claimed_at: 1_700_000_001.25,
            claim_token: "t".to_string(),
            delivery_count: 2,
            incarnation: None,
        }
    }

    /// Goal: no encoding of a key or a value changes unnoticed. Data on disk
    /// outlives the code that wrote it, so a changed encoding is a changed
    /// format.
    ///
    /// Method: encode a fixed key or value with every encoder and compare
    /// its bytes with the snapshot below.
    ///
    /// CHANGING A SNAPSHOT MEANS A BREAKING FORMAT CHANGE: bump
    /// `FORMAT_VERSION` and ship a migration from every earlier version, as
    /// docs/storage-format.md lays out. Only a new entry, for a new key
    /// family or a new optional field, is added without one.
    #[test]
    fn test_format_snapshot() {
        let message = sample_message();
        let claim = sample_claim();
        let group = QueueGroupMeta {
            num_partitions: 2,
            version: 0,
            partition_queues: vec!["g_p0".to_string(), "g_p1".to_string()],
            created_at: 1_700_000_002.0,
        };
        let encoded: Vec<(&str, Vec<u8>)> = vec![
            ("key format", keys::FORMAT.to_vec()),
            ("key queue", keys::queue("q:1")),
            ("key queue nul", keys::queue("a\0b")),
            ("key total_pushed", keys::total_pushed("q:1")),
            ("key total_acked", keys::total_acked("q:1")),
            ("key pending", keys::pending("q:1", 258)),
            ("key message", keys::message("q:1", "m")),
            ("key claim", keys::claim("q:1", "m")),
            ("key acked", keys::acked("q:1", 0x0102_0304_0506_0708, "m")),
            ("key finished", keys::finished("q:1")),
            ("key group", keys::group("g")),
            ("key state", keys::state("ns:x", "k\0")),
            ("key dead_letter", keys::dead_letter("q:1", "m")),
            ("key total_dead_lettered", keys::total_dead_lettered("q:1")),
            ("key total_redriven", keys::total_redriven("q:1")),
            ("key total_purged", keys::total_purged("q:1")),
            ("value format", encode_format(1)),
            ("value queue", encode_queue(5, None)),
            ("value queue max_deliveries", encode_queue(5, Some(0))),
            ("value total", encode_total(258).to_vec()),
            ("value pending", encode_pending("m", 3)),
            ("value message", encode_message(&message)),
            ("value claim", encode_claim(&claim)),
            (
                "value claim incarnation",
                encode_claim(&ClaimInfo {
                    incarnation: Some(0x0102_0304_0506_0708),
                    ..claim.clone()
                }),
            ),
            ("value group", encode_group(&group)),
            (
                "value dead_letter",
                encode_dead_letter(&sample_dead_letter()),
            ),
        ];
        let snapshot: &[(&str, &str)] = &[
            ("key format", "00"),
            ("key queue", "01713a310001"),
            ("key queue nul", "016100ff620001"),
            ("key total_pushed", "02713a310001"),
            ("key total_acked", "03713a310001"),
            ("key pending", "04713a3100010000000000000102"),
            ("key message", "05713a3100016d0001"),
            ("key claim", "06713a3100016d0001"),
            ("key acked", "07713a31000101020304050607086d0001"),
            ("key finished", "08713a310001"),
            ("key group", "09670001"),
            ("key state", "0a6e733a7800016b00ff0001"),
            ("key dead_letter", "0b713a3100016d0001"),
            ("key total_dead_lettered", "0c713a310001"),
            ("key total_redriven", "0d713a310001"),
            ("key total_purged", "0e713a310001"),
            ("value format", "0801"),
            ("value queue", "0805"),
            ("value queue max_deliveries", "08051000"),
            ("value total", "0201000000000000"),
            ("value pending", "0a016d1003"),
            (
                "value message",
                "0a030001ff1100002040fc54d9411a060a016b120176",
            ),
            ("value claim", "0a017712016c1900005040fc54d9412201742802"),
            (
                "value claim incarnation",
                "0a017712016c1900005040fc54d941220174280230888e98a8c0e0808101",
            ),
            (
                "value group",
                "08021a04675f70301a04675f70312100008040fc54d941",
            ),
            (
                "value dead_letter",
                "0a160a030001ff1100002040fc54d9411a060a016b120176100318032201722a017730b8e795ffbc31",
            ),
        ];
        assert_eq!(encoded.len(), snapshot.len());
        let mut changed = Vec::new();
        for ((name, bytes), (snapshot_name, snapshot_hex)) in encoded.iter().zip(snapshot) {
            assert_eq!(name, snapshot_name);
            if hex(bytes) != *snapshot_hex {
                changed.push(format!("{name}: {} (snapshot {snapshot_hex})", hex(bytes)));
            }
        }
        assert!(
            changed.is_empty(),
            "encodings changed, which is a breaking format change:\n{}",
            changed.join("\n")
        );
    }

    /// Goal: every value decodes to what was encoded, with the fields the
    /// key carries put back.
    #[test]
    fn test_values_round_trip() {
        let message = sample_message();
        let decoded = decode_message(&message.msg_id, &encode_message(&message)).unwrap();
        assert_eq!(
            (decoded.payload, decoded.created_at, decoded.metadata),
            (message.payload, message.created_at, message.metadata)
        );
        let claim = sample_claim();
        let decoded = decode_claim(&claim.msg_id, &encode_claim(&claim)).unwrap();
        assert_eq!(
            (decoded.worker_id, decoded.lease_id, decoded.claim_token),
            (claim.worker_id, claim.lease_id, claim.claim_token)
        );
        assert_eq!((decoded.delivery_count, decoded.incarnation), (2, None));
        let current = ClaimInfo {
            incarnation: Some(7),
            ..sample_claim()
        };
        let decoded = decode_claim(&current.msg_id, &encode_claim(&current)).unwrap();
        assert_eq!(decoded.incarnation, Some(7));
        let pending = decode_pending(&encode_pending("m", 3)).unwrap();
        assert_eq!((pending.msg_id.as_str(), pending.delivery_count), ("m", 3));
        let queue = decode_queue(&encode_queue(5, None)).unwrap();
        assert_eq!((queue.max_pending, queue.max_deliveries), (5, None));
        let queue = decode_queue(&encode_queue(0, Some(0))).unwrap();
        assert_eq!((queue.max_pending, queue.max_deliveries), (0, Some(0)));
        let letter = sample_dead_letter();
        let decoded =
            decode_dead_letter(&letter.message.msg_id, &encode_dead_letter(&letter)).unwrap();
        assert_eq!(decoded.message.msg_id, letter.message.msg_id);
        assert_eq!(decoded.message.payload, letter.message.payload);
        assert_eq!(decoded.message.metadata, letter.message.metadata);
        assert_eq!(
            (decoded.delivery_count, decoded.cause, decoded.dead_at_ms),
            (3, DeadLetterCause::Rejected, letter.dead_at_ms)
        );
        assert_eq!(
            (decoded.reason, decoded.worker_id),
            (letter.reason, letter.worker_id)
        );
        let without_cause = proto::DeadLetter {
            message: Some(proto::StoredMessage::default()),
            delivery_count: 1,
            ..proto::DeadLetter::default()
        };
        assert!(decode_dead_letter("m", &without_cause.encode_to_vec()).is_err());
        assert_eq!(decode_total(&encode_total(258)).unwrap(), 258);
        assert_eq!(
            decode_format(&encode_format(FORMAT_VERSION)).unwrap(),
            FORMAT_VERSION
        );
        assert!(decode_total(&[1, 2, 3]).is_err());
        assert!(decode_pending(&encode_pending("m", 0)[..1]).is_err());
        assert!(decode_format(&[]).is_err());
    }
}
