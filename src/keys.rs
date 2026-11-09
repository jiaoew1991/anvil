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

//! Every key anvil stores, and the only code that builds or parses one. The
//! layout is normative: docs/storage-format.md specifies it byte for byte,
//! and the format snapshot test pins it.
//!
//! A key is a tuple, in the style of the FoundationDB tuple layer: a family
//! tag byte, then the family's fields in a fixed order, each encoded so that
//! byte order is tuple order and no encoded field is a prefix of another.
//! The family fixes the field types, so fields carry no type codes.
//!
//! - A string is its bytes with each 0x00 escaped as 0x00 0xFF, followed by
//!   the terminator 0x00 0x01. Any string, including one holding ':' or NUL,
//!   encodes unambiguously; the encoding is prefix-free, so a tuple prefix
//!   that ends in a string (a queue's keys of one family) covers exactly the
//!   keys of that string; and strings sort as their bytes do.
//! - A u64 is 8 bytes big-endian, so numeric order is byte order.

use std::ops::Range;

/// The format marker. A single byte that no family tag equals, so it can
/// collide with no other key, and the smallest key there is, so the first
/// key of a store tells whether it carries a marker.
pub(crate) const FORMAT: &[u8] = &[0x00];

/// Family tags. They stay below 0x20, under every printable ASCII byte that
/// began the keys of the unversioned releases, so no such key reads as a
/// versioned one. A tag is never reused for another layout.
const QUEUE: u8 = 0x01;
const TOTAL_PUSHED: u8 = 0x02;
const TOTAL_ACKED: u8 = 0x03;
const PENDING: u8 = 0x04;
const MESSAGE: u8 = 0x05;
const CLAIM: u8 = 0x06;
const ACKED: u8 = 0x07;
const FINISHED: u8 = 0x08;
const GROUP: u8 = 0x09;
const STATE: u8 = 0x0a;
const DEAD_LETTER: u8 = 0x0b;

const ESCAPE: u8 = 0x00;
const ESCAPED_NUL: u8 = 0xff;
const TERMINATOR: u8 = 0x01;

/// A key that does not have the layout its family requires: a corrupt store
/// or a key outside the range a caller scanned.
#[derive(Debug)]
pub struct MalformedKey(Vec<u8>);

impl std::fmt::Display for MalformedKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "malformed key 0x")?;
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl std::error::Error for MalformedKey {}

struct Key(Vec<u8>);

impl Key {
    /// Starts a key of `family`, sized for `fields` bytes of encoded fields
    /// (a string takes its length plus 2, a u64 8), so a key whose strings
    /// hold no NUL is built in one allocation.
    fn new(family: u8, fields: usize) -> Self {
        let mut bytes = Vec::with_capacity(1 + fields);
        bytes.push(family);
        Self(bytes)
    }

    fn string(mut self, s: &str) -> Self {
        self.escaped(s.as_bytes());
        self.0.extend_from_slice(&[ESCAPE, TERMINATOR]);
        self
    }

    fn escaped(&mut self, bytes: &[u8]) {
        for chunk in bytes.split_inclusive(|&b| b == ESCAPE) {
            self.0.extend_from_slice(chunk);
            if chunk.last() == Some(&ESCAPE) {
                self.0.push(ESCAPED_NUL);
            }
        }
    }

    fn u64(mut self, value: u64) -> Self {
        self.0.extend_from_slice(&value.to_be_bytes());
        self
    }
}

/// Reads the fields of one key in order.
struct Fields<'a> {
    key: &'a [u8],
    rest: &'a [u8],
}

impl<'a> Fields<'a> {
    fn new(key: &'a [u8], family: u8) -> Result<Self, MalformedKey> {
        match key.split_first() {
            Some((&tag, rest)) if tag == family => Ok(Self { key, rest }),
            _ => Err(MalformedKey(key.to_vec())),
        }
    }

    fn malformed(&self) -> MalformedKey {
        MalformedKey(self.key.to_vec())
    }

    fn string(&mut self) -> Result<String, MalformedKey> {
        let mut bytes = Vec::new();
        loop {
            let Some(at) = self.rest.iter().position(|&b| b == ESCAPE) else {
                return Err(self.malformed());
            };
            bytes.extend_from_slice(&self.rest[..at]);
            match self.rest.get(at + 1) {
                Some(&TERMINATOR) => {
                    self.rest = &self.rest[at + 2..];
                    return String::from_utf8(bytes).map_err(|_| self.malformed());
                }
                Some(&ESCAPED_NUL) => {
                    bytes.push(0);
                    self.rest = &self.rest[at + 2..];
                }
                _ => return Err(self.malformed()),
            }
        }
    }

    fn u64(&mut self) -> Result<u64, MalformedKey> {
        let Some((value, rest)) = self.rest.split_first_chunk::<8>() else {
            return Err(self.malformed());
        };
        self.rest = rest;
        Ok(u64::from_be_bytes(*value))
    }

    fn end(self) -> Result<(), MalformedKey> {
        if self.rest.is_empty() {
            Ok(())
        } else {
            Err(self.malformed())
        }
    }
}

/// The range of every key that starts with `prefix`. A prefix never ends in
/// 0xFF (a family tag or a string terminator ends it), so incrementing its
/// last byte bounds it.
fn prefix_range(prefix: Vec<u8>) -> Range<Vec<u8>> {
    let mut end = prefix.clone();
    let last = end.last_mut().expect("a non-empty prefix");
    assert!(*last < 0xff, "a prefix ending in 0xff");
    *last += 1;
    prefix..end
}

/// The record of a queue, which exists exactly as long as the queue does.
pub(crate) fn queue(queue: &str) -> Vec<u8> {
    Key::new(QUEUE, queue.len() + 2).string(queue).0
}

/// Every queue record, in queue-name order.
pub(crate) fn queues() -> Range<Vec<u8>> {
    prefix_range(vec![QUEUE])
}

pub(crate) fn parse_queue(key: &[u8]) -> Result<String, MalformedKey> {
    let mut fields = Fields::new(key, QUEUE)?;
    let queue = fields.string()?;
    fields.end()?;
    Ok(queue)
}

/// The lifetime count of messages pushed to `queue`, merge-summed.
pub(crate) fn total_pushed(queue: &str) -> Vec<u8> {
    Key::new(TOTAL_PUSHED, queue.len() + 2).string(queue).0
}

/// The lifetime count of messages acked in `queue`, merge-summed.
pub(crate) fn total_acked(queue: &str) -> Vec<u8> {
    Key::new(TOTAL_ACKED, queue.len() + 2).string(queue).0
}

/// Whether `key` holds a merge-summed total, the only keys the merge
/// operator may combine.
pub(crate) fn is_total(key: &[u8]) -> bool {
    matches!(key.first(), Some(&(TOTAL_PUSHED | TOTAL_ACKED)))
}

/// The message waiting at `seq` in `queue`.
pub(crate) fn pending(queue: &str, seq: u64) -> Vec<u8> {
    Key::new(PENDING, queue.len() + 10).string(queue).u64(seq).0
}

/// Every pending key of `queue`, in seq order.
pub(crate) fn pending_of(queue: &str) -> Range<Vec<u8>> {
    prefix_range(Key::new(PENDING, queue.len() + 2).string(queue).0)
}

/// Queue and seq of a pending key.
pub(crate) fn parse_pending(key: &[u8]) -> Result<(String, u64), MalformedKey> {
    let mut fields = Fields::new(key, PENDING)?;
    let parsed = (fields.string()?, fields.u64()?);
    fields.end()?;
    Ok(parsed)
}

/// The body of message `msg_id` of `queue`.
pub(crate) fn message(queue: &str, msg_id: &str) -> Vec<u8> {
    Key::new(MESSAGE, queue.len() + msg_id.len() + 4)
        .string(queue)
        .string(msg_id)
        .0
}

/// The claim on message `msg_id` of `queue`.
pub(crate) fn claim(queue: &str, msg_id: &str) -> Vec<u8> {
    Key::new(CLAIM, queue.len() + msg_id.len() + 4)
        .string(queue)
        .string(msg_id)
        .0
}

/// Every claim of `queue`, or of every queue, by queue and msg_id.
pub(crate) fn claims(queue: Option<&str>) -> Range<Vec<u8>> {
    prefix_range(match queue {
        Some(queue) => Key::new(CLAIM, queue.len() + 2).string(queue).0,
        None => vec![CLAIM],
    })
}

/// Queue and msg_id of a claim key.
pub(crate) fn parse_claim(key: &[u8]) -> Result<(String, String), MalformedKey> {
    let mut fields = Fields::new(key, CLAIM)?;
    let parsed = (fields.string()?, fields.string()?);
    fields.end()?;
    Ok(parsed)
}

/// The record that message `msg_id` of `queue` was acked at `timestamp_ns`.
pub(crate) fn acked(queue: &str, timestamp_ns: u64, msg_id: &str) -> Vec<u8> {
    Key::new(ACKED, queue.len() + msg_id.len() + 12)
        .string(queue)
        .u64(timestamp_ns)
        .string(msg_id)
        .0
}

/// Every acked key of `queue`, or of every queue, by queue and timestamp.
pub(crate) fn acked_all(queue: Option<&str>) -> Range<Vec<u8>> {
    prefix_range(match queue {
        Some(queue) => Key::new(ACKED, queue.len() + 2).string(queue).0,
        None => vec![ACKED],
    })
}

/// The acked keys of `queue` stamped before `cutoff_ns`: one contiguous
/// range, since a queue's acked keys sort by timestamp.
pub(crate) fn acked_before(queue: &str, cutoff_ns: u64) -> Range<Vec<u8>> {
    let start = Key::new(ACKED, queue.len() + 2).string(queue);
    let end = Key::new(ACKED, queue.len() + 10)
        .string(queue)
        .u64(cutoff_ns);
    start.0..end.0
}

/// Queue, timestamp and msg_id of an acked key.
pub(crate) fn parse_acked(key: &[u8]) -> Result<(String, u64, String), MalformedKey> {
    let mut fields = Fields::new(key, ACKED)?;
    let parsed = (fields.string()?, fields.u64()?, fields.string()?);
    fields.end()?;
    Ok(parsed)
}

/// Present once `queue` is marked finished.
pub(crate) fn finished(queue: &str) -> Vec<u8> {
    Key::new(FINISHED, queue.len() + 2).string(queue).0
}

/// The record of queue group `group`.
pub(crate) fn group(group: &str) -> Vec<u8> {
    Key::new(GROUP, group.len() + 2).string(group).0
}

/// State `key` of `namespace`.
pub(crate) fn state(namespace: &str, key: &str) -> Vec<u8> {
    Key::new(STATE, namespace.len() + key.len() + 4)
        .string(namespace)
        .string(key)
        .0
}

/// The state keys of `namespace` that start with `prefix`. Escaping maps
/// each byte on its own, so the escaped key starts with the escaped prefix
/// exactly when the key starts with the prefix.
pub(crate) fn state_prefix(namespace: &str, prefix: &str) -> Vec<u8> {
    let mut key = Key::new(STATE, namespace.len() + prefix.len() + 2).string(namespace);
    key.escaped(prefix.as_bytes());
    key.0
}

/// Namespace and key of a state key.
pub(crate) fn parse_state(key: &[u8]) -> Result<(String, String), MalformedKey> {
    let mut fields = Fields::new(key, STATE)?;
    let parsed = (fields.string()?, fields.string()?);
    fields.end()?;
    Ok(parsed)
}

/// The dead letter of message `msg_id` of `queue`.
pub(crate) fn dead_letter(queue: &str, msg_id: &str) -> Vec<u8> {
    Key::new(DEAD_LETTER, queue.len() + msg_id.len() + 4)
        .string(queue)
        .string(msg_id)
        .0
}

/// The dead letters of `queue` by msg_id, those after `start_after` only
/// when it is given. Strings are prefix-free, so the first key past a dead
/// letter's is that key followed by 0x00.
pub(crate) fn dead_letters(queue: &str, start_after: Option<&str>) -> Range<Vec<u8>> {
    let all = prefix_range(Key::new(DEAD_LETTER, queue.len() + 2).string(queue).0);
    match start_after {
        Some(msg_id) => {
            let mut start = dead_letter(queue, msg_id);
            start.push(0x00);
            start..all.end
        }
        None => all,
    }
}

/// Queue and msg_id of a dead letter key.
pub(crate) fn parse_dead_letter(key: &[u8]) -> Result<(String, String), MalformedKey> {
    let mut fields = Fields::new(key, DEAD_LETTER)?;
    let parsed = (fields.string()?, fields.string()?);
    fields.end()?;
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Goal: strings keep their order and stay apart whatever bytes they
    /// hold. Method: encode names that differ only around ':' and NUL, and
    /// check that byte order is string order and each parses back whole.
    #[test]
    fn test_strings_are_ordered_and_unambiguous() {
        let mut names = vec![
            "", "a", "a\0", "a\0b", "a\u{1}", "a:", "a:b", "ab", "b", "\0", "\u{ff}",
        ];
        names.sort();
        let keys: Vec<Vec<u8>> = names.iter().map(|name| queue(name)).collect();
        for pair in keys.windows(2) {
            assert!(pair[0] < pair[1], "{pair:?}");
        }
        for (name, key) in names.iter().zip(&keys) {
            assert_eq!(parse_queue(key).unwrap(), *name);
        }
    }

    /// Goal: a queue's range holds its own keys and no other queue's, even a
    /// queue whose name extends it with ':' or NUL, whatever the seq.
    #[test]
    fn test_queue_ranges_are_exact() {
        let range = pending_of("q");
        for seq in [0, 1, 0xff00_0000_0000_0000, u64::MAX] {
            assert!(range.contains(&pending("q", seq)));
            for other in ["q\0", "q\0\u{1}", "q:", "q:0", "qa", "p", ""] {
                assert!(!range.contains(&pending(other, seq)), "{other:?} {seq}");
            }
        }
        let range = claims(Some("q"));
        assert!(range.contains(&claim("q", "\0")));
        assert!(!range.contains(&claim("q\0", "")));
        assert!(!range.contains(&claim("q:x", "m")));
    }

    /// Goal: the acked keys before a cutoff are exactly those stamped before
    /// it, in timestamp order, whatever the msg_id.
    #[test]
    fn test_acked_before_is_exact() {
        let range = acked_before("q", 100);
        assert!(range.contains(&acked("q", 0, "")));
        assert!(range.contains(&acked("q", 99, "\u{10ffff}")));
        assert!(!range.contains(&acked("q", 100, "")));
        assert!(!range.contains(&acked("q\0", 1, "m")));
        assert!(!range.contains(&acked("", 1, "m")));
        assert!(acked("q", 1, "z") < acked("q", 2, "a"));
        assert_eq!(
            parse_acked(&acked("q:1", 7, "m:2")).unwrap(),
            ("q:1".to_string(), 7, "m:2".to_string())
        );
    }

    /// Goal: a state prefix scan matches keys by their prefix, ':' and NUL
    /// included, and never crosses into another namespace.
    #[test]
    fn test_state_prefix_matches_key_prefix() {
        let prefix = state_prefix("job:a", "k\0");
        assert!(state("job:a", "k\0").starts_with(&prefix));
        assert!(state("job:a", "k\0x").starts_with(&prefix));
        assert!(!state("job:a", "k").starts_with(&prefix));
        assert!(!state("job:a", "kx").starts_with(&prefix));
        assert!(!state("job", "a:k\0").starts_with(&prefix));
        assert_ne!(state("job:a", "k"), state("job", "a:k"));
        assert_eq!(
            parse_state(&state("job:a", "k\0")).unwrap(),
            ("job:a".to_string(), "k\0".to_string())
        );
    }

    /// Goal: a queue's dead letters after a cursor are exactly those whose
    /// msg_id sorts after it, whatever bytes the msg_ids hold, and never
    /// another queue's.
    #[test]
    fn test_dead_letters_after_cursor_are_exact() {
        let range = dead_letters("q", Some("m"));
        assert!(!range.contains(&dead_letter("q", "m")));
        assert!(range.contains(&dead_letter("q", "m\0")));
        assert!(range.contains(&dead_letter("q", "ma")));
        assert!(range.contains(&dead_letter("q", "n")));
        assert!(!range.contains(&dead_letter("q", "a")));
        assert!(!range.contains(&dead_letter("q\0", "z")));
        assert!(!range.contains(&dead_letter("q:", "z")));
        let range = dead_letters("q", None);
        assert!(range.contains(&dead_letter("q", "")));
        assert!(!range.contains(&dead_letter("qa", "")));
        assert_eq!(
            parse_dead_letter(&dead_letter("q:1", "m\0")).unwrap(),
            ("q:1".to_string(), "m\0".to_string())
        );
    }

    /// Goal: a key of one family never parses as another's, and trailing or
    /// missing bytes are refused.
    #[test]
    fn test_malformed_keys_are_refused() {
        assert!(parse_claim(&acked("q", 1, "m")).is_err());
        assert!(parse_dead_letter(&claim("q", "m")).is_err());
        assert!(parse_pending(&pending("q", 1)[..9]).is_err());
        let mut long = claim("q", "m");
        long.push(0);
        assert!(parse_claim(&long).is_err());
        assert!(parse_queue(&[QUEUE, b'q', 0x00, 0x02]).is_err());
        assert!(parse_queue(&[QUEUE, 0xc3, 0x28, 0x00, 0x01]).is_err());
        assert!(is_total(&total_pushed("q")) && is_total(&total_acked("q")));
        assert!(!is_total(&queue("q")) && !is_total(FORMAT));
    }
}
