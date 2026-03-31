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

// Core data structures for Anvil

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

// === Simulated time (test-only) ===
//
// In test builds, `now_secs()` and `now_nanos()` check a global override so that
// DST and time-sensitive tests can control the clock deterministically.
// In release/production builds, they are direct wall-clock reads with zero overhead.

#[cfg(test)]
static SIM_TIME_NANOS: AtomicU64 = AtomicU64::new(0);

/// Mutex to serialize tests that use simulated time.
/// Acquire this lock before calling `set_sim_time_nanos` to prevent
/// parallel tests from stomping on each other's sim time.
#[cfg(test)]
pub static SIM_TIME_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Set simulated time (nanoseconds since epoch). Pass 0 to restore real time.
/// IMPORTANT: Acquire `SIM_TIME_LOCK` before calling this in tests.
#[cfg(test)]
pub fn set_sim_time_nanos(nanos: u64) {
    SIM_TIME_NANOS.store(nanos, Ordering::Release);
}

/// Advance simulated time by the given number of seconds.
#[cfg(test)]
pub fn advance_sim_time_secs(secs: f64) {
    let delta = (secs * 1_000_000_000.0) as u64;
    SIM_TIME_NANOS.fetch_add(delta, Ordering::Release);
}

/// Get current time as Unix timestamp (seconds with fractional part)
#[cfg(not(test))]
pub fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

/// Get current time as Unix timestamp (seconds with fractional part) — test version with sim clock
#[cfg(test)]
pub fn now_secs() -> f64 {
    let sim = SIM_TIME_NANOS.load(Ordering::Acquire);
    if sim > 0 {
        return sim as f64 / 1_000_000_000.0;
    }
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

/// Get current time as nanoseconds since epoch
#[cfg(not(test))]
pub fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

/// Get current time as nanoseconds since epoch — test version with sim clock
#[cfg(test)]
pub fn now_nanos() -> u64 {
    let sim = SIM_TIME_NANOS.load(Ordering::Acquire);
    if sim > 0 {
        return sim;
    }
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

/// Message stored in the queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub msg_id: String,
    pub queue: String,
    pub payload: Vec<u8>,
    pub created_at: f64,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl Message {
    /// Create a new message with auto-generated ID
    pub fn new(queue: String, payload: Vec<u8>) -> Self {
        Self {
            msg_id: uuid::Uuid::now_v7().to_string(),
            queue,
            payload,
            created_at: now_secs(),
            metadata: HashMap::new(),
        }
    }

    /// Create a message with metadata
    pub fn with_metadata(
        queue: String,
        payload: Vec<u8>,
        metadata: HashMap<String, String>,
    ) -> Self {
        Self {
            msg_id: uuid::Uuid::now_v7().to_string(),
            queue,
            payload,
            created_at: now_secs(),
            metadata,
        }
    }
}

/// Information about a claimed message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimInfo {
    pub msg_id: String,
    pub worker_id: String,
    pub lease_id: String,
    pub claimed_at: f64,
    pub claim_token: String,
}

impl ClaimInfo {
    pub fn new(msg_id: String, worker_id: String, lease_id: String) -> Self {
        Self {
            msg_id,
            worker_id,
            lease_id,
            claimed_at: now_secs(),
            claim_token: uuid::Uuid::now_v7().to_string(),
        }
    }
}

/// Message returned by claim with its claim token.
#[derive(Debug, Clone)]
pub struct ClaimedMessage {
    pub message: Message,
    pub claim_token: String,
}

/// Queue group metadata (stored at group_meta:{group_name})
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueGroupMeta {
    pub name: String,
    pub num_partitions: u32,
    pub version: u32,
    pub partition_queues: Vec<String>,
    pub created_at: f64,
}

impl QueueGroupMeta {
    pub fn new(name: String, num_partitions: u32) -> Self {
        let partition_queues = (0..num_partitions)
            .map(|i| format!("{}_p{}", name, i))
            .collect();
        Self {
            name,
            num_partitions,
            version: 0,
            partition_queues,
            created_at: now_secs(),
        }
    }
}

/// Anvil server configuration
#[derive(Debug, Clone)]
pub struct AnvilConfig {
    /// Host to bind to
    pub host: String,
    /// Port to bind to (0 for auto-assign)
    pub port: u16,
    /// SlateDB storage path (memory://, file://, or s3://)
    pub db_path: String,
    /// Claim timeout in seconds (messages reclaimed if worker doesn't heartbeat)
    pub claim_timeout_secs: f64,
    /// Recovery task interval in seconds
    pub recovery_interval_secs: f64,
    /// Maximum queue depth (0 = unlimited) - reserved for future use
    #[allow(dead_code)]
    pub max_queue_depth: usize,
    /// Acked message retention in seconds (messages deleted after this time)
    pub acked_retention_secs: f64,
    /// GC interval in seconds (how often to clean up acked messages)
    pub gc_interval_secs: f64,
}

impl Default for AnvilConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 0,
            db_path: "memory://anvil".to_string(),
            claim_timeout_secs: 60.0,
            recovery_interval_secs: 10.0,
            max_queue_depth: 0,
            acked_retention_secs: 3600.0,
            gc_interval_secs: 60.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_creation() {
        let msg = Message::new("test-queue".to_string(), b"hello".to_vec());
        assert_eq!(msg.queue, "test-queue");
        assert_eq!(msg.payload, b"hello");
        assert!(!msg.msg_id.is_empty());
        assert!(msg.created_at > 0.0);
        assert!(msg.metadata.is_empty());
    }

    #[test]
    fn test_message_with_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert("key1".to_string(), "value1".to_string());
        metadata.insert("key2".to_string(), "value2".to_string());

        let msg = Message::with_metadata("test-queue".to_string(), b"hello".to_vec(), metadata);
        assert_eq!(msg.metadata.len(), 2);
        assert_eq!(msg.metadata.get("key1"), Some(&"value1".to_string()));
    }

    #[test]
    fn test_claim_info() {
        let claim = ClaimInfo::new(
            "msg-123".to_string(),
            "worker-1".to_string(),
            "lease-456".to_string(),
        );

        assert_eq!(claim.msg_id, "msg-123");
        assert_eq!(claim.worker_id, "worker-1");
        assert_eq!(claim.lease_id, "lease-456");
        assert!(claim.claimed_at > 0.0);
        assert!(!claim.claim_token.is_empty());
    }

    #[test]
    fn test_config_default() {
        let config = AnvilConfig::default();

        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 0);
        assert_eq!(config.db_path, "memory://anvil");
        assert_eq!(config.claim_timeout_secs, 60.0);
        assert_eq!(config.recovery_interval_secs, 10.0);
        assert_eq!(config.max_queue_depth, 0);
    }

    #[test]
    fn test_message_serialization() {
        let msg = Message::new("test-queue".to_string(), b"hello".to_vec());

        // Serialize
        let json = serde_json::to_string(&msg).unwrap();
        assert!(json.contains("test-queue"));

        // Deserialize
        let msg2: Message = serde_json::from_str(&json).unwrap();
        assert_eq!(msg.msg_id, msg2.msg_id);
        assert_eq!(msg.payload, msg2.payload);
    }

    #[test]
    fn test_claim_info_serialization() {
        let claim = ClaimInfo::new(
            "msg-123".to_string(),
            "worker-1".to_string(),
            "lease-456".to_string(),
        );

        // Serialize
        let json = serde_json::to_string(&claim).unwrap();
        assert!(json.contains("msg-123"));

        // Deserialize
        let claim2: ClaimInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(claim.msg_id, claim2.msg_id);
        assert_eq!(claim.worker_id, claim2.worker_id);
        assert_eq!(claim.claim_token, claim2.claim_token);
    }

    #[test]
    fn test_now_functions() {
        let secs = now_secs();
        let nanos = now_nanos();

        // Basic sanity checks
        assert!(secs > 0.0);
        assert!(nanos > 0);

        // nanos should be roughly secs * 1e9
        let expected_nanos = (secs * 1_000_000_000.0) as u64;
        assert!((nanos as i64 - expected_nanos as i64).abs() < 1_000_000_000);
    }
}
