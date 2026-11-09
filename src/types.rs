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

use std::collections::HashMap;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// === Simulated time (test-only) ===
//
// In test builds, `now_secs()`, `now_nanos()` and `mono_now()` check a global
// override so that DST and time-sensitive tests can control both clocks
// deterministically. In release/production builds, they are direct clock reads
// with zero overhead.

#[cfg(test)]
static SIM_TIME_NANOS: AtomicU64 = AtomicU64::new(0);

/// Simulated monotonic time, in nanoseconds past `SIM_MONO_ORIGIN`; 0 while
/// time is real.
#[cfg(test)]
static SIM_MONO_NANOS: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
static SIM_MONO_ORIGIN: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

#[cfg(test)]
static SIM_TIME_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Exclusive use of the simulated clocks, which are global to the test
/// binary: a test holds it for its whole run if it moves the clocks, or if
/// it needs them to keep real time while it runs. Releasing it restores
/// real time, so a test that fails mid-simulation cannot hand its clock to
/// the next one.
#[cfg(test)]
pub struct SimClock {
    _lock: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl SimClock {
    /// The lock guards no data, only the right to move the clocks, and every
    /// holder leaves them real on release. A poisoned lock therefore means
    /// only that an earlier holder failed, which must not fail this one too.
    pub fn acquire() -> Self {
        let lock = SIM_TIME_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(
            SIM_TIME_NANOS.load(Ordering::Acquire),
            0,
            "the wall clock was simulated without holding SimClock"
        );
        assert_eq!(
            SIM_MONO_NANOS.load(Ordering::Acquire),
            0,
            "the monotonic clock was simulated without holding SimClock"
        );
        Self { _lock: lock }
    }
}

#[cfg(test)]
impl Drop for SimClock {
    fn drop(&mut self) {
        set_sim_time_nanos(0);
    }
}

/// Set the simulated wall clock (nanoseconds since epoch). Pass 0 to restore
/// real time on both clocks. Any other value steps the wall clock alone, as
/// an NTP correction does: the monotonic clock freezes when simulation starts
/// and then moves only with `advance_sim_time_secs`.
/// IMPORTANT: Hold a `SimClock` while calling this in tests.
#[cfg(test)]
pub fn set_sim_time_nanos(nanos: u64) {
    let was = SIM_TIME_NANOS.swap(nanos, Ordering::AcqRel);
    if nanos == 0 {
        SIM_MONO_NANOS.store(0, Ordering::Release);
    } else if was == 0 {
        SIM_MONO_NANOS.store(1, Ordering::Release);
    }
}

/// Let the given number of seconds pass on both simulated clocks.
#[cfg(test)]
pub fn advance_sim_time_secs(secs: f64) {
    let delta = (secs * 1_000_000_000.0) as u64;
    SIM_TIME_NANOS.fetch_add(delta, Ordering::Release);
    SIM_MONO_NANOS.fetch_add(delta, Ordering::Release);
}

/// The monotonic clock that times leases and claim ages: unlike the wall
/// clock, it never steps, so an NTP correction can neither revive a dead
/// lease nor kill a live one.
#[cfg(not(test))]
pub fn mono_now() -> Instant {
    Instant::now()
}

/// The monotonic clock — test version with sim clock
#[cfg(test)]
pub fn mono_now() -> Instant {
    let sim = SIM_MONO_NANOS.load(Ordering::Acquire);
    if sim > 0 {
        return *SIM_MONO_ORIGIN.get_or_init(Instant::now) + Duration::from_nanos(sim);
    }
    Instant::now()
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
#[derive(Debug, Clone)]
pub struct Message {
    pub msg_id: String,
    pub payload: Vec<u8>,
    pub created_at: f64,
    pub metadata: HashMap<String, String>,
}

impl Message {
    /// Create a new message with auto-generated ID
    pub fn new(payload: Vec<u8>) -> Self {
        Self {
            msg_id: uuid::Uuid::now_v7().to_string(),
            payload,
            created_at: now_secs(),
            metadata: HashMap::new(),
        }
    }

    /// Create a message with metadata
    pub fn with_metadata(payload: Vec<u8>, metadata: HashMap<String, String>) -> Self {
        Self {
            msg_id: uuid::Uuid::now_v7().to_string(),
            payload,
            created_at: now_secs(),
            metadata,
        }
    }
}

/// Information about a claimed message
#[derive(Debug, Clone)]
pub struct ClaimInfo {
    pub msg_id: String,
    pub worker_id: String,
    pub lease_id: String,
    pub claimed_at: f64,
    pub claim_token: String,
    /// How many times the message has been handed out, this claim included.
    pub delivery_count: u32,
}

impl ClaimInfo {
    pub fn new(msg_id: String, worker_id: String, lease_id: String, delivery_count: u32) -> Self {
        assert!(delivery_count >= 1);
        Self {
            msg_id,
            worker_id,
            lease_id,
            claimed_at: now_secs(),
            claim_token: uuid::Uuid::now_v7().to_string(),
            delivery_count,
        }
    }
}

/// Message returned by claim with its claim token.
#[derive(Debug, Clone)]
pub struct ClaimedMessage {
    pub message: Message,
    pub claim_token: String,
    /// 1 on first delivery; each nack or reclaimed lease adds one.
    pub delivery_count: u32,
}

/// Why a message was dead-lettered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadLetterCause {
    /// A nack settled the last delivery its queue allows.
    NackLimit,
    /// A reclaim settled the last delivery its queue allows.
    ReclaimLimit,
    /// A nack asked for it, whatever the delivery count.
    Rejected,
}

impl DeadLetterCause {
    /// The name the wire API and the metrics report.
    pub fn as_str(self) -> &'static str {
        match self {
            DeadLetterCause::NackLimit => "nack_limit",
            DeadLetterCause::ReclaimLimit => "reclaim_limit",
            DeadLetterCause::Rejected => "rejected",
        }
    }
}

/// A message taken out of circulation, kept until a redrive or a purge.
#[derive(Debug, Clone)]
pub struct DeadLetter {
    pub message: Message,
    /// Deliveries the message had, the last one included.
    pub delivery_count: u32,
    pub cause: DeadLetterCause,
    /// The reason of the nack that settled the last delivery; empty for a
    /// reclaim.
    pub reason: String,
    /// The worker that held the last claim.
    pub worker_id: String,
    /// Wall-clock time, milliseconds since the Unix epoch.
    pub dead_at_ms: u64,
}

/// Queue group metadata
#[derive(Debug, Clone)]
pub struct QueueGroupMeta {
    pub num_partitions: u32,
    pub version: u32,
    pub partition_queues: Vec<String>,
    pub created_at: f64,
}

impl QueueGroupMeta {
    pub fn new(name: &str, num_partitions: u32) -> Self {
        let partition_queues = (0..num_partitions)
            .map(|i| format!("{name}_p{i}"))
            .collect();
        Self {
            num_partitions,
            version: 0,
            partition_queues,
            created_at: now_secs(),
        }
    }
}

/// The default of `AnvilConfig::default_max_deliveries`: generous enough
/// that a transient failure is retried well past its cause, small enough
/// that a poison message stops consuming workers within minutes.
pub const DEFAULT_MAX_DELIVERIES: u32 = 20;

/// Anvil server configuration
#[derive(Debug, Clone)]
pub struct AnvilConfig {
    /// Host to bind to
    pub host: String,
    /// Port to bind to (0 for auto-assign)
    pub port: u16,
    /// SlateDB storage path (memory://, file://, or s3://)
    pub db_path: String,
    /// Seconds a lease may go without a heartbeat before its worker is
    /// considered dead and every claim it holds is reclaimed. Detects crashed
    /// workers, so keep it short (a few heartbeat intervals).
    pub lease_timeout_secs: f64,
    /// Seconds a single claim may be held, even under a live lease, before it
    /// is treated as stuck and reclaimed. Bounds task duration, so it must
    /// exceed the slowest legitimate task or that task is redelivered forever.
    pub claim_age_timeout_secs: f64,
    /// Recovery task interval in seconds
    pub recovery_interval_secs: f64,
    /// Maximum queue depth (0 = unlimited) - reserved for future use
    #[allow(dead_code)]
    pub max_queue_depth: usize,
    /// Acked message retention in seconds (messages deleted after this time)
    pub acked_retention_secs: f64,
    /// GC interval in seconds (how often to clean up acked messages)
    pub gc_interval_secs: f64,
    /// Deliveries a message gets before a failed one is dead-lettered, for
    /// every queue that sets no limit of its own; 0 is unlimited. Queues
    /// created before dead letters existed set none, so this governs them.
    pub default_max_deliveries: u32,
}

impl Default for AnvilConfig {
    fn default() -> Self {
        Self {
            host: "0.0.0.0".to_string(),
            port: 0,
            db_path: "memory://anvil".to_string(),
            lease_timeout_secs: 60.0,
            claim_age_timeout_secs: 60.0,
            recovery_interval_secs: 10.0,
            max_queue_depth: 0,
            acked_retention_secs: 3600.0,
            gc_interval_secs: 60.0,
            default_max_deliveries: DEFAULT_MAX_DELIVERIES,
        }
    }
}

impl AnvilConfig {
    /// Refuses a config the broker cannot run as asked. Every entry point,
    /// Rust or Python, checks it before starting: a bad interval would
    /// otherwise panic only the spawned recovery or GC task, leaving a broker
    /// that serves with no recovery or no GC.
    pub fn validate(&self) -> Result<(), String> {
        // A non-positive timeout reclaims every claim on the first recovery
        // tick, so the queue would redeliver work as fast as it hands it out,
        // and a zero interval panics `tokio::time::interval`. NaN, negative,
        // infinite and overflowing values are no `Duration` at all.
        let positive = [
            ("lease_timeout_secs", self.lease_timeout_secs),
            ("claim_age_timeout_secs", self.claim_age_timeout_secs),
            ("recovery_interval_secs", self.recovery_interval_secs),
            ("gc_interval_secs", self.gc_interval_secs),
        ];
        for (name, secs) in positive {
            match Duration::try_from_secs_f64(secs) {
                Ok(duration) if !duration.is_zero() => {}
                _ => return Err(format!("{name} must be a positive duration, got {secs}")),
            }
        }
        // Zero retention is valid: acked messages go at the next GC pass.
        if Duration::try_from_secs_f64(self.acked_retention_secs).is_err() {
            return Err(format!(
                "acked_retention_secs must be a non-negative duration, got {}",
                self.acked_retention_secs
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_creation() {
        let msg = Message::new(b"hello".to_vec());
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

        let msg = Message::with_metadata(b"hello".to_vec(), metadata);
        assert_eq!(msg.metadata.len(), 2);
        assert_eq!(msg.metadata.get("key1"), Some(&"value1".to_string()));
    }

    #[test]
    fn test_claim_info() {
        let claim = ClaimInfo::new(
            "msg-123".to_string(),
            "worker-1".to_string(),
            "lease-456".to_string(),
            1,
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
        assert_eq!(config.lease_timeout_secs, 60.0);
        assert_eq!(config.claim_age_timeout_secs, 60.0);
        assert_eq!(config.recovery_interval_secs, 10.0);
        assert_eq!(config.max_queue_depth, 0);
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
