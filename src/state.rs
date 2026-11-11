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

// In-memory state for Anvil - minimal coordination layer
//
// With atomic counters + CAS in storage, state only provides:
// - Queue registry: track known queues for stats
// - Lease management: track worker heartbeats

use dashmap::DashMap;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::Instant;

use crate::storage::Rejection;
use crate::types::mono_now;

/// Whether a lease last seen at `last_seen` has gone more than
/// `lease_timeout_secs` without a heartbeat. The one liveness rule shared by
/// recovery, which reclaims a dead lease's claims, and pruning, which forgets
/// the lease.
pub fn lease_expired(last_seen: Instant, now: Instant, lease_timeout_secs: f64) -> bool {
    now.saturating_duration_since(last_seen).as_secs_f64() > lease_timeout_secs
}

/// Anvil coordination state - minimal, no message storage
pub struct AnvilState {
    /// Known queue names (for stats listing)
    queues: DashMap<String, ()>,
    /// When each lease was last seen, on the monotonic clock. Only the broker
    /// mints lease ids, and a lease absent from this table is dead: recovery
    /// reclaims its claims, and a worker presenting it gets a new lease.
    leases: DashMap<String, Instant>,
    /// The lease consuming each partition queue of a queue group, by queue
    /// name. A consumer that keeps state per key needs its partition to
    /// itself: two would each see part of a key. One lock over the table,
    /// so a claim takes all the partitions it names or none.
    partition_owners: Mutex<HashMap<String, String>>,
}

impl AnvilState {
    pub fn new() -> Self {
        Self {
            queues: DashMap::new(),
            leases: DashMap::new(),
            partition_owners: Mutex::new(HashMap::new()),
        }
    }

    /// Register a queue name
    pub fn get_or_create_queue(&self, queue: &str) {
        self.queues.entry(queue.to_string()).or_insert(());
    }

    /// Check if queue exists in registry
    #[allow(dead_code)]
    pub fn queue_exists(&self, queue: &str) -> bool {
        self.queues.contains_key(queue)
    }

    /// Delete queue from registry
    pub fn delete_queue(&self, queue: &str) -> bool {
        self.queues.remove(queue).is_some()
    }

    /// Get list of known queues
    pub fn list_queues(&self) -> Vec<String> {
        self.queues.iter().map(|e| e.key().clone()).collect()
    }

    /// The lease for a heartbeat stream whose first ping presents
    /// `presented`: the same lease while the table still holds it, so a
    /// worker that reconnects keeps its claims, else a new one. A lease the
    /// table no longer holds stays dead: after recovery prunes it, or after a
    /// broker restart empties the table, its claims are reclaimed whatever
    /// the worker presents.
    pub fn open_lease(&self, presented: &str) -> String {
        if self.renew_lease(presented) {
            return presented.to_string();
        }
        let lease_id = uuid::Uuid::now_v7().to_string();
        let replaced = self.leases.insert(lease_id.clone(), mono_now());
        assert!(replaced.is_none(), "minted lease {lease_id} twice");
        lease_id
    }

    /// Records a heartbeat for `lease_id`. Returns false, and records
    /// nothing, once the lease has been pruned: a stray ping must not revive
    /// it. Takes no allocation, as it runs on every ping.
    pub fn renew_lease(&self, lease_id: &str) -> bool {
        let Some(mut last_seen) = self.leases.get_mut(lease_id) else {
            return false;
        };
        *last_seen = mono_now();
        true
    }

    /// Registers `lease_id` as seen now, for tests that drive leases with
    /// their own ids and without a heartbeat stream.
    #[cfg(test)]
    pub fn update_lease(&self, lease_id: &str) {
        self.leases.insert(lease_id.to_string(), mono_now());
    }

    /// Forgets every expired lease. Safe at any time: recovery treats a
    /// lease missing from the table as dead, so its claims are still
    /// reclaimed, and `renew_lease` never brings it back.
    pub fn prune_leases(&self, lease_timeout_secs: f64) {
        let now = mono_now();
        self.leases
            .retain(|_, last_seen| !lease_expired(*last_seen, now, lease_timeout_secs));
    }

    /// Makes `lease_id` the owner of every queue in `partition_queues` it
    /// does not own yet, or of none: a queue owned by another lease refuses
    /// the call, and so does a lease the table no longer holds, whose worker
    /// lost its claims and holds state that may be behind its successor's.
    pub fn own_partitions<'a>(
        &self,
        partition_queues: impl Iterator<Item = &'a str> + Clone,
        lease_id: &str,
    ) -> Result<(), Rejection> {
        let mut owners = self.partition_owners.lock().unwrap();
        if !self.leases.contains_key(lease_id) {
            return Err(Rejection::Fenced(format!(
                "lease_not_live: lease={lease_id}"
            )));
        }
        for queue in partition_queues.clone() {
            if let Some(owner) = owners.get(queue) {
                if owner != lease_id {
                    return Err(Rejection::PartitionOwned(format!(
                        "partition_owned: queue={queue}, owner_lease={owner}"
                    )));
                }
            }
        }
        for queue in partition_queues {
            if !owners.contains_key(queue) {
                owners.insert(queue.to_string(), lease_id.to_string());
            }
        }
        Ok(())
    }

    /// Frees the partitions of the leases a recovery pass found dead or
    /// stuck, once it has reclaimed their claims: `recovered` is the leases
    /// the pass took for live, `reclaimed` the queue and lease of each claim
    /// it took back.
    ///
    /// - A dead lease is absent from `recovered` and was not opened since.
    ///   The pass reclaimed every claim it held, so its worker can settle no
    ///   message behind the next owner's back. Until then its partitions
    ///   stay owned, as an ack of the dead lease may still land.
    /// - A stuck lease is live but let a claim on a partition it owns age
    ///   out. The message can only be redelivered to the partition's owner,
    ///   so a worker that stays stuck would hold its partitions for good:
    ///   it loses them all, to whoever claims them next.
    pub fn release_owners(
        &self,
        recovered: &HashMap<String, Instant>,
        reclaimed: &[(String, String)],
    ) {
        let mut owners = self.partition_owners.lock().unwrap();
        let stuck: HashSet<&str> = reclaimed
            .iter()
            .filter(|(queue, lease_id)| owners.get(queue) == Some(lease_id))
            .map(|(_, lease_id)| lease_id.as_str())
            .collect();
        owners.retain(|_, lease_id| {
            let live = recovered.contains_key(lease_id) || self.leases.contains_key(lease_id);
            live && !stuck.contains(lease_id.as_str())
        });
    }

    /// Get a snapshot of all leases (lease_id -> last_seen).
    pub fn lease_snapshot(&self) -> HashMap<String, Instant> {
        self.leases
            .iter()
            .map(|entry| (entry.key().clone(), *entry.value()))
            .collect()
    }
}

impl Default for AnvilState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_state_creation() {
        let state = AnvilState::new();

        state.get_or_create_queue("test-queue");
        assert!(state.queue_exists("test-queue"));

        // Getting again should not panic
        state.get_or_create_queue("test-queue");
        assert!(state.queue_exists("test-queue"));
    }

    #[test]
    fn test_delete_queue() {
        let state = AnvilState::new();

        state.get_or_create_queue("test-queue");
        assert!(state.queue_exists("test-queue"));

        let deleted = state.delete_queue("test-queue");
        assert!(deleted);
        assert!(!state.queue_exists("test-queue"));
    }

    /// A reconnect presenting a held lease keeps it; an unknown or pruned
    /// lease gets a new one; pruning forgets only expired leases, and a
    /// pruned lease cannot be renewed back to life.
    #[test]
    fn test_lease_lifecycle() {
        use crate::types::{advance_sim_time_secs, set_sim_time_nanos, SimClock};
        const LEASE_TIMEOUT_SECS: f64 = 5.0;

        let _clock = SimClock::acquire();
        set_sim_time_nanos(1_735_689_600_000_000_000);
        let state = AnvilState::new();

        let held = state.open_lease("");
        assert_eq!(state.open_lease(&held), held);
        let unknown = state.open_lease("from-before-a-restart");
        assert_ne!(unknown, "from-before-a-restart");

        advance_sim_time_secs(LEASE_TIMEOUT_SECS);
        assert!(state.renew_lease(&held));
        state.prune_leases(LEASE_TIMEOUT_SECS);
        assert_eq!(
            state.lease_snapshot().len(),
            2,
            "pruned a lease at its timeout"
        );

        advance_sim_time_secs(0.001);
        state.prune_leases(LEASE_TIMEOUT_SECS);
        assert_eq!(state.lease_snapshot().keys().collect::<Vec<_>>(), [&held]);
        assert!(!state.renew_lease(&unknown), "renewed a pruned lease");
        assert_ne!(
            state.open_lease(&unknown),
            unknown,
            "reopened a pruned lease"
        );
    }

    #[test]
    fn test_list_queues() {
        let state = AnvilState::new();

        state.get_or_create_queue("queue-a");
        state.get_or_create_queue("queue-b");
        state.get_or_create_queue("queue-c");

        let queues = state.list_queues();
        assert_eq!(queues.len(), 3);
        assert!(queues.contains(&"queue-a".to_string()));
        assert!(queues.contains(&"queue-b".to_string()));
        assert!(queues.contains(&"queue-c".to_string()));
    }
}
