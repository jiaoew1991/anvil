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
use std::collections::HashMap;

use crate::types::now_secs;

/// Anvil coordination state - minimal, no message storage
pub struct AnvilState {
    /// Known queue names (for stats listing)
    queues: DashMap<String, ()>,
    /// Lease last-seen timestamps (seconds since epoch)
    leases: DashMap<String, f64>,
}

impl AnvilState {
    pub fn new() -> Self {
        Self {
            queues: DashMap::new(),
            leases: DashMap::new(),
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

    /// Update lease heartbeat timestamp.
    pub fn update_lease(&self, lease_id: &str) {
        self.leases.insert(lease_id.to_string(), now_secs());
    }

    /// Get a snapshot of all leases (lease_id -> last_seen).
    pub fn lease_snapshot(&self) -> HashMap<String, f64> {
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
