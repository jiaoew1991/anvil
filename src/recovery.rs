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

// Timeout recovery and GC for Anvil
//
// This module handles:
// 1. Runtime recovery: reclaim messages from dead workers (expired claims)
// 2. Garbage collection: delete acked messages after retention period
//
// Note: With the new storage-only model, startup recovery is automatic -
// storage is the source of truth, no memory state needs rebuilding.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration};

use crate::state::AnvilState;
use crate::storage::AnvilStorage;
use crate::types::AnvilConfig;

/// Recovery task manager - recovers expired claims
pub struct RecoveryTask {
    storage: Arc<AnvilStorage>,
    state: Arc<AnvilState>,
    config: AnvilConfig,
    running: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    handle: Option<JoinHandle<()>>,
}

impl RecoveryTask {
    pub fn new(storage: Arc<AnvilStorage>, state: Arc<AnvilState>, config: AnvilConfig) -> Self {
        Self {
            storage,
            state,
            config,
            running: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(Notify::new()),
            handle: None,
        }
    }

    /// Start the background recovery task
    pub fn start(&mut self) {
        if self.running.load(Ordering::SeqCst) {
            return;
        }

        self.running.store(true, Ordering::SeqCst);

        let storage = self.storage.clone();
        let state = self.state.clone();
        let running = self.running.clone();
        let shutdown_notify = self.shutdown_notify.clone();
        let interval_secs = self.config.recovery_interval_secs;
        let timeout_secs = self.config.claim_timeout_secs;

        let handle = tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs_f64(interval_secs));

            loop {
                tokio::select! {
                    biased;
                    // Check for shutdown signal first (highest priority)
                    _ = shutdown_notify.notified() => {
                        break;
                    }
                    _ = ticker.tick() => {
                        // Double-check running flag after waking up
                        if !running.load(Ordering::SeqCst) {
                            break;
                        }

                        let lease_snapshot = state.lease_snapshot();
                        // Recovery is now handled entirely by storage
                        if let Err(e) = storage
                            .recover_expired_claims(timeout_secs, Some(&lease_snapshot))
                            .await
                        {
                            tracing::error!("Recovery error: {}", e);
                        }
                    }
                }
            }
        });

        self.handle = Some(handle);
        tracing::info!(
            "Recovery task started (interval: {}s, timeout: {}s)",
            interval_secs,
            timeout_secs
        );
    }

    /// Stop the recovery task gracefully.
    /// This signals the task to stop and waits for it to complete.
    pub async fn stop_async(&mut self) {
        // Signal the task to stop
        self.running.store(false, Ordering::SeqCst);
        self.shutdown_notify.notify_one();

        // Wait for the task to finish gracefully
        if let Some(handle) = self.handle.take() {
            // Wait for task to complete (with timeout for safety)
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }

        tracing::info!("Recovery task stopped");
    }

    /// Stop the recovery task (sync version).
    /// Signals the task to stop but doesn't wait for completion.
    pub fn stop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        self.shutdown_notify.notify_one();

        // We can't block here, just let the task finish naturally
        // The handle will be dropped which doesn't abort the task
        if let Some(handle) = self.handle.take() {
            // Detach the handle - task will complete on its own
            drop(handle);
        }

        tracing::info!("Recovery task stopped");
    }
}

impl Drop for RecoveryTask {
    fn drop(&mut self) {
        // Signal stop but don't block
        self.running.store(false, Ordering::SeqCst);
        self.shutdown_notify.notify_one();
        // Don't abort - let the task finish naturally to avoid SlateDB panic
    }
}

/// GC task manager for cleaning up acked messages
pub struct GcTask {
    storage: Arc<AnvilStorage>,
    config: AnvilConfig,
    running: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    handle: Option<JoinHandle<()>>,
}

impl GcTask {
    pub fn new(storage: Arc<AnvilStorage>, config: AnvilConfig) -> Self {
        Self {
            storage,
            config,
            running: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(Notify::new()),
            handle: None,
        }
    }

    /// Start the background GC task
    pub fn start(&mut self) {
        if self.running.load(Ordering::SeqCst) {
            return;
        }

        self.running.store(true, Ordering::SeqCst);

        let storage = self.storage.clone();
        let running = self.running.clone();
        let shutdown_notify = self.shutdown_notify.clone();
        let interval_secs = self.config.gc_interval_secs;
        let retention_secs = self.config.acked_retention_secs;
        let retention_ns = (retention_secs * 1_000_000_000.0) as u64;

        let handle = tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs_f64(interval_secs));

            loop {
                tokio::select! {
                    biased;
                    // Check for shutdown signal first (highest priority)
                    _ = shutdown_notify.notified() => {
                        break;
                    }
                    _ = ticker.tick() => {
                        // Double-check running flag after waking up
                        if !running.load(Ordering::SeqCst) {
                            break;
                        }

                        if let Err(e) = storage.gc_acked_messages(retention_ns).await {
                            tracing::error!("GC error: {}", e);
                        }
                    }
                }
            }
        });

        self.handle = Some(handle);
        tracing::info!(
            "GC task started (interval: {}s, retention: {}s)",
            interval_secs,
            retention_secs
        );
    }

    /// Stop the GC task gracefully.
    /// This signals the task to stop and waits for it to complete.
    pub async fn stop_async(&mut self) {
        // Signal the task to stop
        self.running.store(false, Ordering::SeqCst);
        self.shutdown_notify.notify_one();

        // Wait for the task to finish gracefully
        if let Some(handle) = self.handle.take() {
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }

        tracing::info!("GC task stopped");
    }

    /// Stop the GC task (sync version).
    /// Signals the task to stop but doesn't wait for completion.
    pub fn stop(&mut self) {
        self.running.store(false, Ordering::SeqCst);
        self.shutdown_notify.notify_one();

        if let Some(handle) = self.handle.take() {
            drop(handle);
        }

        tracing::info!("GC task stopped");
    }
}

impl Drop for GcTask {
    fn drop(&mut self) {
        // Signal stop but don't block
        self.running.store(false, Ordering::SeqCst);
        self.shutdown_notify.notify_one();
        // Don't abort - let the task finish naturally to avoid SlateDB panic
    }
}
