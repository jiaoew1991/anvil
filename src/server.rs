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

// Anvil gRPC Server
//
// Storage-only model: No startup recovery needed.
// Storage is the source of truth - broker can restart anytime.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use crate::recovery::{GcTask, RecoveryTask};
use crate::service::proto::anvil_server::AnvilServer;
use crate::service::AnvilService;
use crate::state::AnvilState;
use crate::storage::AnvilStorage;
use crate::types::AnvilConfig;

/// Anvil broker inner implementation
pub struct AnvilBrokerInner {
    pub config: AnvilConfig,
    pub state: Arc<AnvilState>,
    pub storage: Arc<AnvilStorage>,
    pub recovery_task: RecoveryTask,
    pub gc_task: GcTask,
    pub actual_port: Option<u16>,
}

impl AnvilBrokerInner {
    /// Create a new broker instance
    #[allow(dead_code)]
    pub async fn new(
        config: AnvilConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("Initializing Anvil broker...");
        tracing::info!("  Storage: {}", config.db_path);
        tracing::info!("  Claim timeout: {}s", config.claim_timeout_secs);
        tracing::info!(
            "  GC interval: {}s, retention: {}s",
            config.gc_interval_secs,
            config.acked_retention_secs
        );

        let storage = Arc::new(AnvilStorage::new(&config.db_path).await?);
        Self::new_with_storage(config, storage).await
    }

    /// Create a new broker instance using existing storage
    pub async fn new_with_storage(
        config: AnvilConfig,
        storage: Arc<AnvilStorage>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let state = Arc::new(AnvilState::new());

        // Recovery and GC tasks now only use storage (no memory state to recover)
        let recovery_task = RecoveryTask::new(storage.clone(), state.clone(), config.clone());
        let gc_task = GcTask::new(storage.clone(), config.clone());

        Ok(Self {
            config,
            state,
            storage,
            recovery_task,
            gc_task,
            actual_port: None,
        })
    }

    /// Start the gRPC server
    pub async fn start(&mut self) -> Result<u16, Box<dyn std::error::Error + Send + Sync>> {
        // No startup recovery needed - storage is the source of truth!
        // Just start background tasks.

        // Start background recovery task (recovers expired claims)
        self.recovery_task.start();

        // Start background GC task (cleans up acked messages)
        self.gc_task.start();

        // Create gRPC service
        let service = AnvilService::new(self.state.clone(), self.storage.clone());

        // Bind to address
        let addr: SocketAddr = format!("{}:{}", self.config.host, self.config.port)
            .parse()
            .map_err(|e| format!("Invalid address: {}", e))?;

        // Use TcpListener to get actual port
        let listener = TcpListener::bind(addr).await?;
        let actual_addr = listener.local_addr()?;
        self.actual_port = Some(actual_addr.port());

        tracing::info!("Anvil server starting on {}", actual_addr);

        // Convert to stream for tonic
        let incoming = TcpListenerStream::new(listener);

        // Spawn server task with HTTP2 keepalive settings
        tokio::spawn(async move {
            if let Err(e) = Server::builder()
                // HTTP2 keepalive: ping every 10s, timeout after 20s without response
                .http2_keepalive_interval(Some(Duration::from_secs(10)))
                .http2_keepalive_timeout(Some(Duration::from_secs(20)))
                // Allow keepalive pings even without active streams
                .tcp_keepalive(Some(Duration::from_secs(30)))
                .add_service(AnvilServer::new(service))
                .serve_with_incoming(incoming)
                .await
            {
                tracing::error!("Server error: {}", e);
            }
        });

        Ok(actual_addr.port())
    }

    /// Stop the broker gracefully (async version)
    pub async fn stop_async(&mut self) {
        tracing::info!("Stopping Anvil broker...");

        // Stop our background tasks that use storage
        self.recovery_task.stop_async().await;
        self.gc_task.stop_async().await;
    }

    /// Stop the broker (sync version - signals stop but doesn't wait)
    pub fn stop(&mut self) {
        tracing::info!("Stopping Anvil broker...");
        self.recovery_task.stop();
        self.gc_task.stop();
        // Note: storage.close() cannot be called here because it's async
        // The async version should be preferred for clean shutdown
    }
}

impl Drop for AnvilBrokerInner {
    fn drop(&mut self) {
        // Use sync stop in Drop - can't block
        // Note: This may not cleanly close SlateDB, but it's the best we can do in Drop
        self.stop();
    }
}
