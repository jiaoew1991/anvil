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

//! Rust gRPC client for Anvil (Protocol v2), exposed to Python via PyO3.
//!
//! All PyO3 methods keep the same names as the old Python client for backward
//! compatibility. Internally they use the unified Protocol v2 RPCs:
//!   - claim / claim_from_group  →  Claim RPC
//!   - ack / nack / ack_and_forward / ack_and_scatter  →  Complete RPC
//!   - push / push_batch  →  Push RPC
//!   - claim_and_complete  →  ClaimAndComplete RPC (new, halves round trips)

use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tonic::transport::Channel;

use crate::service::proto;
use proto::anvil_client::AnvilClient;

// ============================================================================
// Error Handling
// ============================================================================

fn status_to_pyerr(status: tonic::Status) -> PyErr {
    let msg = format!("{}: {}", status.code(), status.message());
    match status.code() {
        tonic::Code::NotFound | tonic::Code::AlreadyExists | tonic::Code::InvalidArgument => {
            pyo3::exceptions::PyValueError::new_err(msg)
        }
        tonic::Code::Unavailable | tonic::Code::Aborted => {
            pyo3::exceptions::PyConnectionError::new_err(msg)
        }
        _ => pyo3::exceptions::PyRuntimeError::new_err(msg),
    }
}

// ============================================================================
// RustMessage — PyO3-exposed message type
// ============================================================================

/// A message claimed from the queue (Rust-backed, zero-copy to Python).
#[pyclass]
#[derive(Clone)]
pub struct RustMessage {
    #[pyo3(get)]
    pub msg_id: String,
    #[pyo3(get)]
    pub queue: String,
    #[pyo3(get)]
    pub payload: Vec<u8>,
    #[pyo3(get)]
    pub created_at: f64,
    #[pyo3(get)]
    pub metadata: HashMap<String, String>,
    #[pyo3(get, set)]
    pub claim_token: Option<String>,
}

#[pymethods]
impl RustMessage {
    fn __repr__(&self) -> String {
        format!(
            "RustMessage(msg_id='{}', queue='{}', payload_len={})",
            self.msg_id,
            self.queue,
            self.payload.len()
        )
    }
}

impl RustMessage {
    /// Create from v2 ClaimMessage (no queue/created_at in wire format)
    fn from_claim_message(msg: &proto::ClaimMessage, queue: &str) -> Self {
        Self {
            msg_id: msg.msg_id.clone(),
            queue: queue.to_string(),
            payload: msg.payload.clone(),
            created_at: 0.0, // v2 drops created_at from wire
            metadata: msg.metadata.clone(),
            claim_token: Some(msg.claim_token.clone()),
        }
    }
}

// ============================================================================
// ClientInner — shared state (not exposed to Python)
// ============================================================================

struct ClientInner {
    runtime: Runtime,
    client: Mutex<Option<AnvilClient<Channel>>>,
    worker_id: String,
    server_address: String,
    heartbeat_interval: Duration,
    connect_timeout: Duration,
    lease_id: parking_lot::RwLock<String>,
    heartbeat_running: AtomicBool,
    heartbeat_handle: Mutex<Option<JoinHandle<()>>>,
}

impl ClientInner {
    fn get_client(&self) -> PyResult<AnvilClient<Channel>> {
        self.client
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err("Client not started"))
    }

    fn get_lease_id(&self) -> String {
        self.lease_id.read().clone()
    }
}

// ============================================================================
// Heartbeat
// ============================================================================

async fn heartbeat_loop(inner: Arc<ClientInner>) {
    let mut reconnect_attempts: u32 = 0;
    while inner.heartbeat_running.load(Ordering::SeqCst) {
        match run_heartbeat_stream(&inner).await {
            Ok(()) => {}
            Err(_) if inner.heartbeat_running.load(Ordering::SeqCst) => {
                reconnect_attempts += 1;
                if reconnect_attempts == 1 {
                    tracing::warn!("Heartbeat disconnected, reconnecting...");
                } else if reconnect_attempts.is_multiple_of(10) {
                    tracing::debug!("Heartbeat reconnect attempt {}", reconnect_attempts);
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(_) => break,
        }
    }
}

async fn run_heartbeat_stream(inner: &Arc<ClientInner>) -> Result<(), tonic::Status> {
    let mut client = inner
        .client
        .lock()
        .unwrap()
        .clone()
        .ok_or_else(|| tonic::Status::internal("Client not available"))?;

    let interval = inner.heartbeat_interval;
    let (tx, rx) = mpsc::channel::<proto::HeartbeatPing>(4);

    let ping_inner = inner.clone();
    let ping_worker_id = inner.worker_id.clone();
    tokio::spawn(async move {
        while ping_inner.heartbeat_running.load(Ordering::SeqCst) {
            let ping = proto::HeartbeatPing {
                worker_id: ping_worker_id.clone(),
                lease_id: ping_inner.get_lease_id(),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64,
            };
            if tx.send(ping).await.is_err() {
                break;
            }
            tokio::time::sleep(interval).await;
        }
    });

    let response = client
        .heartbeat_stream(tokio_stream::wrappers::ReceiverStream::new(rx))
        .await?;
    let mut stream = response.into_inner();

    while inner.heartbeat_running.load(Ordering::SeqCst) {
        match stream.message().await? {
            Some(pong) => {
                let mut lease = inner.lease_id.write();
                *lease = pong.lease_id;
                if !pong.ok {
                    tracing::warn!("Lease invalidated by server");
                    *lease = String::new();
                }
            }
            None => break,
        }
    }
    Ok(())
}

// ============================================================================
// Helper: build proto requests
// ============================================================================

fn make_state_update(
    namespace: Option<String>,
    puts: Option<HashMap<String, Vec<u8>>>,
    deletes: Option<Vec<String>>,
) -> Option<proto::StateUpdate> {
    let ns = namespace.unwrap_or_default();
    let p = puts.unwrap_or_default();
    let d = deletes.unwrap_or_default();
    if ns.is_empty() || (p.is_empty() && d.is_empty()) {
        None
    } else {
        Some(proto::StateUpdate {
            namespace: ns,
            puts: p,
            deletes: d,
        })
    }
}

// ============================================================================
// AnvilRustClient — PyO3-exposed client
// ============================================================================

/// High-performance Rust gRPC client for Anvil (Protocol v2).
#[pyclass]
pub struct AnvilRustClient {
    inner: Arc<ClientInner>,
}

#[pymethods]
impl AnvilRustClient {
    #[new]
    #[pyo3(signature = (server_address, worker_id, heartbeat_interval_secs=5.0, connect_timeout_secs=10.0))]
    fn new(
        server_address: String,
        worker_id: String,
        heartbeat_interval_secs: f64,
        connect_timeout_secs: f64,
    ) -> PyResult<Self> {
        let runtime = Runtime::new().map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to create runtime: {}", e))
        })?;
        Ok(Self {
            inner: Arc::new(ClientInner {
                runtime,
                client: Mutex::new(None),
                worker_id,
                server_address,
                heartbeat_interval: Duration::from_secs_f64(heartbeat_interval_secs),
                connect_timeout: Duration::from_secs_f64(connect_timeout_secs),
                lease_id: parking_lot::RwLock::new(String::new()),
                heartbeat_running: AtomicBool::new(false),
                heartbeat_handle: Mutex::new(None),
            }),
        })
    }

    fn start(&self, py: Python<'_>) -> PyResult<()> {
        let inner = self.inner.clone();
        py.allow_threads(move || {
            let channel = inner.runtime.block_on(async {
                let endpoint = tonic::transport::Endpoint::from_shared(format!(
                    "http://{}",
                    inner.server_address
                ))
                .map_err(|e| {
                    pyo3::exceptions::PyValueError::new_err(format!("Invalid address: {}", e))
                })?
                .connect_timeout(inner.connect_timeout)
                .timeout(Duration::from_secs(30));
                endpoint.connect().await.map_err(|e| {
                    pyo3::exceptions::PyConnectionError::new_err(format!(
                        "Failed to connect to {}: {}",
                        inner.server_address, e
                    ))
                })
            })?;

            *inner.client.lock().unwrap() = Some(AnvilClient::new(channel));
            inner.heartbeat_running.store(true, Ordering::SeqCst);
            let hb_inner = inner.clone();
            let handle = inner.runtime.spawn(heartbeat_loop(hb_inner));
            *inner.heartbeat_handle.lock().unwrap() = Some(handle);

            let deadline = std::time::Instant::now() + inner.connect_timeout;
            while std::time::Instant::now() < deadline {
                if !inner.get_lease_id().is_empty() {
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            Err(pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Failed to acquire lease within {}s",
                inner.connect_timeout.as_secs_f64()
            )))
        })
    }

    fn stop(&self, py: Python<'_>) -> PyResult<()> {
        let inner = self.inner.clone();
        py.allow_threads(move || {
            inner.heartbeat_running.store(false, Ordering::SeqCst);
            if let Some(handle) = inner.heartbeat_handle.lock().unwrap().take() {
                let _ = inner.runtime.block_on(handle);
            }
            *inner.client.lock().unwrap() = None;
            *inner.lease_id.write() = String::new();
        });
        Ok(())
    }

    #[getter]
    fn lease_id(&self) -> String {
        self.inner.get_lease_id()
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<PyObject>,
        _exc_val: Option<PyObject>,
        _exc_tb: Option<PyObject>,
    ) -> PyResult<()> {
        self.stop(py)
    }

    fn __repr__(&self) -> String {
        format!(
            "AnvilRustClient(server='{}', worker='{}')",
            self.inner.server_address, self.inner.worker_id
        )
    }

    // ========================================================================
    // Consumer API (uses unified Claim RPC)
    // ========================================================================

    #[pyo3(signature = (queue, batch_size=1, timeout_ms=5000))]
    fn claim(
        &self,
        py: Python<'_>,
        queue: String,
        batch_size: i32,
        timeout_ms: i32,
    ) -> PyResult<Vec<RustMessage>> {
        let inner = self.inner.clone();
        let q = queue.clone();
        py.allow_threads(move || {
            inner.runtime.block_on(async {
                let mut client = inner.get_client()?;
                let request = proto::ClaimRequest {
                    source: Some(proto::claim_request::Source::Queue(q.clone())),
                    worker_id: inner.worker_id.clone(),
                    lease_id: inner.get_lease_id(),
                    batch_size,
                    timeout_ms,
                };
                let resp = client
                    .claim(request)
                    .await
                    .map_err(status_to_pyerr)?
                    .into_inner();
                Ok(resp
                    .messages
                    .iter()
                    .map(|m| RustMessage::from_claim_message(m, &q))
                    .collect())
            })
        })
    }

    #[pyo3(signature = (group_name, batch_size=1, timeout_ms=5000, assigned_partitions=None, allow_steal=false, steal_pending_threshold=0))]
    #[allow(clippy::too_many_arguments)]
    fn claim_from_group(
        &self,
        py: Python<'_>,
        group_name: String,
        batch_size: i32,
        timeout_ms: i32,
        assigned_partitions: Option<Vec<i32>>,
        allow_steal: bool,
        steal_pending_threshold: i64,
    ) -> PyResult<(Vec<RustMessage>, String, i32)> {
        let inner = self.inner.clone();
        py.allow_threads(move || {
            inner.runtime.block_on(async {
                let mut client = inner.get_client()?;
                let request = proto::ClaimRequest {
                    source: Some(proto::claim_request::Source::Group(
                        proto::GroupClaimSource {
                            group_name: group_name.clone(),
                            assigned_partitions: assigned_partitions.unwrap_or_default(),
                            allow_steal,
                            steal_pending_threshold,
                        },
                    )),
                    worker_id: inner.worker_id.clone(),
                    lease_id: inner.get_lease_id(),
                    batch_size,
                    timeout_ms,
                };
                let resp = client
                    .claim(request)
                    .await
                    .map_err(status_to_pyerr)?
                    .into_inner();
                let source_q = if resp.source_queue.is_empty() {
                    group_name
                } else {
                    resp.source_queue.clone()
                };
                let messages: Vec<RustMessage> = resp
                    .messages
                    .iter()
                    .map(|m| RustMessage::from_claim_message(m, &source_q))
                    .collect();
                Ok((messages, resp.source_queue, resp.source_partition))
            })
        })
    }

    // ========================================================================
    // Complete API (uses unified Complete RPC)
    // ========================================================================

    #[pyo3(signature = (queue, msg_ids, claim_tokens=None, state_namespace=None, state_puts=None, state_deletes=None))]
    #[allow(clippy::too_many_arguments)]
    fn ack(
        &self,
        py: Python<'_>,
        queue: String,
        msg_ids: Vec<String>,
        claim_tokens: Option<Vec<String>>,
        state_namespace: Option<String>,
        state_puts: Option<HashMap<String, Vec<u8>>>,
        state_deletes: Option<Vec<String>>,
    ) -> PyResult<i32> {
        if !msg_ids.is_empty() {
            match &claim_tokens {
                Some(t) if t.len() == msg_ids.len() => {}
                _ => {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "claim_tokens must match msg_ids length",
                    ));
                }
            }
        }
        let inner = self.inner.clone();
        py.allow_threads(move || {
            inner.runtime.block_on(async {
                let mut client = inner.get_client()?;
                let request = proto::CompleteRequest {
                    upstream_queue: queue,
                    msg_ids,
                    claim_tokens: claim_tokens.unwrap_or_default(),
                    worker_id: inner.worker_id.clone(),
                    lease_id: inner.get_lease_id(),
                    action: Some(proto::complete_request::Action::Ack(proto::AckAction {})),
                    state: make_state_update(state_namespace, state_puts, state_deletes),
                };
                let resp = client
                    .complete(request)
                    .await
                    .map_err(status_to_pyerr)?
                    .into_inner();
                Ok(resp.processed_count)
            })
        })
    }

    #[pyo3(signature = (queue, msg_ids, claim_tokens=None, reason="processing_failed", delay_ms=0, state_namespace=None, state_puts=None, state_deletes=None))]
    #[allow(clippy::too_many_arguments)]
    fn nack(
        &self,
        py: Python<'_>,
        queue: String,
        msg_ids: Vec<String>,
        claim_tokens: Option<Vec<String>>,
        reason: &str,
        #[allow(unused_variables)] delay_ms: i32,
        state_namespace: Option<String>,
        state_puts: Option<HashMap<String, Vec<u8>>>,
        state_deletes: Option<Vec<String>>,
    ) -> PyResult<i32> {
        let _ = reason; // v2: NackReason removed (server ignores it)
        if !msg_ids.is_empty() {
            match &claim_tokens {
                Some(t) if t.len() == msg_ids.len() => {}
                _ => {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "claim_tokens must match msg_ids length",
                    ));
                }
            }
        }
        let inner = self.inner.clone();
        py.allow_threads(move || {
            inner.runtime.block_on(async {
                let mut client = inner.get_client()?;
                let request = proto::CompleteRequest {
                    upstream_queue: queue,
                    msg_ids,
                    claim_tokens: claim_tokens.unwrap_or_default(),
                    worker_id: inner.worker_id.clone(),
                    lease_id: inner.get_lease_id(),
                    action: Some(proto::complete_request::Action::Nack(proto::NackAction {})),
                    state: make_state_update(state_namespace, state_puts, state_deletes),
                };
                let resp = client
                    .complete(request)
                    .await
                    .map_err(status_to_pyerr)?
                    .into_inner();
                Ok(resp.processed_count)
            })
        })
    }

    #[pyo3(signature = (upstream_queue, upstream_msg_ids, upstream_claim_tokens, downstream_queue, downstream_payloads, state_namespace=None, state_puts=None, state_deletes=None))]
    #[allow(clippy::too_many_arguments)]
    fn ack_and_forward(
        &self,
        py: Python<'_>,
        upstream_queue: String,
        upstream_msg_ids: Vec<String>,
        upstream_claim_tokens: Option<Vec<String>>,
        downstream_queue: String,
        downstream_payloads: Vec<Vec<u8>>,
        state_namespace: Option<String>,
        state_puts: Option<HashMap<String, Vec<u8>>>,
        state_deletes: Option<Vec<String>>,
    ) -> PyResult<Vec<String>> {
        if !upstream_msg_ids.is_empty() {
            match &upstream_claim_tokens {
                Some(t) if t.len() == upstream_msg_ids.len() => {}
                _ => {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "upstream_claim_tokens must match upstream_msg_ids length",
                    ));
                }
            }
        }
        let inner = self.inner.clone();
        py.allow_threads(move || {
            inner.runtime.block_on(async {
                let mut client = inner.get_client()?;
                let request = proto::CompleteRequest {
                    upstream_queue,
                    msg_ids: upstream_msg_ids,
                    claim_tokens: upstream_claim_tokens.unwrap_or_default(),
                    worker_id: inner.worker_id.clone(),
                    lease_id: inner.get_lease_id(),
                    action: Some(proto::complete_request::Action::Forward(
                        proto::ForwardAction {
                            downstream_queue,
                            payloads: downstream_payloads,
                        },
                    )),
                    state: make_state_update(state_namespace, state_puts, state_deletes),
                };
                let resp = client
                    .complete(request)
                    .await
                    .map_err(status_to_pyerr)?
                    .into_inner();
                if !resp.success {
                    return Err(pyo3::exceptions::PyRuntimeError::new_err(
                        "AckAndForward failed",
                    ));
                }
                Ok(resp.new_msg_ids)
            })
        })
    }

    #[pyo3(signature = (upstream_queue, upstream_msg_ids, upstream_claim_tokens, group_name, partition_payloads, state_namespace=None, state_puts=None, state_deletes=None))]
    #[allow(clippy::too_many_arguments)]
    fn ack_and_scatter(
        &self,
        py: Python<'_>,
        upstream_queue: String,
        upstream_msg_ids: Vec<String>,
        upstream_claim_tokens: Option<Vec<String>>,
        group_name: String,
        partition_payloads: HashMap<i32, Vec<Vec<u8>>>,
        state_namespace: Option<String>,
        state_puts: Option<HashMap<String, Vec<u8>>>,
        state_deletes: Option<Vec<String>>,
    ) -> PyResult<Vec<String>> {
        if !upstream_msg_ids.is_empty() {
            match &upstream_claim_tokens {
                Some(t) if t.len() == upstream_msg_ids.len() => {}
                _ => {
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "upstream_claim_tokens must match upstream_msg_ids length",
                    ));
                }
            }
        }
        let partitions: Vec<proto::PartitionPayload> = partition_payloads
            .into_iter()
            .map(|(pid, payloads)| proto::PartitionPayload {
                partition_id: pid,
                payloads,
            })
            .collect();
        let inner = self.inner.clone();
        py.allow_threads(move || {
            inner.runtime.block_on(async {
                let mut client = inner.get_client()?;
                let request = proto::CompleteRequest {
                    upstream_queue,
                    msg_ids: upstream_msg_ids,
                    claim_tokens: upstream_claim_tokens.unwrap_or_default(),
                    worker_id: inner.worker_id.clone(),
                    lease_id: inner.get_lease_id(),
                    action: Some(proto::complete_request::Action::Scatter(
                        proto::ScatterAction {
                            group_name,
                            partitions,
                        },
                    )),
                    state: make_state_update(state_namespace, state_puts, state_deletes),
                };
                let resp = client
                    .complete(request)
                    .await
                    .map_err(status_to_pyerr)?
                    .into_inner();
                if !resp.success {
                    return Err(pyo3::exceptions::PyRuntimeError::new_err(
                        "AckAndScatter failed",
                    ));
                }
                Ok(resp.new_msg_ids)
            })
        })
    }

    // ========================================================================
    // Producer API (uses unified Push RPC)
    // ========================================================================

    #[pyo3(signature = (queue, payload, metadata=None))]
    fn push(
        &self,
        py: Python<'_>,
        queue: String,
        payload: Vec<u8>,
        metadata: Option<HashMap<String, String>>,
    ) -> PyResult<String> {
        let inner = self.inner.clone();
        py.allow_threads(move || {
            inner.runtime.block_on(async {
                let mut client = inner.get_client()?;
                let request = proto::PushRequest {
                    queue,
                    payloads: vec![payload],
                    metadata: metadata.unwrap_or_default(),
                };
                let resp = client
                    .push(request)
                    .await
                    .map_err(status_to_pyerr)?
                    .into_inner();
                resp.msg_ids.into_iter().next().ok_or_else(|| {
                    pyo3::exceptions::PyRuntimeError::new_err("Push returned no msg_id")
                })
            })
        })
    }

    fn push_batch(
        &self,
        py: Python<'_>,
        queue: String,
        payloads: Vec<Vec<u8>>,
    ) -> PyResult<Vec<String>> {
        let inner = self.inner.clone();
        py.allow_threads(move || {
            inner.runtime.block_on(async {
                let mut client = inner.get_client()?;
                let request = proto::PushRequest {
                    queue,
                    payloads,
                    metadata: HashMap::new(),
                };
                let resp = client
                    .push(request)
                    .await
                    .map_err(status_to_pyerr)?
                    .into_inner();
                Ok(resp.msg_ids)
            })
        })
    }

    // ========================================================================
    // Combined ClaimAndComplete (new — halves round trips)
    // ========================================================================

    /// Combined claim + complete in one RPC round trip.
    ///
    /// On first call, pass complete_request=None. On subsequent calls, pass the
    /// complete request for the previous batch alongside the claim for the next.
    #[pyo3(signature = (claim_request, complete_request=None))]
    fn claim_and_complete(
        &self,
        py: Python<'_>,
        claim_request: PyObject,
        complete_request: Option<PyObject>,
    ) -> PyResult<PyObject> {
        // This method accepts Python dicts and returns a Python dict.
        // For now, expose the raw unified API. The Python wrapper can
        // build the dicts.
        let _ = (py, claim_request, complete_request);
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "claim_and_complete requires Python-level wrapper (use claim + ack separately for now)",
        ))
    }

    // ========================================================================
    // State API (unchanged)
    // ========================================================================

    fn state_get(
        &self,
        py: Python<'_>,
        namespace: String,
        keys: Vec<String>,
    ) -> PyResult<HashMap<String, Vec<u8>>> {
        let inner = self.inner.clone();
        py.allow_threads(move || {
            inner.runtime.block_on(async {
                let mut client = inner.get_client()?;
                let request = proto::StateGetRequest { namespace, keys };
                let resp = client
                    .state_get(request)
                    .await
                    .map_err(status_to_pyerr)?
                    .into_inner();
                Ok(resp.values)
            })
        })
    }

    #[pyo3(signature = (namespace, puts=None, deletes=None))]
    fn state_put(
        &self,
        py: Python<'_>,
        namespace: String,
        puts: Option<HashMap<String, Vec<u8>>>,
        deletes: Option<Vec<String>>,
    ) -> PyResult<(i32, i32)> {
        let inner = self.inner.clone();
        py.allow_threads(move || {
            inner.runtime.block_on(async {
                let mut client = inner.get_client()?;
                let request = proto::StatePutRequest {
                    namespace,
                    puts: puts.unwrap_or_default(),
                    deletes: deletes.unwrap_or_default(),
                };
                let resp = client
                    .state_put(request)
                    .await
                    .map_err(status_to_pyerr)?
                    .into_inner();
                Ok((resp.puts_count, resp.deletes_count))
            })
        })
    }

    // ========================================================================
    // Admin API (unchanged)
    // ========================================================================

    #[pyo3(signature = (queue, max_depth=0))]
    fn create_queue(&self, py: Python<'_>, queue: String, max_depth: i32) -> PyResult<bool> {
        let inner = self.inner.clone();
        py.allow_threads(move || {
            inner.runtime.block_on(async {
                let mut client = inner.get_client()?;
                let request = proto::CreateQueueRequest {
                    queue,
                    max_pending: max_depth.max(0) as u64,
                };
                let resp = client
                    .create_queue(request)
                    .await
                    .map_err(status_to_pyerr)?
                    .into_inner();
                Ok(resp.created)
            })
        })
    }

    #[pyo3(signature = (queue, force=false))]
    fn delete_queue(
        &self,
        py: Python<'_>,
        queue: String,
        #[allow(unused)] force: bool,
    ) -> PyResult<(bool, i32)> {
        let inner = self.inner.clone();
        py.allow_threads(move || {
            inner.runtime.block_on(async {
                let mut client = inner.get_client()?;
                let request = proto::DeleteQueueRequest { queue };
                let resp = client
                    .delete_queue(request)
                    .await
                    .map_err(status_to_pyerr)?
                    .into_inner();
                Ok((resp.deleted, resp.messages_deleted))
            })
        })
    }

    #[pyo3(signature = (queue=None))]
    fn get_stats(&self, py: Python<'_>, queue: Option<String>) -> PyResult<PyObject> {
        let inner = self.inner.clone();
        let stats = py.allow_threads(move || {
            inner.runtime.block_on(async {
                let mut client = inner.get_client()?;
                let request = proto::GetStatsRequest {
                    queue: queue.unwrap_or_default(),
                };
                client
                    .get_stats(request)
                    .await
                    .map_err(status_to_pyerr)
                    .map(|r| r.into_inner())
            })
        })?;
        let queues_dict = PyDict::new(py);
        for (q, s) in &stats.queues {
            let qd = PyDict::new(py);
            qd.set_item("pending_count", s.pending_count)?;
            qd.set_item("claimed_count", s.claimed_count)?;
            qd.set_item("total_pushed", s.total_pushed)?;
            qd.set_item("total_acked", s.total_acked)?;
            queues_dict.set_item(q, qd)?;
        }
        let result = PyDict::new(py);
        result.set_item("queues", queues_dict)?;
        result.set_item("total_workers", stats.total_workers)?;
        result.set_item("uptime_secs", stats.uptime_secs)?;
        Ok(result.into())
    }

    // ========================================================================
    // Queue Completion API (unchanged)
    // ========================================================================

    fn mark_queue_finished(&self, py: Python<'_>, queue: String) -> PyResult<bool> {
        let inner = self.inner.clone();
        py.allow_threads(move || {
            inner.runtime.block_on(async {
                let mut client = inner.get_client()?;
                let request = proto::MarkQueueFinishedRequest { queue };
                let resp = client
                    .mark_queue_finished(request)
                    .await
                    .map_err(status_to_pyerr)?
                    .into_inner();
                Ok(resp.success)
            })
        })
    }

    fn is_queue_finished(&self, py: Python<'_>, queue: String) -> PyResult<PyObject> {
        let inner = self.inner.clone();
        let resp = py.allow_threads(move || {
            inner.runtime.block_on(async {
                let mut client = inner.get_client()?;
                let request = proto::IsQueueFinishedRequest { queue };
                client
                    .is_queue_finished(request)
                    .await
                    .map_err(status_to_pyerr)
                    .map(|r| r.into_inner())
            })
        })?;
        let dict = PyDict::new(py);
        dict.set_item("finished", resp.finished)?;
        dict.set_item("drained", resp.drained)?;
        dict.set_item("safe_to_exit", resp.safe_to_exit)?;
        dict.set_item("pending_count", resp.pending_count)?;
        dict.set_item("claimed_count", resp.claimed_count)?;
        Ok(dict.into())
    }

    // ========================================================================
    // QueueGroup API (unchanged)
    // ========================================================================

    #[pyo3(signature = (group_name, num_partitions, max_pending_per_partition=0))]
    fn create_queue_group(
        &self,
        py: Python<'_>,
        group_name: String,
        num_partitions: i32,
        max_pending_per_partition: u64,
    ) -> PyResult<PyObject> {
        let inner = self.inner.clone();
        let resp = py.allow_threads(move || {
            inner.runtime.block_on(async {
                let mut client = inner.get_client()?;
                let request = proto::CreateQueueGroupRequest {
                    group_name,
                    num_partitions,
                    max_pending_per_partition,
                };
                client
                    .create_queue_group(request)
                    .await
                    .map_err(status_to_pyerr)
                    .map(|r| r.into_inner())
            })
        })?;
        let dict = PyDict::new(py);
        dict.set_item("queue_names", resp.queue_names)?;
        dict.set_item("version", resp.version)?;
        dict.set_item("created", resp.created)?;
        Ok(dict.into())
    }

    fn is_group_finished(&self, py: Python<'_>, group_name: String) -> PyResult<PyObject> {
        let inner = self.inner.clone();
        let resp = py.allow_threads(move || {
            inner.runtime.block_on(async {
                let mut client = inner.get_client()?;
                let request = proto::IsGroupFinishedRequest { group_name };
                client
                    .is_group_finished(request)
                    .await
                    .map_err(status_to_pyerr)
                    .map(|r| r.into_inner())
            })
        })?;
        let partitions = pyo3::types::PyList::empty(py);
        for p in &resp.partitions {
            let pd = PyDict::new(py);
            pd.set_item("partition_id", p.partition_id)?;
            pd.set_item("pending_count", p.pending_count)?;
            pd.set_item("claimed_count", p.claimed_count)?;
            pd.set_item("finished", p.finished)?;
            partitions.append(pd)?;
        }
        let dict = PyDict::new(py);
        dict.set_item("all_finished", resp.all_finished)?;
        dict.set_item("all_drained", resp.all_drained)?;
        dict.set_item("safe_to_exit", resp.safe_to_exit)?;
        dict.set_item("partitions", partitions)?;
        Ok(dict.into())
    }

    fn get_group_stats(&self, py: Python<'_>, group_name: String) -> PyResult<PyObject> {
        let inner = self.inner.clone();
        let resp = py.allow_threads(move || {
            inner.runtime.block_on(async {
                let mut client = inner.get_client()?;
                let request = proto::GetGroupStatsRequest { group_name };
                client
                    .get_group_stats(request)
                    .await
                    .map_err(status_to_pyerr)
                    .map(|r| r.into_inner())
            })
        })?;
        let partitions = pyo3::types::PyList::empty(py);
        for p in &resp.partitions {
            let pd = PyDict::new(py);
            pd.set_item("partition_id", p.partition_id)?;
            pd.set_item("pending_count", p.pending_count)?;
            pd.set_item("claimed_count", p.claimed_count)?;
            pd.set_item("total_pushed", p.total_pushed)?;
            pd.set_item("total_acked", p.total_acked)?;
            partitions.append(pd)?;
        }
        let dict = PyDict::new(py);
        dict.set_item("partitions", partitions)?;
        dict.set_item("total_pending", resp.total_pending)?;
        dict.set_item("total_claimed", resp.total_claimed)?;
        dict.set_item("skew_ratio", resp.skew_ratio)?;
        dict.set_item("hot_partitions", resp.hot_partitions)?;
        dict.set_item("max_partition_pending", resp.max_partition_pending)?;
        dict.set_item("median_partition_pending", resp.median_partition_pending)?;
        dict.set_item("version", resp.version)?;
        Ok(dict.into())
    }

    fn mark_group_finished(&self, py: Python<'_>, group_name: String) -> PyResult<PyObject> {
        let inner = self.inner.clone();
        let resp = py.allow_threads(move || {
            inner.runtime.block_on(async {
                let mut client = inner.get_client()?;
                let request = proto::MarkGroupFinishedRequest { group_name };
                client
                    .mark_group_finished(request)
                    .await
                    .map_err(status_to_pyerr)
                    .map(|r| r.into_inner())
            })
        })?;
        let dict = PyDict::new(py);
        dict.set_item("success", resp.success)?;
        dict.set_item("queues_marked", resp.queues_marked)?;
        Ok(dict.into())
    }
}
