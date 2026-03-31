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

// Anvil Python bindings using PyO3

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use tokio::runtime::Runtime;

pub mod client;
mod recovery;
mod server;
mod service;
mod state;
mod storage;
mod types;

#[cfg(test)]
mod bench;
#[cfg(test)]
mod dst;

use server::AnvilBrokerInner;
use storage::AnvilStorage;
use types::AnvilConfig;

/// Broker error type exposed to Python
#[pyclass]
#[derive(Clone)]
pub struct BrokerError {
    #[pyo3(get)]
    pub kind: String,
    #[pyo3(get)]
    pub message: String,
}

#[pymethods]
impl BrokerError {
    #[new]
    fn new(kind: String, message: String) -> Self {
        Self { kind, message }
    }

    fn __repr__(&self) -> String {
        format!(
            "BrokerError(kind='{}', message='{}')",
            self.kind, self.message
        )
    }

    fn __str__(&self) -> String {
        format!("{}: {}", self.kind, self.message)
    }
}

/// Broker configuration
#[pyclass]
#[derive(Clone)]
pub struct BrokerConfig {
    #[pyo3(get, set)]
    pub db_path: String,
    #[pyo3(get, set)]
    pub host: String,
    #[pyo3(get, set)]
    pub port: u16,
    #[pyo3(get, set)]
    pub claim_timeout_secs: f64,
    #[pyo3(get, set)]
    pub recovery_interval_secs: f64,
    #[pyo3(get, set)]
    pub max_queue_depth: usize,
    #[pyo3(get, set)]
    pub acked_retention_secs: f64,
    #[pyo3(get, set)]
    pub gc_interval_secs: f64,
}

#[pymethods]
impl BrokerConfig {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (db_path, host="0.0.0.0".to_string(), port=0, claim_timeout_secs=60.0, recovery_interval_secs=10.0, max_queue_depth=0, acked_retention_secs=3600.0, gc_interval_secs=60.0))]
    fn new(
        db_path: String,
        host: String,
        port: u16,
        claim_timeout_secs: f64,
        recovery_interval_secs: f64,
        max_queue_depth: usize,
        acked_retention_secs: f64,
        gc_interval_secs: f64,
    ) -> Self {
        Self {
            db_path,
            host,
            port,
            claim_timeout_secs,
            recovery_interval_secs,
            max_queue_depth,
            acked_retention_secs,
            gc_interval_secs,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "BrokerConfig(db_path='{}', host='{}', port={}, claim_timeout_secs={}, gc_interval_secs={})",
            self.db_path, self.host, self.port, self.claim_timeout_secs, self.gc_interval_secs
        )
    }
}

impl From<BrokerConfig> for AnvilConfig {
    fn from(config: BrokerConfig) -> Self {
        AnvilConfig {
            db_path: config.db_path,
            host: config.host,
            port: config.port,
            claim_timeout_secs: config.claim_timeout_secs,
            recovery_interval_secs: config.recovery_interval_secs,
            max_queue_depth: config.max_queue_depth,
            acked_retention_secs: config.acked_retention_secs,
            gc_interval_secs: config.gc_interval_secs,
        }
    }
}

/// Anvil Broker - embedded work queue server
#[pyclass]
pub struct AnvilBroker {
    config: BrokerConfig,
    handle: Option<JoinHandle<()>>,
    running: Arc<AtomicBool>,
    event_handler: Option<PyObject>,
    actual_port: Arc<Mutex<Option<u16>>>,
    // Storage is created inside the broker thread (to keep it in the same tokio runtime)
    // and shared back via this Arc<Mutex<>>
    storage: Arc<Mutex<Option<Arc<AnvilStorage>>>>,
}

#[pymethods]
impl AnvilBroker {
    #[new]
    #[pyo3(signature = (config, event_handler=None))]
    fn new(config: BrokerConfig, event_handler: Option<PyObject>) -> Self {
        Self {
            config,
            handle: None,
            running: Arc::new(AtomicBool::new(false)),
            event_handler,
            actual_port: Arc::new(Mutex::new(None)),
            storage: Arc::new(Mutex::new(None)),
        }
    }

    /// Start the broker (non-blocking)
    fn start(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.running.load(Ordering::SeqCst) {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Broker is already running",
            ));
        }

        self.running.store(true, Ordering::SeqCst);

        let config: AnvilConfig = self.config.clone().into();
        let handler = self.event_handler.as_ref().map(|h| h.clone_ref(py));
        let running = self.running.clone();
        let actual_port = self.actual_port.clone();
        // Storage will be created inside the broker thread and shared back via this Arc<Mutex<>>
        let storage_slot = self.storage.clone();

        let handle = std::thread::spawn(move || {
            // Initialize tracing
            let _ = tracing_subscriber::fmt().try_init();

            // Create a single runtime for the entire broker lifecycle
            // CRITICAL: SlateDB's internal background tasks (compactor, gc, memtable flusher)
            // are bound to the tokio runtime that creates the Db. If storage is created in a
            // different runtime than where it's used, the internal channels get closed when
            // the original runtime is dropped, causing "channel closed" panics.
            let rt = match Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    running.store(false, Ordering::SeqCst);
                    if let Some(h) = &handler {
                        Python::with_gil(|py| {
                            let error = BrokerError::new(
                                "runtime_error".to_string(),
                                format!("Failed to create runtime: {}", e),
                            );
                            let _ = h.call_method1(py, "on_fatal", (error,));
                        });
                    }
                    return;
                }
            };

            rt.block_on(async {
                // Create storage in the same runtime that will use it
                // CRITICAL: SlateDB's internal background tasks (compactor, gc, memtable flusher)
                // are bound to the tokio runtime that creates the Db. Storage must be created
                // and used in the same runtime to avoid "channel closed" panics.
                let storage = match AnvilStorage::new(&config.db_path).await {
                    Ok(s) => Arc::new(s),
                    Err(e) => {
                        running.store(false, Ordering::SeqCst);
                        if let Some(h) = &handler {
                            Python::with_gil(|py| {
                                let error = BrokerError::new(
                                    "storage_error".to_string(),
                                    format!("Failed to open storage {}: {}", config.db_path, e),
                                );
                                let _ = h.call_method1(py, "on_fatal", (error,));
                            });
                        }
                        return;
                    }
                };

                // Share storage reference back to the main struct for get_storage_reader()
                *storage_slot.lock().unwrap() = Some(storage.clone());

                match AnvilBrokerInner::new_with_storage(config, storage.clone()).await {
                    Ok(mut broker) => {
                        match broker.start().await {
                            Ok(port) => {
                                *actual_port.lock().unwrap() = Some(port);

                                // Trigger on_started callback
                                if let Some(h) = &handler {
                                    Python::with_gil(|py| {
                                        let _ = h.call_method1(py, "on_started", (port,));
                                    });
                                }

                                // Keep running until stopped
                                while running.load(Ordering::SeqCst) {
                                    tokio::time::sleep(tokio::time::Duration::from_millis(100))
                                        .await;
                                }

                                // Gracefully stop the broker, waiting for background tasks to finish
                                broker.stop_async().await;

                                // Trigger on_stopped callback
                                if let Some(h) = &handler {
                                    Python::with_gil(|py| {
                                        let _ = h.call_method0(py, "on_stopped");
                                    });
                                }
                            }
                            Err(e) => {
                                running.store(false, Ordering::SeqCst);
                                if let Some(h) = &handler {
                                    Python::with_gil(|py| {
                                        let error = BrokerError::new(
                                            "start_failed".to_string(),
                                            e.to_string(),
                                        );
                                        let _ = h.call_method1(py, "on_fatal", (error,));
                                    });
                                }
                            }
                        }
                    }
                    Err(e) => {
                        running.store(false, Ordering::SeqCst);
                        if let Some(h) = &handler {
                            Python::with_gil(|py| {
                                let error =
                                    BrokerError::new("init_failed".to_string(), e.to_string());
                                let _ = h.call_method1(py, "on_fatal", (error,));
                            });
                        }
                    }
                }
            });
        });

        self.handle = Some(handle);
        Ok(())
    }

    /// Create a storage reader backed by the broker's storage instance
    fn get_storage_reader(&self) -> PyResult<AnvilStorageReader> {
        let storage_guard = self.storage.lock().unwrap();
        let storage = storage_guard.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err(
                "Broker storage not available (start the broker first)",
            )
        })?;
        AnvilStorageReader::from_storage(self.config.db_path.clone(), storage.clone())
    }

    /// Stop the broker
    fn stop(&mut self, py: Python<'_>) -> PyResult<()> {
        self.running.store(false, Ordering::SeqCst);

        if let Some(handle) = self.handle.take() {
            py.allow_threads(|| {
                let _ = handle.join();
            });
        }

        Ok(())
    }

    /// Check if broker is running
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Get the actual port the broker is listening on
    fn get_port(&self) -> Option<u16> {
        *self.actual_port.lock().unwrap()
    }

    /// Get the broker URL (host:port)
    fn get_broker_url(&self) -> Option<String> {
        self.get_port()
            .map(|port| format!("{}:{}", self.config.host, port))
    }

    fn __repr__(&self) -> String {
        let status = if self.is_running() {
            "running"
        } else {
            "stopped"
        };
        format!(
            "AnvilBroker(config={:?}, status={})",
            self.config.__repr__(),
            status
        )
    }
}

/// Anvil Storage Reader - direct storage access (no RPC)
#[pyclass(unsendable)]
pub struct AnvilStorageReader {
    db_path: String,
    runtime: Runtime,
    storage: Arc<AnvilStorage>,
}

#[pymethods]
impl AnvilStorageReader {
    #[new]
    #[pyo3(signature = (db_path))]
    fn new(db_path: String) -> PyResult<Self> {
        let runtime = Runtime::new().map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to create runtime: {}", e))
        })?;
        let storage = runtime.block_on(AnvilStorage::new(&db_path)).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Failed to open storage {}: {}",
                db_path, e
            ))
        })?;
        Ok(Self {
            db_path,
            runtime,
            storage: Arc::new(storage),
        })
    }

    /// Get queue stats (pending/claimed/total)
    fn get_queue_stats(&self, py: Python<'_>, queue: String) -> PyResult<PyObject> {
        let meta = self
            .runtime
            .block_on(self.storage.get_queue_stats(&queue))
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to get stats for {}: {}",
                    queue, e
                ))
            })?;
        let pending = meta.push_seq.saturating_sub(meta.claim_seq);

        let dict = PyDict::new(py);
        dict.set_item("pending_count", pending)?;
        dict.set_item("claimed_count", meta.claimed_count)?;
        dict.set_item("total_pushed", meta.total_pushed)?;
        dict.set_item("total_acked", meta.total_acked)?;
        Ok(dict.into())
    }

    /// Scan acked messages (optionally filtered by queue and time range)
    #[pyo3(signature = (queue=None, start_ns=None, end_ns=None, limit=None))]
    fn scan_acked(
        &self,
        py: Python<'_>,
        queue: Option<String>,
        start_ns: Option<u64>,
        end_ns: Option<u64>,
        limit: Option<usize>,
    ) -> PyResult<PyObject> {
        let entries = self
            .runtime
            .block_on(self.storage.scan_acked(queue.as_deref()))
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to scan acked: {}", e))
            })?;
        let mut results = Vec::new();
        for (queue_name, ts_ns, msg_id) in entries {
            if let Some(start) = start_ns {
                if ts_ns < start {
                    continue;
                }
            }
            if let Some(end) = end_ns {
                if ts_ns > end {
                    continue;
                }
            }
            results.push((queue_name, ts_ns, msg_id));
            if let Some(max_items) = limit {
                if results.len() >= max_items {
                    break;
                }
            }
        }

        let list = PyList::empty(py);
        for (queue_name, ts_ns, msg_id) in results {
            let item = PyDict::new(py);
            item.set_item("queue", queue_name)?;
            item.set_item("timestamp_ns", ts_ns)?;
            item.set_item("msg_id", msg_id)?;
            list.append(item)?;
        }
        Ok(list.into())
    }

    /// Scan claimed messages (optionally filtered by queue)
    #[pyo3(signature = (queue=None, limit=None))]
    fn scan_claimed(
        &self,
        py: Python<'_>,
        queue: Option<String>,
        limit: Option<usize>,
    ) -> PyResult<PyObject> {
        let entries = self
            .runtime
            .block_on(self.storage.scan_claimed(queue.as_deref()))
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to scan claimed: {}", e))
            })?;
        let list = PyList::empty(py);
        let mut count = 0usize;
        for (queue_name, msg_id, claim) in entries {
            let item = PyDict::new(py);
            item.set_item("queue", queue_name)?;
            item.set_item("msg_id", msg_id)?;
            item.set_item("worker_id", claim.worker_id)?;
            item.set_item("lease_id", claim.lease_id)?;
            item.set_item("claimed_at", claim.claimed_at)?;
            item.set_item("claim_token", claim.claim_token)?;
            list.append(item)?;
            count += 1;
            if let Some(max_items) = limit {
                if count >= max_items {
                    break;
                }
            }
        }
        Ok(list.into())
    }

    /// Get state values by keys (bytes)
    fn state_get_batch(
        &self,
        py: Python<'_>,
        namespace: String,
        keys: Vec<String>,
    ) -> PyResult<PyObject> {
        let values = self
            .runtime
            .block_on(self.storage.state_get_batch(&namespace, &keys))
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to read state for {}: {}",
                    namespace, e
                ))
            })?;
        let dict = PyDict::new(py);
        for (key, value) in values {
            dict.set_item(key, PyBytes::new(py, &value))?;
        }
        Ok(dict.into())
    }

    /// Scan state keys by prefix (returns suffix keys and bytes)
    #[pyo3(signature = (namespace, prefix="", limit=None))]
    fn state_scan_prefix(
        &self,
        py: Python<'_>,
        namespace: String,
        prefix: &str,
        limit: Option<usize>,
    ) -> PyResult<PyObject> {
        let entries = self
            .runtime
            .block_on(
                self.storage
                    .state_scan_prefix(&namespace, prefix, limit.unwrap_or(0)),
            )
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to scan state for {}: {}",
                    namespace, e
                ))
            })?;
        let list = PyList::empty(py);
        for (key, value) in entries {
            let item = PyDict::new(py);
            item.set_item("key", key)?;
            item.set_item("value", PyBytes::new(py, &value))?;
            list.append(item)?;
        }
        Ok(list.into())
    }

    /// List queues from storage
    fn list_queues(&self, py: Python<'_>) -> PyResult<PyObject> {
        let queues = self
            .runtime
            .block_on(self.storage.list_queues())
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to list queues: {}", e))
            })?;
        let list = PyList::empty(py);
        for queue in queues {
            list.append(queue)?;
        }
        Ok(list.into())
    }

    fn __repr__(&self) -> String {
        format!("AnvilStorageReader(db_path='{}')", self.db_path)
    }
}

impl AnvilStorageReader {
    fn from_storage(db_path: String, storage: Arc<AnvilStorage>) -> PyResult<Self> {
        let runtime = Runtime::new().map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to create runtime: {}", e))
        })?;
        Ok(Self {
            db_path,
            runtime,
            storage,
        })
    }
}

/// Python module definition
#[pymodule]
fn anvil_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<BrokerConfig>()?;
    m.add_class::<BrokerError>()?;
    m.add_class::<AnvilBroker>()?;
    m.add_class::<AnvilStorageReader>()?;
    m.add_class::<client::AnvilRustClient>()?;
    m.add_class::<client::RustMessage>()?;
    Ok(())
}
