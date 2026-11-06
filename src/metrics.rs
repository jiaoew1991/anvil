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

//! Broker metrics over OpenTelemetry.
//!
//! Export is opt-in through the standard OTLP environment: with
//! `OTEL_EXPORTER_OTLP_ENDPOINT` or `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` set,
//! metrics are pushed over OTLP/HTTP every `OTEL_METRIC_EXPORT_INTERVAL`
//! (default 60 s); without either, every instrument is a no-op and costs
//! nothing on the hot path. `service.name` defaults to `anvil` unless
//! `OTEL_SERVICE_NAME` or `OTEL_RESOURCE_ATTRIBUTES` names one.
//!
//! Queue depth and totals are observed from the in-memory queue counters at
//! export time, so they add no work to push, claim or ack. Only events with
//! no standing counter (redeliveries, reclaimed leases, GC deletions, RPC
//! latency) are recorded where they happen.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock, Weak};
use std::task::{Context, Poll};
use std::time::Instant;

use opentelemetry::metrics::{
    AsyncInstrument, Counter, Histogram, ObservableCounter, ObservableGauge,
};
use opentelemetry::{global, KeyValue};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::Resource;

use crate::state::AnvilState;
use crate::storage::{AnvilStorage, QueueMeta};

const METER_NAME: &str = "anvil";
const SERVICE_NAME_DEFAULT: &str = "anvil";

/// The process-wide provider, installed by the first broker that starts with
/// an OTLP endpoint configured. Instruments bind to the global provider when
/// they are created, so it is installed once and never replaced.
fn provider() -> Option<&'static SdkMeterProvider> {
    static PROVIDER: OnceLock<Option<SdkMeterProvider>> = OnceLock::new();
    PROVIDER
        .get_or_init(|| {
            let endpoint_configured = std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_some()
                || std::env::var_os("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT").is_some();
            if !endpoint_configured {
                return None;
            }
            let exporter = match opentelemetry_otlp::MetricExporter::builder()
                .with_http()
                .build()
            {
                Ok(exporter) => exporter,
                Err(e) => {
                    tracing::error!("OTLP metric exporter disabled: {}", e);
                    return None;
                }
            };
            let mut resource = Resource::builder();
            if std::env::var_os("OTEL_SERVICE_NAME").is_none() {
                resource = resource.with_service_name(SERVICE_NAME_DEFAULT);
            }
            let provider = SdkMeterProvider::builder()
                .with_periodic_exporter(exporter)
                .with_resource(resource.build())
                .build();
            global::set_meter_provider(provider.clone());
            Some(provider)
        })
        .as_ref()
}

/// Instruments recorded where their events happen.
pub struct Events {
    /// Claims of a message that had been delivered before.
    pub redelivered: Counter<u64>,
    /// Claims returned to pending because their lease died or they were held
    /// past `claim_age_timeout_secs`.
    pub reclaimed: Counter<u64>,
    /// Expired acked entries deleted by GC.
    pub gc_deleted: Counter<u64>,
    /// Server-side RPC latency, per the OpenTelemetry RPC conventions.
    pub rpc_duration: Histogram<f64>,
}

/// Event instruments, bound to the exporting provider when one is configured
/// and no-ops otherwise.
pub fn events() -> &'static Events {
    static EVENTS: OnceLock<Events> = OnceLock::new();
    EVENTS.get_or_init(|| {
        // Install first: instruments created before the provider would stay
        // bound to the no-op default for the life of the process.
        provider();
        let meter = global::meter(METER_NAME);
        Events {
            redelivered: meter
                .u64_counter("anvil.messages.redelivered")
                .with_unit("{message}")
                .with_description("Claims of a message that had been delivered before")
                .build(),
            reclaimed: meter
                .u64_counter("anvil.claims.reclaimed")
                .with_unit("{message}")
                .with_description("Claims returned to pending after their lease died or aged out")
                .build(),
            gc_deleted: meter
                .u64_counter("anvil.gc.deleted")
                .with_unit("{entry}")
                .with_description("Expired acked entries deleted by GC")
                .build(),
            rpc_duration: meter
                .f64_histogram("rpc.server.call.duration")
                .with_unit("s")
                .with_description("Duration of inbound RPCs until the response head")
                .build(),
        }
    })
}

/// Queue and lease gauges for one broker, observed at export time. Held by
/// the broker so the registrations live exactly as long as it does.
pub struct BrokerGauges {
    _pending: ObservableGauge<u64>,
    _claimed: ObservableGauge<u64>,
    _pushed: ObservableCounter<u64>,
    _acked: ObservableCounter<u64>,
    _leases: ObservableGauge<u64>,
}

impl BrokerGauges {
    /// Registers the gauges, or returns `None` when export is not configured.
    /// The callbacks hold weak references, so a stopped broker's storage is
    /// freed and simply stops reporting.
    pub fn register(storage: &Arc<AnvilStorage>, state: &Arc<AnvilState>) -> Option<Self> {
        provider()?;
        let meter = global::meter(METER_NAME);

        let gauge = |name: &'static str, description: &'static str, value: QueueValue| {
            meter
                .u64_observable_gauge(name)
                .with_unit("{message}")
                .with_description(description)
                .with_callback(observe_per_queue(storage, value))
                .build()
        };
        let total = |name: &'static str, description: &'static str, value: QueueValue| {
            meter
                .u64_observable_counter(name)
                .with_unit("{message}")
                .with_description(description)
                .with_callback(observe_per_queue(storage, value))
                .build()
        };

        let state: Weak<AnvilState> = Arc::downgrade(state);
        Some(Self {
            _pending: gauge(
                "anvil.queue.pending",
                "Messages waiting to be claimed",
                |meta| meta.push_seq.saturating_sub(meta.claim_seq),
            ),
            _claimed: gauge(
                "anvil.queue.claimed",
                "Messages claimed and not yet acked or returned",
                |meta| meta.claimed_count,
            ),
            _pushed: total(
                "anvil.queue.pushed",
                "Messages pushed since the queue was created",
                |meta| meta.total_pushed,
            ),
            _acked: total(
                "anvil.queue.acked",
                "Messages acked since the queue was created",
                |meta| meta.total_acked,
            ),
            _leases: meter
                .u64_observable_gauge("anvil.leases.active")
                .with_unit("{lease}")
                .with_description("Worker leases currently registered")
                .with_callback(move |observer| {
                    if let Some(state) = state.upgrade() {
                        observer.observe(state.lease_snapshot().len() as u64, &[]);
                    }
                })
                .build(),
        })
    }
}

/// Reads one number per queue from its in-memory metadata.
type QueueValue = fn(&QueueMeta) -> u64;

/// Callback observing `value` for every queue, labelled by queue name. It
/// holds the storage weakly, so it reports nothing once the broker is gone.
fn observe_per_queue(
    storage: &Arc<AnvilStorage>,
    value: QueueValue,
) -> impl Fn(&dyn AsyncInstrument<u64>) + Send + Sync + 'static {
    let storage: Weak<AnvilStorage> = Arc::downgrade(storage);
    move |observer| {
        if let Some(storage) = storage.upgrade() {
            for (queue, meta) in storage.queue_meta_snapshot() {
                observer.observe(value(&meta), &[KeyValue::new("queue", queue)]);
            }
        }
    }
}

/// Pushes buffered metrics to the collector; called when a broker stops.
pub fn flush() {
    if let Some(provider) = provider() {
        if let Err(e) = provider.force_flush() {
            tracing::warn!("metrics flush failed: {}", e);
        }
    }
}

/// Tower layer that times every RPC the gRPC server handles.
#[derive(Clone, Copy, Default)]
pub struct RpcMetricsLayer;

impl<S> tower_layer::Layer<S> for RpcMetricsLayer {
    type Service = RpcMetrics<S>;

    fn layer(&self, inner: S) -> Self::Service {
        RpcMetrics { inner }
    }
}

#[derive(Clone)]
pub struct RpcMetrics<S> {
    inner: S,
}

impl<S, RequestBody, ResponseBody> tower_service::Service<http::Request<RequestBody>>
    for RpcMetrics<S>
where
    S: tower_service::Service<http::Request<RequestBody>, Response = http::Response<ResponseBody>>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<S::Response, S::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<RequestBody>) -> Self::Future {
        // The path is `/anvil.Anvil/<Method>`.
        let method = request
            .uri()
            .path()
            .rsplit('/')
            .next()
            .unwrap_or_default()
            .to_string();
        let started = Instant::now();
        let response = self.inner.call(request);
        Box::pin(async move {
            let response = response.await;
            // A handler error is sent trailers-only, so its grpc-status is in
            // the headers; a success carries status 0 in the trailers, which
            // arrive after this point. Streaming RPCs are timed to their
            // response head.
            let status: i64 = match &response {
                Ok(response) => response
                    .headers()
                    .get("grpc-status")
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse().ok())
                    .unwrap_or(0),
                Err(_) => GRPC_STATUS_UNAVAILABLE,
            };
            events().rpc_duration.record(
                started.elapsed().as_secs_f64(),
                &[
                    KeyValue::new("rpc.system", "grpc"),
                    KeyValue::new("rpc.method", method),
                    KeyValue::new("rpc.grpc.status_code", status),
                ],
            );
            response
        })
    }
}

/// gRPC code reported when the transport fails before any response exists.
const GRPC_STATUS_UNAVAILABLE: i64 = 14;
