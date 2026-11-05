# Anvil

Anvil is a work queue with one producer side and many competing consumers,
served over gRPC and stored in [SlateDB](https://slatedb.io) (`memory://`,
`file://` or `s3://`). It ships as a Rust server with Python bindings (PyO3).

It was extracted from [nurion](https://github.com/lumalabs/nurion), where it
runs as the per-job broker embedded in the Ray driver.

## Guarantees

- **Fenced completion.** Every claim carries a `claim_token` and the worker's
  `lease_id`. A worker whose message was redelivered cannot ack it: the token
  rotates on redelivery and a stale ack is rejected.
- **Atomic complete.** `Complete` acks upstream messages, pushes downstream
  payloads (`ack_and_forward`, `ack_and_scatter`) and writes key-value state in
  one storage batch, so a crash never leaves a message acked without its output.
- **Heartbeat leases.** Workers keep a `HeartbeatStream` open. Claims held by a
  lease that misses heartbeats for `lease_timeout_secs` return to pending; a
  claim held longer than `claim_age_timeout_secs` under a live lease is treated
  as stuck. Set the second above your slowest task.
- **Drain signal.** `claim` reports `upstream_drained` once a queue is marked
  finished and empty.
- **Delivery count.** Every claimed message carries `delivery_count`: 1 on
  first delivery, plus one for each nack or reclaimed lease, so a worker can
  park a poison message instead of retrying it forever.

See [docs/semantics.md](docs/semantics.md) for the full consistency and
recovery model.

## Build

Requires a Rust toolchain, `protoc`, and Python 3.10+. Build with
[maturin](https://www.maturin.rs), not `cargo build` (see [BUILD.md](BUILD.md)):

```bash
uvx maturin build --release      # wheel in target/wheels/
cargo test --release             # Rust test suite
```

## Usage

```python
from anvil_py import AnvilBroker, AnvilRustClient, BrokerConfig

broker = AnvilBroker(BrokerConfig(
    "file:///tmp/anvil",
    lease_timeout_secs=30.0,
    claim_age_timeout_secs=6 * 3600.0,
))
broker.start()  # binds asynchronously; poll get_port() until it is set

client = AnvilRustClient(broker.get_broker_url(), "worker-1")
client.start()
client.create_queue("work")
client.push("work", b"payload")

messages, drained = client.claim("work", batch_size=1)
for message in messages:
    client.ack("work", [message.msg_id], claim_tokens=[message.claim_token])
```

## Metrics

The broker exports OpenTelemetry metrics over OTLP/HTTP when
`OTEL_EXPORTER_OTLP_ENDPOINT` (or `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`) is
set, every `OTEL_METRIC_EXPORT_INTERVAL` milliseconds (default 60000). Without
either variable, every instrument is a no-op. `service.name` defaults to
`anvil`; `OTEL_SERVICE_NAME` and `OTEL_RESOURCE_ATTRIBUTES` override it.

| Metric | Type | Attributes | Meaning |
| --- | --- | --- | --- |
| `anvil.queue.pending` | gauge | `queue` | Messages waiting to be claimed |
| `anvil.queue.claimed` | gauge | `queue` | Messages claimed, not yet acked or returned |
| `anvil.queue.pushed` | counter | `queue` | Messages pushed since the queue was created |
| `anvil.queue.acked` | counter | `queue` | Messages acked since the queue was created |
| `anvil.leases.active` | gauge | | Worker leases currently registered |
| `anvil.messages.redelivered` | counter | `queue` | Claims of a message delivered before |
| `anvil.claims.reclaimed` | counter | `queue` | Claims returned after a dead or aged-out lease |
| `anvil.gc.deleted` | counter | | Expired acked entries deleted |
| `rpc.server.call.duration` | histogram (s) | `rpc.method`, `rpc.grpc.status_code` | Server-side RPC latency |

Queue gauges and totals are read from in-memory counters at export time and
add no work to the request path.

## License

Apache-2.0. See [LICENSE](LICENSE).
