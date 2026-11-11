# Anvil

Anvil is a work queue with one producer side and many competing consumers,
served over gRPC and stored in [SlateDB](https://slatedb.io) (`memory://`,
`file://` or `s3://`). It ships as a Rust server with Python bindings (PyO3).

It was extracted from [nurion](https://github.com/lumalabs/nurion), where it
runs as the per-job broker embedded in the Ray driver.

## Queues and queue groups

A **queue** is for work any worker may take: workers compete for messages
(`claim`). A **queue group** is for work keyed so that one consumer must see
every message of a key, such as a group-by: the producer scatters payloads
over the group's partition queues (`ack_and_scatter`) and each worker claims
only the partitions assigned to it (`claim_from_group`). See
[docs/semantics.md](docs/semantics.md) section 2.5.

## Guarantees

- **Fenced completion.** Every claim carries a `claim_token` and the worker's
  `lease_id`. A worker whose message was redelivered cannot ack it: the token
  rotates on redelivery and a stale ack is rejected with
  `FAILED_PRECONDITION` (`ClaimLostError` in Python), distinct from the
  `INTERNAL` of a broker failure: drop the message rather than retry.
- **Atomic complete.** `Complete` acks upstream messages, pushes downstream
  payloads (`ack_and_forward`, `ack_and_scatter`) and writes key-value state in
  one storage batch, so a crash never leaves a message acked without its output.
- **Heartbeat leases.** Workers keep a `HeartbeatStream` open. Claims held by a
  lease that misses heartbeats for `lease_timeout_secs` return to pending; a
  claim neither taken nor renewed within `claim_age_timeout_secs` under a live
  lease is treated as stuck. A task that may run longer renews its claim from
  its processing loop (`renew`), so it is reclaimed only once it stops making
  progress. A worker whose stream or connection drops keeps its lease, and its
  claims, if it reconnects within `lease_timeout_secs`; later, it is given a
  new lease. Both timeouts run on the broker's monotonic clock, so a wall-clock
  step neither revives a dead lease nor kills a live one.
- **Broker restarts reclaim every claim.** Leases live in the broker's memory.
  After a broker restart, every claim in flight is returned to pending and
  redelivered, even to a worker that reconnects at once: expect acks of
  claims made before the restart to fail with `FAILED_PRECONDITION`. The
  restart gives those deliveries back, so it neither raises a message's
  `delivery_count` nor dead-letters it.
- **Drain signal.** `claim` reports `upstream_drained` once a queue is marked
  finished and empty; dead letters count as settled, and `claim` reports how
  many are left beside the signal.
- **Delivery count.** Every claimed message carries `delivery_count`: 1 on
  first delivery, plus one for each nack or reclaimed lease of a worker.
- **Dead letters.** A nack or reclaim that settles the last delivery a queue
  allows (`max_deliveries` on `CreateQueue`, else the broker's
  `default_max_deliveries`, 20; 0 is unlimited) moves the message to the
  queue's dead letters instead, with its body, the cause and the last nack's
  reason. A nack with `dead_letter=True` does so at once. Dead letters are
  listed, redriven to pending with a fresh delivery count, or purged.
  `get_stats` reports a queue's own `max_deliveries` and the
  `effective_max_deliveries` in force. See
  the runbook in [docs/semantics.md](docs/semantics.md) section 5.6 for
  recovering from a downstream outage.

See [docs/semantics.md](docs/semantics.md) for the full consistency and
recovery model and the error codes of every RPC.

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
    claim_age_timeout_secs=60.0,
))
broker.start()  # binds asynchronously; poll get_port() until it is set

client = AnvilRustClient(broker.get_broker_url(), "worker-1")
client.start()
client.create_queue("work")
client.push("work", b"payload")

claimed = client.claim("work", batch_size=1)  # .messages, .drained, .dead_count
for message in claimed.messages:
    for chunk in work_in_chunks(message.payload):
        process(chunk)
        # Each chunk restarts the claim_age_timeout_secs window.
        client.renew("work", [message.msg_id], [message.claim_token])
    client.ack("work", [message.msg_id], claim_tokens=[message.claim_token])

# Messages that failed every delivery wait as dead letters: redrive them,
# a page at a time (or purge_dead_letters).
start_after = None
while True:
    letters, start_after = client.list_dead_letters(
        "work", start_after=start_after, limit=1000, ids_only=True
    )
    if letters:
        client.redrive_dead_letters("work", [letter.msg_id for letter in letters])
    if start_after is None:
        break
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
| `anvil.queue.dead` | gauge | `queue` | Dead letters waiting for a redrive or a purge |
| `anvil.queue.pushed` | counter | `queue` | Messages pushed since the queue was created |
| `anvil.queue.acked` | counter | `queue` | Messages acked since the queue was created |
| `anvil.leases.active` | gauge | | Worker leases currently registered |
| `anvil.messages.redelivered` | counter | `queue` | Claims of a message delivered before |
| `anvil.claims.reclaimed` | counter | `queue` | Claims taken back after a dead or aged-out lease, returned or dead-lettered |
| `anvil.messages.dead_lettered` | counter | `queue`, `cause` | Messages dead-lettered; `cause` is `nack_limit`, `reclaim_limit` or `rejected` |
| `anvil.gc.deleted` | counter | | Expired acked entries deleted |
| `rpc.server.call.duration` | histogram (s) | `rpc.method`, `rpc.grpc.status_code` | Server-side RPC latency |

Queue gauges and totals are read from in-memory counters at export time and
add no work to the request path.

## Releasing

1. `scripts/bump-version.sh 0.3.0` sets the version in `Cargo.toml`,
   `pyproject.toml` and `Cargo.lock`; merge it through a pull request.
2. Tag the merged commit and push the tag:
   `git tag v0.3.0 origin/main && git push origin v0.3.0`.

The tag starts the Release workflow. It refuses a tag that does not match the
code's version or is not on `main`, runs the tests, benches the code against
the previous release on the same runner (`scripts/bench-compare.sh`), builds
the wheels and publishes a GitHub release with generated notes.

## License

Apache-2.0. See [LICENSE](LICENSE).
