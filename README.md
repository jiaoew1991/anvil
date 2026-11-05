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

## License

Apache-2.0. See [LICENSE](LICENSE).
