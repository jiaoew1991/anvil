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

//! Anvil gRPC Benchmark Suite
//!
//! Standardized benchmarks for measuring throughput and latency of the
//! Anvil gRPC protocol with varying client concurrency.
//!
//! Run with: `cargo test --release bench_ -- --nocapture --ignored`
//!
//! Benchmarks:
//!   - bench_push:      N clients push M messages concurrently
//!   - bench_claim_ack: N clients claim+ack from pre-filled queue
//!   - bench_mixed:     producers push while consumers claim+ack simultaneously
//!
//! Scales tested: 1, 10, 100, 1000 concurrent clients

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use tokio::sync::mpsc;

    use crate::server::AnvilBrokerInner;
    use crate::service::proto;
    use crate::storage::AnvilStorage;
    use crate::types::AnvilConfig;

    use proto::anvil_client::AnvilClient;

    // ========================================================================
    // Benchmark infrastructure
    // ========================================================================

    #[derive(Clone)]
    struct BenchConfig {
        num_clients: usize,
        messages_per_client: usize,
        batch_size: usize,
        payload_bytes: usize,
    }

    #[allow(dead_code)]
    struct BenchResult {
        operation: String,
        num_clients: usize,
        total_messages: u64,
        duration: Duration,
        throughput: f64, // msgs/sec
        latencies_us: Vec<u64>,
    }

    impl BenchResult {
        fn percentile(&self, p: f64) -> u64 {
            if self.latencies_us.is_empty() {
                return 0;
            }
            let idx = ((p / 100.0) * self.latencies_us.len() as f64) as usize;
            let idx = idx.min(self.latencies_us.len() - 1);
            self.latencies_us[idx]
        }

        fn print(&self) {
            println!(
                "  {:20} | {:>6} clients | {:>8} msgs | {:>8.1} msgs/s | p50={:>6}µs  p95={:>6}µs  p99={:>6}µs  max={:>6}µs",
                self.operation,
                self.num_clients,
                self.total_messages,
                self.throughput,
                self.percentile(50.0),
                self.percentile(95.0),
                self.percentile(99.0),
                self.latencies_us.last().copied().unwrap_or(0),
            );
        }
    }

    /// Start a broker on a random port, return (port, storage)
    async fn start_broker() -> (u16, Arc<AnvilStorage>) {
        let config = AnvilConfig {
            db_path: "memory://".to_string(),
            host: "127.0.0.1".to_string(),
            port: 0,
            claim_timeout_secs: 300.0, // long timeout for benchmarks
            recovery_interval_secs: 600.0,
            max_queue_depth: 0,
            acked_retention_secs: 60.0,
            gc_interval_secs: 600.0,
        };

        let storage = Arc::new(AnvilStorage::new(&config.db_path).await.unwrap());
        let mut broker = AnvilBrokerInner::new_with_storage(config, storage.clone())
            .await
            .unwrap();
        let port = broker.start().await.unwrap();

        // Leak the broker to keep it alive for the test
        std::mem::forget(broker);

        (port, storage)
    }

    /// Create a connected tonic client with heartbeat, return (client, lease_id)
    async fn connect_client(
        port: u16,
        worker_id: &str,
    ) -> (AnvilClient<tonic::transport::Channel>, String) {
        let channel = tonic::transport::Endpoint::from_shared(format!("http://127.0.0.1:{}", port))
            .unwrap()
            .connect_timeout(Duration::from_secs(10))
            .connect()
            .await
            .unwrap();

        let mut client = AnvilClient::new(channel);

        // Start heartbeat to get lease_id
        let (tx, rx) = mpsc::channel::<proto::HeartbeatPing>(4);
        let wid = worker_id.to_string();

        tx.send(proto::HeartbeatPing {
            worker_id: wid.clone(),
            lease_id: String::new(),
            timestamp: 0,
        })
        .await
        .unwrap();

        let response = client
            .heartbeat_stream(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .unwrap();
        let mut stream = response.into_inner();

        // Get first pong with lease_id
        let pong = stream.message().await.unwrap().unwrap();
        let lease_id = pong.lease_id.clone();

        // Keep heartbeat alive in background
        let ping_lease = lease_id.clone();
        let ping_wid = wid;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(5)).await;
                let ping = proto::HeartbeatPing {
                    worker_id: ping_wid.clone(),
                    lease_id: ping_lease.clone(),
                    timestamp: 0,
                };
                if tx.send(ping).await.is_err() {
                    break;
                }
                // Drain pong
                match stream.message().await {
                    Ok(Some(_)) => {}
                    _ => break,
                }
            }
        });

        (client, lease_id)
    }

    // ========================================================================
    // Benchmark: Push
    // ========================================================================

    async fn bench_push(port: u16, config: &BenchConfig) -> BenchResult {
        let queue = format!("bench_push_{}", config.num_clients);

        // Create queue
        let (mut admin, admin_lease) = connect_client(port, "admin").await;
        admin
            .create_queue(proto::CreateQueueRequest {
                queue: queue.clone(),
            })
            .await
            .unwrap();
        drop((admin, admin_lease));

        let payload = vec![0u8; config.payload_bytes];
        let total_sent = Arc::new(AtomicU64::new(0));
        let all_latencies: Arc<tokio::sync::Mutex<Vec<u64>>> =
            Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let start = Instant::now();

        let mut handles = Vec::new();
        for i in 0..config.num_clients {
            let q = queue.clone();
            let p = payload.clone();
            let sent = total_sent.clone();
            let lats = all_latencies.clone();
            let msgs = config.messages_per_client;
            let batch = config.batch_size;

            handles.push(tokio::spawn(async move {
                let (mut client, _lease) = connect_client(port, &format!("push-{}", i)).await;
                let mut local_lats = Vec::with_capacity(msgs);

                let mut remaining = msgs;
                while remaining > 0 {
                    let n = remaining.min(batch);
                    let payloads: Vec<Vec<u8>> = (0..n).map(|_| p.clone()).collect();

                    let t = Instant::now();
                    client
                        .push(proto::PushRequest {
                            queue: q.clone(),
                            payloads,
                            metadata: Default::default(),
                        })
                        .await
                        .unwrap();
                    local_lats.push(t.elapsed().as_micros() as u64);

                    sent.fetch_add(n as u64, Ordering::Relaxed);
                    remaining -= n;
                }

                lats.lock().await.extend(local_lats);
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let duration = start.elapsed();
        let total = total_sent.load(Ordering::Relaxed);
        let mut latencies = Arc::try_unwrap(all_latencies).unwrap().into_inner();
        latencies.sort();

        BenchResult {
            operation: "push".to_string(),
            num_clients: config.num_clients,
            total_messages: total,
            duration,
            throughput: total as f64 / duration.as_secs_f64(),
            latencies_us: latencies,
        }
    }

    // ========================================================================
    // Benchmark: Claim + Ack (using unified Complete RPC)
    // ========================================================================

    async fn bench_claim_ack(port: u16, config: &BenchConfig) -> BenchResult {
        let queue = format!("bench_claim_ack_{}", config.num_clients);
        let total_msgs = config.num_clients * config.messages_per_client;

        // Setup: create queue and push messages
        let (mut admin, _) = connect_client(port, "admin-ca").await;
        admin
            .create_queue(proto::CreateQueueRequest {
                queue: queue.clone(),
            })
            .await
            .unwrap();

        // Push all messages first
        let payload = vec![0u8; config.payload_bytes];
        let batch = 100;
        let mut pushed = 0;
        while pushed < total_msgs {
            let n = (total_msgs - pushed).min(batch);
            let payloads: Vec<Vec<u8>> = (0..n).map(|_| payload.clone()).collect();
            admin
                .push(proto::PushRequest {
                    queue: queue.clone(),
                    payloads,
                    metadata: Default::default(),
                })
                .await
                .unwrap();
            pushed += n;
        }
        drop(admin);

        let total_acked = Arc::new(AtomicU64::new(0));
        let all_latencies: Arc<tokio::sync::Mutex<Vec<u64>>> =
            Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let start = Instant::now();

        let mut handles = Vec::new();
        for i in 0..config.num_clients {
            let q = queue.clone();
            let acked = total_acked.clone();
            let lats = all_latencies.clone();
            let target = config.messages_per_client;
            let batch_sz = config.batch_size;

            handles.push(tokio::spawn(async move {
                let (mut client, lease) = connect_client(port, &format!("ca-{}", i)).await;
                let wid = format!("ca-{}", i);
                let mut local_lats = Vec::with_capacity(target);
                let mut done = 0;

                while done < target {
                    let t = Instant::now();

                    // Claim
                    let claim_resp = client
                        .claim(proto::ClaimRequest {
                            source: Some(proto::claim_request::Source::Queue(q.clone())),
                            worker_id: wid.clone(),
                            lease_id: lease.clone(),
                            batch_size: batch_sz as i32,
                            timeout_ms: 1000,
                        })
                        .await
                        .unwrap()
                        .into_inner();

                    if claim_resp.messages.is_empty() {
                        break;
                    }

                    let msg_ids: Vec<String> = claim_resp
                        .messages
                        .iter()
                        .map(|m| m.msg_id.clone())
                        .collect();
                    let tokens: Vec<String> = claim_resp
                        .messages
                        .iter()
                        .map(|m| m.claim_token.clone())
                        .collect();
                    let n = msg_ids.len();

                    // Ack via Complete RPC
                    client
                        .complete(proto::CompleteRequest {
                            upstream_queue: q.clone(),
                            msg_ids,
                            claim_tokens: tokens,
                            worker_id: wid.clone(),
                            lease_id: lease.clone(),
                            action: Some(proto::complete_request::Action::Ack(proto::AckAction {})),
                            state: None,
                        })
                        .await
                        .unwrap();

                    local_lats.push(t.elapsed().as_micros() as u64);
                    done += n;
                    acked.fetch_add(n as u64, Ordering::Relaxed);
                }

                lats.lock().await.extend(local_lats);
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let duration = start.elapsed();
        let total = total_acked.load(Ordering::Relaxed);
        let mut latencies = Arc::try_unwrap(all_latencies).unwrap().into_inner();
        latencies.sort();

        BenchResult {
            operation: "claim+ack".to_string(),
            num_clients: config.num_clients,
            total_messages: total,
            duration,
            throughput: total as f64 / duration.as_secs_f64(),
            latencies_us: latencies,
        }
    }

    // ========================================================================
    // Benchmark: Mixed (producers + consumers simultaneously)
    // ========================================================================

    async fn bench_mixed(port: u16, config: &BenchConfig) -> BenchResult {
        let queue = format!("bench_mixed_{}", config.num_clients);
        let producers = config.num_clients / 2;
        let consumers = config.num_clients - producers;

        let (mut admin, _) = connect_client(port, "admin-mx").await;
        admin
            .create_queue(proto::CreateQueueRequest {
                queue: queue.clone(),
            })
            .await
            .unwrap();
        drop(admin);

        let payload = vec![0u8; config.payload_bytes];
        let total_processed = Arc::new(AtomicU64::new(0));
        let total_pushed = Arc::new(AtomicU64::new(0));
        let producers_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let all_latencies: Arc<tokio::sync::Mutex<Vec<u64>>> =
            Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let start = Instant::now();

        let mut handles = Vec::new();

        // Producers
        for i in 0..producers {
            let q = queue.clone();
            let p = payload.clone();
            let pushed = total_pushed.clone();
            let msgs = config.messages_per_client;
            let batch = config.batch_size;

            handles.push(tokio::spawn(async move {
                let (mut client, _) = connect_client(port, &format!("prod-{}", i)).await;
                let mut remaining = msgs;
                while remaining > 0 {
                    let n = remaining.min(batch);
                    let payloads: Vec<Vec<u8>> = (0..n).map(|_| p.clone()).collect();
                    client
                        .push(proto::PushRequest {
                            queue: q.clone(),
                            payloads,
                            metadata: Default::default(),
                        })
                        .await
                        .unwrap();
                    pushed.fetch_add(n as u64, Ordering::Relaxed);
                    remaining -= n;
                }
            }));
        }

        // Consumers
        for i in 0..consumers {
            let q = queue.clone();
            let processed = total_processed.clone();
            let pushed_ref = total_pushed.clone();
            let done_flag = producers_done.clone();
            let lats = all_latencies.clone();
            let batch_sz = config.batch_size;
            let expected = (producers * config.messages_per_client) / consumers;

            handles.push(tokio::spawn(async move {
                let (mut client, lease) = connect_client(port, &format!("cons-{}", i)).await;
                let wid = format!("cons-{}", i);
                let mut local_lats = Vec::new();
                let mut got = 0usize;

                loop {
                    let t = Instant::now();
                    let claim_resp = client
                        .claim(proto::ClaimRequest {
                            source: Some(proto::claim_request::Source::Queue(q.clone())),
                            worker_id: wid.clone(),
                            lease_id: lease.clone(),
                            batch_size: batch_sz as i32,
                            timeout_ms: 200,
                        })
                        .await
                        .unwrap()
                        .into_inner();

                    if claim_resp.messages.is_empty() {
                        // Check if producers are done and we've consumed enough
                        if done_flag.load(Ordering::Relaxed)
                            && pushed_ref.load(Ordering::Relaxed)
                                == processed.load(Ordering::Relaxed)
                        {
                            break;
                        }
                        if got >= expected {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }

                    let msg_ids: Vec<String> = claim_resp
                        .messages
                        .iter()
                        .map(|m| m.msg_id.clone())
                        .collect();
                    let tokens: Vec<String> = claim_resp
                        .messages
                        .iter()
                        .map(|m| m.claim_token.clone())
                        .collect();
                    let n = msg_ids.len();

                    client
                        .complete(proto::CompleteRequest {
                            upstream_queue: q.clone(),
                            msg_ids,
                            claim_tokens: tokens,
                            worker_id: wid.clone(),
                            lease_id: lease.clone(),
                            action: Some(proto::complete_request::Action::Ack(proto::AckAction {})),
                            state: None,
                        })
                        .await
                        .unwrap();

                    local_lats.push(t.elapsed().as_micros() as u64);
                    got += n;
                    processed.fetch_add(n as u64, Ordering::Relaxed);
                }

                lats.lock().await.extend(local_lats);
            }));
        }

        // Wait for producers first, then signal consumers
        for h in handles.drain(..producers) {
            h.await.unwrap();
        }
        producers_done.store(true, Ordering::Relaxed);

        // Wait for consumers
        for h in handles {
            h.await.unwrap();
        }

        let duration = start.elapsed();
        let total = total_processed.load(Ordering::Relaxed);
        let mut latencies = Arc::try_unwrap(all_latencies).unwrap().into_inner();
        latencies.sort();

        BenchResult {
            operation: "mixed".to_string(),
            num_clients: config.num_clients,
            total_messages: total,
            duration,
            throughput: total as f64 / duration.as_secs_f64(),
            latencies_us: latencies,
        }
    }

    // ========================================================================
    // Benchmark runner
    // ========================================================================

    fn print_header() {
        println!();
        println!("╔══════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
        println!("║                                    Anvil gRPC Benchmark Results                                        ║");
        println!("╠══════════════════════════════════════════════════════════════════════════════════════════════════════════╣");
        println!(
            "  {:20} | {:>13} | {:>9} | {:>13} | {:>43}",
            "Operation", "Clients", "Messages", "Throughput", "Latency (per batch RPC)"
        );
        println!(
            "  {:─>20}─┼─{:─>13}─┼─{:─>9}─┼─{:─>13}─┼─{:─>43}",
            "", "", "", "", ""
        );
    }

    fn print_footer() {
        println!("╚══════════════════════════════════════════════════════════════════════════════════════════════════════════╝");
        println!();
    }

    /// Full benchmark suite: push, claim+ack, mixed across 1/10/100/1000 clients
    #[tokio::test]
    #[ignore] // Run explicitly: cargo test --release bench_full_suite -- --nocapture --ignored
    async fn bench_full_suite() {
        let _ = tracing_subscriber::fmt().try_init();

        let (port, _storage) = start_broker().await;
        // Let broker stabilize
        tokio::time::sleep(Duration::from_millis(500)).await;

        let scales = [1, 10, 100, 1000];
        let payload_bytes = 256;

        print_header();

        for &n in &scales {
            // Scale messages inversely with clients to keep total work ~constant
            let msgs_per_client = (10000 / n).max(10);
            let batch_size = 10.min(msgs_per_client);

            let config = BenchConfig {
                num_clients: n,
                messages_per_client: msgs_per_client,
                batch_size,
                payload_bytes,
            };

            // Push benchmark
            let r = bench_push(port, &config).await;
            r.print();

            // Claim+Ack benchmark
            let r = bench_claim_ack(port, &config).await;
            r.print();

            // Mixed benchmark (skip for 1 client — needs at least 2)
            if n > 1 {
                let r = bench_mixed(port, &config).await;
                r.print();
            }

            println!(
                "  {:─>20}─┼─{:─>13}─┼─{:─>9}─┼─{:─>13}─┼─{:─>43}",
                "", "", "", "", ""
            );
        }

        print_footer();
    }

    /// Quick 1000-client stress test
    #[tokio::test]
    #[ignore]
    async fn bench_1000_clients_stress() {
        let _ = tracing_subscriber::fmt().try_init();

        let (port, _storage) = start_broker().await;
        tokio::time::sleep(Duration::from_millis(500)).await;

        let config = BenchConfig {
            num_clients: 1000,
            messages_per_client: 100,
            batch_size: 10,
            payload_bytes: 256,
        };

        println!();
        println!("=== 1000-Client Stress Test ===");
        println!(
            "  Payload: {} bytes, Batch: {}, Messages/client: {}",
            config.payload_bytes, config.batch_size, config.messages_per_client
        );
        println!();

        print_header();

        let r = bench_push(port, &config).await;
        r.print();

        let r = bench_claim_ack(port, &config).await;
        r.print();

        let r = bench_mixed(port, &config).await;
        r.print();

        print_footer();

        // Assertions: basic sanity
        assert!(r.throughput > 0.0, "Throughput must be positive");
        assert!(r.total_messages > 0, "Must process some messages");
    }
}
