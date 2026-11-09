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

// The broker's guarantees over a weak network. Each worker reaches a real
// broker through its own fault-injecting TCP proxy, which can delay, jitter,
// throttle, stall, black-hole or reset either direction of the worker's
// connections, while the test checks the broker's state over a direct,
// fault-free connection.

#[cfg(test)]
#[allow(clippy::await_holding_lock)] // SIM_TIME_LOCK is intentionally held across awaits to serialize time-sensitive tests
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::future::Future;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex, MutexGuard};
    use std::time::{Duration, Instant};

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::{mpsc, watch};
    use tokio::task::{AbortHandle, JoinHandle};
    use tokio_stream::wrappers::ReceiverStream;
    use tonic::transport::{Channel, Endpoint};
    use tonic::{Code, Status};

    use crate::server::AnvilBrokerInner;
    use crate::service::proto::anvil_client::AnvilClient;
    use crate::service::proto::*;
    use crate::types::{AnvilConfig, SIM_TIME_LOCK};

    const RECOVERY_INTERVAL: Duration = Duration::from_millis(25);
    /// Never reached within a test, for the timeout a test does not exercise.
    const LONG_TIMEOUT_SECS: f64 = 600.0;
    /// Bounds every wait, so a broker that never settles fails the test
    /// instead of hanging it.
    const WAIT_DEADLINE: Duration = Duration::from_secs(20);
    const POLL_INTERVAL: Duration = Duration::from_millis(10);

    /// The broker's clock is global to the test binary and other tests
    /// simulate it, so every test here holds this lock. A poisoned lock only
    /// means an earlier test failed, which must not fail this one too.
    fn real_clock() -> MutexGuard<'static, ()> {
        SIM_TIME_LOCK.lock().unwrap_or_else(|e| e.into_inner())
    }

    // =========================================================================
    // Fault-injecting TCP proxy
    // =========================================================================

    /// One direction of a proxied connection.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Dir {
        /// Worker to broker: requests and heartbeat pings.
        Up = 0,
        /// Broker to worker: responses and pongs.
        Down = 1,
    }

    /// The faults one direction suffers. Bytes keep their order: a chunk is
    /// never delivered before the one read ahead of it.
    #[derive(Clone, Debug, Default)]
    struct LinkFaults {
        latency: Duration,
        /// Uniform extra delay in `[0, jitter)` per chunk.
        jitter: Duration,
        /// Bandwidth cap; `None` is unlimited.
        bytes_per_sec: Option<u64>,
        /// Holds every byte, keeping the connection open, until cleared:
        /// a stalled link, or a partition TCP rides out by retransmitting.
        stalled: bool,
        /// Discards every byte while set, keeping the connection open: the
        /// peer's writes vanish without an error.
        blackhole: bool,
    }

    struct ProxyShared {
        links: Mutex<[LinkFaults; 2]>,
        /// Bumped on every fault change, waking writers held by a stall.
        generation: watch::Sender<u64>,
        /// Abort handles of the pumps of every connection since the last
        /// reset, for the next one. A connection stays listed until then
        /// even if one of its pumps ended: the other may still hold a socket.
        conns: Mutex<Vec<[AbortHandle; 2]>>,
        /// Bytes delivered per direction, so a test can wait for a request
        /// to have reached the broker.
        delivered: [AtomicU64; 2],
        next_conn: AtomicU64,
        seed: u64,
    }

    impl ProxyShared {
        /// Resets every live connection; bytes still held are lost.
        fn reset(&self) {
            for pumps in self.conns.lock().unwrap().drain(..) {
                for pump in pumps {
                    pump.abort();
                }
            }
        }
    }

    /// A TCP proxy in front of the broker. Faults apply to existing and new
    /// connections alike; each connection draws its jitter from its own RNG,
    /// seeded from the proxy's seed and the connection's index.
    struct FaultProxy {
        addr: SocketAddr,
        shared: Arc<ProxyShared>,
        acceptor: JoinHandle<()>,
    }

    impl FaultProxy {
        async fn start(target: SocketAddr, seed: u64) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let shared = Arc::new(ProxyShared {
                links: Mutex::new(Default::default()),
                generation: watch::channel(0).0,
                conns: Mutex::new(Vec::new()),
                delivered: [AtomicU64::new(0), AtomicU64::new(0)],
                next_conn: AtomicU64::new(0),
                seed,
            });
            let accept_shared = shared.clone();
            let acceptor = tokio::spawn(async move {
                loop {
                    let Ok((client, _)) = listener.accept().await else {
                        continue;
                    };
                    let Ok(broker) = TcpStream::connect(target).await else {
                        continue;
                    };
                    Self::proxy_connection(&accept_shared, client, broker);
                }
            });
            Self {
                addr,
                shared,
                acceptor,
            }
        }

        fn proxy_connection(shared: &Arc<ProxyShared>, client: TcpStream, broker: TcpStream) {
            // A zero linger makes dropping a socket send RST, not FIN, so a
            // reset reaches both ends as a connection reset. The deprecation
            // warns of a drop blocking until unsent data drains, which a zero
            // linger never waits for.
            for socket in [&client, &broker] {
                socket.set_nodelay(true).unwrap();
                #[allow(deprecated)]
                socket.set_linger(Some(Duration::ZERO)).unwrap();
            }
            let index = shared.next_conn.fetch_add(1, Ordering::Relaxed);
            let (client_read, client_write) = client.into_split();
            let (broker_read, broker_write) = broker.into_split();
            let up = tokio::spawn(Self::pump(
                shared.clone(),
                index,
                Dir::Up,
                client_read,
                broker_write,
            ));
            let down = tokio::spawn(Self::pump(
                shared.clone(),
                index,
                Dir::Down,
                broker_read,
                client_write,
            ));
            shared
                .conns
                .lock()
                .unwrap()
                .push([up.abort_handle(), down.abort_handle()]);
        }

        /// Copies one direction of a connection through its faults. The
        /// reader never waits on the writer, so a stalled or throttled link
        /// buffers bytes here the way a network path would.
        async fn pump(
            shared: Arc<ProxyShared>,
            index: u64,
            dir: Dir,
            mut from: tokio::net::tcp::OwnedReadHalf,
            mut to: tokio::net::tcp::OwnedWriteHalf,
        ) {
            let mut rng = StdRng::seed_from_u64(shared.seed ^ (index << 1 | dir as u64));
            let (tx, mut rx) = mpsc::unbounded_channel::<(tokio::time::Instant, Vec<u8>)>();
            let reader = {
                let shared = shared.clone();
                async move {
                    let mut last_due = tokio::time::Instant::now();
                    let mut buf = vec![0u8; 16 * 1024];
                    loop {
                        let n = match from.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => n,
                        };
                        let link = shared.links.lock().unwrap()[dir as usize].clone();
                        let jitter = if link.jitter.is_zero() {
                            Duration::ZERO
                        } else {
                            rng.random_range(Duration::ZERO..link.jitter)
                        };
                        let due =
                            (tokio::time::Instant::now() + link.latency + jitter).max(last_due);
                        last_due = due;
                        if tx.send((due, buf[..n].to_vec())).is_err() {
                            return;
                        }
                    }
                }
            };
            let writer = async {
                let mut generation = shared.generation.subscribe();
                while let Some((due, chunk)) = rx.recv().await {
                    tokio::time::sleep_until(due).await;
                    let link = loop {
                        let link = shared.links.lock().unwrap()[dir as usize].clone();
                        if !link.stalled {
                            break link;
                        }
                        if generation.changed().await.is_err() {
                            return;
                        }
                    };
                    if link.blackhole {
                        continue;
                    }
                    if to.write_all(&chunk).await.is_err() {
                        return;
                    }
                    shared.delivered[dir as usize].fetch_add(chunk.len() as u64, Ordering::Relaxed);
                    if let Some(rate) = link.bytes_per_sec {
                        let secs = chunk.len() as f64 / rate as f64;
                        tokio::time::sleep(Duration::from_secs_f64(secs)).await;
                    }
                }
                // The source closed: pass the half-close on.
                let _ = to.shutdown().await;
            };
            tokio::join!(reader, writer);
        }

        /// Changes the faults of one direction.
        fn set(&self, dir: Dir, change: impl FnOnce(&mut LinkFaults)) {
            change(&mut self.shared.links.lock().unwrap()[dir as usize]);
            self.shared.generation.send_modify(|g| *g += 1);
        }

        fn set_both(&self, change: impl Fn(&mut LinkFaults)) {
            self.set(Dir::Up, &change);
            self.set(Dir::Down, &change);
        }

        /// Stalls both directions of every connection, existing and new.
        fn partition(&self) {
            self.set_both(|l| l.stalled = true);
        }

        /// Clears every fault; held bytes flow again.
        fn heal(&self) {
            self.set_both(|l| *l = LinkFaults::default());
        }

        /// Resets every live connection; bytes still held are lost. New
        /// connections are accepted as usual.
        fn reset(&self) {
            self.shared.reset();
        }

        fn delivered(&self, dir: Dir) -> u64 {
            self.shared.delivered[dir as usize].load(Ordering::Relaxed)
        }

        fn url(&self) -> String {
            format!("http://{}", self.addr)
        }
    }

    impl Drop for FaultProxy {
        fn drop(&mut self) {
            self.acceptor.abort();
            self.reset();
        }
    }

    // =========================================================================
    // Broker, worker and checks
    // =========================================================================

    struct Broker {
        inner: AnvilBrokerInner,
        addr: SocketAddr,
        /// A fault-free connection for setup and for checking state.
        admin: AnvilClient<Channel>,
    }

    impl Broker {
        async fn start(lease_timeout_secs: f64, claim_age_timeout_secs: f64) -> Self {
            let config = AnvilConfig {
                host: "127.0.0.1".to_string(),
                db_path: "memory://".to_string(),
                lease_timeout_secs,
                claim_age_timeout_secs,
                recovery_interval_secs: RECOVERY_INTERVAL.as_secs_f64(),
                ..AnvilConfig::default()
            };
            let mut inner = AnvilBrokerInner::new(config).await.unwrap();
            let port = inner.start().await.unwrap();
            let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
            let admin = AnvilClient::connect(format!("http://{addr}"))
                .await
                .unwrap();
            Self { inner, addr, admin }
        }

        async fn create_queue(&self, queue: &str) {
            self.admin
                .clone()
                .create_queue(CreateQueueRequest {
                    queue: queue.to_string(),
                    max_pending: 0,
                })
                .await
                .unwrap();
        }

        async fn push(&self, queue: &str, payloads: Vec<Vec<u8>>) -> Vec<String> {
            self.admin
                .clone()
                .push(PushRequest {
                    queue: queue.to_string(),
                    payloads,
                    metadata: HashMap::new(),
                })
                .await
                .unwrap()
                .into_inner()
                .msg_ids
        }

        async fn stats(&self, queue: &str) -> QueueStats {
            self.admin
                .clone()
                .get_stats(GetStatsRequest {
                    queue: queue.to_string(),
                })
                .await
                .unwrap()
                .into_inner()
                .queues
                .remove(queue)
                .unwrap()
        }

        /// Waits until the stats of `queue` satisfy `done`, returning them
        /// and how long the wait took.
        async fn wait_for_stats(
            &self,
            queue: &str,
            done: impl Fn(&QueueStats) -> bool,
        ) -> (QueueStats, Duration) {
            let start = Instant::now();
            tokio::time::timeout(WAIT_DEADLINE, async {
                loop {
                    let s = self.stats(queue).await;
                    if done(&s) {
                        return (s, start.elapsed());
                    }
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("[{queue}] stats did not settle within {WAIT_DEADLINE:?}"))
        }

        fn lease_last_seen(&self, lease_id: &str) -> Option<Instant> {
            self.inner.state.lease_snapshot().get(lease_id).copied()
        }
    }

    /// Opens a channel to `url` that reconnects on its own after a reset,
    /// the way a worker's long-lived channel does.
    fn channel(url: &str) -> AnvilClient<Channel> {
        let endpoint = Endpoint::from_shared(url.to_string())
            .unwrap()
            .connect_timeout(Duration::from_secs(2));
        AnvilClient::new(endpoint.connect_lazy())
    }

    /// A worker's heartbeat, modelled on the shipped client's
    /// `heartbeat_loop`: one `HeartbeatStream` pinged every `interval`; when
    /// the stream fails it reconnects after `reconnect_delay`, sending the
    /// lease it holds, and adopts whatever lease the broker's pong names.
    struct Heartbeat {
        lease: watch::Receiver<String>,
        /// Streams opened, including the first.
        streams: Arc<AtomicU64>,
        task: JoinHandle<()>,
    }

    impl Heartbeat {
        async fn start(
            client: AnvilClient<Channel>,
            worker_id: &str,
            interval: Duration,
            reconnect_delay: Duration,
        ) -> Self {
            let (lease_tx, mut lease) = watch::channel(String::new());
            let streams = Arc::new(AtomicU64::new(0));
            let task = {
                let streams = streams.clone();
                let worker_id = worker_id.to_string();
                tokio::spawn(async move {
                    loop {
                        streams.fetch_add(1, Ordering::SeqCst);
                        let _ =
                            Self::run_stream(client.clone(), &worker_id, interval, &lease_tx).await;
                        tokio::time::sleep(reconnect_delay).await;
                    }
                })
            };
            tokio::time::timeout(WAIT_DEADLINE, lease.wait_for(|l| !l.is_empty()))
                .await
                .expect("no lease within the deadline")
                .unwrap();
            Self {
                lease,
                streams,
                task,
            }
        }

        async fn run_stream(
            mut client: AnvilClient<Channel>,
            worker_id: &str,
            interval: Duration,
            lease_tx: &watch::Sender<String>,
        ) -> Result<(), Status> {
            let (tx, rx) = mpsc::channel::<HeartbeatPing>(4);
            let pinger = {
                let worker_id = worker_id.to_string();
                let lease = lease_tx.subscribe();
                tokio::spawn(async move {
                    loop {
                        let ping = HeartbeatPing {
                            worker_id: worker_id.clone(),
                            lease_id: lease.borrow().clone(),
                            timestamp: 0,
                        };
                        if tx.send(ping).await.is_err() {
                            return;
                        }
                        tokio::time::sleep(interval).await;
                    }
                })
            };
            // Ends the pinger however this stream ends, including when the
            // worker is killed mid-stream: tonic keeps sending the request
            // stream for as long as it yields pings.
            let _pinger = AbortOnDrop(pinger);
            let mut pongs = client
                .heartbeat_stream(ReceiverStream::new(rx))
                .await?
                .into_inner();
            while let Some(pong) = pongs.message().await? {
                lease_tx.send_replace(pong.lease_id);
            }
            Ok(())
        }

        fn lease_id(&self) -> String {
            self.lease.borrow().clone()
        }

        fn streams(&self) -> u64 {
            self.streams.load(Ordering::SeqCst)
        }

        /// Stops pinging at once, the way a worker that dies does; bytes
        /// already handed to the network are still delivered.
        fn kill(&self) {
            self.task.abort();
        }
    }

    struct AbortOnDrop(JoinHandle<()>);

    impl Drop for AbortOnDrop {
        fn drop(&mut self) {
            self.0.abort();
        }
    }

    impl Drop for Heartbeat {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    fn claim_request(queue: &str, worker_id: &str, lease_id: &str, batch: i32) -> ClaimRequest {
        ClaimRequest {
            source: Some(claim_request::Source::Queue(queue.to_string())),
            worker_id: worker_id.to_string(),
            lease_id: lease_id.to_string(),
            batch_size: batch,
            timeout_ms: 0,
        }
    }

    fn complete_request(
        queue: &str,
        claims: &[&ClaimMessage],
        worker_id: &str,
        lease_id: &str,
        action: complete_request::Action,
    ) -> CompleteRequest {
        CompleteRequest {
            upstream_queue: queue.to_string(),
            msg_ids: claims.iter().map(|c| c.msg_id.clone()).collect(),
            claim_tokens: claims.iter().map(|c| c.claim_token.clone()).collect(),
            worker_id: worker_id.to_string(),
            lease_id: lease_id.to_string(),
            action: Some(action),
            state: None,
        }
    }

    fn ack_action() -> complete_request::Action {
        complete_request::Action::Ack(AckAction {})
    }

    /// A request whose response may never come: the call gives up after
    /// `deadline`, which also tells the broker to cancel it (`grpc-timeout`).
    fn with_deadline<T>(message: T, deadline: Duration) -> tonic::Request<T> {
        let mut request = tonic::Request::new(message);
        request.set_timeout(deadline);
        request
    }

    async fn wait_until(what: &str, mut done: impl FnMut() -> bool) {
        tokio::time::timeout(WAIT_DEADLINE, async {
            while !done() {
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{what}: not within {WAIT_DEADLINE:?}"));
    }

    async fn claim_one(
        client: &AnvilClient<Channel>,
        queue: &str,
        worker_id: &str,
        lease_id: &str,
    ) -> ClaimMessage {
        let mut claimed = client
            .clone()
            .claim(claim_request(queue, worker_id, lease_id, 1))
            .await
            .unwrap()
            .into_inner()
            .messages;
        assert_eq!(claimed.len(), 1, "[{queue}] nothing to claim");
        claimed.pop().unwrap()
    }

    async fn ack(
        client: &AnvilClient<Channel>,
        queue: &str,
        claim: &ClaimMessage,
        worker_id: &str,
        lease_id: &str,
    ) -> Result<CompleteResponse, Status> {
        client
            .clone()
            .complete(complete_request(
                queue,
                &[claim],
                worker_id,
                lease_id,
                ack_action(),
            ))
            .await
            .map(|r| r.into_inner())
    }

    /// Whether a call failed in the network rather than in the broker, so
    /// its outcome is unknown and a worker retries it.
    fn is_transport_failure(status: &Status) -> bool {
        matches!(
            status.code(),
            Code::Unavailable | Code::Cancelled | Code::DeadlineExceeded | Code::Unknown
        )
    }

    /// Repeats `call` while it fails in the network, the way a worker
    /// retries over a channel that is reconnecting.
    async fn retry_transport<T, F: Future<Output = Result<T, Status>>>(
        mut call: impl FnMut() -> F,
    ) -> Result<T, Status> {
        let deadline = Instant::now() + WAIT_DEADLINE;
        loop {
            match call().await {
                Err(status) if is_transport_failure(&status) && Instant::now() < deadline => {
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
                result => return result,
            }
        }
    }

    fn counts(s: &QueueStats) -> (i64, i64, i64, i64) {
        (
            s.pending_count,
            s.claimed_count,
            s.total_pushed,
            s.total_acked,
        )
    }

    // =========================================================================
    // Heartbeats and leases
    // =========================================================================

    /// Goal: a lease survives heartbeats that are slow, jittery, throttled
    /// and bunched up, as long as no gap between two arriving pings reaches
    /// `lease_timeout_secs`. Method: send a worker's pings and RPCs through
    /// a link with latency, jitter and a bandwidth cap that also stalls its
    /// upstream for 60% of the lease timeout every cycle; hold one claim for
    /// several lease timeouts, then check it was never reclaimed and acks.
    #[tokio::test]
    async fn test_slow_heartbeats_within_timeout_keep_lease() {
        let _clock = real_clock();
        let lease_timeout = Duration::from_millis(1000);
        let broker = Broker::start(lease_timeout.as_secs_f64(), LONG_TIMEOUT_SECS).await;
        let queue = "slow";
        broker.create_queue(queue).await;
        broker.push(queue, vec![b"work".to_vec()]).await;
        let proxy = FaultProxy::start(broker.addr, 11).await;
        proxy.set_both(|l| {
            l.latency = Duration::from_millis(80);
            l.jitter = Duration::from_millis(120);
            l.bytes_per_sec = Some(64 * 1024);
        });
        let client = channel(&proxy.url());
        let heartbeat = Heartbeat::start(
            client.clone(),
            "worker",
            Duration::from_millis(100),
            Duration::from_millis(50),
        )
        .await;
        let lease_id = heartbeat.lease_id();
        let claimed = claim_one(&client, queue, "worker", &lease_id).await;

        for _ in 0..4 {
            proxy.set(Dir::Up, |l| l.stalled = true);
            tokio::time::sleep(lease_timeout * 6 / 10).await;
            proxy.set(Dir::Up, |l| l.stalled = false);
            tokio::time::sleep(lease_timeout * 4 / 10).await;
            let s = broker.stats(queue).await;
            assert_eq!(
                (s.pending_count, s.claimed_count),
                (0, 1),
                "a lease whose pings were late but within the timeout lost its claim"
            );
        }
        assert_eq!(heartbeat.streams(), 1, "the heartbeat stream was dropped");
        assert_eq!(heartbeat.lease_id(), lease_id);
        ack(&client, queue, &claimed, "worker", &lease_id)
            .await
            .unwrap();
        assert_eq!(counts(&broker.stats(queue).await), (0, 0, 1, 1));
    }

    /// Goal: pings a dead worker sent before it died keep its lease alive
    /// only for as long as the network held them, never longer. Method: a
    /// worker pinging every 100 ms claims a message; its upstream stalls,
    /// it dies while its pings sit in the stalled link, and the link heals,
    /// delivering them all at once. The claim must be reclaimed one lease
    /// timeout after that delivery at the latest, and not before the lease
    /// last seen at the delivery has expired.
    #[tokio::test]
    async fn test_buffered_pings_of_dead_worker_extend_lease_only_by_their_delay() {
        let _clock = real_clock();
        let lease_timeout = Duration::from_millis(800);
        let broker = Broker::start(lease_timeout.as_secs_f64(), LONG_TIMEOUT_SECS).await;
        let queue = "dead";
        broker.create_queue(queue).await;
        broker.push(queue, vec![b"work".to_vec()]).await;
        let proxy = FaultProxy::start(broker.addr, 12).await;
        let client = channel(&proxy.url());
        let heartbeat = Heartbeat::start(
            client.clone(),
            "dead",
            Duration::from_millis(100),
            Duration::from_millis(50),
        )
        .await;
        let lease_id = heartbeat.lease_id();
        claim_one(&client, queue, "dead", &lease_id).await;

        proxy.set(Dir::Up, |l| l.stalled = true);
        let stalled_bytes = proxy.delivered(Dir::Up);
        tokio::time::sleep(Duration::from_millis(300)).await;
        heartbeat.kill();
        let stall = Duration::from_millis(500);
        tokio::time::sleep(stall - Duration::from_millis(300)).await;
        let seen_before_heal = broker.lease_last_seen(&lease_id).unwrap();
        proxy.heal();
        let healed = Instant::now();
        wait_until("held pings delivered", || {
            proxy.delivered(Dir::Up) > stalled_bytes
        })
        .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        let seen_after_heal = broker.lease_last_seen(&lease_id).unwrap();
        assert!(
            seen_after_heal > seen_before_heal,
            "the held pings were never delivered"
        );

        let (_, reclaimed_after) = broker
            .wait_for_stats(queue, |s| (s.pending_count, s.claimed_count) == (1, 0))
            .await;
        let reclaimed_after = reclaimed_after + Duration::from_millis(50);
        let slack = Duration::from_millis(500);
        assert!(
            reclaimed_after <= lease_timeout + slack,
            "the lease of a dead worker outlived its last delivered ping by {reclaimed_after:?}"
        );
        // The pings delivered at the heal reset the lease, so the claim may
        // not go back before a full lease timeout after them.
        assert!(
            healed.elapsed() + RECOVERY_INTERVAL >= lease_timeout,
            "reclaimed {:?} after the heal, before the lease timeout",
            healed.elapsed()
        );
    }

    /// Goal: a worker whose heartbeat connection is reset keeps its lease
    /// when it reconnects within `lease_timeout_secs`, so its claims stay
    /// its own. Method: a worker pinging every 100 ms, like the shipped
    /// client, claims a message; one connection reset makes it reopen its
    /// `HeartbeatStream` within 50 ms, naming its lease in the pings. It
    /// then pings without a gap for three lease timeouts and acks with the
    /// lease it holds.
    #[tokio::test]
    async fn test_heartbeat_reconnect_keeps_lease() {
        let _clock = real_clock();
        let lease_timeout = Duration::from_millis(800);
        let broker = Broker::start(lease_timeout.as_secs_f64(), LONG_TIMEOUT_SECS).await;
        let queue = "blip";
        broker.create_queue(queue).await;
        broker.push(queue, vec![b"work".to_vec()]).await;
        let proxy = FaultProxy::start(broker.addr, 13).await;
        let client = channel(&proxy.url());
        let heartbeat = Heartbeat::start(
            client.clone(),
            "worker",
            Duration::from_millis(100),
            Duration::from_millis(50),
        )
        .await;
        let lease_before = heartbeat.lease_id();
        let claimed = claim_one(&client, queue, "worker", &lease_before).await;

        proxy.reset();
        wait_until("heartbeat reconnected", || heartbeat.streams() >= 2).await;
        tokio::time::sleep(lease_timeout * 3).await;

        let s = broker.stats(queue).await;
        assert_eq!(
            (s.pending_count, s.claimed_count),
            (0, 1),
            "the claim of a worker that kept pinging was reclaimed"
        );
        let lease_after = heartbeat.lease_id();
        assert_eq!(lease_after, lease_before, "the reconnect changed the lease");
        ack(&client, queue, &claimed, "worker", &lease_after)
            .await
            .unwrap();
    }

    /// Goal: a heartbeat link that stalls for less than `lease_timeout_secs`
    /// keeps the lease, as the contract promises, whatever the stall's
    /// length. Method: with a 60 s lease timeout, stall a worker's upstream
    /// for 31 s, heal it, and check the worker still holds its lease and its
    /// claim. The stall outlasts the broker's HTTP/2 keepalive (10 s + 20 s
    /// without a ping ack), which drops the connection, so the worker keeps
    /// its lease only because a reconnect within the lease timeout does.
    #[tokio::test]
    #[ignore = "slow: stalls a heartbeat for 31 s while holding the global clock lock; run with --ignored"]
    async fn test_heartbeat_stall_within_lease_timeout_keeps_lease() {
        let _clock = real_clock();
        let broker = Broker::start(60.0, LONG_TIMEOUT_SECS).await;
        let queue = "stall";
        broker.create_queue(queue).await;
        broker.push(queue, vec![b"work".to_vec()]).await;
        let proxy = FaultProxy::start(broker.addr, 14).await;
        let client = channel(&proxy.url());
        let heartbeat = Heartbeat::start(
            client.clone(),
            "worker",
            Duration::from_secs(1),
            Duration::from_millis(50),
        )
        .await;
        let lease_before = heartbeat.lease_id();
        let claimed = claim_one(&client, queue, "worker", &lease_before).await;

        proxy.set(Dir::Up, |l| l.stalled = true);
        tokio::time::sleep(Duration::from_secs(31)).await;
        proxy.heal();
        tokio::time::sleep(Duration::from_secs(3)).await;

        assert_eq!(
            heartbeat.lease_id(),
            lease_before,
            "a 31 s stall under a 60 s lease timeout changed the lease"
        );
        ack(&client, queue, &claimed, "worker", &lease_before)
            .await
            .unwrap();
    }

    /// Goal: the broker forgets a lease once it has expired, so workers that
    /// come and go do not grow its lease table, its recovery tick's copy of
    /// it or the `anvil.leases.active` gauge without bound, while a worker
    /// that keeps reconnecting keeps its one lease. Method: reset a live
    /// worker's heartbeat connection 20 times while 20 other workers each
    /// open a lease and die; once the dead leases are several lease timeouts
    /// old, only the live worker's lease may remain.
    #[tokio::test]
    async fn test_expired_leases_are_forgotten() {
        let _clock = real_clock();
        let lease_timeout = Duration::from_millis(300);
        let broker = Broker::start(lease_timeout.as_secs_f64(), LONG_TIMEOUT_SECS).await;
        let proxy = FaultProxy::start(broker.addr, 15).await;
        let interval = Duration::from_millis(50);
        let reconnect_delay = Duration::from_millis(10);
        let live =
            Heartbeat::start(channel(&proxy.url()), "worker", interval, reconnect_delay).await;
        let lease_id = live.lease_id();
        let direct = channel(&format!("http://{}", broker.addr));
        for reconnects in 1..=20 {
            proxy.reset();
            wait_until("heartbeat reconnected", || live.streams() > reconnects).await;
            Heartbeat::start(
                direct.clone(),
                &format!("dead-{reconnects}"),
                interval,
                reconnect_delay,
            )
            .await
            .kill();
        }
        tokio::time::sleep(lease_timeout * 4).await;

        assert_eq!(live.lease_id(), lease_id, "a reconnect changed the lease");
        let leases = broker.inner.state.lease_snapshot();
        assert_eq!(
            leases.keys().collect::<Vec<_>>(),
            [&lease_id],
            "{} leases registered for one live worker",
            leases.len()
        );
    }

    // =========================================================================
    // Lost and delayed responses
    // =========================================================================

    /// Goal: a claim whose response is lost leaves the message claimed by a
    /// worker that never saw it; the broker hands it out again once
    /// `claim_age_timeout_secs` has passed, not before, with
    /// `delivery_count == 2`. Method: under a live lease, send a claim
    /// through a link whose downstream is stalled, give up on it, reset the
    /// link so the response is lost, then time the redelivery and claim the
    /// message again through the healed link.
    #[tokio::test]
    async fn test_lost_claim_response_redelivers_at_claim_age() {
        let _clock = real_clock();
        let claim_age = Duration::from_millis(800);
        let broker = Broker::start(LONG_TIMEOUT_SECS, claim_age.as_secs_f64()).await;
        let queue = "lost-claim";
        broker.create_queue(queue).await;
        broker.push(queue, vec![b"work".to_vec()]).await;
        let heartbeat = Heartbeat::start(
            broker.admin.clone(),
            "worker",
            Duration::from_millis(100),
            Duration::from_millis(50),
        )
        .await;
        let lease_id = heartbeat.lease_id();
        let proxy = FaultProxy::start(broker.addr, 21).await;
        let client = channel(&proxy.url());
        // Connect first, so the stall holds the response and not the setup.
        client
            .clone()
            .get_stats(GetStatsRequest {
                queue: queue.to_string(),
            })
            .await
            .unwrap();

        proxy.set(Dir::Down, |l| l.stalled = true);
        let sent = Instant::now();
        let lost = client
            .clone()
            .claim(with_deadline(
                claim_request(queue, "worker", &lease_id, 1),
                Duration::from_millis(300),
            ))
            .await;
        assert!(lost.is_err(), "the stalled claim response arrived");
        let s = broker.stats(queue).await;
        assert_eq!(
            (s.pending_count, s.claimed_count),
            (0, 1),
            "the lost claim did not claim"
        );
        proxy.reset();
        proxy.heal();

        broker
            .wait_for_stats(queue, |s| (s.pending_count, s.claimed_count) == (1, 0))
            .await;
        assert!(
            sent.elapsed() >= claim_age,
            "the lost claim was reclaimed {:?} after it was sent, before the {claim_age:?} claim age",
            sent.elapsed()
        );
        assert!(
            sent.elapsed() <= claim_age + Duration::from_secs(2),
            "the lost claim was reclaimed only {:?} after it was sent",
            sent.elapsed()
        );

        let again = claim_one(&client, queue, "worker", &lease_id).await;
        assert_eq!(again.payload, b"work");
        assert_eq!(again.delivery_count, 2);
        ack(&client, queue, &again, "worker", &lease_id)
            .await
            .unwrap();
        assert_eq!(counts(&broker.stats(queue).await), (0, 0, 1, 1));
    }

    /// Goal: a worker that retries an ack whose response it never got is
    /// told the claim is gone (`FAILED_PRECONDITION`, so it drops the
    /// message) and the message is acked exactly once and never redelivered;
    /// the same holds when the first ack is still held in the network as the
    /// retry lands and arrives after it. Method: (1) ack through a link with
    /// a stalled downstream, give up, reset, retry; (2) ack through a link
    /// with a stalled upstream, give up, retry over another connection, then
    /// heal the first so its held ack reaches the broker late. Check the
    /// totals and that nothing comes back after the claim age.
    #[tokio::test]
    async fn test_retried_ack_after_lost_or_delayed_response_settles_once() {
        let _clock = real_clock();
        let claim_age = Duration::from_millis(1000);
        let broker = Broker::start(LONG_TIMEOUT_SECS, claim_age.as_secs_f64()).await;
        let queue = "retry";
        broker.create_queue(queue).await;
        broker
            .push(
                queue,
                vec![b"lost-response".to_vec(), b"delayed-request".to_vec()],
            )
            .await;
        let heartbeat = Heartbeat::start(
            broker.admin.clone(),
            "worker",
            Duration::from_millis(100),
            Duration::from_millis(50),
        )
        .await;
        let lease_id = heartbeat.lease_id();
        let proxy = FaultProxy::start(broker.addr, 22).await;
        let client = channel(&proxy.url());
        let first = claim_one(&client, queue, "worker", &lease_id).await;
        let give_up = Duration::from_millis(300);

        proxy.set(Dir::Down, |l| l.stalled = true);
        let lost = client
            .clone()
            .complete(with_deadline(
                complete_request(queue, &[&first], "worker", &lease_id, ack_action()),
                give_up,
            ))
            .await;
        assert!(lost.is_err(), "the stalled ack response arrived");
        proxy.reset();
        proxy.heal();
        let retry = retry_transport(|| ack(&client, queue, &first, "worker", &lease_id)).await;
        assert_eq!(
            retry.unwrap_err().code(),
            Code::FailedPrecondition,
            "a retry of an ack that landed was not refused as a lost claim"
        );
        assert_eq!(broker.stats(queue).await.total_acked, 1);

        let second = claim_one(&client, queue, "worker", &lease_id).await;
        proxy.set(Dir::Up, |l| l.stalled = true);
        let held = client
            .clone()
            .complete(with_deadline(
                complete_request(queue, &[&second], "worker", &lease_id, ack_action()),
                give_up,
            ))
            .await;
        assert!(held.is_err(), "the stalled ack request got through");
        assert_eq!(broker.stats(queue).await.total_acked, 1);
        ack(&broker.admin, queue, &second, "worker", &lease_id)
            .await
            .unwrap();
        proxy.heal();
        tokio::time::sleep(claim_age * 3 / 2).await;

        assert_eq!(counts(&broker.stats(queue).await), (0, 0, 2, 2));
        let more = broker
            .admin
            .clone()
            .claim(claim_request(queue, "worker", &lease_id, 10))
            .await
            .unwrap()
            .into_inner();
        assert!(more.messages.is_empty(), "an acked message was redelivered");
    }

    /// Goal: a worker cut off long enough to lose its lease cannot ack the
    /// message after the network heals, even though its held ack and pings
    /// all arrive then; the new holder's ack stands alone. Method: worker A
    /// claims through a link that then partitions; A acks into the
    /// partition; its lease expires and worker B, on a direct connection,
    /// claims the redelivered message. The heal delivers A's ack, which must
    /// fail with `FAILED_PRECONDITION`, and B's ack must succeed.
    #[tokio::test]
    async fn test_stale_ack_held_by_partition_is_fenced_after_heal() {
        let _clock = real_clock();
        let lease_timeout = Duration::from_millis(600);
        let broker = Broker::start(lease_timeout.as_secs_f64(), LONG_TIMEOUT_SECS).await;
        let queue = "partition";
        broker.create_queue(queue).await;
        broker.push(queue, vec![b"work".to_vec()]).await;
        let proxy = FaultProxy::start(broker.addr, 23).await;
        let a_client = channel(&proxy.url());
        let a_heartbeat = Heartbeat::start(
            a_client.clone(),
            "a",
            Duration::from_millis(100),
            Duration::from_millis(50),
        )
        .await;
        let a_lease = a_heartbeat.lease_id();
        let a_claim = claim_one(&a_client, queue, "a", &a_lease).await;

        proxy.partition();
        let held_ack = {
            let (client, claim, lease) = (a_client.clone(), a_claim.clone(), a_lease.clone());
            tokio::spawn(async move { ack(&client, queue, &claim, "a", &lease).await })
        };
        broker
            .wait_for_stats(queue, |s| (s.pending_count, s.claimed_count) == (1, 0))
            .await;
        let b_heartbeat = Heartbeat::start(
            broker.admin.clone(),
            "b",
            Duration::from_millis(100),
            Duration::from_millis(50),
        )
        .await;
        let b_lease = b_heartbeat.lease_id();
        let b_claim = claim_one(&broker.admin, queue, "b", &b_lease).await;
        assert_eq!(b_claim.msg_id, a_claim.msg_id);
        assert_eq!(b_claim.delivery_count, 2);

        proxy.heal();
        let a_result = tokio::time::timeout(WAIT_DEADLINE, held_ack)
            .await
            .expect("the held ack never completed")
            .unwrap();
        assert_eq!(
            a_result.unwrap_err().code(),
            Code::FailedPrecondition,
            "the partitioned worker acked a message redelivered to another"
        );
        assert_eq!(counts(&broker.stats(queue).await), (0, 1, 1, 0));
        ack(&broker.admin, queue, &b_claim, "b", &b_lease)
            .await
            .unwrap();
        assert_eq!(counts(&broker.stats(queue).await), (0, 0, 1, 1));
    }

    // =========================================================================
    // Completion signals and atomic Completes
    // =========================================================================

    /// Goal: `upstream_drained` is never reported while a message is still
    /// unacked, even while claim responses are delayed or lost, and every
    /// worker still sees it in the end. Method: four workers drain a
    /// finished queue through links with jittered downstream latency that a
    /// seeded chaos task resets every 100-250 ms, so claim responses get
    /// lost and wait out the claim age. A worker that is told the queue is
    /// drained checks the broker's totals before it exits.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_drain_signal_holds_under_lost_responses() {
        let _clock = real_clock();
        const MESSAGES: i64 = 60;
        let broker = Arc::new(Broker::start(LONG_TIMEOUT_SECS, 0.6).await);
        let queue = "drain";
        broker.create_queue(queue).await;
        broker
            .push(
                queue,
                (0..MESSAGES)
                    .map(|i| format!("m{i}").into_bytes())
                    .collect(),
            )
            .await;
        broker
            .admin
            .clone()
            .mark_queue_finished(MarkQueueFinishedRequest {
                queue: queue.to_string(),
            })
            .await
            .unwrap();

        let mut proxies = Vec::new();
        let mut workers = Vec::new();
        for w in 0..4u64 {
            let proxy = FaultProxy::start(broker.addr, 100 + w).await;
            proxy.set(Dir::Down, |l| {
                l.latency = Duration::from_millis(5);
                l.jitter = Duration::from_millis(20);
            });
            let client = channel(&proxy.url());
            let worker_id = format!("w{w}");
            let heartbeat = Heartbeat::start(
                broker.admin.clone(),
                &worker_id,
                Duration::from_millis(100),
                Duration::from_millis(50),
            )
            .await;
            let broker = broker.clone();
            workers.push(tokio::spawn(async move {
                let lease_id = heartbeat.lease_id();
                loop {
                    let request = with_deadline(
                        claim_request(queue, &worker_id, &lease_id, 2),
                        Duration::from_millis(300),
                    );
                    let response = match client.clone().claim(request).await {
                        Ok(response) => response.into_inner(),
                        Err(status) if is_transport_failure(&status) => continue,
                        Err(status) => panic!("claim failed: {status:?}"),
                    };
                    if response.upstream_drained {
                        let s = broker.stats(queue).await;
                        assert_eq!(
                            counts(&s),
                            (0, 0, MESSAGES, MESSAGES),
                            "{worker_id} was told the queue drained with work left"
                        );
                        return;
                    }
                    for message in &response.messages {
                        let acked =
                            retry_transport(|| ack(&client, queue, message, &worker_id, &lease_id))
                                .await;
                        match acked {
                            Ok(_) => {}
                            Err(status) if status.code() == Code::FailedPrecondition => {}
                            Err(status) => panic!("ack failed: {status:?}"),
                        }
                    }
                    if response.messages.is_empty() {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }
            }));
            proxies.push(proxy);
        }

        let chaos = {
            let mut rng = StdRng::seed_from_u64(7);
            let proxies: Vec<_> = proxies.iter().map(|p| p.shared.clone()).collect();
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_millis(rng.random_range(100..250))).await;
                    let target = &proxies[rng.random_range(0..proxies.len())];
                    target.reset();
                }
            })
        };
        for worker in workers {
            tokio::time::timeout(WAIT_DEADLINE, worker)
                .await
                .expect("a worker never saw the queue drain")
                .unwrap();
        }
        chaos.abort();
        assert_eq!(
            counts(&broker.stats(queue).await),
            (0, 0, MESSAGES, MESSAGES)
        );
    }

    /// One way a Complete can die in the network.
    #[derive(Clone, Copy, Debug)]
    enum CompleteFault {
        /// The worker gives up after this long, and the broker cancels the
        /// call when it has not answered by then (`grpc-timeout`).
        Deadline(Duration),
        /// Every connection is reset this long after the call is sent.
        ResetAfter(Duration),
        /// The response is held and then lost to a reset.
        LostResponse,
    }

    /// Goal: a forwarding Complete (scatter to a queue group, or forward to
    /// one queue, each with a state put) stays atomic when its RPC dies at
    /// any point: the upstream ack, the downstream messages and the state
    /// land together or not at all, a retry with the same token succeeds
    /// exactly when the first attempt did not land, and every input ends up
    /// with exactly one set of outputs. Method: for 30 inputs, send the
    /// Complete through a proxy under a seeded fault (a broker-side deadline
    /// of up to 3 ms, a reset up to 3 ms in, or a lost response), let any
    /// write still in flight settle, read every effect back over a direct
    /// connection, retry, and finally drain every downstream queue.
    #[tokio::test]
    async fn test_forward_and_scatter_stay_atomic_when_the_rpc_dies() {
        let _clock = real_clock();
        const INPUTS: usize = 30;
        const PARTITIONS: i32 = 3;
        let broker = Broker::start(LONG_TIMEOUT_SECS, LONG_TIMEOUT_SECS).await;
        let (upstream, group, forward, namespace) = ("src", "out", "fwd", "done");
        broker.create_queue(upstream).await;
        broker.create_queue(forward).await;
        let partitions = broker
            .admin
            .clone()
            .create_queue_group(CreateQueueGroupRequest {
                group_name: group.to_string(),
                num_partitions: PARTITIONS,
                max_pending_per_partition: 0,
            })
            .await
            .unwrap()
            .into_inner()
            .queue_names;
        let mut downstream = partitions.clone();
        downstream.push(forward.to_string());
        broker
            .push(
                upstream,
                (0..INPUTS).map(|i| format!("in{i}").into_bytes()).collect(),
            )
            .await;
        let heartbeat = Heartbeat::start(
            broker.admin.clone(),
            "worker",
            Duration::from_millis(100),
            Duration::from_millis(50),
        )
        .await;
        let lease_id = heartbeat.lease_id();
        let proxy = FaultProxy::start(broker.addr, 31).await;
        let client = channel(&proxy.url());
        let mut rng = StdRng::seed_from_u64(31);

        let downstream_pushed = async || {
            let mut total = 0;
            for queue in &downstream {
                total += broker.stats(queue).await.total_pushed;
            }
            total
        };
        let mut expected_outputs: HashMap<String, usize> = HashMap::new();
        let mut landed_first = 0;
        for i in 0..INPUTS {
            let input = claim_one(&broker.admin, upstream, "worker", &lease_id).await;
            // Each output names its input, so the final drain can match them.
            let output = |k: usize| format!("{}:{k}", input.msg_id).into_bytes();
            let (action, outputs) = if i % 2 == 0 {
                let partitions = (0..PARTITIONS)
                    .map(|pid| PartitionPayload {
                        partition_id: pid,
                        payloads: vec![output(pid as usize)],
                    })
                    .collect();
                let scatter = complete_request::Action::Scatter(ScatterAction {
                    group_name: group.to_string(),
                    partitions,
                });
                (scatter, PARTITIONS as usize)
            } else {
                let forward = complete_request::Action::Forward(ForwardAction {
                    downstream_queue: forward.to_string(),
                    payloads: vec![output(0), output(1)],
                });
                (forward, 2)
            };
            expected_outputs.insert(input.msg_id.clone(), outputs);
            let request = CompleteRequest {
                state: Some(StateUpdate {
                    namespace: namespace.to_string(),
                    puts: HashMap::from([(input.msg_id.clone(), b"done".to_vec())]),
                    deletes: vec![],
                }),
                ..complete_request(upstream, &[&input], "worker", &lease_id, action)
            };
            let pushed_before = downstream_pushed().await;
            let acked_before = broker.stats(upstream).await.total_acked;

            let fault = match rng.random_range(0..3) {
                0 => CompleteFault::Deadline(Duration::from_micros(rng.random_range(0..3000))),
                1 => CompleteFault::ResetAfter(Duration::from_micros(rng.random_range(0..3000))),
                _ => CompleteFault::LostResponse,
            };
            let first = match fault {
                CompleteFault::Deadline(deadline) => {
                    client
                        .clone()
                        .complete(with_deadline(request.clone(), deadline))
                        .await
                }
                CompleteFault::ResetAfter(delay) => {
                    let shared = proxy.shared.clone();
                    let resetter = tokio::spawn(async move {
                        tokio::time::sleep(delay).await;
                        shared.reset();
                    });
                    let result = client.clone().complete(request.clone()).await;
                    resetter.await.unwrap();
                    result
                }
                CompleteFault::LostResponse => {
                    proxy.set(Dir::Down, |l| l.stalled = true);
                    let result = client
                        .clone()
                        .complete(with_deadline(request.clone(), Duration::from_millis(300)))
                        .await;
                    proxy.reset();
                    proxy.heal();
                    result
                }
            };
            if let Err(status) = &first {
                assert!(
                    is_transport_failure(status),
                    "[{fault:?}] the Complete failed in the broker: {status:?}"
                );
            }
            // A cancelled call may have handed its batch to the database.
            tokio::time::sleep(Duration::from_millis(100)).await;

            let still_claimed = broker
                .inner
                .storage
                .scan_claimed(Some(upstream))
                .await
                .unwrap()
                .iter()
                .any(|(_, msg_id, _)| *msg_id == input.msg_id);
            let landed = !still_claimed;
            let state = broker
                .admin
                .clone()
                .state_get(StateGetRequest {
                    namespace: namespace.to_string(),
                    keys: vec![input.msg_id.clone()],
                })
                .await
                .unwrap()
                .into_inner()
                .values;
            let pushed = downstream_pushed().await - pushed_before;
            let acked = broker.stats(upstream).await.total_acked - acked_before;
            let effects = (state.contains_key(&input.msg_id), pushed, acked);
            let all_or_nothing = if landed {
                (true, outputs as i64, 1)
            } else {
                (false, 0, 0)
            };
            assert_eq!(
                effects,
                all_or_nothing,
                "[{fault:?}] a Complete that {} left (state, downstream, acked) = {effects:?}",
                if landed {
                    "acked upstream"
                } else {
                    "did not ack upstream"
                }
            );
            assert!(
                first.is_err() || landed,
                "[{fault:?}] a Complete reported success but did not land"
            );

            let retry = retry_transport(|| {
                let (mut client, request) = (client.clone(), request.clone());
                async move { client.complete(request).await }
            })
            .await;
            if landed {
                landed_first += 1;
                assert_eq!(
                    retry.unwrap_err().code(),
                    Code::FailedPrecondition,
                    "[{fault:?}] the retry of a landed Complete was not refused"
                );
            } else {
                retry.unwrap_or_else(|status| {
                    panic!(
                        "[{fault:?}] the retry of a Complete that never landed failed: {status:?}"
                    )
                });
            }
        }
        assert!(landed_first > 0, "no faulted Complete ever landed");

        let mut outputs_seen: HashMap<String, usize> = HashMap::new();
        for queue in &downstream {
            let drained = broker
                .admin
                .clone()
                .claim(claim_request(queue, "reader", &lease_id, 1000))
                .await
                .unwrap()
                .into_inner()
                .messages;
            for message in drained {
                let payload = String::from_utf8(message.payload).unwrap();
                let (input, _) = payload.rsplit_once(':').unwrap();
                *outputs_seen.entry(input.to_string()).or_default() += 1;
            }
        }
        assert_eq!(
            outputs_seen, expected_outputs,
            "an input lost or doubled its outputs"
        );
        assert_eq!(
            counts(&broker.stats(upstream).await),
            (0, 0, INPUTS as i64, INPUTS as i64)
        );
        let mut queues = downstream.clone();
        queues.push(upstream.to_string());
        crate::dst::tests::check_invariants(&broker.inner.storage, &queues).await;
    }

    // =========================================================================
    // Soak
    // =========================================================================

    /// What every soak worker saw, for the checks at the end.
    #[derive(Default)]
    struct SoakLog {
        /// Every (msg_id, delivery_count) a claim response delivered.
        deliveries: Vec<(String, u32)>,
        /// Every msg_id whose ack the broker confirmed.
        acked: Vec<String>,
    }

    /// Changes one worker's link at random: a fault from the menu, or, half
    /// the time, a heal, so that a stall rarely outlives the lease.
    fn random_fault(proxy: &FaultProxy, rng: &mut StdRng) {
        match rng.random_range(0..12) {
            0 => proxy.set_both(|l| {
                l.latency = Duration::from_millis(20);
                l.jitter = Duration::from_millis(60);
            }),
            1 => proxy.set_both(|l| l.bytes_per_sec = Some(8 * 1024)),
            2 => proxy.set(Dir::Up, |l| l.stalled = true),
            3 => proxy.set(Dir::Down, |l| l.stalled = true),
            4 => proxy.partition(),
            5 => proxy.reset(),
            _ => proxy.heal(),
        }
    }

    /// Runs `workers` workers, each heartbeating and calling the broker
    /// through its own proxy, until they have drained `messages` messages
    /// from a finished queue, while a seeded chaos task changes one link's
    /// faults every 50-200 ms. Each worker renews half its claims and acks
    /// with a state put, retrying calls lost in the network and dropping
    /// claims the broker says it lost. Then checks the guarantees: every
    /// message acked exactly once, no claim delivered twice, the drain seen
    /// by every worker only once nothing was left, every acked message's
    /// state put stored, and no claim or task left behind.
    async fn run_soak(seed: u64, workers: u64, messages: i64) {
        let broker = Arc::new(Broker::start(1.0, 1.0).await);
        let alive_tasks = || {
            tokio::runtime::Handle::current()
                .metrics()
                .num_alive_tasks()
        };
        let queue = "soak";
        broker.create_queue(queue).await;
        for chunk in (0..messages).collect::<Vec<_>>().chunks(100) {
            let payloads = chunk.iter().map(|i| format!("m{i}").into_bytes()).collect();
            broker.push(queue, payloads).await;
        }
        broker
            .admin
            .clone()
            .mark_queue_finished(MarkQueueFinishedRequest {
                queue: queue.to_string(),
            })
            .await
            .unwrap();

        // Counted once the broker serves the admin connection and nothing
        // else, which its server tasks need a moment after the setup to do.
        tokio::time::sleep(Duration::from_millis(200)).await;
        let tasks_before = alive_tasks();

        let log = Arc::new(Mutex::new(SoakLog::default()));
        let mut proxies = Vec::new();
        let mut handles = Vec::new();
        for w in 0..workers {
            let proxy = FaultProxy::start(broker.addr, seed ^ (w << 8)).await;
            let client = channel(&proxy.url());
            let worker_id = format!("w{w}");
            let heartbeat = Heartbeat::start(
                client.clone(),
                &worker_id,
                Duration::from_millis(100),
                Duration::from_millis(50),
            )
            .await;
            let (broker, log) = (broker.clone(), log.clone());
            let mut rng = StdRng::seed_from_u64(seed ^ w);
            handles.push(tokio::spawn(async move {
                loop {
                    // The lease may change under a reconnect; a claim made
                    // under the old one is then lost, which the broker
                    // reports as FAILED_PRECONDITION.
                    let lease_id = heartbeat.lease_id();
                    let request = with_deadline(
                        claim_request(queue, &worker_id, &lease_id, rng.random_range(1..4)),
                        Duration::from_millis(500),
                    );
                    let response = match client.clone().claim(request).await {
                        Ok(response) => response.into_inner(),
                        Err(status) if is_transport_failure(&status) => continue,
                        Err(status) => panic!("claim failed: {status:?}"),
                    };
                    if response.upstream_drained {
                        let s = broker.stats(queue).await;
                        assert_eq!(
                            (s.pending_count, s.claimed_count, s.total_acked),
                            (0, 0, messages),
                            "{worker_id} was told the queue drained with work left"
                        );
                        return;
                    }
                    if response.messages.is_empty() {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    for message in &response.messages {
                        log.lock()
                            .unwrap()
                            .deliveries
                            .push((message.msg_id.clone(), message.delivery_count));
                        tokio::time::sleep(Duration::from_millis(rng.random_range(0..20))).await;
                        if rng.random_bool(0.5) {
                            let renewed = retry_transport(|| {
                                let mut client = client.clone();
                                let request = RenewClaimsRequest {
                                    queue: queue.to_string(),
                                    msg_ids: vec![message.msg_id.clone()],
                                    claim_tokens: vec![message.claim_token.clone()],
                                    worker_id: worker_id.clone(),
                                    lease_id: lease_id.clone(),
                                };
                                async move { client.renew_claims(request).await }
                            })
                            .await;
                            match renewed {
                                Ok(_) => {}
                                Err(status) if status.code() == Code::FailedPrecondition => {
                                    continue
                                }
                                Err(status) => panic!("renew failed: {status:?}"),
                            }
                        }
                        let request = CompleteRequest {
                            state: Some(StateUpdate {
                                namespace: "done".to_string(),
                                puts: HashMap::from([(message.msg_id.clone(), b"1".to_vec())]),
                                deletes: vec![],
                            }),
                            ..complete_request(
                                queue,
                                &[message],
                                &worker_id,
                                &lease_id,
                                ack_action(),
                            )
                        };
                        let acked = retry_transport(|| {
                            let (mut client, request) = (client.clone(), request.clone());
                            async move {
                                client
                                    .complete(with_deadline(request, Duration::from_millis(500)))
                                    .await
                            }
                        })
                        .await;
                        match acked {
                            Ok(_) => log.lock().unwrap().acked.push(message.msg_id.clone()),
                            Err(status) if status.code() == Code::FailedPrecondition => {}
                            Err(status) => panic!("ack failed: {status:?}"),
                        }
                    }
                }
            }));
            proxies.push(proxy);
        }

        let mut rng = StdRng::seed_from_u64(seed);
        let soak = async {
            for handle in &mut handles {
                loop {
                    tokio::select! {
                        result = &mut *handle => {
                            result.unwrap();
                            break;
                        }
                        _ = tokio::time::sleep(Duration::from_millis(rng.random_range(50..200))) => {
                            random_fault(&proxies[rng.random_range(0..proxies.len())], &mut rng);
                        }
                    }
                }
            }
        };
        let soak_deadline = WAIT_DEADLINE * 3;
        tokio::time::timeout(soak_deadline, soak)
            .await
            .unwrap_or_else(|_| {
                panic!("the workers did not drain the queue within {soak_deadline:?}")
            });

        assert_eq!(
            counts(&broker.stats(queue).await),
            (0, 0, messages, messages)
        );
        let log = log.lock().unwrap();
        let acked: HashSet<&String> = log.acked.iter().collect();
        assert_eq!(acked.len(), log.acked.len(), "a message was acked twice");
        let deliveries: HashSet<&(String, u32)> = log.deliveries.iter().collect();
        assert_eq!(
            deliveries.len(),
            log.deliveries.len(),
            "one claim of a message was delivered twice"
        );
        let delivered: HashSet<&String> = log.deliveries.iter().map(|(id, _)| id).collect();
        assert!(delivered.len() as i64 <= messages);
        let state = broker
            .admin
            .clone()
            .state_get(StateGetRequest {
                namespace: "done".to_string(),
                keys: log.deliveries.iter().map(|(id, _)| id.clone()).collect(),
            })
            .await
            .unwrap()
            .into_inner()
            .values;
        assert_eq!(
            state.len(),
            delivered.len(),
            "an acked message lost its state put"
        );
        crate::dst::tests::check_invariants(&broker.inner.storage, &[queue.to_string()]).await;
        assert!(broker
            .inner
            .storage
            .scan_claimed(None)
            .await
            .unwrap()
            .is_empty());

        // Once every worker and connection is gone, the broker's tasks for
        // them must end: none may wait on a connection that no longer exists.
        drop(handles);
        drop(proxies);
        let ended = tokio::time::timeout(WAIT_DEADLINE, async {
            while alive_tasks() > tasks_before {
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        })
        .await;
        assert!(
            ended.is_ok(),
            "{} tasks alive {WAIT_DEADLINE:?} after every worker left, {tasks_before} before they came",
            alive_tasks()
        );
    }

    /// Goal: the guarantees hold together under a mix of every fault the
    /// proxy has. Method: a short `run_soak`; see it for the checks.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_short_soak_under_random_faults() {
        let _clock = real_clock();
        run_soak(0x5eed, 6, 120).await;
    }

    /// Goal: the same as `test_short_soak_under_random_faults`, for as long
    /// as `CHAOS_SOAK_SECS` says (default 300), round after round with a new
    /// seed each, so slow leaks in the broker's tasks show up. Method:
    /// repeated `run_soak` rounds on one process.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "soak"]
    async fn test_long_soak_under_random_faults() {
        let _clock = real_clock();
        let secs: u64 = std::env::var("CHAOS_SOAK_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300);
        let start = Instant::now();
        let mut round = 0;
        while start.elapsed() < Duration::from_secs(secs) {
            run_soak(0x5eed + round, 12, 600).await;
            round += 1;
        }
    }
}
