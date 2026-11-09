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

// An object store that misbehaves the way a remote one does over a weak
// network: it answers late, fails requests with transient errors, times out,
// reports a put as failed after the object landed, and fails everything.
// It wraps a real store, so whatever does land stays readable by a later
// broker opened over the same inner store, which is how a test restarts.
//
// SlateDB wraps every store in its own retrying layer, so a transient fault
// reaches the broker as latency, not as an error; the faults here exercise
// that layer and whatever the broker does while it waits.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::stream::{self, BoxStream, StreamExt};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use slatedb::object_store::path::Path;
use slatedb::object_store::{
    CopyOptions, Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The faults injected into one kind of request.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct OpFaults {
    /// Added to every request before it reaches the inner store.
    pub latency: Duration,
    /// Probability that a request fails without reaching the inner store.
    pub fail_rate: f64,
    /// Probability that a request waits out `timeout` and then fails without
    /// reaching the inner store, as a client-side request timeout does.
    pub timeout_rate: f64,
    pub timeout: Duration,
    /// Probability that a put lands in the inner store and is then reported
    /// as failed, as when the response to a successful upload is lost.
    pub land_then_fail_rate: f64,
}

/// The faults of every kind of request.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Faults {
    pub put: OpFaults,
    pub get: OpFaults,
    pub list: OpFaults,
}

/// Requests seen and faults injected, for asserting that a fault fired and
/// for bounding how hard a caller hammers a failing store.
#[derive(Debug, Default)]
pub(crate) struct FaultCounts {
    pub requests: AtomicU64,
    pub failed: AtomicU64,
    pub landed_then_failed: AtomicU64,
}

#[derive(Clone, Copy, PartialEq)]
enum Op {
    Put,
    Get,
    List,
}

/// What a request does once its latency has passed.
enum Fate {
    Pass,
    Fail,
    LandThenFail,
}

struct Shared {
    faults: Mutex<Faults>,
    rng: Mutex<StdRng>,
    counts: FaultCounts,
}

/// Fault-injecting wrapper; clones share their faults and counts.
#[derive(Clone)]
pub(crate) struct FaultStore {
    inner: Arc<dyn ObjectStore>,
    shared: Arc<Shared>,
}

impl FaultStore {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>, seed: u64) -> Self {
        Self {
            inner,
            shared: Arc::new(Shared {
                faults: Mutex::new(Faults::default()),
                rng: Mutex::new(StdRng::seed_from_u64(seed)),
                counts: FaultCounts::default(),
            }),
        }
    }

    pub(crate) fn set_faults(&self, faults: Faults) {
        *self.shared.faults.lock().unwrap() = faults;
    }

    pub(crate) fn counts(&self) -> &FaultCounts {
        &self.shared.counts
    }

    fn faults_of(&self, op: Op) -> OpFaults {
        let faults = self.shared.faults.lock().unwrap();
        match op {
            Op::Put => faults.put,
            Op::Get => faults.get,
            Op::List => faults.list,
        }
    }

    /// Waits out the latency, then draws the request's fate.
    /// A timed-out request has already waited its timeout when this returns.
    async fn admit(&self, op: Op) -> Fate {
        let counts = &self.shared.counts;
        counts.requests.fetch_add(1, Ordering::Relaxed);
        let faults = self.faults_of(op);
        if !faults.latency.is_zero() {
            tokio::time::sleep(faults.latency).await;
        }
        let (fail, time_out, land_then_fail) = {
            let mut rng = self.shared.rng.lock().unwrap();
            (
                rng.random_bool(faults.fail_rate),
                rng.random_bool(faults.timeout_rate),
                op == Op::Put && rng.random_bool(faults.land_then_fail_rate),
            )
        };
        if time_out {
            tokio::time::sleep(faults.timeout).await;
        }
        if fail || time_out {
            counts.failed.fetch_add(1, Ordering::Relaxed);
            Fate::Fail
        } else if land_then_fail {
            counts.landed_then_failed.fetch_add(1, Ordering::Relaxed);
            Fate::LandThenFail
        } else {
            Fate::Pass
        }
    }

    /// A transient error, which SlateDB's retrying layer retries.
    fn injected(what: &str) -> Error {
        Error::Generic {
            store: "FaultStore",
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("injected fault: {what}"),
            )),
        }
    }
}

impl fmt::Display for FaultStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultStore({})", self.inner)
    }
}

impl fmt::Debug for FaultStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FaultStore({:?})", self.inner)
    }
}

// The trait is declared through `async_trait`, which this crate does not
// depend on, so its methods are written out in the boxed form it expands to.
impl ObjectStore for FaultStore {
    fn put_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> BoxFuture<'async_trait, Result<PutResult>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            match self.admit(Op::Put).await {
                Fate::Fail => Err(Self::injected("put")),
                Fate::LandThenFail => {
                    self.inner.put_opts(location, payload, opts).await?;
                    Err(Self::injected("put landed, response lost"))
                }
                Fate::Pass => self.inner.put_opts(location, payload, opts).await,
            }
        })
    }

    fn put_multipart_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
        opts: PutMultipartOptions,
    ) -> BoxFuture<'async_trait, Result<Box<dyn MultipartUpload>>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            match self.admit(Op::Put).await {
                Fate::Pass | Fate::LandThenFail => {
                    self.inner.put_multipart_opts(location, opts).await
                }
                Fate::Fail => Err(Self::injected("put_multipart")),
            }
        })
    }

    fn get_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 Path,
        options: GetOptions,
    ) -> BoxFuture<'async_trait, Result<GetResult>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            match self.admit(Op::Get).await {
                Fate::Pass | Fate::LandThenFail => self.inner.get_opts(location, options).await,
                Fate::Fail => Err(Self::injected("get")),
            }
        })
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let store = self.clone();
        let prefix = prefix.cloned();
        stream::once(async move {
            match store.admit(Op::List).await {
                Fate::Pass | Fate::LandThenFail => store.inner.list(prefix.as_ref()),
                Fate::Fail => stream::once(async { Err(Self::injected("list")) }).boxed(),
            }
        })
        .flatten()
        .boxed()
    }

    fn list_with_delimiter<'life0, 'life1, 'async_trait>(
        &'life0 self,
        prefix: Option<&'life1 Path>,
    ) -> BoxFuture<'async_trait, Result<ListResult>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            match self.admit(Op::List).await {
                Fate::Pass | Fate::LandThenFail => self.inner.list_with_delimiter(prefix).await,
                Fate::Fail => Err(Self::injected("list_with_delimiter")),
            }
        })
    }

    fn copy_opts<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        from: &'life1 Path,
        to: &'life2 Path,
        options: CopyOptions,
    ) -> BoxFuture<'async_trait, Result<()>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            match self.admit(Op::Put).await {
                Fate::Fail => Err(Self::injected("copy")),
                Fate::LandThenFail => {
                    self.inner.copy_opts(from, to, options).await?;
                    Err(Self::injected("copy landed, response lost"))
                }
                Fate::Pass => self.inner.copy_opts(from, to, options).await,
            }
        })
    }
}
