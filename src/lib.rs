//! Async binary cache with stale-while-revalidate and per-instance single-flight.
//! Storage tiers retain original deadlines on promotion. See the README for semantics.

use std::{
    collections::HashMap,
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{watch, Mutex};

/// Sendable future used by storage adapters and loaders.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Cloneable error shared with all waiters of a single flight.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error(pub String);
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for Error {}

/// Binary entry with absolute Unix millisecond deadlines.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Entry {
    /// Opaque application payload.
    pub value: Vec<u8>,
    /// End of the fresh period, exclusive.
    pub fresh_until_ms: u64,
    /// End of the stale period, exclusive.
    pub expires_at_ms: u64,
}

/// Storage contract: errors must be returned; expired entries may be omitted.
pub trait Store: Send + Sync + 'static {
    /// Retrieve an entry without changing its deadlines.
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Entry>, Error>>;
    /// Store an entry with its original deadlines.
    fn put<'a>(&'a self, key: &'a str, entry: Entry) -> BoxFuture<'a, Result<(), Error>>;
    /// Remove an entry.
    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), Error>>;
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

/// Bounded, process-local memory storage; evicts the earliest-expiring entry.
pub struct MemoryStore {
    capacity: usize,
    entries: Mutex<HashMap<String, Entry>>,
}
impl MemoryStore {
    /// Create a storage tier. Zero capacity is rejected.
    pub fn new(capacity: usize) -> Result<Self, Error> {
        if capacity == 0 {
            return Err(Error("capacity must be positive".into()));
        }
        Ok(Self {
            capacity,
            entries: Mutex::new(HashMap::new()),
        })
    }
}
impl Store for MemoryStore {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Entry>, Error>> {
        Box::pin(async move {
            let mut entries = self.entries.lock().await;
            if entries
                .get(key)
                .is_some_and(|v| v.expires_at_ms <= now_ms())
            {
                entries.remove(key);
            }
            Ok(entries.get(key).cloned())
        })
    }
    fn put<'a>(&'a self, key: &'a str, entry: Entry) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            let mut entries = self.entries.lock().await;
            entries.retain(|_, v| v.expires_at_ms > now_ms());
            if entry.expires_at_ms <= now_ms() {
                entries.remove(key);
                return Ok(());
            }
            if !entries.contains_key(key) && entries.len() >= self.capacity {
                if let Some(oldest) = entries
                    .iter()
                    .min_by_key(|(_, v)| v.expires_at_ms)
                    .map(|(k, _)| k.clone())
                {
                    entries.remove(&oldest);
                }
            }
            entries.insert(key.into(), entry);
            Ok(())
        })
    }
    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            self.entries.lock().await.remove(key);
            Ok(())
        })
    }
}

/// Loader callback. The owned key and future allow refreshes to outlive callers.
pub type Loader = Arc<dyn Fn(String) -> BoxFuture<'static, Result<Vec<u8>, Error>> + Send + Sync>;
type FlightResult = Result<Vec<u8>, Error>;
type Flights = HashMap<String, watch::Sender<Option<FlightResult>>>;

/// A shared refresh result; dropping it does not cancel the refresh.
#[derive(Clone)]
pub struct Refresh {
    receiver: watch::Receiver<Option<FlightResult>>,
}
impl Refresh {
    /// Await completion, including loader, timeout, panic and storage errors.
    pub async fn wait(mut self) -> FlightResult {
        loop {
            if let Some(result) = self.receiver.borrow().clone() {
                return result;
            }
            if self.receiver.changed().await.is_err() {
                return Err(Error("refresh task stopped".into()));
            }
        }
    }
}

/// Cached response and optional observable background refresh.
pub struct Cached {
    /// Application payload.
    pub value: Vec<u8>,
    /// True when returned during the stale period.
    pub stale: bool,
    /// Present for stale responses; await to observe background refresh failures.
    pub refresh: Option<Refresh>,
}

struct Inner {
    stores: Vec<Arc<dyn Store>>,
    loader: Loader,
    fresh: Duration,
    stale: Duration,
    timeout: Duration,
    flights: Mutex<Flights>,
}
/// Cloneable cache. Tiers are ordered fastest to slowest.
#[derive(Clone)]
pub struct Cache {
    inner: Arc<Inner>,
}
impl Cache {
    /// Construct a cache. Freshness and timeout must be positive; stale may be zero.
    pub fn new(
        stores: Vec<Arc<dyn Store>>,
        fresh: Duration,
        stale: Duration,
        timeout: Duration,
        loader: Loader,
    ) -> Result<Self, Error> {
        if stores.is_empty()
            || fresh.is_zero()
            || timeout.is_zero()
            || fresh.as_millis() == 0
            || timeout.as_millis() == 0
        {
            return Err(Error(
                "stores, millisecond fresh period and timeout are required".into(),
            ));
        }
        if fresh.as_millis() + stale.as_millis() > u64::MAX as u128 {
            return Err(Error("TTL exceeds millisecond range".into()));
        }
        Ok(Self {
            inner: Arc::new(Inner {
                stores,
                loader,
                fresh,
                stale,
                timeout,
                flights: Mutex::new(HashMap::new()),
            }),
        })
    }
    async fn lookup(&self, key: &str) -> Result<Option<Entry>, Error> {
        for (index, store) in self.inner.stores.iter().enumerate() {
            if let Some(entry) = store.get(key).await? {
                if entry.expires_at_ms <= now_ms() {
                    continue;
                }
                if entry.fresh_until_ms > entry.expires_at_ms {
                    return Err(Error("invalid entry deadlines".into()));
                }
                for earlier in &self.inner.stores[..index] {
                    earlier.put(key, entry.clone()).await?;
                }
                // Promotion can await network I/O beyond the stale deadline.
                if entry.expires_at_ms <= now_ms() {
                    continue;
                }
                return Ok(Some(entry));
            }
        }
        Ok(None)
    }
    /// Return fresh data, or stale data while a shared refresh runs, or await a miss.
    pub async fn get(&self, key: &str) -> Result<Cached, Error> {
        if let Some(entry) = self.lookup(key).await? {
            let stale = entry.fresh_until_ms <= now_ms();
            let refresh = if stale {
                Some(self.start(key, false).await)
            } else {
                None
            };
            return Ok(Cached {
                value: entry.value,
                stale,
                refresh,
            });
        }
        let value = self.start(key, false).await.wait().await?;
        Ok(Cached {
            value,
            stale: false,
            refresh: None,
        })
    }
    /// Force a refresh, joining an existing flight for the same key if present.
    pub async fn refresh(&self, key: &str) -> Result<Vec<u8>, Error> {
        self.start(key, true).await.wait().await
    }
    /// Delete all tiers. An already-running loader may repopulate the key.
    pub async fn invalidate(&self, key: &str) -> Result<(), Error> {
        for store in &self.inner.stores {
            store.delete(key).await?;
        }
        Ok(())
    }
    async fn start(&self, key: &str, force: bool) -> Refresh {
        let mut flights = self.inner.flights.lock().await;
        if let Some(sender) = flights.get(key) {
            return Refresh {
                receiver: sender.subscribe(),
            };
        }
        let (sender, receiver) = watch::channel(None);
        flights.insert(key.into(), sender);
        let cache = self.clone();
        let key = key.to_owned();
        tokio::spawn(async move {
            let worker_cache = cache.clone();
            let worker_key = key.clone();
            let worker = tokio::spawn(async move {
                if !force {
                    if let Some(entry) = worker_cache.lookup(&worker_key).await? {
                        if entry.fresh_until_ms > now_ms() {
                            return Ok(entry.value);
                        }
                    }
                }
                let value = (worker_cache.inner.loader)(worker_key.clone()).await?;
                let fresh_until_ms =
                    now_ms().saturating_add(worker_cache.inner.fresh.as_millis() as u64);
                let entry = Entry {
                    value: value.clone(),
                    fresh_until_ms,
                    expires_at_ms: fresh_until_ms
                        .saturating_add(worker_cache.inner.stale.as_millis() as u64),
                };
                // Slowest-first writes avoid publishing an L1-only success.
                for store in worker_cache.inner.stores.iter().rev() {
                    store.put(&worker_key, entry.clone()).await?;
                }
                Ok(value)
            });
            let abort = worker.abort_handle();
            let result = match tokio::time::timeout(cache.inner.timeout, worker).await {
                Ok(Ok(result)) => result,
                Ok(Err(error)) => Err(Error(format!("refresh task failed: {error}"))),
                Err(_) => {
                    abort.abort();
                    Err(Error("refresh timed out".into()))
                }
            };
            let mut flights = cache.inner.flights.lock().await;
            if let Some(sender) = flights.remove(&key) {
                sender.send_replace(Some(result));
            }
        });
        Refresh { receiver }
    }
}

#[cfg(feature = "redis")]
mod redis_store;
#[cfg(feature = "redis")]
pub use redis_store::RedisStore;
