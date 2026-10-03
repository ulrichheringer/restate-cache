//! Cache behavioral regression tests.
use restate_cache::{Cache, Entry, Error, Loader, MemoryStore, Store};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
fn epoch() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
fn cache(loader: Loader) -> Cache {
    Cache::new(
        vec![Arc::new(MemoryStore::new(100).unwrap())],
        Duration::from_millis(25),
        Duration::from_secs(1),
        Duration::from_millis(150),
        loader,
    )
    .unwrap()
}

#[tokio::test]
async fn concurrent_misses_share_one_loader() {
    let count = Arc::new(AtomicUsize::new(0));
    let loader_count = count.clone();
    let cache = cache(Arc::new(move |_| {
        let count = loader_count.clone();
        Box::pin(async move {
            count.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(b"value".to_vec())
        })
    }));
    let mut tasks = Vec::new();
    for _ in 0..50 {
        let cache = cache.clone();
        tasks.push(tokio::spawn(
            async move { cache.get("k").await.unwrap().value },
        ));
    }
    for task in tasks {
        assert_eq!(task.await.unwrap(), b"value");
    }
    assert_eq!(count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn stale_returns_immediately_and_refresh_is_observable() {
    let count = Arc::new(AtomicUsize::new(0));
    let loader_count = count.clone();
    let cache = cache(Arc::new(move |_| {
        let count = loader_count.clone();
        Box::pin(async move {
            let n = count.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                Ok(b"old".to_vec())
            } else {
                Err(Error("upstream down".into()))
            }
        })
    }));
    cache.get("k").await.unwrap();
    tokio::time::sleep(Duration::from_millis(35)).await;
    let stale = cache.get("k").await.unwrap();
    assert!(stale.stale);
    assert_eq!(stale.value, b"old");
    assert_eq!(
        stale.refresh.unwrap().wait().await.unwrap_err().0,
        "upstream down"
    );
}

#[tokio::test]
async fn loader_failure_is_shared_and_retryable() {
    let cache = cache(Arc::new(|_| {
        Box::pin(async { Err(Error("broken".into())) })
    }));
    for _ in 0..2 {
        assert!(cache.get("k").await.err().unwrap().0.contains("broken"));
    }
}

#[tokio::test]
async fn panic_does_not_strand_flight() {
    let cache = cache(Arc::new(|_| Box::pin(async { panic!("loader panic") })));
    for _ in 0..2 {
        assert!(tokio::time::timeout(Duration::from_secs(1), cache.get("k"))
            .await
            .unwrap()
            .err()
            .unwrap()
            .0
            .contains("refresh task failed"));
    }
}

#[tokio::test]
async fn timeout_cleans_flight() {
    let cache = cache(Arc::new(|_| Box::pin(std::future::pending())));
    for _ in 0..2 {
        assert_eq!(cache.get("k").await.err().unwrap().0, "refresh timed out");
    }
}

#[tokio::test]
async fn cancelling_waiter_does_not_cancel_loader() {
    let cache = cache(Arc::new(|_| {
        Box::pin(async {
            tokio::time::sleep(Duration::from_millis(30)).await;
            Ok(b"done".to_vec())
        })
    }));
    let caller_cache = cache.clone();
    let caller = tokio::spawn(async move { caller_cache.get("k").await });
    tokio::time::sleep(Duration::from_millis(5)).await;
    caller.abort();
    assert_eq!(cache.get("k").await.unwrap().value, b"done");
}

#[tokio::test]
async fn promotion_preserves_stale_deadlines() {
    let l1 = Arc::new(MemoryStore::new(10).unwrap());
    let l2 = Arc::new(MemoryStore::new(10).unwrap());
    let deadline = epoch() + 500;
    l2.put(
        "k",
        Entry {
            value: b"old".to_vec(),
            fresh_until_ms: epoch() - 1,
            expires_at_ms: deadline,
        },
    )
    .await
    .unwrap();
    let cache = Cache::new(
        vec![l1.clone(), l2],
        Duration::from_secs(60),
        Duration::from_secs(60),
        Duration::from_secs(1),
        Arc::new(|_| Box::pin(async { Err(Error("no refresh".into())) })),
    )
    .unwrap();
    let result = cache.get("k").await.unwrap();
    assert!(result.stale);
    result.refresh.unwrap().wait().await.unwrap_err();
    assert_eq!(l1.get("k").await.unwrap().unwrap().expires_at_ms, deadline);
}

#[tokio::test]
async fn memory_capacity_expiry_and_invalidation() {
    let memory = MemoryStore::new(1).unwrap();
    let entry = Entry {
        value: vec![1],
        fresh_until_ms: epoch() + 100,
        expires_at_ms: epoch() + 100,
    };
    memory.put("a", entry.clone()).await.unwrap();
    memory.put("b", entry).await.unwrap();
    assert!(memory.get("a").await.unwrap().is_none());
    memory.delete("b").await.unwrap();
    assert!(memory.get("b").await.unwrap().is_none());
    memory
        .put(
            "expired",
            Entry {
                value: vec![],
                fresh_until_ms: 0,
                expires_at_ms: 0,
            },
        )
        .await
        .unwrap();
    assert!(memory.get("expired").await.unwrap().is_none());
    let cache = cache(Arc::new(|_| Box::pin(async { Ok(vec![2]) })));
    cache.get("k").await.unwrap();
    cache.invalidate("k").await.unwrap();
    assert_eq!(cache.get("k").await.unwrap().value, vec![2]);
}

struct BrokenStore;

struct SlowPromotion;
impl Store for SlowPromotion {
    fn get<'a>(&'a self, _: &'a str) -> restate_cache::BoxFuture<'a, Result<Option<Entry>, Error>> {
        Box::pin(async { Ok(None) })
    }
    fn put<'a>(&'a self, _: &'a str, _: Entry) -> restate_cache::BoxFuture<'a, Result<(), Error>> {
        Box::pin(async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok(())
        })
    }
    fn delete<'a>(&'a self, _: &'a str) -> restate_cache::BoxFuture<'a, Result<(), Error>> {
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn promotion_that_exceeds_expiry_does_not_serve_expired_data() {
    let l2 = Arc::new(MemoryStore::new(10).unwrap());
    l2.put(
        "k",
        Entry {
            value: b"expired during promotion".to_vec(),
            fresh_until_ms: epoch() - 1,
            expires_at_ms: epoch() + 30,
        },
    )
    .await
    .unwrap();
    let cache = Cache::new(
        vec![Arc::new(SlowPromotion), l2],
        Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::from_secs(1),
        Arc::new(|_| Box::pin(async { Ok(b"new".to_vec()) })),
    )
    .unwrap();
    let result = cache.get("k").await.unwrap();
    assert!(!result.stale);
    assert_eq!(result.value, b"new");
}
impl Store for BrokenStore {
    fn get<'a>(&'a self, _: &'a str) -> restate_cache::BoxFuture<'a, Result<Option<Entry>, Error>> {
        Box::pin(async { Err(Error("storage unavailable".into())) })
    }
    fn put<'a>(&'a self, _: &'a str, _: Entry) -> restate_cache::BoxFuture<'a, Result<(), Error>> {
        Box::pin(async { Err(Error("write failed".into())) })
    }
    fn delete<'a>(&'a self, _: &'a str) -> restate_cache::BoxFuture<'a, Result<(), Error>> {
        Box::pin(async { Ok(()) })
    }
}
#[tokio::test]
async fn storage_errors_are_not_cache_misses() {
    let cache = Cache::new(
        vec![Arc::new(BrokenStore)],
        Duration::from_secs(1),
        Duration::ZERO,
        Duration::from_secs(1),
        Arc::new(|_| Box::pin(async { Ok(vec![]) })),
    )
    .unwrap();
    assert_eq!(cache.get("k").await.err().unwrap().0, "storage unavailable");
    assert_eq!(cache.refresh("k").await.unwrap_err().0, "write failed");
}

#[test]
fn rejects_invalid_configuration() {
    assert!(MemoryStore::new(0).is_err());
    assert!(Cache::new(
        vec![],
        Duration::from_secs(1),
        Duration::ZERO,
        Duration::from_secs(1),
        Arc::new(|_| Box::pin(async { Ok(vec![]) }))
    )
    .is_err());
}
