//! Real Redis/Valkey adapter integration tests.
#![cfg(feature = "redis")]
use restate_cache::{Entry, RedisStore, Store};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[tokio::test]
#[ignore = "requires REDIS_URL pointing to real Redis or Valkey"]
async fn real_server_roundtrip_expiry_and_corruption() {
    let url = std::env::var("REDIS_URL").expect("REDIS_URL required");
    let prefix = format!(
        "restate:test:{}:",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let store = RedisStore::connect(&url, &prefix).await.unwrap();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let entry = Entry {
        value: vec![0, 255, 3],
        fresh_until_ms: now + 100,
        expires_at_ms: now + 500,
    };
    store.put("key", entry.clone()).await.unwrap();
    let actual = store.get("key").await.unwrap().unwrap();
    assert_eq!(actual.value, entry.value);
    assert_eq!(actual.expires_at_ms, entry.expires_at_ms);
    tokio::time::sleep(Duration::from_millis(650)).await;
    assert!(store.get("key").await.unwrap().is_none());
    let mut connection = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: () = redis::cmd("SET")
        .arg(format!("{prefix}key"))
        .arg("invalid-json")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(store.get("key").await.is_err());
    store.delete("key").await.unwrap();
    assert!(store.get("key").await.unwrap().is_none());
}
