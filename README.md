# restate-cache

A small Tokio cache for opaque bytes, with stale-while-revalidate, bounded memory,
per-instance single-flight, composable storage tiers, and optional Redis/Valkey.
MIT licensed. Rust 1.85 or newer.

## Install

The repository is the distribution source until a crates.io release is published.
Add it as a local dependency (or use its Git URL):

```toml
[dependencies]
restate-cache = { path = "../restate-cache", features = ["redis"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The `redis` feature is optional. The default build needs no external service.

## Quick start

```rust
use restate_cache::{Cache, MemoryStore};
use std::{sync::Arc, time::Duration};

# async fn example() -> Result<(), restate_cache::Error> {
let cache = Cache::new(
    vec![Arc::new(MemoryStore::new(1_000)?)],
    Duration::from_secs(30), // fresh
    Duration::from_secs(120), // additional stale period
    Duration::from_secs(5), // refresh timeout
    Arc::new(|key| Box::pin(async move {
        // Replace with an async HTTP/database call. Return serialized bytes.
        Ok(format!("loaded {key}").into_bytes())
    })),
)?;

let result = cache.get("user:42").await?;
assert_eq!(result.value, b"loaded user:42");
if let Some(refresh) = result.refresh {
    // Stale data is already available; observe completion when appropriate.
    refresh.wait().await?;
}
# Ok(()) }
```

Run `cargo run --example basic`. Loaders own their keys, return a `Send + 'static`
future, and can capture cloned application clients. Serialize typed application
values at this boundary using your existing encoding; the cache does not impose
a payload format.

## Redis and Valkey tiers

```rust,ignore
use restate_cache::RedisStore;
let remote = Arc::new(RedisStore::connect("redis://127.0.0.1:6379/", "myapp:v1:").await?);
let cache = Cache::new(
    vec![Arc::new(MemoryStore::new(1_000)?), remote],
    Duration::from_secs(30), Duration::from_secs(120),
    Duration::from_secs(5), loader,
)?;
```

Use Redis 6.2+ or Valkey, which support `SET PXAT`. ConnectionManager reconnects
after connection failures; failed operations are returned to the caller. Server
entries use a JSON envelope with binary payload and absolute deadlines. Namespace
prefixes isolate applications and encoding versions. Configure credentials/TLS
through redis-rs URLs and feature settings as appropriate for your deployment;
the supplied feature enables plain TCP Tokio connections.

## Behavior and guarantees

* Fresh hits return without invoking the loader. Stale hits immediately return
  old bytes and an awaitable `Refresh` handle. Expired entries trigger a blocking
  miss. Stale entries are never extended when refresh fails.
* Simultaneous calls for a key share one loader **within a cloned Cache instance**.
  Separate instances/processes do not share flights or provide distributed locks.
* Dropping/cancelling a waiting caller does not cancel the refresh. Loader panics,
  timeouts and backend errors complete all waiters with an `Error`; later calls
  can retry. A Tokio runtime must remain alive for background work to finish.
* The refresh timeout covers lookup, loader and writes. Timeout aborts the worker;
  already committed backend writes cannot be rolled back. Initial hit lookup and
  invalidation have no library timeout; wrap them with `tokio::time::timeout` when
  a whole-request deadline is needed. Blocking loader code cannot be preempted.
  Loaders must yield cooperatively and adapters must tolerate cancelled futures.
* Tiers are read in order and promoted with the **original absolute deadlines**.
  First valid tier wins, even if stale; stale data triggers the origin loader.
  Writes run slowest to fastest. Storage errors are returned, never treated as
  misses. Multi-tier writes/deletes are not atomic and can partially succeed.
* `refresh(key)` forces origin loading (or joins a flight). `invalidate(key)`
  deletes all tiers, but an existing flight may subsequently repopulate them.
  Coordinate writes externally if stronger invalidation guarantees are needed.
* Deadlines use Unix milliseconds, so shared hosts/servers need synchronized
  clocks. Clock jumps affect freshness. Timeout uses Tokio's monotonic timer.
  Values are cloned on reads; capacity bounds entry count, not total bytes.
* MemoryStore evicts the earliest-expiring entry when full. It prunes expired
  entries on writes and removes an expired requested entry on reads. It is not
  an LRU cache. Refresh handles must be awaited/logged to observe stale failures.

## Custom adapters

Implement the object-safe `Store` trait using boxed `Send` futures. Preserve both
deadlines when storing or promoting entries, propagate errors, and use a stable
namespace. Public `Entry` enables alternate database/object-store integrations.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo build --all-features
REDIS_URL=redis://127.0.0.1:6379 cargo test --all-features --test redis -- --ignored
```

The real-server test is explicitly ignored without infrastructure; CI runs it
against Redis and Valkey in separate service jobs. Unit tests cover concurrency,
cancellation, panics, timeouts, stale errors, tier promotion, expiry, capacity and
strict error propagation. `Cargo.lock` is committed for reproducible CI. Changes
should include focused behavioral tests and pass all commands above.
