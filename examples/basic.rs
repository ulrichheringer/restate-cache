//! Minimal in-memory cache example.
use restate_cache::{Cache, MemoryStore};
use std::{sync::Arc, time::Duration};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cache = Cache::new(
        vec![Arc::new(MemoryStore::new(1_000)?)],
        Duration::from_secs(30),
        Duration::from_secs(120),
        Duration::from_secs(5),
        Arc::new(|key| Box::pin(async move { Ok(format!("loaded {key}").into_bytes()) })),
    )?;
    let cached = cache.get("user:42").await?;
    println!("{}", String::from_utf8(cached.value)?);
    if let Some(refresh) = cached.refresh {
        refresh.wait().await?;
    }
    Ok(())
}
