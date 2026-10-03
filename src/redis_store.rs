use crate::{now_ms, BoxFuture, Entry, Error, Store};

/// Redis/Valkey tier using JSON envelopes and server-side millisecond expiration.
pub struct RedisStore {
    connection: redis::aio::ConnectionManager,
    prefix: String,
}
impl RedisStore {
    /// Connect using a Redis URL. Use a dedicated namespace prefix.
    pub async fn connect(url: &str, prefix: impl Into<String>) -> Result<Self, Error> {
        let client = redis::Client::open(url).map_err(|e| Error(e.to_string()))?;
        let connection = client
            .get_connection_manager()
            .await
            .map_err(|e| Error(e.to_string()))?;
        Ok(Self {
            connection,
            prefix: prefix.into(),
        })
    }
}
impl Store for RedisStore {
    fn get<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<Option<Entry>, Error>> {
        Box::pin(async move {
            let mut connection = self.connection.clone();
            let data: Option<Vec<u8>> = redis::cmd("GET")
                .arg(format!("{}{key}", self.prefix))
                .query_async(&mut connection)
                .await
                .map_err(|e| Error(e.to_string()))?;
            data.map(|bytes| serde_json::from_slice(&bytes).map_err(|e| Error(e.to_string())))
                .transpose()
        })
    }
    fn put<'a>(&'a self, key: &'a str, entry: Entry) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            let mut connection = self.connection.clone();
            let redis_key = format!("{}{key}", self.prefix);
            if entry.expires_at_ms <= now_ms() {
                return redis::cmd("DEL")
                    .arg(redis_key)
                    .query_async(&mut connection)
                    .await
                    .map_err(|e| Error(e.to_string()));
            }
            let data = serde_json::to_vec(&entry).map_err(|e| Error(e.to_string()))?;
            redis::cmd("SET")
                .arg(redis_key)
                .arg(data)
                .arg("PXAT")
                .arg(entry.expires_at_ms)
                .query_async(&mut connection)
                .await
                .map_err(|e| Error(e.to_string()))
        })
    }
    fn delete<'a>(&'a self, key: &'a str) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(async move {
            let mut connection = self.connection.clone();
            redis::cmd("DEL")
                .arg(format!("{}{key}", self.prefix))
                .query_async(&mut connection)
                .await
                .map_err(|e| Error(e.to_string()))
        })
    }
}
