use std::time::{Duration, Instant};

use moka::{Expiry, future::Cache};

use crate::xepak_data::XepakValue;

pub const CACHE_TTL_DEFAULT_SEC: u64 = 60;

/// Each cache value is stored with expiration TTL in seconds.
/// If TTL is zero - default TTL will be used then.
pub type CacheEntry = (XepakValue, u16);

#[derive(Clone)]
pub struct AppCache {
    pub cache: Cache<String, CacheEntry>,
}

impl AppCache {
    pub fn new(capacity: u64, ttl: Duration) -> Self {
        Self {
            cache: Cache::builder()
                .time_to_live(ttl)
                .max_capacity(capacity)
                .expire_after(CacheExpiry)
                .build(),
        }
    }

    pub async fn get(&self, key: &str) -> Option<XepakValue> {
        self.cache.get(key).await.map(|v| v.0)
    }

    pub async fn insert(&self, key: String, value: XepakValue) {
        self.cache.insert(key, (value, 0)).await
    }

    pub async fn insert_ttl(&self, key: String, value: XepakValue, ttl_sec: u16) {
        self.cache.insert(key, (value, ttl_sec)).await
    }

    /// Insert with custom TTL in seconds.
    /// If TTL = 0 - default will be used.
    pub async fn cleanup(&self) {
        self.cache.run_pending_tasks().await;
    }
}

struct CacheExpiry;

impl Expiry<String, CacheEntry> for CacheExpiry {
    fn expire_after_create(
        &self,
        _key: &String,
        entry: &CacheEntry,
        created_at: Instant,
    ) -> Option<Duration> {
        let ttl_sec = if entry.1 == 0 {
            CACHE_TTL_DEFAULT_SEC
        } else {
            entry.1 as u64
        };

        created_at
            .checked_add(Duration::from_secs(ttl_sec))
            .and_then(|v| v.checked_duration_since(Instant::now()))
    }
}
