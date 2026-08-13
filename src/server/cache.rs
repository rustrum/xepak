use std::time::Duration;

use moka::future::Cache;

use crate::xepak_data::XepakValue;

#[derive(Clone)]
pub struct AppCache {
    pub cache: Cache<String, XepakValue>,
}

impl AppCache {
    pub fn new(capacity: u64, ttl: Duration) -> Self {
        Self {
            cache: Cache::builder()
                .time_to_live(ttl)
                .max_capacity(capacity)
                .build(),
        }
    }

    pub async fn get(&self, key: &str) -> Option<XepakValue> {
        self.cache.get(key).await
    }

    pub async fn insert(&self, key: String, value: XepakValue) {
        self.cache.insert(key, value).await
    }

    pub async fn cleanup(&self) {
        self.cache.run_pending_tasks().await;
    }
}
