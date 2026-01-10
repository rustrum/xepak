use std::collections::HashMap;
use std::sync::Arc;

use serde::Deserialize;
use sqlx::QueryBuilder;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Sqlite, SqlitePool};

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StorageSettings {
    Sqlite { id: String, file: String, wal: bool },
}

impl StorageSettings {
    pub fn get_id(&self) -> &str {
        match self {
            StorageSettings::Sqlite { id, .. } => id.as_str(),
        }
    }
}

#[derive(Clone)]
pub struct StorageLink {
    pool: SqlitePool,
}

impl StorageLink {
    pub async fn execute(&self, query: &str) {
        // QueryBuilder::with_arguments(init, arguments)
        let mut connection = self.pool.acquire().await.unwrap();
        // self.pool.
        // sqlx::query(query).bin.fetch_all(&mut *connection);
    }
}

pub async fn init_storage_links(storages: &[StorageSettings]) -> HashMap<String, StorageLink> {
    let mut links = HashMap::new();

    for store_settings in storages {
        match store_settings {
            StorageSettings::Sqlite { id, file, wal } => {
                let options = SqliteConnectOptions::new().filename(file);
                let options = if *wal {
                    options.journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
                } else {
                    options
                };

                let res = links.insert(
                    id.clone(),
                    StorageLink {
                        pool: SqlitePool::connect_lazy_with(options),
                    },
                );

                if res.is_some() {
                    tracing::warn!("Duplicate key \"{id}\" found for storage configuration");
                }
            }
        }
    }
    links
}
