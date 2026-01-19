use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;

use crate::XepakError;
use crate::types::{Record, SqlxValue, XepakValue};
use serde::de::value;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Database, QueryBuilder, TypeInfo, ValueRef};
use sqlx::{Row, Sqlite, SqlitePool};
use sqlx_core::column::Column;

pub async fn init_storage_connectors(
    conf_dir: &PathBuf,
    storages: &[StorageSettings],
) -> HashMap<String, Storage> {
    let mut links = HashMap::new();

    for store_settings in storages {
        match store_settings {
            StorageSettings::Sqlite { id, file, wal } => {
                let file_path = PathBuf::from(file);

                let file = if file_path.is_absolute() {
                    file_path
                } else {
                    conf_dir.join(file_path)
                };

                tracing::info!("Init sqlite storage \"{id}\" using path \"{file:?}\"");
                let options = SqliteConnectOptions::new().filename(file);
                let options = if *wal {
                    options.journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
                } else {
                    options
                };

                let res = links.insert(
                    id.clone(),
                    Storage {
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

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StorageSettings {
    Sqlite {
        #[serde(default)]
        id: String,
        file: String,
        #[serde(default)]
        wal: bool,
    },
}

impl StorageSettings {
    pub fn get_id(&self) -> &str {
        match self {
            StorageSettings::Sqlite { id, .. } => id.as_str(),
        }
    }
}

#[derive(Clone)]
pub struct Storage {
    pool: SqlitePool,
}

impl Storage {
    pub async fn execute(&self, query: &str) -> Result<Vec<Record>, XepakError> {
        // QueryBuilder::with_arguments(init, arguments)
        let mut connection = self.pool.acquire().await.unwrap();
        // self.pool.
        tracing::debug!("Executing query: {}", query);
        let result = sqlx::query(query)
            .fetch_all(&mut *connection)
            .await
            .expect("TODO");

        let mut out = Vec::new();
        for row in result {
            let cols = row.columns();
            let mut out_row = HashMap::new();
            for (idx, c) in cols.iter().enumerate() {
                let col = row.try_get_raw(idx).expect("TODO");
                let cval = XepakValue::try_from(SqlxValue::<sqlx::Sqlite>::new(col)).expect("TODO");

                out_row.insert(c.name().to_string(), cval);
            }
            out.push(out_row);
        }

        Ok(out)
    }
}

pub struct ResourceRequest {}

/// Lightweight schema enforcement.
/// If field is not provided in schema it's type should be resolved automatically
pub struct ResourceShema {}

pub struct Resource {
    sql: String,
}

impl Resource {
    pub async fn request(&self, request: &ResourceRequest) -> Result<Vec<Record>, XepakError> {
        unimplemented!()
    }
}

// pub struct Record {}

// impl Serialize for Record {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: serde::Serializer,
//     {
//         let mut s = serializer.serialize_struct("Record", 1)?;
//         s.serialize_field("Row", "&self.name")?;
//         s.end()
//     }
// }
