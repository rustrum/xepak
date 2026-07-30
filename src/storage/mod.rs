use std::borrow::Cow;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use crate::XepakError;
use crate::sql_key_args::ParametrizedQuery;
use crate::types::{Record, SqlxValue, XepakValue};
use serde::Deserialize;
use sqlx::any::{AnyArguments, AnyConnectOptions, AnyRow};
use sqlx::query::Query;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{Any, AnyPool, ConnectOptions, Pool, Row, migrate::Migrator};
use sqlx_core::column::Column;

pub const LIMIT_KEY: &str = "-limit-";
pub const OFFSET_KEY: &str = "-offset-";

pub async fn init_storage_connectors(
    conf_dir: &Path,
    storages: &[StorageSettings],
) -> Result<HashMap<String, Storage>, XepakError> {
    let mut links = HashMap::new();

    for store_settings in storages {
        match store_settings {
            StorageSettings::Sqlite {
                id,
                file,
                wal,
                migrations_dir,
                create_db,
            } => {
                let pool = if file.is_empty() {
                    let aco = AnyConnectOptions::from_str(
                        "sqlite:///file:inmem?mode=memory&cache=shared",
                    )
                    .expect("Must be valid");

                    tracing::info!("Using SQLite in-memory DB");

                    sqlx::any::AnyPoolOptions::new()
                        .max_connections(1)
                        .min_connections(1)
                        .connect_with(aco)
                        .await
                        .expect("Must work for in memory")
                } else {
                    let db_file = normalize_config_path(conf_dir, file);
                    tracing::info!("Init sqlite storage \"{id}\" using path \"{db_file:?}\"");

                    let options = SqliteConnectOptions::new()
                        .filename(db_file)
                        .create_if_missing(*create_db);

                    let options = if *wal {
                        options.journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
                    } else {
                        options
                    };

                    let aco = AnyConnectOptions::from_str(options.to_url_lossy().as_str())
                        .expect("Query string must be valid but it is not");

                    AnyPool::connect_lazy_with(aco)
                };

                if !migrations_dir.is_empty() {
                    tracing::info!("Applying migrations from: {migrations_dir}");
                    let migrations_path = normalize_config_path(conf_dir, migrations_dir);
                    migrate_stroage(&pool, &migrations_path).await?;
                }
                let res = links.insert(id.clone(), Storage { pool });

                if res.is_some() {
                    tracing::warn!("Duplicate key \"{id}\" found for storage configuration");
                }
            }
        }
    }
    Ok(links)
}

pub fn normalize_config_path(work_dir: &Path, path: &str) -> PathBuf {
    let file_path = PathBuf::from(path);

    if file_path.is_absolute() {
        file_path
    } else {
        work_dir.join(file_path)
    }
}

pub async fn migrate_stroage(pool: &Pool<Any>, dir: &Path) -> Result<(), XepakError> {
    let migrator = Migrator::new(dir)
        .await
        .map_err(|e| XepakError::Other(Arc::new(e.into())))?;

    migrator
        .run(pool)
        .await
        .map_err(|e| XepakError::Other(Arc::new(e.into())))?;

    Ok(())
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum StorageSettings {
    Sqlite {
        #[serde(default)]
        id: String,
        file: String,
        #[serde(default)]
        create_db: bool,
        #[serde(default)]
        wal: bool,
        #[serde(default)]
        migrations_dir: String,
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
    pool: AnyPool,
}

impl Storage {
    /// Execute storage query that returns some rows or empty vec.
    pub async fn query<RA: SqlxRequestArgs>(
        &self,
        request: ResourceRequest<'_, RA>,
    ) -> Result<Vec<Record>, XepakError> {
        let mut connection = self.pool.acquire().await.unwrap();

        let pquery = ParametrizedQuery::new(request.query);
        let query = pquery.build_query("?");
        let sql_query = self.prepare_query(&request, &pquery, &query)?;

        let result = sql_query.fetch_all(&mut *connection).await.expect("TODO");

        let mut out = Vec::new();
        for row in result {
            let out_row = self.map_row(row);
            out.push(out_row);
        }

        Ok(out)
    }

    /// Execute query fetch first row and returns it, if result is empty return Null.
    pub async fn query_one<RA: SqlxRequestArgs>(
        &self,
        request: ResourceRequest<'_, RA>,
    ) -> Result<Option<Record>, XepakError> {
        let mut connection = self.pool.acquire().await.unwrap();

        let pquery = ParametrizedQuery::new(request.query);
        let query = pquery.build_query("?");
        let sql_query = self.prepare_query(&request, &pquery, &query)?;

        let result = sql_query
            .fetch_optional(&mut *connection)
            .await
            .expect("TODO");

        Ok(result.map(|r| self.map_row(r)))
    }

    /// Execute query that must have one row with only one column.
    /// Returns value of this column.
    pub async fn query_value<RA: SqlxRequestArgs>(
        &self,
        request: ResourceRequest<'_, RA>,
    ) -> Result<XepakValue, XepakError> {
        let Some(row) = self.query_one(request).await? else {
            return Err(XepakError::NotConsistent(
                "Expect to have non empty response from DB".to_string(),
            ));
        };

        tracing::debug!("ONE: {row:?}");

        if row.len() > 1 {
            return Err(XepakError::NotConsistent(format!(
                "Expect to have only single column in response not {}",
                row.len()
            )));
        }

        if let Some(entry) = row.into_iter().next() {
            Ok(entry.1)
        } else {
            Err(XepakError::Unexpected(
                "It is impossible! Row must have at least one column".to_string(),
            ))
        }
    }

    fn prepare_query<'q, RA: SqlxRequestArgs>(
        &self,
        request: &'q ResourceRequest<'q, RA>,
        pquery: &'q ParametrizedQuery<'q>,
        query: &'q Cow<'q, str>,
    ) -> Result<Query<'q, Any, AnyArguments<'q>>, XepakError> {
        tracing::debug!("Executing query: {}", pquery.get_query());
        tracing::debug!("Executing build: {}", query);
        let mut sql_query = sqlx::query(query.as_ref());

        tracing::debug!("Query arguments: {:?}", pquery.get_args());
        for argument_name in pquery.get_args() {
            let arg = *argument_name;
            sql_query = match arg {
                LIMIT_KEY => {
                    tracing::debug!("Query limit: {}", request.args.get_rows_limit());
                    sql_query.bind(request.args.get_rows_limit() as i64)
                }
                OFFSET_KEY => {
                    tracing::debug!("Query offset: {}", request.args.get_rows_offset());
                    sql_query.bind(request.args.get_rows_offset() as i64)
                }
                _ => request.args.bind_arg(arg, sql_query)?,
            };
        }

        Ok(sql_query)
    }

    fn map_row(&self, row: AnyRow) -> Record {
        let cols = row.columns();
        let mut out_row = HashMap::new();
        for (idx, c) in cols.iter().enumerate() {
            let col = row.try_get_raw(idx).expect("TODO");
            let cval = XepakValue::try_from(SqlxValue::new(col)).expect("TODO");

            out_row.insert(c.name().to_string(), cval);
        }
        out_row
    }
}

pub struct ResourceRequest<'a, RA: StorageRequestArgs> {
    args: &'a RA,
    query: &'a str,
}

impl<'a, RA: StorageRequestArgs> ResourceRequest<'a, RA> {
    pub fn new(query: &'a str, args: &'a RA) -> Self {
        Self { args, query }
    }
}

pub trait StorageRequestArgs {
    /// Return records fetch limit
    fn get_rows_limit(&self) -> usize;

    /// Return records fetch offset
    fn get_rows_offset(&self) -> usize;
}

/// SQLx related request args bind functionality
pub trait SqlxRequestArgs: StorageRequestArgs {
    fn bind_arg<'a>(
        &'a self,
        arg_name: &str,
        query: sqlx::query::Query<'a, sqlx::Any, sqlx::any::AnyArguments<'a>>,
    ) -> Result<sqlx::query::Query<'a, sqlx::Any, sqlx::any::AnyArguments<'a>>, XepakError>;
}
