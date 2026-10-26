use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use crate::XepakError;
use crate::sql_key_args::ParametrizedQuery;
use crate::xepak_data::XepakValue;
use crate::xepak_data::sql::{SqlxBindable, SqlxValue};
use indexmap::IndexMap;
use serde::Deserialize;
use sqlx::migrate::Migrate;
use sqlx::query::Query;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Acquire, ColumnIndex, Database, IntoArguments, SqlitePool};
use sqlx::{Row, migrate::Migrator};
use sqlx_core::column::Column;

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
                    let options = SqliteConnectOptions::from_str(
                        "sqlite:///file:inmem?mode=memory&cache=shared",
                    )
                    .expect("Constant connection string must work");

                    tracing::info!("Using SQLite in-memory DB");

                    SqlitePoolOptions::new()
                        .max_connections(1)
                        .min_connections(1)
                        .connect_with(options)
                        .await
                        .expect("Must work for in memory")
                } else {
                    let db_file = normalize_config_path(conf_dir, file);
                    create_parent_dirs(&db_file)?;
                    tracing::info!("Init SQLite storage \"{id}\" using path \"{db_file:?}\"");

                    let options = SqliteConnectOptions::new()
                        .filename(db_file)
                        .create_if_missing(*create_db);

                    let options = if *wal {
                        options.journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
                    } else {
                        options
                    };

                    SqlitePool::connect_lazy_with(options)
                };

                if !migrations_dir.is_empty() {
                    tracing::info!("Applying migrations from: {migrations_dir}");
                    let migrations_path = normalize_config_path(conf_dir, migrations_dir);
                    migrate_storage(&pool, &migrations_path).await?;
                }
                let res = links.insert(
                    id.clone(),
                    Storage {
                        pool: StorageConnection::Sqlite(pool),
                    },
                );

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

pub async fn migrate_storage<'a, A>(pool: A, dir: &Path) -> Result<(), XepakError>
where
    A: Acquire<'a>,
    <A::Connection as Deref>::Target: Migrate,
{
    let migrator = Migrator::new(dir)
        .await
        .map_err(|e| XepakError::Storage(format!("{e:?}")))?;

    migrator
        .run(pool)
        .await
        .map_err(|e| XepakError::Storage(format!("{e:?}")))?;

    Ok(())
}

fn create_parent_dirs(file_path: &Path) -> Result<(), XepakError> {
    let Some(parent) = file_path.parent() else {
        return Ok(());
    };
    if parent.as_os_str().is_empty() {
        return Ok(());
    }

    std::fs::create_dir_all(parent).map_err(|err| XepakError::Cfg(err.to_string()))
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
pub enum StorageConnection {
    Sqlite(SqlitePool),
}

#[derive(Clone)]
pub struct Storage {
    pool: StorageConnection,
}

impl Storage {
    /// Execute storage query that returns some rows or empty vec.
    pub async fn query<RA: SqlxRequestArgs>(
        &self,
        request: ResourceRequest<'_, RA>,
    ) -> Result<Vec<XepakValue>, XepakError> {
        match &self.pool {
            StorageConnection::Sqlite(pool) => {
                let mut connection = pool
                    .acquire()
                    .await
                    .map_err(|e| XepakError::Storage(format!("{e:?}")))?;
                self.query_impl::<sqlx::Sqlite, RA>(&mut connection, &request)
                    .await
            }
        }
    }

    async fn query_impl<DB, RA>(
        &self,
        connection: &mut <DB as Database>::Connection,
        request: &ResourceRequest<'_, RA>,
    ) -> Result<Vec<XepakValue>, XepakError>
    where
        DB: sqlx::Database,
        RA: SqlxRequestArgs,
        // XepakValue wrapped types
        for<'a> Option<String>: SqlxBindable<'a, DB>,
        for<'a> bool: SqlxBindable<'a, DB>,
        for<'a> i64: SqlxBindable<'a, DB>,
        for<'a> f64: SqlxBindable<'a, DB>,
        for<'a> String: SqlxBindable<'a, DB>,
        for<'a> Vec<u8>: SqlxBindable<'a, DB>,
        // Neede by fetch_all
        <DB as Database>::Arguments: IntoArguments<DB>,
        for<'c> &'c mut <DB as sqlx::Database>::Connection: sqlx::Executor<'c, Database = DB>,
        // Need by map_row
        usize: sqlx::ColumnIndex<<DB as sqlx::Database>::Row>,
        for<'xv> XepakValue: TryFrom<SqlxValue<'xv, DB>, Error = sqlx::error::BoxDynError>,
    {
        let pquery = ParametrizedQuery::new(request.query);
        let query = pquery.build_query("?");
        let sql_query = self.prepare_query::<DB, RA>(request, &pquery, &query)?;

        let result = sql_query
            .fetch_all(&mut *connection)
            .await
            .map_err(|e| XepakError::Other(Arc::new(e.into())))?;

        let mut out = Vec::new();
        for row in result {
            let out_row = self.map_row(row);
            out.push(out_row?.into());
        }

        Ok(out)
    }

    /// Execute query fetch first row and returns it, if result is empty return Null.
    pub async fn query_one<RA: SqlxRequestArgs>(
        &self,
        request: ResourceRequest<'_, RA>,
    ) -> Result<Option<XepakValue>, XepakError> {
        match &self.pool {
            StorageConnection::Sqlite(pool) => {
                let mut connection = pool
                    .acquire()
                    .await
                    .map_err(|e| XepakError::Storage(format!("{e:?}")))?;
                self.query_one_impl::<sqlx::Sqlite, RA>(&mut connection, &request)
                    .await
            }
        }
    }

    async fn query_one_impl<DB, RA>(
        &self,
        connection: &mut <DB as Database>::Connection,
        request: &ResourceRequest<'_, RA>,
    ) -> Result<Option<XepakValue>, XepakError>
    where
        DB: sqlx::Database,
        RA: SqlxRequestArgs,
        // XepakValue wrapped types
        for<'a> Option<String>: SqlxBindable<'a, DB>,
        for<'a> bool: SqlxBindable<'a, DB>,
        for<'a> i64: SqlxBindable<'a, DB>,
        for<'a> f64: SqlxBindable<'a, DB>,
        for<'a> String: SqlxBindable<'a, DB>,
        for<'a> Vec<u8>: SqlxBindable<'a, DB>,
        // Neede by fetch_all
        <DB as Database>::Arguments: IntoArguments<DB>,
        for<'c> &'c mut <DB as sqlx::Database>::Connection: sqlx::Executor<'c, Database = DB>,
        // Need by map_row
        usize: sqlx::ColumnIndex<<DB as sqlx::Database>::Row>,
        for<'xv> XepakValue: TryFrom<SqlxValue<'xv, DB>, Error = sqlx::error::BoxDynError>,
    {
        let pquery = ParametrizedQuery::new(request.query);
        let query = pquery.build_query("?");
        let sql_query = self.prepare_query::<DB, RA>(request, &pquery, &query)?;

        let result = sql_query
            .fetch_optional(&mut *connection)
            .await
            .map_err(|e| XepakError::Other(Arc::new(e.into())))?;

        result
            .map(|r| self.map_row(r).map(|v| v.into()))
            .transpose()
    }

    /// Execute query that must have one row with only one column.
    /// Returns value of this column.
    pub async fn query_value<RA: SqlxRequestArgs>(
        &self,
        request: ResourceRequest<'_, RA>,
    ) -> Result<XepakValue, XepakError> {
        let Some(row) = self.query_one(request).await? else {
            return Err(XepakError::NotConsistent(
                "Expected to have non empty response from DB".to_string(),
            ));
        };
        let row = row.as_map()?;

        tracing::debug!("ONE: {row:?}");

        if row.len() > 1 {
            return Err(XepakError::NotConsistent(format!(
                "Expected to have only single column in response not {}",
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

    fn prepare_query<'a, DB, RA>(
        &self,
        request: &'a ResourceRequest<'a, RA>,
        pquery: &'a ParametrizedQuery<'a>,
        query: &'a Cow<'a, str>,
    ) -> Result<Query<'a, DB, <DB as Database>::Arguments>, XepakError>
    where
        DB: Database,
        RA: SqlxRequestArgs,
        // XepakValue wrapped types
        Option<String>: SqlxBindable<'a, DB>,
        bool: SqlxBindable<'a, DB>,
        i64: SqlxBindable<'a, DB>,
        f64: SqlxBindable<'a, DB>,
        String: SqlxBindable<'a, DB>,
        Vec<u8>: SqlxBindable<'a, DB>,
    {
        tracing::debug!("Executing query: {}", pquery.get_query());
        tracing::debug!("Executing build: {}", query);

        let mut sql_query = sqlx::query(sqlx::AssertSqlSafe(query.as_ref()));

        tracing::debug!("Query arguments: {:?}", pquery.get_args());
        for argument_name in pquery.get_args() {
            let arg = *argument_name;
            sql_query = request.args.bind_arg(arg, sql_query)?;
        }

        Ok(sql_query)
    }

    fn map_row<DB>(
        &self,
        row: <DB as Database>::Row,
    ) -> Result<IndexMap<String, XepakValue>, XepakError>
    where
        DB: Database,
        usize: ColumnIndex<<DB as Database>::Row>,
        for<'r> XepakValue: TryFrom<SqlxValue<'r, DB>>,
        for<'r> <XepakValue as TryFrom<SqlxValue<'r, DB>>>::Error: std::fmt::Debug,
    {
        // fn map_row(&self, row: AnyRow) -> HashMap<String, XepakValue> {
        let cols = row.columns();
        let mut out_row = IndexMap::new();
        for (idx, c) in cols.iter().enumerate() {
            let col = row
                .try_get_raw(idx)
                .map_err(|e| XepakError::Storage(format!("{e:?}")))?;
            let cval = XepakValue::try_from(SqlxValue::<DB>::new(col))
                .map_err(|e| XepakError::Convert(format!("{e:?}")))?;

            out_row.insert(c.name().to_string(), cval);
        }

        Ok(out_row)
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

/// Not sure about this trait maybe request args structure in the future will be different.
/// Or maybe it should be removed
pub trait StorageRequestArgs {}

/// SQLx related request args bind functionality
pub trait SqlxRequestArgs: StorageRequestArgs {
    fn bind_arg<'a, DB>(
        &'a self,
        arg_name: &str,
        query: Query<'a, DB, <DB as sqlx::Database>::Arguments>,
    ) -> Result<Query<'a, DB, <DB as sqlx::Database>::Arguments>, XepakError>
    where
        DB: sqlx::Database,
        // XepakValue wrapped types
        Option<String>: SqlxBindable<'a, DB>,
        bool: SqlxBindable<'a, DB>,
        i64: SqlxBindable<'a, DB>,
        f64: SqlxBindable<'a, DB>,
        String: SqlxBindable<'a, DB>,
        Vec<u8>: SqlxBindable<'a, DB>;
}
