use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use crate::XepakError;
use crate::sql_key_args::ParametrizedQuery;
use crate::xepak_data::XepakValue;
use crate::xepak_data::sql::SqlxValue;
use serde::Deserialize;
use sqlx::migrate::Migrate;
use sqlx::query::Query;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Acquire, ColumnIndex, Database, Decode, Encode, IntoArguments, SqlitePool, Type};
use sqlx::{Row, migrate::Migrator};
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
                // let pool = if file.is_empty() {
                //     let aco = AnyConnectOptions::from_str(
                //         "sqlite:///file:inmem?mode=memory&cache=shared",
                //     )
                //     .expect("Must be valid");
                //
                //     tracing::info!("Using SQLite in-memory DB");
                //
                //     sqlx::any::AnyPoolOptions::new()
                //         .max_connections(1)
                //         .min_connections(1)
                //         .connect_with(aco)
                //         .await
                //         .expect("Must work for in memory")
                // } else {
                //     let db_file = normalize_config_path(conf_dir, file);
                //     tracing::info!("Init SQLite storage \"{id}\" using path \"{db_file:?}\"");
                //
                //     let options = SqliteConnectOptions::new()
                //         .filename(db_file)
                //         .create_if_missing(*create_db);
                //
                //     let options = if *wal {
                //         options.journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
                //     } else {
                //         options
                //     };
                //
                //     let aco = AnyConnectOptions::from_str(options.to_url_lossy().as_str())
                //         .expect("Query string must be valid but it is not");
                //
                //     AnyPool::connect_lazy_with(aco)
                // };
                let pool = if file.is_empty() {
                    let options = SqliteConnectOptions::from_str(
                        "sqlite:///file:inmem?mode=memory&cache=shared",
                    )
                    .expect("Must be valid");

                    tracing::info!("Using SQLite in-memory DB");

                    SqlitePoolOptions::new()
                        .max_connections(1)
                        .min_connections(1)
                        .connect_with(options)
                        .await
                        .expect("Must work for in memory")
                } else {
                    let db_file = normalize_config_path(conf_dir, file);
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
                let mut connection = pool.acquire().await.unwrap();
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
        // XepakValue related types
        for<'q> Option<String>: Encode<'q, DB>,
        for<'q> String: Encode<'q, DB> + Decode<'q, DB> + Type<DB>,
        for<'q> bool: Encode<'q, DB> + Decode<'q, DB> + Type<DB>,
        for<'q> i64: Encode<'q, DB> + Decode<'q, DB> + Type<DB>,
        for<'q> f64: Encode<'q, DB> + Decode<'q, DB> + Type<DB>,
        for<'q> String: Encode<'q, DB> + Decode<'q, DB> + Type<DB>,
        for<'q> Vec<u8>: Encode<'q, DB> + Decode<'q, DB> + Type<DB>,
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
            .expect("TODO fetch_all");

        let mut out = Vec::new();
        for row in result {
            let out_row = self.map_row(row);
            out.push(out_row.into());
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
                let mut connection = pool.acquire().await.unwrap();
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
        // XepakValue related types
        for<'q> Option<String>: Encode<'q, DB>,
        for<'q> String: Encode<'q, DB> + Decode<'q, DB> + Type<DB>,
        for<'q> bool: Encode<'q, DB> + Decode<'q, DB> + Type<DB>,
        for<'q> i64: Encode<'q, DB> + Decode<'q, DB> + Type<DB>,
        for<'q> f64: Encode<'q, DB> + Decode<'q, DB> + Type<DB>,
        for<'q> String: Encode<'q, DB> + Decode<'q, DB> + Type<DB>,
        for<'q> Vec<u8>: Encode<'q, DB> + Decode<'q, DB> + Type<DB>,
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
            .expect("TODO");

        Ok(result.map(|r| self.map_row(r).into()))
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

    fn prepare_query<'q, DB, RA>(
        &self,
        request: &'q ResourceRequest<'q, RA>,
        pquery: &'q ParametrizedQuery<'q>,
        query: &'q Cow<'q, str>,
    ) -> Result<Query<'q, DB, <DB as Database>::Arguments>, XepakError>
    where
        DB: Database,
        RA: SqlxRequestArgs,
        // Binds types associated with XepakValue
        Option<String>: Encode<'q, DB>,
        String: Encode<'q, DB> + Type<DB>,
        bool: Encode<'q, DB> + Type<DB>,
        i64: Encode<'q, DB> + Type<DB>,
        f64: Encode<'q, DB> + Type<DB>,
        String: Encode<'q, DB> + Type<DB>,
        Vec<u8>: Encode<'q, DB> + Type<DB>,
        // Bounds for the value bound by `bind_arg`
        // XepakValue: Encode<'q, DB> + Type<DB>,
    {
        tracing::debug!("Executing query: {}", pquery.get_query());
        tracing::debug!("Executing build: {}", query);

        let mut sql_query = sqlx::query(sqlx::AssertSqlSafe(query.as_ref()));

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

    fn map_row<DB>(&self, row: <DB as Database>::Row) -> HashMap<String, XepakValue>
    where
        DB: Database,
        usize: ColumnIndex<<DB as Database>::Row>,
        // Option<bool>: Decode< DB> + Type<DB>,
        // String: Decode<'r, DB> + Type<DB>,
        // bool: Decode<'r, DB> + Type<DB>,
        // i64: Decode<'r, DB> + Type<DB>,
        // f64: Decode<'r, DB> + Type<DB>,
        // String: Decode<'r, DB> + Type<DB>,
        // Vec<u8>: Decode<'r, DB> + Type<DB>,
        for<'r> XepakValue: TryFrom<SqlxValue<'r, DB>>,
        for<'r> <XepakValue as TryFrom<SqlxValue<'r, DB>>>::Error: std::fmt::Debug,
    {
        // fn map_row(&self, row: AnyRow) -> HashMap<String, XepakValue> {
        let cols = row.columns();
        let mut out_row = HashMap::new();
        for (idx, c) in cols.iter().enumerate() {
            let col = row.try_get_raw(idx).expect("TODO get raw");
            let cval =
                XepakValue::try_from(SqlxValue::<DB>::new(col)).expect("TODO xepak try_from");

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
// pub trait SqlxRequestArgs: StorageRequestArgs {
//     fn bind_arg<'a>(
//         &'a self,
//         arg_name: &str,
//         query: sqlx::query::Query<'a, sqlx::Any, sqlx::any::AnyArguments>,
//     ) -> Result<sqlx::query::Query<'a, sqlx::Any, sqlx::any::AnyArguments>, XepakError>;
// }
pub trait SqlxRequestArgs: StorageRequestArgs {
    fn bind_arg<'q, DB>(
        &'q self,
        arg_name: &str,
        query: Query<'q, DB, <DB as sqlx::Database>::Arguments>,
    ) -> Result<Query<'q, DB, <DB as sqlx::Database>::Arguments>, XepakError>
    where
        DB: sqlx::Database,
        Option<String>: Encode<'q, DB>,
        String: Encode<'q, DB> + Type<DB>,
        bool: Encode<'q, DB> + Type<DB>,
        i64: Encode<'q, DB> + Type<DB>,
        f64: Encode<'q, DB> + Type<DB>,
        String: Encode<'q, DB> + Type<DB>,
        Vec<u8>: Encode<'q, DB> + Type<DB>;
    // The value borrowed from `self` must be encodable for THIS driver.
    // XepakValue: sqlx::Encode<'q, DB> + sqlx::Type<DB>;
}
