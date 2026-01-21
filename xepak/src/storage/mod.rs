use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;

use crate::XepakError;
use crate::types::{Record, SqlxValue, XepakValue};
use serde::Deserialize;
use sql_key_args::ParametrizedQueryRef;
use sqlx::any::AnyConnectOptions;
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{AnyPool, ConnectOptions, Row};
use sqlx_core::column::Column;

pub const LIMIT_KEY: &str = "-limit-";
pub const OFFSET_KEY: &str = "-offset-";

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

                let aco = AnyConnectOptions::from_str(options.to_url_lossy().as_str())
                    .expect("Query string must be valid but it is not");

                let res = links.insert(
                    id.clone(),
                    Storage {
                        pool: AnyPool::connect_lazy_with(aco),
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
    pool: AnyPool,
}

impl Storage {
    pub async fn execute<RA: SqlxRequestArgs>(
        &self,
        request: ResourceRequest<'_, RA>,
    ) -> Result<Vec<Record>, XepakError> {
        let mut connection = self.pool.acquire().await.unwrap();

        let pquery = ParametrizedQueryRef::new(request.query);

        tracing::debug!("Executing query: {}", pquery.get_query());

        let query = pquery.build_query("?");

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

        let result = sql_query.fetch_all(&mut *connection).await.expect("TODO");

        let mut out = Vec::new();
        for row in result {
            let cols = row.columns();
            let mut out_row = HashMap::new();
            for (idx, c) in cols.iter().enumerate() {
                let col = row.try_get_raw(idx).expect("TODO");
                let cval = XepakValue::try_from(SqlxValue::new(col)).expect("TODO");

                out_row.insert(c.name().to_string(), cval);
            }
            out.push(out_row);
        }

        Ok(out)
    }
}

pub struct ResourceRequest<'a, RA: RequestArgs> {
    args: &'a RA,
    query: &'a str,
}

impl<'a, RA: RequestArgs> ResourceRequest<'a, RA> {
    pub fn new(query: &'a str, args: &'a RA) -> Self {
        Self { args, query }
    }
}

/// Lightweight schema enforcement.
/// If field is not provided in schema it's type should be resolved automatically
pub struct ResourceShema {}

pub struct Resource {
    sql: String,
}

impl Resource {
    // pub async fn request(&self, request: &ResourceRequest) -> Result<Vec<Record>, XepakError> {
    //     unimplemented!()
    // }
}

// impl<'q, DB: Database> Query<'q, DB, <DB as Database>::Arguments<'q>> {
//     /// Bind a value for use with this SQL query.
//     ///
//     /// If the number of times this is called does not match the number of bind parameters that
//     /// appear in the query (`?` for most SQL flavors, `$1 .. $N` for Postgres) then an error
//     /// will be returned when this query is executed.
//     ///
//     /// There is no validation that the value is of the type expected by the query. Most SQL
//     /// flavors will perform type coercion (Postgres will return a database error).
//     ///
//     /// If encoding the value fails, the error is stored and later surfaced when executing the query.
//     pub fn bind<T: 'q + Encode<'q, DB> + Type<DB>>(mut self, value: T) -> Self {

//     pub fn query<DB>(sql: &str) -> Query<'_, DB, <DB as Database>::Arguments<'_>>
// where
//     DB: Database,
pub trait RequestArgs {
    /// Return records fetch limit
    fn get_rows_limit(&self) -> usize;

    /// Return records fetch offset
    fn get_rows_offset(&self) -> usize;
}

/// SQLx related request args bind functionality
pub trait SqlxRequestArgs: RequestArgs {
    fn bind_arg<'a>(
        &'a self,
        arg_name: &str,
        query: sqlx::query::Query<'a, sqlx::Any, sqlx::any::AnyArguments<'a>>,
    ) -> Result<sqlx::query::Query<'a, sqlx::Any, sqlx::any::AnyArguments<'a>>, XepakError>;

    // fn get_offset_limit(&self)
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
