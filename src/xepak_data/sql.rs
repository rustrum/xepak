use sqlx::{Database, Decode, Encode, Type, TypeInfo, ValueRef};

use super::XepakValue;

impl XepakValue {
    pub fn bind_sqlx<'a, DB>(
        &self,
        // query: sqlx::query::Query<'a, sqlx::Any, sqlx::any::AnyArguments>,
        query: sqlx::query::Query<'a, DB, <DB as sqlx::Database>::Arguments>,
        // ) -> sqlx::query::Query<'a, sqlx::Any, sqlx::any::AnyArguments>
    ) -> sqlx::query::Query<'a, DB, <DB as sqlx::Database>::Arguments>
    where
        DB: sqlx::Database,
        Option<String>: Encode<'a, DB> + Type<DB>,
        f64: Type<DB>,
        String: Encode<'a, DB> + Type<DB>,
        bool: Encode<'a, DB> + Type<DB>,
        i64: Encode<'a, DB> + Type<DB>,
        f64: Encode<'a, DB> + Type<DB>,
        String: Encode<'a, DB> + Type<DB>,
        Vec<u8>: Encode<'a, DB> + Type<DB>,
    {
        match self {
            XepakValue::Null => query.bind(None::<String>),
            XepakValue::Boolean(v) => query.bind(*v),
            XepakValue::Integer(v) => query.bind(*v as i64),
            XepakValue::Float(v) => query.bind(*v),
            XepakValue::Text(v) => query.bind(v.clone()),
            XepakValue::Blob(v) => query.bind(v.clone()),
            XepakValue::Tuple(_) | XepakValue::Dict(_) => query.bind(self.as_string()),
        }
    }

    /// Not sure that it will provide some optimizations but anyway.
    // pub fn bind_sql_move<'a>(
    //     self,
    //     query: sqlx::query::Query<'a, sqlx::Any, sqlx::any::AnyArguments>,
    // ) -> sqlx::query::Query<'a, sqlx::Any, sqlx::any::AnyArguments> {
    pub fn bind_sql_move<'q, DB>(
        self,
        query: sqlx::query::Query<'q, DB, <DB as sqlx::Database>::Arguments>,
    ) -> sqlx::query::Query<'q, DB, <DB as sqlx::Database>::Arguments>
    where
        DB: sqlx::Database,
        String: Encode<'q, DB>,
        bool: Encode<'q, DB> + Type<DB>,
        i64: Encode<'q, DB> + Type<DB>,
        f64: Encode<'q, DB> + Type<DB>,
        String: Encode<'q, DB> + Type<DB>,
        Vec<u8>: Encode<'q, DB> + Type<DB>,
        Option<String>: Encode<'q, DB>,
    {
        match self {
            XepakValue::Null => query.bind(None::<String>),
            XepakValue::Boolean(v) => query.bind(v),
            XepakValue::Integer(v) => query.bind(v as i64),
            XepakValue::Float(v) => query.bind(v),
            XepakValue::Text(v) => query.bind(v),
            XepakValue::Blob(v) => query.bind(v),
            XepakValue::Tuple(_) | XepakValue::Dict(_) => query.bind(self.as_string()),
        }
    }
}

// pub struct SqlxValue<'r>(pub sqlx::any::AnyValueRef<'r>);
/// A workaround to fix rust error: `try_from` has an incompatible type for trait.
pub struct SqlxValue<'r, DB: Database>(pub <DB as Database>::ValueRef<'r>);

impl<'r, DB: Database> SqlxValue<'r, DB> {
    pub fn new(value: <DB as Database>::ValueRef<'r>) -> Self {
        Self(value)
    }
}

impl<'r, DB: Database> TryFrom<SqlxValue<'r, DB>> for XepakValue
where
    Option<bool>: Decode<'r, DB> + Type<DB>,
    String: Decode<'r, DB> + Type<DB>,
    bool: Decode<'r, DB> + Type<DB>,
    i64: Decode<'r, DB> + Type<DB>,
    f64: Decode<'r, DB> + Type<DB>,
    String: Decode<'r, DB> + Type<DB>,
    Vec<u8>: Decode<'r, DB> + Type<DB>,
{
    type Error = sqlx::error::BoxDynError;

    fn try_from(vw: SqlxValue<'r, DB>) -> Result<Self, Self::Error> {
        let value = vw.0;

        // 1) NULL first. Ask the value's storage class - not `name()`,
        if value.is_null() {
            return Ok(Self::Null);
        }

        // 2) Type-name dispatch covering Postgres, MySQL, SQLite storage classes.
        let res = match value.type_info().name() {
            // Integers
            // Postgres: int2/int4/int8; MySQL: tinyint..bigint; SQLite: INTEGER
            "INTEGER" | "INT" | "INT2" | "INT4" | "INT8" | "TINYINT" | "SMALLINT" | "MEDIUMINT"
            | "BIGINT" => {
                let v: i64 = Decode::<DB>::decode(value)?;
                Self::Integer(v as i128)
            }
            // Floats
            // Postgres: float4/float8; MySQL: FLOAT/DOUBLE; SQLite: REAL
            "REAL" | "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE" => {
                let v: f64 = Decode::<DB>::decode(value)?;
                Self::Float(v)
            }
            // Binary
            // Postgres: bytea; MySQL: varbinary / *blob; SQLite: BLOB
            "BLOB" | "BYTEA" | "VARBINARY" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
                let b: Vec<u8> = Decode::<DB>::decode(value)?;
                Self::Blob(b)
            }
            // Everything else -> Text (TEXT, VARCHAR, CHAR, DATETIME, DATE, TIME,
            // NUMERIC, JSON, UUID, …). SQLite reports DATETIME columns that hold
            // text as "TEXT" storage-class, so they land here correctly.
            _ => Self::Text(Decode::<DB>::decode(value)?),
        };

        Ok(res)
    }
}

/*
    fn try_from(vw: SqlxValue<'r, DB>) -> Result<Self, Self::Error> {
        let value = vw.0;

        // Use the Database's TypeInfo to check column type names
        let type_info = value.type_info();

        // Maybe use type_info.type_compatible(other)
        let res = match type_info.name() {
            "NULL" => Self::Null,
            "INTEGER" | "INT" | "BIGINT" => {
                // TODO handle unsigned integers better
                let v: i64 = sqlx::Decode::<DB>::decode(value)?;
                Self::Integer(v as i128)
            }
            "REAL" | "DOUBLE" => {
                // TODO handle unsigned integers better
                let v: f64 = sqlx::Decode::<DB>::decode(value)?;
                Self::Float(v)
            }
            "BLOB" => {
                let b: Vec<u8> = sqlx::Decode::<DB>::decode(value)?;
                Self::Blob(b)
            }
            _ => Self::Text(sqlx::Decode::<DB>::decode(value)?),
        };

        Ok(res)
    }
SQLITE
            DataType::Null => "NULL",
            DataType::Text => "TEXT",
            DataType::Float => "REAL",
            DataType::Blob => "BLOB",
            DataType::Int4 | DataType::Integer => "INTEGER",
            DataType::Numeric => "NUMERIC",

            // non-standard extensions
            DataType::Bool => "BOOLEAN",
            DataType::Date => "DATE",
            DataType::Time => "TIME",
            DataType::Datetime => "DATETIME",
*/
