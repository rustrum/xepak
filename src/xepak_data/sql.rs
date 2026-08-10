use sqlx::{TypeInfo, ValueRef};

use super::XepakValue;

impl XepakValue {
    pub fn bind_sqlx<'a>(
        self,
        query: sqlx::query::Query<'a, sqlx::Any, sqlx::any::AnyArguments>,
    ) -> sqlx::query::Query<'a, sqlx::Any, sqlx::any::AnyArguments> {
        match &self {
            XepakValue::Null => query.bind(None::<String>),
            XepakValue::Boolean(v) => query.bind(*v),
            XepakValue::Integer(v) => query.bind(*v as i64),
            XepakValue::Float(v) => query.bind(*v),
            XepakValue::Text(v) => query.bind(v.clone()),
            XepakValue::Blob(v) => query.bind(v.clone()),
            XepakValue::Tuple(_) | XepakValue::Map(_) => query.bind(self.as_string()),
        }
    }
}

/// A workaround to fix rust error: `try_from` has an incompatible type for trait.
pub struct SqlxValue<'r>(pub sqlx::any::AnyValueRef<'r>);

impl<'r> SqlxValue<'r> {
    pub fn new(value: sqlx::any::AnyValueRef<'r>) -> Self {
        Self(value)
    }
}

impl<'r> TryFrom<SqlxValue<'r>> for XepakValue {
    type Error = sqlx::error::BoxDynError;

    fn try_from(vw: SqlxValue<'r>) -> Result<Self, Self::Error> {
        let value = vw.0;

        // Use the Database's TypeInfo to check column type names
        let type_info = value.type_info();

        //Maybe use type_info.type_compatible(other)
        let res = match type_info.name() {
            "NULL" => Self::Null,
            "INTEGER" | "INT" | "BIGINT" => {
                // TODO handle unsigned integers better
                let v: i64 = sqlx::Decode::<sqlx::Any>::decode(value)?;
                Self::Integer(v as i128)
            }
            "REAL" | "DOUBLE" => {
                // TODO handle unsigned integers better
                let v: f64 = sqlx::Decode::<sqlx::Any>::decode(value)?;
                Self::Float(v)
            }
            "BLOB" => {
                let b: Vec<u8> = sqlx::Decode::<sqlx::Any>::decode(value)?;
                Self::Blob(b)
            }
            _ => Self::Text(sqlx::Decode::<sqlx::Any>::decode(value)?),
        };

        Ok(res)
    }
}

/*
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
