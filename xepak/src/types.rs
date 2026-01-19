//! Module provides unified set of types for storage and response/request arguments.
//! The main goals are:
//!  - project any DB type to valid output type (JSON/CBOR)
//!  - project input to compatible DB type (String -> something)
//!  - have some validation logic for input types only

use std::{collections::HashMap, marker::PhantomData};

use serde::Serialize;
use sqlx::{TypeInfo, ValueRef as _};

/// Record representation from storage
pub type Record = HashMap<String, XepakValue>;

/// A workaround to fix rust error: `try_from` has an incompatible type for trait.
pub struct SqlxValue<'r, DB: sqlx::Database>(pub DB::ValueRef<'r>, pub PhantomData<DB>);

impl<'r, DB: sqlx::Database> SqlxValue<'r, DB> {
    pub fn new(value: DB::ValueRef<'r>) -> Self {
        Self(value, PhantomData)
    }
}

/// Represents unified type that is matched with a proper [`XepakValueWrapper`].
pub enum XepakType {
    Null,
    Integer,
    Text,
}

/// Unified value wrapper for input/output (IDK a better solution than using enum yet).
///
/// It should be able to serialize into proper JSON/CBOR representation.
/// Plus it must be compatible with sqlx type system to be used as a query argument.
///
/// Deserialization will be a little bit tricky
#[derive(Debug)]
pub enum XepakValue {
    /// Null/nothing/undefined type
    Null,
    /// Any integer type
    Integer(i128),
    /// Any text type: TEXT, VARCHAR, etc.
    /// It is default type for de/serialization of any unknown data.
    Text(String),
}

impl XepakValue {
    /// Returns type associated with a current wrapped value.
    pub fn get_type(&self) -> XepakType {
        match self {
            Self::Null => XepakType::Null,
            Self::Integer(_) => XepakType::Integer,
            Self::Text(_) => XepakType::Text,
        }
    }
}

impl<'r, DB: sqlx::Database> TryFrom<SqlxValue<'r, DB>> for XepakValue
where
    for<'a> i64: sqlx::Decode<'a, DB> + sqlx::Type<DB>,
    for<'a> String: sqlx::Decode<'a, DB> + sqlx::Type<DB>,
{
    type Error = sqlx::error::BoxDynError;

    fn try_from(vw: SqlxValue<'r, DB>) -> Result<Self, Self::Error> {
        let value = vw.0;

        // Use the Database's TypeInfo to check column type names
        let type_info = value.type_info();

        let res = match type_info.name() {
            "NULL" => Self::Null,
            "INTEGER" | "INT" | "BIGINT" => {
                // TODO handle unsigned integers better
                let v: i64 = sqlx::Decode::decode(value)?;
                Self::Integer(v as i128)
            }
            _ => Self::Text(sqlx::Decode::decode(value)?),
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

impl Serialize for XepakValue {
    fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            XepakValue::Null => ser.serialize_none(),
            XepakValue::Integer(v) => ser.serialize_i128(*v),
            XepakValue::Text(v) => ser.serialize_str(v.as_str()),
        }
    }
}

impl minicbor::Encode<()> for XepakValue {
    fn encode<W: minicbor::encode::Write>(
        &self,
        e: &mut minicbor::Encoder<W>,
        _ctx: &mut (),
    ) -> Result<(), minicbor::encode::Error<W::Error>> {
        match self {
            XepakValue::Null => e.null(),
            XepakValue::Integer(v) => e.encode(*v as i64),
            XepakValue::Text(v) => e.encode(v),
        };
        Ok(())
    }
}
