//! Module provides unified set of types for storage and response/request arguments.
//! The main goals are:
//!  - project any DB type to valid output type (JSON/CBOR)
//!  - project input to compatible DB type (String -> something)
//!  - have some validation logic for input types only

use std::collections::HashMap;

use serde::Serialize;
use sqlx::{TypeInfo, ValueRef as _};

use crate::XepakError;

/// Record representation from storage
pub type Record = HashMap<String, XepakValue>;

/// A workaround to fix rust error: `try_from` has an incompatible type for trait.
pub struct SqlxValue<'r>(pub sqlx::any::AnyValueRef<'r>);

impl<'r> SqlxValue<'r> {
    pub fn new(value: sqlx::any::AnyValueRef<'r>) -> Self {
        Self(value)
    }
}

/// Represents unified type that is matched with a proper [`XepakValueWrapper`].
pub enum XepakType {
    Null,
    Integer,
    Float,
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
    /// Any float type
    Float(f64),
    /// Any text type: TEXT, VARCHAR, etc.
    /// It is default type for de/serialization of any unknown data.
    Text(String),
    //TODO add BLOB
}

impl XepakValue {
    /// Returns type associated with a current wrapped value.
    pub fn get_type(&self) -> XepakType {
        match self {
            Self::Null => XepakType::Null,
            Self::Integer(_) => XepakType::Integer,
            Self::Float(_) => XepakType::Float,
            Self::Text(_) => XepakType::Text,
        }
    }

    pub fn from_str_as(v: &str, parse_as: XepakType) -> Result<Self, XepakError> {
        let xv = match parse_as {
            XepakType::Null => Self::Null,
            XepakType::Integer => {
                let parsed = v.parse().map_err(|e| XepakError::Decode(format!("{e}")))?;
                Self::Integer(parsed)
            }
            XepakType::Float => {
                let parsed = v.parse().map_err(|e| XepakError::Decode(format!("{e}")))?;
                Self::Float(parsed)
            }
            XepakType::Text => Self::Text(v.to_string()),
        };
        Ok(xv)
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
            // TODO add BLOB
            "REAL" => {
                // TODO handle unsigned integers better
                let v: f64 = sqlx::Decode::<sqlx::Any>::decode(value)?;
                Self::Float(v)
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

impl Serialize for XepakValue {
    fn serialize<S>(&self, ser: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            XepakValue::Null => ser.serialize_none(),
            XepakValue::Integer(v) => ser.serialize_i128(*v),
            XepakValue::Float(v) => ser.serialize_f64(*v),
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
            XepakValue::Null => e.null()?,
            // TODO deal with possible unsigned integers here
            XepakValue::Integer(v) => e.encode(*v as i64)?,
            XepakValue::Float(v) => e.encode(v)?,
            XepakValue::Text(v) => e.encode(v)?,
        };
        Ok(())
    }
}
