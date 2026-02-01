//! Module provides unified set of types for storage and response/request arguments.
//! The main goals are:
//!  - project any DB type to valid output type (JSON/CBOR)
//!  - project input to compatible DB type (String -> something)
//!  - have some validation logic for input types only

use std::{borrow::Cow, collections::HashMap};

use sqlx::{TypeInfo, ValueRef as _, encode::IsNull};
use sqlx_core::any::AnyValueKind;
use strum::Display;

use crate::XepakError;

/// Schema for input/output arguments
pub type Schema = HashMap<String, ArgSchema>;

/// Record representation from storage
pub type Record = HashMap<String, XepakValue>;

#[derive(Debug)]
pub struct ArgSchema {}

/// A workaround to fix rust error: `try_from` has an incompatible type for trait.
pub struct SqlxValue<'r>(pub sqlx::any::AnyValueRef<'r>);

impl<'r> SqlxValue<'r> {
    pub fn new(value: sqlx::any::AnyValueRef<'r>) -> Self {
        Self(value)
    }
}

/// Represents unified type that is matched with a proper [`XepakValueWrapper`].
#[derive(Clone, Copy, Display, Debug)]
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
#[derive(Debug, Clone)]
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

    pub fn as_integer(&self) -> Result<i128, XepakError> {
        const TO_TYPE: XepakType = XepakType::Integer;
        match self {
            XepakValue::Null => Err(XepakError::Convert(
                self.get_type(),
                TO_TYPE,
                "Not possible".to_string(),
            )),
            XepakValue::Integer(v) => Ok(*v),
            XepakValue::Float(v) => {
                if v.fract().abs() > f64::EPSILON {
                    Err(XepakError::Convert(
                        self.get_type(),
                        TO_TYPE,
                        format!("Has fractional part {v}"),
                    ))
                } else if *v > i64::MAX as f64 || *v < i64::MIN as f64 {
                    Err(XepakError::Convert(
                        self.get_type(),
                        TO_TYPE,
                        format!("Out of range {v}"),
                    ))
                } else {
                    Ok(unsafe { v.to_int_unchecked() })
                }
            }
            XepakValue::Text(v) => {
                let parsed = v
                    .parse()
                    .map_err(|e| XepakError::Convert(self.get_type(), TO_TYPE, format!("{e}")))?;

                Ok(parsed)
            }
        }
    }

    pub fn bind_sqlx<'a>(
        &'a self,
        query: sqlx::query::Query<'a, sqlx::Any, sqlx::any::AnyArguments<'a>>,
    ) -> sqlx::query::Query<'a, sqlx::Any, sqlx::any::AnyArguments<'a>> {
        match self {
            XepakValue::Null => query.bind(None::<String>),
            XepakValue::Integer(v) => query.bind(*v as i64),
            XepakValue::Float(v) => query.bind(v),
            XepakValue::Text(v) => query.bind(v),
        }
    }
}

impl From<&str> for XepakValue {
    fn from(value: &str) -> Self {
        Self::Text(value.to_string())
    }
}

impl From<String> for XepakValue {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<f64> for XepakValue {
    fn from(value: f64) -> Self {
        Self::Float(value)
    }
}

impl From<i128> for XepakValue {
    fn from(value: i128) -> Self {
        Self::Integer(value)
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

impl sqlx::Encode<'_, sqlx::Any> for XepakValue {
    fn encode_by_ref(
        &self,
        buf: &mut <sqlx::Any as sqlx::Database>::ArgumentBuffer<'_>,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        let res = match self {
            XepakValue::Null => IsNull::Yes,
            XepakValue::Integer(v) => {
                buf.0.push(AnyValueKind::BigInt(*v as i64));
                IsNull::No
            }
            XepakValue::Float(v) => {
                buf.0.push(AnyValueKind::Double(*v));
                IsNull::No
            }
            XepakValue::Text(v) => {
                buf.0.push(AnyValueKind::Text(Cow::Owned(v.clone())));
                IsNull::No
            }
        };

        Ok(res)
    }
}

impl<'de> serde::Deserialize<'de> for XepakValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;

        Ok(XepakValue::Text(value))
    }
}

impl serde::Serialize for XepakValue {
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
