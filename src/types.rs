//! Module provides unified set of types for storage and response/request arguments.
//! The main goals are:
//!  - project any DB type to valid output type (JSON/CBOR)
//!  - project input to compatible DB type (String -> something)
//!  - have some validation logic for input types only

use std::collections::HashMap;

use serde::Deserialize;
use sqlx::{TypeInfo, ValueRef as _};
use strum::Display;

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
#[derive(Display, Debug, Default, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum XepakType {
    /// By default all inputs are handled as text.
    #[default]
    Text,
    /// Null/Unit type when we know that value is null and have no idea what type it is.
    Null,
    Boolean,
    Int,
    Float,
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
    Boolean(bool),
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
            Self::Boolean(_) => XepakType::Boolean,
            Self::Integer(_) => XepakType::Int,
            Self::Float(_) => XepakType::Float,
            Self::Text(_) => XepakType::Text,
        }
    }

    pub fn is_null(&self) -> bool {
        if let Self::Null = self { true } else { false }
    }

    pub fn from_str_as(v: &str, parse_as: XepakType) -> Result<Self, XepakError> {
        let xv = match parse_as {
            XepakType::Null => Self::Null,
            XepakType::Boolean => {
                let parsed = v.parse().map_err(|e| XepakError::Decode(format!("{e}")))?;
                Self::Boolean(parsed)
            }
            XepakType::Int => {
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

    pub fn as_int(&self) -> Result<i128, XepakError> {
        const TO_TYPE: XepakType = XepakType::Int;
        match self {
            XepakValue::Null => Err(XepakError::ConvertValue(
                self.get_type(),
                TO_TYPE,
                "Not possible".to_string(),
            )),
            XepakValue::Boolean(v) => Ok(if *v { 1 } else { 0 }),
            XepakValue::Integer(v) => Ok(*v),
            XepakValue::Float(v) => {
                if v.fract().abs() > f64::EPSILON {
                    Err(XepakError::ConvertValue(
                        self.get_type(),
                        TO_TYPE,
                        format!("Has fractional part {v}"),
                    ))
                } else if *v > i64::MAX as f64 || *v < i64::MIN as f64 {
                    Err(XepakError::ConvertValue(
                        self.get_type(),
                        TO_TYPE,
                        format!("Out of range {v}"),
                    ))
                } else {
                    Ok(unsafe { v.to_int_unchecked() })
                }
            }
            XepakValue::Text(v) => {
                let parsed = v.parse().map_err(|e| {
                    XepakError::ConvertValue(self.get_type(), TO_TYPE, format!("{e}"))
                })?;

                Ok(parsed)
            }
        }
    }

    pub fn as_string(&self) -> Result<String, XepakError> {
        Ok(match self {
            XepakValue::Null => "".to_string(),
            XepakValue::Boolean(v) => v.to_string(),
            XepakValue::Integer(v) => v.to_string(),
            XepakValue::Float(v) => v.to_string(),
            XepakValue::Text(v) => v.clone(),
        })
    }

    pub fn as_bool(&self) -> Result<bool, XepakError> {
        const TO_TYPE: XepakType = XepakType::Boolean;
        Ok(match self {
            XepakValue::Null => {
                return Err(XepakError::ConvertValue(
                    self.get_type(),
                    TO_TYPE,
                    "Not possible".to_string(),
                ));
            }
            XepakValue::Boolean(v) => *v,
            XepakValue::Integer(v) => {
                if *v == 0 {
                    false
                } else if *v == 1 {
                    true
                } else {
                    return Err(XepakError::ConvertValue(
                        self.get_type(),
                        TO_TYPE,
                        format!("Integer value {} can't be a boolean", v),
                    ));
                }
            }
            XepakValue::Float(v) => {
                if *v == 0.0 {
                    false
                } else if *v == 1.0 {
                    true
                } else {
                    return Err(XepakError::ConvertValue(
                        self.get_type(),
                        TO_TYPE,
                        format!("Float value {} can't be a boolean", v),
                    ));
                }
            }
            XepakValue::Text(v) => v.parse().map_err(|e| XepakError::Decode(format!("{e}")))?,
        })
    }
    pub fn as_float(&self) -> Result<f64, XepakError> {
        const TO_TYPE: XepakType = XepakType::Float;
        Ok(match self {
            XepakValue::Null => {
                return Err(XepakError::ConvertValue(
                    self.get_type(),
                    TO_TYPE,
                    "Not possible".to_string(),
                ));
            }
            XepakValue::Boolean(v) => {
                if *v {
                    1.0
                } else {
                    0.0
                }
            }
            XepakValue::Integer(v) => {
                if *v > f64::MAX as i128 || *v < f64::MIN as i128 {
                    return Err(XepakError::ConvertValue(
                        self.get_type(),
                        TO_TYPE,
                        format!("Out of range value {v}"),
                    ));
                } else {
                    *v as f64
                }
            }
            XepakValue::Float(v) => *v,
            XepakValue::Text(v) => v.parse().map_err(|e| XepakError::Decode(format!("{e}")))?,
        })
    }

    pub fn bind_sqlx<'a>(
        &'a self,
        query: sqlx::query::Query<'a, sqlx::Any, sqlx::any::AnyArguments<'a>>,
    ) -> sqlx::query::Query<'a, sqlx::Any, sqlx::any::AnyArguments<'a>> {
        match self {
            XepakValue::Null => query.bind(None::<String>),
            XepakValue::Boolean(v) => query.bind(*v),
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

impl TryFrom<&serde_json::Value> for XepakValue {
    type Error = XepakError;

    fn try_from(value: &serde_json::Value) -> Result<Self, Self::Error> {
        match value {
            serde_json::Value::Null => Ok(XepakValue::Null),
            serde_json::Value::Bool(v) => Ok(XepakValue::Boolean(*v)),
            serde_json::Value::Number(number) => {
                Ok(if number.is_f64() {
                    // Should be valid value here
                    XepakValue::Float(number.as_f64().unwrap_or_default())
                } else {
                    // All non f64 Numbers could be converted to i128
                    XepakValue::Integer(number.as_i128().unwrap_or_default())
                })
            }
            serde_json::Value::String(v) => Ok(XepakValue::Text(v.clone())),
            serde_json::Value::Array(_values) => Err(XepakError::Decode(
                "Cant decode from JSON array".to_string(),
            )),
            serde_json::Value::Object(_map) => Err(XepakError::Decode(
                "Cant decode from JSON object".to_string(),
            )),
        }
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
            "REAL" | "DOUBLE" => {
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
            XepakValue::Boolean(v) => ser.serialize_bool(*v),
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
            XepakValue::Boolean(v) => e.encode(v)?,
            XepakValue::Integer(v) => e.encode(*v as i64)?,
            XepakValue::Float(v) => e.encode(v)?,
            XepakValue::Text(v) => e.encode(v)?,
        };
        Ok(())
    }
}
