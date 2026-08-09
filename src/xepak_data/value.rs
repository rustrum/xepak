use base64::Engine;
use std::collections::HashMap;
use strum::Display;

use super::XepakDataError;
use super::XepakType;

/// Unified value wrapper for input/output (IDK a better solution than using enum yet).
///
/// It should be able to serialize into proper JSON/CBOR representation.
/// Plus it must be compatible with sqlx type system to be used as a query argument.
///
/// Deserialization will be a little bit tricky
#[derive(Debug, Clone, Display)]
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
    /// Some kind of a binary blob
    Blob(Vec<u8>),

    Tuple(Vec<XepakValue>),

    Map(HashMap<String, XepakValue>),
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
            Self::Blob(_) => XepakType::Blob,
            Self::Tuple(_) => XepakType::Tuple,
            Self::Map(_) => XepakType::Map,
        }
    }

    pub fn is_null(&self) -> bool {
        if let Self::Null = self { true } else { false }
    }

    pub fn from_str_as(v: &str, parse_as: XepakType) -> Result<Self, XepakDataError> {
        let xv = match parse_as {
            XepakType::Null => Self::Null,
            XepakType::Boolean => {
                let parsed = v
                    .parse()
                    .map_err(|e| XepakDataError::Decode(format!("{e}")))?;
                Self::Boolean(parsed)
            }
            XepakType::Int => {
                let parsed = v
                    .parse()
                    .map_err(|e| XepakDataError::Decode(format!("{e}")))?;
                Self::Integer(parsed)
            }
            XepakType::Float => {
                let parsed = v
                    .parse()
                    .map_err(|e| XepakDataError::Decode(format!("{e}")))?;
                Self::Float(parsed)
            }
            XepakType::Text => Self::Text(v.to_string()),
            XepakType::Blob => unimplemented!(),
            XepakType::Tuple => unimplemented!(),
            XepakType::Map => unimplemented!(),
        };
        Ok(xv)
    }

    pub fn as_int(&self) -> Result<i128, XepakDataError> {
        const TO_TYPE: XepakType = XepakType::Int;
        match self {
            XepakValue::Null | XepakValue::Blob(_) | XepakValue::Tuple(_) | XepakValue::Map(_) => {
                Err(XepakDataError::ConvertValue(
                    self.get_type(),
                    TO_TYPE,
                    "Not possible".to_string(),
                ))
            }
            XepakValue::Boolean(v) => Ok(if *v { 1 } else { 0 }),
            XepakValue::Integer(v) => Ok(*v),
            XepakValue::Float(v) => {
                if v.fract().abs() > f64::EPSILON {
                    Err(XepakDataError::ConvertValue(
                        self.get_type(),
                        TO_TYPE,
                        format!("Has fractional part {v}"),
                    ))
                } else if *v > i64::MAX as f64 || *v < i64::MIN as f64 {
                    Err(XepakDataError::ConvertValue(
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
                    XepakDataError::ConvertValue(self.get_type(), TO_TYPE, format!("{e}"))
                })?;

                Ok(parsed)
            }
        }
    }

    pub fn as_string(&self) -> String {
        match self {
            XepakValue::Null => "".to_string(),
            XepakValue::Boolean(v) => v.to_string(),
            XepakValue::Integer(v) => v.to_string(),
            XepakValue::Float(v) => v.to_string(),
            XepakValue::Text(v) => v.clone(),
            XepakValue::Blob(v) => base64::engine::general_purpose::STANDARD.encode(v),
            XepakValue::Tuple(v) => serde_json::to_string(v).unwrap_or_else(|_| "[]".to_string()),
            XepakValue::Map(v) => serde_json::to_string(v).unwrap_or_else(|_| "{}".to_string()),
        }
    }

    pub fn as_bool(&self) -> Result<bool, XepakDataError> {
        const TO_TYPE: XepakType = XepakType::Boolean;
        Ok(match self {
            XepakValue::Null | XepakValue::Blob(_) | XepakValue::Tuple(_) | XepakValue::Map(_) => {
                return Err(XepakDataError::ConvertValue(
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
                    return Err(XepakDataError::ConvertValue(
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
                    return Err(XepakDataError::ConvertValue(
                        self.get_type(),
                        TO_TYPE,
                        format!("Float value {} can't be a boolean", v),
                    ));
                }
            }
            XepakValue::Text(v) => v
                .parse()
                .map_err(|e| XepakDataError::Decode(format!("{e}")))?,
        })
    }

    pub fn as_float(&self) -> Result<f64, XepakDataError> {
        const TO_TYPE: XepakType = XepakType::Float;
        Ok(match self {
            XepakValue::Null | XepakValue::Blob(_) | XepakValue::Tuple(_) | XepakValue::Map(_) => {
                return Err(XepakDataError::ConvertValue(
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
                    return Err(XepakDataError::ConvertValue(
                        self.get_type(),
                        TO_TYPE,
                        format!("Out of range value {v}"),
                    ));
                } else {
                    *v as f64
                }
            }
            XepakValue::Float(v) => *v,
            XepakValue::Text(v) => v
                .parse()
                .map_err(|e| XepakDataError::Decode(format!("{e}")))?,
        })
    }

    pub fn as_blob(&self) -> Result<Vec<u8>, XepakDataError> {
        const TO_TYPE: XepakType = XepakType::Blob;
        Ok(match self {
            XepakValue::Null => vec![],
            XepakValue::Blob(v) => v.clone(),
            XepakValue::Text(v) => {
                let value = base64::engine::general_purpose::STANDARD
                    .decode(v)
                    .map_err(|e| XepakDataError::Decode(format!("{e}")))?;
                value
            }
            _ => {
                return Err(XepakDataError::ConvertValue(
                    self.get_type(),
                    TO_TYPE,
                    "Not possible".to_string(),
                ));
            }
        })
    }

    pub fn as_tuple(&self) -> Result<Vec<XepakValue>, XepakDataError> {
        const TO_TYPE: XepakType = XepakType::Blob;
        Ok(match self {
            XepakValue::Null => Vec::new(),
            XepakValue::Tuple(v) => v.clone(),
            _ => {
                return Err(XepakDataError::ConvertValue(
                    self.get_type(),
                    TO_TYPE,
                    "Not possible".to_string(),
                ));
            }
        })
    }

    pub fn as_map(&self) -> Result<HashMap<String, XepakValue>, XepakDataError> {
        const TO_TYPE: XepakType = XepakType::Blob;
        Ok(match self {
            XepakValue::Null => HashMap::new(),
            XepakValue::Map(v) => v.clone(),
            _ => {
                return Err(XepakDataError::ConvertValue(
                    self.get_type(),
                    TO_TYPE,
                    "Not possible".to_string(),
                ));
            }
        })
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

impl From<HashMap<String, XepakValue>> for XepakValue {
    fn from(value: HashMap<String, XepakValue>) -> Self {
        Self::Map(value)
    }
}

impl From<Vec<XepakValue>> for XepakValue {
    fn from(value: Vec<XepakValue>) -> Self {
        Self::Tuple(value)
    }
}
