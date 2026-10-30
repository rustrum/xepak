use base64::Engine;
use indexmap::IndexMap;

use super::XepakDataError;
use super::XepakType;

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
    /// Any integer type.
    /// Consider the maximum value is a u64 (i128 is just a container here).
    Integer(i128),
    /// Any float type
    Float(f64),
    /// Any text type: TEXT, VARCHAR, etc.
    /// It is default type for de/serialization of any unknown data.
    Text(String),
    /// Some kind of a binary blob
    Blob(Vec<u8>),

    Tuple(Vec<XepakValue>),

    Dict(IndexMap<String, XepakValue>),
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
            Self::Dict(_) => XepakType::Dict,
        }
    }

    /// Return true for empty dict, tuple or null.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Null => true,
            Self::Tuple(items) => items.is_empty(),
            Self::Dict(items) => items.is_empty(),
            _ => false,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn is_map(&self) -> bool {
        matches!(self, Self::Dict(_))
    }

    pub fn is_tuple(&self) -> bool {
        matches!(self, Self::Tuple(_))
    }

    /// If value is a tuple return its size or None
    pub fn tuple_len(&self) -> Option<usize> {
        if let Self::Tuple(value) = self {
            Some(value.len())
        } else {
            None
        }
    }

    /// If value is a dict returns if it contains given key or false.
    pub fn dict_contains(&self, key: &str) -> bool {
        if let Self::Dict(value) = self {
            value.contains_key(key)
        } else {
            false
        }
    }

    /// If value is a dict returns inner value by key or None for all other cases.
    pub fn dict_get(&self, key: &str) -> Option<&XepakValue> {
        if let Self::Dict(value) = self {
            value.get(key)
        } else {
            None
        }
    }

    /// If value is a tuple returns inner value by index or None for all other cases.
    pub fn tuple_get(&self, id: usize) -> Option<&XepakValue> {
        if let Self::Tuple(value) = self {
            value.get(id)
        } else {
            None
        }
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
            XepakType::Blob => {
                return Err(XepakDataError::Decode(
                    "Cant decode String to Blob".to_string(),
                ));
            }
            XepakType::Tuple => {
                return Err(XepakDataError::Decode(
                    "Cant decode String to Tuple".to_string(),
                ));
            }
            XepakType::Dict => {
                return Err(XepakDataError::Decode(
                    "Cant decode String to Dict".to_string(),
                ));
            }
        };
        Ok(xv)
    }

    pub fn as_int(&self) -> Result<i128, XepakDataError> {
        const TO_TYPE: XepakType = XepakType::Int;
        match self {
            XepakValue::Null | XepakValue::Blob(_) | XepakValue::Tuple(_) | XepakValue::Dict(_) => {
                Err(XepakDataError::ConvertValue(
                    self.get_type(),
                    TO_TYPE,
                    "Not possible".to_string(),
                ))
            }
            XepakValue::Boolean(v) => Ok(if *v { 1 } else { 0 }),
            XepakValue::Integer(v) => Ok(*v),
            XepakValue::Float(v) => {
                if v.is_nan() || v.is_infinite() {
                    return Err(XepakDataError::ConvertValue(
                        self.get_type(),
                        TO_TYPE,
                        format!("Float value is not supported {v}"),
                    ));
                }
                if v.fract() != 0.0 {
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
            XepakValue::Dict(v) => serde_json::to_string(v).unwrap_or_else(|_| "{}".to_string()),
        }
    }

    pub fn as_bool(&self) -> Result<bool, XepakDataError> {
        const TO_TYPE: XepakType = XepakType::Boolean;
        Ok(match self {
            XepakValue::Null | XepakValue::Blob(_) | XepakValue::Tuple(_) | XepakValue::Dict(_) => {
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
            XepakValue::Null | XepakValue::Blob(_) | XepakValue::Tuple(_) | XepakValue::Dict(_) => {
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
            XepakValue::Text(v) => base64::engine::general_purpose::STANDARD
                .decode(v)
                .map_err(|e| XepakDataError::Decode(format!("{e}")))?,
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

    pub fn as_map(&self) -> Result<IndexMap<String, XepakValue>, XepakDataError> {
        const TO_TYPE: XepakType = XepakType::Dict;
        Ok(match self {
            XepakValue::Null => Default::default(),
            XepakValue::Dict(v) => v.clone(),
            _ => {
                return Err(XepakDataError::ConvertValue(
                    self.get_type(),
                    TO_TYPE,
                    "Not possible".to_string(),
                ));
            }
        })
    }

    pub fn to_type(
        &self,
        to_type: XepakType,
        nulls_allowed: bool,
    ) -> Result<XepakValue, XepakDataError> {
        if self.is_null() && nulls_allowed {
            return Ok(Self::Null);
        }

        let value = match to_type {
            XepakType::Null => Self::Null,
            XepakType::Boolean => Self::Boolean(self.as_bool()?),
            XepakType::Int => Self::Integer(self.as_int()?),
            XepakType::Float => Self::Float(self.as_float()?),
            XepakType::Text => Self::Text(self.as_string()),
            XepakType::Blob => Self::Blob(self.as_blob()?),
            XepakType::Tuple => Self::Tuple(self.as_tuple()?),
            XepakType::Dict => Self::Dict(self.as_map()?),
        };
        Ok(value)
    }
}

impl std::fmt::Display for XepakValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.as_string())
    }
}
impl PartialEq for XepakValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Boolean(l0), Self::Boolean(r0)) => l0 == r0,
            (Self::Integer(l0), Self::Integer(r0)) => l0 == r0,
            (Self::Float(l0), Self::Float(r0)) => l0 == r0,
            (Self::Text(l0), Self::Text(r0)) => l0 == r0,
            (Self::Blob(l0), Self::Blob(r0)) => l0 == r0,
            (Self::Tuple(l0), Self::Tuple(r0)) => l0 == r0,
            (Self::Dict(l0), Self::Dict(r0)) => l0 == r0,
            _ => core::mem::discriminant(self) == core::mem::discriminant(other),
        }
    }
}

impl From<bool> for XepakValue {
    fn from(value: bool) -> Self {
        Self::Boolean(value)
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

impl From<usize> for XepakValue {
    fn from(value: usize) -> Self {
        Self::Integer(value as i128)
    }
}

impl From<isize> for XepakValue {
    fn from(value: isize) -> Self {
        Self::Integer(value as i128)
    }
}

impl From<i32> for XepakValue {
    fn from(value: i32) -> Self {
        Self::Integer(value as i128)
    }
}
impl From<i128> for XepakValue {
    fn from(value: i128) -> Self {
        Self::Integer(value)
    }
}

impl From<IndexMap<String, XepakValue>> for XepakValue {
    fn from(value: IndexMap<String, XepakValue>) -> Self {
        Self::Dict(value)
    }
}

// impl From<HashMap<String, XepakValue>> for XepakValue {
//     fn from(value: HashMap<String, XepakValue>) -> Self {
//         Self::Dict(value)
//     }
// }

impl From<Vec<XepakValue>> for XepakValue {
    fn from(value: Vec<XepakValue>) -> Self {
        Self::Tuple(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn as_int_from_f64_error() {
        let value = XepakValue::Float(f64::NAN);
        let result = value.as_int();
        assert!(
            result.is_err(),
            "NaN should not convert to int, got {result:?}"
        );

        let inf = XepakValue::Float(f64::INFINITY);
        let result_inf = inf.as_int();
        assert!(
            result_inf.is_err(),
            "Infinity should not convert to int, got {result_inf:?}"
        );

        let ninf = XepakValue::Float(f64::NEG_INFINITY);
        let result_ninf = ninf.as_int();
        assert!(
            result_ninf.is_err(),
            "Negative Infinity should not convert to int, got {result_ninf:?}"
        );

        // Test various fractional values
        let pi = XepakValue::Float(std::f64::consts::PI);
        let result_pi = pi.as_int();
        assert!(
            result_pi.is_err(),
            "PI should not convert (has fractional part), got {result_pi:?}"
        );

        let zero_point_5 = XepakValue::Float(0.5);
        let result_half = zero_point_5.as_int();
        assert!(
            result_half.is_err(),
            "0.5 should not convert (has fractional part), got {result_half:?}"
        );

        let negative_fractional = XepakValue::Float(-123.456);
        let result_neg = negative_fractional.as_int();
        assert!(
            result_neg.is_err(),
            "-123.456 should not convert (has fractional part), got {result_neg:?}"
        );
    }

    #[test]
    fn as_int_max_min_f64() {
        // Test f64::MAX and f64::MIN (negative) — both should be rejected:
        // 1. Beyond i64 range check
        // 2. They are not exact integers in integer representation sense

        let max_val = XepakValue::Float(f64::MAX);
        let result_max = max_val.as_int();
        assert!(
            result_max.is_err(),
            "f64::MAX should not convert to int, got {result_max:?}"
        );

        let min_val = XepakValue::Float(f64::MIN_POSITIVE); // most negative finite float (~-1.79e308)
        let result_min = min_val.as_int();
        assert!(
            result_min.is_err(),
            "f64 minimum should not convert to int, got {result_min:?}"
        );

        let near_max = XepakValue::Float((i64::MAX as i128) as f64);
        let result_near_max = near_max.as_int();
        assert!(
            result_near_max.is_ok(),
            "f64 within int range should convert, got {result_near_max:?}"
        );

        let near_min = XepakValue::Float((i64::MIN as i128) as f64);
        let result_near_min = near_min.as_int();
        assert!(
            result_near_min.is_ok(),
            "f64 within int range should convert, got {result_near_min:?}"
        );

        // Zero is a valid integer float
        let zero_val = XepakValue::Float(0.0);
        let result_zero = zero_val.as_int();
        assert_eq!(result_zero.unwrap(), 0);

        // Negative zero must be zero in int
        let neg_zero = XepakValue::Float(-0.0_f64);
        let result_zero = neg_zero.as_int();
        assert!(
            matches!(result_zero, Ok(value) if value == 0),
            "Negative zero must convert to zero"
        );
    }
}
