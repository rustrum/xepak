use base64::Engine as _i;
use serde::ser::{SerializeMap as _, SerializeSeq as _};

use super::XepakDataError;
use super::XepakValue;

impl XepakValue {
    pub fn to_json(&self) -> Result<String, XepakDataError> {
        serde_json::to_string(self).map_err(|err| {
            XepakDataError::Decode(format!(
                "Fail to encode XepakValue into CBOR vec {}",
                err.to_string()
            ))
        })
    }

    pub fn to_cbor_vec(&self) -> Result<Vec<u8>, XepakDataError> {
        cbor2::to_vec(self).map_err(|err| {
            XepakDataError::Decode(format!(
                "Fail to encode XepakValue into CBOR vec {}",
                err.to_string()
            ))
        })
    }
}
impl<'de> serde::Deserialize<'de> for XepakValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        // It is weird to deserialize everyting as a string.
        // Would not work with other CBOR deserializers
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
            XepakValue::Blob(v) => {
                // By default Blob is saved as b64 string
                let str_value = base64::engine::general_purpose::STANDARD.encode(v);
                ser.serialize_str(str_value.as_str())
            }
            XepakValue::Map(v) => {
                let mut map = ser.serialize_map(Some(v.len()))?;
                for (k, v) in v {
                    map.serialize_entry(k, v)?;
                }
                map.end()
            }
            XepakValue::Tuple(v) => {
                let mut seq = ser.serialize_seq(Some(v.len()))?;
                for element in v {
                    seq.serialize_element(element)?;
                }
                seq.end()
            }
        }
    }
}

impl TryFrom<&serde_json::Value> for XepakValue {
    type Error = XepakDataError;

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
            serde_json::Value::Array(_values) => Err(XepakDataError::Decode(
                "Cant decode from JSON array".to_string(),
            )),
            serde_json::Value::Object(_map) => Err(XepakDataError::Decode(
                "Cant decode from JSON object".to_string(),
            )),
        }
    }
}
