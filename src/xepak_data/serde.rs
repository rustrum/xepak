use std::collections::HashMap;

use base64::Engine as _i;
use serde::de::Visitor;
use serde::ser::{SerializeMap as _, SerializeSeq as _};

use super::XepakDataError;
use super::XepakValue;

impl XepakValue {
    pub fn to_json(&self) -> Result<String, XepakDataError> {
        serde_json::to_string(self).map_err(|err| {
            XepakDataError::Decode(format!("Fail to encode XepakValue into CBOR vec {err}"))
        })
    }

    pub fn to_cbor_vec(&self) -> Result<Vec<u8>, XepakDataError> {
        cbor2::to_vec(self).map_err(|err| {
            XepakDataError::Decode(format!("Fail to encode XepakValue into CBOR vec {err}"))
        })
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
                if ser.is_human_readable() {
                    // In JSON serialise it as b64 string
                    let str_value = base64::engine::general_purpose::STANDARD.encode(v);
                    ser.serialize_str(str_value.as_str())
                } else {
                    ser.serialize_bytes(v)
                }
            }
            XepakValue::Map(v) => {
                // let non_null_keys = v.iter().filter(|(_, v)| !v.is_null()).count();
                // let mut map = ser.serialize_map(Some(non_null_keys))?;
                let mut map = ser.serialize_map(Some(v.len()))?;
                for (k, v) in v {
                    // if !v.is_null() {
                    // Serialize only non null keys
                    map.serialize_entry(k, v)?;
                    // }
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

struct DeserializeVisitor;

impl<'de> Visitor<'de> for DeserializeVisitor {
    type Value = XepakValue;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str(
            "any XepakValue: null, boolean, integer, float, text, bytes, sequence or map",
        )
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(XepakValue::Null)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(XepakValue::Null)
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(XepakValue::Boolean(v))
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_i128(v as i128)
    }

    fn visit_i128<E>(self, v: i128) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(XepakValue::Integer(v))
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_u128(v as u128)
    }

    fn visit_u128<E>(self, v: u128) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v > i128::MAX as u128 {
            return Err(serde::de::Error::custom(format!(
                "Unsigned value {v} is out of i128 range"
            )));
        }
        Ok(XepakValue::Integer(v as i128))
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(XepakValue::Float(v))
    }

    fn visit_char<E>(self, v: char) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_string(v.to_string())
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_string(v.to_string())
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(XepakValue::Text(v))
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(XepakValue::Blob(v.to_vec()))
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(XepakValue::Blob(v))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(seq.size_hint().unwrap_or(0));
        while let Some(element) = seq.next_element()? {
            values.push(element);
        }
        Ok(XepakValue::Tuple(values))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut values = HashMap::with_capacity(map.size_hint().unwrap_or(0));
        while let Some((key, value)) = map.next_entry()? {
            values.insert(key, value);
        }
        Ok(XepakValue::Map(values))
    }
}

impl<'de> serde::Deserialize<'de> for XepakValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(DeserializeVisitor)
    }
}

#[cfg(test)]
mod tests {
    use crate::xepak_data::XepakType;

    use super::*;
    use maplit::hashmap;

    fn dict_all_types() -> XepakValue {
        XepakValue::Map(hashmap! {
            "null".to_string() => XepakValue::Null,
            "bool".to_string() => XepakValue::Boolean(true),
            "int".to_string() => XepakValue::Integer(42),
            "float".to_string() => XepakValue::Float(3.42),
            "text".to_string() => XepakValue::Text("hello".to_string()),
            "blob".to_string() => XepakValue::Blob(vec![0u8, 1, 2, 3]),
            "tuple".to_string() => XepakValue::Tuple(vec![
                XepakValue::Integer(1),
                XepakValue::Integer(2),
            ]),
            "map".to_string() => XepakValue::Map(
                hashmap! {
                    "a".to_string() => XepakValue::Integer(1),
                    "b".to_string() => XepakValue::Float(1.12),
                    "c".to_string() => XepakValue::Boolean(true),
                }
            )
        })
    }

    #[test]
    fn json_decode_encode_consistency() {
        let value = dict_all_types();
        let expected = value.clone();

        let json = value.to_json().unwrap();
        let mut decoded: XepakValue = serde_json::from_str(&json).unwrap();

        // Blob serializes as a base64 string, so it can't roundtrip as Blob via JSON
        if let XepakValue::Map(map) = &mut decoded {
            map.entry("blob".to_string())
                .and_modify(|e| *e = e.to_type(XepakType::Blob).expect("Must convert to blob"));
        }
        assert_eq!(decoded, expected);

        // Just check same for root tuple lement
        let tuple = XepakValue::Tuple(vec![
            XepakValue::Null,
            XepakValue::Integer(1),
            XepakValue::Float(2.5),
            XepakValue::Text("three".to_string()),
            XepakValue::Boolean(false),
        ]);
        let json = tuple.to_json().unwrap();
        let decoded: XepakValue = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, tuple);
    }

    #[test]
    fn cbor_decode_encode_consistency() {
        let value = dict_all_types();

        let bytes = value.to_cbor_vec().unwrap();
        let decoded: XepakValue = cbor2::from_slice(&bytes).unwrap();
        assert_eq!(decoded, value);

        // Just check same for root tuple lement
        let tuple = XepakValue::Tuple(vec![
            XepakValue::Null,
            XepakValue::Integer(1),
            XepakValue::Float(2.5),
            XepakValue::Text("three".to_string()),
            XepakValue::Boolean(false),
        ]);
        let bytes = tuple.to_cbor_vec().unwrap();
        let decoded: XepakValue = cbor2::from_slice(&bytes).unwrap();
        assert_eq!(decoded, tuple);
    }

    #[test]
    fn deserialize_values_with_lower_byte_size_into_wider_types() {
        // smaller integers (i8..i64, u8..u64) must fit into XepakValue::Integer
        let cases = [
            (
                XepakValue::Integer(i8::MIN as i128),
                serde_json::json!(i8::MIN),
            ),
            (
                XepakValue::Integer(i8::MAX as i128),
                serde_json::json!(i8::MAX),
            ),
            (
                XepakValue::Integer(i16::MIN as i128),
                serde_json::json!(i16::MIN),
            ),
            (
                XepakValue::Integer(i16::MAX as i128),
                serde_json::json!(i16::MAX),
            ),
            (
                XepakValue::Integer(i32::MIN as i128),
                serde_json::json!(i32::MIN),
            ),
            (
                XepakValue::Integer(i32::MAX as i128),
                serde_json::json!(i32::MAX),
            ),
            (
                XepakValue::Integer(i64::MAX as i128),
                serde_json::json!(i64::MAX),
            ),
            (
                XepakValue::Integer(u8::MAX as i128),
                serde_json::json!(u8::MAX),
            ),
            (
                XepakValue::Integer(u16::MAX as i128),
                serde_json::json!(u16::MAX),
            ),
            (
                XepakValue::Integer(u32::MAX as i128),
                serde_json::json!(u32::MAX),
            ),
            (
                XepakValue::Integer(u64::MAX as i128),
                serde_json::json!(u64::MAX),
            ),
        ];
        for (expected, json_value) in cases {
            let json = json_value.to_string();
            let decoded: XepakValue = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded, expected, "failed to decode {json}");
        }

        // i128 extreme values don't roundtrip through serde_json (no arbitrary precision),
        // but they do through CBOR
        for big in [i128::MAX, i128::MIN, u64::MAX as i128] {
            let value = XepakValue::Integer(big);
            let bytes = value.to_cbor_vec().unwrap();
            let decoded: XepakValue = cbor2::from_slice(&bytes).unwrap();
            assert_eq!(decoded, value, "failed to decode {big}");
        }
    }
}
