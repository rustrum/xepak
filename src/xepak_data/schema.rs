use std::collections::HashMap;

use serde::Deserialize;

use crate::{
    XepakError,
    xepak_data::{XepakDataError, XepakType, XepakValue},
};

/// Schema variant matching [`XepakValue`] variants.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Schema {
    Boolean {
        #[serde(default)]
        required: bool,
        #[serde(default)]
        validate: SchemaValidator,
    },
    Int {
        #[serde(default)]
        required: bool,
        #[serde(default)]
        validate: SchemaValidator,
    },
    Float {
        #[serde(default)]
        required: bool,
        #[serde(default)]
        validate: SchemaValidator,
    },
    Text {
        #[serde(default)]
        required: bool,
        #[serde(default)]
        validate: SchemaValidator,
    },
    Blob {
        #[serde(default)]
        required: bool,
        #[serde(default)]
        validate: SchemaValidator,
    },
    Tuple {
        #[serde(default)]
        items: Vec<Schema>,
        /// If true then we assume that tuple have items that repeats endlessly.
        /// So bascially we will apply items in a loop to all elements of the schema.
        /// It also makes all nested Items required untill last element.
        #[serde(default)]
        items_repeat: bool,
        #[serde(default)]
        required: bool,
        #[serde(default)]
        validate: SchemaValidator,
    },
    Dict {
        items: HashMap<String, Schema>,
        #[serde(default)]
        required: bool,
        #[serde(default)]
        validate: SchemaValidator,
    },
}

impl Default for Schema {
    fn default() -> Self {
        Schema::Dict {
            items: Default::default(),
            required: Default::default(),
            validate: Default::default(),
        }
    }
}

impl Schema {
    /// Returns associated [`XepakType`]
    pub fn linked_type(&self) -> XepakType {
        match self {
            Schema::Boolean { .. } => XepakType::Boolean,
            Schema::Int { .. } => XepakType::Int,
            Schema::Float { .. } => XepakType::Float,
            Schema::Text { .. } => XepakType::Text,
            Schema::Blob { .. } => XepakType::Blob,
            Schema::Tuple { .. } => XepakType::Tuple,
            Schema::Dict { .. } => XepakType::Dict,
        }
    }

    pub fn required(&self) -> bool {
        match self {
            Schema::Boolean { required, .. }
            | Schema::Int { required, .. }
            | Schema::Float { required, .. }
            | Schema::Text { required, .. }
            | Schema::Blob { required, .. }
            | Schema::Tuple { required, .. }
            | Schema::Dict { required, .. } => *required,
        }
    }

    /// True when schema carries no constraints (empty dict or tuple).
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Tuple { items, .. } => items.is_empty(),
            Self::Dict { items, .. } => items.is_empty(),
            _ => false,
        }
    }

    pub fn is_dict(&self) -> bool {
        matches!(self, Self::Dict { .. })
    }

    pub fn get_validator(&self) -> &SchemaValidator {
        match self {
            Schema::Boolean { validate, .. } => validate,
            Schema::Int { validate, .. } => validate,
            Schema::Float { validate, .. } => validate,
            Schema::Text { validate, .. } => validate,
            Schema::Blob { validate, .. } => validate,
            Schema::Tuple { validate, .. } => validate,
            Schema::Dict { validate, .. } => validate,
        }
    }

    /// Kinda workaround to return argument based schema (top level dict).
    /// TODO: maybe deprecate it later but it will require RequestInput refactoring.
    pub fn get_arg_schema(&self, key: &str) -> Option<&Self> {
        if let Self::Dict { items, .. } = self {
            items.get(key)
        } else {
            None
        }
    }

    pub fn get_arg_schema_strict(&self, key: &str) -> Result<&Self, XepakError> {
        if let Some(sch) = self.get_arg_schema(key) {
            Ok(sch)
        } else {
            Err(XepakError::Input(format!(
                "Input argument \"{key}\" not allowed in this context!"
            )))
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SchemaValidator {
    NotNull,

    /// Validate integer/float or text length by range
    Range {
        from: usize,
        to: usize,
    },

    /// Validate float by range defined in floats (applied also to int and text for compatibility)
    RangeFloat {
        from: f64,
        to: f64,
    },

    /// Combine nested validators via logical AND (all must succeed)
    And {
        nested: Vec<SchemaValidator>,
    },

    /// Combine nested validators via logical OR (at least one must succeed)
    Or {
        nested: Vec<SchemaValidator>,
    },
}

impl Default for SchemaValidator {
    fn default() -> Self {
        Self::And { nested: Vec::new() }
    }
}

#[cfg(test)]
impl From<Vec<SchemaValidator>> for SchemaValidator {
    fn from(value: Vec<SchemaValidator>) -> Self {
        SchemaValidator::And { nested: value }
    }
}

/// Convert [`XepakValue`] to another [`XepakValue`] according to the [`Schema`].
/// Note that null is always converts ot itself.
///
/// - `schema` - schema to apply
/// - `value` - value to apply schema
/// - `key` - optional debug string referencing full path to the current value
/// - `strict` - fail on unknown keys/elements
/// - `validate` - run value validation
pub fn apply_schema(
    schema: &Schema,
    value: XepakValue,
    key: &str,
    strict: bool,
    validate: bool,
) -> Result<XepakValue, XepakDataError> {
    // Note required must be checked at a top level
    // Not good but if we already passing XepakValue then it must exists
    // so cheking if required is no longer makes sense
    let linked_type = schema.linked_type();
    tracing::warn!("Linked type: {linked_type:?}");
    let value = value.to_type(linked_type, true)?;

    if validate {
        apply_validator(schema.get_validator(), key, &value)?;
    }

    // Consider that value was converted so no need to check for it's type anymore
    let result_value: XepakValue = match schema {
        Schema::Dict { items, .. } => {
            let mut value_dict = value.as_map()?;
            if strict {
                for k in value_dict.keys() {
                    if !items.contains_key(k) {
                        return Err(XepakDataError::Validate(format!(
                            "unknown key {}.{} in the input.",
                            key, k
                        )));
                    }
                }
            }

            for (k, v) in items {
                let key_ref = format!("{key}.{k}");
                if let Some(value) = value_dict.remove(k) {
                    value_dict.insert(
                        k.to_string(),
                        apply_schema(v, value, &key_ref, strict, validate)?,
                    );
                } else {
                    if v.required() {
                        return Err(XepakDataError::Validate(format!(
                            "key {key_ref} does not exists"
                        )));
                    }
                }
            }
            value_dict.into()
        }
        Schema::Tuple {
            items,
            items_repeat,
            ..
        } => {
            let mut value_tuple = value.as_tuple()?;
            let mut idx = 0;
            let mut iteration = 0;
            'schema_repeat: loop {
                for s in items {
                    let key_ref = format!("{key}[{idx}]");
                    // End of input vec reached. Must exit
                    if value_tuple.len() <= idx {
                        if *items_repeat && iteration > 0 {
                            // for 2nd path just exit parent loop
                            // on the initial pass we must iterate over all schema items
                            break 'schema_repeat;
                        }
                        if s.required() {
                            return Err(XepakDataError::Validate(format!(
                                "tuple element {key_ref} must be defined"
                            )));
                        }
                        // Can't move and apply schema
                        // so basically we must not iterate here
                        // but it could be usefule for such cases where tuple shape is (required, not_required, required)
                        // Which is not OK but we need to react on this somehow
                        idx += 1;
                        continue;
                    }
                    let old_val = std::mem::replace(&mut value_tuple[idx], XepakValue::Null);
                    let result = apply_schema(s, old_val, &key_ref, strict, validate)?;
                    value_tuple[idx] = result;
                    idx += 1;
                }
                if !*items_repeat && items.len() > idx && strict {
                    // something still left but we have strict contstraint
                    return Err(XepakDataError::Validate(format!(
                        "tuple element is not allowed at {key}[{idx}]"
                    )));
                }
                if !*items_repeat && items.len() <= idx {
                    // No items left - no need to run them agains schema
                    break;
                }
                iteration += 1;
            }
            value_tuple.into()
        }
        _ => value,
    };

    Ok(result_value)
}

/// Convert [`XepakValue`] to another [`XepakValue`] according to the [`Schema`].
/// Note that null is always converts ot itself.
/// If argument name is not in the schema, it will not be converted (if `strict` is false).
/// With `strict` flag true an error will be returned for all unknown argument names.
pub fn convert_with_schema(
    schema: &Schema,
    arg_name: &str,
    value: XepakValue,
) -> Result<XepakValue, XepakError> {
    // Null is null for any type so it can't be converted to anything.
    if let XepakValue::Null = value {
        return Ok(value);
    }

    Ok(match schema.linked_type() {
        XepakType::Null => {
            return Err(XepakError::Input(format!(
                "Can't convert argument \"{arg_name}\" to Null type! Why are you doing this?"
            )));
        }
        XepakType::Text => XepakValue::Text(value.as_string()),
        XepakType::Boolean => XepakValue::Boolean(value.as_bool()?),
        XepakType::Int => XepakValue::Integer(value.as_int()?),
        XepakType::Float => XepakValue::Float(value.as_float()?),
        XepakType::Blob => XepakValue::Blob(value.as_blob()?),
        XepakType::Tuple => XepakValue::Tuple(value.as_tuple()?),
        XepakType::Dict => XepakValue::Dict(value.as_map()?),
    })
}

pub fn validate_dict_with_schema(
    schema: &Schema,
    values: &HashMap<String, XepakValue>,
) -> Result<(), XepakError> {
    for (arg_name, value) in values {
        if let Some(arg_schema) = schema.get_arg_schema(arg_name) {
            apply_validator(arg_schema.get_validator(), arg_name, value)?;
        }
    }
    Ok(())
}

pub fn apply_validator(
    validator: &SchemaValidator,
    key_ref: &str,
    value: &XepakValue,
) -> Result<(), XepakDataError> {
    match validator {
        SchemaValidator::NotNull => {
            if let XepakValue::Null = value {
                return Err(XepakDataError::Validate(format!(
                    "Element \"{key_ref}\" must not be null/undefined"
                )));
            }
        }
        SchemaValidator::Range { from, to } => match value {
            XepakValue::Text(value) => {
                let l = value.len();
                if l < *from || l > *to {
                    return Err(XepakDataError::Validate(format!(
                        "Element \"{key_ref}\" length {l} is not within a range {from}..={to}"
                    )));
                }
            }
            XepakValue::Integer(value) => {
                let v = *value as usize;
                if v < *from || v > *to {
                    return Err(XepakDataError::Validate(format!(
                        "Element \"{key_ref}\" value {v} is not within a range {from}..={to}"
                    )));
                }
            }
            XepakValue::Float(value) => {
                let v = *value;
                if v < (*from as f64) || v > (*to as f64) {
                    return Err(XepakDataError::Validate(format!(
                        "Element \"{key_ref}\" value {v} is not within a range {from}..={to}"
                    )));
                }
            }
            XepakValue::Tuple(value) => {
                let v = value.len();
                if v < *from || v > *to {
                    return Err(XepakDataError::Validate(format!(
                        "Element \"{key_ref}\" length {v} is not within a range {from}..={to}"
                    )));
                }
            }
            _ => {
                return Err(XepakDataError::Validate(format!(
                    "Element \"{key_ref}\" is not valid for a range check {value:?}"
                )));
            }
        },
        SchemaValidator::RangeFloat { from, to } => match value {
            XepakValue::Text(value) => {
                let l = value.len() as f64;
                if l < *from || l > *to {
                    return Err(XepakDataError::Validate(format!(
                        "Element \"{key_ref}\" length {l} is not within a range {from}..={to}"
                    )));
                }
            }
            XepakValue::Integer(value) => {
                let v = *value as f64;
                if v < *from || v > *to {
                    return Err(XepakDataError::Validate(format!(
                        "Element \"{key_ref}\" value {value} is not within a range {from}..={to}"
                    )));
                }
            }
            XepakValue::Float(value) => {
                let v = *value;
                if v < *from || v > *to {
                    return Err(XepakDataError::Validate(format!(
                        "Element \"{key_ref}\" value {value} is not within a range {from}..={to}"
                    )));
                }
            }
            XepakValue::Tuple(value) => {
                let v = value.len() as f64;
                if v < *from || v > *to {
                    return Err(XepakDataError::Validate(format!(
                        "Element \"{key_ref}\" length {v} is not within a range {from}..={to}"
                    )));
                }
            }
            _ => {
                return Err(XepakDataError::Validate(format!(
                    "Element \"{key_ref}\" is not valid for a float range check {value:?}"
                )));
            }
        },
        SchemaValidator::And { nested } => {
            for v in nested {
                apply_validator(v, key_ref, value)?;
            }
        }
        SchemaValidator::Or { nested } => {
            let mut last_error = None;
            for v in nested {
                match apply_validator(v, key_ref, value) {
                    Ok(_) => {
                        return Ok(());
                    }
                    Err(err) => last_error = Some(err),
                };
            }

            // It always exit from cycle with `last_error`
            // No error could be only if there are not nested conditions
            if let Some(err) = last_error {
                return Err(err);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn schema_for_simple_top_level_values() {
        let schema = Schema::Int {
            required: Default::default(),
            validate: Default::default(),
        };

        let value = XepakValue::from(123);
        apply_schema(&schema, value, "test", false, false).unwrap();

        let schema_bool = Schema::Boolean {
            required: Default::default(),
            validate: Default::default(),
        };

        let value_bool = XepakValue::Boolean(true);
        apply_schema(&schema_bool, value_bool, "test", false, false).unwrap();

        let schema_float = Schema::Float {
            required: Default::default(),
            validate: Default::default(),
        };

        let value_float = XepakValue::from(std::f64::consts::PI);
        apply_schema(&schema_float, value_float, "test", false, false).unwrap();

        let schema_text = Schema::Text {
            required: Default::default(),
            validate: Default::default(),
        };

        let value_text = XepakValue::from("hello");
        apply_schema(&schema_text, value_text, "test", false, false).unwrap();

        let schema_blob = Schema::Blob {
            required: Default::default(),
            validate: Default::default(),
        };

        let value_blob = XepakValue::Blob(vec![1u8, 2, 3]);
        apply_schema(&schema_blob, value_blob, "test", false, false).unwrap();
    }

    #[test]
    fn schema_for_tuples() {
        let items = vec![
            Schema::Int {
                required: true,
                validate: Default::default(),
            },
            Schema::Text {
                required: false,
                validate: Default::default(),
            },
            Schema::Float {
                required: false,
                validate: Default::default(),
            },
        ];
        let schema = Schema::Tuple {
            items,
            items_repeat: false,
            required: Default::default(),
            validate: Default::default(),
        };

        // case 1: single Int element, tuple has only first item
        let value: XepakValue = XepakValue::from(vec![XepakValue::from(2)]);
        let result = apply_schema(&schema, value, "", false, false);
        assert!(matches!(result, Ok(XepakValue::Tuple(v)) if v.len() == 1));

        // case 2: Text that can be converted to Int for first element
        let value: XepakValue = XepakValue::from(vec![XepakValue::from("42")]);
        let result = apply_schema(&schema, value, "", false, false);
        assert_eq!(
            result.unwrap(),
            XepakValue::Tuple(vec![XepakValue::Integer(42)])
        );

        // case 3: Float with fractional part for first element (should fail)
        let value: XepakValue = XepakValue::from(vec![XepakValue::from(std::f64::consts::PI)]);
        let result = apply_schema(&schema, value, "", false, false);
        assert!(result.is_err());

        // case 4: Boolean for first element (should convert to Int)
        let value: XepakValue = XepakValue::from(vec![XepakValue::from(true)]);
        let result = apply_schema(&schema, value, "", false, false);
        assert_eq!(
            result.unwrap(),
            XepakValue::Tuple(vec![XepakValue::Integer(1)])
        );

        // case 5: Tuple with all three elements
        let value: XepakValue = XepakValue::from(vec![
            XepakValue::from(1),
            XepakValue::from("hello"),
            XepakValue::from(std::f64::consts::PI),
        ]);
        let result = apply_schema(&schema, value, "", false, false);
        assert_eq!(
            result.unwrap(),
            XepakValue::Tuple(vec![
                XepakValue::Integer(1),
                XepakValue::Text("hello".to_string()),
                XepakValue::Float(std::f64::consts::PI),
            ])
        );

        // case 6: Tuple with more than schema items (strict=false should allow)
        let value: XepakValue = XepakValue::from(vec![
            XepakValue::from(1),
            XepakValue::from("hello"),
            XepakValue::from(std::f64::consts::PI),
            XepakValue::from(99),
        ]);
        let result = apply_schema(&schema, value, "", false, false);
        assert!(matches!(result, Ok(XepakValue::Tuple(v)) if v.len() == 4));

        // case 7: Required element missing (should fail)
        let value: XepakValue = XepakValue::from(vec![]);
        let result = apply_schema(&schema, value, "", false, false);
        assert!(result.is_err());
    }

    #[test]
    fn schema_for_tuples_repeat_one() {
        // Schema with one required item that repeats
        let items = vec![Schema::Int {
            required: true,
            validate: Default::default(),
        }];
        let schema = Schema::Tuple {
            items,
            items_repeat: true,
            required: Default::default(),
            validate: Default::default(),
        };

        // case 1: Single element tuple - should apply schema once
        let value: XepakValue = XepakValue::from(vec![XepakValue::from(42)]);
        let result = apply_schema(&schema, value, "", false, false);
        assert!(matches!(result, Ok(XepakValue::Tuple(v)) if v.len() == 1));

        // case 2: Multiple elements - schema repeats for each
        let value: XepakValue = XepakValue::from(vec![
            XepakValue::from(1),
            XepakValue::from(2),
            XepakValue::from(3),
        ]);
        let result = apply_schema(&schema, value, "", false, false);
        assert!(matches!(result, Ok(XepakValue::Tuple(v)) if v.len() == 3));

        // case 3: Empty tuple - should fail because required element missing
        let value: XepakValue = XepakValue::from(vec![]);
        let result = apply_schema(&schema, value, "", false, false);
        assert!(result.is_err());

        // case 4: Multiple Text values that convert to Int
        let value: XepakValue =
            XepakValue::from(vec![XepakValue::from("1"), XepakValue::from("2")]);
        let result = apply_schema(&schema, value, "", false, false);
        assert_eq!(
            result.unwrap(),
            XepakValue::Tuple(vec![XepakValue::Integer(1), XepakValue::Integer(2)])
        );

        // case 5: Mix of valid types that all convert to Int
        let value: XepakValue = XepakValue::from(vec![
            XepakValue::from(1),
            XepakValue::from("2"),
            XepakValue::from(true),
        ]);
        let result = apply_schema(&schema, value, "", false, false);
        assert_eq!(
            result.unwrap(),
            XepakValue::Tuple(vec![
                XepakValue::Integer(1),
                XepakValue::Integer(2),
                XepakValue::Integer(1)
            ])
        );
    }

    #[test]
    fn schema_for_tuples_repeat() {
        let items = vec![
            Schema::Int {
                required: true,
                validate: Default::default(),
            },
            Schema::Text {
                required: false,
                validate: Default::default(),
            },
        ];
        let schema = Schema::Tuple {
            items,
            items_repeat: true,
            required: Default::default(),
            validate: Default::default(),
        };

        // 6 elements with 2-item schema repeating 3 times
        let value: XepakValue = XepakValue::from(vec![
            XepakValue::from(1),
            XepakValue::from("a"),
            XepakValue::from(2),
            XepakValue::from("b"),
            XepakValue::from(3),
            XepakValue::from("c"),
        ]);
        let result = apply_schema(&schema, value, "", false, false);
        assert_eq!(
            result.unwrap(),
            XepakValue::Tuple(vec![
                XepakValue::Integer(1),
                XepakValue::Text("a".to_string()),
                XepakValue::Integer(2),
                XepakValue::Text("b".to_string()),
                XepakValue::Integer(3),
                XepakValue::Text("c".to_string()),
            ])
        );

        // Incomplete final iteration with items_repeat - non-required item missing is OK
        let items = vec![
            Schema::Int {
                required: true,
                validate: Default::default(),
            },
            Schema::Text {
                required: false,
                validate: Default::default(),
            },
        ];
        let schema = Schema::Tuple {
            items,
            items_repeat: true,
            required: Default::default(),
            validate: Default::default(),
        };
        let value: XepakValue = XepakValue::from(vec![
            XepakValue::from(1),
            XepakValue::from("a"),
            XepakValue::from(2), // missing second item in final iteration, but it's not required
        ]);
        let result = apply_schema(&schema, value, "", false, false);
        assert_eq!(
            result.unwrap(),
            XepakValue::Tuple(vec![
                XepakValue::Integer(1),
                XepakValue::Text("a".to_string()),
                XepakValue::Integer(2),
            ])
        );
    }

    #[test]
    fn validator_range() {
        let values_ok = vec![
            XepakValue::Integer(2),
            XepakValue::Integer(6),
            XepakValue::Float(2.0),
            XepakValue::Float(6.0),
            XepakValue::Text("hello".to_string()),
            XepakValue::Text("he".to_string()),
            XepakValue::Text("hello!".to_string()),
            XepakValue::Tuple(vec![XepakValue::from(1), XepakValue::from(2)]),
            XepakValue::Tuple(vec![
                XepakValue::from(1),
                XepakValue::from(2),
                XepakValue::from(2),
                XepakValue::from(2),
                XepakValue::from(2),
                XepakValue::from(2),
            ]),
        ];

        let values_err = vec![
            XepakValue::Null,
            XepakValue::Integer(1),
            XepakValue::Integer(8),
            XepakValue::Float(1.5),
            XepakValue::Float(6.1),
            XepakValue::Text("a".to_string()),
            XepakValue::Text("hello to all!".to_string()),
            XepakValue::Tuple(vec![XepakValue::from(1)]),
            XepakValue::Tuple(vec![
                XepakValue::from(1),
                XepakValue::from(2),
                XepakValue::from(2),
                XepakValue::from(2),
                XepakValue::from(2),
                XepakValue::from(2),
                XepakValue::from(2),
            ]),
        ];

        let validator = SchemaValidator::Range { from: 2, to: 6 };

        for v in values_ok {
            let result = apply_validator(&validator, "", &v);
            assert!(result.is_ok(), "{v:?} -> {result:?}");
        }
        for v in values_err {
            let result = apply_validator(&validator, "", &v);
            assert!(result.is_err(), "{v:?} -> {result:?}");
        }
    }

    #[test]
    fn validator_range_float() {
        let values_ok = vec![
            XepakValue::Integer(3),
            XepakValue::Integer(4),
            XepakValue::Float(2.0),
            XepakValue::Float(3.0),
            XepakValue::Float(5.0),
            XepakValue::Text("ab".to_string()),
            XepakValue::Text("abcd".to_string()),
            XepakValue::Tuple(vec![XepakValue::from(1), XepakValue::from(2)]),
            XepakValue::Tuple(vec![
                XepakValue::from(1),
                XepakValue::from(2),
                XepakValue::from(3),
                XepakValue::from(4),
            ]),
        ];

        let values_err = vec![
            XepakValue::Integer(1),
            XepakValue::Integer(6),
            XepakValue::Float(1.5),
            XepakValue::Float(5.1),
            XepakValue::Text("a".to_string()),
            XepakValue::Text("hello to all!".to_string()),
            XepakValue::Tuple(vec![XepakValue::from(1)]),
            XepakValue::Tuple(vec![
                XepakValue::from(1),
                XepakValue::from(2),
                XepakValue::from(3),
                XepakValue::from(4),
                XepakValue::from(5),
                XepakValue::from(6),
            ]),
        ];

        let validator = SchemaValidator::RangeFloat { from: 2.0, to: 5.0 };

        for v in values_ok {
            let result = apply_validator(&validator, "", &v);
            assert!(result.is_ok(), "{v:?} -> {result:?}");
        }
        for v in values_err {
            let result = apply_validator(&validator, "", &v);
            assert!(result.is_err(), "{v:?} -> {result:?}");
        }
    }
    #[test]
    fn schema_validate_not_null() {
        let schemas = vec![
            Schema::Int {
                required: Default::default(),
                validate: SchemaValidator::NotNull,
            },
            Schema::Boolean {
                required: Default::default(),
                validate: SchemaValidator::NotNull,
            },
            Schema::Float {
                required: Default::default(),
                validate: SchemaValidator::NotNull,
            },
            Schema::Text {
                required: Default::default(),
                validate: SchemaValidator::NotNull,
            },
            Schema::Blob {
                required: Default::default(),
                validate: SchemaValidator::NotNull,
            },
            Schema::Tuple {
                items: vec![],
                items_repeat: false,
                required: Default::default(),
                validate: SchemaValidator::NotNull,
            },
            Schema::Dict {
                items: Default::default(),
                required: Default::default(),
                validate: SchemaValidator::NotNull,
            },
        ];

        for s in schemas {
            let result = apply_schema(&s, XepakValue::Null, "", false, true);
            assert!(
                matches!(result, Err(XepakDataError::Validate(ref msg)) if msg.contains("not be null")),
                "{result:?}"
            );
        }
    }
}
