use std::collections::HashMap;

use serde::Deserialize;

use crate::{
    XepakError,
    types::{XepakType, XepakValue},
};

/// Schema for input/output arguments
pub type Schema = HashMap<String, ArgSchema>;

#[derive(Debug, Clone, Deserialize)]
pub struct ArgSchema {
    /// Type of the argument
    #[serde(default, rename = "type")]
    pub ty: XepakType,

    #[serde(default)]
    pub scope: ArgSchemaScope,

    /// Input argument must be provided before handling resource.
    #[serde(default)]
    pub required: bool,

    #[serde(default)]
    pub validate: Vec<ArgSchemaValidator>,
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArgSchemaScope {
    #[default]
    All,
    Input,
    Output,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ArgSchemaValidator {
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
        nested: Vec<ArgSchemaValidator>,
    },

    NotNull,

    /// Combine nested validators via logical OR (at least one must succeed)
    Or {
        nested: Vec<ArgSchemaValidator>,
    },
}

/// Convert [`XepakValue`] to another [`XepakValue`] according to the [`Schema`].
/// Note that null is always converts ot itself.
/// If argument name is not in the schema, it will not be converted (if `strict` is false).
/// With `strict` flag true an error will be returned for all unknown argument names.
pub fn convert_with_schema(
    schema: &Schema,
    arg_name: &str,
    value: XepakValue,
    strict: bool,
) -> Result<XepakValue, XepakError> {
    // TODO dealt with input scope here somehow
    let Some(aschema) = schema.get(arg_name) else {
        if strict {
            return Err(XepakError::Input(format!(
                "Input argument \"{arg_name}\" not allowed in this context!"
            )));
        } else {
            return Ok(value);
        }
    };

    // Null is null for any type so it can't be converted to anything.
    if let XepakValue::Null = value {
        return Ok(value);
    }

    Ok(match aschema.ty {
        XepakType::Null => {
            return Err(XepakError::Input(format!(
                "Can't convert argument \"{arg_name}\" to Null type! Why are you doing this?"
            )));
        }
        XepakType::Text => XepakValue::Text(value.as_string()?),
        XepakType::Boolean => XepakValue::Boolean(value.as_bool()?),
        XepakType::Int => XepakValue::Integer(value.as_int()?),
        XepakType::Float => XepakValue::Float(value.as_float()?),
    })
}

pub fn validate_with_schema(
    schema: &Schema,
    values: &HashMap<String, XepakValue>,
) -> Result<(), XepakError> {
    for (arg_name, value) in values {
        if let Some(arg_schema) = schema.get(arg_name) {
            for validator in &arg_schema.validate {
                apply_validator(validator, arg_name, value)?;
            }
        }
    }
    Ok(())
}

pub fn apply_validator(
    validator: &ArgSchemaValidator,
    name: &str,
    value: &XepakValue,
) -> Result<(), XepakError> {
    match validator {
        ArgSchemaValidator::NotNull => {
            if let XepakValue::Null = value {
                return Err(XepakError::Input(format!(
                    "Argument \"{name}\" must not be null/undefined"
                )));
            }
        }
        ArgSchemaValidator::Range { from, to } => match value {
            XepakValue::Text(value) => {
                let l = value.len();
                if l < *from || l > *to {
                    return Err(XepakError::Input(format!(
                        "Argument \"{name}\" length {l} is not within a range {from}..={to}"
                    )));
                }
            }
            XepakValue::Integer(value) => {
                let v = *value as usize;
                if v < *from || v > *to {
                    return Err(XepakError::Input(format!(
                        "Argument \"{name}\" value {value} is not within a range {from}..={to}"
                    )));
                }
            }
            XepakValue::Float(value) => {
                let v = *value as usize;
                if v < *from || v > *to {
                    return Err(XepakError::Input(format!(
                        "Argument \"{name}\" value {value} is not within a range {from}..={to}"
                    )));
                }
            }
            _ => {
                return Err(XepakError::Input(format!(
                    "Argument \"{name}\" must be a text, but got {value:?}"
                )));
            }
        },
        ArgSchemaValidator::RangeFloat { from, to } => match value {
            XepakValue::Text(value) => {
                let l = value.len() as f64;
                if l < *from || l > *to {
                    return Err(XepakError::Input(format!(
                        "Argument \"{name}\" length {l} is not within a range {from}..={to}"
                    )));
                }
            }
            XepakValue::Integer(value) => {
                let v = *value as f64;
                if v < *from || v > *to {
                    return Err(XepakError::Input(format!(
                        "Argument \"{name}\" value {value} is not within a range {from}..={to}"
                    )));
                }
            }
            XepakValue::Float(value) => {
                let v = *value;
                if v < *from || v > *to {
                    return Err(XepakError::Input(format!(
                        "Argument \"{name}\" value {value} is not within a range {from}..={to}"
                    )));
                }
            }
            _ => {
                return Err(XepakError::Input(format!(
                    "Argument \"{name}\" must be a text, but got {value:?}"
                )));
            }
        },
        ArgSchemaValidator::And { nested } => {
            for v in nested {
                apply_validator(v, name, value)?;
            }
        }
        ArgSchemaValidator::Or { nested } => {
            let mut last_error = None;
            for v in nested {
                match apply_validator(v, name, value) {
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
