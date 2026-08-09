//! Xepak data module represents unified/dynamic data types wrappers that
//! transcend all application and could be converted to/from JSON/CBOR/SQL and scripting types.

pub mod serde;
pub mod sql;
pub mod value;

use ::serde::Deserialize;
use strum::Display;
use thiserror::Error;

pub use value::XepakValue;

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
    Blob,
    Tuple,
    Map,
}

#[derive(Error, Debug, Clone)]
pub enum XepakDataError {
    #[error("Can't covert xepak type from {0} to {1}: {2}")]
    ConvertValue(XepakType, XepakType, String),

    #[error("Can't covert xepak value: {0}")]
    Convert(String),

    #[error("Xepak value decode error: {0}")]
    Decode(String),
    // #[error("Unexpected: {0}")]
    // Unexpected(String),
    //
    // #[error("Other error: {0}")]
    // Other(Arc<Box<dyn core::error::Error + Send + Sync>>),
}
