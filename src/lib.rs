pub mod auth;
pub mod cfg;
pub mod schema;
pub mod script_lua;
pub mod script_rhai;
pub mod server;
mod sql_key_args;
pub mod storage;
pub mod xepak_data;

use std::sync::Arc;

use rhai::{EvalAltResult, ParseError};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::xepak_data::XepakDataError;

/*
TODO: Think about representing errors in a form of an object like
{
    "code": "trip_not_possible",
    "message": "Trip is not possible, please check start/stop coordinates and try again."
}
*/

#[derive(Error, Debug, Clone)]
pub enum XepakError {
    #[error("Config error: {0}")]
    Cfg(String),

    #[error("IO error: {0}")]
    Io(#[from] Arc<std::io::Error>),

    #[error("Input error: {0}")]
    Input(String),

    #[error("Record not found: {0}")]
    NotFound(String),

    #[error("{0}")]
    Forbidden(String),

    /// Server error with message that will be displayed to client
    #[error("{0}")]
    WeScrewed(String),

    #[error("Convert error: {0}")]
    Convert(String),

    #[error("Decode error: {0}")]
    Decode(String),

    #[error("Script parse error: {0}")]
    ScriptParse(#[from] ParseError),

    #[error("Script execution error: {0}")]
    Script(#[from] Arc<EvalAltResult>),

    #[error("Lua script error: {0}")]
    LuaScript(String),

    #[error("Inconsistency found: {0}")]
    NotConsistent(String),

    #[error("Unexpected: {0}")]
    Unexpected(String),

    #[error("Other error: {0}")]
    Other(Arc<Box<dyn core::error::Error + Send + Sync>>),
}

impl XepakError {
    pub fn other<E>(err: E) -> Self
    where
        E: core::error::Error + Send + Sync + 'static,
    {
        Self::Other(Arc::new(Box::new(err)))
    }

    /// Expectable errors should be convertible to HTTPS response with verbose message.
    /// This is type of errors that are expected during execution and should not be logged as errors.
    pub fn is_expectable(&self) -> bool {
        match self {
            XepakError::Input(_)
            | XepakError::NotFound(_)
            | XepakError::Decode(_)
            | XepakError::WeScrewed(_)
            | XepakError::Forbidden(_) => false,
            _ => false,
        }
    }
}

impl From<XepakDataError> for XepakError {
    fn from(value: XepakDataError) -> Self {
        match value {
            XepakDataError::Convert(_) | XepakDataError::ConvertValue(_, _, _) => {
                XepakError::Convert(value.to_string())
            }
            XepakDataError::Decode(msg) => XepakError::Decode(msg),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct DataSource {}
