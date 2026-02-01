pub mod cfg;
pub mod script;
pub mod server;
pub mod storage;
pub mod types;

use std::collections::HashMap;

use rhai::ParseError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::types::XepakType;

/*

Think about representing errors in a form of an object like
{
    "code": "trip_not_possible",
    "message": "Trip is not possible, please check start/stop coordinates and try again."
}
*/

#[derive(Error, Debug)]
pub enum XepakError {
    #[error("Configuration error: {0}")]
    Cfg(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Input error: {0}")]
    Input(String),

    #[error("Record not found: {0}")]
    NotFound(String),

    #[error("Can't covert type from {0} to {1}: {2}")]
    Convert(XepakType, XepakType, String),

    #[error("Decode error: {0}")]
    Decode(String),

    #[error("Script parse error: {0}")]
    ScriptParse(#[from] ParseError),

    #[error("Really unexpected error: {0}")]
    Unexpected(String),
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct DataSource {}
