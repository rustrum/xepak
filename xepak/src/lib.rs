pub mod cfg;
pub mod server;
pub mod storage;
pub mod types;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum XepakError {
    #[error("Configuration error: {0}")]
    Cfg(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Input error: {0}")]
    Input(String),

    #[error("Decode error error: {0}")]
    Decode(String),
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct DataSource {}
