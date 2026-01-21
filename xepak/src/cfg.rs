use std::{collections::HashSet, fs, path::PathBuf};

use serde::Deserialize;

use crate::{XepakError, storage::StorageSettings};

/// Main configuration file that properties could be overwritten via ENV or not ? (TODO).
#[derive(Clone, Debug, Default, Deserialize)]
pub struct XepakConf {
    /// Port to listen on.
    #[serde(default = "default_port")]
    pub port: u16,

    /// Path to the directory with specs.
    #[serde(default = "default_specs_dir")]
    pub specs_dir: PathBuf,

    /// Storage connection settings.
    #[serde(default)]
    pub storage: Vec<StorageSettings>,
}

impl XepakConf {
    pub fn validate(&self) -> bool {
        let mut result = true;

        if self.storage.is_empty() {
            result = false;
            tracing::warn!("Storage configuration is empty");
        }
        result
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct XepakSpecs {
    #[serde(default)]
    pub auth: Option<XepakAuthSpecs>,
    #[serde(default)]
    pub script: Vec<RhaiScript>,
    #[serde(default)]
    pub endpoint: Vec<EndpointSpecs>,
}

impl XepakSpecs {
    /// Extend current specs with values from other.
    pub fn extend(&mut self, other: XepakSpecs) {
        if let Some(auth) = other.auth {
            self.auth = Some(auth);
        }

        self.script.extend(other.script);
        self.endpoint.extend(other.endpoint);
    }

    pub fn validate(&self) -> bool {
        let mut result = true;

        let mut ids = HashSet::new();
        for script in &self.script {
            if !ids.insert(script.id.clone()) {
                tracing::warn!("Duplicate script found with id: {}", script.id);
                result = false;
            }
        }

        let mut ids = HashSet::new();
        for ep in &self.endpoint {
            if !ids.insert(ep.uri.clone()) {
                tracing::warn!("Duplicate endpoint for URI: {}", ep.uri);
                result = false;
            }
        }

        result
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct XepakAuthSpecs {}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct RhaiScript {
    pub id: String,
    pub script: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct EndpointSpecs {
    pub uri: String,

    pub resource: ResourceSpecs,
    /// Expected (allowed) input arguments (URI path args already included)

    #[serde(default)]
    pub args: Vec<String>,
    // pub validators: Vec<Validator>,
    #[serde(default = "default_limit_key")]
    pub limit_arg: String,

    #[serde(default = "default_limit_max")]
    pub limit_max: usize,

    #[serde(default = "default_offset_key")]
    pub offset_arg: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResourceSpecs {
    Sql {
        // TODO: add read/write attribute here?
        #[serde(default)]
        data_source: String,
        query: String,
        /// Add pagination to SQL query offset/limit
        #[serde(default)]
        paginated: bool,
        /// Response will be a single record instead of a list
        #[serde(default)]
        one_record: bool,
    },
}

pub fn load_conf_file(file_path: &str) -> Result<XepakConf, XepakError> {
    let path = PathBuf::from(&file_path);

    let buf = fs::read(&path)
        .map_err(|e| XepakError::Cfg(format!("Can't read file {file_path}: {e}")))?;

    let conf: XepakConf = toml::from_slice(&buf)
        .map_err(|e| XepakError::Cfg(format!("Can't parse file {file_path}: {e}")))?;

    let _ = conf.validate();

    Ok(conf)
}

// TODO: override from ENV maybe? as a separate function

pub fn load_specs_from_dir(dir_path: PathBuf) -> Result<XepakSpecs, XepakError> {
    let dir_content = fs::read_dir(&dir_path)
        .map_err(|e| XepakError::Cfg(format!("Can't read directory {dir_path:?}: {e}")))?;

    let mut result = XepakSpecs::default();
    for entry in dir_content {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() || path.extension().unwrap_or_default().to_ascii_lowercase() != "toml" {
            continue;
        }
        let buf = fs::read(&path)
            .map_err(|e| XepakError::Cfg(format!("Can't read file {path:?}: {e}")))?;

        let specs: XepakSpecs = toml::from_slice(&buf)
            .map_err(|e| XepakError::Cfg(format!("Can't parse file {path:?}: {e}")))?;

        result.extend(specs);
    }

    let _ = result.validate();

    Ok(result)
}

fn default_port() -> u16 {
    8080
}

fn default_specs_dir() -> PathBuf {
    PathBuf::from("./specs")
}

fn default_limit_key() -> String {
    "limit".to_string()
}

fn default_offset_key() -> String {
    "offset".to_string()
}

fn default_limit_max() -> usize {
    1000
}
