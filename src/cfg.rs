use std::{
    collections::{HashMap, HashSet},
    fs,
    path::PathBuf,
    sync::Arc,
};

use bon::Builder;
use serde::Deserialize;

use crate::{
    XepakError, schema::Schema, server::processor::PreProcessor, storage::StorageSettings,
};

/// Main configuration file that properties could be overwritten via ENV or not ? (TODO).
#[derive(Builder, Clone, Debug, Default, Deserialize)]
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

    #[serde(default)]
    pub registry: HashMap<String, toml::Value>,
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

#[derive(Builder, Clone, Debug, Default, Deserialize)]
pub struct XepakSpecs {
    #[serde(default)]
    pub script: Vec<RhaiScript>,

    #[serde(default)]
    pub endpoint: Vec<EndpointSpecs>,

    /// Shared registry for all pre-processors
    #[serde(default)]
    pub shared_pre_processors: HashMap<String, PreProcessor>,

    #[serde(default)]
    pub default_pre_processors: Vec<PreProcessor>,
}

impl XepakSpecs {
    /// Extend current specs with values from other.
    pub fn extend(&mut self, other: XepakSpecs) {
        self.default_pre_processors
            .extend(other.default_pre_processors);
        self.shared_pre_processors
            .extend(other.shared_pre_processors);

        self.script.extend(other.script);
        self.endpoint.extend(other.endpoint);
    }

    /// Returns false if specs has minor errors and Err on bit structural issues.
    pub fn validate(&self) -> Result<bool, XepakError> {
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

        for (ppid, pp) in &self.shared_pre_processors {
            if let PreProcessor::Ref { .. } = pp {
                return Err(XepakError::Cfg(format!(
                    "Shared pre-processor \"{ppid}\" can't have a \"Ref\" type."
                )));
            }
        }

        for pp in &self.default_pre_processors {
            if let PreProcessor::Ref { id } = pp
                && !self.shared_pre_processors.contains_key(id)
            {
                return Err(XepakError::Cfg(format!(
                    "Can't find pre-processor by reference: \"{id}\""
                )));
            }
        }

        Ok(result)
    }
}

#[derive(Builder, Clone, Debug, Deserialize)]
pub struct XepakAuthSpecs {
    // TODO: configure different types of auth specs
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct RhaiScript {
    pub id: String,
    pub script: String,
}

#[derive(Builder, Clone, Debug, Deserialize)]
pub struct EndpointSpecs {
    pub uri: String,

    pub resource: ResourceSpecs,
    /// Expected (allowed) input arguments (URI path args already included)

    #[serde(default)]
    pub args: Vec<String>,

    // pub validators: Vec<Validator>,
    #[serde(default = "default_limit_key")]
    pub limit_arg: String,

    /// Max limit value for paginated queries
    #[serde(default)]
    pub fetch_limit: usize,

    #[serde(default = "default_offset_key")]
    pub offset_arg: String,

    /// Response will be a single record instead of a list.
    /// Will return 404 if no record available
    #[serde(default)]
    pub single_record_response: bool,

    /// Do not use default pre processors
    /// for the current endpoint.
    #[serde(default)]
    pub pre_processors_ignore_default: bool,

    /// This logic handle requests to extract/validate data
    #[serde(default)]
    pub pre_processors: Vec<PreProcessor>,

    #[serde(default)]
    pub strict_schema: bool,

    #[serde(default)]
    pub schema: Schema,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResourceSpecs {
    Query {
        #[serde(default)]
        data_source: String,
        query: String,
    },

    // Will be renamed to QueryScript
    QueryScriptLua {
        #[serde(default)]
        data_source: String,
        script: String,
    },

    DataScript {
        #[serde(default)]
        data_source: String,
        script: String,
    },

    // Almost deprecated
    QueryScriptRhai {
        #[serde(default)]
        data_source: String,
        script: String,
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
        let entry = entry.map_err(Arc::new)?;
        let path = entry.path();
        if !path.is_file()
            || !path
                .extension()
                .unwrap_or_default()
                .eq_ignore_ascii_case("toml")
        {
            continue;
        }
        let buf = fs::read(&path)
            .map_err(|e| XepakError::Cfg(format!("Can't read file {path:?}: {e}")))?;

        let specs: XepakSpecs = toml::from_slice(&buf)
            .map_err(|e| XepakError::Cfg(format!("Can't parse file {path:?}: {e}")))?;

        result.extend(specs);
    }

    let _ = result.validate()?;

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
