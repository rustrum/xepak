use std::collections::HashSet;

use actix_web::http::Method;
use bon::Builder;
use serde::{Deserialize, Deserializer};

use crate::{schema::Schema, server::processor::PreProcessor};

/// Schemas that applies to input/output data.
#[derive(Default, Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EndpointSchemas {
    #[serde(default)]
    pub input: Schema,

    #[serde(default)]
    pub output: Schema,
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

#[derive(Builder, Clone, Debug, Deserialize)]
pub struct EndpointSpecs {
    /// URI template for this endpoint
    pub uri: String,

    /// List of allowed HTTP methods.
    /// If not defined then only GET method will be allowed.
    #[serde(deserialize_with = "deserialize_methods", default)]
    pub allow_methods: HashSet<Method>,

    pub resource: ResourceSpecs,

    // pub validators: Vec<Validator>,
    /// Custom name for an offset argument if needed.
    #[serde(default = "default_offset_key")]
    pub offset_arg: String,

    /// Custom name for a limit argument if needed.
    #[serde(default = "default_limit_key")]
    pub limit_arg: String,

    /// Max limit value for paginated queries
    #[serde(default)]
    pub fetch_limit: usize,

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
    pub schema: EndpointSchemas,
}

#[derive(Builder, Clone, Debug, Deserialize)]
pub struct EndpointRpcSpecs {
    /// URI template for this RPC
    pub uri: String,

    pub resource: ResourceSpecs,

    /// Max limit value for paginated queries
    #[serde(default)]
    pub fetch_limit: usize,

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
    pub schema: EndpointSchemas,
}

pub fn deserialize_methods<'de, D>(ds: D) -> Result<HashSet<Method>, D::Error>
where
    D: Deserializer<'de>,
{
    let smethods = Vec::<String>::deserialize(ds)?;
    let mut methods = HashSet::<Method>::new();
    for method in smethods {
        let m: Method = method
            .to_uppercase()
            .trim()
            .try_into()
            .map_err(serde::de::Error::custom)?;
        methods.insert(m);
    }
    Ok(methods)
}

fn default_limit_key() -> String {
    "limit".to_string()
}

fn default_offset_key() -> String {
    "offset".to_string()
}
