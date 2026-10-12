use std::collections::{HashMap, HashSet};

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
}

/// REST endpoint specification linked to some URI.
#[derive(Builder, Clone, Debug, Deserialize)]
pub struct RestEpSpecs {
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

/// JSON-RPC endpoint specification.
/// Obvious note - each URI defines different RPC service.
#[derive(Builder, Clone, Debug, Deserialize)]
pub struct RpcEpSpecs {
    /// URI template for this RPC
    pub uri: String,

    pub methods: HashMap<String, RpcEpMethodSpecs>,
}

/// JSON-RPC Method definition.
/// It is very similar to [`RestEpSpecs`], RPC method ~= REST endpoint.
#[derive(Builder, Clone, Debug, Deserialize)]
pub struct RpcEpMethodSpecs {
    #[serde(default)]
    pub schema: EndpointSchemas,

    pub resource: ResourceSpecs,

    /// Max limit value for paginated queries
    #[serde(default)]
    pub fetch_limit: usize,

    /// Response will be a single record instead of a list.
    /// Will return empty dict if no record available
    #[serde(default)]
    pub single_record_response: bool,

    /// Do not use default pre processors
    /// for the current endpoint.
    #[serde(default)]
    pub pre_processors_ignore_default: bool,

    /// This logic handle requests to extract/validate data
    #[serde(default)]
    pub pre_processors: Vec<PreProcessor>,
}

/// MCP server specification.
///
/// I have to say that MCP is the most dump protocol implementation I've ever seen.
/// It is stupid, unresonable, illogical.
/// This is why Xepak will implement only part of this garbadge that is compatible with JSON-RPC.
#[derive(Builder, Clone, Debug, Deserialize)]
pub struct McpEpSpecs {
    /// URI template for this RPC
    pub uri: String,

    /// MCP name
    pub name: String,

    /// MCP version
    pub protocol_version: String,

    /// MCP server instructions for agent
    pub instructions: String,

    /// MCP optional server version field
    #[serde(default)]
    pub server_version: String,

    pub tools: HashMap<String, McpEpToolSpecs>,
}

/*
interface McpTool {
  name: string;
  title?: string;
  description?: string;

  inputSchema: {
    type: "object";
    properties?: Record<string, unknown>;
    required?: string[];
    additionalProperties?: boolean;
  };

  outputSchema?: Record<string, unknown>;

  annotations?: {
    readOnlyHint?: boolean;
    destructiveHint?: boolean;
    idempotentHint?: boolean;
    openWorldHint?: boolean;
  };
}
*/
/// MCP tool
#[derive(Builder, Clone, Debug, Deserialize)]
pub struct McpEpToolSpecs {
    pub title: String,

    pub description: String,

    #[serde(default)]
    pub annotations: Option<McpToolAnnotations>,

    #[serde(default)]
    pub schema: EndpointSchemas,

    pub resource: ResourceSpecs,

    /// Max limit value for paginated queries
    #[serde(default)]
    pub fetch_limit: usize,

    /// Response will be a single record instead of a list.
    /// Will return empty dict if no record available
    #[serde(default)]
    pub single_record_response: bool,

    /// Do not use default pre processors
    /// for the current endpoint.
    #[serde(default)]
    pub pre_processors_ignore_default: bool,

    /// This logic handle requests to extract/validate data
    #[serde(default)]
    pub pre_processors: Vec<PreProcessor>,
}

#[derive(Default, Builder, Clone, Debug, Deserialize)]
pub struct McpToolAnnotations {
    /// tool does not modify state
    pub read_only: Option<bool>,
    /// tool may perform destructive operations
    pub destructive: Option<bool>,
    /// calling the tool multiple times with the same arguments has the same final effect as calling it once.
    pub idempotent: Option<bool>,
    /// tool may interact with external systems
    pub open_world: Option<bool>,
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
