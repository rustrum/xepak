use std::{collections::HashMap, pin::Pin, sync::Arc};

use actix_web::{
    Handler, HttpRequest, HttpResponse,
    dev::HttpServiceFactory,
    web::{self, Bytes, Data},
};
use serde::Deserialize;

use super::rpc::{JSON_RPC_VERSION, JsonRpcRequest, RPC_ERROR_INVALID_REQUEST, RPC_ERROR_PARSE};
use crate::{
    XepakError,
    cfg::ResourceRef,
    server::{
        RequestInput, XepakAppData,
        cfg::{McpEpSpecs, McpEpToolSpecs},
        handlers::{
            EndpointHandlerArgs, build_pre_processors, error_to_rpc_response, handle_resource,
            rpc_error_response, rpc_success_response, validate_resource,
            validate_rpc_request_method,
        },
        processor::PreProcessorHandler,
        to_error_object,
    },
    xepak_data::XepakValue,
};
use serde_json::Value;

const NO_DATA_RESPONSE: &str = "No data available for provided request";

#[derive(Clone)]
struct McpToolHandler {
    _rref: ResourceRef,
    script_cache_key: String,
    pre_processors: Arc<Vec<Box<dyn PreProcessorHandler>>>,
}

#[derive(Clone)]
pub struct McpHandler {
    _rref: ResourceRef,
    ep: Arc<McpEpSpecs>,
    tools: HashMap<String, McpToolHandler>,
}

impl McpHandler {
    pub fn new(rref: ResourceRef, ep: McpEpSpecs, app: &XepakAppData) -> Result<Self, XepakError> {
        let tools = Self::init_tools(app, rref.clone(), &ep.tools)?;
        Ok(Self {
            _rref: rref,
            ep: Arc::new(ep),
            tools,
        })
    }

    fn init_tools(
        app: &XepakAppData,
        rref: ResourceRef,
        tools: &HashMap<String, McpEpToolSpecs>,
    ) -> Result<HashMap<String, McpToolHandler>, XepakError> {
        let mut result = HashMap::new();
        for (key, specs) in tools {
            validate_resource(app, &specs.resource)?;

            let mref = rref.nested(key);
            let pre_processors = build_pre_processors(
                app,
                rref.nested(key),
                &specs.pre_processors,
                specs.pre_processors_ignore_default,
            )?;

            let m_handler = McpToolHandler {
                _rref: mref,
                script_cache_key: rref.nested("lua-fn").to_string(),
                pre_processors: Arc::new(pre_processors),
            };
            result.insert(key.clone(), m_handler);
        }
        Ok(result)
    }

    async fn handle_request(
        &self,
        req: HttpRequest,
        state: Data<XepakAppData>,
        body: Bytes,
    ) -> HttpResponse {
        if body.iter().copied().find(|b| !b.is_ascii_whitespace()) != Some(b'{') {
            return rpc_error_response(
                None,
                RPC_ERROR_INVALID_REQUEST,
                "Only root level JSON objects are supported for requests".to_string(),
            );
        }

        if let Err(e) = validate_rpc_request_method(&req) {
            return error_to_rpc_response(None, e);
        }

        let rpc_request: JsonRpcRequest<Option<Value>> = match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(e) => {
                return rpc_error_response(
                    None,
                    RPC_ERROR_PARSE,
                    format!("Parse error: invalid JSON ({e})"),
                );
            }
        };

        if rpc_request.jsonrpc != JSON_RPC_VERSION {
            return rpc_error_response(
                rpc_request.id,
                RPC_ERROR_INVALID_REQUEST,
                "Supported only JSON RPC version = {JSON_RPC_VERSION}".to_string(),
            );
        }

        let rpc_id = rpc_request.id.clone();

        match self.handle_mcp_request(req, state, rpc_request).await {
            Ok(r) => rpc_success_response(rpc_id, r),
            Err(e) => error_to_rpc_response(rpc_id, e),
        }
    }

    async fn handle_mcp_request(
        &self,
        req: HttpRequest,
        state: Data<XepakAppData>,
        request: JsonRpcRequest<Option<Value>>,
    ) -> Result<XepakValue, XepakError> {
        let result = match request.method.as_str() {
            "initialize" => self.mcp_initialize(),
            // Notification: no JSON-RPC response, return empty 202.
            "notifications/initialized" => XepakValue::Null,
            "ping" => XepakValue::Dict(Default::default()),
            "tools/list" => self.mcp_tools_list(),
            "tools/call" => self.mcp_tool_call(req, state, request).await?,
            _ => {
                return Err(XepakError::Input(format!(
                    "MCP method not found: {}",
                    request.method
                )));
            }
        };
        Ok(result)
    }

    fn mcp_initialize(&self) -> XepakValue {
        XepakValue::from(HashMap::from([
            (
                "protocolVersion".to_string(),
                XepakValue::from(self.ep.protocol_version.as_str()),
            ),
            (
                "instructions".to_string(),
                XepakValue::from(self.ep.instructions.as_str()),
            ),
            (
                "serverInfo".to_string(),
                XepakValue::from(HashMap::from([
                    ("name".to_string(), XepakValue::from(self.ep.name.as_str())),
                    (
                        "version".to_string(),
                        XepakValue::from(self.ep.server_version.as_str()),
                    ),
                ])),
            ),
            (
                "capabilities".to_string(),
                XepakValue::from(HashMap::from([(
                    "tools".to_string(),
                    // MCP methods list is static thus -> listChanged = false
                    XepakValue::from(HashMap::from([(
                        "listChanged".to_string(),
                        XepakValue::from(false),
                    )])),
                )])),
            ),
        ]))
    }

    fn mcp_tools_list(&self) -> XepakValue {
        let mut tools = Vec::new();
        for (name, specs) in &self.ep.tools {
            let mut tool_dict = HashMap::new();
            tool_dict.insert("name".to_string(), XepakValue::Text(name.clone()));
            if !specs.title.is_empty() {
                tool_dict.insert("title".to_string(), XepakValue::Text(specs.title.clone()));
            }
            tool_dict.insert(
                "description".to_string(),
                XepakValue::Text(specs.description.clone()),
            );

            let input_schema = schema::xepak_schema_to_mcp(&specs.schema.input);
            tool_dict.insert("inputSchema".to_string(), input_schema);

            if let Some(annotations) = &specs.annotations {
                let mut anno_dict = HashMap::new();
                if let Some(v) = annotations.read_only {
                    anno_dict.insert("readOnlyHint".to_string(), XepakValue::from(v));
                }
                if let Some(v) = annotations.destructive {
                    anno_dict.insert("destructiveHint".to_string(), XepakValue::from(v));
                }
                if let Some(v) = annotations.idempotent {
                    anno_dict.insert("idempotentHint".to_string(), XepakValue::from(v));
                }
                if let Some(v) = annotations.open_world {
                    anno_dict.insert("openWorldHint".to_string(), XepakValue::from(v));
                }

                if !anno_dict.is_empty() {
                    tool_dict.insert("annotations".to_string(), XepakValue::Dict(anno_dict));
                }
            }

            tools.push(XepakValue::Dict(tool_dict));
        }

        XepakValue::Dict(HashMap::from([(
            "tools".to_string(),
            XepakValue::Tuple(tools),
        )]))
    }

    async fn mcp_tool_call(
        &self,
        req: HttpRequest,
        state: Data<XepakAppData>,
        request: JsonRpcRequest<Option<Value>>,
    ) -> Result<XepakValue, XepakError> {
        // TODO: maybe should start to return mcp tool response with isError = true
        let tool_params =
            bind_mcp_params::<McpToolParams<XepakValue>>(request.params, "tools/call")?;
        let tool_name = &tool_params.name;
        let Some(tool_handler) = self.tools.get(tool_name) else {
            return Err(XepakError::Input(format!(
                "MCP tool not found: {}",
                request.method
            )));
        };
        let Some(tool_specs) = self.ep.tools.get(tool_name) else {
            // This must not happen
            return Err(XepakError::Input(format!(
                "MCP tool specs not found: {}",
                request.method
            )));
        };

        let mut input = self
            .pre_process_request(&req, &state, tool_specs, tool_handler)
            .await?;

        input.body_args = input.apply_schema_to(".arguments", tool_params.arguments, true)?;

        tracing::debug!("Calling MCP tool: {}", tool_name);

        // No data found for this request.
        let resource_response = handle_resource(
            &input,
            &state,
            &tool_handler.script_cache_key,
            &tool_specs.resource,
            false, // No single record responses for MCP
        )
        .await;

        let response = match resource_response {
            Ok(r) => r,
            Err(e) => {
                // I do not want to create new function for MCP only
                let (_, error_ojb) = to_error_object(e);
                let msg = error_ojb
                    .dict_get("message")
                    .cloned()
                    .unwrap_or(XepakValue::from("Unexpected"));
                return mcp_tool_error(msg.to_string().as_str());
            }
        };

        // TODO : if no data available must still return text with readable info
        if response.is_empty() {
            return Ok(XepakValue::from(NO_DATA_RESPONSE));
        }

        mcp_tool_success_content(response)
    }

    async fn pre_process_request(
        &self,
        req: &HttpRequest,
        state: &Data<XepakAppData>,
        specs: &McpEpToolSpecs,
        handler: &McpToolHandler,
    ) -> Result<RequestInput, XepakError> {
        let mut input = RequestInput::new(
            specs.schema.input.clone(),
            true,
            req.method().to_string(),
            &self.ep.uri,
            req.path(),
        );

        let body = Bytes::new();
        for p in handler.pre_processors.as_ref() {
            p.handle(req, state, &body, &mut input).await?;
        }

        Ok(input)
    }
}

impl Handler<EndpointHandlerArgs> for McpHandler {
    type Output = HttpResponse;
    type Future = Pin<Box<dyn Future<Output = Self::Output> + 'static>>;

    fn call(&self, (req, state, body): EndpointHandlerArgs) -> Self::Future {
        tracing::debug!("Calling RPC endpoint CALL function with {:?}", self.ep);
        let this = self.clone();
        Box::pin(async move { this.handle_request(req, state, body).await })
    }
}

impl HttpServiceFactory for McpHandler {
    fn register(self, config: &mut actix_web::dev::AppService) {
        let name = format!("RPC Entrypoint: {}", self.ep.uri);
        tracing::debug!("Registering [{:?}]: {name}", std::thread::current().id());

        web::resource(self.ep.uri.clone())
            .route(web::route().to(self))
            .register(config);
    }
}

/// Bind a present `params` value to the concrete request type.
fn bind_mcp_params<T: serde::de::DeserializeOwned>(
    params: Option<Value>,
    method: &str,
) -> Result<T, XepakError> {
    let value = params.ok_or_else(|| {
        XepakError::Input(format!("MCP params property is required for {method}"))
    })?;
    serde_json::from_value(value)
        .map_err(|e| XepakError::Input(format!("Invalid params for {method}: {e}")))
}

fn mcp_tool_error(message: &str) -> Result<XepakValue, XepakError> {
    Ok(XepakValue::Dict(HashMap::from([
        ("isError".to_string(), XepakValue::from(true)),
        (
            "content".to_string(),
            xepak_to_text_content(XepakValue::from(message))?,
        ),
    ])))
}

/// Builds MCP tool success response with `value` serialized into single text element.
fn mcp_tool_success_content(value: XepakValue) -> Result<XepakValue, XepakError> {
    Ok(XepakValue::Dict(HashMap::from([
        ("isError".to_string(), XepakValue::from(false)),
        (
            "content".to_string(),
            XepakValue::from(vec![xepak_to_text_content(value)?]),
        ),
    ])))
}

/// Wraps value into a single MCP text content element.
/// Original value will be JSON serialized as a string.
fn xepak_to_text_content(value: XepakValue) -> Result<XepakValue, XepakError> {
    let text = match value {
        XepakValue::Text(txt) => txt,
        jval => serde_json::to_string(&jval).map_err(XepakError::other)?,
    };
    Ok(XepakValue::Dict(HashMap::from([
        ("type".to_string(), XepakValue::from("text")),
        ("text".to_string(), XepakValue::from(text)),
    ])))
}

#[derive(Deserialize)]
struct McpToolParams<T> {
    name: String,
    arguments: T,
}

pub mod schema {
    use std::collections::HashMap;

    use crate::{schema::Schema, xepak_data::XepakValue};

    /// Build MCP schema representation in a form of a nested structure of [`XepakValue`].
    pub fn xepak_schema_to_mcp(schema: &Schema) -> XepakValue {
        match schema {
            Schema::Dict { items, .. } => {
                let mut properties = HashMap::new();
                for (name, sub_schema) in items {
                    properties.insert(name.clone(), build_property(sub_schema));
                }

                let required: Vec<String> = items
                    .iter()
                    .filter(|(_, s)| s.required())
                    .map(|(name, _)| name.clone())
                    .collect();

                XepakValue::Dict({
                    let mut result = HashMap::from([
                        ("type".to_string(), XepakValue::Text("object".to_string())),
                        ("properties".to_string(), XepakValue::Dict(properties)),
                    ]);

                    if !required.is_empty() {
                        result.insert(
                            "required".to_string(),
                            XepakValue::Tuple(required.into_iter().map(XepakValue::Text).collect()),
                        );
                    }
                    result
                })
            }
            Schema::Tuple { items, .. } => {
                let mut array_items = Vec::new();
                for item in items {
                    array_items.push(build_property(item));
                }

                XepakValue::Dict({
                    HashMap::from([
                        ("type".to_string(), XepakValue::Text("array".to_string())),
                        (
                            "items".to_string(),
                            if array_items.len() == 1 {
                                array_items[0].clone()
                            } else {
                                XepakValue::Tuple(array_items)
                            },
                        ),
                    ])
                })
            }
            Schema::Boolean { .. } => build_base_schema("boolean", schema),
            Schema::Int { .. } => build_base_schema("integer", schema),
            Schema::Float { .. } => build_base_schema("number", schema),
            Schema::Text { .. } => build_base_schema("string", schema),
            Schema::Blob { .. } => build_base_schema("object", schema),
        }
    }

    fn build_base_schema(typ: &str, schema: &Schema) -> XepakValue {
        let mut result = HashMap::from([("type".to_string(), XepakValue::Text(typ.to_string()))]);

        if schema.required() {
            result.insert("required".to_string(), XepakValue::Boolean(true));
        }

        match schema.get_validator() {
            crate::schema::SchemaValidator::NotNull => {}
            crate::schema::SchemaValidator::Range { from, to } => {
                result.insert("minLength".to_string(), XepakValue::Integer(*from as i128));
                result.insert("maxLength".to_string(), XepakValue::Integer(*to as i128));
            }
            crate::schema::SchemaValidator::RangeFloat { from, to } => {
                result.insert("minimum".to_string(), XepakValue::Float(*from));
                result.insert("maximum".to_string(), XepakValue::Float(*to));
            }
            crate::schema::SchemaValidator::And { .. }
            | crate::schema::SchemaValidator::Or { .. } => {}
        }

        XepakValue::Dict(result)
    }

    fn build_property(schema: &Schema) -> XepakValue {
        match schema {
            Schema::Dict { items, .. } => {
                let mut properties = HashMap::new();

                for (name, sub_schema) in items {
                    properties.insert(name.clone(), build_property(sub_schema));
                }

                let required: Vec<String> = items
                    .iter()
                    .filter(|(_, s)| s.required())
                    .map(|(name, _)| name.clone())
                    .collect();

                XepakValue::Dict({
                    let result = HashMap::from([
                        ("type".to_string(), XepakValue::Text("object".to_string())),
                        ("properties".to_string(), XepakValue::Dict(properties)),
                    ]);

                    if !required.is_empty() {
                        let mut r = result;
                        r.insert(
                            "required".to_string(),
                            XepakValue::Tuple(required.into_iter().map(XepakValue::Text).collect()),
                        );
                        r
                    } else {
                        result
                    }
                })
            }
            Schema::Tuple { items, .. } => {
                let mut array_items = Vec::new();
                for item in items {
                    array_items.push(build_property(item));
                }

                XepakValue::Dict({
                    HashMap::from([
                        ("type".to_string(), XepakValue::Text("array".to_string())),
                        (
                            "items".to_string(),
                            if array_items.len() == 1 {
                                array_items[0].clone()
                            } else {
                                XepakValue::Tuple(array_items)
                            },
                        ),
                    ])
                })
            }
            Schema::Boolean { .. } => build_base_schema("boolean", schema),
            Schema::Int { .. } => build_base_schema("integer", schema),
            Schema::Float { .. } => build_base_schema("number", schema),
            Schema::Text { .. } => build_base_schema("string", schema),
            Schema::Blob { .. } => build_base_schema("object", schema),
        }
    }

    #[cfg(test)]
    mod tests {
        use serde_json::json;

        use crate::schema::SchemaValidator;

        use super::*;

        fn assert_json_equal(actual: XepakValue, expected: &serde_json::Value) {
            let actual_json = serde_json::to_value(actual).expect("Failed to serialize XepakValue");
            assert_eq!(actual_json, *expected, "JSON mismatch");
        }

        #[test]
        fn test_simple_string_schema() {
            let schema = Schema::Text {
                required: true,
                validate: Default::default(),
            };

            let result = xepak_schema_to_mcp(&schema);
            let expected = json!({
                "type": "string",
                "required": true
            });

            assert_json_equal(result, &expected);
        }

        #[test]
        fn test_simple_integer_schema() {
            let schema = Schema::Int {
                required: false,
                validate: Default::default(),
            };

            let result = xepak_schema_to_mcp(&schema);
            let expected = json!({
                "type": "integer"
            });

            assert_json_equal(result, &expected);
        }

        #[test]
        fn test_nested_object_schema() {
            let mut items = HashMap::new();
            items.insert(
                "name".to_string(),
                Schema::Text {
                    required: true,
                    validate: Default::default(),
                },
            );
            items.insert(
                "age".to_string(),
                Schema::Int {
                    required: false,
                    validate: Default::default(),
                },
            );

            let schema = Schema::Dict {
                items,
                required: false,
                validate: Default::default(),
            };

            let result = xepak_schema_to_mcp(&schema);
            let expected = json!({
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "required": true
                    },
                    "age": {
                        "type": "integer"
                    }
                },
                "required": ["name"]
            });

            assert_json_equal(result, &expected);
        }

        #[test]
        fn test_tuple_schema() {
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
                items_repeat: false,
                required: false,
                validate: Default::default(),
            };

            let result = xepak_schema_to_mcp(&schema);
            let expected = json!({
                "type": "array",
                "items": [
                    {
                        "type": "integer",
                        "required": true
                    },
                    {
                        "type": "string"
                    }
                ]
            });

            assert_json_equal(result, &expected);
        }

        #[test]
        fn test_with_range_validator() {
            let schema = Schema::Text {
                required: false,
                validate: SchemaValidator::Range { from: 1, to: 100 },
            };

            let result = xepak_schema_to_mcp(&schema);
            let expected = json!({
                "type": "string",
                "minLength": 1,
                "maxLength": 100
            });

            assert_json_equal(result, &expected);
        }

        #[test]
        fn test_with_float_range_validator() {
            let schema = Schema::Float {
                required: false,
                validate: SchemaValidator::RangeFloat {
                    from: 0.0,
                    to: 100.0,
                },
            };

            let result = xepak_schema_to_mcp(&schema);
            let expected = json!({
                "type": "number",
                "minimum": 0.0,
                "maximum": 100.0
            });

            assert_json_equal(result, &expected);
        }

        #[test]
        fn test_not_null_validator() {
            let schema = Schema::Int {
                required: false,
                validate: SchemaValidator::NotNull,
            };

            let result = xepak_schema_to_mcp(&schema);
            let expected = json!({
                "type": "integer"
            });

            assert_json_equal(result, &expected);
        }
    }
}
