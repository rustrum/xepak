use std::{collections::HashMap, pin::Pin, sync::Arc};

use actix_web::{
    Handler, HttpRequest, HttpResponse,
    dev::HttpServiceFactory,
    web::{self, Bytes, Data},
};
use base64::Engine as _;
use indexmap::IndexMap;
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
        input::{ARG_NAME_LIMIT, ARG_NAME_OFFSET},
        processor::PreProcessorHandler,
        to_error_object,
    },
    xepak_data::XepakValue,
};
use serde_json::Value;

// TODO must move to next protocol version.
// would require dropping initialization
// must refactor initialization to server/discover method
// const PROTOCOL_VERSION: &str = "2026-07-28";
const PROTOCOL_VERSION: &str = "2024-11-05";

const NO_DATA_RESPONSE: &str = "Empty! No data available for your request.";

#[derive(Deserialize)]
struct McpToolParams<T> {
    name: String,
    arguments: T,
}

#[derive(Clone)]
struct McpToolHandler {
    _rref: ResourceRef,
    script_cache_key: String,
    pre_processors: Arc<Vec<Box<dyn PreProcessorHandler>>>,
}

#[derive(Clone)]
pub struct McpHandler {
    _rref: ResourceRef,
    specs: Arc<McpEpSpecs>,
    tools: HashMap<String, McpToolHandler>,
}

impl McpHandler {
    pub fn new(
        rref: ResourceRef,
        specs: McpEpSpecs,
        app: &XepakAppData,
    ) -> Result<Self, XepakError> {
        let sref = rref.nested(&specs.uri);
        let tools = Self::init_tools(app, sref.clone(), &specs.tools)?;
        Ok(Self {
            _rref: sref,
            specs: Arc::new(specs),
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
                mref.nested("pp").nested(key),
                &specs.pre_processors,
                specs.pre_processors_ignore_default,
            )?;

            let m_handler = McpToolHandler {
                _rref: mref.clone(),
                script_cache_key: mref.nested("lua-fn").to_string(),
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
                "Only supported JSON RPC version = {JSON_RPC_VERSION}".to_string(),
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
        tracing::debug!("MCP intialize called");
        XepakValue::from(IndexMap::from([
            (
                "protocolVersion".to_string(),
                XepakValue::from(PROTOCOL_VERSION),
            ),
            (
                "instructions".to_string(),
                XepakValue::from(self.specs.instructions.as_str()),
            ),
            (
                "serverInfo".to_string(),
                XepakValue::from(IndexMap::from([
                    (
                        "name".to_string(),
                        XepakValue::from(self.specs.name.as_str()),
                    ),
                    (
                        "version".to_string(),
                        XepakValue::from(self.specs.server_version.as_str()),
                    ),
                ])),
            ),
            (
                "capabilities".to_string(),
                XepakValue::from(IndexMap::from([(
                    "tools".to_string(),
                    // MCP methods list is static thus -> listChanged = false
                    XepakValue::from(IndexMap::from([(
                        "listChanged".to_string(),
                        XepakValue::from(false),
                    )])),
                )])),
            ),
        ]))
    }

    fn mcp_tools_list(&self) -> XepakValue {
        tracing::debug!("MCP tools/list called");
        let mut tools = Vec::new();
        for (name, specs) in &self.specs.tools {
            let mut tool_dict = IndexMap::new();
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
                let mut anno_dict = IndexMap::new();
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

        XepakValue::Dict(IndexMap::from([(
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
        let tool_params =
            bind_mcp_params::<McpToolParams<XepakValue>>(request.params, "tools/call")?;
        let tool_name = &tool_params.name;
        let Some(tool_handler) = self.tools.get(tool_name) else {
            return Err(XepakError::Input(format!(
                "MCP tool not found: {tool_name}"
            )));
        };
        let Some(tool_specs) = self.specs.tools.get(tool_name) else {
            // This must not happen
            return Err(XepakError::Input(format!(
                "MCP tool specs not found: {}",
                request.method
            )));
        };

        let input = self
            .pre_process_request(
                &req,
                &state,
                tool_specs,
                tool_handler,
                tool_params.arguments,
            )
            .await?;

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
                tracing::error!("{e}");
                // I do not want to create new function for MCP only
                let (_, error_obj) = to_error_object(e);
                let msg = error_obj
                    .dict_get("message")
                    .cloned()
                    .unwrap_or(XepakValue::from("Invalid request or server error"));
                return mcp_tool_error(msg.to_string().as_str());
            }
        };

        // Returns meaningfull MCP response if no data available
        if response.is_empty() {
            let msg = if self.specs.empty_response_text.is_empty() {
                XepakValue::from(NO_DATA_RESPONSE)
            } else {
                XepakValue::from(self.specs.empty_response_text.as_str())
            };
            return mcp_tool_success_content(msg, (None, None));
        }

        mcp_tool_success_content(response, self.extract_limit_offset(&input))
    }

    fn extract_limit_offset(&self, input: &RequestInput) -> (Option<usize>, Option<usize>) {
        (
            input
                .get_arg(ARG_NAME_LIMIT)
                .map(|v| v.as_int().unwrap_or_default() as usize),
            input
                .get_arg(ARG_NAME_OFFSET)
                .map(|v| v.as_int().unwrap_or_default() as usize),
        )
    }

    async fn pre_process_request(
        &self,
        req: &HttpRequest,
        state: &Data<XepakAppData>,
        specs: &McpEpToolSpecs,
        handler: &McpToolHandler,
        tool_arguments: XepakValue,
    ) -> Result<RequestInput, XepakError> {
        let mut input = RequestInput::new(
            specs.schema.input.clone(),
            true,
            req.method().to_string(),
            &self.specs.uri,
            req.path(),
        );
        let body = Bytes::new();

        input.body_args = input.apply_schema_to(".arguments", tool_arguments, true)?;

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
        tracing::trace!("Calling MCP endpoint CALL function with {:?}", self.specs);
        let this = self.clone();
        Box::pin(async move { this.handle_request(req, state, body).await })
    }
}

impl HttpServiceFactory for McpHandler {
    fn register(self, config: &mut actix_web::dev::AppService) {
        let name = format!("MCP Entrypoint: {}", self.specs.uri);
        tracing::debug!("Registering [{:?}]: {name}", std::thread::current().id());

        web::resource(self.specs.uri.clone())
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
    Ok(XepakValue::Dict(IndexMap::from([
        ("isError".to_string(), XepakValue::from(true)),
        (
            "content".to_string(),
            XepakValue::from(vec![xepak_to_text_content(
                XepakValue::from(message),
                false,
            )?]),
        ),
    ])))
}

/// Builds MCP tool success response with `value` serialized into single text element.
fn mcp_tool_success_content(
    value: XepakValue,
    limit_offset: (Option<usize>, Option<usize>),
) -> Result<XepakValue, XepakError> {
    Ok(XepakValue::Dict(IndexMap::from([
        ("isError".to_string(), XepakValue::from(false)),
        (
            "content".to_string(),
            xepak_to_content_elements(value, limit_offset)?,
        ),
    ])))
}

/// Wraps value in to array for MCP content elements for tuples
/// or single row tuple if not
fn xepak_to_content_elements(
    value: XepakValue,
    limit_offset: (Option<usize>, Option<usize>),
) -> Result<XepakValue, XepakError> {
    let mut result = Vec::new();
    match value {
        XepakValue::Tuple(items) => {
            let mut markdown = String::new();
            markdown.push_str("# Result details\n");
            markdown.push_str("total-records-fetched: ");
            markdown.push_str(&items.len().to_string());
            markdown.push('\n');

            let (limit, offset) = limit_offset;
            if let Some(o) = offset {
                markdown.push_str("with-offset: ");
                markdown.push_str(&o.to_string());
                markdown.push('\n');
            }
            if let Some(l) = limit {
                markdown.push_str("with-limit: ");
                markdown.push_str(&l.to_string());
                markdown.push('\n');
                if l == items.len() {
                    markdown.push_str("note: Records count equals to limit. More data could be available to retreive.");
                    markdown.push('\n');
                }
            }
            markdown.push('\n');

            for (idx, xv) in items.into_iter().enumerate() {
                let record = xepak_to_markdown_kv(xv, Some(idx + 1));

                markdown.push_str(&record);
                markdown.push('\n');
            }
            result.push(XepakValue::Dict(IndexMap::from([
                ("type".to_string(), XepakValue::from("text")),
                ("text".to_string(), XepakValue::from(markdown)),
            ])));
        }
        xv => {
            if !xv.is_empty() {
                result.push(xepak_to_text_content(xv, false)?)
            }
        }
    }

    Ok(XepakValue::from(result))
}

/// Wraps value into a single MCP text content element.
/// Original value will be JSON string or markdown-kv.
fn xepak_to_text_content(value: XepakValue, markdown_kv: bool) -> Result<XepakValue, XepakError> {
    let text_value = match value {
        XepakValue::Text(txt) => txt,
        xval => {
            if markdown_kv {
                xepak_to_markdown_kv(xval, None)
            } else {
                serde_json::to_string(&xval).map_err(XepakError::other)?
            }
        }
    };
    Ok(XepakValue::Dict(IndexMap::from([
        ("type".to_string(), XepakValue::from("text")),
        ("text".to_string(), XepakValue::from(text_value)),
    ])))
}

/// Convert [`XepakValue`] to text in markdown-kv format
/// `seq_number` - If set will output record with sequential number
fn xepak_to_markdown_kv(value: XepakValue, seq_number: Option<usize>) -> String {
    let mut result = "# Record".to_string();

    if let Some(seq) = seq_number {
        result.push_str(&format!(" {seq}"));
    }
    result.push('\n');

    let data = match value {
        XepakValue::Dict(dict) => {
            let mut kv_map = String::new();
            for (k, v) in dict {
                kv_map.push_str(&k);
                kv_map.push_str(": ");

                let val = v.as_string();
                let valtrim = val.trim();
                if valtrim.contains('\n') {
                    kv_map.push_str(" |\n");
                    let val_indent: String = valtrim
                        .split_inclusive('\n')
                        .map(|line| format!("  {}", line))
                        .collect();
                    kv_map.push_str(&val_indent);
                } else {
                    kv_map.push_str(valtrim);
                }

                kv_map.push('\n');
            }
            kv_map
        }
        XepakValue::Blob(blob) => {
            let encoded = base64::engine::general_purpose::STANDARD.encode(blob);
            format!("BASE64: {encoded}")
        }
        xv => xv.as_string(),
    };
    result.push_str(&data);
    result.push('\n');
    result
}

pub mod schema {
    use indexmap::IndexMap;

    use crate::{schema::Schema, xepak_data::XepakValue};

    /// Build MCP schema representation in a form of a nested structure of [`XepakValue`].
    pub fn xepak_schema_to_mcp(schema: &Schema) -> XepakValue {
        match schema {
            Schema::Dict { items, .. } => {
                let mut properties = IndexMap::new();
                for (name, sub_schema) in items {
                    properties.insert(name.clone(), build_property(sub_schema));
                }

                let required: Vec<String> = items
                    .iter()
                    .filter(|(_, s)| s.required())
                    .map(|(name, _)| name.clone())
                    .collect();

                XepakValue::Dict({
                    let mut result = IndexMap::from([
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
                    IndexMap::from([
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
        let mut result = IndexMap::from([("type".to_string(), XepakValue::Text(typ.to_string()))]);

        if schema.required() {
            result.insert("required".to_string(), XepakValue::Boolean(true));
        }

        match schema.get_validator() {
            crate::schema::SchemaValidator::NotNull => {}
            crate::schema::SchemaValidator::Range { from, to } => {
                result.insert("minimum".to_string(), XepakValue::Integer(*from as i128));
                result.insert("maximum".to_string(), XepakValue::Integer(*to as i128));
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
                let mut properties = IndexMap::new();

                for (name, sub_schema) in items {
                    properties.insert(name.clone(), build_property(sub_schema));
                }

                let required: Vec<String> = items
                    .iter()
                    .filter(|(_, s)| s.required())
                    .map(|(name, _)| name.clone())
                    .collect();

                XepakValue::Dict({
                    let result = IndexMap::from([
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
                    IndexMap::from([
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
        use std::collections::HashMap;

        use serde_json::json;

        use crate::schema::SchemaValidator;

        use super::*;

        fn assert_json_equal(actual: XepakValue, expected: &serde_json::Value) {
            let actual_json =
                serde_json::to_value(actual).expect("Test failed to serialize XepakValue");
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
                "minimum": 1,
                "maximum": 100
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
