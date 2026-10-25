use std::{collections::HashMap, pin::Pin, sync::Arc};

use actix_web::{
    Handler, HttpRequest, HttpResponse,
    dev::HttpServiceFactory,
    http::Method,
    web::{self, Bytes, Data},
};
use serde::{Deserialize, Serialize};

use crate::{
    XepakError,
    cfg::ResourceRef,
    server::{
        RequestInput, XepakAppData,
        cfg::{RpcEpMethodSpecs, RpcEpSpecs},
        handlers::{EndpointHandlerArgs, build_pre_processors, handle_resource, validate_resource},
        processor::PreProcessorHandler,
    },
    xepak_data::XepakValue,
};

pub const JSON_RPC_VERSION: &str = "2.0";

pub const RPC_ERROR_INVALID_REQUEST: isize = -32600;

pub const RPC_ERROR_METHOD_NOT_FOUND: isize = -32601;

pub const RPC_ERROR_INVALID_PARAMS: isize = -32602;

pub const RPC_ERROR_INTERNAL: isize = -32603;

pub const RPC_ERROR_FORBIDDEN: isize = -32403;

pub const RPC_ERROR_PARSE: isize = -32700;

#[derive(Clone)]
struct RpcMethodHandler {
    _rref: ResourceRef,
    script_cache_key: String,
    pre_processors: Arc<Vec<Box<dyn PreProcessorHandler>>>,
}

#[derive(Clone)]
pub struct RpcHandler {
    _rref: ResourceRef,
    ep: Arc<RpcEpSpecs>,
    methods: HashMap<String, RpcMethodHandler>,
}

impl RpcHandler {
    pub fn new(rref: ResourceRef, ep: RpcEpSpecs, app: &XepakAppData) -> Result<Self, XepakError> {
        let methods = Self::init_methods(app, rref.clone(), &ep.methods)?;
        Ok(Self {
            _rref: rref,
            ep: Arc::new(ep),
            methods,
        })
    }

    fn init_methods(
        app: &XepakAppData,
        rref: ResourceRef,
        methods: &HashMap<String, RpcEpMethodSpecs>,
    ) -> Result<HashMap<String, RpcMethodHandler>, XepakError> {
        let mut result = HashMap::new();
        for (key, specs) in methods {
            validate_resource(app, &specs.resource)?;

            let mref = rref.nested(key);
            let pre_processors = build_pre_processors(
                app,
                rref.nested(key),
                &specs.pre_processors,
                specs.pre_processors_ignore_default,
            )?;

            let m_handler = RpcMethodHandler {
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

        let rpc_request: JsonRpcRequest<Option<XepakValue>> = match serde_json::from_slice(&body) {
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

        match self.handle_rpc(req, state, rpc_request).await {
            Ok(r) => rpc_success_response(rpc_id, r),
            Err(e) => error_to_rpc_response(rpc_id, e),
        }
    }

    async fn handle_rpc(
        &self,
        req: HttpRequest,
        state: Data<XepakAppData>,
        request: JsonRpcRequest<Option<XepakValue>>,
    ) -> Result<XepakValue, XepakError> {
        let Some(specs) = self.ep.methods.get(&request.method) else {
            return Err(XepakError::Input(format!(
                "method not found: {}",
                request.method
            )));
        };
        let Some(handler) = self.methods.get(&request.method) else {
            return Err(XepakError::Input(format!(
                "handler method not found: {}",
                request.method
            )));
        };

        let input = self
            .pre_process_request(&req, &state, specs, handler, request.params)
            .await?;

        tracing::debug!("Calling RPC method: {}", request.method);

        handle_resource(
            &input,
            &state,
            &handler.script_cache_key,
            &specs.resource,
            specs.single_record_response,
        )
        .await
    }

    async fn pre_process_request(
        &self,
        req: &HttpRequest,
        state: &Data<XepakAppData>,
        specs: &RpcEpMethodSpecs,
        handler: &RpcMethodHandler,
        params: Option<XepakValue>,
    ) -> Result<RequestInput, XepakError> {
        let mut input = RequestInput::new(
            specs.schema.input.clone(),
            true,
            req.method().to_string(),
            &self.ep.uri,
            req.path(),
        );
        let body = Bytes::new();
        if let Some(params_value) = params {
            input.body_args = input.apply_schema_to(".params", params_value, true)?;
        }
        for p in handler.pre_processors.as_ref() {
            p.handle(req, state, &body, &mut input).await?;
        }

        Ok(input)
    }
}

impl Handler<EndpointHandlerArgs> for RpcHandler {
    type Output = HttpResponse;
    type Future = Pin<Box<dyn Future<Output = Self::Output> + 'static>>;

    fn call(&self, (req, state, body): EndpointHandlerArgs) -> Self::Future {
        tracing::trace!("Calling RPC endpoint CALL function with {:?}", self.ep);
        let this = self.clone();
        Box::pin(async move { this.handle_request(req, state, body).await })
    }
}

impl HttpServiceFactory for RpcHandler {
    fn register(self, config: &mut actix_web::dev::AppService) {
        let name = format!("RPC Entrypoint: {}", self.ep.uri);
        tracing::debug!("Registering [{:?}]: {name}", std::thread::current().id());

        web::resource(self.ep.uri.clone())
            .route(web::route().to(self))
            .register(config);
    }
}

pub fn validate_rpc_request_method(req: &HttpRequest) -> Result<(), XepakError> {
    if req.method() == Method::POST {
        return Ok(());
    }

    Err(XepakError::Input(format!(
        "(๑•ᗝ•)૭ Request method {} not allowed!",
        req.method()
    )))
}

/// Build a JSON-RPC error response (protocol-level failure).
pub fn rpc_error_response(id: Option<RpcId>, code: isize, message: String) -> HttpResponse {
    let payload = JsonRpcErrorPayload {
        code,
        message,
        data: None,
    };
    let envelope = JsonRpcError {
        jsonrpc: JSON_RPC_VERSION.to_string(),
        id,
        error: payload,
    };
    let body = serde_json::to_string(&envelope).expect("Expect JsonRpcError to be serializable");
    HttpResponse::Ok()
        .insert_header(("Content-Type", "application/json"))
        .body(body)
}

pub fn error_to_rpc_response(id: Option<RpcId>, error: XepakError) -> HttpResponse {
    match error {
        XepakError::NotFound(msg) => rpc_error_response(id, RPC_ERROR_METHOD_NOT_FOUND, msg),
        XepakError::Input(msg) => {
            let msg = msg.to_string();
            let code = if msg.contains("method not") {
                RPC_ERROR_METHOD_NOT_FOUND
            } else {
                RPC_ERROR_INVALID_REQUEST
            };
            rpc_error_response(id, code, msg)
        }
        XepakError::Decode(msg) | XepakError::WeScrewed(msg) => {
            rpc_error_response(id, RPC_ERROR_INTERNAL, msg)
        }
        XepakError::Forbidden(msg) => rpc_error_response(id, RPC_ERROR_FORBIDDEN, msg),
        _ => {
            tracing::error!("(O_o) error happened! {error:?}");
            rpc_error_response(id, RPC_ERROR_INTERNAL, "unknown_error".into())
        }
    }
}

/// Wrap a typed, serializable result in a JSON-RPC success response.
pub fn rpc_success_response(id: Option<RpcId>, result: XepakValue) -> HttpResponse {
    tracing::trace!("RPC response: {result:?}");
    let envelope = JsonRpcResponse {
        jsonrpc: JSON_RPC_VERSION.to_string(),
        id,
        result,
    };

    let body = serde_json::to_string(&envelope).expect("Expect response to be serializable");
    HttpResponse::Ok()
        .insert_header(("Content-Type", "application/json"))
        .body(body)
}

pub fn default_xepak() -> XepakValue {
    XepakValue::Null
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum RpcId {
    Number(i64),
    String(String),
    Null,
}

/// JSON-RPC 2.0 request envelope.
#[derive(Deserialize)]
pub struct JsonRpcRequest<T> {
    pub jsonrpc: String,
    pub id: Option<RpcId>,
    pub method: String,
    // #[serde(default = "default_xepak")]
    #[serde(default)]
    pub params: T,
}

#[derive(Serialize)]
pub struct JsonRpcResponse<T> {
    pub jsonrpc: String,
    pub id: Option<RpcId>,
    pub result: T,
}

#[derive(Serialize)]
pub struct JsonRpcError {
    pub jsonrpc: String,
    pub id: Option<RpcId>,
    pub error: JsonRpcErrorPayload,
}

#[derive(Serialize)]
pub struct JsonRpcErrorPayload {
    pub code: isize,
    pub message: String,
    pub data: Option<String>,
}
