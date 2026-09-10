use actix_web::{
    Handler, HttpRequest, HttpResponse,
    dev::HttpServiceFactory,
    http::{Method, StatusCode, header::ACCEPT},
    web::{self, Bytes, Data},
};
use rhai::{AST, Engine};
use std::{pin::Pin, sync::Arc};

use super::{EndpointHandlerArgs, to_cbor_response, to_json_response};
use crate::{
    XepakError,
    cfg::{EndpointSpecs, ResourceRef, ResourceSpecs},
    script_lua::{execute_lua_script, init_lua_env_fn},
    script_rhai::{build_rhai_ast, build_rhai_engine, execute_script_blocking},
    server::{
        CONTENT_TYPE_CBOR, RequestInput, XepakAppData,
        processor::{PreProcessorHandler, build_pre_processor, init_required_pre_processors},
        to_error_object,
    },
    storage::{ResourceRequest, SqlxRequestArgs, Storage},
    xepak_data::{XepakType, XepakValue},
};

#[derive(Clone)]
pub struct EndpointHandler {
    _rref: ResourceRef,
    ep: Arc<EndpointSpecs>,
    rhai_engine: Arc<Option<Engine>>,
    handler_rhai: Arc<Option<AST>>,
    resource_fn_key: String,
    processors: Arc<Vec<Box<dyn PreProcessorHandler>>>,
}

impl EndpointHandler {
    pub fn new(
        rref: ResourceRef,
        ep: EndpointSpecs,
        app: &XepakAppData,
    ) -> Result<Self, XepakError> {
        let mut rhai_engine = None;

        let handler_rhai = match &ep.resource {
            ResourceSpecs::QueryScriptRhai { script, .. } => {
                if rhai_engine.is_none() {
                    rhai_engine = Some(build_rhai_engine(app));
                }

                let Some(rhai) = &rhai_engine else {
                    return Err(XepakError::Unexpected(
                        "Engine must exists here".to_string(),
                    ));
                };

                Some(build_rhai_ast(rhai, script)?)
            }
            _ => None,
        };

        // This is just a validation to fail early if LUA syntax incorrect
        match &ep.resource {
            ResourceSpecs::QueryScriptLua { script, .. }
            | ResourceSpecs::DataScript { script, .. } => {
                init_lua_env_fn(app, script)?;
            }
            _ => {}
        }

        let pre_processors = Self::build_pre_processors(rref.nested("pp"), &ep, app)?;
        Ok(Self {
            resource_fn_key: rref.nested("resource-fn").to_string(),
            _rref: rref,
            ep: Arc::new(ep),
            rhai_engine: Arc::new(rhai_engine),
            handler_rhai: Arc::new(handler_rhai),
            // handler_lua: Arc::new(handler_lua),
            processors: Arc::new(pre_processors),
        })
    }

    fn build_pre_processors(
        rref: ResourceRef,
        ep: &EndpointSpecs,
        app: &XepakAppData,
    ) -> Result<Vec<Box<dyn PreProcessorHandler>>, XepakError> {
        let mut processors: Vec<Box<dyn PreProcessorHandler>> = init_required_pre_processors();

        let mut order = 0u16;
        // Default pre processors
        if !ep.pre_processors_ignore_default {
            for specs in &app.default_pre_processors {
                order += 1;
                processors.push(build_pre_processor(
                    app,
                    &rref,
                    order,
                    specs,
                    &app.shared_pre_processors,
                )?);
            }
        }
        // Pre processors for current handler
        for specs in &ep.pre_processors {
            order += 1;
            processors.push(build_pre_processor(
                app,
                &rref,
                order,
                specs,
                &app.shared_pre_processors,
            )?);
        }

        // Here PP order could change (depends on the basic priority each handlers had)
        processors.sort_by_key(|b| std::cmp::Reverse(b.priority()));

        Ok(processors)
    }

    fn validate_method_allowed(&self, req: &HttpRequest) -> Result<(), XepakError> {
        let am = &self.ep.allow_methods;
        if am.is_empty() && (req.method() == Method::GET || req.method() == Method::DELETE) {
            return Ok(());
        }
        if am.contains(req.method()) {
            return Ok(());
        }

        Err(XepakError::Input(format!(
            "Request method {} not allowed!",
            req.method()
        )))
    }

    async fn handle(
        &self,
        req: HttpRequest,
        state: Data<XepakAppData>,
        body: Bytes,
    ) -> HttpResponse {
        tracing::debug!("Handler called for {:?}", self.ep.uri);

        let mut ri = match self.pre_process_request(&req, &state, &body).await {
            Ok(result) => result,
            Err(err) => {
                let (status_code, data) = to_error_object(err);
                return self.data_to_response(&req, None, status_code, &data);
            }
        };
        // Maybe it should be in processors
        ri.parse_offset_limit(&self.ep.offset_arg, &self.ep.limit_arg, self.ep.fetch_limit);

        // TODO rethink this with new storage api for query/query_one
        let data = match self.handle_resource(&ri, &state).await {
            Ok(d) => d,
            Err(err) => {
                let (status_code, data) = to_error_object(err);
                return self.data_to_response(&req, None, status_code, &data);
            }
        };

        self.build_response(&req, &ri, data)
    }

    async fn pre_process_request(
        &self,
        req: &HttpRequest,
        state: &Data<XepakAppData>,
        body: &Bytes,
    ) -> Result<RequestInput, XepakError> {
        self.validate_method_allowed(req)?;

        let mut input = RequestInput::new(
            self.ep.schema.clone(),
            self.ep.strict_schema,
            req.method().to_string(),
            &self.ep.uri,
            req.path(),
        );

        for p in self.processors.as_ref() {
            p.handle(req, state, body, &mut input).await?;
        }

        Ok(input)
    }

    async fn handle_resource(
        &self,
        input: &RequestInput,
        state: &Data<XepakAppData>,
    ) -> Result<XepakValue, XepakError> {
        match &self.ep.resource {
            ResourceSpecs::Query { data_source, query } => {
                let Some(ds) = state.get_data_source(data_source) else {
                    return Err(XepakError::Cfg(format!(
                        "Data source does not exists \"{data_source}\""
                    )));
                };

                let rr = ResourceRequest::new(query, input);
                self.run_query(ds, rr).await
            }
            ResourceSpecs::QueryScriptRhai { data_source, .. } => {
                let Some(ds) = state.get_data_source(data_source) else {
                    return Err(XepakError::Cfg(format!(
                        "Data source does not exists \"{data_source}\""
                    )));
                };

                let result = execute_script_blocking(
                    state.clone(),
                    self.ep.uri.clone(),
                    self.rhai_engine.clone(),
                    self.handler_rhai.clone(),
                    input.clone(),
                )
                .await?;

                let query = if result.is_string() {
                    result.to_string()
                } else {
                    tracing::error!("Rhai script must return string instead: {result:?}");
                    return Err(XepakError::Unexpected(format!(
                        "Rhai script must return string instead: {result:?}"
                    )));
                };

                let rr = ResourceRequest::new(&query, input);
                self.run_query(ds, rr).await
            }
            ResourceSpecs::QueryScriptLua {
                data_source,
                script,
            } => {
                let Some(ds) = state.get_data_source(data_source) else {
                    return Err(XepakError::Cfg(format!(
                        "Data source does not exists \"{data_source}\""
                    )));
                };

                let query = execute_lua_script::<String>(
                    state,
                    input.clone(),
                    &self.resource_fn_key,
                    script,
                )
                .await?;

                let rr = ResourceRequest::new(&query, input);
                self.run_query(ds, rr).await
            }
            ResourceSpecs::DataScript { script, .. } => {
                execute_lua_script(state, input.clone(), &self.resource_fn_key, script).await
            }
        }
    }

    async fn run_query<RA: SqlxRequestArgs>(
        &self,
        ds: &Storage,
        request: ResourceRequest<'_, RA>,
    ) -> Result<XepakValue, XepakError> {
        if self.ep.single_record_response {
            ds.query_one(request)
                .await
                .map(|v| v.unwrap_or(XepakValue::Null))
        } else {
            ds.query(request).await.map(Into::into)
        }
    }

    fn data_to_response(
        &self,
        req: &HttpRequest,
        input: Option<&RequestInput>,
        status_code: StatusCode,
        data: &XepakValue,
    ) -> HttpResponse {
        let cbor_response = if let Some(accept) = req.headers().get(ACCEPT)
            && accept.eq(CONTENT_TYPE_CBOR)
        {
            true
        } else {
            false
        };

        // TODO should normalize headers output instead providing limit/offset
        let (limit, offset) = match input {
            Some(inp) => (inp.get_limit(), inp.get_offset()),
            None => (0, 0),
        };

        if cbor_response {
            to_cbor_response(status_code, data, limit, offset)
        } else {
            to_json_response(status_code, data, limit, offset)
        }
    }
    fn build_response(
        &self,
        req: &HttpRequest,
        input: &RequestInput,
        data: XepakValue,
    ) -> HttpResponse {
        match data.get_type() {
            XepakType::Null => {
                let (status_code, err_data) = to_error_object(XepakError::NotFound(format!(
                    "Record not found at URI: {}",
                    req.uri()
                )));

                self.data_to_response(req, Some(input), status_code, &err_data)
            }
            XepakType::Map | XepakType::Tuple => {
                self.data_to_response(req, Some(input), StatusCode::OK, &data)
            }
            _ => {
                let (status_code, err_data) = to_error_object(XepakError::NotConsistent(format!(
                    "Return data types could be only Array|Map|Null not {}",
                    data.get_type()
                )));
                self.data_to_response(req, Some(input), status_code, &err_data)
            }
        }
    }
}

impl Handler<EndpointHandlerArgs> for EndpointHandler {
    type Output = HttpResponse;
    type Future = Pin<Box<dyn Future<Output = Self::Output> + 'static>>;

    fn call(&self, (req, state, body): EndpointHandlerArgs) -> Self::Future {
        tracing::debug!("Handler CALL called for {:?}", self.ep);
        let this = self.clone();
        Box::pin(async move { this.handle(req, state, body).await })
    }
}

impl HttpServiceFactory for EndpointHandler {
    fn register(self, config: &mut actix_web::dev::AppService) {
        let name = format!("Entrypoint: {}", self.ep.uri);
        tracing::debug!("Registering [{:?}]: {name}", std::thread::current().id());

        web::resource(self.ep.uri.clone())
            .route(web::route().to(self))
            // .route(web::route().to(move |req, state, body| {
            //     let h = self.clone();
            //     async move { h.handle(req, state, body).await }
            // }))
            // .route(web::route().to(self))
            .register(config);

        // web::resource("/user/list")
        //     // .route(web::route().to(self))
        //     // .route(web::route().to(move |req, state, body| {
        //     //     let h = self.clone();
        //     //     async move { h.handle(req, state, body).await }
        //     // }))
        //     // .route(web::route().to(self))
        //     .register(config);
    }
}
