use std::{collections::HashMap, pin::Pin, sync::Arc};

use actix_web::{
    Handler, HttpRequest, HttpResponse, HttpResponseBuilder,
    body::BoxBody,
    dev::HttpServiceFactory,
    http::{
        StatusCode,
        header::{ACCEPT, CONTENT_TYPE},
    },
    web::{self, Bytes, Data},
};
use getopt3::new;
use rhai::{AST, Engine};
use serde::Serialize;

use crate::{
    XepakError,
    auth::SimpleAuthProcessor,
    cfg::{EndpointSpecs, ResourceSpecs},
    script::{build_rhai_ast, build_rhai_engine, execute_script_blocking},
    server::{
        CONTENT_TYPE_CBOR, CONTENT_TYPE_JSON, LIMIT_HEADER, OFFSET_HEADER, RequestArgs,
        XepakAppData,
        processor::{
            BodyToArgsProcessor, InputArgsValidator, PreProcessor, PreProcessorHandler,
            QueryArgsProcessor,
        },
        to_error_object,
    },
    storage::ResourceRequest,
    types::XepakValue,
};

type EndpointHandlerArgs = (HttpRequest, Data<XepakAppData>, Bytes);

#[derive(Clone)]
pub struct EndpointHandler {
    ep: Arc<EndpointSpecs>,
    rhai_engine: Arc<Option<Engine>>,
    handler_script: Arc<Option<AST>>,
    // processor_scrips: Arc<HashMap<usize, AST>>,
    processors: Arc<Vec<Box<dyn PreProcessorHandler + Send + Sync>>>,
}

impl EndpointHandler {
    pub fn new(ep: EndpointSpecs, app: &XepakAppData) -> Result<Self, XepakError> {
        let mut rhai_engine = None;

        let handler_script = match &ep.resource {
            ResourceSpecs::QueryScript { script, .. } => {
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

        let mut processors: Vec<Box<dyn PreProcessorHandler + Send + Sync>> = vec![
            Box::new(QueryArgsProcessor {}),
            Box::new(InputArgsValidator {}),
        ];

        for p in &ep.processor {
            match p {
                PreProcessor::ParseBodyArgs => processors.push(Box::new(BodyToArgsProcessor {})),
                PreProcessor::CheckAuth { allow } => {
                    processors.push(Box::new(SimpleAuthProcessor::new(allow.as_slice())))
                }
            }
        }

        processors.sort_by_key(|b| std::cmp::Reverse(b.priority()));

        Ok(Self {
            ep: Arc::new(ep),
            rhai_engine: Arc::new(rhai_engine),
            handler_script: Arc::new(handler_script),
            // processor_scrips: Arc::new(Default::default()),
            processors: Arc::new(processors),
        })
    }

    async fn handle(
        &self,
        req: HttpRequest,
        state: Data<XepakAppData>,
        body: Bytes,
    ) -> HttpResponse {
        tracing::debug!("Handler called for {:?}", self.ep);

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
    ) -> Result<RequestArgs, XepakError> {
        let mut input = RequestArgs::new(
            self.ep.schema.clone(),
            self.ep.strict_schema,
            &self.ep.uri,
            req.path(),
        );

        for p in self.processors.as_ref() {
            p.handle(req, state, body, &mut input)?;
        }

        Ok(input)
    }

    async fn handle_resource(
        &self,
        input: &RequestArgs,
        state: &Data<XepakAppData>,
    ) -> Result<Vec<HashMap<String, XepakValue>>, XepakError> {
        match &self.ep.resource {
            ResourceSpecs::Query { data_source, query } => {
                let Some(ds) = state.get_data_source(data_source) else {
                    return Err(XepakError::Cfg(format!(
                        "Data source does not exists \"{data_source}\""
                    )));
                };

                let rr = ResourceRequest::new(query, input);
                ds.query(rr).await
            }
            ResourceSpecs::QueryScript { data_source, .. } => {
                let Some(ds) = state.get_data_source(data_source) else {
                    return Err(XepakError::Cfg(format!(
                        "Data source does not exists \"{data_source}\""
                    )));
                };

                let result = execute_script_blocking(
                    state.clone(),
                    self.ep.uri.clone(),
                    self.rhai_engine.clone(),
                    self.handler_script.clone(),
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
                ds.query(rr).await
            }
        }
    }

    fn data_to_response<R>(
        &self,
        req: &HttpRequest,
        input: Option<&RequestArgs>,
        status_code: StatusCode,
        data: &R,
    ) -> HttpResponse
    where
        R: Serialize + minicbor::Encode<()>,
    {
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
        input: &RequestArgs,
        data: Vec<HashMap<String, XepakValue>>,
    ) -> HttpResponse {
        if self.ep.single_record_response {
            if data.len() > 1 {
                tracing::warn!("More than one record returned for URI:{}", req.uri());
            }

            let Some(one_row_data) = data.first() else {
                let (status_code, err_data) = to_error_object(XepakError::NotFound(format!(
                    "Record not found at URI: {}",
                    req.uri()
                )));

                return self.data_to_response(req, Some(input), status_code, &err_data);
            };

            self.data_to_response(req, Some(input), StatusCode::OK, &one_row_data)
        } else {
            self.data_to_response(req, Some(input), StatusCode::OK, &data)
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

fn to_json_response<T: Serialize>(
    code: StatusCode,
    data: &T,
    limit: usize,
    offset: usize,
) -> HttpResponse<BoxBody> {
    match serde_json::to_string(data) {
        Ok(body) => {
            let mut resp = HttpResponseBuilder::new(code);
            resp.append_header((CONTENT_TYPE, CONTENT_TYPE_JSON));
            if limit > 0 {
                resp.append_header((LIMIT_HEADER, limit.to_string()));
            }
            if offset > 0 {
                resp.append_header((OFFSET_HEADER, offset.to_string()));
            }

            resp.body(body)
        }
        Err(e) => {
            tracing::error!("Can't serialize response: {e}");
            HttpResponse::InternalServerError().body(format!("{e}"))
        }
    }
}

fn to_cbor_response<T: minicbor::Encode<()>>(
    code: StatusCode,
    data: &T,
    limit: usize,
    offset: usize,
) -> HttpResponse<BoxBody> {
    match minicbor::to_vec(data) {
        Ok(body) => {
            let mut resp = HttpResponseBuilder::new(code);
            resp.append_header((CONTENT_TYPE, CONTENT_TYPE_CBOR));
            if limit > 0 {
                resp.append_header((LIMIT_HEADER, limit.to_string()));
            }
            if offset > 0 {
                resp.append_header((OFFSET_HEADER, offset.to_string()));
            }

            resp.body(body)
        }
        Err(e) => {
            tracing::error!("Can't serialize response: {e}");
            HttpResponse::InternalServerError().body(format!("{e}"))
        }
    }
}
