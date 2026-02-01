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
use rhai::{AST, Dynamic, Engine, Scope};
use serde::Serialize;

use crate::{
    XepakError,
    cfg::{EndpointSpecs, ResourceSpecs},
    script::{RhaiRequestContext, build_rhai_ast, build_rhai_engine},
    server::{
        CONTENT_TYPE_CBOR, CONTENT_TYPE_JSON, LIMIT_HEADER, OFFSET_HEADER, RequestArgs,
        XepakAppData,
        processor::{BodyToArgsProcessor, PreProcessor, PreProcessorHandler, QueryArgsProcessor},
        to_error_object,
    },
    storage::ResourceRequest,
    types::XepakValue,
};

/// Return CBOR or JSON representation
macro_rules! to_http_response {
    ($is_cbor:expr, $status_code:expr, $result:expr, $limit:expr, $offset:expr) => {{
        if $is_cbor {
            to_cbor_response($status_code, $result, $limit, $offset)
        } else {
            to_json_response($status_code, $result, $limit, $offset)
        }
    }};
}

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
    pub fn new(ep: EndpointSpecs) -> Result<Self, XepakError> {
        let mut rhai_engine = None;

        let handler_script = match &ep.resource {
            ResourceSpecs::QueryScript { script, .. } => {
                if rhai_engine.is_none() {
                    rhai_engine = Some(build_rhai_engine());
                }

                let Some(rhai) = &rhai_engine else {
                    return Err(XepakError::Unexpected(
                        "Engine must exists here".to_string(),
                    ));
                };

                Some(build_rhai_ast(&rhai, script)?)
            }
            _ => None,
        };

        let mut processors: Vec<Box<dyn PreProcessorHandler + Send + Sync>> =
            vec![Box::new(QueryArgsProcessor {})];

        // TODO sord pre processors
        for p in &ep.processor {
            match p {
                PreProcessor::ParseBodyArgs => processors.push(Box::new(BodyToArgsProcessor {})),
            }
        }

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

        let mut ri = self
            .pre_process_request(&req, &state, &body)
            .await
            .expect("TODO");

        // Maybe it should be in processors
        ri.parse_offset_limit(&self.ep.offset_arg, &self.ep.limit_arg, self.ep.fetch_limit);

        let result = self
            .handle_resource(&ri, &state)
            .await
            .expect("TODO error handle");

        self.build_response(&req, &ri, result)
    }

    async fn pre_process_request(
        &self,
        req: &HttpRequest,
        state: &Data<XepakAppData>,
        body: &Bytes,
    ) -> Result<RequestArgs, XepakError> {
        let mut input = RequestArgs::new(&self.ep.uri, req.path());
        // let ri = RequestInput::new(&self.ep, &req);

        // let mut rab = RequestArgsBuilder::default();

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
                let ds = state.get_data_source(&data_source).expect("TODO");

                let rr = ResourceRequest::new(&query, input);
                ds.fetch_records(rr).await
            }
            ResourceSpecs::QueryScript { data_source, .. } => {
                let ds = state.get_data_source(&data_source).expect("TODO");

                let Some(rhai) = self.rhai_engine.as_ref() else {
                    todo!("Error");
                };
                let Some(ast) = self.handler_script.as_ref() else {
                    todo!("Error");
                };

                let mut scope = Scope::new();
                scope.set_value("ctx", RhaiRequestContext::from(input.clone()));

                let query = match rhai.eval_ast_with_scope::<Dynamic>(&mut scope, ast) {
                    Ok(result) => {
                        if result.is_string() {
                            result.to_string()
                        } else {
                            tracing::error!("Rhai script must return string instead: {result:?}");
                            todo!("TODO errors")
                        }
                    }
                    Err(e) => {
                        tracing::error!("{e}");
                        todo!("TODO deal with errors")
                    }
                };

                let rr = ResourceRequest::new(&query, input);
                ds.fetch_records(rr).await
            }
        }
    }

    fn build_response(
        &self,
        req: &HttpRequest,
        input: &RequestArgs,
        result: Vec<HashMap<String, XepakValue>>,
    ) -> HttpResponse {
        let cbor_response = if let Some(accept) = req.headers().get(ACCEPT)
            && accept.eq(CONTENT_TYPE_CBOR)
        {
            true
        } else {
            false
        };

        if self.ep.single_record_response {
            if result.len() > 1 {
                tracing::warn!("More than one record returned for URI:{}", req.uri());
            }

            let Some(result) = result.iter().next() else {
                let (code, response) = to_error_object(XepakError::NotFound(format!(
                    "Record not found at URI: {}",
                    req.uri()
                )));

                return to_http_response!(
                    cbor_response,
                    code,
                    &response,
                    input.get_limit(),
                    input.get_offset()
                );
            };

            to_http_response!(
                cbor_response,
                StatusCode::OK,
                &result,
                input.get_limit(),
                input.get_offset()
            )
        } else {
            to_http_response!(
                cbor_response,
                StatusCode::OK,
                &result,
                input.get_limit(),
                input.get_offset()
            )
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
            if (limit > 0) {
                resp.append_header((LIMIT_HEADER, limit.to_string()));
            }
            if (offset > 0) {
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
