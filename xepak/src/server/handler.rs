use std::{pin::Pin, sync::Arc};

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
    script::{RhaiRequestContext, init_rhai_script},
    server::{
        CONTENT_TYPE_CBOR, CONTENT_TYPE_JSON, LIMIT_HEADER, OFFSET_HEADER, RequestInput,
        XepakAppData, to_error_object,
    },
    storage::ResourceRequest,
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

pub type EndpointHandlerArgs = (HttpRequest, Data<XepakAppData>, Bytes);

#[derive(Clone)]
pub struct EndpointHandler {
    ep: Arc<EndpointSpecs>,
    script_env: Arc<Option<(Engine, AST)>>,
}

impl EndpointHandler {
    pub fn new(ep: EndpointSpecs) -> Result<Self, XepakError> {
        let script_env = match &ep.resource {
            ResourceSpecs::QueryScript { script, .. } => Some(init_rhai_script(script)?),
            _ => None,
        };

        Ok(Self {
            ep: Arc::new(ep),
            script_env: Arc::new(script_env),
        })
    }

    async fn handle(
        &self,
        req: HttpRequest,
        state: Data<XepakAppData>,
        _body: Bytes,
    ) -> HttpResponse {
        tracing::debug!("Handler called for {:?}", self.ep);
        let ri = RequestInput::new(&self.ep, &req);
        let result = match &self.ep.resource {
            ResourceSpecs::Query { data_source, query } => {
                let ds = state.get_data_source(&data_source).expect("TODO");

                let rr = ResourceRequest::new(&query, &ri);
                ds.fetch_records(rr).await.expect("TODO QUERY")
            }
            ResourceSpecs::QueryScript {
                data_source,
                script,
            } => {
                let ds = state.get_data_source(&data_source).expect("TODO");
                match self.script_env.as_ref() {
                    Some((rhai, ast)) => {
                        let mut scope = Scope::new();
                        scope.set_value("ctx", RhaiRequestContext::from(ri.args.clone()));

                        let query = match rhai.eval_ast_with_scope::<Dynamic>(&mut scope, ast) {
                            Ok(result) => {
                                if result.is_string() {
                                    result.to_string()
                                } else {
                                    tracing::error!(
                                        "Rhai script must return string instead: {result:?}"
                                    );
                                    todo!("TODO")
                                }
                            }
                            Err(e) => {
                                tracing::error!("{e}");
                                todo!("TODO deal with errors")
                            }
                        };

                        let rr = ResourceRequest::new(&query, &ri);
                        ds.fetch_records(rr).await.expect("TODO QUERY")
                    }
                    None => {
                        // XepakError::Unexpected("Script env must be available".to_string())
                        unimplemented!("TODO implement error handling")
                    }
                }
            }
        };

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
                    ri.get_limit(),
                    ri.get_offset()
                );
            };

            to_http_response!(
                cbor_response,
                StatusCode::OK,
                &result,
                ri.get_limit(),
                ri.get_offset()
            )
        } else {
            to_http_response!(
                cbor_response,
                StatusCode::OK,
                &result,
                ri.get_limit(),
                ri.get_offset()
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
        tracing::debug!("Registering resource: {}", name);

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
