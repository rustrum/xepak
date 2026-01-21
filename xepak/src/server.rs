use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use actix_web::body::BoxBody;
use actix_web::dev::{HttpServiceFactory, Server};
use actix_web::http::header::{ACCEPT, CONTENT_TYPE};
use actix_web::middleware::Logger;
use actix_web::web::ServiceConfig;
use actix_web::{App, Handler, HttpResponse};
use actix_web::{
    HttpRequest, HttpServer,
    web::{self, Bytes, Data},
};
use serde::Serialize;
use sql_key_args::{ParametrizedQuery, ParametrizedQueryRef, SqlLexer};
use tracing_subscriber::fmt::format;

use crate::XepakError;
use crate::cfg::{EndpointSpecs, ResourceSpecs, XepakConf, XepakSpecs};
use crate::storage::{
    RequestArgs, ResourceRequest, SqlxRequestArgs, Storage, init_storage_connectors,
};

const OFFSET_HEADER: &str = "X-Offset";
const LIMIT_HEADER: &str = "X-Limit";
const CONTENT_TYPE_CBOR: &str = "application/cbor";
const CONTENT_TYPE_JSON: &str = "application/json";

#[derive(Clone)]
struct XepakAppData {
    storage_links: HashMap<String, Storage>,
}

impl XepakAppData {
    fn get_data_source(&self, key: &str) -> Option<&Storage> {
        self.storage_links.get(key)
    }
}

fn init_app_data() -> XepakAppData {
    unimplemented!()
}

pub async fn init_server(
    conf_dir: PathBuf,
    config: XepakConf,
    specs: XepakSpecs,
) -> Result<Server, XepakError> {
    // if config.specs.deceit.is_empty() {
    //     log::warn!("Starting server without deceits in specs");
    // }
    // let port = config.port;
    let port = 8080;

    let storage_links = init_storage_connectors(&conf_dir, &config.storage).await;
    let app_data = XepakAppData { storage_links };
    // let data: Data<ApateState> = Data::new(config.into_state());

    // let mut app = App::new()
    // // .app_data(data.clone())
    // .wrap(Logger::default());
    // #[cfg(feature = "server")]
    // {
    //     app = app
    //         .service(web::scope(handlers::ADMIN_API).configure(handlers::admin_service_config));
    // }
    // app.default_service(web::to(handlers::apate_server_handler));

    let server = HttpServer::new(move || {
        let mut app = App::new()
            .app_data(Data::new(app_data.clone()))
            // .service(web::scope("/") ...
            .configure(|cfg: &mut ServiceConfig| {
                configure_endpoint_handlers(cfg, &specs.endpoint);
            })
            .wrap(Logger::default());
        // let endpoint = web::scope("some/endpoint").configure(cfg_fn)
        // web::sc
        // app.service()
        app
    })
    .bind((Ipv4Addr::UNSPECIFIED, port))?
    .keep_alive(actix_web::http::KeepAlive::Disabled)
    .run();

    Ok(server)
}

pub fn configure_endpoint_handlers(cfg: &mut ServiceConfig, endpoints: &[EndpointSpecs]) {
    for ep in endpoints {
        cfg.service(EndpointHandler::new(ep.clone()));
    }
}

struct RequestInput<'a> {
    specs: &'a EndpointSpecs,
    query_args: HashMap<String, String>,
    path_args: HashMap<String, String>,
}

impl<'a> RequestInput<'a> {
    fn new(specs: &'a EndpointSpecs, req: &HttpRequest) -> Self {
        let mut this = Self {
            specs,
            query_args: Default::default(),
            path_args: Default::default(),
        };

        let qstring = req.uri().query().unwrap_or_default();
        if let Ok(qa) = serde_urlencoded::from_str::<HashMap<String, String>>(qstring) {
            this.query_args = qa;
        } else {
            tracing::warn!("Can't decode query string from URL");
        }

        let mut path = actix_router::Path::new(req.path().to_string());

        let resource = actix_router::ResourceDef::new(specs.uri.clone());
        resource.capture_match_info(&mut path);

        this.path_args = path
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        this
    }

    fn get_arg_value(&self, argument: &str) -> Option<&String> {
        let pa: Option<&String> = self.path_args.get(argument);
        if pa.is_none() {
            self.query_args.get(argument)
        } else {
            None
        }
    }

    fn get_offset(&self) -> usize {
        let Some(svalue) = self.get_arg_value(&self.specs.offset_arg) else {
            return 0;
        };
        match svalue.parse() {
            Ok(v) => v,
            Err(_) => {
                tracing::debug!(
                    "Can't parse argument \"{}\" value {svalue} as number",
                    self.specs.offset_arg
                );
                0
            }
        }
    }

    fn get_limit(&self) -> usize {
        let Some(svalue) = self.get_arg_value(&self.specs.limit_arg) else {
            return self.specs.limit_max;
        };

        match svalue.parse() {
            Ok(v) => {
                if v > self.specs.limit_max {
                    self.specs.limit_max
                } else {
                    v
                }
            }
            Err(_) => {
                tracing::debug!(
                    "Can't parse argument \"{}\" value {svalue} as number",
                    self.specs.limit_arg
                );
                0
            }
        }
    }
}

impl RequestArgs for RequestInput<'_> {
    fn get_rows_limit(&self) -> usize {
        self.get_limit()
    }

    fn get_rows_offset(&self) -> usize {
        self.get_offset()
    }
}

impl SqlxRequestArgs for RequestInput<'_> {
    fn bind_arg<DB>(
        &self,
        arg_name: &str,
        query: sqlx::query::Query<'_, DB, <DB>::Arguments<'_>>,
    ) -> Result<sqlx::query::Query<'_, DB, <DB>::Arguments<'_>>, XepakError>
    where
        DB: sqlx::Database,
    {
        let Some(value) = self.get_arg_value(arg_name) else {
            return Err(XepakError::Input(format!(
                "Can't bind argument '{arg_name}' - does not exists in request."
            )));
        };

        // TODO should bind with resepect of the schema
        // Ok(query.bind(value.as_str()))
        // Ok(query.bind(value.as_str()))

        todo!("FUCK not working implementation")
    }
}

type EndpointHandlerArgs = (HttpRequest, Data<XepakAppData>, Bytes);

#[derive(Clone)]
struct EndpointHandler {
    ep: Arc<EndpointSpecs>,
}

impl EndpointHandler {
    fn new(ep: EndpointSpecs) -> Self {
        Self { ep: Arc::new(ep) }
    }

    async fn handle(
        &self,
        req: HttpRequest,
        state: Data<XepakAppData>,
        _body: Bytes,
    ) -> HttpResponse {
        tracing::debug!("Handler called for {:?}", self.ep);
        let mut return_one_record = false;
        let result = match &self.ep.resource {
            ResourceSpecs::Sql {
                data_source,
                query,
                one_record: single_record,
                paginated,
            } => {
                return_one_record = *single_record;

                let ds = state.get_data_source(&data_source).expect("TODO");

                let ri = RequestInput::new(&self.ep, &req);
                let rr = ResourceRequest::new(&query, &ri);
                ds.execute(rr).await.expect("TODO QUERY")
            }
        };

        if let Some(accept) = req.headers().get(ACCEPT)
            && accept.eq(CONTENT_TYPE_CBOR)
        {
            to_cbor_response(&result, 0, 0)
        } else {
            to_json_response(&result, 0, 0)
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

fn to_json_response<T: Serialize>(data: &T, offset: usize, limit: usize) -> HttpResponse<BoxBody> {
    match serde_json::to_string(data) {
        Ok(body) => {
            let mut resp = HttpResponse::Ok();
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
    data: &T,
    offset: usize,
    limit: usize,
) -> HttpResponse<BoxBody> {
    match minicbor::to_vec(data) {
        Ok(body) => {
            let mut resp = HttpResponse::Ok();
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
