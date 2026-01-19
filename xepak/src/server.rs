use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::pin::Pin;
use std::ptr::addr_eq;
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

use crate::XepakError;
use crate::cfg::{EndpointSpecs, ResourceSpecs, XepakConf, XepakSpecs};
use crate::storage::{Storage, init_storage_connectors};

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
        let result = match &self.ep.resource {
            ResourceSpecs::Sql { data_source, query } => {
                let ds = state.get_data_source(&data_source).expect("TODO");
                ds.execute(query).await.expect("TODO QUERY")
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
