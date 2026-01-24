pub mod handler;

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;

use actix_web::body::BoxBody;
use actix_web::dev::{HttpServiceFactory, Server};
use actix_web::http::StatusCode;
use actix_web::http::header::{ACCEPT, CONTENT_TYPE};
use actix_web::middleware::Logger;
use actix_web::web::ServiceConfig;
use actix_web::{App, Handler, HttpResponse, HttpResponseBuilder};
use actix_web::{
    HttpRequest, HttpServer,
    web::{self, Bytes, Data},
};
use rhai::{AST, Engine, ParseError};
use serde::Serialize;

use crate::XepakError;
use crate::cfg::{EndpointSpecs, ResourceSpecs, XepakConf, XepakSpecs};
use crate::server::handler::EndpointHandler;
use crate::storage::{
    ResourceRequest, SqlxRequestArgs, Storage, StorageRequestArgs, init_storage_connectors,
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

    // Required to use with sqlx::Any connector
    sqlx::any::install_default_drivers();

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

    let mut endpoints = Vec::new();
    for espec in specs.endpoint {
        endpoints.push(EndpointHandler::new(espec)?);
    }

    let server = HttpServer::new(move || {
        let ep_config = endpoints.clone();
        let mut app = App::new()
            .app_data(Data::new(app_data.clone()))
            // .service(web::scope("/") ...
            .configure(|cfg: &mut ServiceConfig| {
                for eh in ep_config {
                    cfg.service(eh);
                }
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

#[derive(Debug, Clone)]
pub struct RequestArgs {
    query_args: Arc<HashMap<String, String>>,
    path_args: Arc<HashMap<String, String>>,
}

impl RequestArgs {
    pub fn new(uri: &str, req: &HttpRequest) -> Self {
        let qstring = req.uri().query().unwrap_or_default();
        let query_args =
            if let Ok(qa) = serde_urlencoded::from_str::<HashMap<String, String>>(qstring) {
                qa
            } else {
                tracing::warn!("Can't decode query string from URL");
                Default::default()
            };

        let mut path = actix_router::Path::new(req.path().to_string());

        let resource = actix_router::ResourceDef::new(uri);
        resource.capture_match_info(&mut path);

        let path_args = path
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        RequestArgs {
            query_args: Arc::new(query_args),
            path_args: Arc::new(path_args),
        }
    }

    pub fn has_any_arg(&self, arg_name: &str) -> bool {
        if self.path_args.contains_key(arg_name) {
            return true;
        }
        return self.query_args.contains_key(arg_name);
    }

    pub fn get_arg_value(&self, argument: &str) -> Option<&String> {
        let path_arg: Option<&String> = self.path_args.get(argument);
        if path_arg.is_none() {
            self.query_args.get(argument)
        } else {
            path_arg
        }
    }
}

#[derive(Debug)]
struct RequestInput<'a> {
    specs: &'a EndpointSpecs,
    args: RequestArgs,
}

impl<'a> RequestInput<'a> {
    fn new(specs: &'a EndpointSpecs, req: &HttpRequest) -> Self {
        Self {
            args: RequestArgs::new(&specs.uri, req),
            specs,
        }
    }

    fn get_offset(&self) -> usize {
        let Some(svalue) = self.args.get_arg_value(&self.specs.offset_arg) else {
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
        let Some(svalue) = self.args.get_arg_value(&self.specs.limit_arg) else {
            return self.specs.fetch_limit;
        };

        match svalue.parse() {
            Ok(v) => {
                if v > self.specs.fetch_limit {
                    self.specs.fetch_limit
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

impl StorageRequestArgs for RequestInput<'_> {
    fn get_rows_limit(&self) -> usize {
        self.get_limit()
    }

    fn get_rows_offset(&self) -> usize {
        self.get_offset()
    }
}

impl SqlxRequestArgs for RequestInput<'_> {
    fn bind_arg<'a>(
        &'a self,
        arg_name: &str,
        query: sqlx::query::Query<'a, sqlx::Any, sqlx::any::AnyArguments<'a>>,
    ) -> Result<sqlx::query::Query<'a, sqlx::Any, sqlx::any::AnyArguments<'a>>, XepakError> {
        let Some(value) = self.args.get_arg_value(arg_name) else {
            return Err(XepakError::Input(format!(
                "Can't bind argument '{arg_name}' - does not exists in request."
            )));
        };

        // TODO should bind with resepect of the schema
        Ok(query.bind(value.as_str()))
    }
}

pub fn to_error_object(err: XepakError) -> (StatusCode, HashMap<String, String>) {
    let mut result = HashMap::<String, String>::with_capacity(2);
    let mut code = StatusCode::INTERNAL_SERVER_ERROR;
    match err {
        XepakError::Input(msg) => {
            code = StatusCode::BAD_REQUEST;
            result.insert("code".to_string(), "bad_request".to_string());
            result.insert("message".to_string(), msg);
        }
        XepakError::NotFound(msg) => {
            result.insert("code".to_string(), "not_found".to_string());
            result.insert("message".to_string(), msg);
            code = StatusCode::NOT_FOUND;
        }
        _ => {
            result.insert("code".to_string(), "internal_error".to_string());
        }
    }

    (code, result)
}
