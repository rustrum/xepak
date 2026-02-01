pub mod handler;
pub mod processor;

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;

use actix_web::App;
use actix_web::dev::Server;
use actix_web::http::StatusCode;
use actix_web::middleware::Logger;
use actix_web::web::ServiceConfig;
use actix_web::{HttpRequest, HttpServer, web::Data};

use crate::XepakError;
use crate::cfg::{EndpointSpecs, XepakConf, XepakSpecs};
use crate::server::handler::EndpointHandler;
use crate::storage::{SqlxRequestArgs, Storage, StorageRequestArgs, init_storage_connectors};
use crate::types::{Schema, XepakValue};

const OFFSET_HEADER: &str = "X-Offset";
const LIMIT_HEADER: &str = "X-Limit";
const CONTENT_TYPE_CBOR: &str = "application/cbor";
const CONTENT_TYPE_JSON: &str = "application/json";

#[derive(Clone)]
pub struct XepakAppData {
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
    /// Arguments parsed from URI (higher priority)
    path_args: Arc<HashMap<String, XepakValue>>,
    /// Final input args storage with schema applied
    args: Arc<HashMap<String, XepakValue>>,

    limit: usize,
    offset: usize,
}

impl RequestArgs {
    pub fn new(uri_pattern: &str, req_path: &str) -> Self {
        // Todo return result that will validate path_args against schema

        let mut path = actix_router::Path::new(req_path);

        let resource = actix_router::ResourceDef::new(uri_pattern);
        resource.capture_match_info(&mut path);

        let path_args = path
            .iter()
            .map(|(k, v)| (k.to_string(), XepakValue::Text(v.to_string())))
            .collect();

        RequestArgs {
            path_args: Arc::new(path_args),
            args: Arc::new(Default::default()),
            limit: 0,
            offset: 0,
        }
    }

    pub fn has_any_arg(&self, arg_name: &str) -> bool {
        if self.path_args.contains_key(arg_name) {
            return true;
        }
        return self.args.contains_key(arg_name);
    }

    pub fn get_arg_value(&self, argument: &str) -> Option<&XepakValue> {
        let path_arg = self.path_args.get(argument);
        if path_arg.is_none() {
            self.args.get(argument)
        } else {
            path_arg
        }
    }

    pub fn get_limit(&self) -> usize {
        self.limit
    }

    pub fn get_offset(&self) -> usize {
        self.offset
    }

    /// Will try to parse limit/offset from existing arguments if possible.
    /// Output debug message if parsing failed.
    pub fn parse_offset_limit(&mut self, offset_arg: &str, limit_arg: &str, limit_max: usize) {
        if !limit_arg.is_empty() {
            self.limit = self.parse_usize_from(limit_arg).unwrap_or(limit_max);
        }
        if !offset_arg.is_empty() {
            self.offset = self.parse_usize_from(offset_arg).unwrap_or_default();
        }
    }

    fn parse_usize_from(&self, arg_name: &str) -> Option<usize> {
        let Some(value) = self.get_arg_value(arg_name) else {
            return None;
        };

        let ivalue = match value.as_integer() {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!("Can't get int from arg {arg_name}: {e}");
                return None;
            }
        };

        if ivalue < 0 || ivalue > usize::MAX as i128 {
            tracing::debug!("Value not in range for arg {arg_name}: {ivalue}");
            return None;
        }

        Some(ivalue as usize)
    }

    /// Could return error if input value can't be converted into proper type according to schema or validated.
    pub fn set_arg_validate(
        &mut self,
        name: String,
        value: XepakValue,
        // schema: &Schema,
    ) -> Result<(), XepakError> {
        let Some(args) = Arc::get_mut(&mut self.args) else {
            return Err(XepakError::Unexpected(
                "Must acquire args &mut reference here by design".to_string(),
            ));
        };

        args.insert(name, value);
        // TODO deal with schema
        // probably request args must be available for the request schema
        Ok(())
    }
}

impl StorageRequestArgs for RequestArgs {
    fn get_rows_limit(&self) -> usize {
        self.get_limit()
    }

    fn get_rows_offset(&self) -> usize {
        self.get_offset()
    }
}

impl SqlxRequestArgs for RequestArgs {
    fn bind_arg<'a>(
        &'a self,
        arg_name: &str,
        query: sqlx::query::Query<'a, sqlx::Any, sqlx::any::AnyArguments<'a>>,
    ) -> Result<sqlx::query::Query<'a, sqlx::Any, sqlx::any::AnyArguments<'a>>, XepakError> {
        let Some(value) = self.get_arg_value(arg_name) else {
            return Err(XepakError::Input(format!(
                "Can't bind argument '{arg_name}' - does not exists in request."
            )));
        };

        // TODO should bind with resepect of the schema
        Ok(value.bind_sqlx(query))
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
