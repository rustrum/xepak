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
use actix_web::{HttpServer, web::Data};

use crate::XepakError;
use crate::cfg::{XepakConf, XepakSpecs};
use crate::schema::{Schema, convert_with_schema};
use crate::server::handler::EndpointHandler;
use crate::storage::{SqlxRequestArgs, Storage, StorageRequestArgs, init_storage_connectors};
use crate::types::XepakValue;

const OFFSET_HEADER: &str = "X-Offset";
const LIMIT_HEADER: &str = "X-Limit";
const CONTENT_TYPE_CBOR: &str = "application/cbor";
const CONTENT_TYPE_JSON: &str = "application/json";

#[derive(Clone)]
pub struct XepakAppData {
    storage_links: HashMap<String, Storage>,
}

impl XepakAppData {
    pub fn get_data_source(&self, key: &str) -> Option<&Storage> {
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
        endpoints.push(EndpointHandler::new(espec, &app_data)?);
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
    .bind((Ipv4Addr::UNSPECIFIED, port))
    .map_err(Arc::new)?
    .keep_alive(actix_web::http::KeepAlive::Disabled)
    .run();

    Ok(server)
}

#[derive(Debug, Clone)]
pub struct RequestArgs {
    pub(crate) schema: Schema,

    /// If true - fail on non existing args
    strict_schema: bool,

    /// Arguments parsed from URI (higher priority)
    pub(crate) path_args: Arc<HashMap<String, XepakValue>>,

    /// Final input args storage with schema applied
    pub(crate) args: Arc<HashMap<String, XepakValue>>,

    limit: usize,

    offset: usize,
}

impl RequestArgs {
    pub fn new(schema: Schema, strict_schema: bool, uri_pattern: &str, req_path: &str) -> Self {
        // Todo return result that will validate path_args against schema

        let mut path = actix_router::Path::new(req_path);

        let resource = actix_router::ResourceDef::new(uri_pattern);
        resource.capture_match_info(&mut path);

        let path_args = path
            .iter()
            .map(|(k, v)| (k.to_string(), XepakValue::Text(v.to_string())))
            .collect();

        RequestArgs {
            schema,
            strict_schema,
            path_args: Arc::new(path_args),
            args: Arc::new(Default::default()),
            limit: 0,
            offset: 0,
        }
    }

    pub fn new_in_script(args: HashMap<String, XepakValue>, limit: usize, offset: usize) -> Self {
        RequestArgs {
            schema: Schema::default(),
            strict_schema: false,
            path_args: Arc::new(Default::default()),
            args: Arc::new(args),
            limit,
            offset,
        }
    }

    pub fn has_any_arg(&self, arg_name: &str) -> bool {
        if self.path_args.contains_key(arg_name) {
            return true;
        }
        self.args.contains_key(arg_name)
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
        let value = self.get_arg_value(arg_name)?;

        let ivalue = match value.as_int() {
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

    /// Set argument value and apply schema conversion to it if any defined.
    /// Strict rule does apply only to `from_request = true` arguments.
    pub fn set_arg_with_schema(
        &mut self,
        name: String,
        value: XepakValue,
        from_request: bool,
    ) -> Result<(), XepakError> {
        let Some(args) = Arc::get_mut(&mut self.args) else {
            return Err(XepakError::Unexpected(
                "Must acquire args &mut reference here by design".to_string(),
            ));
        };

        let value = convert_with_schema(
            &self.schema,
            name.as_str(),
            value,
            self.strict_schema && from_request,
        )?;

        args.insert(name, value);

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

        // TODO should bind with respect to the schema
        Ok(value.bind_sqlx(query))
    }
}

pub fn to_error_object(err: XepakError) -> (StatusCode, HashMap<String, XepakValue>) {
    let mut result = HashMap::<String, XepakValue>::with_capacity(2);
    let mut code = StatusCode::from_u16(520).expect("Must not fail (^_^)");
    match err {
        XepakError::NotFound(msg) => {
            result.insert("code".to_string(), "not_found".into());
            result.insert("message".to_string(), msg.into());
            code = StatusCode::NOT_FOUND;
        }
        XepakError::Input(msg) => {
            code = StatusCode::BAD_REQUEST;
            result.insert("code".to_string(), "bad_request".into());
            result.insert("message".to_string(), msg.into());
        }
        XepakError::Decode(msg) | XepakError::WeScrewed(msg) => {
            code = StatusCode::INTERNAL_SERVER_ERROR;
            result.insert("code".to_string(), "internal_error".into());
            result.insert("message".to_string(), msg.into());
        }
        XepakError::Forbidden(msg) => {
            code = StatusCode::FORBIDDEN;
            result.insert("code".to_string(), "forbidden".into());
            result.insert("message".to_string(), msg.into());
        }
        _ => {
            result.insert("code".to_string(), "unknown_error".into());
        }
    }
    (code, result)
}
