pub mod cache;
pub mod handler;
pub mod input;
pub mod processor;
pub mod registry;

use std::collections::{HashMap, HashSet};
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use actix_web::App;
use actix_web::dev::Server;
use actix_web::http::StatusCode;
use actix_web::middleware::Logger;
use actix_web::web::ServiceConfig;
use actix_web::{HttpServer, web::Data};

use crate::XepakError;
use crate::cfg::{XepakConf, XepakSpecs};
use crate::server::cache::AppCache;
use crate::server::handler::EndpointHandler;
use crate::server::processor::PreProcessor;
use crate::server::registry::AppRegistry;
use crate::storage::{Storage, init_storage_connectors};
use crate::xepak_data::XepakValue;

pub use input::RequestInput;

const OFFSET_HEADER: &str = "X-Offset";
const LIMIT_HEADER: &str = "X-Limit";
pub const CONTENT_TYPE_CBOR: &str = "application/cbor";
pub const CONTENT_TYPE_JSON: &str = "application/json";

#[derive(Clone)]
pub struct XepakAppData {
    storage_links: HashMap<String, Storage>,

    shared_pre_processors: HashMap<String, PreProcessor>,

    default_pre_processors: Vec<PreProcessor>,

    registry: AppRegistry,

    cache: AppCache,
}

impl XepakAppData {
    pub fn get_data_source(&self, key: &str) -> Option<&Storage> {
        self.storage_links.get(key)
    }

    pub fn get_auth_data(&self, api_key: &str) -> Option<&(String, HashSet<String>)> {
        self.registry.auth.get(api_key)
    }

    pub fn get_secret(&self, key: &str) -> Option<&String> {
        self.registry.secrets.get(key)
    }
    pub fn get_registry_value(&self, key: &str) -> Option<&XepakValue> {
        self.registry.data.get(key)
    }

    pub async fn cache_get(&self, key: &str) -> Option<XepakValue> {
        self.cache.get(key).await
    }

    pub async fn cache_insert(&self, key: String, value: XepakValue) {
        self.cache.insert(key, value).await
    }
}

pub async fn init_server(
    conf_dir: PathBuf,
    config: XepakConf,
    specs: XepakSpecs,
) -> Result<Server, XepakError> {
    // if config.specs.deceit.is_empty() {
    //     log::warn!("Starting server without deceits in specs");
    // }
    let port = config.port;

    // Required to use with sqlx::Any connector
    sqlx::any::install_default_drivers();

    let storage_links = init_storage_connectors(&conf_dir, &config.storage).await?;

    let app_data = XepakAppData {
        storage_links,
        registry: AppRegistry::try_from(config.registry)?,
        shared_pre_processors: specs.shared_pre_processors,
        default_pre_processors: specs.default_pre_processors,
        cache: AppCache::new(10_000, Duration::from_mins(5)),
    };

    cache_cleanup(app_data.cache.clone());
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

    // Defining Endpoints here required all nested data to be send+sync
    let mut endpoints = Vec::new();
    for espec in specs.endpoint {
        endpoints.push(EndpointHandler::new(espec, &app_data)?);
    }

    let server = HttpServer::new(move || {
        let ep_config = endpoints.clone();
        App::new()
            .app_data(Data::new(app_data.clone()))
            // .service(web::scope("/") ...
            .configure(|cfg: &mut ServiceConfig| {
                for eh in ep_config {
                    cfg.service(eh);
                }
            })
            .wrap(Logger::default())
    })
    .bind((Ipv4Addr::UNSPECIFIED, port))
    .map_err(Arc::new)?
    .keep_alive(actix_web::http::KeepAlive::Disabled)
    .run();

    Ok(server)
}

fn cache_cleanup(cache: AppCache) {
    tokio::spawn(async move {
        // Use an interval instead of sleep to prevent timing drift
        let mut interval = tokio::time::interval(Duration::from_secs(60));

        loop {
            // The first tick completes immediately, so we skip it or just let it run
            interval.tick().await;
            cache.cleanup().await;
        }
    });
}

pub fn to_error_object(err: XepakError) -> (StatusCode, XepakValue) {
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
    (code, result.into())
}
