use std::{fs, path::PathBuf, str::FromStr};

use actix_web::dev::ServerHandle;

use xepak_rest::{cfg::*, server::init_server, storage::StorageSettings};

pub const DEFAULT_TEST_PORT: u16 = 4321;

pub const INIT_DELAY_DEFAULT: usize = 1;

/// Directory with basic test configuration
pub const CONFIG_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/xepak");

/// Default configuration file
pub const CONFIG_FILE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/xepak/config.toml");

/// Default test specs directory
pub const SPECS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/xepak/specs");

pub fn get_target_test_dir() -> PathBuf {
    let cargo_target = match std::env::var("CARGO_TARGET_DIR") {
        Ok(dir) => dir,
        Err(_) => PathBuf::from_str(env!("CARGO_MANIFEST_DIR"))
            .unwrap()
            .join("target")
            .to_string_lossy()
            .to_string(),
    };

    let dir_path = PathBuf::from_str(&cargo_target)
        .expect("Must work")
        .join("tests");

    if !dir_path.exists() {
        std::fs::create_dir_all(&dir_path).expect("Must create test dir");
    }

    dir_path
}

pub fn clean_directory_files(dir_path: &PathBuf) -> std::io::Result<()> {
    if !dir_path.exists() {
        return Ok(());
    }

    for entry in fs::read_dir(dir_path)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() {
            fs::remove_dir_all(path)?;
        } else {
            fs::remove_file(path)?;
        }
    }
    Ok(())
}

#[allow(irrefutable_let_patterns)]
pub fn build_xepak_config(config_file: &str) -> XepakConf {
    let mut config = load_conf_file(config_file).expect("Should have valid config");

    config.port = DEFAULT_TEST_PORT;

    let target_test_dir = get_target_test_dir();
    clean_directory_files(&target_test_dir).expect("Must be done");

    // Reconfigure DB here
    let storage_settings = config.storage[0].clone();
    config.storage[0] = if let StorageSettings::Sqlite {
        id,
        create_db,
        wal,
        migrations_dir,
        ..
    } = storage_settings
    {
        StorageSettings::Sqlite {
            id,
            file: target_test_dir
                .join("test_db.sqlite3")
                .to_string_lossy()
                .to_string(),
            create_db,
            wal,
            migrations_dir,
        }
    } else {
        panic!("Do not expect other test config here");
    };

    config
}

pub async fn init_default_test_server(delay_ms: usize) -> XepakTestServer {
    let config = build_xepak_config(CONFIG_FILE);
    let specs_dir = PathBuf::from_str(SPECS_DIR).expect("Specs dir must exists");
    let specs = load_specs_from_dir(specs_dir).expect("Should have valid specs");

    XepakTestServer::start(config, specs, delay_ms).await
}

pub struct XepakTestServer {
    server_handle: ServerHandle,
    #[allow(dead_code)]
    handle: std::thread::JoinHandle<Result<(), std::io::Error>>,
}

impl Drop for XepakTestServer {
    fn drop(&mut self) {
        let stopping = self.server_handle.stop(false);

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let spawn_handle = handle.spawn(stopping);
            while spawn_handle.is_finished() {
                // It looks stupid but it work when running inside a Tokio runtime
                // I was not able to use something like blocks_on here
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        } else {
            // Not inside a Tokio runtime
            let trt = tokio::runtime::Runtime::new().unwrap();
            trt.block_on(stopping);
        }
    }
}

impl XepakTestServer {
    /// Start a test server with the given configuration.
    /// Arguments:
    /// * `config`: The configuration for the server.
    /// * `specs`: Rest specification.
    /// * `delay_ms`: Delay after server start to let slow envs to inintialize.
    pub async fn start(config: XepakConf, specs: XepakSpecs, delay_ms: usize) -> XepakTestServer {
        let conf_dir = PathBuf::from_str(CONFIG_DIR).expect("(O_O) Path ?!");

        let server = init_server(conf_dir, config, specs)
            .await
            .expect("Test server must start");

        let server_handle = server.handle();
        let handle = std::thread::spawn(move || {
            actix_web::rt::Runtime::new()
                .expect("Runtime expected")
                .block_on(server)
        });

        if delay_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(delay_ms as u64));
        }

        Self {
            handle,
            server_handle,
        }
    }
}

pub mod client {

    use reqwest::{
        Response,
        header::{HeaderMap, HeaderName, HeaderValue},
    };
    use serde_json::Value;
    use std::fmt::Display;
    use url::form_urlencoded;
    use xepak_rest::types::XepakValue;

    use sqlx_core::HashMap;

    use super::*;

    fn api_url(uri: &str) -> String {
        format!("http://localhost:{DEFAULT_TEST_PORT}{uri}")
    }

    fn to_query_string<T: Display>(qargs: HashMap<String, T>) -> String {
        let mut query = form_urlencoded::Serializer::new(String::new());

        for (k, v) in qargs {
            query.append_pair(&k, &v.to_string());
        }

        query.finish().to_string()
    }

    pub async fn get_with_query<T: Display>(uri: &str, query_args: HashMap<String, T>) {
        get_resource(uri, query_args, HashMap::<String, T>::new()).await;
    }

    pub async fn get(uri: &str) -> Response {
        get_resource(
            uri,
            HashMap::<String, String>::new(),
            HashMap::<String, String>::new(),
        )
        .await
    }

    pub async fn get_resource<T: Display>(
        uri: &str,
        query_args: HashMap<String, T>,
        headers: HashMap<String, T>,
    ) -> Response {
        let client = reqwest::Client::new();

        let mut uri = api_url(uri);

        if !query_args.is_empty() {
            let qs = to_query_string(query_args);
            uri = format!("{uri}?{qs}");
        }

        let hm: HeaderMap = headers
            .into_iter()
            .map(|(k, v)| {
                (
                    HeaderName::from_str(&k).expect("Valid header key required"),
                    HeaderValue::from_str(&v.to_string()).expect("Valid header value required"),
                )
            })
            .collect();

        let builder = client.get(uri).headers(hm);

        let response = builder.send().await.expect("Request must not fail");

        response
    }

    pub async fn response_json(response: Response) -> reqwest::Result<Value> {
        let jv: Value = response.json().await?;
        // Vec<XepakValue>::
        Ok(jv)
    }

    pub async fn post_resource<T: Display>(
        uri: &str,
        query_args: HashMap<String, T>,
        headers: HashMap<String, T>,
    ) {
        let client = reqwest::Client::new();

        let mut uri = api_url(uri);

        if !query_args.is_empty() {
            let qs = to_query_string(query_args);
            uri = format!("{uri}?{qs}");
        }

        let response = client.get(uri).send().await.expect("Request failed");
    }
}
