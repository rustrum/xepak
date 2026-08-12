#[path = "../common.rs"]
mod common;

mod errors;
mod queries;

use std::{collections::HashMap, env, path::PathBuf, str::FromStr};

use common::*;
use maplit::hashmap;
use serial_test::serial;
use xepak_rest::{
    cfg::{load_conf_file, load_specs_from_dir},
    server::registry::SecretsRegistrySpec,
    xepak_data::XepakValue,
};

#[tokio::test]
#[serial]
async fn script_access_registry_kv() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;
    let response = client::get("/script/lua/registry").await;
    let result: HashMap<String, XepakValue> = client::extract_from_json(response, None).await;

    assert_eq!(result.get("int_val"), Some(&XepakValue::Integer(1)),);
    assert_eq!(result.get("float_val"), Some(&XepakValue::Float(2.2)),);
    assert_eq!(
        result.get("string_val"),
        Some(&XepakValue::Text("registry_val".to_string())),
    );

    let expected_map = hashmap! {
        "host".to_string() => XepakValue::Text("localhost".to_string()),
        "weird.key".to_string() => XepakValue::Text("some_value".to_string()),
    };
    assert_eq!(result.get("map_val"), Some(&XepakValue::Map(expected_map)),);
}

#[tokio::test]
#[serial]
async fn script_access_secrets_registry() {
    unsafe {
        env::set_var("SECRET_ENV_1", "secret_1_real_value");
        env::set_var("SECRET_ENV_2", "secret_2_real_value");
    }

    let mut config = load_conf_file(CONFIG_FILE).expect("Should have valid config");
    let specs_dir = PathBuf::from_str(SPECS_DIR).expect("Specs dir must exists");
    let specs = load_specs_from_dir(specs_dir).expect("Should have valid specs");

    // Tweaking secrets configuration
    let mut secrets_map = toml::map::Map::new();
    secrets_map.insert(
        "secret1".to_string(),
        toml::Value::try_from(SecretsRegistrySpec::Env {
            name: "SECRET_ENV_1".to_string(),
        })
        .unwrap(),
    );
    secrets_map.insert(
        "secret2".to_string(),
        toml::Value::try_from(SecretsRegistrySpec::Env {
            name: "SECRET_ENV_2".to_string(),
        })
        .unwrap(),
    );
    secrets_map.insert(
        "secret4".to_string(),
        toml::Value::try_from(SecretsRegistrySpec::Text {
            value: "raw_text_value_4".to_string(),
        })
        .unwrap(),
    );
    let secrets_def = toml::Value::Table(secrets_map);
    config.registry.insert("secrets".to_string(), secrets_def);

    let _server = XepakTestServer::start(config, specs, INIT_DELAY_DEFAULT).await;

    let response = client::get("/script/lua/secrets").await;

    let result: HashMap<String, String> = client::extract_from_json(response, None).await;

    assert_eq!(result["secret_1"], "secret_1_real_value");
    assert_eq!(result["secret_2"], "secret_2_real_value");
    assert_eq!(result["secret_4"], "raw_text_value_4");
    assert!(!result.contains_key("secret_3"));
}
