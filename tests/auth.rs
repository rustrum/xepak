mod common;

use common::*;
use maplit::hashset;
use reqwest::StatusCode;
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    str::FromStr as _,
};
use xepak_rest::cfg::{load_conf_file, load_specs_from_dir};

use serial_test::serial;

#[derive(Deserialize, Debug)]
struct AuthScriptRecord {
    pub id: String,
    pub roles: HashSet<String>,
    pub is_admin: bool,
    pub is_manager: bool,
}

async fn check_script_auth(uri: &str, key: Option<&str>, expected: AuthScriptRecord) {
    let headers = if let Some(k) = key {
        HashMap::from([("x-api-key".to_string(), k.to_string())])
    } else {
        HashMap::new()
    };

    let response = client::get_resource(uri, HashMap::<String, String>::new(), headers).await;

    assert!(
        response.status().is_success(),
        "Request must be accepted, got {}",
        response.status()
    );

    let actual: AuthScriptRecord = client::extract_from_json(response, None).await;

    assert_eq!(actual.id, expected.id, "id mismatch");
    assert_eq!(actual.roles, expected.roles, "roles mismatch");
    assert_eq!(actual.is_admin, expected.is_admin, "is_admin mismatch");
    assert_eq!(
        actual.is_manager, expected.is_manager,
        "is_manager mismatch"
    );
}

#[tokio::test]
#[serial]
async fn auth_script_arguments() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    const URI: &str = "/auth/script/info";

    check_script_auth(
        URI,
        Some("BossKEY"),
        AuthScriptRecord {
            id: "boss".to_string(),
            roles: hashset! {"ADMIN".to_string(), "MANAGER".to_string()},
            is_admin: true,
            is_manager: true,
        },
    )
    .await;

    check_script_auth(
        URI,
        Some("ManagerKEY"),
        AuthScriptRecord {
            id: "manager".to_string(),
            roles: hashset! {"MANAGER".to_string()},
            is_admin: false,
            is_manager: true,
        },
    )
    .await;

    check_script_auth(
        URI,
        Some("AdminKEY"),
        AuthScriptRecord {
            id: "admin".to_string(),
            roles: Default::default(),
            is_admin: false,
            is_manager: false,
        },
    )
    .await;

    check_script_auth(
        URI,
        Some("UserKEY"),
        AuthScriptRecord {
            id: "user".to_string(),
            roles: Default::default(),
            is_admin: false,
            is_manager: false,
        },
    )
    .await;
}

#[tokio::test]
#[serial]
async fn auth_script_anon_arguments() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    const URI: &str = "/auth/script/anon";

    check_script_auth(
        URI,
        Some("BossKEY"),
        AuthScriptRecord {
            id: "boss".to_string(),
            roles: hashset! {"ADMIN".to_string(), "MANAGER".to_string()},
            is_admin: true,
            is_manager: true,
        },
    )
    .await;

    check_script_auth(
        URI,
        None,
        AuthScriptRecord {
            id: "".to_string(),
            roles: Default::default(),
            is_admin: false,
            is_manager: false,
        },
    )
    .await;
}

async fn assert_nodef_access() {
    let response = client::get("/auth/script/nodef").await;
    assert!(
        response.status().is_success(),
        "Nodef must be always accessible. Got response {}",
        response.status()
    );
}

#[tokio::test]
#[serial]
async fn auth_default_pre_processor() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    const URI: &str = "/auth/script/public";

    // Without default pre-processors, public endpoint should be accessible without key
    let response = client::get(URI).await;
    assert!(
        response.status().is_success(),
        "Public endpoint must be accessible without API key when no default pre-processor is set, got {}",
        response.status()
    );

    assert_nodef_access().await;

    drop(_server);

    // Now adding default auth pre-processor to configuration
    let config = load_conf_file(CONFIG_FILE).expect("Should have valid config");
    let specs_dir = PathBuf::from_str(SPECS_DIR).expect("Specs dir must exists");
    let mut specs = load_specs_from_dir(specs_dir).expect("Should have valid specs");

    use xepak_rest::server::processor::PreProcessor;
    specs
        .default_pre_processors
        .push(PreProcessor::SimpleAuthentication {
            anonymous_auth: false,
        });

    let _server = XepakTestServer::start(config, specs, INIT_DELAY_DEFAULT).await;

    // With default pre-processor, public endpoint should require API key
    let response = client::get(URI).await;
    assert_eq!(
        StatusCode::FORBIDDEN,
        response.status(),
        "Public endpoint must be rejected without API key when default pre-processor is set"
    );

    // Valid API keys should still work
    let valid_keys = ["BossKEY", "ManagerKEY", "AdminKEY", "UserKEY"];
    for key in valid_keys {
        let response = client::get_resource(
            URI,
            HashMap::<String, String>::new(),
            HashMap::from([("x-api-key".to_string(), key.to_string())]),
        )
        .await;
        assert!(
            response.status().is_success(),
            "Valid key \"{key}\" must be accepted with default pre-processor, got {}",
            response.status()
        );
    }

    // This one skips default pre-processors so it should be public
    assert_nodef_access().await;
}

#[tokio::test]
#[serial]
async fn auth_require_key() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    const URI: &str = "/auth/script/info";

    // No API key provided
    let response = client::get(URI).await;
    assert_eq!(
        StatusCode::FORBIDDEN,
        response.status(),
        "Without API key must be rejected"
    );

    // Invalid API key
    check_keys_are_rejected(URI, &["My_Invalid_Key"], "Invalid API key must be rejected").await;

    // Valid API keys from tests_cfg.toml
    let valid_keys = ["BossKEY", "ManagerKEY", "AdminKEY", "UserKEY"];
    for key in valid_keys {
        let response = client::get_resource(
            URI,
            HashMap::<String, String>::new(),
            HashMap::from([("x-api-key".to_string(), key.to_string())]),
        )
        .await;
        assert!(
            response.status().is_success(),
            "Valid key \"{key}\" must be accepted, got {}",
            response.status()
        );
    }
}

#[tokio::test]
#[serial]
async fn auth_boss_endpoint() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    const URI: &str = "/auth/posts/boss";

    // Only tokens with ADMIN role must be allowed
    let response = client::get_resource(
        URI,
        HashMap::<String, String>::new(),
        HashMap::from([("x-api-key".to_string(), "BossKEY".to_string())]),
    )
    .await;
    assert!(
        response.status().is_success(),
        "BossKEY (ADMIN) must be accepted, got {}",
        response.status()
    );

    // Tokens without ADMIN role must be rejected
    check_keys_are_rejected(URI, &["ManagerKEY", "AdminKEY", "UserKEY"], "No ADMIN role").await;
}

#[tokio::test]
#[serial]
async fn auth_manager_endpoint() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    const URI: &str = "/auth/posts/manager";

    // Only tokens with MANAGER role must be allowed
    for key in ["BossKEY", "ManagerKEY"] {
        let response = client::get_resource(
            URI,
            HashMap::<String, String>::new(),
            HashMap::from([("x-api-key".to_string(), key.to_string())]),
        )
        .await;
        assert!(
            response.status().is_success(),
            "Key \"{key}\" (MANAGER role) must be accepted, got {}",
            response.status()
        );
    }

    // Tokens without MANAGER role must be rejected
    check_keys_are_rejected(URI, &["AdminKEY", "UserKEY"], "No MANAGER role").await;
}

#[tokio::test]
#[serial]
async fn auth_admin_endpoint() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    const URI: &str = "/auth/posts/admin";

    // #admin rule matches by user id, only token with id "admin" must be allowed
    let response = client::get_resource(
        URI,
        HashMap::<String, String>::new(),
        HashMap::from([("x-api-key".to_string(), "AdminKEY".to_string())]),
    )
    .await;
    assert!(
        response.status().is_success(),
        "AdminKEY (id=admin) must be accepted, got {}",
        response.status()
    );

    // Tokens with other ids must be rejected
    check_keys_are_rejected(URI, &["BossKEY", "ManagerKEY", "UserKEY"], "id != admin").await;
}

async fn check_keys_are_rejected(uri: &str, keys: &[&str], message: &str) {
    for key in keys {
        let response = client::get_resource(
            uri,
            HashMap::<String, String>::new(),
            HashMap::from([("x-api-key".to_string(), (*key).to_string())]),
        )
        .await;
        assert_eq!(
            StatusCode::FORBIDDEN,
            response.status(),
            "{message} Key \"{key}\" must be rejected"
        );
    }
}
