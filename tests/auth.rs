mod common;

use common::*;
use reqwest::StatusCode;
use std::collections::HashMap;

use serial_test::serial;

#[tokio::test]
#[serial]
async fn auth_public_endpoint() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    const URI: &str = "/auth/posts/public";

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
