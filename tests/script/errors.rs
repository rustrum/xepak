use super::common::*;

use reqwest::StatusCode;
use serde_json::Value as JsonValue;

use serial_test::serial;

#[tokio::test]
#[serial]
async fn test_script_errors_spec() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    let response = client::get("/script/lua/err").await;
    assert!(
        response.status().is_success(),
        "Status is {}",
        response.status()
    );
}

#[tokio::test]
#[serial]
async fn test_script_input_error() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    let response = client::get("/script/lua/err?etype=in").await;
    assert_eq!(StatusCode::BAD_REQUEST, response.status());

    let rjson = response.json::<JsonValue>().await.unwrap();

    assert_eq!("bad_request", rjson.get("code").unwrap().as_str().unwrap());
    assert_eq!(
        "Input error from LUA",
        rjson.get("message").unwrap().as_str().unwrap()
    );
}

#[tokio::test]
#[serial]
async fn test_script_not_found_error() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    let response = client::get("/script/lua/err?etype=nf").await;
    assert_eq!(StatusCode::NOT_FOUND, response.status());

    let rjson = response.json::<JsonValue>().await.unwrap();

    assert_eq!("not_found", rjson.get("code").unwrap().as_str().unwrap());
    assert_eq!(
        "Not found error from LUA",
        rjson.get("message").unwrap().as_str().unwrap()
    );
}

#[tokio::test]
#[serial]
async fn test_script_forbidden_error() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    let response = client::get("/script/lua/err?etype=fb").await;
    assert_eq!(StatusCode::FORBIDDEN, response.status());

    let rjson = response.json::<JsonValue>().await.unwrap();

    assert_eq!("forbidden", rjson.get("code").unwrap().as_str().unwrap());
    assert_eq!(
        "Forbidden error from LUA",
        rjson.get("message").unwrap().as_str().unwrap()
    );
}

#[tokio::test]
#[serial]
async fn test_script_server_error() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    let response = client::get("/script/lua/err?etype=srv").await;
    assert_eq!(StatusCode::INTERNAL_SERVER_ERROR, response.status());

    let rjson = response.json::<JsonValue>().await.unwrap();

    assert_eq!(
        "internal_error",
        rjson.get("code").unwrap().as_str().unwrap()
    );
    assert_eq!(
        "Server error from LUA",
        rjson.get("message").unwrap().as_str().unwrap()
    );
}
