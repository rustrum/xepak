mod common;

use common::*;
use reqwest::StatusCode;
use serde_json::Value as JsonValue;

use serial_test::serial;

#[tokio::test]
#[serial]
async fn allow_methods_config() {
    // init_logger();
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    let resp = client::get("/input/method/default").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.json::<JsonValue>().await.unwrap();
    assert_eq!(body["method"], "GET");

    let resp = client::post("/input/method/default", "").await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = resp.json::<JsonValue>().await.unwrap();
    assert_eq!(body["code"], "bad_request");
    assert_eq!(body["message"], "Request method POST not allowed!");

    let resp = client::post("/input/method/post-put", "").await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.json::<JsonValue>().await.unwrap();
    println!("{body}");
    assert_eq!(body["method"], "POST");

    let resp = client::get("/input/method/post-put").await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = resp.json::<JsonValue>().await.unwrap();
    assert_eq!(body["code"], "bad_request");
    assert_eq!(body["message"], "Request method GET not allowed!");
}
