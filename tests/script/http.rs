use super::common::*;

use apate::{
    ApateConfigBuilder,
    deceit::{DeceitBuilder, DeceitResponseBuilder},
    output::OutputType,
    test::ApateTestServer,
};
use reqwest::{Client, StatusCode};
use serde_json::Value as JsonValue;

use serial_test::serial;

const INIT_DELAY_DEFAULT: usize = 1;

fn init_test_server() -> ApateTestServer {
    const GET_RHAI_SCRIPT: &str = r#"
    let headers = ctx.load_headers();
    let data = #{ 
        message: "GET Success", 
        api_key: headers.get("x-api-key"),
    };
    return to_json_blob(data);
    "#;

    const POST_RHAI_SCRIPT: &str = r#"
    let headers = ctx.load_headers();
    let input = from_json_blob(ctx.load_body());
    let data = #{ 
        message: "POST Success", 
        api_key: headers.get("x-api-key"),
        input: input,
    };
    return to_json_blob(data);
    "#;

    let config = ApateConfigBuilder::default()
        .add_deceit(
            DeceitBuilder::with_uris(&["/http/get"])
                .require_method("GET")
                .add_header("Content-Type", "application/json")
                .add_response(
                    DeceitResponseBuilder::default()
                        .code(200)
                        .with_output_type(OutputType::Rhai)
                        .with_output(GET_RHAI_SCRIPT)
                        .build(),
                )
                .build(),
        )
        .add_deceit(
            DeceitBuilder::with_uris(&["/http/post"])
                .require_method("POST")
                .add_header("Content-Type", "application/json")
                .add_response(
                    DeceitResponseBuilder::default()
                        .code(200)
                        .with_output_type(OutputType::Rhai)
                        .with_output(POST_RHAI_SCRIPT)
                        .build(),
                )
                .build(),
        )
        .add_deceit(
            DeceitBuilder::with_uris(&["/http/missing"])
                .require_method("GET")
                .add_header("Content-Type", "application/json")
                .add_response(
                    DeceitResponseBuilder::default()
                        .code(404)
                        .with_output(r#"{"error":"not_found"}"#)
                        .build(),
                )
                .build(),
        )
        .build();

    ApateTestServer::start(config, INIT_DELAY_DEFAULT)
}

#[tokio::test]
#[serial]
async fn test_apate_mock() {
    let _server = init_test_server();

    let client = Client::new();

    // Test GET endpoint directly
    let resp = client
        .get("http://localhost:8228/http/get")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.json::<JsonValue>().await.unwrap();
    assert_eq!(body["message"], "GET Success");

    // Test POST endpoint directly
    let resp = client
        .post("http://localhost:8228/http/post")
        .json(&serde_json::json!({ "message": "test" }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.json::<JsonValue>().await.unwrap();
    assert_eq!(body["message"], "POST Success");

    // Test missing endpoint directly
    let resp = client
        .get("http://localhost:8228/http/missing")
        .send()
        .await
        .unwrap();
    println!("Missing endpoint status: {}", resp.status());
    println!("Missing endpoint body: {:?}", resp.text().await.unwrap());
}

#[tokio::test]
#[serial]
async fn test_script_http_requests() {
    let _apate = init_test_server();
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    // http_get path
    let response = client::get("/script/http/get").await;
    let result: JsonValue = client::extract_from_json(response, Some(StatusCode::OK)).await;
    assert_eq!(result["success"], true);
    assert_eq!(result["status"], 200);
    assert_eq!(result["body"]["message"], "GET Success");
    assert_eq!(result["body"]["api_key"], "GET-API-key");

    // http_post_json path
    let response = client::get("/script/http/post").await;
    let result: JsonValue = client::extract_from_json(response, Some(StatusCode::OK)).await;
    assert_eq!(result["success"], true);
    assert_eq!(result["status"], 200);
    assert_eq!(result["body"]["message"], "POST Success");
    assert_eq!(result["body"]["api_key"], "POST-API-key");
    assert_eq!(result["body"]["input"]["message"], "Hello from Xepak");

    // http error path (404 with JSON response)
    let response = client::get("/script/http/error").await;
    let result: JsonValue = client::extract_from_json(response, Some(StatusCode::OK)).await;
    println!("Error result: {:?}", result);

    // is_success() checks HTTP status (4xx/5xx = false), not our custom field
    assert_eq!(result["success"], false); // should be false because apate returns 404
    assert_eq!(result["status"], 404);
}
