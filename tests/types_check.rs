mod common;

use base64::Engine;
use common::*;
use serde_json::Value as JsonValue;

use serial_test::serial;

#[tokio::test]
#[serial]
async fn return_null_types() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    let response = client::get("/typecheck/1").await;
    assert!(response.status().is_success());

    let jvalue = response.json::<JsonValue>().await.unwrap();

    assert!(jvalue.is_object());

    let jobj = jvalue.as_object().unwrap();
    // Columns count could change in future
    assert_eq!(5, jobj.len());

    for (key, value) in jobj {
        if key == "id" {
            continue;
        }
        assert!(value.is_null())
    }
}

// #[tokio::main(flavor = "current_thread")]
// #[test]
#[tokio::test]
#[serial]
async fn non_null_types() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    let response = client::get("/typecheck/2").await;
    assert!(response.status().is_success());

    let jvalue = response.json::<JsonValue>().await.unwrap();

    assert!(jvalue.is_object());

    println!("JSON {jvalue}");

    let jobj = jvalue.as_object().unwrap();

    // Columns count could change in future
    assert_eq!(5, jobj.len());

    assert_eq!(42, jobj.get("type_int").unwrap().as_i64().unwrap());
    assert_eq!(3.3, jobj.get("type_real").unwrap().as_f64().unwrap());
    assert_eq!(
        "This is TEXT",
        jobj.get("type_text").unwrap().as_str().unwrap()
    );

    let blob64 = jobj.get("type_blob").unwrap().as_str().unwrap();
    let blob = base64::engine::general_purpose::STANDARD
        .decode(blob64)
        .unwrap();
    assert_eq!(vec![0x2a], blob);
}
