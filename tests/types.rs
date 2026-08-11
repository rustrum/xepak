mod common;

use std::collections::HashMap;

use base64::Engine;
use common::domain::TypesRecord;
use common::*;
use reqwest::StatusCode;
use serde_json::Value as JsonValue;

use serial_test::serial;
use xepak_rest::xepak_data::XepakValue;

// #[tokio::main(flavor = "current_thread")]
// #[test]
#[tokio::test]
#[serial]
async fn check_null_values_response() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    let response = client::get("/alltypes/1").await;
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

#[tokio::test]
#[serial]
async fn check_not_null_values_response() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    let response = client::get("/alltypes/2").await;
    assert!(response.status().is_success());

    let jvalue = response.json::<JsonValue>().await.unwrap();

    assert!(jvalue.is_object());

    println!("JSON {jvalue}");

    let jobj = jvalue.as_object().unwrap();

    // Columns count could change in future
    assert_eq!(5, jobj.len());

    assert_eq!(42, jobj.get("type_int").unwrap().as_i64().unwrap());
    assert_eq!(2.2, jobj.get("type_real").unwrap().as_f64().unwrap());
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

#[tokio::test]
#[serial]
async fn same_response_json_cbor() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    let response = client::get("/alltypes/3").await;
    let json_data: TypesRecord = client::extract_from_json(response, Some(StatusCode::OK)).await;

    assert_eq!(3, json_data.id);
    assert_eq!(Some("Third record".to_string()), json_data.type_text);
    assert_eq!(Some(33), json_data.type_int);
    assert_eq!(Some(3.3), json_data.type_real);
    assert_eq!(Some(vec![0x2b]), json_data.type_blob);

    let cresponse = client::get_resource(
        "/alltypes/3",
        HashMap::<String, String>::new(),
        client::cbor_headers(),
    )
    .await;

    let cbor_data: TypesRecord = client::extract_from_cbor(cresponse, Some(StatusCode::OK)).await;

    assert_eq!(json_data, cbor_data);
}

#[tokio::test]
#[serial]
async fn response_json_cbor_with_nulls() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    // JSON
    let response = client::get("/alltypes/4").await;
    let json_data: TypesRecord = client::extract_from_json(response, Some(StatusCode::OK)).await;

    assert_eq!(4, json_data.id);
    assert_eq!(Some("Fourth record".to_string()), json_data.type_text);
    assert_eq!(None, json_data.type_int);
    assert_eq!(Some(4.4), json_data.type_real);
    assert_eq!(None, json_data.type_blob);

    let response = client::get("/alltypes/5").await;
    let json_data: TypesRecord = client::extract_from_json(response, Some(StatusCode::OK)).await;

    assert_eq!(5, json_data.id);
    assert_eq!(None, json_data.type_text);
    assert_eq!(Some(55), json_data.type_int);
    assert_eq!(None, json_data.type_real);
    assert_eq!(Some(vec![0x2d]), json_data.type_blob);

    // CBOR
    let response = client::get_resource(
        "/alltypes/4",
        HashMap::<String, String>::new(),
        client::cbor_headers(),
    )
    .await;
    let json_data: TypesRecord = client::extract_from_cbor(response, Some(StatusCode::OK)).await;

    assert_eq!(4, json_data.id);
    assert_eq!(Some("Fourth record".to_string()), json_data.type_text);
    assert_eq!(None, json_data.type_int);
    assert_eq!(Some(4.4), json_data.type_real);
    assert_eq!(None, json_data.type_blob);

    let response = client::get_resource(
        "/alltypes/5",
        HashMap::<String, String>::new(),
        client::cbor_headers(),
    )
    .await;
    let json_data: TypesRecord = client::extract_from_cbor(response, Some(StatusCode::OK)).await;

    assert_eq!(5, json_data.id);
    assert_eq!(None, json_data.type_text);
    assert_eq!(Some(55), json_data.type_int);
    assert_eq!(None, json_data.type_real);
    assert_eq!(Some(vec![0x2d]), json_data.type_blob);
}

#[tokio::test]
#[serial]
async fn script_response_types() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    // Map response
    let response = client::get("/alltypes/script/table").await;
    let value: XepakValue = client::extract_from_json(response, Some(StatusCode::OK)).await;

    assert!(value.is_map());
    let value = value.as_map().unwrap();

    // Nill values are considered abscent in mlua
    assert!(!value.contains_key("null_val"));
    assert!(matches!(value["bool_val"], XepakValue::Boolean(false)));
    assert!(matches!(value["int_val"], XepakValue::Integer(1)));
    assert!(matches!(value["float_val"], XepakValue::Float(2.2)));
    assert!(matches!(&value["string_val"], XepakValue::Text(txt) if txt == "String from LUA"));

    // Tuple response
    let response = client::get("/alltypes/script/tuple").await;
    let value: XepakValue = client::extract_from_json(response, Some(StatusCode::OK)).await;

    assert!(value.is_tuple());
    let value = value.as_tuple().unwrap();

    // Nill values in sequences are bad but xepak could handle this
    assert_eq!(
        value,
        vec![
            XepakValue::Null,
            XepakValue::Boolean(true),
            XepakValue::Integer(1),
            XepakValue::Float(2.2),
            XepakValue::Text("String from LUA".to_string())
        ]
    );

    // Empty tuple
    let response = client::get("/alltypes/script/tuple?empty=1").await;
    let value: XepakValue = client::extract_from_json(response, Some(StatusCode::OK)).await;

    assert!(value.is_tuple());
    let value = value.as_tuple().unwrap();
    assert!(value.is_empty());
}
