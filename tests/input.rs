mod common;

use common::domain::TypesRecord;
use common::*;
use reqwest::StatusCode;
use serde_json::Value as JsonValue;

use serial_test::serial;

#[tokio::test]
#[serial]
async fn allow_methods_config() {
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
    assert_eq!(body["method"], "POST");

    let resp = client::get("/input/method/post-put").await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = resp.json::<JsonValue>().await.unwrap();
    assert_eq!(body["code"], "bad_request");
    assert_eq!(body["message"], "Request method GET not allowed!");
}

#[tokio::test]
#[serial]
async fn test_cbor_non_dict_root_returns_400() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    let resp = client::post_cbor_accept("/input/body/args/simple", vec![1i64, 2, 3], true).await;

    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = resp.json::<JsonValue>().await.unwrap();
    assert_eq!(body["code"], "bad_request");
}

async fn record_send_receive(
    record: &TypesRecord,
    input_cbor: bool,
    output_cbor: bool,
    url: &str,
) -> TypesRecord {
    let resp = if input_cbor {
        client::post_cbor_accept(url, record, !output_cbor).await
    } else {
        client::post_json_accept(url, record, output_cbor).await
    };
    assert_eq!(resp.status(), StatusCode::OK, "Response must be 200 OK");

    let bytes = resp.bytes().await.expect("Must read response body");

    if output_cbor {
        cbor2::from_slice(&bytes).expect("Must deserialize CBOR response as TypesRecord")
    } else {
        serde_json::from_slice(&bytes).expect("Must deserialize JSON response as TypesRecord")
    }
}

async fn payload_inout_tests(input_cbor: bool, output_cbor: bool, url: &str, check_id_eq: bool) {
    // Variant 1: all fields present and not null
    {
        let record = TypesRecord {
            id: 11,
            type_text: Some("Hello all".to_string()),
            type_int: Some(42),
            type_real: Some(std::f64::consts::PI),
            type_blob: Some(vec![0x01, 0x02, 0x03]),
        };

        let result = record_send_receive(&record, input_cbor, output_cbor, url).await;

        if check_id_eq {
            assert_eq!(result.id, record.id, "ID must match input ID");
        } else {
            assert!(result.id > 0, "ID must be auto-generated and > 0");
        }
        assert_eq!(result.type_text.as_deref(), Some("Hello all"));
        assert_eq!(result.type_int, Some(42));
        assert!((result.type_real.unwrap() - std::f64::consts::PI).abs() < f64::EPSILON);
        assert_eq!(result.type_blob, Some(vec![0x01, 0x02, 0x03]));
    }

    // Variant 2: half of fields are null
    {
        let record = TypesRecord {
            id: 22,
            type_text: None,
            type_int: None,
            type_real: Some(2.71),
            type_blob: Some(vec![0xAA, 0xBB]),
        };

        let result = record_send_receive(&record, input_cbor, output_cbor, url).await;

        if check_id_eq {
            assert_eq!(result.id, record.id, "ID must match input ID");
        } else {
            assert!(result.id > 0, "ID must be auto-generated and > 0");
        }
        assert_eq!(result.type_text, None, "type_text must be NULL");
        assert_eq!(result.type_int, None, "type_int must be NULL");
        assert!((result.type_real.unwrap() - 2.71).abs() < f64::EPSILON);
        assert_eq!(result.type_blob, Some(vec![0xAA, 0xBB]));
    }

    // Variant 3: other half of fields are null
    {
        let record = TypesRecord {
            id: 33,
            type_text: Some("World".to_string()),
            type_int: Some(999),
            type_real: None,
            type_blob: None,
        };

        let result = record_send_receive(&record, input_cbor, output_cbor, url).await;

        if check_id_eq {
            assert_eq!(result.id, record.id, "ID must match input ID");
        } else {
            assert!(result.id > 0, "ID must be auto-generated and > 0");
        }
        assert_eq!(result.type_text.as_deref(), Some("World"));
        assert_eq!(result.type_int, Some(999));
        assert_eq!(result.type_real, None, "type_real must be NULL");
        assert_eq!(result.type_blob, None, "type_blob must be NULL");
    }
}

#[tokio::test]
#[serial]
async fn test_types_record_db_roundtrip() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;
    payload_inout_tests(false, false, "/input/body/args/types-record", false).await;
    payload_inout_tests(true, true, "/input/body/args/types-record", false).await;
    payload_inout_tests(false, true, "/input/body/args/types-record", false).await;
    payload_inout_tests(true, false, "/input/body/args/types-record", false).await;
}

#[tokio::test]
#[serial]
async fn test_body_args_simple_echo() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;
    payload_inout_tests(false, false, "/input/body/args/simple", true).await;
    payload_inout_tests(true, true, "/input/body/args/simple", true).await;
    payload_inout_tests(false, true, "/input/body/args/simple", true).await;
    payload_inout_tests(true, false, "/input/body/args/simple", true).await;
}
