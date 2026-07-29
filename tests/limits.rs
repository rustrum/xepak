mod common;

use common::domain::TypesRecord;
use common::*;
use reqwest::Response;

use serial_test::serial;

fn get_header_value(key: &str, response: &Response) -> String {
    response
        .headers()
        .get(key)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string()
}

async fn limits_test_template(uri: &str) {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    let response = client::get(uri).await;

    let limit_h = get_header_value("x-limit", &response);
    assert_eq!("4", limit_h);
    let result: Vec<TypesRecord> = client::extract_from_json(response, None).await;

    assert_eq!(4, result.len());
    assert_eq!(1, result[0].id);
    assert_eq!(4, result[3].id);

    // Limit is greater than max (not allowed)
    let response = client::get(&format!("{uri}?limit=5&offset=3")).await;

    let offset_h = get_header_value("x-offset", &response);
    assert_eq!("3", offset_h);

    let limit_h = get_header_value("x-limit", &response);
    assert_eq!("4", limit_h);

    let result: Vec<TypesRecord> = client::extract_from_json(response, None).await;
    assert_eq!(4, result.len());
    assert_eq!(4, result[0].id);
    assert_eq!(7, result[3].id);

    // Limit is smaller than max
    let response = client::get(&format!("{uri}?limit=2&offset=6")).await;

    let offset_h = get_header_value("x-offset", &response);
    assert_eq!("6", offset_h);

    let limit_h = get_header_value("x-limit", &response);
    assert_eq!("2", limit_h);

    let result: Vec<TypesRecord> = client::extract_from_json(response, None).await;
    assert_eq!(2, result.len());
    assert_eq!(7, result[0].id);
    assert_eq!(8, result[1].id);

    // Offset & no limit
    let response = client::get(&format!("{uri}?offset=7")).await;

    let offset_h = get_header_value("x-offset", &response);
    assert_eq!("7", offset_h);

    let limit_h = get_header_value("x-limit", &response);
    assert_eq!("4", limit_h);

    let result: Vec<TypesRecord> = client::extract_from_json(response, None).await;
    assert_eq!(1, result.len());
    assert_eq!(8, result[0].id);

    // Offset overflow
    let response = client::get(&format!("{uri}?offset=10")).await;

    let offset_h = get_header_value("x-offset", &response);
    assert_eq!("10", offset_h);

    let limit_h = get_header_value("x-limit", &response);
    assert_eq!("4", limit_h);

    let result: Vec<TypesRecord> = client::extract_from_json(response, None).await;
    assert!(result.is_empty());
}

#[tokio::test]
#[serial]
async fn limits_text_query() {
    limits_test_template("/limits").await
}

#[tokio::test]
#[serial]
async fn limits_lua_query() {
    limits_test_template("/limits/lua").await
}
