use super::common::domain::{ErrorResponse, PostsRecord};
use super::common::*;

use reqwest::StatusCode;
use serde_json::Value as JsonValue;

use serial_test::serial;

async fn validate_posts_response(uri: &str, expected_user_id: u64) {
    let response = client::get(uri).await;
    assert!(
        response.status().is_success(),
        "Status is {}",
        response.status()
    );

    let rows: Vec<PostsRecord> = response.json::<Vec<PostsRecord>>().await.unwrap();
    assert!(
        !rows.is_empty(),
        "Expected at least one post for user_id={expected_user_id}"
    );

    for row in &rows {
        assert_eq!(
            expected_user_id, row.user_id,
            "All posts must belong to user_id={expected_user_id}"
        );
    }
}

/// Checking hos query_one() works and ctx:set_value()
#[tokio::test]
#[serial]
async fn check_query_one_set_value() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    validate_posts_response("/script/lua/query/posts/user1", 1).await;
    validate_posts_response("/script/lua/query/posts/user2", 2).await;
}

#[tokio::test]
#[serial]
async fn check_posts_expected_error() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    let response = client::get("/script/lua/query/posts/nobody").await;
    assert_eq!(StatusCode::BAD_REQUEST, response.status());

    let rjson = response.json::<JsonValue>().await.unwrap();

    assert_eq!("bad_request", rjson.get("code").unwrap().as_str().unwrap());
}

/// Test inside script query function
/// Uses error_not_found as workaround to return query results
#[tokio::test]
#[serial]
async fn check_query_types() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    // Test tt="query" - (user_id=1 has 2 posts)
    {
        let response = client::get("/script/lua/query/check/1/query").await;
        assert_eq!(StatusCode::NOT_FOUND, response.status());

        let resp =
            client::extract_from_json::<ErrorResponse>(response, Some(StatusCode::NOT_FOUND)).await;
        assert_eq!("!rows_count:2", resp.message);
    }

    // Test tt="query_value" - (user_id=1 has 2 posts)
    {
        let response = client::get("/script/lua/query/check/1/query_value").await;
        assert_eq!(StatusCode::NOT_FOUND, response.status());

        let resp =
            client::extract_from_json::<ErrorResponse>(response, Some(StatusCode::NOT_FOUND)).await;
        assert_eq!("!count:2", resp.message);
    }

    // Test tt="query_one" - (user_id=3 has post with id=4)
    {
        let response = client::get("/script/lua/query/check/3/query_one").await;
        assert_eq!(StatusCode::NOT_FOUND, response.status());

        let resp =
            client::extract_from_json::<ErrorResponse>(response, Some(StatusCode::NOT_FOUND)).await;
        assert_eq!("!post_id:4", resp.message);
    }

    // Test tt="query_one" - (user_id=1 has first post with id=1)
    {
        let response = client::get("/script/lua/query/check/1/query_one").await;
        assert_eq!(StatusCode::NOT_FOUND, response.status());

        let resp =
            client::extract_from_json::<ErrorResponse>(response, Some(StatusCode::NOT_FOUND)).await;
        assert_eq!("!post_id:1", resp.message);
    }

    // Test invalid tt - should return input error
    {
        let response = client::get("/script/lua/query/check/1/invalid").await;
        assert_eq!(StatusCode::BAD_REQUEST, response.status());

        let resp =
            client::extract_from_json::<ErrorResponse>(response, Some(StatusCode::BAD_REQUEST))
                .await;
        assert_eq!("bad_request", resp.code);
        assert_eq!("Input 'tt' value is not correct: invalid", resp.message);
    }
}
