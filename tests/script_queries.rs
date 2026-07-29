mod common;

use common::domain::PostsRecord;
use common::*;
use reqwest::StatusCode;
use serde_json::Value as JsonValue;

use serial_test::serial;

async fn check_posts_response(uri: &str, expected_user_id: u64) {
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

#[tokio::test]
#[serial]
async fn script_query_posts_happy_path() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    check_posts_response("/script/lua/query/posts/user1", 1).await;
    check_posts_response("/script/lua/query/posts/user2", 2).await;
}

#[tokio::test]
#[serial]
async fn test_script_query_posts_not_found_user() {
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;

    let response = client::get("/script/lua/query/posts/nobody").await;
    assert_eq!(StatusCode::BAD_REQUEST, response.status());

    let rjson = response.json::<JsonValue>().await.unwrap();

    assert_eq!("bad_request", rjson.get("code").unwrap().as_str().unwrap());
}
