mod common;

use std::collections::HashMap;

use jsonrpsee::http_client::HttpClientBuilder;
use jsonrpsee::proc_macros::rpc;
use serial_test::serial;
use xepak::xepak_data::XepakValue;

use crate::common::client;
use crate::common::domain::PostsRecord;
use crate::common::{INIT_DELAY_DEFAULT, init_default_test_server, init_logger};

#[rpc(client)]
pub trait TestRpc {
    #[method(name = "get")]
    async fn get(&self, id: usize) -> Result<Option<PostsRecord>, ErrorObjectOwned>;

    #[method(name = "get_compact")]
    async fn get_compact(
        &self,
        id: usize,
        fields: &[&str],
    ) -> Result<HashMap<String, XepakValue>, ErrorObjectOwned>;

    #[method(name = "all_posts")]
    async fn all_posts(&self) -> Result<Vec<PostsRecord>, ErrorObjectOwned>;

    #[method(name = "posts", param_kind = map)]
    async fn posts(
        &self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<PostsRecord>, ErrorObjectOwned>;

    #[method(name = "script", param_kind = map)]
    async fn script(&self, input: XepakValue) -> Result<XepakValue, ErrorObjectOwned>;
}

#[tokio::test]
#[serial]
async fn rpc_resourcess() {
    init_logger();
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;
    let uri = client::api_url("/rpc");
    let client = HttpClientBuilder::default()
        .build(uri)
        .expect("RPSee client");

    // Query get all
    let posts_call = client.all_posts().await;
    assert!(posts_call.is_ok(), "{posts_call:?}");
    let posts = posts_call.unwrap();
    assert_eq!(posts.len(), 4);

    // Query with limit/offset
    let posts = client.posts(10, 0).await.expect("OK");
    assert_eq!(posts.len(), 4);

    let posts = client.posts(10, 2).await.expect("OK");
    assert_eq!(posts.len(), 2);
    assert_eq!(posts[0].id, 3);

    let posts = client.posts(2, 1).await.expect("OK");
    assert_eq!(posts.len(), 2);
    assert_eq!(posts[0].id, 2);

    let posts = client.posts(5, 10).await.expect("OK");
    assert_eq!(posts.len(), 0);

    // With tuple arguments
    let post = client.get(1).await;
    assert!(
        matches!(&post, Ok(Some(p)) if p.id == 1 && p.content.contains("cool")),
        "{post:?}"
    );

    let post = client.get(2).await;
    assert!(
        matches!(&post, Ok(Some(p)) if p.id == 2 && p.content.contains("best")),
        "{post:?}"
    );

    let post = client.get(13).await;
    assert!(matches!(&post, Ok(None)), "{post:?}");

    // Lua sql script
    let post = client.get_compact(1, &["id"]).await;
    assert!(
        matches!(&post, Ok(p) if p.len() == 1 && p.contains_key("id")),
        "{post:?}"
    );

    let post = client.get_compact(2, &["id", "title"]).await;
    assert!(
        matches!(&post, Ok(p) if p.len() == 2 && p.contains_key("id") && p.contains_key("title")),
        "{post:?}"
    );

    // Lua data script
    let input = XepakValue::from(vec![
        XepakValue::from("Abascus"),
        XepakValue::from(1),
        XepakValue::from(2.33),
    ]);
    let resp = client.script(input.clone()).await;
    assert!(matches!(&resp, Ok(r) if r == &input), "{resp:?}");
}
