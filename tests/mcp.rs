mod common;

use std::collections::HashMap;

use jsonrpsee::http_client::{HeaderMap, HeaderValue, HttpClientBuilder};
use jsonrpsee::proc_macros::rpc;
use serde::Deserialize;
use serde_json::Value;
use serial_test::serial;
use xepak::xepak_data::XepakValue;

use crate::common::*;

/// MCP `tools/call` result envelope.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpToolCallResult {
    pub is_error: bool,
    pub content: Vec<McpToolContent>,
}

/// A single MCP content element.
#[derive(Debug, Deserialize)]
pub struct McpToolContent {
    #[serde(rename = "type")]
    pub content_type: String,
    pub text: String,
}

#[rpc(client)]
pub trait TestMcp {
    #[method(name = "initialize")]
    async fn initialize(&self) -> Result<Value, ErrorObjectOwned>;

    #[method(name = "tools/list")]
    async fn tools_list(&self) -> Result<Value, ErrorObjectOwned>;

    #[method(name = "tools/call", param_kind = map)]
    async fn tools_call(
        &self,
        name: &str,
        arguments: XepakValue,
    ) -> Result<McpToolCallResult, ErrorObjectOwned>;
}

/// Build a dict [`XepakValue`] for MCP tool arguments.
fn args(pairs: &[(&str, XepakValue)]) -> XepakValue {
    let map: HashMap<String, XepakValue> = pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    XepakValue::from(map)
}

#[tokio::test]
#[serial]
async fn mcp_initialize_tools_and_call() {
    // init_logger();
    let _server = init_default_test_server(INIT_DELAY_DEFAULT).await;
    let uri = client::api_url("/mcp");
    let client = HttpClientBuilder::default()
        .build(uri.clone())
        .expect("jsonrpsee MCP client must build");

    // 1) initialize must return server info and capabilities
    let init = client.initialize().await;
    assert!(init.is_ok(), "{init:?}");
    let init = init.unwrap();
    assert_eq!(
        init.get("protocolVersion").and_then(Value::as_str),
        Some("2024-11-05"),
        "{init:?}"
    );
    assert_eq!(
        init.get("serverInfo")
            .and_then(|v| v.get("name"))
            .and_then(Value::as_str),
        Some("Test MCP posts server"),
        "{init:?}"
    );
    assert_eq!(
        init.get("capabilities")
            .and_then(|v| v.get("tools"))
            .and_then(|v| v.get("listChanged"))
            .and_then(Value::as_bool),
        Some(false),
        "{init:?}"
    );

    // 2) tools/list must expose the configured tools
    let list = client.tools_list().await;
    assert!(list.is_ok(), "{list:?}");
    let list = list.unwrap();
    let tools = list
        .get("tools")
        .and_then(Value::as_array)
        .expect("tools must be an array: {list:?}");
    let names: Vec<&str> = tools
        .iter()
        .filter_map(|t| t.get("name").and_then(Value::as_str))
        .collect();
    assert_eq!(names.len(), 3, "{names:?}");
    assert!(names.contains(&"list_posts"), "{names:?}");
    assert!(names.contains(&"get_post"), "{names:?}");
    assert!(names.contains(&"auth"), "{names:?}");

    // 3) tools/call list_posts must return all posts
    let call = client
        .tools_call(
            "list_posts",
            args(&[
                ("limit", XepakValue::from(10i128)),
                ("offset", XepakValue::from(0i128)),
            ]),
        )
        .await;
    assert!(call.is_ok(), "{call:?}");
    let call = call.unwrap();
    assert!(!call.is_error, "{call:?}");
    assert_eq!(call.content.len(), 4, "{call:?}");
    assert_eq!(call.content[0].content_type, "text", "{call:?}");
    let post: domain::PostsRecord =
        serde_json::from_str(&call.content[0].text).expect("list_posts content must be JSON");
    assert_eq!(post.id, 1, "{post:?}");

    // 4) tools/call get_post must return the requested post
    let call = client
        .tools_call("get_post", args(&[("post_id", XepakValue::from(1i128))]))
        .await;
    assert!(call.is_ok(), "{call:?}");
    let call = call.unwrap();
    assert!(!call.is_error, "{call:?}");
    let post: domain::PostsRecord =
        serde_json::from_str(&call.content[0].text).expect("get_post content must be JSON");
    assert_eq!(post.id, 1, "{post:?}");

    // Auth check
    let mut headers = HeaderMap::new();
    let auth_value = HeaderValue::from_str("Bearer BossKEY").expect("Value expected");
    headers.insert("Authorization", auth_value);

    let client_auth = HttpClientBuilder::default()
        .set_headers(headers)
        .build(uri)
        .expect("RPSee client");

    let result = client_auth.tools_call("auth", XepakValue::Null).await;
    assert!(
        matches!(&result, Ok(r) if r.content[0].text  == "Authorized"),
        "{result:?}"
    );

    let result = client.tools_call("auth", XepakValue::Null).await;
    assert!(result.is_err(), "{result:?}");
}
