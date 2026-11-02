use reqwest::{StatusCode, header::CONTENT_TYPE};
use serde_json::json;

use crate::common::{mock_rpc_server, rpc, spawn_app, test_settings};

mod common;

#[tokio::test]
async fn metrics_expose_what_the_proxy_did() {
    let upstream = mock_rpc_server::ok("0x1").await;
    let addr = spawn_app(test_settings(vec![rpc("one", upstream.uri())])).await;
    let client = reqwest::Client::new();

    client
        .post(format!("{addr}/rpc"))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}))
        .send()
        .await
        .unwrap();

    let res = client.get(format!("{addr}/metrics")).send().await.unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(res.headers()[CONTENT_TYPE], "text/plain; version=0.0.4");
    let body = res.text().await.unwrap();
    for line in [
        r#"rpc_attempts_total{outcome="success",upstream="one"} 1"#,
        r#"rpc_requests_total{method="eth_blockNumber",outcome="success"} 1"#,
    ] {
        assert!(
            body.lines().any(|l| l == line),
            "missing {line:?} in:\n{body}"
        );
    }
}
