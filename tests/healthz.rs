use reqwest::StatusCode;

use crate::common::{rpc, spawn_app, test_settings};

mod common;

/// Nothing listens here: `/healthz` reports that the process is up, not that any
/// upstream is reachable.
const DEAD_URL: &str = "http://127.0.0.1:1/";

#[tokio::test]
async fn healthz_returns_ok() {
    let settings = test_settings(vec![rpc("fake", DEAD_URL)]);
    let addr = spawn_app(settings).await;

    let res = reqwest::Client::new()
        .get(format!("{addr}/healthz"))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    assert!(!res.bytes().await.unwrap().is_empty());
}
