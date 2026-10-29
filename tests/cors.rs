use reqwest::{
    Response, StatusCode,
    header::{
        ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
        ACCESS_CONTROL_MAX_AGE,
    },
};
use serde_json::json;

use crate::common::{mock_rpc_server, rpc, spawn_app, test_settings};

mod common;

const ALLOWED: &str = "https://app.example";
const DEAD_URL: &str = "http://127.0.0.1:1/";

async fn spawn_with_origins(origins: &[&str], upstream: &str) -> String {
    let mut settings = test_settings(vec![rpc("one", upstream)]);
    settings.application.cors.allowed_origins =
        origins.iter().map(|origin| origin.to_string()).collect();
    spawn_app(settings).await
}

async fn preflight(addr: &str, origin: &str) -> Response {
    reqwest::Client::new()
        .request(reqwest::Method::OPTIONS, format!("{addr}/rpc"))
        .header("Origin", origin)
        .header("Access-Control-Request-Method", "POST")
        .header("Access-Control-Request-Headers", "content-type")
        .send()
        .await
        .unwrap()
}

fn header<'a>(res: &'a Response, name: &reqwest::header::HeaderName) -> Option<&'a str> {
    res.headers().get(name).map(|value| value.to_str().unwrap())
}

#[tokio::test]
async fn a_preflight_from_an_allowed_origin_is_granted() {
    let addr = spawn_with_origins(&[ALLOWED], DEAD_URL).await;

    let res = preflight(&addr, ALLOWED).await;

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(header(&res, &ACCESS_CONTROL_ALLOW_ORIGIN), Some(ALLOWED));
    assert_eq!(header(&res, &ACCESS_CONTROL_ALLOW_METHODS), Some("POST"));
    assert_eq!(
        header(&res, &ACCESS_CONTROL_ALLOW_HEADERS),
        Some("content-type")
    );
    assert_eq!(header(&res, &ACCESS_CONTROL_MAX_AGE), Some("3600"));
}

#[tokio::test]
async fn a_preflight_from_another_origin_gets_no_allow_origin() {
    let addr = spawn_with_origins(&[ALLOWED], DEAD_URL).await;

    let res = preflight(&addr, "https://evil.example").await;

    assert_eq!(header(&res, &ACCESS_CONTROL_ALLOW_ORIGIN), None);
}

#[tokio::test]
async fn a_post_from_an_allowed_origin_carries_allow_origin() {
    let upstream = mock_rpc_server::ok("0x1").await;
    let addr = spawn_with_origins(&[ALLOWED], &upstream.uri()).await;

    let res = reqwest::Client::new()
        .post(format!("{addr}/rpc"))
        .header("Origin", ALLOWED)
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(header(&res, &ACCESS_CONTROL_ALLOW_ORIGIN), Some(ALLOWED));
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["result"], "0x1");
}

#[tokio::test]
async fn a_wildcard_allows_any_origin() {
    let addr = spawn_with_origins(&["*"], DEAD_URL).await;

    let res = preflight(&addr, "https://anything.example").await;

    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(header(&res, &ACCESS_CONTROL_ALLOW_ORIGIN), Some("*"));
}

#[tokio::test]
async fn no_origins_means_no_cors() {
    let addr = spawn_with_origins(&[], DEAD_URL).await;

    let res = preflight(&addr, ALLOWED).await;

    assert_eq!(res.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(header(&res, &ACCESS_CONTROL_ALLOW_ORIGIN), None);
}
