use std::sync::Arc;

use axum::{body::Body, http::Request};
use http_body_util::BodyExt;
use reqwest::StatusCode;
use rpc_plus_plus::{
    decider::round_robin::RoundRobin, observer::NoopObserver, proxy::ProxyStateBuilder, route,
    settings::RpcSettings, start_up::build_upstreams,
};
use tower::ServiceExt;

#[tokio::test]
async fn healthz_returns_ok() {
    let rpcs = vec![RpcSettings {
        label: "fake".to_string(),
        rpc_url: "http://127.0.0.1:1/".to_string(),
    }];
    let upstreams = build_upstreams(rpcs, 1);
    let decider = Arc::new(RoundRobin::new(upstreams).expect("decider build failed"));
    let observer = Arc::new(NoopObserver::new());

    let state = ProxyStateBuilder::new(decider, observer)
        .build()
        .expect("State build failed");
    let app = route::build_router(Arc::new(state));

    let res = app
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    assert!(!body.is_empty());
}
