use axum::{body::Body, http::Request};
use http_body_util::BodyExt;
use reqwest::StatusCode;
use rpc_plus_plus::{
    route, rpc_handler::round_robin_handler::RoundRobinHandlerBuilder, settings::RpcSettings,
    start_up::build_handlers,
};
use tower::ServiceExt;

#[tokio::test]
async fn healthz_returns_ok() {
    let upstream = vec![RpcSettings {
        label: "fake".to_string(),
        rpc_url: "http://127.0.0.1:1/".to_string(),
    }];
    let rcp_handlers = build_handlers(upstream, 1);

    let state = RoundRobinHandlerBuilder::default()
        .with_handlers(rcp_handlers)
        .build()
        .expect("State build failed");
    let app = route::build_router(state);

    let res = app
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    assert!(!body.is_empty());
}
