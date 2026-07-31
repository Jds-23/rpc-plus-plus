use axum::{body::Body, http::Request};
use http_body_util::BodyExt;
use reqwest::StatusCode;
use rpc_plus_plus::{
    route,
    settings::RpcSettings,
    start_up::{build_handlers, build_state},
};
use tower::ServiceExt;

#[tokio::test]
async fn healthz_returns_ok() {
    let rcp_handlers = build_handlers(vec![RpcSettings {
        label: "fake".to_string(),
        rpc_url: "http://127.0.0.1:1/".to_string(),
    }]);

    let state = build_state(rcp_handlers);
    let app = route::build_router(state);

    let res = app
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = res.into_body().collect().await.unwrap().to_bytes();
    assert!(!body.is_empty());
}
