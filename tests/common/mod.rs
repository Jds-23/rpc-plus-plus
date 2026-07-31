pub mod mock_rpc_server;
use rpc_plus_plus::{
    route::build_router,
    settings::RpcSettings,
    start_up::{build_handlers, build_state},
};

pub async fn spawn_app(upstreams: Vec<RpcSettings>) -> String {
    let app = build_router(build_state(build_handlers(upstreams)));
    // port 0 => OS picks a free port, tests can run in parallel
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}
