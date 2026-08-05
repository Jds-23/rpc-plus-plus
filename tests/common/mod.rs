pub mod mock_rpc_server;
use std::sync::Arc;

use rpc_plus_plus::{
    decider::round_robin::RoundRobin, proxy::ProxyState, route::build_router,
    settings::RpcSettings, start_up::build_handlers,
};

/// Builds the rotation the proxy decides over. Every test drives the same
/// construction path as `main`: settings -> handlers -> decider.
pub fn round_robin(upstreams: Vec<RpcSettings>) -> Arc<RoundRobin> {
    let handlers = build_handlers(upstreams, 1);
    Arc::new(RoundRobin::new(handlers).expect("decider build failed"))
}

pub async fn spawn_app(state: Arc<ProxyState>) -> String {
    let app = build_router(state);
    // port 0 => OS picks a free port, tests can run in parallel
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}
