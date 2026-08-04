pub mod mock_rpc_server;
use std::sync::Arc;

use rpc_plus_plus::{route::build_router, rpc_handler::round_robin_handler::Inner};

pub async fn spawn_app(state: Arc<Inner>) -> String {
    let app = build_router(state);
    // port 0 => OS picks a free port, tests can run in parallel
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}
