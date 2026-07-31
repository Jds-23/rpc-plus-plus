use axum::{body::Bytes, extract::State, response::Response};

use crate::rpc_handler::round_robin_handler::RoundRobinHandler;

pub async fn rpc_proxy(State(state): State<RoundRobinHandler>, body: Bytes) -> Response {
    state.proxy(body).await
}
