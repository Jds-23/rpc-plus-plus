use std::sync::Arc;

use axum::{body::Bytes, extract::State, response::Response};

use crate::rpc_handler::proxy::Proxy;

pub async fn rpc_proxy(State(state): State<Arc<Proxy>>, body: Bytes) -> Response {
    state.proxy(body).await
}
