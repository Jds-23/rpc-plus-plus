use std::sync::Arc;

use axum::{body::Bytes, extract::State, response::Response};

use crate::proxy::ProxyState;

pub async fn post_rpc(State(state): State<Arc<ProxyState>>, body: Bytes) -> Response {
    state.proxy(body).await
}
