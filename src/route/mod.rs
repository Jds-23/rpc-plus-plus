use std::sync::Arc;

use axum::{
    Router,
    routing::{get, post},
};

use crate::{
    proxy::ProxyState,
    route::{healthz::get_health, rpc::rpc_proxy},
};

pub mod healthz;
pub mod rpc;

pub(crate) fn build_router(state: Arc<ProxyState>) -> Router {
    Router::new()
        .route("/healthz", get(get_health))
        .route("/rpc", post(rpc_proxy))
        .with_state(state)
}
