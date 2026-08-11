use std::sync::Arc;

use axum::{
    Router,
    routing::{get, post},
};
use prometheus::Registry;

use crate::{
    proxy::ProxyState,
    route::{healthz::get_health, metrics::get_metrics, rpc::rpc_proxy},
};

pub mod healthz;
pub mod metrics;
pub mod rpc;

pub(crate) fn build_router(state: Arc<ProxyState>, registry: Arc<Registry>) -> Router {
    Router::new()
        .route("/healthz", get(get_health))
        .route("/rpc", post(rpc_proxy))
        .with_state(state)
        .merge(
            Router::new()
                .route("/metrics", get(get_metrics))
                .with_state(registry),
        )
}
