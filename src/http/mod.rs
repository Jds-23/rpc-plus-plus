use std::sync::Arc;

use axum::{
    Router,
    routing::{get, post},
};
use prometheus::Registry;

use crate::{
    http::{healthz::get_healthz, metrics::get_metrics, rpc::post_rpc},
    proxy::Pipeline,
};

pub mod healthz;
pub mod metrics;
pub mod rpc;

pub(crate) fn build_router(pipeline: Arc<Pipeline>, registry: Arc<Registry>) -> Router {
    Router::new()
        .route("/healthz", get(get_healthz))
        .route("/rpc", post(post_rpc))
        .with_state(pipeline)
        .merge(
            Router::new()
                .route("/metrics", get(get_metrics))
                .with_state(registry),
        )
}
