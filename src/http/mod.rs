use std::sync::Arc;

use axum::{
    Router,
    routing::{get, post},
};
use prometheus::Registry;
use tower_http::cors::CorsLayer;

use crate::{
    http::{healthz::get_healthz, metrics::get_metrics, rpc::post_rpc},
    proxy::Pipeline,
};

pub(crate) mod cors;
pub mod healthz;
pub mod metrics;
pub mod rpc;

pub(crate) fn build_router(
    pipeline: Arc<Pipeline>,
    registry: Arc<Registry>,
    cors: Option<CorsLayer>,
) -> Router {
    // Not `route_layer`: the OPTIONS preflight lands in the 405 fallback.
    let rpc = match cors {
        Some(cors) => post(post_rpc).layer(cors),
        None => post(post_rpc),
    };

    Router::new()
        .route("/healthz", get(get_healthz))
        .route("/rpc", rpc)
        .with_state(pipeline)
        .merge(
            Router::new()
                .route("/metrics", get(get_metrics))
                .with_state(registry),
        )
}
