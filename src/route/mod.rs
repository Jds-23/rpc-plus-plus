use axum::{
    Router,
    routing::{get, post},
};

use crate::{
    route::{healthz::get_health, rpc::rpc_proxy},
    rpc_handler::round_robin_handler::RoundRobinHandler,
};

pub mod healthz;
pub mod rpc;

pub fn build_router(state: RoundRobinHandler) -> Router {
    Router::new()
        .route("/healthz", get(get_health))
        .route("/rpc", post(rpc_proxy))
        .with_state(state)
}
