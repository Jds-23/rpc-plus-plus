use std::sync::Arc;

use axum::{body::Bytes, extract::State, response::Response};

use crate::proxy::Pipeline;

pub async fn post_rpc(State(pipeline): State<Arc<Pipeline>>, body: Bytes) -> Response {
    pipeline.proxy(body).await
}
