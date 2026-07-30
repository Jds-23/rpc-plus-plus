use axum::{body::Bytes, extract::State, response::Response};

use crate::{decider::Decider, rpc_handler::RoundRobinHandler};

pub async fn rpc_proxy(State(state):State<RoundRobinHandler>,body: Bytes)->Response {
    state.decide().unwrap().proxy(body).await
}
