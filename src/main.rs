use std::sync::Arc;

use axum::{
    Router,
    routing::{get, post},
};
use rpc_plus_plus::{
    decider::RoundRobin,
    route::{healthz::get_health, rpc::rpc_proxy},
    rpc_handler::{RoundRobinHandler, RpcHandler, RpcHandlerBuilder},
    settings, telemetry,
};

#[tokio::main]
async fn main() {
    telemetry::init();

    let settings = match settings::get_settings() {
        Ok(settings) => settings,
        Err(e) => panic!("{e}"),
    };

    let handlers: Vec<RpcHandler> = settings
        .rpcs
        .iter()
        .filter_map(|item| {
            match RpcHandlerBuilder::default()
                .with_url(item.rpc_url.clone())
                .build()
            {
                Ok(item) => Some(item),
                Err(err) => {
                    tracing::warn!(
                        url = %&item.label,
                        error = format!("{err:#}"),
                        "skipping rpc backend"
                    );
                    None
                }
            }
        })
        .collect();

    if handlers.len() < 1 {
        tracing::error!("zero handlers created");
        std::process::exit(1);
    }

    tracing::info!(count=%&handlers.len(),"starting proxy");

    let state: RoundRobinHandler = Arc::new(RoundRobin::new(handlers.into_iter()));

    let app = Router::new()
        .route("/healthz", get(get_health))
        .route("/rpc", post(rpc_proxy))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080")
        .await
        .unwrap();

    axum::serve(listener, app).await.unwrap();
}
