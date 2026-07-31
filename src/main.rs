use std::sync::Arc;

use rpc_plus_plus::{
    decider::RoundRobin, route, rpc_handler::RoundRobinHandler, settings, start_up::build_handlers,
    telemetry,
};

#[tokio::main]
async fn main() {
    telemetry::init();

    let settings = match settings::get_settings() {
        Ok(settings) => settings,
        Err(e) => panic!("{e}"),
    };

    let handlers = build_handlers(settings.rpcs);

    if handlers.is_empty() {
        tracing::error!("zero handlers created");
        std::process::exit(1);
    }

    tracing::info!(count=%&handlers.len(),"starting proxy");

    let state: RoundRobinHandler = Arc::new(RoundRobin::new(handlers.into_iter()));

    let app = route::build_router(state);

    let listener =
        tokio::net::TcpListener::bind(format!("127.0.0.1:{}", settings.application_port))
            .await
            .unwrap();

    axum::serve(listener, app).await.unwrap();
}
