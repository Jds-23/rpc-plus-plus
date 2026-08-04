use rpc_plus_plus::{
    route, rpc_handler::round_robin_handler::RoundRobinHandlerBuilder, settings,
    start_up::build_handlers, telemetry,
};

#[tokio::main]
async fn main() {
    telemetry::init();

    let settings = match settings::get_settings() {
        Ok(settings) => settings,
        Err(err) => {
            tracing::error!(error = format!("{err:#}"), "failed to load settings");
            std::process::exit(1);
        }
    };

    let handlers = build_handlers(settings.rpcs, settings.rpc_timeout_in_secs);

    if handlers.is_empty() {
        tracing::error!("zero handlers created");
        std::process::exit(1);
    }

    tracing::info!(count=%&handlers.len(),"starting proxy");

    let state = match RoundRobinHandlerBuilder::default()
        .with_max_attempt(settings.max_attempt)
        .with_retry_after_in_secs(settings.retry_after_in_secs)
        .with_handlers(handlers)
        .build()
    {
        Ok(state) => state,
        Err(err) => {
            tracing::error!(error = %err, "failed to build proxy state");
            std::process::exit(1);
        }
    };

    let app = route::build_router(state);

    let listener =
        tokio::net::TcpListener::bind(format!("127.0.0.1:{}", settings.application_port))
            .await
            .unwrap();

    axum::serve(listener, app).await.unwrap();
}
