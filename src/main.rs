use std::sync::Arc;

use rpc_plus_plus::{
    decider::{Decider, round_robin::RoundRobin},
    route,
    rpc_handler::proxy::ProxyBuilder,
    settings,
    start_up::build_handlers,
    telemetry,
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

    let decider = match RoundRobin::new(handlers) {
        Ok(decider) => decider,
        Err(err) => {
            tracing::error!(error = %err, "failed to build decider");
            std::process::exit(1);
        }
    };

    tracing::info!(count=%&decider.active_list_count(),"starting proxy");

    let decider = Arc::new(decider);

    let state = match ProxyBuilder::default()
        .with_max_attempt(settings.max_attempt)
        .with_retry_after_in_secs(settings.retry_after_in_secs)
        .with_decider(decider)
        .build()
    {
        Ok(state) => state,
        Err(err) => {
            tracing::error!(error = %err, "failed to build proxy state");
            std::process::exit(1);
        }
    };

    let app = route::build_router(Arc::new(state));

    let listener =
        tokio::net::TcpListener::bind(format!("127.0.0.1:{}", settings.application_port))
            .await
            .unwrap();

    axum::serve(listener, app).await.unwrap();
}
