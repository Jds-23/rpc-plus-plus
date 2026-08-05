use rpc_plus_plus::{settings, start_up, telemetry};

#[tokio::main]
async fn main() {
    telemetry::init();

    // Both arms fail the same way, so they collapse into one `startup_failed`
    // call site rather than one per construction step.
    let started = match settings::get_settings() {
        Ok(settings) => start_up::start(settings).await,
        Err(err) => Err(err),
    };

    let (listener, app) = match started {
        Ok(started) => started,
        Err(err) => {
            // `{err:#}` walks the anyhow context chain; plain Display stops at
            // the outermost frame and drops the cause.
            tracing::error!(event = "startup_failed", error = format!("{err:#}"));
            std::process::exit(1);
        }
    };

    if let Err(err) = axum::serve(listener, app).await {
        tracing::error!(event = "server_stopped", error = %err);
        std::process::exit(1);
    }
}
