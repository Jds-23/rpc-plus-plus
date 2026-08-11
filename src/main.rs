use std::sync::Arc;

use rpc_plus_plus::{
    observer::MetricsObserver, settings, start_up::Application, telemetry, upstream::UpstreamId,
};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    telemetry::init();

    let built = match settings::get_settings() {
        Ok(settings) => {
            let observer = Arc::new(MetricsObserver::new(
                settings
                    .rpcs
                    .iter()
                    .map(|rpc| UpstreamId::new(rpc.label.as_str())),
            ));

            Application::build(settings, observer).await
        }
        Err(err) => Err(err),
    };

    let app = match built {
        Ok(app) => app,
        Err(err) => {
            tracing::error!(event = "startup_failed", error = format!("{err:#}"));
            std::process::exit(1);
        }
    };

    let shutdown = CancellationToken::new();
    tokio::spawn(watch_for_shutdown(shutdown.clone()));

    if let Err(err) = app.run_until_stopped(shutdown).await {
        tracing::error!(event = "server_stopped", error = format!("{err:#}"));
        std::process::exit(1);
    }
}

async fn watch_for_shutdown(shutdown: CancellationToken) {
    let interrupt = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl-c");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to listen for SIGTERM")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = interrupt => {}
        _ = terminate => {}
    }

    shutdown.cancel();
}
