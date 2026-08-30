use rpc_plus_plus::{app, config, telemetry};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    telemetry::init();
    let shutdown = CancellationToken::new();

    let app = match build(&shutdown).await {
        Ok(app) => app,
        Err(err) => {
            tracing::error!(event = "startup_failed", error = format!("{err:#}"));
            std::process::exit(1);
        }
    };

    tokio::spawn(watch_for_shutdown(shutdown.clone()));

    if let Err(err) = app.run_until_stopped(shutdown).await {
        tracing::error!(event = "server_stopped", error = format!("{err:#}"));
        std::process::exit(1);
    }
}

async fn build(shutdown: &CancellationToken) -> anyhow::Result<app::Application> {
    app::build(config::get_settings()?, shutdown).await
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
