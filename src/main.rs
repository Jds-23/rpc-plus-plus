use std::sync::Arc;

use rpc_plus_plus::{
    decider::{
        Decider,
        prefer_least_error::{PreferLeastError, REFRESH_DEFAULT, WINDOW_DEFAULT},
        round_robin::RoundRobin,
    },
    observer::MetricsObserver,
    settings,
    start_up::{Application, build_upstreams},
    telemetry,
};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::info;

#[tokio::main]
async fn main() {
    telemetry::init();
    let mut tasks = JoinSet::new();
    let shutdown = CancellationToken::new();

    let built = match settings::get_settings() {
        Ok(settings) => {
            let upstreams = build_upstreams(settings.rpcs, settings.rpc_timeout_in_secs);
            let observer = Arc::new(MetricsObserver::new(
                upstreams.iter().map(|u| u.id().clone()),
            ));

            let decider: Arc<dyn Decider> = match settings.decider.as_str() {
                "ROUND_ROBIN" => match RoundRobin::new(upstreams) {
                    Ok(decider) => {
                        tracing::info!(
                            event = "decider_selected",
                            decider = format!("ROUND_ROBIN")
                        );
                        Arc::new(decider)
                    }
                    Err(err) => startup_failed(format!("decider build failed {err:#}")),
                },
                "PREFER_LEAST_ERRORS" => match PreferLeastError::spawn(
                    upstreams,
                    observer.clone(),
                    &mut tasks,
                    shutdown.clone(),
                    REFRESH_DEFAULT,
                    WINDOW_DEFAULT,
                ) {
                    Ok(decider) => {
                        tracing::info!(
                            event = "decider_selected",
                            decider = format!("PREFER_LEAST_ERRORS")
                        );
                        decider
                    }
                    Err(err) => startup_failed(format!("decider build failed {err:#}")),
                },
                decider_type => startup_failed(format!("invalid decider {decider_type}")),
            };

            Application::build(settings.application, observer, decider, tasks).await
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

fn startup_failed(error: String) -> ! {
    tracing::error!(event = "startup_failed", error,);
    std::process::exit(1)
}
