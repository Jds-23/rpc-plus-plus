use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use axum::Router;
use prometheus::Registry;
use tokio::{net::TcpListener, task::JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{
    config::{ApplicationSettings, DeciderKind},
    decider::{Decider, prefer_least_errors::PreferLeastErrors, round_robin::RoundRobin},
    http,
    observer::{MetricsObserver, prometheus::Collector},
    proxy::Pipeline,
    upstream::Upstream,
};

pub struct Application {
    listener: TcpListener,
    router: Router,
    port: u16,
    tasks: JoinSet<()>,
}

impl Application {
    pub async fn build(
        application_settings: ApplicationSettings,
        observer: Arc<MetricsObserver>,
        decider: Arc<dyn Decider>,
        tasks: JoinSet<()>,
    ) -> Result<Self> {
        let collector =
            Collector::new(observer.clone()).context("failed to build the metrics collector")?;

        let registry = Registry::new();
        registry
            .register(Box::new(collector))
            .context("failed to register the metrics collector")?;

        let upstream_count = decider.upstream_len();

        let pipeline = Pipeline::builder()
            .decider(decider)
            .observer(observer)
            .max_attempt(application_settings.proxy.max_attempt)
            .retry_after(Duration::from_secs(
                application_settings.proxy.retry_after_in_secs,
            ))
            .build()?;

        let router = http::build_router(Arc::new(pipeline), Arc::new(registry));

        let addr = format!(
            "{}:{}",
            application_settings.host, application_settings.port
        );
        let listener = TcpListener::bind(&addr)
            .await
            .with_context(|| format!("failed to bind {addr}"))?;

        let port = listener
            .local_addr()
            .context("failed to read the bound address")?
            .port();

        tracing::info!(event = "startup_ready", upstreams = upstream_count, port);

        Ok(Application {
            listener,
            router,
            port,
            tasks,
        })
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub async fn run_until_stopped(mut self, shutdown: CancellationToken) -> Result<()> {
        let served = axum::serve(self.listener, self.router)
            .with_graceful_shutdown(shutdown.clone().cancelled_owned())
            .await
            .context("server error");

        shutdown.cancel();
        self.tasks.shutdown().await;

        served
    }
}

/// Builds the configured decider. `PreferLeastErrors` owns a refresh task, so it
/// takes `tasks` and `shutdown`; `RoundRobin` needs neither and ignores them.
pub fn build_decider(
    kind: DeciderKind,
    upstreams: Vec<Upstream>,
    observer: Arc<MetricsObserver>,
    tasks: &mut JoinSet<()>,
    shutdown: CancellationToken,
) -> Result<Arc<dyn Decider>> {
    let decider: Arc<dyn Decider> = match kind {
        DeciderKind::RoundRobin => {
            Arc::new(RoundRobin::new(upstreams).context("failed to build the decider")?)
        }
        DeciderKind::PreferLeastErrors => PreferLeastErrors::builder()
            .upstreams(upstreams)
            .observer(observer)
            .tasks(tasks)
            .shutdown(shutdown)
            .spawn()
            .context("failed to build the decider")?,
    };

    tracing::info!(event = "decider_selected", decider = kind.as_str());

    Ok(decider)
}
