use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result};
use axum::Router;
use prometheus::Registry;
use tokio::{net::TcpListener, task::JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{
    config::{ApplicationSettings, DeciderKind, Settings},
    decider::{Decider, prefer_least_errors::PreferLeastErrors, round_robin::RoundRobin},
    http,
    observer::{MetricsObserver, prometheus::Collector},
    proxy::Pipeline,
    upstream::{Upstream, build_all},
};

pub struct Application {
    listener: TcpListener,
    router: Router,
    port: u16,
    tasks: JoinSet<()>,
}

#[bon::bon]
impl Application {
    #[builder(finish_fn = build)]
    pub async fn new(
        settings: ApplicationSettings,
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
            .max_attempt(settings.proxy.max_attempt)
            .retry_after(Duration::from_secs(settings.proxy.retry_after_in_secs))
            .build()?;

        let router = http::build_router(Arc::new(pipeline), Arc::new(registry));

        let addr = format!("{}:{}", settings.host, settings.port);
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
}

impl Application {
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

/// Wires the whole application out of settings: upstreams, the observer they
/// report to, the configured decider and its tasks, then the server itself.
pub async fn build(settings: Settings, shutdown: &CancellationToken) -> Result<Application> {
    let mut tasks = JoinSet::new();

    let upstreams = build_all(
        settings.upstreams,
        settings.application.proxy.rpc_timeout_in_secs,
    );
    let observer = Arc::new(MetricsObserver::new(
        upstreams.iter().map(|upstream| upstream.id().clone()),
    ));
    let decider = build_decider(
        settings.decider,
        upstreams,
        observer.clone(),
        &mut tasks,
        shutdown.clone(),
    )?;

    Application::builder()
        .settings(settings.application)
        .observer(observer)
        .decider(decider)
        .tasks(tasks)
        .build()
        .await
}
