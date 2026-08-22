use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Router;
use prometheus::Registry;
use tokio::{net::TcpListener, task::JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{
    decider::Decider,
    observer::MetricsObserver,
    proxy::ProxyStateBuilder,
    route::{self, metrics::MetricsCollector},
    settings::{ApplicationSettings, RpcSettings},
    upstream::{Upstream, UpstreamBuilder},
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
        let collector = MetricsCollector::new(observer.clone())
            .context("failed to build the metrics collector")?;

        let registry = Registry::new();
        registry
            .register(Box::new(collector))
            .context("failed to register the metrics collector")?;

        let upstream_count = decider.upstream_len();

        let state = ProxyStateBuilder::new(decider, observer)
            .with_max_attempt(application_settings.proxy.max_attempt)?
            .with_retry_after_in_secs(application_settings.proxy.retry_after_in_secs)
            .build();

        let router = route::build_router(Arc::new(state), Arc::new(registry));

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

pub fn build_upstreams<I>(rpcs: I, rpc_timeout_in_secs: u64) -> Vec<Upstream>
where
    I: IntoIterator<Item = RpcSettings>,
{
    rpcs.into_iter()
        .filter_map(|item| {
            UpstreamBuilder::new(&item.label, &item.rpc_url)
                .and_then(|builder| {
                    builder
                        .with_rpc_timeout_in_secs(rpc_timeout_in_secs)
                        .build()
                })
                .map_err(|err| {
                    tracing::warn!(
                        event = "upstream_skipped",
                        upstream = %item.label,
                        error = %err,
                    );
                })
                .ok()
        })
        .collect()
}
