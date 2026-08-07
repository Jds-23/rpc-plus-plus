use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Router;
use tokio::{net::TcpListener, task::JoinSet};
use tokio_util::sync::CancellationToken;

use crate::{
    decider::round_robin::RoundRobin,
    observer::{NoopObserver, Observer},
    proxy::ProxyStateBuilder,
    route,
    settings::{RpcSettings, Settings},
    upstream::{Upstream, UpstreamBuilder},
};

pub struct Application {
    listener: TcpListener,
    router: Router,
    port: u16,
    tasks: JoinSet<()>,
}

impl Application {
    pub async fn build(settings: Settings, observer: Arc<dyn Observer>) -> Result<Self> {
        let upstreams = build_upstreams(settings.rpcs, settings.rpc_timeout_in_secs);
        let upstream_count = upstreams.len();

        let decider = Arc::new(RoundRobin::new(upstreams).context("failed to build decider")?);

        let state = ProxyStateBuilder::default()
            .with_max_attempt(settings.max_attempt)
            .with_retry_after_in_secs(settings.retry_after_in_secs)
            .with_decider(decider)
            .with_observer(observer)
            .build()
            .context("failed to build proxy state")?;

        let router = route::build_router(Arc::new(state));
        let addr = format!("127.0.0.1:{}", settings.application_port);
        let listener = TcpListener::bind(&addr)
            .await
            .with_context(|| format!("failed to bind {addr}"))?;
        let port = listener
            .local_addr()
            .context("failed to read the bound address")?
            .port();
        tracing::info!(event = "startup_ready", upstreams = upstream_count, port,);

        Ok(Application {
            listener,
            router,
            port,
            tasks: JoinSet::new(),
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
            match UpstreamBuilder::default()
                .with_label(item.label.as_str())
                .with_rpc_timeout_in_secs(rpc_timeout_in_secs)
                .with_url(item.rpc_url)
                .build()
            {
                Ok(upstream) => Some(upstream),
                Err(err) => {
                    tracing::warn!(
                        event = "upstream_skipped",
                        upstream = %&item.label,
                        error = %err,
                    );
                    None
                }
            }
        })
        .collect()
}

/// Owns the bind so that `startup_ready` can be emitted once the socket is
/// actually listening, and so the upstream count never has to escape to `main`
/// just to be logged. `main` is left with nothing but the serve loop.
pub async fn start(settings: Settings) -> Result<(TcpListener, Router)> {
    let upstreams = build_upstreams(settings.rpcs, settings.rpc_timeout_in_secs);
    let upstream_count = upstreams.len();

    let decider = Arc::new(RoundRobin::new(upstreams).context("failed to build decider")?);
    let observer = Arc::new(NoopObserver::new());

    let state = ProxyStateBuilder::new(decider, observer)
        .with_max_attempt(settings.max_attempt)?
        .with_retry_after_in_secs(settings.retry_after_in_secs)
        .build();

    let app = route::build_router(Arc::new(state));

    let addr = format!("127.0.0.1:{}", settings.application_port);
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("failed to bind {addr}"))?;

    tracing::info!(
        event = "startup_ready",
        upstreams = upstream_count,
        port = settings.application_port,
    );

    Ok((listener, app))
}
