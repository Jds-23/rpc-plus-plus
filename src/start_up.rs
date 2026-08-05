use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Router;
use tokio::net::TcpListener;

use crate::{
    decider::round_robin::RoundRobin,
    proxy::ProxyStateBuilder,
    route,
    rpc_handler::{RpcHandler, RpcHandlerBuilder},
    settings::{RpcSettings, Settings},
};

pub fn build_handlers<I>(rpcs: I, rpc_timeout_in_secs: u64) -> Vec<RpcHandler>
where
    I: IntoIterator<Item = RpcSettings>,
{
    rpcs.into_iter()
        .filter_map(|item| {
            match RpcHandlerBuilder::default()
                .with_label(item.label.clone())
                .with_rpc_timeout_in_secs(rpc_timeout_in_secs)
                .with_url(item.rpc_url)
                .build()
            {
                Ok(item) => Some(item),
                Err(err) => {
                    tracing::warn!(
                        event = "upstream_skipped",
                        upstream = %&item.label,
                        error = format!("{err:#}"),
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
    let handlers = build_handlers(settings.rpcs, settings.rpc_timeout_in_secs);
    let upstreams = handlers.len();

    let decider = Arc::new(RoundRobin::new(handlers).context("failed to build decider")?);

    let state = ProxyStateBuilder::default()
        .with_max_attempt(settings.max_attempt)
        .with_retry_after_in_secs(settings.retry_after_in_secs)
        .with_decider(decider)
        .build()
        .context("failed to build proxy state")?;

    let app = route::build_router(Arc::new(state));

    let addr = format!("127.0.0.1:{}", settings.application_port);
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("failed to bind {addr}"))?;

    tracing::info!(
        event = "startup_ready",
        upstreams,
        port = settings.application_port,
    );

    Ok((listener, app))
}
