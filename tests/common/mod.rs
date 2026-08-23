// Each integration test binary compiles this module in full, so whatever a given
// binary does not reach for reads as dead code. `healthz` needs no mock upstream;
// only the shutdown test touches `TestApp`'s handles.
#![allow(dead_code)]

pub mod mock_rpc_server;
use std::{sync::Arc, time::Duration};

use rpc_plus_plus::{
    decider::{Decider, prefer_least_errors::PreferLeastErrors, round_robin::RoundRobin},
    observer::MetricsObserver,
    settings::{ApplicationSettings, DeciderKind, ProxySettings, RpcSettings, Settings},
    start_up::{Application, build_upstreams},
    upstream::{Upstream, UpstreamId},
};
use tokio::task::{JoinHandle, JoinSet};
use tokio_util::sync::CancellationToken;

pub fn test_settings(rpcs: Vec<RpcSettings>) -> Settings {
    Settings {
        rpcs,
        application: ApplicationSettings {
            host: "127.0.0.1".to_string(),
            port: 0,
            proxy: ProxySettings {
                max_attempt: 3,
                retry_after_in_secs: 1,
            },
        },
        rpc_timeout_in_secs: 1,
        decider: DeciderKind::RoundRobin,
    }
}

pub fn rpc(label: &str, rpc_url: impl Into<String>) -> RpcSettings {
    RpcSettings {
        label: label.to_string(),
        rpc_url: rpc_url.into(),
    }
}

pub struct TestApp {
    pub addr: String,
    pub shutdown: CancellationToken,
    pub server: JoinHandle<anyhow::Result<()>>,
}

pub fn upstreams(settings: &Settings) -> Vec<Upstream> {
    let rpcs: Vec<RpcSettings> = settings
        .rpcs
        .iter()
        .map(|entry| rpc(&entry.label, entry.rpc_url.clone()))
        .collect();

    build_upstreams(rpcs, settings.rpc_timeout_in_secs)
}

fn round_robin(settings: &Settings) -> Arc<dyn Decider> {
    Arc::new(RoundRobin::new(upstreams(settings)).expect("decider build failed"))
}

/// Dropping a `JoinSet` aborts its tasks, so the refresher lives exactly as long
/// as the caller's `tasks`.
pub fn prefer_least_errors(
    settings: &Settings,
    observer: Arc<MetricsObserver>,
    tasks: &mut JoinSet<()>,
    shutdown: CancellationToken,
    refresh: Duration,
    window: usize,
) -> Arc<dyn Decider> {
    PreferLeastErrors::spawn(
        upstreams(settings),
        observer,
        tasks,
        shutdown,
        refresh,
        window,
    )
    .expect("decider build failed")
}

pub fn observer_for(settings: &Settings) -> Arc<MetricsObserver> {
    Arc::new(MetricsObserver::new(
        settings
            .rpcs
            .iter()
            .map(|rpc| UpstreamId::new(rpc.label.as_str())),
    ))
}

pub async fn spawn_app_with_handle(settings: Settings) -> TestApp {
    let observer = observer_for(&settings);
    let decider = round_robin(&settings);
    build_app(settings.application, observer, decider).await
}

pub async fn spawn_app(settings: Settings) -> String {
    spawn_app_with_handle(settings).await.addr
}

pub async fn spawn_app_with_observer(settings: Settings, observer: Arc<MetricsObserver>) -> String {
    let decider = round_robin(&settings);
    spawn_app_with_observer_and_decider(settings, observer, decider).await
}

pub async fn spawn_app_with_decider(settings: Settings, decider: Arc<dyn Decider>) -> String {
    let observer = observer_for(&settings);
    spawn_app_with_observer_and_decider(settings, observer, decider).await
}

/// One observer for both: a decider scoring off its own instance reads only zeros.
pub async fn spawn_app_with_observer_and_decider(
    settings: Settings,
    observer: Arc<MetricsObserver>,
    decider: Arc<dyn Decider>,
) -> String {
    build_app(settings.application, observer, decider)
        .await
        .addr
}

async fn build_app(
    application: ApplicationSettings,
    observer: Arc<MetricsObserver>,
    decider: Arc<dyn Decider>,
) -> TestApp {
    let app = Application::build(application, observer, decider, JoinSet::new())
        .await
        .expect("app build failed");
    let addr = format!("http://127.0.0.1:{}", app.port());

    let shutdown = CancellationToken::new();
    let server = tokio::spawn(app.run_until_stopped(shutdown.clone()));

    TestApp {
        addr,
        shutdown,
        server,
    }
}
