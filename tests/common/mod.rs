// Each integration test binary compiles this module in full, so whatever a given
// binary does not reach for reads as dead code. `healthz` needs no mock upstream;
// only the shutdown test touches `TestApp`'s handles.
#![allow(dead_code)]

pub mod mock_rpc_server;
use std::sync::Arc;

use rpc_plus_plus::{
    observer::Observer,
    settings::{RpcSettings, Settings},
    start_up::Application,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub fn test_settings(rpcs: Vec<RpcSettings>) -> Settings {
    Settings {
        rpcs,
        application_host: "127.0.0.1".to_string(),
        application_port: 0,
        max_attempt: 3,
        rpc_timeout_in_secs: 1,
        retry_after_in_secs: 1,
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

pub async fn spawn_app_with_handle(settings: Settings, observer: Arc<dyn Observer>) -> TestApp {
    let app = Application::build(settings, observer)
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

pub async fn spawn_app(settings: Settings, observer: Arc<dyn Observer>) -> String {
    spawn_app_with_handle(settings, observer).await.addr
}
