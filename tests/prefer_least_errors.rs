use std::time::Duration;

use reqwest::StatusCode;
use rpc_plus_plus::upstream::UpstreamId;
use serde_json::json;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::common::{
    mock_rpc_server, observer_for, prefer_least_errors, rpc, spawn_app_with_observer_and_decider,
    test_settings,
};

mod common;

/// The ranking itself is covered by unit tests against `refresh`. What only an
/// integration test can show is that the observer the proxy records into is the
/// one the refresher scores from — a second instance would read zeros forever.
#[tokio::test]
async fn the_decider_scores_from_the_observer_the_proxy_writes_to() {
    let healthy = mock_rpc_server::ok("0xa").await;
    let sick = mock_rpc_server::failing(StatusCode::INTERNAL_SERVER_ERROR).await;

    let mut settings = test_settings(vec![rpc("sick", sick.uri()), rpc("healthy", healthy.uri())]);
    settings.application.proxy.retry_after_in_secs = 0;

    let observer = observer_for(&settings);
    let shutdown = CancellationToken::new();
    let mut tasks = JoinSet::new();

    let decider = prefer_least_errors(
        &settings,
        observer.clone(),
        &mut tasks,
        shutdown.clone(),
        Duration::from_millis(50),
        1,
    );
    assert_eq!(tasks.len(), 1, "the refresher should be parked in `tasks`");

    let addr = spawn_app_with_observer_and_decider(settings, observer.clone(), decider).await;

    let client = reqwest::Client::new();
    for _ in 0..10 {
        client
            .post(format!("{addr}/rpc"))
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}))
            .send()
            .await
            .unwrap();
    }

    let sick_stats = observer.snapshot(&UpstreamId::new("sick")).unwrap();
    let healthy_stats = observer.snapshot(&UpstreamId::new("healthy")).unwrap();
    assert!(sick_stats.error_status > 0, "proxy recorded no failures");
    assert!(healthy_stats.success > 0, "proxy recorded no successes");

    shutdown.cancel();
    tasks.join_all().await;
}
