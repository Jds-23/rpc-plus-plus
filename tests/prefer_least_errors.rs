use std::time::{Duration, Instant};

use reqwest::StatusCode;
use serde_json::json;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::common::{
    mock_rpc_server, observer_for, prefer_least_errors, rpc, spawn_app_with_observer_and_decider,
    test_settings,
};

mod common;

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
        100,
    );

    let addr = spawn_app_with_observer_and_decider(settings, observer, decider).await;

    let client = reqwest::Client::new();
    let send = || async {
        let res = client
            .post(format!("{addr}/rpc"))
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}))
            .send()
            .await
            .unwrap();
        let body: serde_json::Value = res.json().await.unwrap();
        assert_eq!(body["result"], "0xa");
    };

    for _ in 0..20 {
        send().await;
    }
    assert_eq!(
        sick.received_requests().await.unwrap().len(),
        20,
        "`sick` leads until the ranking has seen enough calls"
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let before = sick.received_requests().await.unwrap().len();
        send().await;
        if sick.received_requests().await.unwrap().len() == before {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the proxy kept leading with `sick` after recording its failures"
        );
    }

    shutdown.cancel();
    tasks.join_all().await;
}
