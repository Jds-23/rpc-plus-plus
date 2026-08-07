use std::{sync::Arc, time::Instant};

use reqwest::StatusCode;
use rpc_plus_plus::{observer::NoopObserver, proxy::ProxyStateBuilder, settings::RpcSettings};
use serde_json::json;

use crate::common::{mock_rpc_server, round_robin, spawn_app};

mod common;

#[tokio::test]
async fn proxies_reponse_from_upstream() {
    let mock_rpc_server = mock_rpc_server::ok("0x1").await;
    let upstreams: Vec<RpcSettings> = vec![RpcSettings {
        label: "one".to_string(),
        rpc_url: mock_rpc_server.uri(),
    }];
    let observer = Arc::new(NoopObserver::new());

    let state = ProxyStateBuilder::new(round_robin(upstreams), observer)
        .build()
        .expect("State build failed");
    let addr = spawn_app(Arc::new(state)).await;

    let res = reqwest::Client::new()
        .post(format!("{addr}/rpc"))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["result"], "0x1");
}

#[tokio::test]
async fn round_robin_distributes_evenly() {
    let a = mock_rpc_server::ok("0xa").await;
    let b = mock_rpc_server::ok("0xb").await;
    let upstreams: Vec<RpcSettings> = vec![
        RpcSettings {
            label: "one".to_string(),
            rpc_url: a.uri(),
        },
        RpcSettings {
            label: "two".to_string(),
            rpc_url: b.uri(),
        },
    ];
    let state = ProxyStateBuilder::new(round_robin(upstreams), Arc::new(NoopObserver::new()))
        .build()
        .expect("State build failed");
    let addr = spawn_app(Arc::new(state)).await;

    let client = reqwest::Client::new();
    for index in 0..4 {
        let res = client
            .post(format!("{addr}/rpc"))
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}))
            .send()
            .await
            .unwrap();
        assert!(res.status().is_success());
        let body: serde_json::Value = res.json().await.unwrap();
        if index % 2 == 0 {
            assert_eq!(body["result"], "0xa");
        } else {
            assert_eq!(body["result"], "0xb");
        }
    }

    // Unaffected by the chain: when everything succeeds only the head is ever used.
    assert_eq!(a.received_requests().await.unwrap().len(), 2);
    assert_eq!(b.received_requests().await.unwrap().len(), 2);
}

#[tokio::test]
async fn works_fine_when_one_uptream_works() {
    let a = mock_rpc_server::ok("0xa").await;
    let b = mock_rpc_server::failing(StatusCode::SERVICE_UNAVAILABLE).await;
    let upstreams: Vec<RpcSettings> = vec![
        RpcSettings {
            label: "one".to_string(),
            rpc_url: a.uri(),
        },
        RpcSettings {
            label: "two".to_string(),
            rpc_url: b.uri(),
        },
    ];
    let state = ProxyStateBuilder::new(round_robin(upstreams), Arc::new(NoopObserver::new()))
        .with_retry_after_in_secs(0)
        .build()
        .expect("State build failed");
    let addr = spawn_app(Arc::new(state)).await;

    let client = reqwest::Client::new();
    for _ in 0..4 {
        let res = client
            .post(format!("{addr}/rpc"))
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}))
            .send()
            .await
            .unwrap();
        assert!(res.status().is_success());
        let body: serde_json::Value = res.json().await.unwrap();
        assert_eq!(body["result"], "0xa");
    }

    // The cursor advances once per request, so the chain starts at `b` on every
    // other request. `b` is only tried on the two requests where it leads; under
    // v0.1's per-attempt rotation it saw 3.
    assert_eq!(a.received_requests().await.unwrap().len(), 4);
    assert_eq!(b.received_requests().await.unwrap().len(), 2);
}

#[tokio::test]
async fn non_works_fine_retry_and_propagate_last_error() {
    let a = mock_rpc_server::failing(StatusCode::SERVICE_UNAVAILABLE).await;
    let b = mock_rpc_server::failing(StatusCode::SERVICE_UNAVAILABLE).await;
    let upstreams: Vec<RpcSettings> = vec![
        RpcSettings {
            label: "one".to_string(),
            rpc_url: a.uri(),
        },
        RpcSettings {
            label: "two".to_string(),
            rpc_url: b.uri(),
        },
    ];
    let state = ProxyStateBuilder::new(round_robin(upstreams), Arc::new(NoopObserver::new()))
        .with_max_attempt(2)
        .with_retry_after_in_secs(0)
        .build()
        .expect("State build failed");
    let addr = spawn_app(Arc::new(state)).await;

    let client = reqwest::Client::new();
    for _ in 0..4 {
        let res = client
            .post(format!("{addr}/rpc"))
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}))
            .send()
            .await
            .unwrap();
        assert!(res.status().is_success());
        let body: serde_json::Value = res.json().await.unwrap();
        assert_eq!(body["error"]["code"], -32603);
    }

    // max_attempt 2 over 2 upstreams is a full-length chain, so the split is
    // unchanged: 4 requests x 2 distinct upstreams = 8 calls.
    assert_eq!(a.received_requests().await.unwrap().len(), 4);
    assert_eq!(b.received_requests().await.unwrap().len(), 4);
}

/// `max_attempt` is a cap over *distinct* upstreams, not a retry budget. With one
/// upstream configured the chain has one entry, so a failure is answered after a
/// single attempt and `retry_after` never elapses.
#[tokio::test]
async fn single_upstream_is_tried_once() {
    let a = mock_rpc_server::failing(StatusCode::SERVICE_UNAVAILABLE).await;
    let upstreams: Vec<RpcSettings> = vec![RpcSettings {
        label: "one".to_string(),
        rpc_url: a.uri(),
    }];
    let state = ProxyStateBuilder::new(round_robin(upstreams), Arc::new(NoopObserver::new()))
        .with_max_attempt(3)
        .with_retry_after_in_secs(3)
        .build()
        .expect("State build failed");
    let addr = spawn_app(Arc::new(state)).await;

    let started_at = Instant::now();
    let res = reqwest::Client::new()
        .post(format!("{addr}/rpc"))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}))
        .send()
        .await
        .unwrap();
    let elapsed = started_at.elapsed();

    assert!(res.status().is_success());
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["error"]["code"], -32603);

    // One attempt, despite max_attempt 3.
    assert_eq!(a.received_requests().await.unwrap().len(), 1);
    // No inter-attempt sleep, so nowhere near the 3s retry_after.
    assert!(
        elapsed < std::time::Duration::from_secs(1),
        "answered in {elapsed:?}, expected no retry_after sleep"
    );
}
