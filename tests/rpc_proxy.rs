use reqwest::StatusCode;
use rpc_plus_plus::{
    rpc_handler::round_robin_handler::RoundRobinHandlerBuilder, settings::RpcSettings,
};
use serde_json::json;

use crate::common::{mock_rpc_server, spawn_app};

mod common;

#[tokio::test]
async fn proxies_reponse_from_upstream() {
    let mock_rpc_server = mock_rpc_server::ok("0x1").await;
    let upstreams: Vec<RpcSettings> = vec![RpcSettings {
        label: "one".to_string(),
        rpc_url: mock_rpc_server.uri(),
    }];
    let state = RoundRobinHandlerBuilder::default()
        .with_rpc_setttings(upstreams)
        .build()
        .expect("State build failed");
    let addr = spawn_app(state).await;

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
    let state = RoundRobinHandlerBuilder::default()
        .with_rpc_setttings(upstreams)
        .build()
        .expect("State build failed");
    let addr = spawn_app(state).await;

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
    let state = RoundRobinHandlerBuilder::default()
        .with_rpc_setttings(upstreams)
        .with_retry_after_in_secs(0)
        .build()
        .expect("State build failed");
    let addr = spawn_app(state).await;

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

    assert_eq!(a.received_requests().await.unwrap().len(), 4);
    assert_eq!(b.received_requests().await.unwrap().len(), 3);
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
    let state = RoundRobinHandlerBuilder::default()
        .with_rpc_setttings(upstreams)
        .with_max_attempt(2)
        .with_retry_after_in_secs(0)
        .build()
        .expect("State build failed");
    let addr = spawn_app(state).await;

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

    assert_eq!(a.received_requests().await.unwrap().len(), 6);
    assert_eq!(b.received_requests().await.unwrap().len(), 6);
}
