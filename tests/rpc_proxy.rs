use rpc_plus_plus::settings::RpcSettings;
use serde_json::json;

use crate::common::{mock_rpc_server, spawn_app};

mod common;

#[tokio::test]
async fn proxies_reponse_from_upstream() {
    let mock_rpc_server=mock_rpc_server::get("0x1").await;
    let upstreams: Vec<RpcSettings>=vec![RpcSettings{
        label:"one".to_string(),
        rpc_url:mock_rpc_server.uri()
    }];
    let addr = spawn_app(upstreams).await;

    let res=reqwest::Client::new()
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
    let a = mock_rpc_server::get("0xa").await;
    let b = mock_rpc_server::get("0xb").await;
    let addr = common::spawn_app(vec![RpcSettings{
        label:"one".to_string(),
        rpc_url:a.uri()
    }, 
    RpcSettings{
        label:"two".to_string(),
        rpc_url:b.uri()
    }]).await;

    let client = reqwest::Client::new();
    for _ in 0..4 {
        client
            .post(format!("{addr}/rpc"))
            .json(&json!({"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}))
            .send()
            .await
            .unwrap();
    }

    assert_eq!(a.received_requests().await.unwrap().len(), 2);
    assert_eq!(b.received_requests().await.unwrap().len(), 2);
}