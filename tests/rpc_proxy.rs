use std::time::{Duration, Instant};

use futures_util::future::join_all;
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::common::{mock_rpc_server, rpc, spawn_app, test_settings};

mod common;

#[tokio::test]
async fn proxies_reponse_from_upstream() {
    let mock_rpc_server = mock_rpc_server::ok("0x1").await;
    let settings = test_settings(vec![rpc("one", mock_rpc_server.uri())]);
    let addr = spawn_app(settings).await;

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
    let settings = test_settings(vec![rpc("one", a.uri()), rpc("two", b.uri())]);
    let addr = spawn_app(settings).await;

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
    let mut settings = test_settings(vec![rpc("one", a.uri()), rpc("two", b.uri())]);
    settings.application.proxy.retry_after_in_secs = 0;
    let addr = spawn_app(settings).await;

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
    let mut settings = test_settings(vec![rpc("one", a.uri()), rpc("two", b.uri())]);
    settings.application.proxy.max_attempt = 2;
    settings.application.proxy.retry_after_in_secs = 0;
    let addr = spawn_app(settings).await;

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
    let mut settings = test_settings(vec![rpc("one", a.uri())]);
    settings.application.proxy.max_attempt = 3;
    settings.application.proxy.retry_after_in_secs = 3;
    let addr = spawn_app(settings).await;

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

/// The throttled upstream answers HTTP 200, so only the decoded body marks it as
/// a failure the chain should move past.
#[tokio::test]
async fn a_json_rpc_error_moves_to_the_next_upstream() {
    let limited = mock_rpc_server::rpc_erroring(-32005, "limit exceeded").await;
    let live = mock_rpc_server::ok("0xb").await;
    let mut settings = test_settings(vec![rpc("one", limited.uri()), rpc("two", live.uri())]);
    settings.application.proxy.retry_after_in_secs = 0;
    let addr = spawn_app(settings).await;

    let res = reqwest::Client::new()
        .post(format!("{addr}/rpc"))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}))
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 200);
    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["result"], "0xb");

    assert_eq!(limited.received_requests().await.unwrap().len(), 1);
    assert_eq!(live.received_requests().await.unwrap().len(), 1);
}

const SEND_RAW: &str = "eth_sendRawTransaction";

/// The first upstream is slower than `hedge.after_in_millis`; a read would be
/// hedged to the second, a write must wait it out.
#[tokio::test]
async fn a_slow_write_is_not_hedged() {
    let slow = mock_rpc_server::slow("0xa", std::time::Duration::from_millis(500)).await;
    let fast = mock_rpc_server::ok("0xb").await;
    let mut settings = test_settings(vec![rpc("one", slow.uri()), rpc("two", fast.uri())]);
    settings.application.proxy.hedge.enabled = true;
    settings.application.proxy.hedge.after_in_millis = 50;
    let addr = spawn_app(settings).await;

    let res = reqwest::Client::new()
        .post(format!("{addr}/rpc"))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":SEND_RAW,"params":["0x00"]}))
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["result"], "0xa", "we waited for the only legal call");
    assert_eq!(slow.received_requests().await.unwrap().len(), 1);
    assert_eq!(
        fast.received_requests().await.unwrap().len(),
        0,
        "a write must never reach a second upstream"
    );
}

#[tokio::test]
async fn a_failed_write_is_not_retried_while_hedging() {
    let down = mock_rpc_server::failing(StatusCode::SERVICE_UNAVAILABLE).await;
    let live = mock_rpc_server::ok("0xb").await;
    let mut settings = test_settings(vec![rpc("one", down.uri()), rpc("two", live.uri())]);
    settings.application.proxy.hedge.enabled = true;
    settings.application.proxy.retry_after_in_secs = 0;
    let addr = spawn_app(settings).await;

    let res = reqwest::Client::new()
        .post(format!("{addr}/rpc"))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":SEND_RAW,"params":["0x00"]}))
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["error"]["code"], -32603, "its own failure");
    assert_eq!(down.received_requests().await.unwrap().len(), 1);
    assert_eq!(
        live.received_requests().await.unwrap().len(),
        0,
        "the first send may already have landed"
    );
}

/// The control for the two above: the same slow chain does hedge a read.
#[tokio::test]
async fn a_slow_read_is_hedged() {
    let slow = mock_rpc_server::slow("0xa", std::time::Duration::from_millis(500)).await;
    let fast = mock_rpc_server::ok("0xb").await;
    let mut settings = test_settings(vec![rpc("one", slow.uri()), rpc("two", fast.uri())]);
    settings.application.proxy.hedge.enabled = true;
    settings.application.proxy.hedge.after_in_millis = 50;
    let addr = spawn_app(settings).await;

    let res = reqwest::Client::new()
        .post(format!("{addr}/rpc"))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}))
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = res.json().await.unwrap();
    assert_eq!(body["result"], "0xb", "the hedge answered first");
}

const UPSTREAM_DELAY: Duration = Duration::from_millis(300);

/// One slow upstream behind a proxy with dedup set to `dedup`.
async fn slow_proxy(dedup: bool) -> (String, wiremock::MockServer) {
    let upstream = mock_rpc_server::slow("0x10", UPSTREAM_DELAY).await;
    let mut settings = test_settings(vec![rpc("one", upstream.uri())]);
    settings.application.proxy.dedup.enabled = dedup;
    (spawn_app(settings).await, upstream)
}

async fn post(client: &reqwest::Client, addr: &str, request: Value) -> Value {
    let res = client
        .post(format!("{addr}/rpc"))
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), 200);
    res.json().await.unwrap()
}

/// `requests` sent together, so they overlap in flight.
async fn all_at_once(addr: &str, requests: Vec<Value>) -> Vec<Value> {
    let client = reqwest::Client::new();
    join_all(requests.into_iter().map(|r| post(&client, addr, r))).await
}

fn block_number(id: Value) -> Value {
    json!({"jsonrpc":"2.0","id":id,"method":"eth_blockNumber","params":[]})
}

/// `rpc_requests_coalesced_total` as `/metrics` reports it.
async fn coalesced(addr: &str) -> u64 {
    let metrics = reqwest::get(format!("{addr}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    metrics
        .lines()
        .find_map(|line| line.strip_prefix("rpc_requests_coalesced_total "))
        .expect("the family is registered even with dedup off")
        .parse()
        .unwrap()
}

async fn hits(upstream: &wiremock::MockServer) -> usize {
    upstream.received_requests().await.unwrap().len()
}

#[tokio::test]
async fn concurrent_identical_reads_share_one_upstream_call() {
    let (addr, upstream) = slow_proxy(true).await;
    let mut ids: Vec<Value> = (0..10).map(Value::from).collect();
    ids.push(json!("abc"));

    let answers = all_at_once(&addr, ids.iter().cloned().map(block_number).collect()).await;

    assert_eq!(
        hits(&upstream).await,
        1,
        "11 identical reads, one upstream call"
    );
    assert_eq!(coalesced(&addr).await, 10, "everyone but the leader");
    for (answer, id) in answers.iter().zip(&ids) {
        assert_eq!(&answer["id"], id, "every caller gets its own id back");
        assert_eq!(answer["result"], "0x10");
    }
}

#[tokio::test]
async fn identical_writes_are_never_coalesced() {
    let (addr, upstream) = slow_proxy(true).await;
    let send = |id: u64| json!({"jsonrpc":"2.0","id":id,"method":"eth_sendRawTransaction","params":["0xf86c"]});

    all_at_once(&addr, vec![send(1), send(2)]).await;

    assert_eq!(hits(&upstream).await, 2, "two sends are two intents");
}

#[tokio::test]
async fn different_params_are_different_questions() {
    let (addr, upstream) = slow_proxy(true).await;
    let balance = |who: &str| json!({"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":[who, "latest"]});

    all_at_once(&addr, vec![balance("0xaa"), balance("0xbb")]).await;

    assert_eq!(hits(&upstream).await, 2);
}

#[tokio::test]
async fn the_leader_hanging_up_strands_no_follower() {
    let (addr, upstream) = slow_proxy(true).await;
    let client = reqwest::Client::new();

    let leader = tokio::spawn({
        let (client, addr) = (client.clone(), addr.clone());
        async move { post(&client, &addr, block_number(json!(0))).await }
    });
    tokio::time::sleep(UPSTREAM_DELAY / 6).await;
    let followers: Vec<_> = (1..4)
        .map(|id| {
            let (client, addr) = (client.clone(), addr.clone());
            tokio::spawn(async move { post(&client, &addr, block_number(json!(id))).await })
        })
        .collect();
    tokio::time::sleep(UPSTREAM_DELAY / 6).await;
    leader.abort();

    for (id, follower) in (1..4).zip(followers) {
        let answer = follower.await.expect("follower panicked");
        assert_eq!(
            answer["id"], id,
            "a follower is answered after the leader left"
        );
        assert_eq!(answer["result"], "0x10");
    }
    assert_eq!(hits(&upstream).await, 1, "and it is still the one call");
}

#[tokio::test]
async fn a_finished_flight_is_not_a_cache() {
    let (addr, upstream) = slow_proxy(true).await;

    all_at_once(&addr, vec![block_number(json!(1))]).await;
    all_at_once(&addr, vec![block_number(json!(2))]).await;

    assert_eq!(hits(&upstream).await, 2, "a later request asks again");
}

#[tokio::test]
async fn dedup_off_sends_every_request() {
    let (addr, upstream) = slow_proxy(false).await;

    all_at_once(&addr, (0..5).map(|id| block_number(json!(id))).collect()).await;

    assert_eq!(
        hits(&upstream).await,
        5,
        "off by default, and off means today's path"
    );
    assert_eq!(coalesced(&addr).await, 0);
}
