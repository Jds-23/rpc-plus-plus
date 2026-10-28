use std::{sync::Arc, time::Instant};

use futures_util::future::join_all;
use reqwest::StatusCode;
use rpc_plus_plus::{
    config::{Settings, UpstreamSettings},
    observer::{
        MetricsObserver,
        snapshot::{HedgeSnapshot, RequestSnapshot},
    },
    upstream::UpstreamId,
};
use serde_json::json;

use crate::common::{mock_rpc_server, rpc, spawn_app_with_observer, test_settings};

mod common;

/// Nothing listens here, so the call fails before a response exists.
const DEAD_URL: &str = "http://127.0.0.1:1";

/// Every upstream gets a turn, and `retry_after` is zeroed so the loop does not
/// sleep between attempts.
fn settings(upstreams: Vec<UpstreamSettings>) -> Settings {
    let mut settings = test_settings(upstreams);
    settings.application.proxy.max_attempt = settings.upstreams.len() as u64;
    settings.application.proxy.retry_after_in_secs = 0;
    settings
}

/// `MetricsObserver` only counts upstreams it was built with, so the ids come
/// back alongside it to read the stats out again.
fn observer(labels: &[&str]) -> (Vec<UpstreamId>, Arc<MetricsObserver>) {
    let ids: Vec<UpstreamId> = labels.iter().copied().map(UpstreamId::new).collect();
    let observer = Arc::new(MetricsObserver::new(ids.clone()));
    (ids, observer)
}

async fn post(addr: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{addr}/rpc"))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}))
        .send()
        .await
        .unwrap()
}

/// Counters carry no ordering, so what is asserted is the split: the failure
/// lands on the upstream that produced it and never on its replacement.
#[tokio::test]
async fn each_upstream_counts_its_own_outcome() {
    let live = mock_rpc_server::ok("0x1").await;
    let (ids, observer) = observer(&["dead", "live"]);
    let addr = spawn_app_with_observer(
        settings(vec![rpc("dead", DEAD_URL), rpc("live", live.uri())]),
        observer.clone(),
    )
    .await;

    let res = post(&addr).await;
    assert_eq!(res.status(), 200);

    let dead = observer.snapshot(&ids[0]).unwrap();
    assert_eq!(dead.unreachable, 1, "no listener, so nothing to connect to");
    assert_eq!(dead.success, 0);
    assert_eq!(dead.read_failed, 0);
    assert_eq!(dead.error_status, 0);

    let live = observer.snapshot(&ids[1]).unwrap();
    assert_eq!(live.success, 1);
    assert_eq!(live.unreachable, 0);
    assert_eq!(live.read_failed, 0);
    assert_eq!(live.error_status, 0);
}

/// The snapshot classes a failure without keeping its code, so the 500 itself is
/// only visible in the error the client gets back.
#[tokio::test]
async fn an_error_status_lands_in_the_error_status_counter() {
    let failing = mock_rpc_server::failing(StatusCode::INTERNAL_SERVER_ERROR).await;
    let (ids, observer) = observer(&["one"]);
    let addr =
        spawn_app_with_observer(settings(vec![rpc("one", failing.uri())]), observer.clone()).await;

    let body: serde_json::Value = post(&addr).await.json().await.unwrap();
    assert_eq!(
        body["error"]["message"],
        "upstream returned error status 500"
    );

    let stats = observer.snapshot(&ids[0]).unwrap();
    assert_eq!(stats.error_status, 1);
    assert_eq!(stats.success, 0);
    assert_eq!(stats.unreachable, 0);
    assert_eq!(stats.read_failed, 0);
}

#[tokio::test]
async fn recorded_duration_stays_within_the_request() {
    let live = mock_rpc_server::ok("0x1").await;
    let (ids, observer) = observer(&["dead", "live"]);
    let addr = spawn_app_with_observer(
        settings(vec![rpc("dead", DEAD_URL), rpc("live", live.uri())]),
        observer.clone(),
    )
    .await;

    let started = Instant::now();
    post(&addr).await;
    let whole_request = started.elapsed().as_micros() as u64;

    let recorded: u64 = ids
        .iter()
        .map(|id| observer.snapshot(id).unwrap().duration_micros_total)
        .sum();
    assert!(
        recorded <= whole_request,
        "recorded {recorded}us exceeds the request it happened inside ({whole_request}us)"
    );
}

/// A rate limit arrives as HTTP 200 with a JSON-RPC `error` member. Until the
/// body was decoded this read as a clean success, which is what left the ranking
/// blind to a throttled provider.
#[tokio::test]
async fn a_json_rpc_error_on_a_200_is_not_a_success() {
    let limited = mock_rpc_server::rpc_erroring(-32005, "limit exceeded").await;
    let (ids, observer) = observer(&["limited"]);
    let addr = spawn_app_with_observer(
        settings(vec![rpc("limited", limited.uri())]),
        observer.clone(),
    )
    .await;

    post(&addr).await;

    let stats = observer.snapshot(&ids[0]).unwrap();
    assert_eq!(stats.rpc_error, 1);
    assert_eq!(stats.success, 0, "the HTTP status was 200 and it still is");
    assert_eq!(stats.error_status, 0);
    assert_eq!(stats.unreachable, 0);
    assert_eq!(stats.read_failed, 0);
}

/// Both counters land on the pair: the slow upstream that ran past its
/// `hedge_after`, and the one the hedge went to.
#[tokio::test]
async fn a_hedge_and_its_win_land_on_the_overtaken_pair() {
    let slow = mock_rpc_server::slow("0xa", std::time::Duration::from_millis(500)).await;
    let fast = mock_rpc_server::ok("0xb").await;
    let mut settings = settings(vec![rpc("slow", slow.uri()), rpc("fast", fast.uri())]);
    settings.application.proxy.hedge.enabled = true;
    settings.application.proxy.hedge.after_in_millis = 50;
    let (ids, observer) = observer(&["slow", "fast"]);
    let addr = spawn_app_with_observer(settings, observer.clone()).await;

    let res = post(&addr).await;
    assert_eq!(res.status(), 200);

    let slow_to_fast = observer.hedge_snapshot(&ids[0], &ids[1]).unwrap();
    assert_eq!(slow_to_fast, HedgeSnapshot { started: 1, won: 1 });

    let fast_to_slow = observer.hedge_snapshot(&ids[1], &ids[0]).unwrap();
    assert_eq!(fast_to_slow, HedgeSnapshot { started: 0, won: 0 });

    assert_eq!(observer.snapshot(&ids[1]).unwrap().success, 1);
}

#[tokio::test]
async fn an_overtaken_call_is_recorded_as_abandoned_at_the_overtake() {
    let slow = mock_rpc_server::slow("0xa", std::time::Duration::from_millis(500)).await;
    let fast = mock_rpc_server::ok("0xb").await;
    let mut settings = settings(vec![rpc("slow", slow.uri()), rpc("fast", fast.uri())]);
    settings.application.proxy.hedge.enabled = true;
    settings.application.proxy.hedge.after_in_millis = 50;
    let (ids, observer) = observer(&["slow", "fast"]);
    let addr = spawn_app_with_observer(settings, observer.clone()).await;

    post(&addr).await;

    let slow = observer.snapshot(&ids[0]).unwrap();
    assert_eq!(slow.abandoned, 1);
    assert_eq!(slow.success, 0, "it never answered");
    assert!(
        (50_000..500_000).contains(&slow.duration_micros_total),
        "dropped once the hedge answered, past hedge_after and short of its 500ms: {}us",
        slow.duration_micros_total
    );
}

#[tokio::test]
async fn every_follower_of_a_flight_is_counted_coalesced() {
    let slow = mock_rpc_server::slow("0x10", std::time::Duration::from_millis(300)).await;
    let mut settings = settings(vec![rpc("slow", slow.uri())]);
    settings.application.proxy.dedup.enabled = true;
    let (_, observer) = observer(&["slow"]);
    let addr = spawn_app_with_observer(settings, observer.clone()).await;

    let client = reqwest::Client::new();
    join_all((0..5).map(|id| {
        client
            .post(format!("{addr}/rpc"))
            .json(&json!({"jsonrpc":"2.0","id":id,"method":"eth_blockNumber","params":[]}))
            .send()
    }))
    .await;

    assert_eq!(slow.received_requests().await.unwrap().len(), 1);
    assert_eq!(observer.coalesced(), 4, "the leader made the call");
    assert_eq!(observer.requests().answered, 1, "followers ran no chain");
}

#[tokio::test]
async fn a_failover_is_one_request_of_two_attempts() {
    let live = mock_rpc_server::ok("0x1").await;
    let (_, observer) = observer(&["dead", "live"]);
    let addr = spawn_app_with_observer(
        settings(vec![rpc("dead", DEAD_URL), rpc("live", live.uri())]),
        observer.clone(),
    )
    .await;

    assert_eq!(post(&addr).await.status(), 200);

    assert_eq!(
        observer.requests(),
        RequestSnapshot {
            answered: 1,
            failed: 0,
            attempt_buckets: [0, 1, 0, 0, 0],
            attempts_total: 2,
        }
    );
}

#[tokio::test]
async fn an_exhausted_chain_is_a_failed_request() {
    let (_, observer) = observer(&["dead"]);
    let addr =
        spawn_app_with_observer(settings(vec![rpc("dead", DEAD_URL)]), observer.clone()).await;

    post(&addr).await;

    let requests = observer.requests();
    assert_eq!((requests.answered, requests.failed), (0, 1));
}

#[tokio::test]
async fn nothing_is_coalesced_with_dedup_off() {
    let slow = mock_rpc_server::slow("0x10", std::time::Duration::from_millis(300)).await;
    let (_, observer) = observer(&["slow"]);
    let addr =
        spawn_app_with_observer(settings(vec![rpc("slow", slow.uri())]), observer.clone()).await;

    join_all((0..3).map(|_| post(&addr))).await;

    assert_eq!(observer.coalesced(), 0);
}
