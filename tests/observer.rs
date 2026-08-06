use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use reqwest::StatusCode;
use rpc_plus_plus::{
    observer::Observer,
    proxy::{ProxyState, ProxyStateBuilder},
    settings::RpcSettings,
    upstream::{CallRecord, UpstreamId},
};
use serde_json::json;

use crate::common::{mock_rpc_server, round_robin, spawn_app};

mod common;

/// `CallRecord` flattened to something owned. The `Err` status stands in for the
/// failure's class — only `Unreachable` records `None`.
#[derive(Debug)]
struct Recorded {
    upstream: UpstreamId,
    duration: Duration,
    outcome: Result<StatusCode, (Option<StatusCode>, String)>,
}

#[derive(Default)]
struct RecordingObserver {
    records: Mutex<Vec<Recorded>>,
}

impl RecordingObserver {
    fn take(&self) -> Vec<Recorded> {
        std::mem::take(&mut self.records.lock().unwrap())
    }
}

impl Observer for RecordingObserver {
    fn record(&self, upstream: &UpstreamId, record: CallRecord<'_>) {
        let outcome = match record.outcome {
            Ok(http_status) => Ok(http_status),
            Err(failure) => Err((failure.http_status(), failure.to_string())),
        };
        self.records.lock().unwrap().push(Recorded {
            upstream: upstream.clone(),
            duration: record.duration,
            outcome,
        });
    }
}

/// Nothing listens here, so the call fails before a response exists.
const DEAD_URL: &str = "http://127.0.0.1:1";

fn rpc(label: &str, rpc_url: impl Into<String>) -> RpcSettings {
    RpcSettings {
        label: label.to_string(),
        rpc_url: rpc_url.into(),
    }
}

/// `retry_after` is zeroed so the loop does not sleep between attempts.
fn state(rpcs: Vec<RpcSettings>, observer: Arc<RecordingObserver>) -> Arc<ProxyState> {
    let max_attempt = rpcs.len() as u64;
    let state = ProxyStateBuilder::default()
        .with_decider(round_robin(rpcs))
        .with_observer(observer)
        .with_max_attempt(max_attempt)
        .with_retry_after(Duration::ZERO)
        .build()
        .expect("state build failed");
    Arc::new(state)
}

async fn post(addr: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{addr}/rpc"))
        .json(&json!({"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}))
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn every_attempt_is_recorded_in_chain_order() {
    let live = mock_rpc_server::ok("0x1").await;
    let observer = Arc::new(RecordingObserver::default());
    let addr = spawn_app(state(
        vec![rpc("dead", DEAD_URL), rpc("live", live.uri())],
        observer.clone(),
    ))
    .await;

    let res = post(&addr).await;
    assert_eq!(res.status(), 200);

    let records = observer.take();
    assert_eq!(records.len(), 2);

    assert_eq!(records[0].upstream.as_str(), "dead");
    let (http_status, _) = records[0]
        .outcome
        .as_ref()
        .expect_err("the dead upstream should have failed");
    assert_eq!(*http_status, None, "no response arrived, so no status");

    assert_eq!(records[1].upstream.as_str(), "live");
    assert_eq!(records[1].outcome.as_ref().copied(), Ok(StatusCode::OK));
}

#[tokio::test]
async fn an_error_status_is_recorded_with_its_code() {
    let failing = mock_rpc_server::failing(StatusCode::INTERNAL_SERVER_ERROR).await;
    let observer = Arc::new(RecordingObserver::default());
    let addr = spawn_app(state(vec![rpc("one", failing.uri())], observer.clone())).await;

    let body: serde_json::Value = post(&addr).await.json().await.unwrap();
    assert_eq!(
        body["error"]["message"],
        "upstream returned error status 500"
    );

    let records = observer.take();
    assert_eq!(records.len(), 1);
    let (http_status, message) = records[0]
        .outcome
        .as_ref()
        .expect_err("a 500 should have failed the attempt");
    assert_eq!(*http_status, Some(StatusCode::INTERNAL_SERVER_ERROR));
    assert_eq!(message, "upstream returned error status 500");
}

#[tokio::test]
async fn the_recorded_duration_is_per_call() {
    let live = mock_rpc_server::ok("0x1").await;
    let observer = Arc::new(RecordingObserver::default());
    let addr = spawn_app(state(
        vec![rpc("dead", DEAD_URL), rpc("live", live.uri())],
        observer.clone(),
    ))
    .await;

    let started = Instant::now();
    post(&addr).await;
    let whole_request = started.elapsed();

    let records = observer.take();
    let recorded: Duration = records.iter().map(|record| record.duration).sum();
    assert!(
        recorded <= whole_request,
        "recorded {recorded:?} exceeds the request it happened inside ({whole_request:?})"
    );
}
