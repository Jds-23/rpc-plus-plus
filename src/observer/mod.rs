pub mod prometheus;
pub mod snapshot;

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use tracing::warn;

use crate::{
    jsonrpc::{OTHER_METHOD, method_labels},
    observer::snapshot::{
        ATTEMPT_BOUNDS, BUCKET_BOUNDS_MICROS, HedgeSnapshot, HedgeStats, RequestSnapshot,
        RequestStats, Snapshot, UpstreamStats,
    },
    upstream::{
        UpstreamId,
        call::{CallError, CallRecord},
    },
};

pub trait Observer: Send + Sync + 'static {
    fn record(&self, upstream: &UpstreamId, record: CallRecord<'_>);
    fn record_abandoned(&self, upstream: &UpstreamId, elapsed: Duration);
    /// `overtaken` passed its `hedge_after`, so the timer started a call to `to`.
    fn record_hedge(&self, overtaken: &UpstreamId, to: &UpstreamId);
    /// The hedge from `overtaken` to `to` answered first.
    fn record_hedge_win(&self, overtaken: &UpstreamId, to: &UpstreamId);
    fn record_coalesced(&self);
    /// `from` failed retryably and the chain moved on to the next upstream.
    fn record_failover(&self, from: &UpstreamId);
    /// Once per request that ran its own chain, however it ended.
    fn record_request(
        &self,
        method: &'static str,
        answered: bool,
        attempts: usize,
        elapsed: Duration,
    );
}

pub struct MetricsObserver {
    stats: HashMap<UpstreamId, Arc<UpstreamStats>>,
    hedges: HashMap<(UpstreamId, UpstreamId), Arc<HedgeStats>>,
    coalesced: AtomicU64,
    failovers: HashMap<UpstreamId, AtomicU64>,
    requests: HashMap<&'static str, RequestStats>,
}

impl MetricsObserver {
    pub fn new(upstreams: impl IntoIterator<Item = UpstreamId>) -> Self {
        let mut stats = HashMap::new();
        for upstream in upstreams.into_iter() {
            stats.insert(upstream.clone(), Arc::new(UpstreamStats::default()));
        }
        let mut hedges = HashMap::new();
        for overtaken in stats.keys() {
            for to in stats.keys().filter(|to| *to != overtaken) {
                hedges.insert(
                    (overtaken.clone(), to.clone()),
                    Arc::new(HedgeStats::default()),
                );
            }
        }
        let failovers = stats
            .keys()
            .map(|upstream| (upstream.clone(), AtomicU64::new(0)))
            .collect();
        MetricsObserver {
            stats,
            hedges,
            coalesced: AtomicU64::new(0),
            failovers,
            requests: method_labels()
                .map(|method| (method, RequestStats::default()))
                .collect(),
        }
    }

    /// `None` for an upstream this observer was not built with.
    pub fn snapshot(&self, upstream: &UpstreamId) -> Option<Snapshot> {
        Some(self.stats.get(upstream)?.snapshot())
    }

    pub fn snapshots(&self) -> Vec<(&UpstreamId, Snapshot)> {
        let mut out: Vec<_> = self
            .stats
            .iter()
            .map(|(upstream_id, stat)| (upstream_id, stat.snapshot()))
            .collect();
        out.sort_unstable_by(|(a, _), (b, _)| a.as_str().cmp(b.as_str()));
        out
    }

    /// `None` for a pair this observer was not built with.
    pub fn hedge_snapshot(&self, overtaken: &UpstreamId, to: &UpstreamId) -> Option<HedgeSnapshot> {
        let key = (overtaken.clone(), to.clone());
        Some(self.hedges.get(&key)?.snapshot())
    }

    /// Sorted by `(overtaken, to)` label, like `snapshots`.
    pub fn hedge_snapshots(&self) -> Vec<(&UpstreamId, &UpstreamId, HedgeSnapshot)> {
        let mut out: Vec<_> = self
            .hedges
            .iter()
            .map(|((overtaken, to), stat)| (overtaken, to, stat.snapshot()))
            .collect();
        out.sort_unstable_by(|(a, x, _), (b, y, _)| {
            (a.as_str(), x.as_str()).cmp(&(b.as_str(), y.as_str()))
        });
        out
    }

    fn hedge_stats(&self, overtaken: &UpstreamId, to: &UpstreamId) -> Option<&Arc<HedgeStats>> {
        let stat = self.hedges.get(&(overtaken.clone(), to.clone()));
        if stat.is_none() {
            warn!(event = "metrics_upstream_unknown", upstream = %overtaken, to = %to);
        }
        stat
    }

    pub fn coalesced(&self) -> u64 {
        self.coalesced.load(Ordering::Relaxed)
    }

    /// Sorted by upstream label, like `snapshots`.
    pub fn failovers(&self) -> Vec<(&UpstreamId, u64)> {
        let mut out: Vec<_> = self
            .failovers
            .iter()
            .map(|(upstream, count)| (upstream, count.load(Ordering::Relaxed)))
            .collect();
        out.sort_unstable_by(|(a, _), (b, _)| a.as_str().cmp(b.as_str()));
        out
    }

    pub fn requests(&self) -> Vec<(&'static str, RequestSnapshot)> {
        let mut out: Vec<_> = self
            .requests
            .iter()
            .map(|(method, stat)| (*method, stat.snapshot()))
            .collect();
        out.sort_unstable_by_key(|(method, _)| *method);
        out
    }

    pub fn snapshot_map(&self) -> HashMap<UpstreamId, Snapshot> {
        let mut out = HashMap::new();
        for (upstream_id, stat) in self.stats.iter() {
            out.insert(upstream_id.clone(), stat.snapshot());
        }
        out
    }
}

impl Observer for MetricsObserver {
    fn record_hedge(&self, overtaken: &UpstreamId, to: &UpstreamId) {
        if let Some(stat) = self.hedge_stats(overtaken, to) {
            stat.started.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_hedge_win(&self, overtaken: &UpstreamId, to: &UpstreamId) {
        if let Some(stat) = self.hedge_stats(overtaken, to) {
            stat.won.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn record_coalesced(&self) {
        self.coalesced.fetch_add(1, Ordering::Relaxed);
    }

    fn record_failover(&self, from: &UpstreamId) {
        let Some(count) = self.failovers.get(from) else {
            warn!(event = "metrics_upstream_unknown", upstream = %from);
            return;
        };
        count.fetch_add(1, Ordering::Relaxed);
    }

    fn record_request(
        &self,
        method: &'static str,
        answered: bool,
        attempts: usize,
        elapsed: Duration,
    ) {
        let Some(stat) = self
            .requests
            .get(method)
            .or_else(|| self.requests.get(OTHER_METHOD))
        else {
            return;
        };
        let outcome = if answered {
            &stat.answered
        } else {
            &stat.failed
        };
        outcome.fetch_add(1, Ordering::Relaxed);

        let attempts = attempts as u64;
        stat.attempts_total.fetch_add(attempts, Ordering::Relaxed);
        let bucket_idx = ATTEMPT_BOUNDS.partition_point(|&b| b < attempts);
        if let Some(bucket) = stat.attempt_buckets.get(bucket_idx) {
            bucket.fetch_add(1, Ordering::Relaxed);
        }

        record_duration(&stat.duration_buckets, &stat.duration_micros_total, elapsed);
    }

    fn record(&self, upstream: &UpstreamId, record: CallRecord<'_>) {
        let Some(stat) = self.stats.get(upstream) else {
            warn!(event = "metrics_upstream_unknown", upstream = %upstream);
            return;
        };
        let counter = match &record.outcome {
            Ok(_) => &stat.success,
            Err(CallError::ErrorStatus { .. }) => &stat.error_status,
            Err(CallError::ReadFailed { .. }) => &stat.read_failed,
            Err(CallError::Unreachable { .. }) => &stat.unreachable,
            Err(CallError::RpcError {
                retryable: false, ..
            }) => &stat.rpc_caller_fault,
            Err(CallError::RpcError { .. }) => &stat.rpc_error,
        };

        counter.fetch_add(1, Ordering::Relaxed);
        let duration_micros =
            record_duration(&stat.buckets, &stat.duration_micros_total, record.duration);

        if let Err(err) = &record.outcome
            && err.is_retryable()
        {
            stat.retryable_failures.fetch_add(1, Ordering::Relaxed);
            stat.retryable_failures_micros_total
                .fetch_add(duration_micros, Ordering::Relaxed);
        }
    }

    fn record_abandoned(&self, upstream: &UpstreamId, elapsed: Duration) {
        let Some(stat) = self.stats.get(upstream) else {
            warn!(event = "metrics_upstream_unknown", upstream = %upstream);
            return;
        };
        stat.abandoned.fetch_add(1, Ordering::Relaxed);
        record_duration(&stat.buckets, &stat.duration_micros_total, elapsed);
    }
}

fn record_duration(
    buckets: &[AtomicU64; BUCKET_BOUNDS_MICROS.len()],
    total_micros: &AtomicU64,
    duration: Duration,
) -> u64 {
    let duration_micros = duration.as_micros() as u64;

    total_micros.fetch_add(duration_micros, Ordering::Relaxed);

    let bucket_idx = BUCKET_BOUNDS_MICROS.partition_point(|&b| b < duration_micros);
    if let Some(bucket) = buckets.get(bucket_idx) {
        bucket.fetch_add(1, Ordering::Relaxed);
    }
    duration_micros
}

#[cfg(test)]
mod tests {
    use reqwest::StatusCode;

    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    fn create_observer() -> Arc<MetricsObserver> {
        let upstreams = vec![UpstreamId::new("upstream-1"), UpstreamId::new("upstream-2")];
        Arc::new(MetricsObserver::new(upstreams))
    }

    #[test]
    fn test_snapshots_are_sorted_by_label() {
        let observer = MetricsObserver::new(vec![
            UpstreamId::new("zulu"),
            UpstreamId::new("alpha"),
            UpstreamId::new("mike"),
        ]);

        let labels: Vec<&str> = observer
            .snapshots()
            .iter()
            .map(|(upstream_id, _)| upstream_id.as_str())
            .collect();

        assert_eq!(labels, ["alpha", "mike", "zulu"]);
    }

    #[tokio::test]
    async fn test_single_record_updates_bucket() {
        let observer = create_observer();
        let upstream_id = UpstreamId::new("upstream-1");

        let record = CallRecord {
            outcome: Ok(StatusCode::OK),
            duration: Duration::from_micros(100_000),
        };

        observer.record(&upstream_id, record);

        let snapshot = observer.snapshot(&upstream_id).unwrap();
        assert_eq!(snapshot.success, 1);
        assert_eq!(snapshot.buckets[4], 1);
    }

    #[tokio::test]
    async fn test_record_lands_in_exactly_one_bucket() {
        let observer = create_observer();
        let upstream_id = UpstreamId::new("upstream-1");

        let record = CallRecord {
            outcome: Ok(StatusCode::OK),
            duration: Duration::from_micros(50_000),
        };

        observer.record(&upstream_id, record);

        let counts = observer.snapshot(&upstream_id).unwrap().buckets;

        let mut expected = [0u64; BUCKET_BOUNDS_MICROS.len()];
        expected[3] = 1;
        assert_eq!(counts, expected);
    }

    #[tokio::test]
    async fn test_concurrent_records_same_upstream() {
        let observer = create_observer();
        let upstream_id = UpstreamId::new("upstream-1");

        let mut handles = vec![];

        for i in 0..100 {
            let obs = observer.clone();
            let id = upstream_id.clone();

            let handle = tokio::spawn(async move {
                let record = CallRecord {
                    outcome: Ok(StatusCode::OK),
                    duration: Duration::from_micros(100_000 - (i % 10) * 1_000),
                };
                obs.record(&id, record);
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let snapshot = observer.snapshot(&upstream_id).unwrap();
        assert_eq!(snapshot.success, 100);
        assert_eq!(snapshot.buckets[4], 100);
    }

    #[tokio::test]
    async fn test_concurrent_records_multiple_upstreams() {
        let observer = create_observer();
        let upstreams = vec![UpstreamId::new("upstream-1"), UpstreamId::new("upstream-2")];

        let mut handles = vec![];

        for (idx, upstream_id) in upstreams.iter().cycle().take(50).enumerate() {
            let obs = observer.clone();
            let id = upstream_id.clone();

            let handle = tokio::spawn(async move {
                let record = CallRecord {
                    outcome: Ok(StatusCode::OK),
                    duration: Duration::from_micros(100_000 + idx as u64 * 1_000),
                };
                obs.record(&id, record);
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        for upstream_id in upstreams {
            assert_eq!(observer.snapshot(&upstream_id).unwrap().success, 25);
        }
    }

    #[tokio::test]
    async fn test_error_counters() {
        let observer = create_observer();
        let upstream_id = UpstreamId::new("upstream-1");

        let read_failed_error = CallError::ReadFailed {
            error: "EOF".to_string(),
            http_status: StatusCode::OK,
        };
        let unreachable_error = CallError::Unreachable {
            error: "unreachable".to_string(),
        };
        let records = vec![
            CallRecord {
                outcome: Ok(StatusCode::OK),
                duration: Duration::from_micros(50_000),
            },
            CallRecord {
                outcome: Err(&CallError::ErrorStatus {
                    http_status: StatusCode::INTERNAL_SERVER_ERROR,
                }),
                duration: Duration::from_micros(100_000),
            },
            CallRecord {
                outcome: Err(&read_failed_error),
                duration: Duration::from_micros(150_000),
            },
            CallRecord {
                outcome: Err(&unreachable_error),
                duration: Duration::from_micros(200_000),
            },
        ];

        for record in records {
            observer.record(&upstream_id, record);
        }

        let snapshot = observer.snapshot(&upstream_id).unwrap();
        assert_eq!(snapshot.success, 1);
        assert_eq!(snapshot.error_status, 1);
        assert_eq!(snapshot.read_failed, 1);
        assert_eq!(snapshot.unreachable, 1);
    }

    #[test]
    fn a_client_error_is_not_a_retryable_failure() {
        let observer = create_observer();
        let upstream_id = UpstreamId::new("upstream-1");

        let records = [
            (Ok(StatusCode::OK), 10_000),
            (
                Err(&CallError::ErrorStatus {
                    http_status: StatusCode::INTERNAL_SERVER_ERROR,
                }),
                2_000,
            ),
            (
                Err(&CallError::ErrorStatus {
                    http_status: StatusCode::BAD_REQUEST,
                }),
                3_000,
            ),
        ];
        for (outcome, micros) in records {
            observer.record(
                &upstream_id,
                CallRecord {
                    outcome,
                    duration: Duration::from_micros(micros),
                },
            );
        }

        let snapshot = observer.snapshot(&upstream_id).unwrap();
        assert_eq!(
            snapshot.retryable_failures, 1,
            "the 500 only; a 400 is the caller's"
        );
        assert_eq!(snapshot.retryable_failures_micros_total, 2_000);
        assert_eq!(snapshot.duration_micros_total, 15_000);
    }

    #[test]
    fn an_abandoned_attempt_counts_its_time_but_no_outcome() {
        let observer = create_observer();
        let upstream_id = UpstreamId::new("upstream-1");

        observer.record_abandoned(&upstream_id, Duration::from_micros(150_000));

        let snapshot = observer.snapshot(&upstream_id).unwrap();
        assert_eq!(snapshot.abandoned, 1);
        assert_eq!(snapshot.success, 0);
        assert_eq!(snapshot.duration_micros_total, 150_000);
        assert_eq!(snapshot.buckets[5], 1, "the (100ms, 250ms] slot");
    }

    #[test]
    fn hedges_are_counted_per_ordered_pair() {
        let observer = create_observer();
        let one = UpstreamId::new("upstream-1");
        let two = UpstreamId::new("upstream-2");

        observer.record_hedge(&one, &two);
        observer.record_hedge(&one, &two);
        observer.record_hedge_win(&one, &two);

        assert_eq!(
            observer.hedge_snapshot(&one, &two),
            Some(HedgeSnapshot { started: 2, won: 1 })
        );
        assert_eq!(
            observer.hedge_snapshot(&two, &one),
            Some(HedgeSnapshot { started: 0, won: 0 }),
            "the reverse pair is its own series"
        );
        assert_eq!(observer.hedge_snapshot(&one, &one), None, "no self pair");
    }

    #[test]
    fn coalesced_requests_are_counted() {
        let observer = create_observer();

        observer.record_coalesced();
        observer.record_coalesced();

        assert_eq!(observer.coalesced(), 2);
    }

    #[test]
    fn failovers_are_counted_by_the_upstream_left_behind() {
        let observer = create_observer();
        let one = UpstreamId::new("upstream-1");

        observer.record_failover(&one);
        observer.record_failover(&one);
        observer.record_failover(&UpstreamId::new("unknown"));

        let counts: Vec<(&str, u64)> = observer
            .failovers()
            .into_iter()
            .map(|(upstream, count)| (upstream.as_str(), count))
            .collect();
        assert_eq!(counts, [("upstream-1", 2), ("upstream-2", 0)]);
    }

    #[test]
    fn requests_are_counted_by_outcome_and_attempts() {
        let observer = create_observer();

        observer.record_request("eth_call", true, 1, Duration::from_millis(40));
        observer.record_request("eth_call", true, 2, Duration::from_millis(80));
        observer.record_request("eth_call", false, 3, Duration::from_millis(300));
        observer.record_request("eth_call", false, 11, Duration::from_secs(6));

        assert_eq!(
            request_snapshot(&observer, "eth_call"),
            RequestSnapshot {
                answered: 2,
                failed: 2,
                attempt_buckets: [1, 1, 1, 0, 0],
                attempts_total: 17,
                duration_buckets: [0, 0, 0, 1, 1, 0, 1, 0, 0, 0],
                duration_micros_total: 6_420_000,
            },
            "11 attempts and 6s are both past the last bound: +Inf only"
        );
    }

    #[test]
    fn requests_are_counted_per_method() {
        let observer = create_observer();

        observer.record_request("eth_call", true, 1, Duration::from_millis(10));
        observer.record_request("eth_getLogs", false, 2, Duration::from_millis(10));

        assert_eq!(request_snapshot(&observer, "eth_call").answered, 1);
        assert_eq!(request_snapshot(&observer, "eth_getLogs").failed, 1);
        assert_eq!(
            request_snapshot(&observer, "eth_blockNumber"),
            RequestSnapshot::default()
        );
    }

    #[test]
    fn an_unknown_method_lands_on_other() {
        let observer = create_observer();

        observer.record_request("eth_madeUp", true, 1, Duration::from_millis(10));

        assert_eq!(request_snapshot(&observer, OTHER_METHOD).answered, 1);
        assert!(
            observer
                .requests()
                .iter()
                .all(|(method, _)| *method != "eth_madeUp"),
            "the label set stays the one built up front"
        );
    }

    fn request_snapshot(observer: &MetricsObserver, method: &str) -> RequestSnapshot {
        observer
            .requests()
            .into_iter()
            .find(|(label, _)| *label == method)
            .map(|(_, snapshot)| snapshot)
            .unwrap()
    }

    #[test]
    fn a_hedge_from_an_unknown_upstream_is_dropped() {
        let observer = create_observer();
        let ghost = UpstreamId::new("ghost");
        let one = UpstreamId::new("upstream-1");

        observer.record_hedge(&ghost, &one);

        assert!(
            observer
                .hedge_snapshots()
                .iter()
                .all(|(_, _, count)| count.started == 0)
        );
    }

    #[tokio::test]
    async fn test_duration_total_accumulates() {
        let observer = create_observer();
        let upstream_id = UpstreamId::new("upstream-1");

        let durations = vec![100_000, 50_000, 200_000, 75_000];

        for duration_micros in &durations {
            let record = CallRecord {
                outcome: Ok(StatusCode::OK),
                duration: Duration::from_micros(*duration_micros),
            };
            observer.record(&upstream_id, record);
        }

        let total: u64 = durations.iter().sum();
        assert_eq!(
            observer
                .snapshot(&upstream_id)
                .unwrap()
                .duration_micros_total,
            total
        );
    }

    #[tokio::test]
    async fn test_concurrent_duration_accumulation() {
        let observer = create_observer();
        let upstream_id = UpstreamId::new("upstream-1");

        let mut handles = vec![];
        let mut expected_total = 0u64;

        for i in 0..50 {
            let obs = observer.clone();
            let id = upstream_id.clone();
            let duration_micros = (i * 10_000) as u64;
            expected_total += duration_micros;

            let handle = tokio::spawn(async move {
                let record = CallRecord {
                    outcome: Ok(StatusCode::OK),
                    duration: Duration::from_micros(duration_micros),
                };
                obs.record(&id, record);
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        assert_eq!(
            observer
                .snapshot(&upstream_id)
                .unwrap()
                .duration_micros_total,
            expected_total
        );
    }
}
