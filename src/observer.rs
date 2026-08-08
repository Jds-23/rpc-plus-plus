use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
};

use crate::upstream::{CallError, CallRecord, UpstreamId};

pub trait Observer: Send + Sync + 'static {
    fn record(&self, upstream: &UpstreamId, record: CallRecord<'_>);
}

#[derive(Default)]
pub struct NoopObserver {}

impl NoopObserver {
    pub fn new() -> Self {
        NoopObserver::default()
    }
}

impl Observer for NoopObserver {
    fn record(&self, upstream: &UpstreamId, record: CallRecord<'_>) {
        let _upstream = upstream;
        let _record = record;
    }
}

pub const BUCKET_BOUNDS_MICROS: [u64; 12] = [
    5_000,     // 0.005  local node floor
    10_000,    // 0.01
    25_000,    // 0.025
    50_000,    // 0.05
    100_000,   // 0.1
    250_000,   // 0.25
    500_000,   // 0.5
    1_000_000, // 1.0
    1_500_000, // 1.5
    2_000_000, // 2.0
    3_000_000, // 3.0   = DEFAULT_RPC_TIMEOUT_IN_SECS
    4_000_000, // 4.0   timeout fired late
];

#[derive(Default)]
pub struct UpstreamStats {
    pub success: AtomicU64,
    pub unreachable: AtomicU64,
    pub read_failed: AtomicU64,
    pub error_status: AtomicU64,

    pub buckets: [AtomicU64; BUCKET_BOUNDS_MICROS.len()],
    pub duration_micros_total: AtomicU64,
}

impl UpstreamStats {
    pub fn snapshot(&self) -> StatsSnapshot {
        StatsSnapshot {
            success: self.success.load(Ordering::Relaxed),
            unreachable: self.unreachable.load(Ordering::Relaxed),
            read_failed: self.read_failed.load(Ordering::Relaxed),
            error_status: self.error_status.load(Ordering::Relaxed),
            buckets: self
                .buckets
                .each_ref()
                .map(|bucket| bucket.load(Ordering::Relaxed)),
            duration_micros_total: self.duration_micros_total.load(Ordering::Relaxed),
        }
    }
}

pub struct StatsSnapshot {
    pub success: u64,
    pub unreachable: u64,
    pub read_failed: u64,
    pub error_status: u64,

    pub buckets: [u64; BUCKET_BOUNDS_MICROS.len()],
    pub duration_micros_total: u64,
}

pub struct MetricsObserver {
    stats: HashMap<UpstreamId, UpstreamStats>,
}

impl MetricsObserver {
    pub fn new(upstreams: impl IntoIterator<Item = UpstreamId>) -> Self {
        let mut stats = HashMap::new();
        for upstream in upstreams.into_iter() {
            stats.insert(upstream.clone(), UpstreamStats::default());
        }
        MetricsObserver { stats }
    }

    /// `None` for an upstream this observer was not built with.
    pub fn snapshot(&self, upstream: &UpstreamId) -> Option<StatsSnapshot> {
        Some(self.stats.get(upstream)?.snapshot())
    }
}

impl Observer for MetricsObserver {
    fn record(&self, upstream: &UpstreamId, record: CallRecord<'_>) {
        let stat = self
            .stats
            .get(upstream)
            .expect("Trying to set record for an unidentified upstream");
        let counter = match &record.outcome {
            Ok(_) => &stat.success,
            Err(CallError::ErrorStatus { .. }) => &stat.error_status,
            Err(CallError::ReadFailed { .. }) => &stat.read_failed,
            Err(CallError::Unreachable { .. }) => &stat.unreachable,
        };

        counter.fetch_add(1, Ordering::Relaxed);

        let duration_micros = record.duration.as_micros() as u64;

        stat.duration_micros_total
            .fetch_add(duration_micros, Ordering::Relaxed);

        // Update buckets: increment all buckets >= this duration
        let bucket_idx = BUCKET_BOUNDS_MICROS.partition_point(|&b| b < duration_micros);
        if bucket_idx < stat.buckets.len() {
            stat.buckets[bucket_idx].fetch_add(1, Ordering::Relaxed);
        }
    }
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

    // update across tasks
    #[tokio::test]
    async fn test_single_record_updates_bucket() {
        let observer = create_observer();
        let upstream_id = UpstreamId::new("upstream-1");

        let record = CallRecord {
            outcome: Ok(StatusCode::OK),
            duration: Duration::from_micros(100_000), // 0.1ms
        };

        observer.record(&upstream_id, record);

        let stats = observer.stats.get(&upstream_id).unwrap();
        assert_eq!(stats.success.load(Ordering::Relaxed), 1);
        assert_eq!(stats.buckets[4].load(Ordering::Relaxed), 1); // 100_000 bucket
    }

    /// Slots hold the count that fell into that band alone, not the running
    /// `le` total — accumulating into cumulative form is the exposition's job.
    #[tokio::test]
    async fn test_record_lands_in_exactly_one_bucket() {
        let observer = create_observer();
        let upstream_id = UpstreamId::new("upstream-1");

        let record = CallRecord {
            outcome: Ok(StatusCode::OK),
            duration: Duration::from_micros(50_000), // the bound at index 3
        };

        observer.record(&upstream_id, record);

        let stats = observer.stats.get(&upstream_id).unwrap();
        let counts = stats
            .buckets
            .each_ref()
            .map(|bucket| bucket.load(Ordering::Relaxed));

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

        let stats = observer.stats.get(&upstream_id).unwrap();
        assert_eq!(stats.success.load(Ordering::Relaxed), 100);

        // All 100 records fit in bucket >= 100_000
        assert!(stats.buckets[4].load(Ordering::Relaxed) >= 100);
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

        // Each upstream got 25 records
        for upstream_id in upstreams {
            let stats = observer.stats.get(&upstream_id).unwrap();
            assert_eq!(stats.success.load(Ordering::Relaxed), 25);
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

        let stats = observer.stats.get(&upstream_id).unwrap();
        assert_eq!(stats.success.load(Ordering::Relaxed), 1);
        assert_eq!(stats.error_status.load(Ordering::Relaxed), 1);
        assert_eq!(stats.read_failed.load(Ordering::Relaxed), 1);
        assert_eq!(stats.unreachable.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_duration_total_accumulates() {
        let observer = create_observer();
        let upstream_id = UpstreamId::new("upstream-1");

        let durations = vec![100_000, 50_000, 200_000, 75_000]; // micros

        for duration_micros in &durations {
            let record = CallRecord {
                outcome: Ok(StatusCode::OK),
                duration: Duration::from_micros(*duration_micros),
            };
            observer.record(&upstream_id, record);
        }

        let stats = observer.stats.get(&upstream_id).unwrap();
        let total: u64 = durations.iter().sum();
        assert_eq!(stats.duration_micros_total.load(Ordering::Relaxed), total);
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

        let stats = observer.stats.get(&upstream_id).unwrap();
        assert_eq!(
            stats.duration_micros_total.load(Ordering::Relaxed),
            expected_total
        );
    }
}
