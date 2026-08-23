pub mod prometheus;
pub mod snapshot;

use std::{
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
};

use tracing::warn;

use crate::{
    observer::snapshot::{BUCKET_BOUNDS_MICROS, Snapshot, UpstreamStats},
    upstream::{
        UpstreamId,
        call::{CallError, CallRecord},
    },
};

pub trait Observer: Send + Sync + 'static {
    fn record(&self, upstream: &UpstreamId, record: CallRecord<'_>);
}

pub struct MetricsObserver {
    stats: HashMap<UpstreamId, Arc<UpstreamStats>>,
}

impl MetricsObserver {
    pub fn new(upstreams: impl IntoIterator<Item = UpstreamId>) -> Self {
        let mut stats = HashMap::new();
        for upstream in upstreams.into_iter() {
            stats.insert(upstream.clone(), Arc::new(UpstreamStats::default()));
        }
        MetricsObserver { stats }
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

    pub fn snapshot_map(&self) -> HashMap<UpstreamId, Snapshot> {
        let mut out = HashMap::new();
        for (upstream_id, stat) in self.stats.iter() {
            out.insert(upstream_id.clone(), stat.snapshot());
        }
        out
    }
}

impl Observer for MetricsObserver {
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
        };

        counter.fetch_add(1, Ordering::Relaxed);

        let duration_micros = record.duration.as_micros() as u64;

        stat.duration_micros_total
            .fetch_add(duration_micros, Ordering::Relaxed);

        // The slot of the first bound this duration is `le`. Anything past the
        // last bound gets no slot — `+Inf` covers it, derived at render.
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

    /// Built out of order on purpose: `HashMap` iteration is randomised per
    /// process, so an unsorted scrape would pass some runs and reorder others.
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
            duration: Duration::from_micros(100_000), // 100ms
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

        // 91_000..=100_000 all sit in the one (50_000, 100_000] slot.
        assert_eq!(stats.buckets[4].load(Ordering::Relaxed), 100);
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
