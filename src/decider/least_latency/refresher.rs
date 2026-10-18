use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::{
    decider::{Ranking, least_latency::LeastLatency},
    observer::{MetricsObserver, snapshot::Snapshot},
    upstream::{Upstream, UpstreamId},
};

pub struct LatencyRefresher {
    pub metrics_observer: Arc<MetricsObserver>,
    pub baseline: VecDeque<HashMap<UpstreamId, Snapshot>>,
    pub decider: Arc<LeastLatency>,
    pub interval: Duration,
    pub window_ticks: usize,
    pub min_samples: u64,
    pub margin: f64,
    pub penalty: Duration,
}

impl LatencyRefresher {
    pub async fn run(mut self, shutdown: CancellationToken) {
        let mut ticker = tokio::time::interval(self.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        ticker.tick().await;
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return,
                _ = ticker.tick() => self.refresh(),
            }
        }
    }

    pub fn refresh(&mut self) {
        let started = Instant::now();
        let current: HashMap<UpstreamId, Snapshot> = self.metrics_observer.snapshot_map();
        let baseline = self
            .baseline
            .front()
            .expect("seeded at construction and trimmed only after a push");
        let current_ranking = self.decider.ranking.load_full();

        let window_secs = self.baseline.len() as u64 * self.interval.as_secs();
        let want_secs = self.window_ticks as u64 * self.interval.as_secs();
        if self.baseline.len() < self.window_ticks {
            tracing::warn!(
                event = "ranking_window_incomplete",
                have_secs = window_secs,
                want_secs,
            );
        }

        let incumbent = current_ranking
            .upstreams
            .first()
            .map(|upstream| upstream.id());

        let mut scored: Vec<(Arc<Upstream>, SortKey)> = current_ranking
            .upstreams
            .iter()
            .map(|upstream| {
                let id = upstream.id();
                let Some(window) = current
                    .get(id)
                    .zip(baseline.get(id))
                    .map(|(current, baseline)| current.diff(baseline))
                else {
                    tracing::warn!(event = "ranking_upstream_missing", upstream = %id);
                    return (upstream.clone(), SortKey::unscored());
                };

                let score = if window.calls() < self.min_samples {
                    None
                } else {
                    window.latency_score(self.penalty)
                };
                let key = match score {
                    None => SortKey::unscored(),
                    Some(score) if incumbent == Some(id) => {
                        SortKey::scored(score.mul_f64(self.margin))
                    }
                    Some(score) => SortKey::scored(score),
                };

                (upstream.clone(), key)
            })
            .collect();

        scored.sort_by(|(_, a), (_, b)| a.cmp(b));

        let order: Vec<&str> = scored
            .iter()
            .map(|(upstream, _)| upstream.id().as_str())
            .collect();
        let scores: Vec<String> = scored.iter().map(|(_, key)| key.render()).collect();
        tracing::info!(
            event = "ranking_rebuilt",
            decider = "LEAST_LATENCY",
            order = ?order,
            scores = ?scores,
            window_secs,
            duration_us = started.elapsed().as_micros() as u64,
        );

        self.decider.ranking.store(Arc::new(Ranking {
            upstreams: scored.into_iter().map(|(upstream, _)| upstream).collect(),
        }));
        self.baseline.push_back(current);
        if self.baseline.len() > self.window_ticks {
            self.baseline.pop_front();
        }
    }
}

#[derive(PartialEq)]
struct SortKey {
    unscored: bool,
    score: Duration,
}

impl SortKey {
    fn scored(score: Duration) -> Self {
        SortKey {
            unscored: false,
            score,
        }
    }

    fn unscored() -> Self {
        SortKey {
            unscored: true,
            score: Duration::ZERO,
        }
    }

    fn render(&self) -> String {
        if self.unscored {
            "unscored".to_string()
        } else {
            format!("{:.1}", self.score.as_secs_f64() * 1000.0)
        }
    }

    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.unscored
            .cmp(&other.unscored)
            .then_with(|| self.score.cmp(&other.score))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, atomic::AtomicU64},
        time::Duration,
    };

    use arc_swap::ArcSwap;
    use reqwest::StatusCode;

    use super::*;
    use crate::{
        decider::{
            Ranking,
            least_latency::{MIN_WINDOW_SAMPLES, PROMOTION_MARGIN, REFRESH_DEFAULT},
            upstream,
        },
        observer::Observer,
        upstream::call::{CallError, CallRecord},
    };

    const PENALTY: Duration = Duration::from_secs(3);

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn harness(labels: &[&str]) -> (LatencyRefresher, Arc<LeastLatency>, Arc<MetricsObserver>) {
        harness_windowed(labels, 1)
    }

    fn harness_windowed(
        labels: &[&str],
        window_ticks: usize,
    ) -> (LatencyRefresher, Arc<LeastLatency>, Arc<MetricsObserver>) {
        let observer = Arc::new(MetricsObserver::new(
            labels.iter().map(|label| UpstreamId::new(*label)),
        ));
        let upstreams = labels
            .iter()
            .map(|label| Arc::new(upstream(label)))
            .collect();
        let decider = Arc::new(LeastLatency {
            ranking: ArcSwap::from_pointee(Ranking { upstreams }),
            requests: AtomicU64::new(0),
        });
        let refresher = LatencyRefresher {
            metrics_observer: observer.clone(),
            baseline: VecDeque::from([observer.snapshot_map()]),
            decider: decider.clone(),
            interval: REFRESH_DEFAULT,
            window_ticks,
            min_samples: MIN_WINDOW_SAMPLES,
            margin: PROMOTION_MARGIN,
            penalty: PENALTY,
        };
        (refresher, decider, observer)
    }

    fn record_ok(observer: &MetricsObserver, label: &str, n: u64, each: Duration) {
        let id = UpstreamId::new(label);
        for _ in 0..n {
            observer.record(
                &id,
                CallRecord {
                    outcome: Ok(StatusCode::OK),
                    duration: each,
                },
            );
        }
    }

    fn record_err(
        observer: &MetricsObserver,
        label: &str,
        n: u64,
        each: Duration,
        failure: &CallError,
    ) {
        let id = UpstreamId::new(label);
        for _ in 0..n {
            observer.record(
                &id,
                CallRecord {
                    outcome: Err(failure),
                    duration: each,
                },
            );
        }
    }

    fn record_abandoned(observer: &MetricsObserver, label: &str, n: u64, each: Duration) {
        let id = UpstreamId::new(label);
        for _ in 0..n {
            observer.record_abandoned(&id, each);
        }
    }

    fn order(decider: &LeastLatency) -> Vec<String> {
        decider
            .ranking
            .load()
            .upstreams
            .iter()
            .map(|upstream| upstream.id().to_string())
            .collect()
    }

    #[test]
    fn ranks_the_faster_upstream_first() {
        let (mut refresher, decider, observer) = harness(&["a", "b"]);
        record_ok(&observer, "a", 40, ms(200));
        record_ok(&observer, "b", 40, ms(20));

        refresher.refresh();

        assert_eq!(order(&decider), ["b", "a"]);
    }

    #[test]
    fn ranks_on_the_window_not_lifetime() {
        let (mut refresher, decider, observer) = harness(&["a", "b"]);
        record_ok(&observer, "a", 1000, ms(10));
        record_ok(&observer, "b", 1000, ms(30));
        refresher.refresh();
        assert_eq!(order(&decider), ["a", "b"]);

        record_ok(&observer, "a", 40, ms(500));
        record_ok(&observer, "b", 40, ms(30));
        refresher.refresh();

        assert_eq!(
            order(&decider),
            ["b", "a"],
            "a's fast past hid its slow present"
        );
    }

    #[test]
    fn a_fast_retryable_failure_ranks_last() {
        let (mut refresher, decider, observer) = harness(&["broken", "healthy"]);
        let failure = CallError::ErrorStatus {
            http_status: StatusCode::INTERNAL_SERVER_ERROR,
        };
        record_err(&observer, "broken", 40, ms(2), &failure);
        record_ok(&observer, "healthy", 40, ms(200));

        refresher.refresh();

        assert_eq!(
            order(&decider),
            ["healthy", "broken"],
            "a 500 in 2ms did not answer in 2ms"
        );
    }

    #[test]
    fn non_retryable_errors_are_not_penalised() {
        let (mut refresher, decider, observer) = harness(&["head", "rival"]);
        let bad_request = CallError::ErrorStatus {
            http_status: StatusCode::BAD_REQUEST,
        };
        let reverted = CallError::RpcError {
            http_status: StatusCode::OK,
            code: 3,
            retryable: false,
        };
        record_ok(&observer, "head", 40, ms(100));
        record_err(&observer, "rival", 20, ms(20), &bad_request);
        record_err(&observer, "rival", 20, ms(20), &reverted);

        refresher.refresh();

        assert_eq!(
            order(&decider),
            ["rival", "head"],
            "the rival answered every call in 20ms, and every upstream would have said the same"
        );
    }

    #[test]
    fn a_challenger_must_beat_the_incumbent_by_the_margin() {
        let (mut refresher, decider, observer) = harness(&["head", "rival"]);
        record_ok(&observer, "head", 40, ms(50));
        record_ok(&observer, "rival", 40, ms(45));
        refresher.refresh();
        assert_eq!(
            order(&decider),
            ["head", "rival"],
            "5ms of jitter swapped the head"
        );

        let (mut refresher, decider, observer) = harness(&["head", "rival"]);
        record_ok(&observer, "head", 40, ms(50));
        record_ok(&observer, "rival", 40, ms(30));
        refresher.refresh();
        assert_eq!(order(&decider), ["rival", "head"]);
    }

    #[test]
    fn an_idle_challenger_does_not_take_the_head() {
        let (mut refresher, decider, observer) = harness(&["head", "idle"]);
        record_ok(&observer, "head", 40, ms(500));

        refresher.refresh();

        assert_eq!(
            order(&decider),
            ["head", "idle"],
            "an upstream with no traffic unseated a measured one"
        );
    }

    #[test]
    fn unscored_upstreams_keep_their_previous_order() {
        let (mut refresher, decider, observer) = harness(&["a", "b", "c"]);
        record_ok(&observer, "c", 40, ms(10));

        refresher.refresh();

        assert_eq!(order(&decider), ["c", "a", "b"]);
    }

    #[test]
    fn abandoned_attempts_cost_the_time_they_ran() {
        let (mut refresher, decider, observer) = harness(&["head", "rival"]);
        record_ok(&observer, "head", 10, ms(10));
        record_abandoned(&observer, "head", 30, ms(1000));
        record_ok(&observer, "rival", 40, ms(100));

        refresher.refresh();

        assert_eq!(
            order(&decider),
            ["rival", "head"],
            "the head's dropped calls were left out of its score"
        );
    }

    #[test]
    fn abandoned_attempts_count_toward_the_sample_minimum() {
        let (mut refresher, decider, observer) = harness(&["head", "rival"]);
        record_ok(&observer, "head", 40, ms(100));
        record_abandoned(&observer, "rival", 20, ms(50));

        refresher.refresh();

        assert_eq!(
            order(&decider),
            ["rival", "head"],
            "twenty abandoned calls were not enough to score the rival"
        );
    }

    #[test]
    fn a_single_tick_window_keeps_one_baseline() {
        let (mut refresher, _decider, observer) = harness_windowed(&["a"], 1);

        for _ in 0..5 {
            record_ok(&observer, "a", 40, ms(10));
            refresher.refresh();
            assert_eq!(refresher.baseline.len(), 1, "the baseline should not grow");
        }
    }

    #[test]
    fn the_baseline_spans_the_configured_window() {
        let (mut refresher, decider, observer) = harness_windowed(&["a", "b"], 3);

        record_ok(&observer, "a", 40, ms(1000));
        record_ok(&observer, "b", 40, ms(100));
        refresher.refresh();
        assert_eq!(order(&decider), ["b", "a"]);

        for _ in 0..2 {
            record_ok(&observer, "a", 40, ms(10));
            refresher.refresh();
        }

        assert_eq!(refresher.baseline.len(), 3);
        assert_eq!(order(&decider), ["b", "a"], "the window forgot too early");
    }
}
