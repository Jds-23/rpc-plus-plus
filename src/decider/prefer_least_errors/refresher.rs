use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::{
    decider::{Ranking, prefer_least_errors::PreferLeastErrors, window::Window},
    observer::{MetricsObserver, snapshot::Snapshot},
    upstream::{Upstream, UpstreamId},
};

pub struct RankingRefresher {
    pub metrics_observer: Arc<MetricsObserver>,
    pub window: Window,
    pub decider: Arc<PreferLeastErrors>,
    pub interval: Duration,
    pub min_samples: u64,
    pub margin: f64,
}

impl RankingRefresher {
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
        let baseline = self.window.oldest();
        let current_ranking = self.decider.ranking.load_full();

        let window_secs = self.window.span().as_secs();
        let want_secs = self.window.wanted_span().as_secs();
        if !self.window.is_full() {
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

                let key = if window.answered() < self.min_samples {
                    SortKey::unscored()
                } else if incumbent == Some(id) {
                    SortKey::scored(window.error_rate() * (1.0 - self.margin))
                } else {
                    SortKey::scored(window.error_rate())
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
            decider = "PREFER_LEAST_ERRORS",
            order = ?order,
            scores = ?scores,
            window_secs,
            duration_us = started.elapsed().as_micros() as u64,
        );

        self.decider.ranking.store(Arc::new(Ranking {
            upstreams: scored.into_iter().map(|(upstream, _)| upstream).collect(),
        }));
        self.window.advance(current);
    }
}

#[derive(PartialEq)]
struct SortKey {
    unscored: bool,
    error_rate: f64,
}

impl SortKey {
    fn scored(error_rate: f64) -> Self {
        SortKey {
            unscored: false,
            error_rate,
        }
    }

    fn unscored() -> Self {
        SortKey {
            unscored: true,
            error_rate: 0.0,
        }
    }

    fn render(&self) -> String {
        if self.unscored {
            "unscored".to_string()
        } else {
            format!("{:.4}", self.error_rate)
        }
    }

    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.unscored
            .cmp(&other.unscored)
            .then_with(|| self.error_rate.total_cmp(&other.error_rate))
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use arc_swap::ArcSwap;
    use reqwest::StatusCode;

    use super::*;
    use crate::{
        decider::{
            Decider, Ranking,
            prefer_least_errors::{MIN_WINDOW_SAMPLES, PROMOTION_MARGIN, REFRESH_DEFAULT},
            upstream,
        },
        observer::Observer,
        upstream::call::{CallError, CallRecord},
    };

    fn harness(
        labels: &[&str],
    ) -> (
        RankingRefresher,
        Arc<PreferLeastErrors>,
        Arc<MetricsObserver>,
    ) {
        harness_windowed(labels, 1)
    }

    fn harness_windowed(
        labels: &[&str],
        window_ticks: usize,
    ) -> (
        RankingRefresher,
        Arc<PreferLeastErrors>,
        Arc<MetricsObserver>,
    ) {
        let observer = Arc::new(MetricsObserver::new(
            labels.iter().map(|label| UpstreamId::new(*label)),
        ));
        let upstreams = labels
            .iter()
            .map(|label| Arc::new(upstream(label)))
            .collect();
        let decider = Arc::new(PreferLeastErrors {
            ranking: ArcSwap::from_pointee(Ranking { upstreams }),
        });
        let refresher = RankingRefresher {
            metrics_observer: observer.clone(),
            window: Window::new(observer.snapshot_map(), window_ticks, REFRESH_DEFAULT),
            decider: decider.clone(),
            interval: REFRESH_DEFAULT,
            min_samples: MIN_WINDOW_SAMPLES,
            margin: PROMOTION_MARGIN,
        };
        (refresher, decider, observer)
    }

    fn record(observer: &MetricsObserver, label: &str, ok: u64, err: u64) {
        let id = UpstreamId::new(label);
        for _ in 0..ok {
            observer.record(
                &id,
                CallRecord {
                    outcome: Ok(StatusCode::OK),
                    duration: Duration::from_millis(1),
                },
            );
        }
        let failure = CallError::ErrorStatus {
            http_status: StatusCode::INTERNAL_SERVER_ERROR,
        };
        for _ in 0..err {
            observer.record(
                &id,
                CallRecord {
                    outcome: Err(&failure),
                    duration: Duration::from_millis(1),
                },
            );
        }
    }

    fn record_abandoned(observer: &MetricsObserver, label: &str, n: u64) {
        let id = UpstreamId::new(label);
        for _ in 0..n {
            observer.record_abandoned(&id, Duration::from_millis(1));
        }
    }

    fn order(decider: &PreferLeastErrors) -> Vec<String> {
        decider
            .decide(usize::MAX)
            .iter()
            .map(|upstream| upstream.id().to_string())
            .collect()
    }

    #[test]
    fn ranks_the_healthier_upstream_first() {
        let (mut refresher, decider, observer) = harness(&["a", "b"]);
        record(&observer, "a", 0, 40);
        record(&observer, "b", 40, 0);

        refresher.refresh();

        assert_eq!(order(&decider), ["b", "a"]);
    }

    #[test]
    fn ranks_on_the_window_not_lifetime() {
        let (mut refresher, decider, observer) = harness(&["a", "b"]);

        record(&observer, "a", 0, 40);
        record(&observer, "b", 40, 0);
        refresher.refresh();
        assert_eq!(order(&decider), ["b", "a"]);

        record(&observer, "a", 40, 0);
        record(&observer, "b", 0, 40);
        refresher.refresh();

        assert_eq!(order(&decider), ["a", "b"], "a never recovered");
    }

    #[test]
    fn a_bad_window_is_not_punished_twice() {
        let (mut refresher, decider, observer) = harness(&["a", "b"]);
        record(&observer, "a", 0, 40);
        record(&observer, "b", 40, 0);
        refresher.refresh();

        record(&observer, "a", 40, 0);
        record(&observer, "b", 20, 20);
        refresher.refresh();

        assert_eq!(order(&decider), ["a", "b"]);
    }

    #[test]
    fn an_idle_challenger_does_not_take_the_head() {
        let (mut refresher, decider, observer) = harness(&["head", "idle"]);
        record(&observer, "head", 38, 2);

        refresher.refresh();

        assert_eq!(
            order(&decider),
            ["head", "idle"],
            "an upstream with no traffic unseated a measured one"
        );
    }

    #[test]
    fn a_challenger_must_beat_the_incumbent_by_the_margin() {
        let (mut refresher, decider, observer) = harness(&["head", "rival"]);
        record(&observer, "head", 90, 10);
        record(&observer, "rival", 91, 9);
        refresher.refresh();
        assert_eq!(
            order(&decider),
            ["head", "rival"],
            "a 1pp gap swapped the head"
        );

        let (mut refresher, decider, observer) = harness(&["head", "rival"]);
        record(&observer, "head", 90, 10);
        record(&observer, "rival", 95, 5);
        refresher.refresh();
        assert_eq!(order(&decider), ["rival", "head"]);
    }

    #[test]
    fn abandoned_attempts_are_not_errors() {
        let (mut refresher, decider, observer) = harness(&["head", "rival"]);
        record(&observer, "head", 40, 0);
        record_abandoned(&observer, "head", 40);
        record(&observer, "rival", 38, 2);

        refresher.refresh();

        assert_eq!(
            order(&decider),
            ["head", "rival"],
            "the head's hedge losers were scored as failures"
        );
    }

    #[test]
    fn equal_error_rates_keep_their_previous_order() {
        let (mut refresher, decider, observer) = harness(&["a", "b"]);
        record(&observer, "a", 20, 20);
        record(&observer, "b", 20, 20);

        refresher.refresh();
        assert_eq!(order(&decider), ["a", "b"]);

        record(&observer, "a", 20, 20);
        record(&observer, "b", 20, 20);
        refresher.refresh();
        assert_eq!(order(&decider), ["a", "b"], "a tie reordered");
    }

    #[test]
    fn a_single_tick_window_keeps_one_baseline() {
        let (mut refresher, _decider, observer) = harness_windowed(&["a"], 1);

        for _ in 0..5 {
            record(&observer, "a", 40, 0);
            refresher.refresh();
            assert_eq!(
                refresher.window.ticks_held(),
                1,
                "the baseline should not grow"
            );
        }
    }

    #[test]
    fn the_baseline_spans_the_configured_window() {
        let (mut refresher, decider, observer) = harness_windowed(&["a", "b"], 3);

        record(&observer, "a", 0, 40);
        record(&observer, "b", 40, 0);
        refresher.refresh();
        assert_eq!(order(&decider), ["b", "a"]);

        for _ in 0..2 {
            record(&observer, "a", 40, 0);
            refresher.refresh();
        }

        assert_eq!(refresher.window.ticks_held(), 3);
        assert_eq!(order(&decider), ["b", "a"], "the window forgot too early");
    }
}
