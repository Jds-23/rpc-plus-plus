use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::{
    decider::prefer_least_errors::{PreferLeastErrors, Ranking},
    observer::{MetricsObserver, snapshot::Snapshot},
    upstream::{Upstream, UpstreamId},
};

pub struct RankingRefresher {
    pub metrics_observer: Arc<MetricsObserver>,
    pub baseline: VecDeque<HashMap<UpstreamId, Snapshot>>,
    pub decider: Arc<PreferLeastErrors>,
    pub interval: Duration,
    pub window_ticks: usize,
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
        let baseline = self
            .baseline
            .front()
            .expect("seeded at construction and trimmed only after a push");
        let current_ranking = self.decider.ranking.load_full();

        // The front snapshot is one tick old per entry held, so the deque's depth
        // is the span this diff covers.
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
                // `spawn` refuses an upstream the observer does not track, and the
                let Some(window) = current
                    .get(id)
                    .zip(baseline.get(id))
                    .map(|(current, baseline)| current.diff(baseline))
                else {
                    tracing::warn!(event = "ranking_upstream_missing", upstream = %id);
                    return (upstream.clone(), SortKey::unscored());
                };

                let key = if window.total() < self.min_samples {
                    // A starved upstream rates a perfect 0.0, which would unseat a
                    // proven one on no evidence.
                    SortKey::unscored()
                } else if incumbent == Some(id) {
                    SortKey::scored(window.error_rate() * self.margin)
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
            Decider,
            prefer_least_errors::{
                MIN_WINDOW_SAMPLES, PROMOTION_MARGIN, REFRESH_DEFAULT, Ranking, upstream,
            },
        },
        observer::Observer,
        upstream::call::{CallError, CallRecord},
    };

    /// Builds the refresher by hand rather than through `spawn`, so the ranking
    /// can be driven with no runtime, no ticker and no clock.
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
            baseline: VecDeque::from([observer.snapshot_map()]),
            decider: decider.clone(),
            interval: REFRESH_DEFAULT,
            window_ticks,
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

    /// The property that separates this decider from one scoring lifetime totals:
    /// `a` carries the worse record overall but owns the latest window.
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

    /// The baseline advances every tick, so a quiet window is a clean slate rather
    /// than a re-run of the last one.
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

    /// The head takes nearly every request, so a challenger can sit at zero traffic
    /// and rate a perfect 0.0. Ranking on that would swap the two every tick.
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
        // 9% against the incumbent's 10% clears the raw comparison but not the 20%
        // margin, so the head holds.
        let (mut refresher, decider, observer) = harness(&["head", "rival"]);
        record(&observer, "head", 90, 10);
        record(&observer, "rival", 91, 9);
        refresher.refresh();
        assert_eq!(
            order(&decider),
            ["head", "rival"],
            "a 1pp gap swapped the head"
        );

        // 5% against 10% clears it.
        let (mut refresher, decider, observer) = harness(&["head", "rival"]);
        record(&observer, "head", 90, 10);
        record(&observer, "rival", 95, 5);
        refresher.refresh();
        assert_eq!(order(&decider), ["rival", "head"]);
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

    /// Regression: trimming on `==` never fired for a single-tick window, so the
    /// baseline froze at construction and the deque grew without bound.
    #[test]
    fn a_single_tick_window_keeps_one_baseline() {
        let (mut refresher, _decider, observer) = harness_windowed(&["a"], 1);

        for _ in 0..5 {
            record(&observer, "a", 40, 0);
            refresher.refresh();
            assert_eq!(refresher.baseline.len(), 1, "the baseline should not grow");
        }
    }

    /// A three-tick window ranks on three intervals of history, not one.
    #[test]
    fn the_baseline_spans_the_configured_window() {
        let (mut refresher, decider, observer) = harness_windowed(&["a", "b"], 3);

        // Tick 1: `a` is the worse of the two.
        record(&observer, "a", 0, 40);
        record(&observer, "b", 40, 0);
        refresher.refresh();
        assert_eq!(order(&decider), ["b", "a"]);

        // Two clean ticks for `a`. A one-tick window would already have promoted
        // it; across three ticks its earlier failures still count against it.
        for _ in 0..2 {
            record(&observer, "a", 40, 0);
            refresher.refresh();
        }

        assert_eq!(refresher.baseline.len(), 3);
        assert_eq!(order(&decider), ["b", "a"], "the window forgot too early");
    }
}
