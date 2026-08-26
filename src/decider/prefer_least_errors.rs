use std::{
    collections::{HashMap, VecDeque},
    fmt::{Debug, Display},
    sync::Arc,
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use tokio::{task::JoinSet, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use crate::{
    decider::Decider,
    observer::{MetricsObserver, StatsSnapshot},
    upstream::{Upstream, UpstreamId},
};

// Known gaps, carried in docs/design/v0.2-DECIDER.md under Known Bugs:
//
// - `rank` on `attempt_started` (`src/proxy.rs`) — the 0-indexed chain position, so
//   first-choice hit rate is one filter rather than a join.
// - `refresh`, `window_ticks`, `min_samples` and `margin` belong in settings
//   (`src/settings.rs`) rather than in parameters and constants.
// - JSON-RPC errors riding on HTTP 200 count as successes (`src/observer.rs`), so a
//   `-32005` rate limit reads here as perfect health.

pub const REFRESH_DEFAULT: Duration = Duration::from_secs(15);

pub const WINDOW_DEFAULT: usize = 4;

/// A window carrying fewer calls than this is noise, not signal.
const MIN_WINDOW_SAMPLES: u64 = 20;

/// A challenger has to be 20% better than the incumbent head to take its place.
const PROMOTION_MARGIN: f64 = 0.8;

/// The order `decide` hands out, best first.
pub struct Ranking {
    upstreams: Vec<Arc<Upstream>>,
}

pub struct PreferLeastErrors {
    ranking: ArcSwap<Ranking>,
}

struct RankingRefresher {
    metrics_observer: Arc<MetricsObserver>,
    baseline: VecDeque<HashMap<UpstreamId, StatsSnapshot>>,
    decider: Arc<PreferLeastErrors>,
    interval: Duration,
    window_ticks: usize,
    min_samples: u64,
    margin: f64,
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
        let current: HashMap<UpstreamId, StatsSnapshot> = self.metrics_observer.snapshot_map();
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
                    return (upstream.clone(), SortKey::Unscored);
                };

                let key = if window.total() < self.min_samples {
                    // A starved upstream rates a perfect 0.0, which would unseat a
                    // proven one on no evidence.
                    SortKey::Unscored
                } else if incumbent == Some(id) {
                    SortKey::Scored(window.error_rate() * self.margin)
                } else {
                    SortKey::Scored(window.error_rate())
                };

                (upstream.clone(), key)
            })
            .collect();

        scored.sort_by(|(_, a), (_, b)| a.cmp(b));

        let order: Vec<&str> = scored
            .iter()
            .map(|(upstream, _)| upstream.id().as_str())
            .collect();
        let scores: Vec<&SortKey> = scored.iter().map(|(_, key)| key).collect();
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

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("upstream must not be empty")]
    EmptyUpstreams,
    #[error("upstream {0} is not tracked by the observer")]
    UnknownUpstream(UpstreamId),
    #[error("the minimum window is 1, given 0")]
    ZeroWindow,
}

impl PreferLeastErrors {
    pub fn spawn(
        upstreams: impl IntoIterator<Item = Upstream>,
        metrics_observer: Arc<MetricsObserver>,
        tasks: &mut JoinSet<()>,
        shutdown: CancellationToken,
        refresh: Duration,
        window: usize,
    ) -> Result<Arc<Self>, BuildError> {
        let upstreams: Vec<Arc<Upstream>> = upstreams.into_iter().map(Arc::new).collect();
        if upstreams.is_empty() {
            return Err(BuildError::EmptyUpstreams);
        }
        if window.eq(&0) {
            return Err(BuildError::ZeroWindow);
        }
        for upstream in &upstreams {
            if metrics_observer.snapshot(upstream.id()).is_none() {
                return Err(BuildError::UnknownUpstream(upstream.id().clone()));
            }
        }
        let decider = Arc::new(PreferLeastErrors {
            ranking: ArcSwap::from_pointee(Ranking { upstreams }),
        });
        let ranking_refresher = RankingRefresher {
            baseline: VecDeque::from([metrics_observer.snapshot_map()]),
            metrics_observer: metrics_observer.clone(),
            decider: decider.clone(),
            interval: refresh,
            window_ticks: window,
            min_samples: MIN_WINDOW_SAMPLES,
            margin: PROMOTION_MARGIN,
        };
        tasks.spawn(ranking_refresher.run(shutdown));

        Ok(decider)
    }
}

#[derive(PartialEq)]
enum SortKey {
    Unscored,
    Scored(f64),
}

impl Display for SortKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unscored => write!(f, "unscored"),
            Self::Scored(error_rate) => write!(f, "{:.4}", error_rate),
        }
    }
}

impl Debug for SortKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl Eq for SortKey {}

impl PartialOrd for SortKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (Self::Unscored, Self::Unscored) => std::cmp::Ordering::Equal,
            (Self::Unscored, Self::Scored(_)) => std::cmp::Ordering::Greater, // Scored first
            (Self::Scored(_), Self::Unscored) => std::cmp::Ordering::Less,
            (Self::Scored(a), Self::Scored(b)) => a.total_cmp(b),
        }
    }
}

impl Decider for PreferLeastErrors {
    fn decide(&self, max: usize) -> Vec<Arc<Upstream>> {
        let ranking = self.ranking.load();
        ranking.upstreams.iter().take(max).cloned().collect()
    }

    fn upstream_len(&self) -> usize {
        self.ranking.load().upstreams.len()
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use reqwest::StatusCode;
    use tokio::task::JoinSet;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{
        observer::Observer,
        upstream::{CallError, CallRecord},
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
            .map(|label| {
                Arc::new(Upstream::new(
                    format!("http://{label}.invalid"),
                    UpstreamId::new(*label),
                ))
            })
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

    #[test]
    fn decide_caps_at_max_and_zero_yields_nothing() {
        let (_, decider, _) = harness(&["a", "b", "c"]);

        assert!(decider.decide(0).is_empty());
        assert_eq!(decider.decide(2).len(), 2);
        assert_eq!(
            decider.decide(9).len(),
            3,
            "max beyond the list should clamp"
        );
        assert_eq!(decider.upstream_len(), 3);
    }

    #[tokio::test]
    async fn creates_a_task() {
        let mut tasks: JoinSet<()> = JoinSet::new();
        assert_eq!(tasks.len(), 0);
        let upstreams: Vec<Upstream> =
            vec![Upstream::new("url".to_string(), UpstreamId::new("label"))];
        let metrics_observer = Arc::new(MetricsObserver::new(vec![UpstreamId::new("label")]));
        let shutdown = CancellationToken::new();
        let _decider = PreferLeastErrors::spawn(
            upstreams,
            metrics_observer,
            &mut tasks,
            shutdown.clone(),
            REFRESH_DEFAULT,
            1,
        );
        assert_eq!(tasks.len(), 1);
        shutdown.cancel();
        tasks.join_all().await;
    }

    #[tokio::test]
    async fn spawn_rejects_an_empty_upstream_list() {
        let mut tasks: JoinSet<()> = JoinSet::new();
        let observer = Arc::new(MetricsObserver::new(vec![]));

        let built = PreferLeastErrors::spawn(
            Vec::new(),
            observer,
            &mut tasks,
            CancellationToken::new(),
            REFRESH_DEFAULT,
            1,
        );

        assert!(matches!(built, Err(BuildError::EmptyUpstreams)));
        assert_eq!(tasks.len(), 0, "a rejected build should spawn nothing");
    }

    #[tokio::test]
    async fn spawn_rejects_a_zero_window() {
        let mut tasks: JoinSet<()> = JoinSet::new();
        let observer = Arc::new(MetricsObserver::new(vec![UpstreamId::new("one")]));
        let upstreams = vec![Upstream::new("url".to_string(), UpstreamId::new("one"))];

        let built = PreferLeastErrors::spawn(
            upstreams,
            observer,
            &mut tasks,
            CancellationToken::new(),
            REFRESH_DEFAULT,
            0,
        );

        assert!(matches!(built, Err(BuildError::ZeroWindow)));
        assert_eq!(tasks.len(), 0, "a rejected build should spawn nothing");
    }

    /// Without this the upstream would score off a map that does not hold it.
    #[tokio::test]
    async fn spawn_rejects_an_upstream_the_observer_does_not_track() {
        let mut tasks: JoinSet<()> = JoinSet::new();
        let observer = Arc::new(MetricsObserver::new(vec![UpstreamId::new("known")]));
        let upstreams = vec![
            Upstream::new("url".to_string(), UpstreamId::new("known")),
            Upstream::new("url".to_string(), UpstreamId::new("stranger")),
        ];

        let built = PreferLeastErrors::spawn(
            upstreams,
            observer,
            &mut tasks,
            CancellationToken::new(),
            REFRESH_DEFAULT,
            1,
        );

        match built {
            Err(BuildError::UnknownUpstream(id)) => {
                assert_eq!(id.as_str(), "stranger")
            }
            Err(other) => panic!("expected UnknownUpstream, got {other:?}"),
            Ok(_) => panic!("an upstream the observer does not track should be refused"),
        }
        assert_eq!(tasks.len(), 0, "a rejected build should spawn nothing");
    }
}
