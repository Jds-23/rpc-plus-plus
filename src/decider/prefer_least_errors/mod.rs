pub mod refresher;

use std::{collections::VecDeque, sync::Arc, time::Duration};

use arc_swap::ArcSwap;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::{
    decider::{Decider, prefer_least_errors::refresher::RankingRefresher},
    observer::MetricsObserver,
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

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("upstream must not be empty")]
    EmptyUpstreams,
    #[error("upstream {0} is not tracked by the observer")]
    UnknownUpstream(UpstreamId),
    #[error("the minimum window is 1, given 0")]
    ZeroWindow,
}

#[bon::bon]
impl PreferLeastErrors {
    #[builder(finish_fn = spawn)]
    pub fn new(
        upstreams: impl IntoIterator<Item = Upstream>,
        observer: Arc<MetricsObserver>,
        tasks: &mut JoinSet<()>,
        shutdown: CancellationToken,
        #[builder(default = REFRESH_DEFAULT)] refresh: Duration,
        #[builder(default = WINDOW_DEFAULT)] window: usize,
    ) -> Result<Arc<Self>, BuildError> {
        let upstreams: Vec<Arc<Upstream>> = upstreams.into_iter().map(Arc::new).collect();
        if upstreams.is_empty() {
            return Err(BuildError::EmptyUpstreams);
        }
        if window.eq(&0) {
            return Err(BuildError::ZeroWindow);
        }
        for upstream in &upstreams {
            if observer.snapshot(upstream.id()).is_none() {
                return Err(BuildError::UnknownUpstream(upstream.id().clone()));
            }
        }
        let decider = Arc::new(PreferLeastErrors {
            ranking: ArcSwap::from_pointee(Ranking { upstreams }),
        });
        let ranking_refresher = RankingRefresher {
            baseline: VecDeque::from([observer.snapshot_map()]),
            metrics_observer: observer,
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
fn upstream(label: &str) -> Upstream {
    Upstream::builder()
        .label(label)
        .url(format!("http://{label}.invalid"))
        .build()
        .expect("upstream build failed")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::task::JoinSet;
    use tokio_util::sync::CancellationToken;

    use super::*;

    fn ranked(labels: &[&str]) -> PreferLeastErrors {
        PreferLeastErrors {
            ranking: ArcSwap::from_pointee(Ranking {
                upstreams: labels
                    .iter()
                    .map(|label| Arc::new(upstream(label)))
                    .collect(),
            }),
        }
    }

    #[test]
    fn decide_caps_at_max_and_zero_yields_nothing() {
        let decider = ranked(&["a", "b", "c"]);

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
        let upstreams: Vec<Upstream> = vec![upstream("label")];
        let metrics_observer = Arc::new(MetricsObserver::new(vec![UpstreamId::new("label")]));
        let shutdown = CancellationToken::new();
        let _decider = PreferLeastErrors::builder()
            .upstreams(upstreams)
            .observer(metrics_observer)
            .tasks(&mut tasks)
            .shutdown(shutdown.clone())
            .window(1)
            .spawn();
        assert_eq!(tasks.len(), 1);
        shutdown.cancel();
        tasks.join_all().await;
    }

    #[tokio::test]
    async fn spawn_rejects_an_empty_upstream_list() {
        let mut tasks: JoinSet<()> = JoinSet::new();
        let observer = Arc::new(MetricsObserver::new(vec![]));

        let built = PreferLeastErrors::builder()
            .upstreams(Vec::new())
            .observer(observer)
            .tasks(&mut tasks)
            .shutdown(CancellationToken::new())
            .window(1)
            .spawn();

        assert!(matches!(built, Err(BuildError::EmptyUpstreams)));
        assert_eq!(tasks.len(), 0, "a rejected build should spawn nothing");
    }

    #[tokio::test]
    async fn spawn_rejects_a_zero_window() {
        let mut tasks: JoinSet<()> = JoinSet::new();
        let observer = Arc::new(MetricsObserver::new(vec![UpstreamId::new("one")]));
        let upstreams = vec![upstream("one")];

        let built = PreferLeastErrors::builder()
            .upstreams(upstreams)
            .observer(observer)
            .tasks(&mut tasks)
            .shutdown(CancellationToken::new())
            .window(0)
            .spawn();

        assert!(matches!(built, Err(BuildError::ZeroWindow)));
        assert_eq!(tasks.len(), 0, "a rejected build should spawn nothing");
    }

    /// Without this the upstream would score off a map that does not hold it.
    #[tokio::test]
    async fn spawn_rejects_an_upstream_the_observer_does_not_track() {
        let mut tasks: JoinSet<()> = JoinSet::new();
        let observer = Arc::new(MetricsObserver::new(vec![UpstreamId::new("known")]));
        let upstreams = vec![upstream("known"), upstream("stranger")];

        let built = PreferLeastErrors::builder()
            .upstreams(upstreams)
            .observer(observer)
            .tasks(&mut tasks)
            .shutdown(CancellationToken::new())
            .window(1)
            .spawn();

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
