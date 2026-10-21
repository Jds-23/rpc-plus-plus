pub mod refresher;

use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::{
    decider::{BuildError, Decider, Ranking, least_latency::refresher::LatencyRefresher},
    observer::MetricsObserver,
    upstream::Upstream,
};

pub const REFRESH_DEFAULT: Duration = Duration::from_secs(15);

pub const WINDOW_DEFAULT: usize = 4;

const MIN_WINDOW_SAMPLES: u64 = 20;

const PROMOTION_MARGIN: f64 = 0.8;

const PROBE_EVERY: u64 = 20;

pub struct LeastLatency {
    ranking: ArcSwap<Ranking>,
    requests: AtomicU64,
}

#[bon::bon]
impl LeastLatency {
    #[builder(finish_fn = spawn)]
    pub fn new(
        upstreams: impl IntoIterator<Item = Upstream>,
        observer: Arc<MetricsObserver>,
        tasks: &mut JoinSet<()>,
        shutdown: CancellationToken,
        penalty: Duration,
        #[builder(default = REFRESH_DEFAULT)] refresh: Duration,
        #[builder(default = WINDOW_DEFAULT)] window: usize,
        #[builder(default = MIN_WINDOW_SAMPLES)] min_samples: u64,
    ) -> Result<Arc<Self>, BuildError> {
        let upstreams: Vec<Arc<Upstream>> = upstreams.into_iter().map(Arc::new).collect();
        if upstreams.is_empty() {
            return Err(BuildError::EmptyUpstreams);
        }
        if window == 0 {
            return Err(BuildError::ZeroWindow);
        }
        for upstream in &upstreams {
            if observer.snapshot(upstream.id()).is_none() {
                return Err(BuildError::UnknownUpstream(upstream.id().clone()));
            }
        }
        let decider = Arc::new(LeastLatency {
            ranking: ArcSwap::from_pointee(Ranking { upstreams }),
            requests: AtomicU64::new(0),
        });
        let latency_refresher = LatencyRefresher {
            baseline: VecDeque::from([observer.snapshot_map()]),
            metrics_observer: observer,
            decider: decider.clone(),
            interval: refresh,
            window_ticks: window,
            min_samples,
            margin: PROMOTION_MARGIN,
            penalty,
        };
        tasks.spawn(latency_refresher.run(shutdown));

        Ok(decider)
    }
}

impl Decider for LeastLatency {
    fn decide(&self, max: usize) -> Vec<Arc<Upstream>> {
        let ranking = self.ranking.load();
        let request = self.requests.fetch_add(1, Ordering::Relaxed);
        let challengers = ranking.upstreams.len().saturating_sub(1);
        if challengers == 0 || !request.is_multiple_of(PROBE_EVERY) {
            return ranking.upstreams.iter().take(max).cloned().collect();
        }
        let probed = (request / PROBE_EVERY) as usize % challengers + 1;
        let upstreams = &ranking.upstreams;
        std::iter::once(&upstreams[probed])
            .chain(&upstreams[..probed])
            .chain(&upstreams[probed + 1..])
            .take(max)
            .cloned()
            .collect()
    }

    fn upstream_len(&self) -> usize {
        self.ranking.load().upstreams.len()
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use tokio::task::JoinSet;
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::{decider::upstream, upstream::UpstreamId};

    const PENALTY: Duration = Duration::from_secs(3);

    fn ranked(labels: &[&str]) -> LeastLatency {
        LeastLatency {
            ranking: ArcSwap::from_pointee(Ranking {
                upstreams: labels
                    .iter()
                    .map(|label| Arc::new(upstream(label)))
                    .collect(),
            }),
            requests: AtomicU64::new(0),
        }
    }

    fn labels(chain: &[Arc<Upstream>]) -> Vec<&str> {
        chain
            .iter()
            .map(|upstream| upstream.id().as_str())
            .collect()
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

    #[test]
    fn one_request_in_probe_every_goes_to_a_challenger_in_turn() {
        let decider = ranked(&["a", "b", "c"]);
        let mut firsts: HashMap<String, usize> = HashMap::new();

        for _ in 0..1000 {
            let first = decider.decide(usize::MAX)[0].id().to_string();
            *firsts.entry(first).or_default() += 1;
        }

        assert_eq!(firsts["a"], 950);
        assert_eq!(firsts["b"], 25, "the challengers were not probed evenly");
        assert_eq!(firsts["c"], 25, "the challengers were not probed evenly");
    }

    #[test]
    fn a_probe_moves_the_challenger_ahead_of_the_rest_in_order() {
        let decider = ranked(&["a", "b", "c", "d"]);

        let probe = decider.decide(usize::MAX);
        assert_eq!(labels(&probe), ["b", "a", "c", "d"]);

        for _ in 1..PROBE_EVERY {
            decider.decide(usize::MAX);
        }
        assert_eq!(labels(&decider.decide(usize::MAX)), ["c", "a", "b", "d"]);
    }

    #[test]
    fn a_probe_is_still_capped_at_max() {
        let decider = ranked(&["a", "b", "c"]);

        assert_eq!(labels(&decider.decide(2)), ["b", "a"]);
    }

    #[test]
    fn a_fleet_of_one_never_probes() {
        let decider = ranked(&["solo"]);

        for _ in 0..(PROBE_EVERY * 3) {
            assert_eq!(labels(&decider.decide(usize::MAX)), ["solo"]);
        }
    }

    #[tokio::test]
    async fn creates_a_task() {
        let mut tasks: JoinSet<()> = JoinSet::new();
        let observer = Arc::new(MetricsObserver::new(vec![UpstreamId::new("label")]));
        let shutdown = CancellationToken::new();

        let _decider = LeastLatency::builder()
            .upstreams(vec![upstream("label")])
            .observer(observer)
            .tasks(&mut tasks)
            .shutdown(shutdown.clone())
            .penalty(PENALTY)
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

        let built = LeastLatency::builder()
            .upstreams(Vec::new())
            .observer(observer)
            .tasks(&mut tasks)
            .shutdown(CancellationToken::new())
            .penalty(PENALTY)
            .window(1)
            .spawn();

        assert!(matches!(built, Err(BuildError::EmptyUpstreams)));
        assert_eq!(tasks.len(), 0, "a rejected build should spawn nothing");
    }

    #[tokio::test]
    async fn spawn_rejects_a_zero_window() {
        let mut tasks: JoinSet<()> = JoinSet::new();
        let observer = Arc::new(MetricsObserver::new(vec![UpstreamId::new("one")]));

        let built = LeastLatency::builder()
            .upstreams(vec![upstream("one")])
            .observer(observer)
            .tasks(&mut tasks)
            .shutdown(CancellationToken::new())
            .penalty(PENALTY)
            .window(0)
            .spawn();

        assert!(matches!(built, Err(BuildError::ZeroWindow)));
        assert_eq!(tasks.len(), 0, "a rejected build should spawn nothing");
    }

    #[tokio::test]
    async fn spawn_rejects_an_upstream_the_observer_does_not_track() {
        let mut tasks: JoinSet<()> = JoinSet::new();
        let observer = Arc::new(MetricsObserver::new(vec![UpstreamId::new("known")]));

        let built = LeastLatency::builder()
            .upstreams(vec![upstream("known"), upstream("stranger")])
            .observer(observer)
            .tasks(&mut tasks)
            .shutdown(CancellationToken::new())
            .penalty(PENALTY)
            .window(1)
            .spawn();

        match built {
            Err(BuildError::UnknownUpstream(id)) => assert_eq!(id.as_str(), "stranger"),
            Err(other) => panic!("expected UnknownUpstream, got {other:?}"),
            Ok(_) => panic!("an upstream the observer does not track should be refused"),
        }
        assert_eq!(tasks.len(), 0, "a rejected build should spawn nothing");
    }
}
