//! Ex8 — the whole decider, end to end · budget 45 min
//!
//! Buys: spec §4.1 as a test. "One starts degrading and responding slowly. The
//! proxy's latency tracking detects this and begins routing new requests to
//! the two faster upstreams." Everything before this drill was one piece; this
//! is the loop that `src/decider/prefer_least_errors/refresher.rs` already
//! runs for error rates — snapshot, diff against a baseline, score, sort,
//! publish with one `ArcSwap::store` — with ex3's score, ex5's probes and ex6's
//! margin plugged in.
//!
//! PREDICTION (fill before you write code):
//! - a/b/c at 20/30/40ms, 1s ticks, a two-tick window. `a` jumps to 1s at
//!   t = 3.5s. At which tick does `b` take the head — and why not the first tick
//!   after the jump? (Hint: when does a 1s call get *recorded*?)
//! - `a` recovers to 20ms at t = 7.5s. Does it get the head back? What has to
//!   happen first, and which constant decides it?
//! - `decide` runs on every request while `refresh` rebuilds the ranking. What
//!   does a request see mid-rebuild? Is there a lock?
//!
//! RESULT:
//! -
//!
//! Assert: see the tests. Run them red first and read the messages.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use latency_lab::{Fake, Snapshot, TIMEOUT, Verdict, ms};
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;

pub const MIN_SAMPLES: u64 = 5;
pub const PROMOTION_MARGIN: f64 = 0.8;
pub const PROBE_EVERY: u64 = 10;

/// GIVEN, ex3's answer: windowed mean, failures cost the deadline.
pub fn score(window: &Snapshot) -> Option<Duration> {
    let total = window.total();
    if total == 0 {
        return None;
    }
    let measured = window.duration_micros_total - window.failed_micros_total;
    let penalised = window.failed * TIMEOUT.as_micros() as u64;
    Some(Duration::from_micros((measured + penalised) / total))
}

/// GIVEN: the decider. `decide` is one atomic load — no lock, ever.
pub struct LeastLatency {
    ranking: ArcSwap<Vec<Arc<Fake>>>,
}

impl LeastLatency {
    pub fn new(fleet: &[Arc<Fake>]) -> Arc<Self> {
        Arc::new(LeastLatency {
            ranking: ArcSwap::from_pointee(fleet.to_vec()),
        })
    }

    pub fn decide(&self, max: usize) -> Vec<Arc<Fake>> {
        self.ranking.load().iter().take(max).cloned().collect()
    }

    pub fn order(&self) -> Vec<&'static str> {
        self.ranking.load().iter().map(|u| u.name).collect()
    }
}

pub struct Refresher {
    pub decider: Arc<LeastLatency>,
    pub fleet: Vec<Arc<Fake>>,
    /// Oldest first. Seeded with one snapshot; never longer than `window_ticks`.
    pub baseline: VecDeque<HashMap<&'static str, Snapshot>>,
    pub window_ticks: usize,
}

impl Refresher {
    pub fn new(decider: Arc<LeastLatency>, fleet: Vec<Arc<Fake>>, window_ticks: usize) -> Self {
        let mut refresher = Refresher {
            decider,
            fleet,
            baseline: VecDeque::new(),
            window_ticks,
        };
        refresher.baseline.push_back(refresher.snapshot());
        refresher
    }

    pub fn snapshot(&self) -> HashMap<&'static str, Snapshot> {
        self.fleet
            .iter()
            .map(|u| (u.name, u.stats.snapshot()))
            .collect()
    }

    /// Rebuild the ranking from the window. Shape of `RankingRefresher::refresh`.
    ///
    /// Contract:
    /// - each upstream's window is its current snapshot minus the *oldest*
    ///   baseline;
    /// - it scores `score(window)` with at least `MIN_SAMPLES` calls, else it
    ///   is unscored;
    /// - order as ex6: lower first, unscored last, the current head competes
    ///   at score × `PROMOTION_MARGIN`, ties keep the current order;
    /// - publish with a single `store` — `decide` never sees half a ranking;
    /// - push the current snapshot, then trim the baseline to `window_ticks`.
    #[allow(unused_variables)]
    pub fn refresh(&mut self) {
        // `RankingRefresher::refresh` in `src/` is this function with an error
        // rate where the latency goes. Read it, close it, write this one.
        todo!("fill the blank")
    }

    /// GIVEN: the ticker, as in `src/`. The first tick fires at once and is
    /// skipped, so the first refresh has a full interval of traffic.
    pub async fn run(mut self, interval: Duration) {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            self.refresh();
        }
    }
}

/// GIVEN: a client sending a request every 10ms. Ex5's routing: the head gets
/// the request, except one in `PROBE_EVERY` that goes to a challenger in turn.
/// Each request runs on its own task, so a slow upstream does not slow the
/// client.
pub fn client(decider: Arc<LeastLatency>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(ms(10));
        for request in 0u64.. {
            ticker.tick().await;
            let ranking = decider.decide(usize::MAX);
            let target = if request % PROBE_EVERY == 0 && ranking.len() > 1 {
                let challengers = &ranking[1..];
                challengers[(request / PROBE_EVERY) as usize % challengers.len()].clone()
            } else {
                ranking[0].clone()
            };
            tokio::spawn(async move {
                let _ = target.attempt().await;
            });
        }
    })
}

fn fleet(latencies_ms: &[(&'static str, u64)]) -> Vec<Arc<Fake>> {
    latencies_ms
        .iter()
        .map(|&(name, latency)| Arc::new(Fake::new(name, ms(latency), Verdict::Succeeds)))
        .collect()
}

/// Spec §4.1: returns the head at the end of each second, for 12 seconds.
/// `a` degrades to 1s at t = 3.5s and recovers at t = 7.5s.
async fn degrade_and_recover() -> Vec<&'static str> {
    let fleet = fleet(&[("a", 20), ("b", 30), ("c", 40)]);
    let decider = LeastLatency::new(&fleet);
    let refresher = Refresher::new(decider.clone(), fleet.clone(), 2);
    let refreshing = tokio::spawn(refresher.run(Duration::from_secs(1)));
    let traffic = client(decider.clone());

    // Half a second off the tick, so each reading sees the tick before it.
    tokio::time::sleep(ms(500)).await;
    let mut heads = Vec::new();
    for second in 0..12 {
        match second {
            3 => fleet[0].set_latency(ms(1000)),
            7 => fleet[0].set_latency(ms(20)),
            _ => {}
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        heads.push(decider.order()[0]);
    }
    traffic.abort();
    refreshing.abort();
    heads
}

#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() {
    for (second, head) in degrade_and_recover().await.into_iter().enumerate() {
        println!("t = {:>2}.5s  head = {head}", second + 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use latency_lab::Outcome;

    fn by_hand(window_ticks: usize) -> (Refresher, Arc<LeastLatency>, Vec<Arc<Fake>>) {
        let fleet = fleet(&[("a", 0), ("b", 0)]);
        let decider = LeastLatency::new(&fleet);
        (Refresher::new(decider.clone(), fleet.clone(), window_ticks), decider, fleet)
    }

    #[test]
    fn ranks_on_the_window() {
        let (mut refresher, decider, fleet) = by_hand(1);
        fleet[0].stats.record_n(100, ms(20), Outcome::Ok);
        fleet[1].stats.record_n(100, ms(30), Outcome::Ok);
        refresher.refresh();
        assert_eq!(decider.order(), ["a", "b"]);

        fleet[0].stats.record_n(100, ms(500), Outcome::Ok);
        fleet[1].stats.record_n(100, ms(30), Outcome::Ok);
        refresher.refresh();
        assert_eq!(
            decider.order(),
            ["b", "a"],
            "a one-tick window sees only the 500ms calls, not `a`'s fast past"
        );
    }

    #[test]
    fn a_quiet_tick_keeps_the_order() {
        let (mut refresher, decider, fleet) = by_hand(1);
        fleet[1].stats.record_n(100, ms(10), Outcome::Ok);
        refresher.refresh();
        assert_eq!(decider.order(), ["b", "a"]);

        refresher.refresh();
        assert_eq!(
            decider.order(),
            ["b", "a"],
            "no calls is no evidence — an empty tick must not reshuffle"
        );
    }

    #[test]
    fn the_baseline_never_outgrows_the_window() {
        let (mut refresher, _decider, fleet) = by_hand(3);
        for _ in 0..10 {
            fleet[0].stats.record_n(10, ms(10), Outcome::Ok);
            refresher.refresh();
            assert!(refresher.baseline.len() <= 3, "the deque grew past the window");
        }
        assert_eq!(refresher.baseline.len(), 3);
    }

    #[test]
    fn a_failing_upstream_is_slow_not_fast() {
        let (mut refresher, decider, fleet) = by_hand(1);
        fleet[0].stats.record_n(100, ms(1), Outcome::Failed);
        fleet[1].stats.record_n(100, ms(200), Outcome::Ok);
        refresher.refresh();

        assert_eq!(decider.order(), ["b", "a"], "ex3, wired in");
    }

    #[tokio::test(start_paused = true)]
    async fn spec_4_1_a_slow_upstream_loses_the_head_and_wins_it_back() {
        let heads = degrade_and_recover().await;

        assert_eq!(&heads[..3], ["a"; 3], "`a` is fastest and keeps the head");
        assert!(
            heads[3..6].contains(&"b"),
            "within three ticks of `a` going to 1s, `b` holds the head: {heads:?}"
        );
        assert_eq!(
            heads.last(),
            Some(&"a"),
            "`a` is back at 20ms, probed, and 20ms beats `b`'s 30ms × 0.8: {heads:?}"
        );
    }
}
