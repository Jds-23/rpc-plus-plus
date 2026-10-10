//! Ex5 — nobody measures the upstream nobody calls · budget 40 min
//!
//! Buys: spec §4.1 — "the proxy's latency tracking detects this and begins
//! routing new requests to the two faster upstreams". It cannot detect a
//! faster upstream it never sends anything to. `decide` hands the head nearly
//! every request (`walk` only reaches #2 on a failure), so a challenger's
//! window sits empty, scores `None`, ranks last — and stays there while the
//! head degrades to two seconds. `prefer_least_errors` dodges this because a
//! failing head *fails* and spills traffic onto #2. A slow head never does.
//!
//! PREDICTION (fill before you write code):
//! - Head 20ms, rival 30ms, every request to the head. The head degrades to
//!   2s. After five more ticks, who is head?
//! - One request in 20 goes to a challenger. What does that cost, and how many
//!   ticks until the rival takes over?
//! - Three upstreams, one probe in 20. How do the two challengers share the
//!   probes — and what happens to the rotation if you pick by `rand`?
//!
//! RESULT:
//! -
//!
//! Assert: see the tests. Run them red first and read the messages.

use latency_lab::{Fake, Outcome, Snapshot, Verdict, ms};

/// One request in this many is a probe to a challenger.
pub const PROBE_EVERY: u64 = 20;

/// A window with fewer calls than this is noise and scores `None`.
pub const MIN_SAMPLES: u64 = 5;

pub const REQUESTS_PER_TICK: u64 = 100;

/// Which upstream serves request number `request`. `ranking` holds indices
/// into the fleet, best first, and is never empty.
///
/// Contract:
/// - one request in `PROBE_EVERY` goes to a challenger (anyone but the head),
///   the rest to the head;
/// - probes rotate through the challengers, so each one gets its share;
/// - deterministic — a function of `request`, no randomness;
/// - a fleet of one always answers the head.
#[allow(unused_variables)]
pub fn route(ranking: &[usize], request: u64) -> usize {
    todo!("fill the blank")
}

/// GIVEN: today's routing in one line — the head takes everything.
pub fn head_only(ranking: &[usize], _request: u64) -> usize {
    ranking[0]
}

/// GIVEN: rank on the window's mean, unscored last, ties keep their order.
pub fn rank(previous: &[usize], windows: &[Snapshot]) -> Vec<usize> {
    let score = |i: usize| {
        let window = &windows[i];
        (window.total() >= MIN_SAMPLES).then(|| window.lifetime_mean().unwrap())
    };
    let mut next = previous.to_vec();
    next.sort_by_key(|&i| (score(i).is_none(), score(i)));
    next
}

/// GIVEN: a fleet, a ranking, a request counter. Each tick serves
/// `REQUESTS_PER_TICK` requests through `router`, then re-ranks on that tick.
pub struct Fleet {
    pub upstreams: Vec<Fake>,
    pub ranking: Vec<usize>,
    request: u64,
}

impl Fleet {
    pub fn new(latencies_ms: &[u64]) -> Self {
        let names = ["a", "b", "c", "d"];
        Fleet {
            upstreams: latencies_ms
                .iter()
                .zip(names)
                .map(|(&latency, name)| Fake::new(name, ms(latency), Verdict::Succeeds))
                .collect(),
            ranking: (0..latencies_ms.len()).collect(),
            request: 0,
        }
    }

    pub fn tick(&mut self, router: fn(&[usize], u64) -> usize) {
        let baseline: Vec<Snapshot> = self.upstreams.iter().map(|u| u.stats.snapshot()).collect();
        for _ in 0..REQUESTS_PER_TICK {
            let upstream = &self.upstreams[router(&self.ranking, self.request)];
            upstream.stats.record(upstream.latency(), Outcome::Ok);
            self.request += 1;
        }
        let windows: Vec<Snapshot> = self
            .upstreams
            .iter()
            .zip(&baseline)
            .map(|(u, base)| u.stats.snapshot().diff(base))
            .collect();
        self.ranking = rank(&self.ranking, &windows);
    }

    pub fn head(&self) -> &'static str {
        self.upstreams[self.ranking[0]].name
    }
}

fn degrade_scenario(router: fn(&[usize], u64) -> usize) -> Vec<&'static str> {
    let mut fleet = Fleet::new(&[20, 30]);
    let mut heads = Vec::new();
    for tick in 0..8 {
        if tick == 3 {
            fleet.upstreams[0].set_latency(ms(2000));
        }
        fleet.tick(router);
        heads.push(fleet.head());
    }
    heads
}

fn main() {
    println!("head-only: {:?}", degrade_scenario(head_only));
    println!("probing:   {:?}", degrade_scenario(route));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn without_probes_a_slow_head_is_never_unseated() {
        let heads = degrade_scenario(head_only);

        assert_eq!(
            heads.last(),
            Some(&"a"),
            "GIVEN, the trap: `a` is at 2s and still head — `b` has no window to beat it with"
        );
    }

    #[test]
    fn probes_let_the_faster_rival_take_over() {
        let heads = degrade_scenario(route);

        assert_eq!(&heads[..3], ["a"; 3], "while `a` is the faster one it keeps the head");
        assert_eq!(
            heads[3], "b",
            "`a` degrades during tick 3, and that same tick's probes already measured `b`"
        );
        assert!(heads[3..].iter().all(|&h| h == "b"));
    }

    #[test]
    fn probes_cost_one_in_n_shared_by_every_challenger() {
        let ranking = [2, 0, 1];
        let mut served = [0u64; 3];
        for request in 0..1000 {
            served[route(&ranking, request)] += 1;
        }

        assert_eq!(served[2], 950, "the head keeps 19 requests in 20");
        assert_eq!(
            (served[0], served[1]),
            (25, 25),
            "the 50 probes rotate evenly — neither challenger is starved"
        );
    }

    #[test]
    fn a_fleet_of_one_has_no_one_to_probe() {
        assert!((0..100).all(|request| route(&[0], request) == 0));
    }
}
