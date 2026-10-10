//! Ex3 — the fastest upstream is the broken one · budget 40 min · DO NOT CUT
//!
//! Buys: a least-latency decider that does not route every request to a dead
//! node. A 500 comes back in 2ms; a rate-limited 429 in 1ms; a refused
//! connection in microseconds. Ranked on raw latency, the upstream that fails
//! fastest wins the head — and `walk` retries every request off it.
//!
//! PREDICTION (fill before you write code):
//! - A fails every call in 2ms. B succeeds every call in 200ms. Which one does
//!   ex1's `window_mean` rank first?
//! - 90 calls OK at 100ms, 10 failed at 1ms, the penalty is 3s. Score?
//! - `src/`'s `Snapshot` has `duration_micros_total` and nothing else on time.
//!   Can this score be computed from it? What field does the impl have to add?
//!
//! RESULT:
//! -
//!
//! Assert: see the tests. Run them red first and read the messages.

use latency_lab::{Outcome, Snapshot, Stats, TIMEOUT, ms};
use std::time::Duration;

/// What one failed call costs in the score. The attempt deadline: a failure
/// spends the caller's time just like a timeout does, then a retry.
pub const FAILURE_PENALTY: Duration = TIMEOUT;

/// GIVEN, ex1's answer, already windowed: the plain mean of `window`.
pub fn naive_mean(window: &Snapshot) -> Option<Duration> {
    window.lifetime_mean()
}

/// The latency score of `window`, lower is better.
///
/// Contract:
/// - the mean over every call in the window, where a `Failed` call counts as
///   `FAILURE_PENALTY` instead of its own measured duration;
/// - `Ok` and `Abandoned` calls count as measured;
/// - `None` for an empty window.
#[allow(unused_variables)]
pub fn score(window: &Snapshot) -> Option<Duration> {
    // Every field you need is on `Snapshot`. Check which of them `src/` has.
    todo!("fill the blank")
}

fn fails_fast() -> Snapshot {
    let stats = Stats::default();
    stats.record_n(100, ms(2), Outcome::Failed);
    stats.snapshot()
}

fn slow_but_ok() -> Snapshot {
    let stats = Stats::default();
    stats.record_n(100, ms(200), Outcome::Ok);
    stats.snapshot()
}

fn main() {
    for (name, window) in [("fails-fast", fails_fast()), ("slow-but-ok", slow_but_ok())] {
        println!(
            "{name:>11}: naive {:?}  score {:?}",
            naive_mean(&window).unwrap(),
            score(&window).unwrap(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fast_failure_does_not_win_the_head() {
        let (broken, healthy) = (fails_fast(), slow_but_ok());

        assert!(
            naive_mean(&broken) < naive_mean(&healthy),
            "GIVEN, the trap: by plain latency the 500-ing upstream is 100× better"
        );
        assert!(
            score(&broken) > score(&healthy),
            "a call that failed in 2ms did not answer in 2ms — it never answered"
        );
    }

    #[test]
    fn failures_cost_the_penalty_in_proportion() {
        let stats = Stats::default();
        stats.record_n(90, ms(100), Outcome::Ok);
        stats.record_n(10, ms(1), Outcome::Failed);

        assert_eq!(
            score(&stats.snapshot()),
            Some(ms(390)),
            "(90×100ms + 10×3s) / 100 — the failures' own 1ms is thrown away"
        );
    }

    #[test]
    fn a_clean_window_scores_its_plain_mean() {
        let stats = Stats::default();
        stats.record_n(4, ms(30), Outcome::Ok);
        stats.record_n(1, ms(80), Outcome::Abandoned);

        assert_eq!(
            score(&stats.snapshot()),
            Some(ms(40)),
            "no failures, no penalty: abandoned calls count as measured"
        );
    }

    #[test]
    fn an_empty_window_has_no_score() {
        assert_eq!(score(&Snapshot::default()), None);
    }
}
