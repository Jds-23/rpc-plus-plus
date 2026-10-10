//! Ex1 — the window, not the lifetime · budget 20 min
//!
//! Buys: spec §3.1 — "latency scores are continuously updated based on real
//! request performance". `Snapshot` counters only ever grow, so the cheapest
//! latency number there is — `duration_micros_total / total` — is a lifetime
//! average. An upstream that was fast for a day and is slow *now* still reads
//! fast for hours.
//!
//! PREDICTION (fill before you write code):
//! - 1000 calls at 10ms, then 50 at 800ms. Lifetime mean? Window mean over
//!   just the last 50?
//! - A window with zero calls. What should the mean be — and what goes wrong
//!   if it is `Duration::ZERO`?
//!
//! RESULT:
//! -
//!
//! Assert: see the tests. Run them red first and read the messages.

use latency_lab::{Outcome, Snapshot, Stats, ms};
use std::time::Duration;

/// Mean latency of the calls between `baseline` and `current`.
///
/// Contract:
/// - only calls recorded after `baseline` count — never the lifetime;
/// - every outcome counts (ex3 is where failures get their own treatment);
/// - `None` when the window holds no calls.
#[allow(unused_variables)]
pub fn window_mean(current: &Snapshot, baseline: &Snapshot) -> Option<Duration> {
    // `Snapshot::diff` gives you the window. What you divide by what is the
    // drill.
    todo!("fill the blank")
}

fn main() {
    let stats = Stats::default();
    stats.record_n(1000, ms(10), Outcome::Ok);
    let baseline = stats.snapshot();
    stats.record_n(50, ms(800), Outcome::Ok);
    let current = stats.snapshot();

    println!("lifetime mean: {:?}", current.lifetime_mean());
    println!("window mean:   {:?}", window_mean(&current, &baseline));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_slow_now_is_not_hidden_by_a_fast_past() {
        let stats = Stats::default();
        stats.record_n(1000, ms(10), Outcome::Ok);
        let baseline = stats.snapshot();
        stats.record_n(50, ms(800), Outcome::Ok);
        let current = stats.snapshot();

        assert!(
            current.lifetime_mean().unwrap() < ms(50),
            "GIVEN, the trap: a lifetime mean still calls this upstream fast"
        );
        assert_eq!(
            window_mean(&current, &baseline),
            Some(ms(800)),
            "the window holds only the last 50 calls, and all of them took 800ms"
        );
    }

    #[test]
    fn an_empty_window_is_unknown_not_instant() {
        let stats = Stats::default();
        stats.record_n(100, ms(10), Outcome::Ok);
        let snapshot = stats.snapshot();

        assert_eq!(
            window_mean(&snapshot, &snapshot),
            None,
            "no calls is no evidence — ZERO would rank an idle upstream fastest of all"
        );
    }

    #[test]
    fn a_mixed_window_is_averaged() {
        let stats = Stats::default();
        let baseline = stats.snapshot();
        stats.record_n(3, ms(10), Outcome::Ok);
        stats.record_n(1, ms(50), Outcome::Failed);

        assert_eq!(
            window_mean(&stats.snapshot(), &baseline),
            Some(ms(20)),
            "(3×10 + 50) / 4 — every outcome counts, failures included, for now"
        );
    }
}
