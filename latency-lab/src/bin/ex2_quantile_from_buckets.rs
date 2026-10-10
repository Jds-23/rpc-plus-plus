//! Ex2 — a quantile out of ten buckets · budget 40 min
//!
//! Buys: a latency score that is not a mean. Spec §3.2 already promises
//! p50/p95/p99 on the dashboard; Prometheus derives them from the very buckets
//! `src/observer/snapshot.rs` keeps. The decider can read the same buckets —
//! no per-call samples, no allocation, ten `u64`s per upstream.
//!
//! PREDICTION (fill before you write code):
//! - A: 95 calls at 20ms + 5 calls that hit the 3s timeout. B: 100 calls at
//!   150ms. Which ranks first by mean? By p50? By p99?
//! - Ten calls, all inside the (10ms, 25ms] bucket. What is "the" p50 — and
//!   what assumption picks the number?
//! - Every call took 8s, past the last bound. What can the p50 honestly be?
//!
//! RESULT:
//! -
//!
//! Assert: see the tests. Run them red first and read the messages.

#[allow(unused_imports)]
use latency_lab::{BUCKET_BOUNDS_MICROS, Outcome, Snapshot, Stats, ms};
use std::time::Duration;

/// The `q` quantile (`0.0 < q <= 1.0`) of the calls in `window`.
///
/// Contract — Prometheus's `histogram_quantile`, so the decider and the
/// dashboard agree on what "p99" means:
/// - the target rank is `q × total`;
/// - walk the buckets in order; the answer lies in the first non-empty bucket
///   whose running count reaches the rank;
/// - inside that bucket, interpolate linearly between its lower and upper
///   bound (calls assumed evenly spread). Slot 0's lower bound is zero;
/// - a rank that falls past the last bucket (the overflow) answers the last
///   bound — the histogram cannot see further;
/// - `None` for an empty window.
#[allow(unused_variables)]
pub fn quantile(window: &Snapshot, q: f64) -> Option<Duration> {
    // Shape of it: one pass over the buckets carrying the running count and
    // the current bucket's lower bound. Work in `f64` micros for the rank and
    // the interpolation; the bounds are in `BUCKET_BOUNDS_MICROS`.
    todo!("fill the blank")
}

fn tail_heavy() -> Snapshot {
    let stats = Stats::default();
    stats.record_n(95, ms(20), Outcome::Ok);
    stats.record_n(5, ms(3000), Outcome::Ok);
    stats.snapshot()
}

fn steady() -> Snapshot {
    let stats = Stats::default();
    stats.record_n(100, ms(150), Outcome::Ok);
    stats.snapshot()
}

fn main() {
    for (name, window) in [("tail-heavy", tail_heavy()), ("steady", steady())] {
        println!(
            "{name:>10}: mean {:?}  p50 {:?}  p99 {:?}",
            window.lifetime_mean().unwrap(),
            quantile(&window, 0.5).unwrap(),
            quantile(&window, 0.99).unwrap(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_quantile_you_rank_on_picks_the_winner() {
        let (a, b) = (tail_heavy(), steady());

        assert!(
            a.lifetime_mean() > b.lifetime_mean(),
            "GIVEN: by mean, five timeouts make A the slower upstream"
        );
        assert!(
            quantile(&a, 0.5) < quantile(&b, 0.5),
            "by p50, A answers the typical call ~10× faster than B"
        );
        assert!(
            quantile(&a, 0.99).unwrap() > Duration::from_secs(1),
            "and by p99, A is the one that hangs your user for seconds"
        );
        assert!(quantile(&a, 0.99) > quantile(&b, 0.99));
    }

    #[test]
    fn inside_a_bucket_the_answer_is_interpolated() {
        let stats = Stats::default();
        stats.record_n(10, ms(12), Outcome::Ok);
        let window = stats.snapshot();

        assert_eq!(
            quantile(&window, 0.5),
            Some(Duration::from_micros(17_500)),
            "all ten sit in (10ms, 25ms]; halfway through that bucket is 17.5ms, \
             whatever the calls really took — the bucket forgot"
        );
        assert_eq!(
            quantile(&window, 1.0),
            Some(ms(25)),
            "p100 is the bucket's upper bound"
        );
    }

    #[test]
    fn the_rank_walks_across_buckets() {
        let stats = Stats::default();
        stats.record_n(50, ms(3), Outcome::Ok);
        stats.record_n(50, ms(40), Outcome::Ok);
        let window = stats.snapshot();

        assert_eq!(
            quantile(&window, 0.5),
            Some(ms(5)),
            "rank 50 is the last call in (0, 5ms]: that bucket's upper bound"
        );
        assert_eq!(
            quantile(&window, 0.75),
            Some(ms(25) + Duration::from_micros(12_500)),
            "rank 75 is halfway into (25ms, 50ms] — empty buckets in between are skipped"
        );
    }

    #[test]
    fn past_the_last_bound_answers_the_last_bound() {
        let stats = Stats::default();
        stats.record_n(10, ms(8000), Outcome::Ok);
        let window = stats.snapshot();

        assert_eq!(
            quantile(&window, 0.5),
            Some(ms(5000)),
            "the overflow has no upper bound; claim the last one, not infinity"
        );
    }

    #[test]
    fn an_empty_window_has_no_quantile() {
        assert_eq!(quantile(&Snapshot::default(), 0.5), None);
    }
}
