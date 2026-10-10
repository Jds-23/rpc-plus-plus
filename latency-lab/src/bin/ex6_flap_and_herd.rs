//! Ex6 — flapping, and the herd a margin cannot stop · budget 35 min
//!
//! Buys: a ranking that holds still. Latency is noisier than an error rate:
//! two healthy providers sit within a few ms of each other and trade places on
//! every tick. Each swap moves *all* the traffic, which is the next problem —
//! the new head gets slower because it is now the head. `prefer_least_errors`
//! answers the first with `PROMOTION_MARGIN`. This drill reuses that answer and
//! then finds where it stops working.
//!
//! PREDICTION (fill before you write code):
//! - Head at 100ms, rival jittering 95/105ms for ten ticks. Swaps with no
//!   margin? With a 0.8 margin?
//! - Two identical upstreams: 50ms idle, 100ms while they are head and carry
//!   all the traffic. With the margin, how many of ten ticks swap the head?
//! - If the margin can't fix that, what can? (Not in `rank`.)
//!
//! RESULT:
//! -
//!
//! Assert: see the tests. Run them red first and read the messages.

use latency_lab::ms;
use std::time::Duration;

/// The head's score is multiplied by this before comparing, so a challenger
/// has to be 20% faster to take its place. Same constant as `src/`.
pub const PROMOTION_MARGIN: f64 = 0.8;

/// The next ranking, best first. `previous` is the current ranking (indices,
/// never empty); `scores[i]` is upstream `i`'s window score, `None` when it
/// had too few calls to judge.
///
/// Contract:
/// - lower score ranks first;
/// - every scored upstream ranks ahead of every unscored one;
/// - the current head competes with its score × `PROMOTION_MARGIN`;
/// - ties, and the unscored among themselves, keep their `previous` order.
#[allow(unused_variables)]
pub fn rank(previous: &[usize], scores: &[Option<Duration>]) -> Vec<usize> {
    // `rank_no_margin` below is most of it. `SortKey` in
    // `src/decider/prefer_least_errors/refresher.rs` is the rest.
    todo!("fill the blank")
}

/// GIVEN: the same sort with no margin, for comparison.
pub fn rank_no_margin(previous: &[usize], scores: &[Option<Duration>]) -> Vec<usize> {
    let mut next = previous.to_vec();
    next.sort_by_key(|&i| (scores[i].is_none(), scores[i]));
    next
}

type Ranker = fn(&[usize], &[Option<Duration>]) -> Vec<usize>;

/// Head 100ms, rival jittering 95/105ms. Returns how many ticks changed the head.
fn jitter_swaps(ranker: Ranker) -> usize {
    let mut ranking = vec![0, 1];
    let mut swaps = 0;
    for tick in 0..10 {
        let rival = if tick % 2 == 0 { ms(95) } else { ms(105) };
        let next = ranker(&ranking, &[Some(ms(100)), Some(rival)]);
        swaps += usize::from(next[0] != ranking[0]);
        ranking = next;
    }
    swaps
}

/// Two identical upstreams, 50ms idle, 100ms while carrying the head's traffic.
fn herd_swaps(ranker: Ranker) -> usize {
    let mut ranking = vec![0, 1];
    let mut swaps = 0;
    for _ in 0..10 {
        let loaded = |i: usize| if ranking[0] == i { ms(100) } else { ms(50) };
        let next = ranker(&ranking, &[Some(loaded(0)), Some(loaded(1))]);
        swaps += usize::from(next[0] != ranking[0]);
        ranking = next;
    }
    swaps
}

fn main() {
    println!(
        "jitter: {} swaps without margin, {} with",
        jitter_swaps(rank_no_margin),
        jitter_swaps(rank)
    );
    println!(
        "herd:   {} swaps without margin, {} with",
        herd_swaps(rank_no_margin),
        herd_swaps(rank)
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_inside_the_margin_does_not_flap() {
        assert!(
            jitter_swaps(rank_no_margin) >= 5,
            "GIVEN, the trap: without a margin, 5ms of noise moves all traffic on every tick"
        );
        assert_eq!(jitter_swaps(rank), 0, "95ms does not beat 100ms × 0.8");
    }

    #[test]
    fn the_margin_is_a_threshold_not_a_wall() {
        assert_eq!(
            rank(&[0, 1], &[Some(ms(100)), Some(ms(81))]),
            [0, 1],
            "81ms is not 20% faster than 100ms"
        );
        assert_eq!(
            rank(&[0, 1], &[Some(ms(100)), Some(ms(79))]),
            [1, 0],
            "79ms is"
        );
        assert_eq!(
            rank(&[0, 1, 2], &[Some(ms(100)), Some(ms(300)), Some(ms(200))]),
            [0, 2, 1],
            "behind the head, plain order — the margin only guards the head"
        );
    }

    #[test]
    fn unscored_ranks_last_in_its_old_order() {
        assert_eq!(
            rank(&[0, 1, 2], &[Some(ms(500)), None, None]),
            [0, 1, 2],
            "a slow measured head beats two unknowns"
        );
        assert_eq!(
            rank(&[0, 1, 2], &[None, None, Some(ms(500))]),
            [2, 0, 1],
            "an unmeasured head is no reason to keep it ahead of a measured one"
        );
    }

    #[test]
    fn ties_keep_their_order() {
        assert_eq!(rank(&[1, 0], &[Some(ms(80)), Some(ms(80))]), [1, 0]);
        assert_eq!(rank(&[2, 0, 1], &[Some(ms(9)), Some(ms(9)), Some(ms(9))]), [2, 0, 1]);
    }

    #[test]
    fn the_margin_does_not_stop_a_herd() {
        assert_eq!(
            herd_swaps(rank),
            10,
            "this passes when `rank` is right, and documents what it cannot fix: \
             idle 50ms beats loaded 100ms × 0.8 every time. The head is slow *because* \
             it is the head — compare loaded to loaded, or stop sending it 100%"
        );
    }
}
