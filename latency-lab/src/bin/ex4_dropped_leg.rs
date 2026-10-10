//! Ex4 — the leg nobody waited for · budget 45 min · DO NOT CUT
//!
//! Buys: latency numbers that include the slow calls. `try_once`
//! (`src/proxy/attempt.rs`) records *after* `upstream.call(body).await`. When
//! the hedger's race is won by the other leg, or the client hangs up, the
//! future is dropped at that `.await` and the record line never runs. The
//! slower an upstream is, the more of its calls get dropped — so the slow ones
//! vanish from its stats and what remains looks fast. Survivorship bias, built
//! out of a drop.
//!
//! PREDICTION (fill before you write code):
//! - Primary takes 500ms, the hedge fires at 100ms to a 50ms backup. Twenty
//!   requests. How many records does the primary get under `Fake::attempt`?
//! - Same race, recording on drop. What duration lands for each dropped leg —
//!   and is it the primary's real latency?
//! - A call completes normally. How do you make sure the drop path does not
//!   record it a second time?
//!
//! RESULT:
//! -
//!
//! Assert: see the tests. Run them red first and read the messages.

#[allow(unused_imports)]
use latency_lab::{Fake, Outcome, Stats, Verdict, ms};
use std::future::Future;
use std::time::Duration;
#[allow(unused_imports)]
use tokio::time::Instant;

/// `Fake::attempt`, but every call records exactly once — including the ones
/// that are dropped before they answer.
///
/// Contract:
/// - completes → records its measured duration as `Ok` or `Failed`;
/// - dropped mid-call → records the time elapsed so far as `Abandoned`;
/// - never both, never neither.
///
/// Shape of it: an RAII guard that holds `&upstream.stats` and the start
/// instant, records `Abandoned` in its `Drop`, and is disarmed on completion.
#[allow(unused_variables)]
pub async fn guarded_attempt(upstream: &Fake) -> Result<&'static str, &'static str> {
    // The guard is yours to define, right here below the function. `Fake::attempt`
    // in `src/lib.rs` is the unguarded version to start from.
    todo!("fill the blank")
}

/// GIVEN: the hedger's race in miniature. Whichever leg answers first wins;
/// the other is dropped where it stands.
pub async fn race<T>(primary: impl Future<Output = T>, hedge: impl Future<Output = T>) -> T {
    tokio::select! {
        answer = primary => answer,
        answer = hedge => answer,
    }
}

const HEDGE_AFTER: Duration = Duration::from_millis(100);

async fn naive_request(primary: &Fake, backup: &Fake) {
    race(primary.attempt(), async {
        tokio::time::sleep(HEDGE_AFTER).await;
        backup.attempt().await
    })
    .await
    .unwrap();
}

async fn guarded_request(primary: &Fake, backup: &Fake) {
    race(guarded_attempt(primary), async {
        tokio::time::sleep(HEDGE_AFTER).await;
        guarded_attempt(backup).await
    })
    .await
    .unwrap();
}

#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() {
    for guarded in [false, true] {
        let primary = Fake::new("primary", ms(500), Verdict::Succeeds);
        let backup = Fake::new("backup", ms(50), Verdict::Succeeds);
        for _ in 0..20 {
            if guarded {
                guarded_request(&primary, &backup).await;
            } else {
                naive_request(&primary, &backup).await;
            }
        }
        let seen = primary.stats.snapshot();
        println!(
            "{}: primary entered {} calls, recorded {}, mean {:?}",
            if guarded { "guarded" } else { "naive  " },
            primary.entered(),
            seen.total(),
            seen.lifetime_mean(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn the_naive_attempt_loses_every_dropped_leg() {
        let primary = Fake::new("primary", ms(500), Verdict::Succeeds);
        let backup = Fake::new("backup", ms(50), Verdict::Succeeds);

        for _ in 0..20 {
            naive_request(&primary, &backup).await;
        }

        assert_eq!(primary.entered(), 20, "GIVEN: the primary was called 20 times");
        assert_eq!(
            primary.stats.snapshot().total(),
            0,
            "GIVEN, the trap: and not one of them was recorded — to the decider, \
             the slowest upstream in the fleet has no latency at all"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_dropped_leg_is_recorded_as_abandoned() {
        let primary = Fake::new("primary", ms(500), Verdict::Succeeds);
        let backup = Fake::new("backup", ms(50), Verdict::Succeeds);

        for _ in 0..20 {
            guarded_request(&primary, &backup).await;
        }

        let seen = primary.stats.snapshot();
        assert_eq!(seen.abandoned, 20, "every dropped leg leaves a record");
        assert_eq!(seen.ok + seen.failed, 0, "and none of them answered");
        assert_eq!(
            seen.lifetime_mean(),
            Some(ms(150)),
            "each was dropped at 150ms (hedge at 100 + backup's 50) — a lower \
             bound on the real 500ms, but no longer silence"
        );
        assert_eq!(
            backup.stats.snapshot().ok,
            20,
            "the winning legs record as usual"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_completed_call_records_once() {
        let upstream = Fake::new("node", ms(40), Verdict::Succeeds);

        guarded_attempt(&upstream).await.unwrap();

        let seen = upstream.stats.snapshot();
        assert_eq!(
            (seen.ok, seen.abandoned),
            (1, 0),
            "the guard fired on a call that finished: disarm it before it drops"
        );
        assert_eq!(seen.lifetime_mean(), Some(ms(40)));
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_call_records_as_failed() {
        let upstream = Fake::new("node", ms(2), Verdict::Fails);

        guarded_attempt(&upstream).await.unwrap_err();

        let seen = upstream.stats.snapshot();
        assert_eq!((seen.failed, seen.abandoned), (1, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn a_hung_call_cut_by_the_deadline_records_the_deadline() {
        let upstream = Fake::new("node", ms(0), Verdict::Hangs);

        let cut = tokio::time::timeout(latency_lab::TIMEOUT, guarded_attempt(&upstream)).await;

        assert!(cut.is_err(), "GIVEN: the deadline fired");
        let seen = upstream.stats.snapshot();
        assert_eq!(seen.abandoned, 1, "`timeout` drops the call — same path as a hedge loser");
        assert_eq!(seen.lifetime_mean(), Some(latency_lab::TIMEOUT));
    }
}
