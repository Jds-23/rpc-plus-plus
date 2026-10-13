//! Ex5 — the hedge budget · budget 40 min · DO NOT CUT
//!
//! Buys: the gap's reason for existing — "keeps hedging and retries from
//! blowing provider quotas; eRPC warns hedges are paid duplicates"
//! (`docs/erpc-gap-priorities.md`, #9). A hedge is cheap insurance when one
//! call in twenty is slow. When the primary slows down for *everyone* — the
//! exact moment it is under strain — every request runs past `hedge_after`,
//! every request hedges, and the bill doubles. Per-upstream buckets cannot see
//! this: each leg is within its own upstream's rate. This drill owns that
//! failure mode: the hedge storm.
//!
//! The fix is a budget across requests, the shape Finagle and gRPC use for
//! retries: every request earns `percent` of a hedge, every hedge costs one.
//! Steady state, at most `percent`% of requests hedge.
//!
//! PREDICTION (fill before you write code):
//! - Primary 300ms, backup 50ms, `hedge_after` 100ms, 100 requests, no
//!   budget. How many upstream calls?
//! - A 10% budget starting full at 5 hedges. How many hedges in the same 100?
//! - 20 fast requests, then 20 slow ones. The budget is asked when the request
//!   *starts* instead of when the timer fires. How many slow requests hedge?
//! - `race` in `src/proxy/hedge.rs`: what caps the hedges of one request
//!   today? And the hedges per second across all requests?
//!
//! RESULT:
//! -
//!
//! Assert: see the tests. Run them red first and read the messages.

use std::pin::pin;
use std::sync::Mutex;
use std::time::Duration;

use ratelimit_lab::{CallError, Fake, Verdict, ms};

/// What a hedge costs, in the budget's units: hundredths of a hedge, so a
/// 10% deposit is exactly 10 and never drifts like `0.1` summed ten times.
pub const HEDGE: u32 = 100;

pub struct HedgeBudget {
    /// Hundredths of a hedge each request earns.
    percent: u32,
    /// Most hundredths the budget holds: `cap` hedges.
    ceiling: u32,
    balance: Mutex<u32>,
}

impl HedgeBudget {
    /// Starts full: `cap` hedges are available before any request has earned one.
    pub fn new(percent: u32, cap: u32) -> Self {
        Self {
            percent,
            ceiling: cap * HEDGE,
            balance: Mutex::new(cap * HEDGE),
        }
    }

    pub fn balance(&self) -> u32 {
        *self.balance.lock().unwrap()
    }

    /// One request arrived: earn `percent` hundredths of a hedge.
    ///
    /// Contract:
    /// - never more than `ceiling` — a quiet hour must not save up a storm;
    /// - safe from many tasks at once.
    #[allow(unused_variables)]
    pub fn deposit(&self) {
        todo!("fill the blank")
    }

    /// The hedge timer fired: may this request hedge?
    ///
    /// Contract:
    /// - `true` and one `HEDGE` taken when the balance holds one;
    /// - `false` and nothing taken otherwise;
    /// - two tasks can never spend the same hedge.
    pub fn try_spend(&self) -> bool {
        todo!("fill the blank")
    }
}

/// GIVEN: `race` (`src/proxy/hedge.rs`) cut down to two legs. `may_hedge` is
/// asked only when the primary runs past `hedge_after`; a `false` waits on the
/// primary instead — a withheld hedge is slower, never a failure.
///
/// Returns the answer and whether a hedge was started.
pub async fn race(
    primary: &Fake,
    backup: &Fake,
    hedge_after: Duration,
    may_hedge: impl FnOnce() -> bool,
) -> (Result<&'static str, CallError>, bool) {
    let mut first = pin!(primary.call(1));
    tokio::select! {
        biased;
        answer = &mut first => return (answer, false),
        _ = tokio::time::sleep(hedge_after) => {}
    }
    if !may_hedge() {
        return (first.await, false);
    }
    let mut second = pin!(backup.call(1));
    tokio::select! {
        answer = &mut first => (answer, true),
        answer = &mut second => (answer, true),
    }
}

/// How one request asks for a hedge.
#[derive(Clone, Copy, Debug)]
pub enum Policy {
    /// GIVEN, broken on purpose: what `race` does today.
    Unbudgeted,
    /// GIVEN, broken on purpose: deposit, then spend at once "to keep it simple".
    AskEarly,
    /// Deposit at the start; spend only if the timer fires.
    Budgeted,
}

pub async fn serve(policy: Policy, budget: &HedgeBudget, primary: &Fake, backup: &Fake) -> bool {
    let (answer, hedged) = match policy {
        Policy::Unbudgeted => race(primary, backup, HEDGE_AFTER, || true).await,
        Policy::AskEarly => {
            budget.deposit();
            let allowed = budget.try_spend();
            race(primary, backup, HEDGE_AFTER, || allowed).await
        }
        Policy::Budgeted => {
            budget.deposit();
            race(primary, backup, HEDGE_AFTER, || budget.try_spend()).await
        }
    };
    assert!(answer.is_ok(), "the fakes always answer: {answer:?}");
    hedged
}

const HEDGE_AFTER: Duration = ms(100);
const FAST: Duration = ms(50);
const SLOW: Duration = ms(300);

/// `n` requests one after another; how many of them hedged.
pub async fn run(policy: Policy, budget: &HedgeBudget, primary: &Fake, backup: &Fake, n: usize) -> usize {
    let mut hedges = 0;
    for _ in 0..n {
        hedges += usize::from(serve(policy, budget, primary, backup).await);
    }
    hedges
}

fn pair(primary_latency: Duration) -> (std::sync::Arc<Fake>, std::sync::Arc<Fake>) {
    (
        Fake::new("alchemy", primary_latency, Verdict::Succeeds),
        Fake::new("infura", FAST, Verdict::Succeeds),
    )
}

fn calls(primary: &Fake, backup: &Fake) -> usize {
    primary.entered() + backup.entered()
}

#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() {
    println!("primary slowed to 300ms, hedge_after 100ms, 100 requests:");
    for policy in [Policy::Unbudgeted, Policy::Budgeted] {
        let (primary, backup) = pair(SLOW);
        let budget = HedgeBudget::new(10, 5);
        let hedges = run(policy, &budget, &primary, &backup, 100).await;
        println!("  {policy:?}: {hedges} hedges, {} upstream calls", calls(&primary, &backup));
    }

    println!("20 fast requests, then the primary slows for 20:");
    for policy in [Policy::AskEarly, Policy::Budgeted] {
        let (primary, backup) = pair(FAST);
        let budget = HedgeBudget::new(10, 5);
        run(policy, &budget, &primary, &backup, 20).await;
        primary.set_latency(SLOW);
        let hedges = run(policy, &budget, &primary, &backup, 20).await;
        println!("  {policy:?}: {hedges} of the slow 20 hedged");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::time::Instant;

    use super::*;

    #[tokio::test(start_paused = true)]
    async fn unbudgeted_hedging_doubles_the_bill_when_the_primary_slows() {
        let (primary, backup) = pair(SLOW);
        let budget = HedgeBudget::new(10, 5);

        let hedges = run(Policy::Unbudgeted, &budget, &primary, &backup, 100).await;

        assert_eq!(hedges, 100, "GIVEN: every request ran past 100ms, every request hedged");
        assert_eq!(
            calls(&primary, &backup),
            200,
            "GIVEN, and this is the bug: 100 requests, 200 paid calls. A hedge was \
             insurance against the odd slow call; with the primary slow for \
             everyone, it is a 2× bill at the moment the fleet is under strain"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn asking_early_spends_the_budget_on_requests_that_did_not_need_it() {
        let (primary, backup) = pair(FAST);
        let budget = HedgeBudget::new(10, 5);
        // GIVEN, but it runs on your `deposit` and `try_spend`: red until they are.
        let warmup = run(Policy::AskEarly, &budget, &primary, &backup, 20).await;
        assert_eq!(warmup, 0, "GIVEN: fast requests never reach the timer, so none hedged");
        primary.set_latency(SLOW);

        let hedges = run(Policy::AskEarly, &budget, &primary, &backup, 20).await;

        assert_eq!(
            hedges, 2,
            "GIVEN, and this is the bug: 20 fast requests spent 6 hedges they never \
             used, so when the primary slowed only 2 of 20 could hedge"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_budget_holds_hedges_to_its_share() {
        let (primary, backup) = pair(SLOW);
        let budget = HedgeBudget::new(10, 5);

        let hedges = run(Policy::Budgeted, &budget, &primary, &backup, 100).await;

        assert_eq!(
            hedges, 14,
            "5 from the starting balance, then one per ten requests: requests 11, 21, \
             …, 91. More means a deposit or a spend is off; 100 means nothing is spent"
        );
        assert_eq!(calls(&primary, &backup), 114, "114 paid calls for 100 requests, not 200");
    }

    #[tokio::test(start_paused = true)]
    async fn a_withheld_hedge_waits_on_the_primary() {
        let (primary, backup) = pair(SLOW);
        let budget = HedgeBudget::new(10, 0);
        let t0 = Instant::now();

        let hedged = serve(Policy::Budgeted, &budget, &primary, &backup).await;

        assert!(!hedged, "an empty budget: no hedge");
        assert_eq!(
            t0.elapsed(),
            SLOW,
            "and the request still answered, at the primary's 300ms. A withheld hedge \
             costs latency, never an error"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn only_a_fired_timer_spends() {
        let (primary, backup) = pair(FAST);
        let budget = HedgeBudget::new(10, 5);
        primary.set_latency(SLOW);
        run(Policy::Budgeted, &budget, &primary, &backup, 3).await;
        let after_slow = budget.balance();
        primary.set_latency(FAST);

        run(Policy::Budgeted, &budget, &primary, &backup, 20).await;

        assert_eq!(
            after_slow, 220,
            "500 → 400 → 310 → 220: three hedges, two deposits — the first one hit the cap"
        );
        assert_eq!(
            budget.balance(),
            420,
            "twenty fast requests earn 20 × 10 and spend nothing — the budget is asked \
             when the timer fires, not when the request starts"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_quiet_hour_does_not_save_up_a_storm() {
        let (primary, backup) = pair(FAST);
        let budget = HedgeBudget::new(10, 5);
        run(Policy::Budgeted, &budget, &primary, &backup, 1000).await;
        assert_eq!(budget.balance(), 5 * HEDGE, "1000 fast requests: the budget sits at its cap");
        primary.set_latency(SLOW);

        let hedges = run(Policy::Budgeted, &budget, &primary, &backup, 100).await;

        assert_eq!(
            hedges, 14,
            "the same 14 as a fresh budget. 1000 × 10 = 100 hedges' worth earned while \
             quiet — uncapped, all 100 slow requests would have hedged"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn many_tasks_never_overspend() {
        let budget = Arc::new(HedgeBudget::new(10, 5));

        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..50 {
            let budget = budget.clone();
            tasks.spawn(async move { budget.try_spend() });
        }
        let spent = tasks.join_all().await.into_iter().filter(|&spent| spent).count();

        assert_eq!(spent, 5, "fifty hedge timers at once, a balance of five");
        assert_eq!(budget.balance(), 0);
    }
}
