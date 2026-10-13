//! Ex4 — the provider says no · budget 40 min
//!
//! Buys: our bucket is a guess at the provider's quota. The quota may be
//! shared with another service on the same key, the plan may have changed, the
//! provider may be counting differently. When the guess is wrong, the provider
//! answers 429 (or `-32005` in a 200 body), and a proxy that keeps sending pays
//! a round trip for every no — and under `walk`, a retry on top. This drill owns
//! that failure mode: a local bucket that is full while the provider is saying
//! no.
//!
//! PREDICTION (fill before you write code):
//! - Our bucket allows 20/s; the provider allows 10 per 1s window. 200
//!   requests, one every 50ms, pushback ignored. How many 429s?
//! - Same, with `Retry-After` honored. How many?
//! - Same, provider sends no `Retry-After`, default cooldown 1s. How many 429s,
//!   and how many requests served?
//! - `CallError::ErrorStatus` (`src/upstream/call.rs`): does it keep the
//!   `Retry-After` header today? And a `-32005` in a 200 body — what header
//!   would it have?
//!
//! RESULT:
//! -
//!
//! Assert: see the tests. Run them red first and read the messages.

use std::sync::Mutex;
use std::time::Duration;

use ratelimit_lab::{Fake, Quota, ms};
use tokio::time::Instant;

/// No `Retry-After` to go on: back off this long.
pub const DEFAULT_COOLDOWN: Duration = Duration::from_secs(1);
/// However long the provider asks for. Past this, the decider's ranking
/// (`src/decider/prefer_least_errors/`, refreshed every 15s) is the better tool.
pub const MAX_COOLDOWN: Duration = Duration::from_secs(30);

struct State {
    tokens: f64,
    last: Instant,
    /// The provider said no; ask again after this.
    cooldown_until: Option<Instant>,
}

/// GIVEN: ex2's limiter, plus a cooldown that `try_acquire` respects.
pub struct Limiter {
    state: Mutex<State>,
    rate_per_sec: f64,
    burst: f64,
}

impl Limiter {
    pub fn new(rate_per_sec: f64, burst: u32) -> Self {
        Self {
            state: Mutex::new(State {
                tokens: f64::from(burst),
                last: Instant::now(),
                cooldown_until: None,
            }),
            rate_per_sec,
            burst: f64::from(burst),
        }
    }

    fn refill(&self, state: &mut State, now: Instant) {
        let elapsed = now.saturating_duration_since(state.last).as_secs_f64();
        state.tokens = (state.tokens + elapsed * self.rate_per_sec).min(self.burst);
        state.last = now;
    }

    pub fn try_acquire(&self) -> bool {
        let now = Instant::now();
        let mut state = self.state.lock().unwrap();
        if state.cooldown_until.is_some_and(|until| now < until) {
            return false;
        }
        self.refill(&mut state, now);
        if state.tokens < 1.0 {
            return false;
        }
        state.tokens -= 1.0;
        true
    }

    /// The provider answered 429 or `-32005`. Back off.
    ///
    /// Contract:
    /// - no calls until `now + retry_after`, or `now + DEFAULT_COOLDOWN` when the
    ///   provider gave no hint;
    /// - never longer than `MAX_COOLDOWN`, whatever the provider asked;
    /// - never shortens a cooldown already running;
    /// - the bucket is emptied: when the cooldown ends, only what was earned
    ///   during it goes out, not the burst that was sitting there when the
    ///   provider said no.
    #[allow(unused_variables)]
    pub fn on_rate_limited(&self, retry_after: Option<Duration>) {
        todo!("fill the blank")
    }
}

/// GIVEN, broken on purpose: what the proxy does today — the 429 is a retryable
/// failure and nothing more.
pub fn ignore(_: &Limiter, _: Option<Duration>) {}

pub fn back_off(limiter: &Limiter, retry_after: Option<Duration>) {
    limiter.on_rate_limited(retry_after);
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Tally {
    /// Refused by our own limiter; the provider never saw them.
    pub held_back: usize,
    pub served: usize,
    /// Answered 429 by the provider: a round trip paid for nothing.
    pub rejected: usize,
}

const REQUESTS: u32 = 200;
const EVERY: Duration = ms(50);
const LATENCY: Duration = ms(10);

/// GIVEN: one request every 50ms for 10s, through `limiter`, to `upstream`.
/// `react` is told about every 429.
pub async fn drive(limiter: &Limiter, upstream: &Fake, react: fn(&Limiter, Option<Duration>)) -> Tally {
    let t0 = Instant::now();
    let mut tally = Tally::default();
    for i in 0..REQUESTS {
        tokio::time::sleep_until(t0 + EVERY * i).await;
        if !limiter.try_acquire() {
            tally.held_back += 1;
            continue;
        }
        match upstream.call(1).await {
            Ok(_) => tally.served += 1,
            Err(failure) if failure.is_rate_limited() => {
                tally.rejected += 1;
                react(limiter, failure.retry_after());
            }
            Err(failure) => panic!("the fake only answers or 429s: {failure:?}"),
        }
    }
    tally
}

/// Ten per second, as the provider counts it — half of what our bucket allows.
fn provider(sends_retry_after: bool) -> std::sync::Arc<Fake> {
    let quota = Quota {
        limit: 10,
        window: Duration::from_secs(1),
        sends_retry_after,
    };
    Fake::with_quota("alchemy", LATENCY, quota)
}

/// What we think the provider allows.
fn limiter() -> Limiter {
    Limiter::new(20.0, 5)
}

#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() {
    println!("ours 20/s, the provider's 10/s, 200 requests at 20/s:");
    let tally = drive(&limiter(), &provider(true), ignore).await;
    println!("  ignored          {tally:?}");
    let tally = drive(&limiter(), &provider(true), back_off).await;
    println!("  Retry-After      {tally:?}");
    let tally = drive(&limiter(), &provider(false), back_off).await;
    println!("  default cooldown {tally:?}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn ignoring_the_provider_pays_a_round_trip_per_no() {
        let tally = drive(&limiter(), &provider(true), ignore).await;

        assert_eq!(tally.held_back, 0, "GIVEN: our bucket said yes to every request");
        assert_eq!(
            tally.rejected, 100,
            "GIVEN, and this is the bug: the provider said no 100 times, and we asked \
             again every 50ms. Each no is a round trip — and in `walk`, a 429 is \
             retryable, so a retry on the next upstream on top"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retry_after_turns_a_no_into_a_pause() {
        let tally = drive(&limiter(), &provider(true), back_off).await;

        assert_eq!(
            tally.rejected, 10,
            "one 429 per 1s window: the first no is the signal, and the cooldown to \
             the window's end keeps the other nine from being sent"
        );
        assert_eq!(tally.served, 100, "and the provider's whole quota is still used: 10/s × 10s");
    }

    #[tokio::test(start_paused = true)]
    async fn without_a_hint_the_default_cooldown_still_backs_off() {
        let tally = drive(&limiter(), &provider(false), back_off).await;

        assert_eq!(
            tally.rejected, 5,
            "no `Retry-After`: each 429 buys a 1s pause, which overshoots the window \
             by half — one no every 2s"
        );
        assert_eq!(
            tally.served, 95,
            "the price of guessing: 5 requests the provider would have taken were \
             held back. Still better than 100 rejections"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_cooldown_has_a_ceiling() {
        let limiter = limiter();
        let t0 = Instant::now();

        limiter.on_rate_limited(Some(Duration::from_secs(3600)));

        tokio::time::sleep_until(t0 + MAX_COOLDOWN - ms(1)).await;
        assert!(!limiter.try_acquire(), "still cooling down just before the cap");
        tokio::time::sleep_until(t0 + MAX_COOLDOWN).await;
        assert!(
            limiter.try_acquire(),
            "`Retry-After: 3600` benched the upstream for an hour. One bad header \
             must not take an upstream out longer than `MAX_COOLDOWN`"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_short_hint_does_not_cut_a_long_cooldown() {
        let limiter = limiter();
        let t0 = Instant::now();

        limiter.on_rate_limited(Some(Duration::from_secs(10)));
        limiter.on_rate_limited(Some(Duration::from_secs(1)));

        tokio::time::sleep_until(t0 + Duration::from_secs(5)).await;
        assert!(
            !limiter.try_acquire(),
            "two hedge legs got 429s, 10s and 1s. The later, shorter one reopened the \
             upstream 9s early — straight into the provider's 10s"
        );
        tokio::time::sleep_until(t0 + Duration::from_secs(10)).await;
        assert!(limiter.try_acquire(), "and at 10s it is open again");
    }

    #[tokio::test(start_paused = true)]
    async fn no_hint_means_the_default() {
        let limiter = limiter();
        let t0 = Instant::now();

        limiter.on_rate_limited(None);

        tokio::time::sleep_until(t0 + DEFAULT_COOLDOWN - ms(1)).await;
        assert!(!limiter.try_acquire(), "no hint is not no cooldown");
        tokio::time::sleep_until(t0 + DEFAULT_COOLDOWN).await;
        assert!(limiter.try_acquire(), "`DEFAULT_COOLDOWN` and not a moment longer");
    }

    #[tokio::test(start_paused = true)]
    async fn the_burst_does_not_survive_a_no() {
        let limiter = Limiter::new(1.0, 10);
        let t0 = Instant::now();

        limiter.on_rate_limited(Some(Duration::from_secs(1)));

        tokio::time::sleep_until(t0 + Duration::from_secs(1)).await;
        let granted = (0..10).filter(|_| limiter.try_acquire()).count();
        assert_eq!(
            granted, 1,
            "1s of cooldown at 1/s earns 1 token. 10 means the full bucket from \
             before the 429 went out the moment the cooldown ended — a burst \
             straight into a second 429"
        );
    }
}
