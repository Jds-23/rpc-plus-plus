//! Ex2 — one bucket, many requests · budget 30 min
//!
//! Buys: the bucket lives on an `Upstream`, and `Arc<Upstream>` is shared by
//! every request in flight — and by both legs of a hedge. So "is there a token"
//! and "take it" are asked by many tasks at once. Put the critical section in
//! the wrong place and the limiter either lets twice its budget through, or
//! makes the upstream serve one call at a time. This drill owns both of those:
//! the wrong critical section.
//!
//! Two designs, GIVEN and broken, one on each side of the answer:
//! - `racy_call`: check in one moment, take in another — the shape of "the
//!   decider filters limited upstreams, `walk` charges them later".
//! - `serial_call`: hold an async lock across the upstream call.
//!
//! PREDICTION (fill before you write code):
//! - Budget of 1 token, two hedge legs through `racy_call` at once. How many
//!   calls reach the provider?
//! - Budget of 10, ten requests through `serial_call`, 100ms each. How long
//!   until the last one answers?
//! - `Decider::decide` (`src/decider.rs`) returns `Vec<Arc<Upstream>>`. Can
//!   the limiter's `try_acquire` take `&mut self`?
//!
//! RESULT:
//! -
//!
//! Assert: see the tests. Run them red first and read the messages.

use std::sync::Mutex;
use std::time::Duration;

use futures::future::join_all;
use ratelimit_lab::{CallError, Fake, Verdict, ms};
use tokio::time::Instant;

/// GIVEN: ex1's answer.
pub struct TokenBucket {
    rate_per_sec: f64,
    burst: f64,
    tokens: f64,
    last: Instant,
}

impl TokenBucket {
    pub fn new(rate_per_sec: f64, burst: u32, now: Instant) -> Self {
        Self {
            rate_per_sec,
            burst: f64::from(burst),
            tokens: f64::from(burst),
            last: now,
        }
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate_per_sec).min(self.burst);
        self.last = now;
    }

    pub fn try_acquire(&mut self, now: Instant) -> bool {
        self.refill(now);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// For the broken designs only: look without taking.
    pub fn has_token(&mut self, now: Instant) -> bool {
        self.refill(now);
        self.tokens >= 1.0
    }

    /// For the broken designs only: take, whether or not it is there.
    pub fn take(&mut self, now: Instant) {
        self.refill(now);
        self.tokens -= 1.0;
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Refused {
    /// Our own limiter said no; the provider never heard of this call.
    Limited,
    Call(CallError),
}

/// What will hang off `Upstream`: `&self` in, because `Upstream` is shared.
pub struct Limiter {
    bucket: Mutex<TokenBucket>,
}

impl Limiter {
    pub fn new(rate_per_sec: f64, burst: u32) -> Self {
        Self {
            bucket: Mutex::new(TokenBucket::new(rate_per_sec, burst, Instant::now())),
        }
    }

    pub fn tokens(&self) -> f64 {
        self.bucket.lock().unwrap().tokens
    }
}

/// GIVEN, broken on purpose: check, yield, take. The `yield_now` stands in for
/// everything between `decide` and the call: the request's other work, another
/// request being scheduled, the hedge timer firing.
pub async fn racy_call(limiter: &Limiter, upstream: &Fake) -> Result<&'static str, Refused> {
    if !limiter.bucket.lock().unwrap().has_token(Instant::now()) {
        return Err(Refused::Limited);
    }
    tokio::task::yield_now().await;
    limiter.bucket.lock().unwrap().take(Instant::now());
    upstream.call(1).await.map_err(Refused::Call)
}

/// GIVEN, broken on purpose: a `tokio::sync::Mutex` makes holding the guard
/// across `.await` compile — so it gets held across the call.
pub struct SerialLimiter {
    bucket: tokio::sync::Mutex<TokenBucket>,
}

impl SerialLimiter {
    pub fn new(rate_per_sec: f64, burst: u32) -> Self {
        Self {
            bucket: tokio::sync::Mutex::new(TokenBucket::new(rate_per_sec, burst, Instant::now())),
        }
    }
}

pub async fn serial_call(limiter: &SerialLimiter, upstream: &Fake) -> Result<&'static str, Refused> {
    let mut bucket = limiter.bucket.lock().await;
    if !bucket.try_acquire(Instant::now()) {
        return Err(Refused::Limited);
    }
    upstream.call(1).await.map_err(Refused::Call)
}

/// Takes a token, then calls the upstream.
///
/// Contract:
/// - no token → `Err(Refused::Limited)`, and the upstream is never called;
/// - two tasks can never both spend the same token;
/// - the lock is released before the call: the limiter caps the *rate* of
///   calls, never how many are in flight at once.
#[allow(unused_variables)]
pub async fn call_limited(limiter: &Limiter, upstream: &Fake) -> Result<&'static str, Refused> {
    todo!("fill the blank")
}

const LATENCY: Duration = ms(100);
/// Slow enough that nothing refills during a test.
const TRICKLE: f64 = 0.001;

#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() {
    let upstream = Fake::new("node", LATENCY, Verdict::Succeeds);
    let limiter = Limiter::new(TRICKLE, 1);
    let _ = tokio::join!(racy_call(&limiter, &upstream), racy_call(&limiter, &upstream));
    println!(
        "racy:   budget 1, two hedge legs → {} calls reached the provider, tokens left {}",
        upstream.entered(),
        limiter.tokens()
    );

    let upstream = Fake::new("node", LATENCY, Verdict::Succeeds);
    let limiter = SerialLimiter::new(TRICKLE, 10);
    let t0 = Instant::now();
    join_all((0..10).map(|_| serial_call(&limiter, &upstream))).await;
    println!("serial: ten 100ms calls took {:?}", t0.elapsed());

    let upstream = Fake::new("node", LATENCY, Verdict::Succeeds);
    let limiter = Limiter::new(TRICKLE, 10);
    let t0 = Instant::now();
    join_all((0..10).map(|_| call_limited(&limiter, &upstream))).await;
    println!("yours:  ten 100ms calls took {:?}", t0.elapsed());
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn upstream() -> Arc<Fake> {
        Fake::new("node", LATENCY, Verdict::Succeeds)
    }

    #[tokio::test(start_paused = true)]
    async fn checking_and_taking_apart_lets_both_legs_through() {
        let upstream = upstream();
        let limiter = Limiter::new(TRICKLE, 1);

        let (a, b) = tokio::join!(racy_call(&limiter, &upstream), racy_call(&limiter, &upstream));

        assert!(a.is_ok() && b.is_ok(), "GIVEN: both legs were let through");
        assert_eq!(
            upstream.entered(),
            2,
            "GIVEN, and this is the bug: a budget of 1, two calls at the provider. \
             Both legs saw the one token before either took it — with N requests in \
             that gap, the budget is N"
        );
        assert!(limiter.tokens() < 0.0, "GIVEN: and the bucket is in debt");
    }

    #[tokio::test(start_paused = true)]
    async fn an_async_lock_held_across_the_call_serializes_the_upstream() {
        let upstream = upstream();
        let limiter = SerialLimiter::new(TRICKLE, 10);
        let t0 = Instant::now();

        let answers = join_all((0..10).map(|_| serial_call(&limiter, &upstream))).await;

        assert!(answers.iter().all(Result::is_ok), "GIVEN: within budget, all ten answered");
        assert_eq!(
            t0.elapsed(),
            LATENCY * 10,
            "GIVEN, and this is the bug: ten calls, 100ms each, one at a time. The \
             limiter was meant to cap calls per second and became a semaphore of 1 \
             — std's `Mutex` would have refused to compile this"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn one_token_one_call() {
        let upstream = upstream();
        let limiter = Limiter::new(TRICKLE, 1);

        let (a, b) = tokio::join!(
            call_limited(&limiter, &upstream),
            call_limited(&limiter, &upstream)
        );

        let refused = [&a, &b].iter().filter(|r| ***r == Err(Refused::Limited)).count();
        assert_eq!(refused, 1, "one leg gets the token, the other is refused: {a:?} {b:?}");
        assert_eq!(upstream.entered(), 1, "and the refused one never reached the provider");
    }

    #[tokio::test(start_paused = true)]
    async fn calls_overlap() {
        let upstream = upstream();
        let limiter = Limiter::new(TRICKLE, 10);
        let t0 = Instant::now();

        let answers = join_all((0..10).map(|_| call_limited(&limiter, &upstream))).await;

        assert!(answers.iter().all(Result::is_ok), "ten tokens, ten calls: {answers:?}");
        assert_eq!(
            t0.elapsed(),
            LATENCY,
            "ten calls in flight together take one call's 100ms. Longer means the \
             lock was still held while the upstream answered"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn many_tasks_never_overspend() {
        let upstream = upstream();
        let limiter = Arc::new(Limiter::new(TRICKLE, 5));

        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..50 {
            let (limiter, upstream) = (limiter.clone(), upstream.clone());
            tasks.spawn(async move { call_limited(&limiter, &upstream).await });
        }
        let answered = tasks.join_all().await.iter().filter(|r| r.is_ok()).count();

        assert_eq!(answered, 5, "fifty spawned requests, a budget of five");
        assert_eq!(upstream.entered(), 5, "the provider saw exactly the budget");
    }
}
