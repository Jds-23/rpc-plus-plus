//! Ex1 — the token bucket · budget 30 min
//!
//! Buys: the per-upstream state the gap names (`docs/erpc-gap-priorities.md`,
//! #9). One bucket per `Upstream`, refilled lazily from the clock — no timer
//! task, no background tick. Every later drill stands on this one, and the
//! easy version of it starves the moment the upstream gets busy. This drill
//! owns that failure mode.
//!
//! PREDICTION (fill before you write code):
//! - `NaiveBucket`, 50 tokens/s, burst 5, polled every 10ms for 2s. How many
//!   calls does it grant?
//! - Your bucket, same polling. How many?
//! - Burst 5, rate 10/s, idle for 10 minutes, then 100 calls in one instant.
//!   How many go out?
//! - `Upstream` (`src/upstream/mod.rs`) is shared as `Arc<Upstream>` by every
//!   request in flight. How many of its fields change after `build`?
//!
//! RESULT:
//! -
//!
//! Assert: see the tests. Run them red first and read the messages.

use std::time::Duration;

use ratelimit_lab::ms;
use tokio::time::Instant;

/// GIVEN, broken on purpose: whole tokens, refilled from whole milliseconds.
/// Looks fine in every test that polls slower than one token's worth of time.
pub struct NaiveBucket {
    rate_per_sec: u64,
    burst: u64,
    tokens: u64,
    last: Instant,
}

impl NaiveBucket {
    pub fn new(rate_per_sec: u64, burst: u64, now: Instant) -> Self {
        Self {
            rate_per_sec,
            burst,
            tokens: burst,
            last: now,
        }
    }

    pub fn try_acquire(&mut self, now: Instant) -> bool {
        let elapsed_ms = now.duration_since(self.last).as_millis() as u64;
        let earned = elapsed_ms * self.rate_per_sec / 1000;
        self.tokens = (self.tokens + earned).min(self.burst);
        self.last = now;
        if self.tokens >= 1 {
            self.tokens -= 1;
            true
        } else {
            false
        }
    }
}

pub struct TokenBucket {
    rate_per_sec: f64,
    burst: f64,
    tokens: f64,
    last: Instant,
}

impl TokenBucket {
    /// Starts full: a fresh upstream may take `burst` calls at once.
    pub fn new(rate_per_sec: f64, burst: u32, now: Instant) -> Self {
        Self {
            rate_per_sec,
            burst: f64::from(burst),
            tokens: f64::from(burst),
            last: now,
        }
    }

    pub fn tokens(&self) -> f64 {
        self.tokens
    }

    /// Takes one token if one is there.
    ///
    /// Contract:
    /// - refill lazily: the time since the last call, times `rate_per_sec`;
    /// - a fraction of a token earned now is still there on the next call;
    /// - never more than `burst` tokens, however long the bucket sat idle;
    /// - a refusal costs nothing: `tokens` never goes below zero.
    #[allow(unused_variables)]
    pub fn try_acquire(&mut self, now: Instant) -> bool {
        todo!("fill the blank")
    }
}

/// How many of the polls, one every `every` for `span`, were granted.
fn poll(mut try_acquire: impl FnMut(Instant) -> bool, every: Duration, span: Duration) -> usize {
    let t0 = Instant::now();
    let polls = (span.as_millis() / every.as_millis()) as u32;
    (0..polls).filter(|&i| try_acquire(t0 + every * i)).count()
}

fn main() {
    let t0 = Instant::now();
    let mut naive = NaiveBucket::new(50, 5, t0);
    let mut bucket = TokenBucket::new(50.0, 5, t0);
    println!(
        "50/s, burst 5, polled every 10ms for 2s: naive granted {}, bucket granted {}",
        poll(|now| naive.try_acquire(now), ms(10), ms(2000)),
        poll(|now| bucket.try_acquire(now), ms(10), ms(2000)),
    );

    let mut bucket = TokenBucket::new(10.0, 5, t0);
    let later = t0 + Duration::from_secs(600);
    let burst = (0..100).filter(|_| bucket.try_acquire(later)).count();
    println!("10/s, burst 5, idle 10 min, then 100 calls at once: {burst} went out");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_naive_bucket_starves_when_polled_faster_than_a_token() {
        let mut naive = NaiveBucket::new(50, 5, Instant::now());

        let granted = poll(|now| naive.try_acquire(now), ms(10), ms(2000));

        assert_eq!(
            granted, 5,
            "GIVEN, and this is the bug: only the starting burst went out. Each 10ms \
             poll earns 10×50/1000 = 0 whole tokens and then resets `last`, so the \
             half token it really earned is thrown away. The busier the upstream, \
             the deader the bucket"
        );
    }

    #[test]
    fn the_naive_bucket_looks_fine_when_polled_slowly() {
        let mut naive = NaiveBucket::new(50, 5, Instant::now());

        let granted = poll(|now| naive.try_acquire(now), ms(100), ms(2000));

        assert_eq!(
            granted, 20,
            "GIVEN: at one poll per 100ms every poll earns 5 whole tokens, so every \
             poll is granted — the test a slow suite would write, and it passes"
        );
    }

    #[test]
    fn a_fresh_bucket_grants_its_burst_then_refuses() {
        let t0 = Instant::now();
        let mut bucket = TokenBucket::new(10.0, 5, t0);

        let granted = (0..8).filter(|_| bucket.try_acquire(t0)).count();

        assert_eq!(granted, 5, "a new bucket is full: `burst` calls at once, then no");
    }

    #[test]
    fn fractions_of_a_token_add_up() {
        let mut bucket = TokenBucket::new(50.0, 5, Instant::now());

        let granted = poll(|now| bucket.try_acquire(now), ms(10), ms(2000));

        assert_eq!(
            granted, 104,
            "5 from the burst + 1.99s × 50/s = 104.5 earned → 104 granted. Fewer \
             means a fraction of a token was dropped somewhere between polls"
        );
    }

    #[test]
    fn idle_time_cannot_bank_a_burst() {
        let t0 = Instant::now();
        let mut bucket = TokenBucket::new(10.0, 5, t0);
        let later = t0 + Duration::from_secs(600);

        let granted = (0..100).filter(|_| bucket.try_acquire(later)).count();

        assert_eq!(
            granted, 5,
            "10 idle minutes earn 6000 tokens; without the `burst` cap they all go \
             out in one instant — 100 calls at a provider that allows 10/s"
        );
    }

    #[test]
    fn a_refusal_costs_nothing() {
        let t0 = Instant::now();
        let mut bucket = TokenBucket::new(10.0, 1, t0);
        assert!(bucket.try_acquire(t0));

        let refused = (0..10).filter(|_| !bucket.try_acquire(t0)).count();
        assert_eq!(refused, 10, "the bucket is empty: all ten refused");
        assert!(
            bucket.tokens() >= 0.0,
            "tokens went to {} — a refusal took one anyway",
            bucket.tokens()
        );

        assert!(
            bucket.try_acquire(t0 + ms(100)),
            "100ms at 10/s is one token. Not granted means the ten refusals were \
             charged: under load, every no makes the next yes later"
        );
        assert!(!bucket.try_acquire(t0 + ms(100)), "and only one");
    }
}
