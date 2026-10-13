//! Ex6 — compute units, not requests · budget 25 min
//!
//! Buys: providers don't sell requests, they sell compute units. An
//! `eth_blockNumber` and an `eth_getLogs` are one request each and several
//! times apart in price. A bucket that counts requests is set from the cheap
//! method and blown by the expensive one — at a request rate well inside the
//! limit it was set for. This drill owns that failure mode: counting requests
//! when the provider counts CUs.
//!
//! PREDICTION (fill before you write code):
//! - 50 requests/s, every 5th `eth_getLogs` (75 CU), the rest
//!   `eth_blockNumber` (10 CU). How many CU/s is that?
//! - The provider allows 500 CU/s. The request bucket was set at 40/s +
//!   burst 10 — "500 CU ÷ 10 CU each". Over 10s, how many 429s?
//! - A method missing from our table costs what? What goes wrong if the
//!   answer is 1?
//! - The method name is already parsed on the hot path in `src/jsonrpc.rs`.
//!   By which functions, and how many times per request?
//!
//! RESULT:
//! -
//!
//! Assert: see the tests. Run them red first and read the messages.

use std::sync::Arc;
use std::time::Duration;

use ratelimit_lab::{Fake, Quota, ms};
use tokio::time::Instant;

/// Our copy of the provider's price list. Shaped like a real provider's CU
/// table; the numbers are illustrative, not any provider's current prices.
pub const CU_TABLE: &[(&str, u32)] = &[
    ("eth_chainId", 0),
    ("eth_blockNumber", 10),
    ("eth_getTransactionReceipt", 15),
    ("eth_getBlockByNumber", 16),
    ("eth_getBalance", 19),
    ("eth_call", 26),
    ("eth_getLogs", 75),
    ("eth_sendRawTransaction", 250),
    ("debug_traceTransaction", 309),
];

/// GIVEN: what the provider actually charges. It knows a method our copy of
/// the table missed.
pub fn provider_price(method: &str) -> u64 {
    match method {
        "trace_block" => 300,
        _ => CU_TABLE
            .iter()
            .find(|(name, _)| *name == method)
            .map_or(10, |(_, cu)| u64::from(*cu)),
    }
}

/// What a call to `method` will cost against the provider's quota.
///
/// Contract:
/// - a method in `CU_TABLE` costs what the table says;
/// - a method that is not costs as much as the most expensive one that is:
///   overcharging an unknown method wastes a little quota, undercharging it is
///   the 429 this whole lab is about.
#[allow(unused_variables)]
pub fn cost(method: &str) -> u32 {
    todo!("fill the blank")
}

/// Ex1's bucket, counting compute units instead of calls.
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

    pub fn drained(rate_per_sec: f64, burst: u32, now: Instant) -> Self {
        Self {
            tokens: 0.0,
            ..Self::new(rate_per_sec, burst, now)
        }
    }

    pub fn tokens(&self) -> f64 {
        self.tokens
    }

    fn refill(&mut self, now: Instant) {
        let elapsed = now.saturating_duration_since(self.last).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.rate_per_sec).min(self.burst);
        self.last = now;
    }

    /// GIVEN: ex1's answer — one call, one token.
    pub fn try_acquire(&mut self, now: Instant) -> bool {
        self.refill(now);
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }

    /// Takes `n` tokens if `n` are there.
    ///
    /// Contract:
    /// - all or nothing: a refusal takes no tokens at all;
    /// - a cost larger than `burst` could never fit: charge it as `burst`, so
    ///   the call waits for a full bucket instead of waiting forever.
    #[allow(unused_variables)]
    pub fn try_acquire_n(&mut self, now: Instant, n: u32) -> bool {
        todo!("fill the blank")
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Tally {
    /// Refused by our own bucket; the provider never saw them.
    pub held_back: usize,
    pub served: usize,
    /// Answered 429 by the provider.
    pub rejected: usize,
}

const EVERY: Duration = ms(20);
const SPAN: Duration = Duration::from_secs(10);

/// GIVEN: every 5th request is `eth_getLogs`, the rest `eth_blockNumber`.
pub fn method_at(i: u32) -> &'static str {
    if i % 5 == 4 { "eth_getLogs" } else { "eth_blockNumber" }
}

/// 500 CU per 1s window.
pub fn provider() -> Arc<Fake> {
    let quota = Quota {
        limit: 500,
        window: Duration::from_secs(1),
        sends_retry_after: false,
    };
    Fake::with_quota("alchemy", ms(5), quota)
}

/// GIVEN: one request every 20ms for 10s. `admit` is our limiter's say;
/// the provider charges its own price either way.
pub async fn drive(upstream: &Fake, mut admit: impl FnMut(Instant, &str) -> bool) -> Tally {
    let t0 = Instant::now();
    let mut tally = Tally::default();
    for i in 0..(SPAN.as_millis() / EVERY.as_millis()) as u32 {
        tokio::time::sleep_until(t0 + EVERY * i).await;
        let method = method_at(i);
        if !admit(Instant::now(), method) {
            tally.held_back += 1;
            continue;
        }
        match upstream.call(provider_price(method)).await {
            Ok(_) => tally.served += 1,
            Err(failure) if failure.is_rate_limited() => tally.rejected += 1,
            Err(failure) => panic!("the fake only answers or 429s: {failure:?}"),
        }
    }
    tally
}

/// GIVEN, broken on purpose: "500 CU ÷ 10 CU per call = 50 calls/s, keep 10
/// back for burst".
pub fn requests_bucket() -> TokenBucket {
    TokenBucket::new(40.0, 10, Instant::now())
}

/// 500 CU per window = burst + rate × 1s (see `Quota` in `src/lib.rs`).
pub fn cu_bucket() -> TokenBucket {
    TokenBucket::new(400.0, 100, Instant::now())
}

#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() {
    println!("50 req/s, 1 in 5 eth_getLogs, provider allows 500 CU/s, 10s:");
    let upstream = provider();
    let mut bucket = requests_bucket();
    let tally = drive(&upstream, |now, _| bucket.try_acquire(now)).await;
    println!("  by request {tally:?} · CU served {}", upstream.cu_served());

    let upstream = provider();
    let mut bucket = cu_bucket();
    let tally = drive(&upstream, |now, method| bucket.try_acquire_n(now, cost(method))).await;
    println!("  by CU      {tally:?} · CU served {}", upstream.cu_served());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn a_request_bucket_is_blown_by_the_expensive_method() {
        let upstream = provider();
        let mut bucket = requests_bucket();

        let tally = drive(&upstream, |now, _| bucket.try_acquire(now)).await;

        assert_eq!(
            tally.rejected, 169,
            "GIVEN, and this is the bug: a bucket sized for 10-CU calls let through 409 \
             calls averaging 23 CU — ~940 CU/s at a provider that sells 500. 169 of \
             them, 41%, came back 429: round trips paid for nothing"
        );
        assert_eq!(tally.served, 240, "GIVEN: 240 served — remember this number");
    }

    #[test]
    fn known_methods_cost_what_the_table_says() {
        assert_eq!(cost("eth_blockNumber"), 10);
        assert_eq!(cost("eth_getLogs"), 75, "one getLogs is 7.5 blockNumbers");
        assert_eq!(cost("eth_chainId"), 0, "free is a price too");
    }

    #[test]
    fn an_unknown_method_costs_the_most() {
        assert_eq!(
            cost("trace_block"),
            309,
            "missing from our table; the provider charges 300 for it. Priced at 1, \
             a client sending nothing else gets 300× the quota through"
        );
    }

    #[test]
    fn a_refused_cost_takes_nothing() {
        let t0 = Instant::now();
        let mut bucket = TokenBucket::new(400.0, 100, t0);
        assert!(bucket.try_acquire_n(t0, 90), "90 of 100");

        assert!(!bucket.try_acquire_n(t0, 75), "10 left, 75 asked: no");
        assert_eq!(
            bucket.tokens(),
            10.0,
            "all or nothing. A partial take leaves the bucket short for the cheap \
             calls behind it, and still sends nothing"
        );
        assert!(bucket.try_acquire_n(t0, 10), "the 10 are still there for a blockNumber");
    }

    #[test]
    fn a_cost_bigger_than_the_bucket_still_fits_eventually() {
        let t0 = Instant::now();
        let mut bucket = TokenBucket::drained(100.0, 300, t0);

        assert!(!bucket.try_acquire_n(t0 + ms(2999), 309), "not yet: the bucket is not full");
        assert!(
            bucket.try_acquire_n(t0 + Duration::from_secs(3), 309),
            "309 CU never fits a 300 bucket. Unclamped, every debug_traceTransaction \
             through this upstream is refused forever"
        );
        assert_eq!(bucket.tokens(), 0.0, "charged as the whole bucket");
    }

    #[tokio::test(start_paused = true)]
    async fn a_cu_bucket_keeps_the_provider_happy() {
        let upstream = provider();
        let mut bucket = cu_bucket();

        let tally = drive(&upstream, |now, method| bucket.try_acquire_n(now, cost(method))).await;

        assert_eq!(tally.rejected, 0, "counted in CUs, under the window's 500: no 429s");
        assert_eq!(
            tally.served, 401,
            "and *more* requests served than the request bucket's 240: when a getLogs \
             does not fit, the blockNumbers behind it still do"
        );
        assert!(
            upstream.cu_served() >= 4000,
            "and the quota is still used: 100 + 400/s × 10s ≈ 4100 CU, served {}",
            upstream.cu_served()
        );
    }
}
