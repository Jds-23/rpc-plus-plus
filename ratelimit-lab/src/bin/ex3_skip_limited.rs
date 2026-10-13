//! Ex3 — skip, don't wait · budget 45 min · DO NOT CUT
//!
//! Buys: the gap's own sentence — "a limited upstream is skipped like a failing
//! one" (`docs/erpc-gap-priorities.md`, #9). *Like* a failing one, but not
//! *as* one: a failure has been paid for (a call, a `retry_after`, an attempt);
//! a skip must cost nothing. Get the placement in `walk` wrong and the limiter
//! adds latency, burns attempts, or charges upstreams that were never called.
//! This drill owns that failure mode: limiting that costs more than it saves.
//!
//! Three GIVEN walks, each broken in one way, then yours:
//! - `walk_waiting`: a limited upstream is waited on until a token comes.
//! - `walk_counting_skips`: a limited upstream is treated as a failed attempt.
//! - `walk_charging_up_front`: the whole chain is charged as soon as it is decided.
//!
//! PREDICTION (fill before you write code):
//! - Head upstream empty at 1 token/s, the next one idle, both answer in
//!   100ms. How long does `walk_waiting` take? Yours?
//! - Chain [limited, limited, healthy], `max_attempt` 2. What does
//!   `walk_counting_skips` answer?
//! - The head answers. How many tokens did a 3-upstream chain spend under
//!   `walk_charging_up_front`?
//! - In `Core::walk` (`src/proxy/mod.rs`), between which two statements does
//!   the limiter check go so a skip pays neither `retry_after` nor an attempt?
//!
//! RESULT:
//! -
//!
//! Assert: see the tests. Run them red first and read the messages.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ratelimit_lab::{CallError, Fake, Verdict, ms};
use tokio::time::Instant;

/// GIVEN: ex1's bucket behind ex2's lock, plus a counter of tokens handed out.
pub struct Limiter {
    bucket: Mutex<Bucket>,
    rate_per_sec: f64,
    granted: AtomicUsize,
}

struct Bucket {
    tokens: f64,
    last: Instant,
    burst: f64,
}

impl Limiter {
    /// Full: `burst` tokens to spend.
    pub fn new(rate_per_sec: f64, burst: u32) -> Self {
        Self::with_tokens(rate_per_sec, burst, f64::from(burst))
    }

    /// Empty: the next token is `1 / rate_per_sec` away.
    pub fn drained(rate_per_sec: f64, burst: u32) -> Self {
        Self::with_tokens(rate_per_sec, burst, 0.0)
    }

    fn with_tokens(rate_per_sec: f64, burst: u32, tokens: f64) -> Self {
        Self {
            bucket: Mutex::new(Bucket {
                tokens,
                last: Instant::now(),
                burst: f64::from(burst),
            }),
            rate_per_sec,
            granted: AtomicUsize::new(0),
        }
    }

    pub fn try_acquire(&self) -> bool {
        let now = Instant::now();
        let mut bucket = self.bucket.lock().unwrap();
        let elapsed = now.saturating_duration_since(bucket.last).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.rate_per_sec).min(bucket.burst);
        bucket.last = now;
        if bucket.tokens < 1.0 {
            return false;
        }
        bucket.tokens -= 1.0;
        self.granted.fetch_add(1, Ordering::SeqCst);
        true
    }

    /// Waits until a token comes, then takes it.
    pub async fn acquire(&self) {
        while !self.try_acquire() {
            let missing = 1.0 - self.bucket.lock().unwrap().tokens;
            tokio::time::sleep(Duration::from_secs_f64(missing / self.rate_per_sec)).await;
        }
    }

    /// Tokens handed out so far — what this upstream has been charged.
    pub fn granted(&self) -> usize {
        self.granted.load(Ordering::SeqCst)
    }
}

/// One entry of the chain `decide` hands out: the upstream and its limiter.
/// In the real code the limiter will be a field of `Upstream`.
pub struct Lane {
    pub upstream: Arc<Fake>,
    pub limiter: Limiter,
}

impl Lane {
    pub fn name(&self) -> &'static str {
        self.upstream.name()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Failure {
    /// The chain was empty.
    NoUpstream,
    /// Every upstream was skipped; none was called. Ex7 answers it.
    AllLimited,
    Call(CallError),
}

/// What a walk did, not just what it answered — `Finished` in `src/proxy/mod.rs`.
#[derive(Debug)]
pub struct Walked {
    pub result: Result<&'static str, Failure>,
    /// Upstreams called, in order.
    pub tried: Vec<&'static str>,
    /// Upstreams passed over for want of a token.
    pub skipped: Vec<&'static str>,
}

/// GIVEN, broken on purpose: `src/proxy/mod.rs`'s `walk`, waiting for a token.
pub async fn walk_waiting(chain: &[Lane], max_attempt: usize, retry_after: Duration) -> Walked {
    let mut tried = Vec::new();
    let mut last_failure = None;
    for lane in chain {
        if tried.len() >= max_attempt {
            break;
        }
        if !tried.is_empty() {
            tokio::time::sleep(retry_after).await;
        }
        lane.limiter.acquire().await;
        tried.push(lane.name());
        match lane.upstream.call(1).await {
            Ok(answer) => return walked(Ok(answer), tried, Vec::new()),
            Err(failure) => {
                let retryable = failure.is_retryable();
                last_failure = Some(failure);
                if !retryable {
                    break;
                }
            }
        }
    }
    walked(Err(last_failure.map_or(Failure::NoUpstream, Failure::Call)), tried, Vec::new())
}

/// GIVEN, broken on purpose: "skipped like a failing one", read literally.
pub async fn walk_counting_skips(chain: &[Lane], max_attempt: usize, retry_after: Duration) -> Walked {
    let mut tried = Vec::new();
    let mut last_failure = None;
    for lane in chain {
        if tried.len() >= max_attempt {
            break;
        }
        if !tried.is_empty() {
            tokio::time::sleep(retry_after).await;
        }
        tried.push(lane.name());
        if !lane.limiter.try_acquire() {
            last_failure = Some(Failure::AllLimited);
            continue;
        }
        match lane.upstream.call(1).await {
            Ok(answer) => return walked(Ok(answer), tried, Vec::new()),
            Err(failure) => {
                let retryable = failure.is_retryable();
                last_failure = Some(Failure::Call(failure));
                if !retryable {
                    break;
                }
            }
        }
    }
    walked(Err(last_failure.unwrap_or(Failure::NoUpstream)), tried, Vec::new())
}

/// GIVEN, broken on purpose: charge at `decide`, then walk what was admitted.
/// Tempting because `decide` already has the chain in hand.
pub async fn walk_charging_up_front(chain: &[Lane], max_attempt: usize, retry_after: Duration) -> Walked {
    let (admitted, skipped): (Vec<&Lane>, Vec<&Lane>) = chain
        .iter()
        .take(max_attempt)
        .partition(|lane| lane.limiter.try_acquire());
    let skipped = skipped.iter().map(|lane| lane.name()).collect();
    let mut tried = Vec::new();
    let mut last_failure = None;
    for lane in admitted {
        if !tried.is_empty() {
            tokio::time::sleep(retry_after).await;
        }
        tried.push(lane.name());
        match lane.upstream.call(1).await {
            Ok(answer) => return walked(Ok(answer), tried, skipped),
            Err(failure) => {
                let retryable = failure.is_retryable();
                last_failure = Some(failure);
                if !retryable {
                    break;
                }
            }
        }
    }
    walked(Err(last_failure.map_or(Failure::AllLimited, Failure::Call)), tried, skipped)
}

fn walked(result: Result<&'static str, Failure>, tried: Vec<&'static str>, skipped: Vec<&'static str>) -> Walked {
    Walked {
        result,
        tried,
        skipped,
    }
}

/// `Core::walk`, with a limiter on every upstream.
///
/// Contract:
/// - no token → the upstream goes to `skipped`, and the walk moves on at once:
///   no `retry_after`, not in `tried`, not counted against `max_attempt`;
/// - a token is taken only from an upstream about to be called — never from
///   one the walk does not reach;
/// - otherwise `walk` as it is: `retry_after` between two calls, a retryable
///   failure moves on, a final one stops, at most `max_attempt` calls;
/// - nothing called because everything was skipped → `Err(Failure::AllLimited)`;
///   called and failed → the last failure; empty chain → `Err(Failure::NoUpstream)`.
#[allow(unused_variables)]
pub async fn walk(chain: &[Lane], max_attempt: usize, retry_after: Duration) -> Walked {
    todo!("fill the blank")
}

const LATENCY: Duration = ms(100);
const RETRY_AFTER: Duration = Duration::from_secs(1);
/// Slow enough that nothing refills during a test.
const TRICKLE: f64 = 0.001;

fn lane(name: &'static str, verdict: Verdict, limiter: Limiter) -> Lane {
    Lane {
        upstream: Fake::new(name, LATENCY, verdict),
        limiter,
    }
}

fn open(name: &'static str) -> Lane {
    lane(name, Verdict::Succeeds, Limiter::new(TRICKLE, 10))
}

#[cfg(test)]
fn limited(name: &'static str) -> Lane {
    lane(name, Verdict::Succeeds, Limiter::drained(TRICKLE, 10))
}

fn head_limited() -> [Lane; 2] {
    [
        lane("alchemy", Verdict::Succeeds, Limiter::drained(1.0, 10)),
        open("infura"),
    ]
}

fn report(name: &str, walked: Walked, t0: Instant) {
    println!(
        "  {name}: {:?} in {:?} · tried {:?} skipped {:?}",
        walked.result,
        t0.elapsed(),
        walked.tried,
        walked.skipped
    );
}

#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() {
    println!("head empty at 1 token/s, next one open, 100ms each, retry_after 1s:");
    let chain = head_limited();
    let t0 = Instant::now();
    report("waiting ", walk_waiting(&chain, 3, RETRY_AFTER).await, t0);
    let chain = head_limited();
    let t0 = Instant::now();
    report("counting", walk_counting_skips(&chain, 3, RETRY_AFTER).await, t0);
    let chain = head_limited();
    let t0 = Instant::now();
    report("yours   ", walk(&chain, 3, RETRY_AFTER).await, t0);

    let chain = [open("alchemy"), open("infura"), open("quicknode")];
    walk_charging_up_front(&chain, 3, RETRY_AFTER).await;
    let up_front: usize = chain.iter().map(|lane| lane.limiter.granted()).sum();
    let chain = [open("alchemy"), open("infura"), open("quicknode")];
    walk(&chain, 3, RETRY_AFTER).await;
    let yours: usize = chain.iter().map(|lane| lane.limiter.granted()).sum();
    println!("head answers, 3-upstream chain: tokens spent up front {up_front}, yours {yours}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn waiting_for_a_token_idles_beside_a_free_upstream() {
        let chain = [lane("a", Verdict::Succeeds, Limiter::drained(1.0, 10)), open("b")];
        let t0 = Instant::now();

        let walked = walk_waiting(&chain, 3, RETRY_AFTER).await;

        assert_eq!(walked.result, Ok("a"), "GIVEN: the head answered — eventually");
        assert_eq!(
            t0.elapsed(),
            Duration::from_secs(1) + LATENCY,
            "GIVEN, and this is the bug: 1s waiting for the head's next token, while \
             `b` sat idle with tokens to spare. The limiter turned into latency"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn counting_skips_as_attempts_strands_a_healthy_upstream() {
        let chain = [limited("a"), limited("b"), open("c")];
        let t0 = Instant::now();

        let walked = walk_counting_skips(&chain, 2, RETRY_AFTER).await;

        assert_eq!(
            walked.result,
            Err(Failure::AllLimited),
            "GIVEN, and this is the bug: two skips spent both attempts; `c` was \
             healthy and never asked"
        );
        assert_eq!(
            t0.elapsed(),
            RETRY_AFTER,
            "GIVEN: and the client waited a full `retry_after` between two upstreams \
             neither of which was ever called"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn charging_up_front_bills_upstreams_never_called() {
        let chain = [open("a"), open("b"), open("c")];

        let walked = walk_charging_up_front(&chain, 3, RETRY_AFTER).await;

        assert_eq!(walked.result, Ok("a"), "GIVEN: the head answered");
        assert_eq!(
            (chain[1].limiter.granted(), chain[2].limiter.granted()),
            (1, 1),
            "GIVEN, and this is the bug: `b` and `c` each paid a token for a call \
             they never got. With `max_attempt` 3 every request spends 3× its \
             budget, and the fallbacks run dry while serving nobody"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_limited_head_is_skipped_at_once() {
        let chain = [lane("a", Verdict::Succeeds, Limiter::drained(1.0, 10)), open("b")];
        let t0 = Instant::now();

        let walked = walk(&chain, 3, RETRY_AFTER).await;

        assert_eq!(walked.result, Ok("b"), "the open upstream answers");
        assert_eq!(walked.skipped, ["a"], "`a` was skipped, and it says so");
        assert_eq!(walked.tried, ["b"], "a skip is not a try");
        assert_eq!(
            t0.elapsed(),
            LATENCY,
            "one call's latency and nothing else: no wait for `a`'s token, no \
             `retry_after` for passing it over"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_skip_does_not_spend_an_attempt() {
        let chain = [limited("a"), limited("b"), open("c")];

        let walked = walk(&chain, 1, RETRY_AFTER).await;

        assert_eq!(
            walked.result,
            Ok("c"),
            "`max_attempt` 1 is one *call*. Two skips cost nothing, so `c` still gets it"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retry_after_is_paid_only_between_calls() {
        let chain = [
            lane("a", Verdict::Fails, Limiter::new(TRICKLE, 10)),
            limited("b"),
            open("c"),
        ];
        let t0 = Instant::now();

        let walked = walk(&chain, 3, RETRY_AFTER).await;

        assert_eq!(walked.result, Ok("c"));
        assert_eq!(walked.tried, ["a", "c"]);
        assert_eq!(
            t0.elapsed(),
            LATENCY + RETRY_AFTER + LATENCY,
            "`a` fails (100ms), one `retry_after` (1s), `c` answers (100ms). A \
             second `retry_after` means the skip of `b` paid one too"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn only_called_upstreams_are_charged() {
        let chain = [open("a"), open("b"), open("c")];

        let walked = walk(&chain, 3, RETRY_AFTER).await;

        assert_eq!(walked.result, Ok("a"));
        let charged: Vec<usize> = chain.iter().map(|lane| lane.limiter.granted()).collect();
        assert_eq!(
            charged,
            [1, 0, 0],
            "the head was called and paid; the fallbacks were never reached and \
             must not pay"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_attempt_cap_still_holds() {
        let chain = [
            lane("a", Verdict::Fails, Limiter::new(TRICKLE, 10)),
            lane("b", Verdict::Fails, Limiter::new(TRICKLE, 10)),
            open("c"),
        ];

        let walked = walk(&chain, 2, RETRY_AFTER).await;

        assert_eq!(walked.result, Err(Failure::Call(CallError::Unreachable)), "two calls, both failed");
        assert_eq!(walked.tried, ["a", "b"], "skips are free; calls still count");
        assert_eq!(chain[2].limiter.granted(), 0, "and `c`, never reached, was never charged");
    }

    #[tokio::test(start_paused = true)]
    async fn everything_limited_is_its_own_failure() {
        let chain = [limited("a"), limited("b")];
        let t0 = Instant::now();

        let walked = walk(&chain, 3, RETRY_AFTER).await;

        assert_eq!(
            walked.result,
            Err(Failure::AllLimited),
            "nothing was called, so there is no call failure to report — and \
             `NoUpstream` would claim the chain was empty"
        );
        assert_eq!(walked.skipped, ["a", "b"]);
        assert_eq!(t0.elapsed(), Duration::ZERO, "and it took no time: ex7 decides whether to wait");
        assert_eq!(chain[0].upstream.entered() + chain[1].upstream.entered(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn an_empty_chain_is_no_upstream() {
        let walked = walk(&[], 3, RETRY_AFTER).await;

        assert_eq!(walked.result, Err(Failure::NoUpstream));
    }
}
