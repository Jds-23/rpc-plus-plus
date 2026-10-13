//! Ex7 — when every upstream is limited · budget 60 min
//!
//! Buys: the request path with every piece in place — ex3's skip, ex4's
//! pushback, ex6's cost — and the one question none of them answers: what to
//! tell a client when *every* upstream is out of tokens. Today that request
//! would end as `Failure::NoUpstream` (`src/proxy/mod.rs`), a `-32603` that
//! says the proxy is broken, for a token that may be 5ms away — and a client's
//! retry loop turns one refused request into a storm. Waiting is no fix on its
//! own: a wait the deadline can't cover is a hung connection that fails anyway.
//! This drill owns that failure mode: all limited, answered wrong.
//!
//! Composes: ex2 (the lock), ex3 (skip), ex4 (`on_rate_limited`), ex6
//! (`cost`, `try_acquire_n`), all GIVEN here as their answers.
//!
//! PREDICTION (fill before you write code):
//! - Both upstreams empty, the next token 5ms away, deadline 1s. What does
//!   `serve_no_wait` answer, and when? And yours?
//! - Next token 10s away, deadline 1s. When does yours answer, and with what?
//! - 2000 requests over 10s at two upstreams that sell 500 CU/s each. How many
//!   429s reach the proxy? How many `-32005`s reach the clients?
//! - In `finish` (`src/proxy/mod.rs`), which JSON-RPC code does a client get
//!   for `Failure::NoUpstream` today?
//!
//! RESULT:
//! -
//!
//! Assert: see the tests. Run them red first and read the messages.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use ratelimit_lab::{CallError, Fake, JSONRPC_INTERNAL_ERROR, JSONRPC_LIMIT_EXCEEDED, Quota, ms};
use tokio::time::Instant;

pub const DEFAULT_COOLDOWN: Duration = Duration::from_secs(1);
pub const MAX_COOLDOWN: Duration = Duration::from_secs(30);
/// Float slack: 5ms at 200/s is 1 token, give or take the last bit.
const EPSILON: f64 = 1e-9;

/// GIVEN: ex6's table and answer.
pub const CU_TABLE: &[(&str, u32)] = &[
    ("eth_chainId", 0),
    ("eth_blockNumber", 10),
    ("eth_getBalance", 19),
    ("eth_call", 26),
    ("eth_getLogs", 75),
];

pub fn cost(method: &str) -> u32 {
    let priced = CU_TABLE.iter().find(|(name, _)| *name == method);
    priced.map_or_else(|| CU_TABLE.iter().map(|(_, cu)| *cu).max().unwrap_or(0), |(_, cu)| *cu)
}

struct State {
    tokens: f64,
    last: Instant,
    cooldown_until: Option<Instant>,
}

/// GIVEN: ex2 + ex4 + ex6 — one lock, CU tokens, a cooldown.
pub struct Limiter {
    state: Mutex<State>,
    rate_per_sec: f64,
    burst: f64,
}

impl Limiter {
    pub fn new(rate_per_sec: f64, burst: u32) -> Self {
        Self::with_tokens(rate_per_sec, burst, f64::from(burst))
    }

    pub fn drained(rate_per_sec: f64, burst: u32) -> Self {
        Self::with_tokens(rate_per_sec, burst, 0.0)
    }

    fn with_tokens(rate_per_sec: f64, burst: u32, tokens: f64) -> Self {
        Self {
            state: Mutex::new(State {
                tokens,
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

    pub fn try_acquire_n(&self, n: u32) -> bool {
        let now = Instant::now();
        let mut state = self.state.lock().unwrap();
        if state.cooldown_until.is_some_and(|until| now < until) {
            return false;
        }
        self.refill(&mut state, now);
        let n = f64::from(n).min(self.burst);
        if state.tokens + EPSILON < n {
            return false;
        }
        state.tokens = (state.tokens - n).max(0.0);
        true
    }

    pub fn on_rate_limited(&self, retry_after: Option<Duration>) {
        let now = Instant::now();
        let pause = retry_after.unwrap_or(DEFAULT_COOLDOWN).min(MAX_COOLDOWN);
        let mut state = self.state.lock().unwrap();
        self.refill(&mut state, now);
        state.tokens = 0.0;
        let until = now + pause;
        state.cooldown_until = Some(state.cooldown_until.map_or(until, |current| current.max(until)));
    }

    /// How long until `try_acquire_n(n)` would say yes — cooldown included,
    /// rounded up to the millisecond.
    pub fn ready_in(&self, n: u32) -> Duration {
        let now = Instant::now();
        let state = self.state.lock().unwrap();
        let start = state.cooldown_until.map_or(now, |until| until.max(now));
        let earned = start.saturating_duration_since(state.last).as_secs_f64() * self.rate_per_sec;
        let tokens = (state.tokens + earned).min(self.burst);
        let missing = (f64::from(n).min(self.burst) - tokens).max(0.0);
        let wait_ms = (missing / self.rate_per_sec * 1000.0 - EPSILON).ceil().max(0.0);
        (start - now) + ms(wait_ms as u64)
    }
}

pub struct Lane {
    pub upstream: Arc<Fake>,
    pub limiter: Limiter,
}

/// What the client gets back on failure: a JSON-RPC error object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RpcFault {
    pub code: i64,
    pub message: &'static str,
}

pub const NO_UPSTREAM: RpcFault = RpcFault {
    code: JSONRPC_INTERNAL_ERROR,
    message: "no upstream available",
};
pub const LIMIT_EXCEEDED: RpcFault = RpcFault {
    code: JSONRPC_LIMIT_EXCEEDED,
    message: "every upstream is rate limited",
};
pub const UPSTREAM_FAILED: RpcFault = RpcFault {
    code: JSONRPC_INTERNAL_ERROR,
    message: "upstream failed",
};

/// How one pass over the chain ended.
pub enum Pass {
    Answered(&'static str),
    /// At least one upstream was called; none answered. The last failure.
    Failed(CallError),
    /// Nobody was called: every upstream was skipped for want of tokens.
    AllLimited,
}

/// GIVEN: ex3's walk with ex4's pushback and ex6's cost — and, to keep this
/// drill on its own question, no `max_attempt` and no `retry_after`.
pub async fn pass(chain: &[Lane], cu: u32) -> Pass {
    let mut last_failure = None;
    for lane in chain {
        if !lane.limiter.try_acquire_n(cu) {
            continue;
        }
        match lane.upstream.call(u64::from(cu)).await {
            Ok(answer) => return Pass::Answered(answer),
            Err(failure) => {
                if failure.is_rate_limited() {
                    lane.limiter.on_rate_limited(failure.retry_after());
                }
                let retryable = failure.is_retryable();
                last_failure = Some(failure);
                if !retryable {
                    break;
                }
            }
        }
    }
    last_failure.map_or(Pass::AllLimited, Pass::Failed)
}

/// GIVEN: the failure a client gets when a pass called someone and lost.
pub fn fault_for(failure: &CallError) -> RpcFault {
    if failure.is_rate_limited() { LIMIT_EXCEEDED } else { UPSTREAM_FAILED }
}

/// GIVEN, broken on purpose: what today's `finish` does with an all-limited
/// chain — it is `Failure::NoUpstream`, a `-32603`.
pub async fn serve_no_wait(chain: &[Lane], method: &str, _deadline: Duration) -> Result<&'static str, RpcFault> {
    match pass(chain, cost(method)).await {
        Pass::Answered(answer) => Ok(answer),
        Pass::Failed(failure) => Err(fault_for(&failure)),
        Pass::AllLimited => Err(NO_UPSTREAM),
    }
}

/// GIVEN, broken on purpose: wait for the next token, however long it takes.
pub async fn serve_waiting(chain: &[Lane], method: &str, _deadline: Duration) -> Result<&'static str, RpcFault> {
    let cu = cost(method);
    loop {
        match pass(chain, cu).await {
            Pass::Answered(answer) => return Ok(answer),
            Pass::Failed(failure) => return Err(fault_for(&failure)),
            Pass::AllLimited => {
                let wait = chain.iter().map(|lane| lane.limiter.ready_in(cu)).min();
                tokio::time::sleep(wait.unwrap_or_default()).await;
            }
        }
    }
}

/// One request: pass over the chain; if everyone was limited, wait for the
/// first token — or don't.
///
/// Contract:
/// - an empty chain → `Err(NO_UPSTREAM)`;
/// - `Pass::Answered` → `Ok`; `Pass::Failed` → `Err(fault_for(..))`;
/// - `Pass::AllLimited` → find the soonest `ready_in(cost)` across the chain.
///   If the request can wait that long and still be inside `deadline`
///   (measured from when `serve` was called), sleep and pass again;
/// - otherwise `Err(LIMIT_EXCEEDED)` *at once* — waiting out a deadline that
///   cannot be met holds a connection and still fails.
#[allow(unused_variables)]
pub async fn serve(chain: &[Lane], method: &str, deadline: Duration) -> Result<&'static str, RpcFault> {
    todo!("fill the blank")
}

const LATENCY: Duration = ms(10);
const DEADLINE: Duration = Duration::from_secs(1);

/// An upstream whose own quota never bites, behind `limiter`.
fn lane(name: &'static str, limiter: Limiter) -> Lane {
    Lane {
        upstream: Fake::new(name, LATENCY, ratelimit_lab::Verdict::Succeeds),
        limiter,
    }
}

/// An upstream that sells 500 CU per 1s window, behind a bucket sized to fit.
fn sold(name: &'static str) -> Lane {
    let quota = Quota {
        limit: 500,
        window: Duration::from_secs(1),
        sends_retry_after: true,
    };
    Lane {
        upstream: Fake::with_quota(name, LATENCY, quota),
        limiter: Limiter::new(400.0, 100),
    }
}

#[derive(Debug, Default)]
pub struct Load {
    pub served: usize,
    pub limit_exceeded: usize,
    pub other: usize,
    /// 429s the providers sent back to the proxy.
    pub provider_429s: usize,
    pub slowest: Duration,
}

const LOAD_REQUESTS: u32 = 2000;
const LOAD_EVERY: Duration = ms(5);
const LOAD_DEADLINE: Duration = ms(200);

/// GIVEN: 2000 requests, one every 5ms, at two upstreams selling 500 CU/s each
/// — ~4600 CU/s asked of 1000. Each request is its own task.
pub async fn load() -> Load {
    let chain = Arc::new([sold("alchemy"), sold("infura")]);
    let t0 = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..LOAD_REQUESTS {
        let chain = chain.clone();
        let method = if i % 5 == 4 { "eth_getLogs" } else { "eth_blockNumber" };
        tasks.spawn(async move {
            tokio::time::sleep_until(t0 + LOAD_EVERY * i).await;
            let started = Instant::now();
            let answer = serve(&chain[..], method, LOAD_DEADLINE).await;
            (answer, started.elapsed())
        });
    }
    let mut load = Load::default();
    for (answer, took) in tasks.join_all().await {
        load.slowest = load.slowest.max(took);
        match answer {
            Ok(_) => load.served += 1,
            Err(fault) if fault == LIMIT_EXCEEDED => load.limit_exceeded += 1,
            Err(_) => load.other += 1,
        }
    }
    load.provider_429s = chain.iter().map(|lane| lane.upstream.rejected()).sum();
    load
}

#[tokio::main(flavor = "current_thread", start_paused = true)]
async fn main() {
    println!("both upstreams empty at 200 CU/s, eth_blockNumber (10 CU) → next token 50ms away, deadline 1s:");
    let chain = [lane("alchemy", Limiter::drained(200.0, 100)), lane("infura", Limiter::drained(200.0, 100))];
    let t0 = Instant::now();
    let answer = serve_no_wait(&chain, "eth_blockNumber", DEADLINE).await;
    println!("  no wait: {answer:?} at {:?}", t0.elapsed());
    let t0 = Instant::now();
    let answer = serve(&chain, "eth_blockNumber", DEADLINE).await;
    println!("  yours:   {answer:?} at {:?}", t0.elapsed());

    println!("next token 10s away, deadline 1s:");
    let chain = [lane("alchemy", Limiter::drained(1.0, 100))];
    let t0 = Instant::now();
    let answer = serve(&chain, "eth_blockNumber", DEADLINE).await;
    println!("  yours:   {answer:?} at {:?}", t0.elapsed());

    let load = load().await;
    println!(
        "load, {LOAD_REQUESTS} requests over 10s, deadline {LOAD_DEADLINE:?}: {load:?}"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn both_empty(rate: f64) -> [Lane; 2] {
        [lane("a", Limiter::drained(rate, 100)), lane("b", Limiter::drained(rate, 100))]
    }

    #[tokio::test(start_paused = true)]
    async fn no_wait_calls_a_5ms_token_a_broken_proxy() {
        let chain = both_empty(2000.0);
        let t0 = Instant::now();

        let answer = serve_no_wait(&chain, "eth_blockNumber", DEADLINE).await;

        assert_eq!(
            answer,
            Err(NO_UPSTREAM),
            "GIVEN, and this is the bug: `-32603 no upstream available`, an internal \
             error, for 10 CU that both upstreams earn back in 5ms. The client's retry \
             loop resends at once, and keeps resending"
        );
        assert_eq!(t0.elapsed(), Duration::ZERO, "GIVEN: and it said so instantly");
    }

    #[tokio::test(start_paused = true)]
    async fn waiting_regardless_outlives_the_deadline() {
        let chain = [lane("a", Limiter::drained(1.0, 100))];
        let t0 = Instant::now();

        let answer = serve_waiting(&chain, "eth_blockNumber", DEADLINE).await;

        assert_eq!(answer, Ok("a"), "GIVEN: it answered…");
        assert_eq!(
            t0.elapsed(),
            Duration::from_secs(10) + LATENCY,
            "GIVEN, and this is the bug: …after 10s, to a client whose deadline was 1s. \
             It held a connection and a task for 9s past the point anyone was listening"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_token_that_fits_the_deadline_is_waited_for() {
        let chain = both_empty(2000.0);
        let t0 = Instant::now();

        let answer = serve(&chain, "eth_blockNumber", DEADLINE).await;

        assert_eq!(answer, Ok("a"), "10 CU at 2000 CU/s is 5ms away: worth the wait");
        assert_eq!(t0.elapsed(), ms(5) + LATENCY, "5ms waiting, then one call");
    }

    #[tokio::test(start_paused = true)]
    async fn a_token_past_the_deadline_fails_fast_with_32005() {
        let chain = [lane("a", Limiter::drained(1.0, 100))];
        let t0 = Instant::now();

        let answer = serve(&chain, "eth_blockNumber", DEADLINE).await;

        assert_eq!(
            answer,
            Err(LIMIT_EXCEEDED),
            "`-32005`: the code providers use for exactly this, which clients and \
             libraries already back off on — not `-32603`, which reads as a bug"
        );
        assert_eq!(
            t0.elapsed(),
            Duration::ZERO,
            "and at once: 10s away does not fit 1s, so waiting any of it only holds \
             the connection"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_soonest_upstream_is_waited_for() {
        let chain = [lane("a", Limiter::drained(200.0, 100)), lane("b", Limiter::drained(500.0, 100))];
        let t0 = Instant::now();

        let answer = serve(&chain, "eth_blockNumber", DEADLINE).await;

        assert_eq!(answer, Ok("b"), "`b` earns 10 CU in 20ms, `a` in 50ms");
        assert_eq!(t0.elapsed(), ms(20) + LATENCY, "so the wait is `b`'s 20ms, not the head's 50ms");
    }

    #[tokio::test(start_paused = true)]
    async fn the_cost_sets_the_wait() {
        let rate = 100.0;

        let chain = [lane("a", Limiter::drained(rate, 100))];
        let t0 = Instant::now();
        let answer = serve(&chain, "eth_getLogs", DEADLINE).await;
        assert_eq!(answer, Ok("a"), "75 CU at 100 CU/s: 750ms, inside 1s");
        assert_eq!(t0.elapsed(), ms(750) + LATENCY);

        let chain = [lane("a", Limiter::drained(rate, 100))];
        let t0 = Instant::now();
        let answer = serve(&chain, "eth_getLogs", ms(500)).await;
        assert_eq!(answer, Err(LIMIT_EXCEEDED), "the same 750ms does not fit 500ms");
        assert_eq!(t0.elapsed(), Duration::ZERO, "fail fast, by the getLogs' price, not one token's");
    }

    #[tokio::test(start_paused = true)]
    async fn a_cooldown_counts_as_not_ready() {
        let chain = [lane("a", Limiter::new(1000.0, 100))];
        chain[0].limiter.on_rate_limited(Some(Duration::from_secs(5)));

        let answer = serve(&chain, "eth_blockNumber", DEADLINE).await;

        assert_eq!(
            answer,
            Err(LIMIT_EXCEEDED),
            "full of tokens, but the provider asked for 5s. `ready_in` sees the \
             cooldown; a wait computed from tokens alone would have waited 0ms and spun"
        );
        assert_eq!(chain[0].upstream.entered(), 0, "and the provider was left alone");
    }

    #[tokio::test(start_paused = true)]
    async fn an_empty_chain_is_no_upstream() {
        assert_eq!(serve(&[], "eth_blockNumber", DEADLINE).await, Err(NO_UPSTREAM));
    }

    #[tokio::test(start_paused = true)]
    async fn under_overload_the_proxy_says_no_before_the_provider_does() {
        let load = load().await;

        assert_eq!(
            load.provider_429s, 0,
            "four times the quota asked for, and not one 429 from a provider: every \
             refusal happened here, where it costs nothing"
        );
        assert_eq!(load.other, 0, "no `-32603`s: overload is `-32005`, never an internal error");
        assert!(load.limit_exceeded > 0, "the excess was refused, with `-32005`");
        assert!(
            load.slowest <= LOAD_DEADLINE + LATENCY,
            "and no request outlived its deadline (plus the one call in flight): \
             slowest {:?}",
            load.slowest
        );
    }
}
