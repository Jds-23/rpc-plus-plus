//! Shared scaffolding for the drills. Nothing here is the lesson — it is the
//! stand-in for `Upstream::call` (`src/upstream/mod.rs`) and `CallError`
//! (`src/upstream/call.rs`), plus a provider that keeps its own quota, so each
//! exercise only contains the primitive it is about.
//!
//! Deliberate departures from the real code:
//! - `UpstreamId` wraps `&'static str`, not `Arc<str>`.
//! - `Fake::call` takes the call's cost in compute units instead of a body, and
//!   answers with the upstream's name, so a test can tell who answered.
//! - `CallError::ErrorStatus` carries `retry_after`. The real one does not:
//!   `Upstream::call_inner` reads `res.status()`, then `res.bytes()`, and the
//!   headers go with the response. Ex4 is about that missing field.
//! - `CallError` has no `ReadFailed`; nothing here reads a body.
//! - The provider's quota is a fixed window, the way providers usually publish
//!   it ("N CU per second"). The proxy's limiter is a token bucket. The two do
//!   not line up exactly — see `Quota`.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use std::{fmt, sync::Arc};

use tokio::time::Instant;

/// `src/jsonrpc.rs:69` — "limit exceeded". Retryable there, and the code ex7
/// answers with when every upstream is limited.
pub const JSONRPC_LIMIT_EXCEEDED: i64 = -32005;
/// `src/jsonrpc.rs` — what `finish` (`src/proxy/mod.rs`) answers a failure with.
pub const JSONRPC_INTERNAL_ERROR: i64 = -32603;

pub const TOO_MANY_REQUESTS: u16 = 429;

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct UpstreamId(pub &'static str);

impl fmt::Debug for UpstreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

/// `src/upstream/call.rs`, minus `ReadFailed`, plus `retry_after`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallError {
    Unreachable,
    ErrorStatus {
        http_status: u16,
        /// The provider's `Retry-After`, when it sent one. Not in the real type.
        retry_after: Option<Duration>,
    },
    RpcError {
        code: i64,
        retryable: bool,
    },
}

impl CallError {
    /// Same rules as the real `CallError::is_retryable`.
    pub fn is_retryable(&self) -> bool {
        match self {
            CallError::Unreachable => true,
            CallError::ErrorStatus { http_status, .. } => {
                *http_status == TOO_MANY_REQUESTS || *http_status >= 500
            }
            CallError::RpcError { retryable, .. } => *retryable,
        }
    }

    /// The provider said "slow down", in either of the two ways providers say it.
    pub fn is_rate_limited(&self) -> bool {
        matches!(
            self,
            CallError::ErrorStatus { http_status: TOO_MANY_REQUESTS, .. }
                | CallError::RpcError { code: JSONRPC_LIMIT_EXCEEDED, .. }
        )
    }

    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            CallError::ErrorStatus { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
}

/// What a fake upstream does with a call its quota lets through.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Answers `Ok(name)` after `latency`.
    Succeeds,
    /// Answers `Err(Unreachable)` after `latency`. Retryable.
    Fails,
}

/// The provider's side of the deal: `limit` compute units per fixed `window`,
/// windows starting when the `Fake` is built.
///
/// A token bucket of `rate` per second and `burst` can put `burst + rate × 1s`
/// into one 1s window. Staying under a provider's `limit` therefore means
/// `burst + rate ≤ limit`, not `rate ≤ limit`.
#[derive(Clone, Copy, Debug)]
pub struct Quota {
    pub limit: u64,
    pub window: Duration,
    /// Whether a 429 carries `Retry-After` (time left in the window).
    pub sends_retry_after: bool,
}

struct Window {
    start: Instant,
    used: u64,
}

/// A fake upstream: a latency and a verdict (both changeable mid-test), an
/// optional provider quota, and counters.
///
/// - `entered`: calls that reached the provider, rejected ones included.
/// - `rejected`: calls the provider answered 429 — each one a paid round trip.
/// - `finished`: calls that ran their `latency` and got the verdict.
/// - `cu_served`: compute units the provider accepted.
///
/// `entered > rejected + finished` after the dust settles means a call was
/// dropped mid-flight.
pub struct Fake {
    pub id: UpstreamId,
    latency: Mutex<Duration>,
    verdict: Mutex<Verdict>,
    quota: Option<Quota>,
    window: Mutex<Window>,
    entered: AtomicUsize,
    rejected: AtomicUsize,
    finished: AtomicUsize,
    cu_served: AtomicU64,
}

impl fmt::Debug for Fake {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.id)
    }
}

impl Fake {
    pub fn new(name: &'static str, latency: Duration, verdict: Verdict) -> Arc<Self> {
        Self::build(name, latency, verdict, None)
    }

    pub fn with_quota(name: &'static str, latency: Duration, quota: Quota) -> Arc<Self> {
        Self::build(name, latency, Verdict::Succeeds, Some(quota))
    }

    fn build(
        name: &'static str,
        latency: Duration,
        verdict: Verdict,
        quota: Option<Quota>,
    ) -> Arc<Self> {
        Arc::new(Self {
            id: UpstreamId(name),
            latency: Mutex::new(latency),
            verdict: Mutex::new(verdict),
            quota,
            window: Mutex::new(Window {
                start: Instant::now(),
                used: 0,
            }),
            entered: AtomicUsize::new(0),
            rejected: AtomicUsize::new(0),
            finished: AtomicUsize::new(0),
            cu_served: AtomicU64::new(0),
        })
    }

    pub fn name(&self) -> &'static str {
        self.id.0
    }

    pub fn set_verdict(&self, verdict: Verdict) {
        *self.verdict.lock().unwrap() = verdict;
    }

    pub fn set_latency(&self, latency: Duration) {
        *self.latency.lock().unwrap() = latency;
    }

    pub fn entered(&self) -> usize {
        self.entered.load(Ordering::SeqCst)
    }

    pub fn rejected(&self) -> usize {
        self.rejected.load(Ordering::SeqCst)
    }

    pub fn finished(&self) -> usize {
        self.finished.load(Ordering::SeqCst)
    }

    pub fn cu_served(&self) -> u64 {
        self.cu_served.load(Ordering::SeqCst)
    }

    /// Shaped like `Upstream::call`: `&self`, awaits, returns a `Result`.
    ///
    /// Over quota: a 429 after the same `latency` — a rejection still costs a
    /// round trip. Within quota: `latency`, then the verdict.
    pub async fn call(&self, cu: u64) -> Result<&'static str, CallError> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        let admitted = self.admit(cu);
        let latency = *self.latency.lock().unwrap();
        tokio::time::sleep(latency).await;
        if let Err(retry_after) = admitted {
            self.rejected.fetch_add(1, Ordering::SeqCst);
            return Err(CallError::ErrorStatus {
                http_status: TOO_MANY_REQUESTS,
                retry_after,
            });
        }
        self.finished.fetch_add(1, Ordering::SeqCst);
        match *self.verdict.lock().unwrap() {
            Verdict::Succeeds => Ok(self.name()),
            Verdict::Fails => Err(CallError::Unreachable),
        }
    }

    /// `Err(retry_after)` when the provider's window has no room for `cu`.
    fn admit(&self, cu: u64) -> Result<(), Option<Duration>> {
        let Some(quota) = self.quota else {
            self.cu_served.fetch_add(cu, Ordering::SeqCst);
            return Ok(());
        };
        let now = Instant::now();
        let mut window = self.window.lock().unwrap();
        while now >= window.start + quota.window {
            window.start += quota.window;
            window.used = 0;
        }
        if window.used + cu > quota.limit {
            let left = window.start + quota.window - now;
            return Err(quota.sends_retry_after.then_some(left));
        }
        window.used += cu;
        self.cu_served.fetch_add(cu, Ordering::SeqCst);
        Ok(())
    }
}

pub const fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}
