//! Shared scaffolding for the drills. Nothing here is the lesson — it is the
//! stand-in for `UpstreamStats` / `Snapshot` (`src/observer/snapshot.rs`) and
//! `Upstream::call` (`src/upstream/mod.rs`), so each exercise only contains the
//! primitive it is about.
//!
//! One deliberate departure from `src/`: `Snapshot` here splits out `failed`,
//! `abandoned` and `failed_micros_total`. Ex3 and ex4 are about why the proxy's
//! own snapshot will need them.

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::time::Instant;

/// Same bounds as `src/observer/snapshot.rs`. Slot `i` counts durations in
/// `(BOUNDS[i-1], BOUNDS[i]]`; slot 0 starts at zero. Past the last bound there
/// is no slot — `total() - buckets.sum()` is the overflow.
pub const BUCKET_BOUNDS_MICROS: [u64; 10] = [
    5_000, 10_000, 25_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 3_000_000, 5_000_000,
];

/// The proxy's per-attempt deadline (`DEFAULT_RPC_TIMEOUT_IN_SECS`).
pub const TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The upstream answered.
    Ok,
    /// The upstream answered with an error, or never connected.
    Failed,
    /// Nobody waited for the answer: a hedge loser, a client disconnect.
    Abandoned,
}

/// Cumulative counters for one upstream, like `UpstreamStats`. Never reset —
/// a window is the difference of two snapshots.
#[derive(Default, Debug)]
pub struct Stats {
    ok: AtomicU64,
    failed: AtomicU64,
    abandoned: AtomicU64,
    buckets: [AtomicU64; BUCKET_BOUNDS_MICROS.len()],
    duration_micros_total: AtomicU64,
    failed_micros_total: AtomicU64,
}

impl Stats {
    pub fn record(&self, duration: Duration, outcome: Outcome) {
        let counter = match outcome {
            Outcome::Ok => &self.ok,
            Outcome::Failed => &self.failed,
            Outcome::Abandoned => &self.abandoned,
        };
        counter.fetch_add(1, Ordering::Relaxed);

        let micros = duration.as_micros() as u64;
        self.duration_micros_total.fetch_add(micros, Ordering::Relaxed);
        if outcome == Outcome::Failed {
            self.failed_micros_total.fetch_add(micros, Ordering::Relaxed);
        }

        let slot = BUCKET_BOUNDS_MICROS.partition_point(|&b| b < micros);
        if slot < self.buckets.len() {
            self.buckets[slot].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// `n` records of the same duration and outcome. Test shorthand.
    pub fn record_n(&self, n: u64, duration: Duration, outcome: Outcome) {
        for _ in 0..n {
            self.record(duration, outcome);
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            ok: self.ok.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            abandoned: self.abandoned.load(Ordering::Relaxed),
            buckets: self.buckets.each_ref().map(|b| b.load(Ordering::Relaxed)),
            duration_micros_total: self.duration_micros_total.load(Ordering::Relaxed),
            failed_micros_total: self.failed_micros_total.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub ok: u64,
    pub failed: u64,
    pub abandoned: u64,
    pub buckets: [u64; BUCKET_BOUNDS_MICROS.len()],
    /// Every recorded call's duration, whatever its outcome — like `src/`.
    pub duration_micros_total: u64,
    /// The `Failed` share of `duration_micros_total`.
    pub failed_micros_total: u64,
}

impl Snapshot {
    pub fn total(&self) -> u64 {
        self.ok + self.failed + self.abandoned
    }

    /// The calls between `base` and `self`, field by field. Saturating, like
    /// `src/`, so a swapped pair reads as an empty window, not a panic.
    pub fn diff(&self, base: &Self) -> Snapshot {
        let mut buckets = [0; BUCKET_BOUNDS_MICROS.len()];
        for (slot, out) in buckets.iter_mut().enumerate() {
            *out = self.buckets[slot].saturating_sub(base.buckets[slot]);
        }
        Snapshot {
            ok: self.ok.saturating_sub(base.ok),
            failed: self.failed.saturating_sub(base.failed),
            abandoned: self.abandoned.saturating_sub(base.abandoned),
            buckets,
            duration_micros_total: self
                .duration_micros_total
                .saturating_sub(base.duration_micros_total),
            failed_micros_total: self
                .failed_micros_total
                .saturating_sub(base.failed_micros_total),
        }
    }

    /// Mean over every call since zero. The number ex1 is about *not* using.
    pub fn lifetime_mean(&self) -> Option<Duration> {
        let total = self.total();
        (total > 0).then(|| Duration::from_micros(self.duration_micros_total / total))
    }
}

/// What a fake upstream does when called.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// Answers `Ok` after its latency.
    Succeeds,
    /// Answers `Err` after its latency. A fast 500 is a `Fails` with 2ms.
    Fails,
    /// Never answers.
    Hangs,
}

/// A fake upstream whose latency and verdict can change mid-test — the
/// "one starts degrading" of spec §4.1. Carries its own `Stats`, which only
/// `attempt` (or your ex4 blank) writes to; `call` itself records nothing.
#[derive(Debug)]
pub struct Fake {
    pub name: &'static str,
    latency_micros: AtomicU64,
    verdict: Mutex<Verdict>,
    entered: AtomicU64,
    finished: AtomicU64,
    pub stats: Stats,
}

impl Fake {
    pub fn new(name: &'static str, latency: Duration, verdict: Verdict) -> Self {
        Self {
            name,
            latency_micros: AtomicU64::new(latency.as_micros() as u64),
            verdict: Mutex::new(verdict),
            entered: AtomicU64::new(0),
            finished: AtomicU64::new(0),
            stats: Stats::default(),
        }
    }

    pub fn latency(&self) -> Duration {
        Duration::from_micros(self.latency_micros.load(Ordering::SeqCst))
    }

    pub fn set_latency(&self, latency: Duration) {
        self.latency_micros
            .store(latency.as_micros() as u64, Ordering::SeqCst);
    }

    pub fn set_verdict(&self, verdict: Verdict) {
        *self.verdict.lock().unwrap() = verdict;
    }

    pub fn entered(&self) -> u64 {
        self.entered.load(Ordering::SeqCst)
    }

    pub fn finished(&self) -> u64 {
        self.finished.load(Ordering::SeqCst)
    }

    /// Shaped like `Upstream::call`: awaits, returns a `Result`, records nothing.
    pub async fn call(&self) -> Result<&'static str, &'static str> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        let verdict = *self.verdict.lock().unwrap();
        match verdict {
            Verdict::Hangs => std::future::pending::<()>().await,
            _ => tokio::time::sleep(self.latency()).await,
        }
        self.finished.fetch_add(1, Ordering::SeqCst);
        match verdict {
            Verdict::Succeeds => Ok(self.name),
            _ => Err(self.name),
        }
    }

    /// `try_once` in `src/proxy/attempt.rs`, faithfully: await the call, *then*
    /// record. Ex4 is about the line this skips when the future is dropped.
    pub async fn attempt(&self) -> Result<&'static str, &'static str> {
        let started = Instant::now();
        let answer = self.call().await;
        let outcome = if answer.is_ok() { Outcome::Ok } else { Outcome::Failed };
        self.stats.record(started.elapsed(), outcome);
        answer
    }
}

pub fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}
