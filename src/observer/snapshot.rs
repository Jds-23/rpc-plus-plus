use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

pub const BUCKET_BOUNDS_MICROS: [u64; 10] = [
    5_000,     // 0.005  local node floor
    10_000,    // 0.01
    25_000,    // 0.025
    50_000,    // 0.05
    100_000,   // 0.1
    250_000,   // 0.25
    500_000,   // 0.5
    1_000_000, // 1.0
    3_000_000, // 3.0   = DEFAULT_RPC_TIMEOUT_IN_SECS
    5_000_000, // 5.0   timeout fired late
];

#[derive(Default)]
pub struct UpstreamStats {
    pub success: AtomicU64,
    pub unreachable: AtomicU64,
    pub read_failed: AtomicU64,
    pub error_status: AtomicU64,
    pub rpc_error: AtomicU64,

    pub retryable_failures: AtomicU64,

    pub buckets: [AtomicU64; BUCKET_BOUNDS_MICROS.len()],
    pub duration_micros_total: AtomicU64,
    pub retryable_failures_micros_total: AtomicU64,
}

impl UpstreamStats {
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            success: self.success.load(Ordering::Relaxed),
            unreachable: self.unreachable.load(Ordering::Relaxed),
            read_failed: self.read_failed.load(Ordering::Relaxed),
            error_status: self.error_status.load(Ordering::Relaxed),
            rpc_error: self.rpc_error.load(Ordering::Relaxed),
            retryable_failures: self.retryable_failures.load(Ordering::Relaxed),
            buckets: self
                .buckets
                .each_ref()
                .map(|bucket| bucket.load(Ordering::Relaxed)),
            duration_micros_total: self.duration_micros_total.load(Ordering::Relaxed),
            retryable_failures_micros_total: self
                .retryable_failures_micros_total
                .load(Ordering::Relaxed),
        }
    }
}

#[derive(Default)]
pub struct Snapshot {
    pub success: u64,
    pub unreachable: u64,
    pub read_failed: u64,
    pub error_status: u64,
    pub rpc_error: u64,
    pub retryable_failures: u64,

    pub buckets: [u64; BUCKET_BOUNDS_MICROS.len()],
    pub duration_micros_total: u64,
    pub retryable_failures_micros_total: u64,
}

/// Hedges between one ordered pair of upstreams.
#[derive(Default)]
pub struct HedgeStats {
    pub started: AtomicU64,
    pub won: AtomicU64,
}

impl HedgeStats {
    pub fn snapshot(&self) -> HedgeSnapshot {
        HedgeSnapshot {
            started: self.started.load(Ordering::Relaxed),
            won: self.won.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HedgeSnapshot {
    pub started: u64,
    pub won: u64,
}

pub struct Diff {
    success: u64,
    unreachable: u64,
    read_failed: u64,
    error_status: u64,
    rpc_error: u64,
    retryable_failures: u64,
    duration_micros: u64,
    retryable_failures_micros: u64,
}

impl Diff {
    pub fn error(&self) -> u64 {
        self.error_status + self.read_failed + self.unreachable + self.rpc_error
    }
    pub fn success(&self) -> u64 {
        self.success
    }
    pub fn total(&self) -> u64 {
        self.error() + self.success()
    }
    pub fn error_rate(&self) -> f64 {
        let total = self.total();
        if total == 0 {
            0.0
        } else {
            self.error() as f64 / self.total() as f64
        }
    }
    pub fn latency_score(&self, penalty: Duration) -> Option<Duration> {
        let total = self.total();
        if total == 0 {
            return None;
        }
        let answered_micros = self
            .duration_micros
            .saturating_sub(self.retryable_failures_micros);
        let retryable_failures_cost_micros = self
            .retryable_failures
            .saturating_mul(penalty.as_micros() as u64);
        Some(Duration::from_micros(
            answered_micros.saturating_add(retryable_failures_cost_micros) / total,
        ))
    }
}

impl Snapshot {
    pub fn diff(&self, base: &Self) -> Diff {
        let d = |f: fn(&Self) -> u64| f(self).saturating_sub(f(base));
        Diff {
            success: d(|diff| diff.success),
            unreachable: d(|diff| diff.unreachable),
            read_failed: d(|diff| diff.read_failed),
            error_status: d(|diff| diff.error_status),
            rpc_error: d(|diff| diff.rpc_error),
            retryable_failures: d(|diff| diff.retryable_failures),
            duration_micros: d(|diff| diff.duration_micros_total),
            retryable_failures_micros: d(|diff| diff.retryable_failures_micros_total),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PENALTY: Duration = Duration::from_secs(3);

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    #[derive(Default)]
    struct Calls(Snapshot);

    impl Calls {
        fn ok(mut self, n: u64, each: Duration) -> Self {
            self.0.success += n;
            self.0.duration_micros_total += n * each.as_micros() as u64;
            self
        }

        fn client_errors(mut self, n: u64, each: Duration) -> Self {
            self.0.error_status += n;
            self.0.duration_micros_total += n * each.as_micros() as u64;
            self
        }

        fn retryable_failures(mut self, n: u64, each: Duration) -> Self {
            let micros = n * each.as_micros() as u64;
            self.0.error_status += n;
            self.0.retryable_failures += n;
            self.0.duration_micros_total += micros;
            self.0.retryable_failures_micros_total += micros;
            self
        }

        fn window(self) -> Diff {
            self.0.diff(&Snapshot::default())
        }
    }

    #[test]
    fn a_slow_now_is_not_hidden_by_a_fast_past() {
        let baseline = Calls::default().ok(1000, ms(10)).0;
        let current = Calls::default().ok(1000, ms(10)).ok(50, ms(800)).0;

        assert_eq!(
            current.diff(&baseline).latency_score(PENALTY),
            Some(ms(800)),
            "the window holds only the last 50 calls, and all of them took 800ms"
        );
    }

    #[test]
    fn a_fast_failure_does_not_win_the_head() {
        let broken = Calls::default().retryable_failures(100, ms(2)).window();
        let healthy = Calls::default().ok(100, ms(200)).window();

        assert!(
            broken.latency_score(PENALTY) > healthy.latency_score(PENALTY),
            "a call that failed in 2ms did not answer in 2ms"
        );
    }

    #[test]
    fn failures_cost_the_penalty_in_proportion() {
        let window = Calls::default()
            .ok(90, ms(100))
            .retryable_failures(10, ms(1))
            .window();

        assert_eq!(
            window.latency_score(PENALTY),
            Some(ms(390)),
            "(90×100ms + 10×3s) / 100; the failures' own 1ms is thrown away"
        );
    }

    #[test]
    fn a_client_error_counts_as_answered() {
        let window = Calls::default()
            .ok(4, ms(30))
            .client_errors(1, ms(80))
            .window();

        assert_eq!(
            window.latency_score(PENALTY),
            Some(ms(40)),
            "a 4xx is not retryable, so it pays no penalty"
        );
    }

    #[test]
    fn an_empty_window_has_no_score() {
        assert_eq!(Calls::default().window().latency_score(PENALTY), None);
    }
}
