use std::sync::atomic::{AtomicU64, Ordering};

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

    pub buckets: [AtomicU64; BUCKET_BOUNDS_MICROS.len()],
    pub duration_micros_total: AtomicU64,
}

impl UpstreamStats {
    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            success: self.success.load(Ordering::Relaxed),
            unreachable: self.unreachable.load(Ordering::Relaxed),
            read_failed: self.read_failed.load(Ordering::Relaxed),
            error_status: self.error_status.load(Ordering::Relaxed),
            buckets: self
                .buckets
                .each_ref()
                .map(|bucket| bucket.load(Ordering::Relaxed)),
            duration_micros_total: self.duration_micros_total.load(Ordering::Relaxed),
        }
    }
}

pub struct Snapshot {
    pub success: u64,
    pub unreachable: u64,
    pub read_failed: u64,
    pub error_status: u64,

    pub buckets: [u64; BUCKET_BOUNDS_MICROS.len()],
    pub duration_micros_total: u64,
}

pub struct Diff {
    success: u64,
    unreachable: u64,
    read_failed: u64,
    error_status: u64,
}

impl Diff {
    pub fn error(&self) -> u64 {
        self.error_status + self.read_failed + self.unreachable
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
}

impl Snapshot {
    pub fn diff(&self, base: &Self) -> Diff {
        let d = |f: fn(&Self) -> u64| f(self).saturating_sub(f(base));
        Diff {
            success: d(|diff| diff.success),
            unreachable: d(|diff| diff.unreachable),
            read_failed: d(|diff| diff.read_failed),
            error_status: d(|diff| diff.error_status),
        }
    }
}
