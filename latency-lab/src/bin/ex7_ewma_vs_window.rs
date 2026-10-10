//! Ex7 — EWMA, and why it decays by time · budget 30 min
//!
//! Buys: the other design. Ex1's window throws everything out each tick and
//! needs a deque of snapshots; an EWMA is one `f64` per upstream, updated on
//! every call, with no ticker at all. Finagle, Linkerd and Envoy route on one.
//! The textbook version weighs every *sample* the same — and an upstream that
//! is only probed now and then (ex5) then remembers last week far too well.
//!
//! PREDICTION (fill before you write code):
//! - 1000 calls at 10ms, a minute of silence, then one call at 900ms. Per-sample
//!   EWMA with α = 0.1: new value? Time-decayed with a 5s half-life?
//! - Value 100ms, then 200ms observed exactly one half-life later. New value?
//! - 300 slow calls land in the same instant. With pure time decay, how much
//!   does the value move? Why does that need a floor?
//! - After the minute of silence but *before* the 900ms call, what does either
//!   EWMA say? What does ex1's window say?
//!
//! RESULT:
//! -
//!
//! Assert: see the tests. Run them red first and read the messages.

use latency_lab::ms;
use std::time::Duration;

/// A sample never weighs less than this, however close it lands to the last.
pub const MIN_WEIGHT: f64 = 0.01;

/// GIVEN: the textbook EWMA. Every sample moves the value by `alpha`.
pub struct PerSample {
    alpha: f64,
    micros: Option<f64>,
}

impl PerSample {
    pub fn new(alpha: f64) -> Self {
        PerSample { alpha, micros: None }
    }

    pub fn observe(&mut self, sample: Duration) {
        let sample = sample.as_micros() as f64;
        self.micros = Some(match self.micros {
            None => sample,
            Some(value) => value + self.alpha * (sample - value),
        });
    }

    pub fn value(&self) -> Option<Duration> {
        self.micros.map(|m| Duration::from_micros(m.round() as u64))
    }
}

/// An EWMA whose memory is measured in time, not in samples.
#[allow(dead_code)]
pub struct TimeDecayed {
    half_life: Duration,
    /// `(value in micros, when the last sample landed)`.
    state: Option<(f64, Duration)>,
}

impl TimeDecayed {
    pub fn new(half_life: Duration) -> Self {
        TimeDecayed { half_life, state: None }
    }

    /// Fold in `sample`, observed at `at` (time since any fixed start).
    ///
    /// Contract:
    /// - the first sample becomes the value;
    /// - after that, the sample's weight is `1 - 2^(-dt / half_life)`, where
    ///   `dt` is the time since the previous sample — one half-life of silence
    ///   and the new sample is worth half the value;
    /// - the weight is never below `MIN_WEIGHT`.
    #[allow(unused_variables)]
    pub fn observe(&mut self, sample: Duration, at: Duration) {
        // `PerSample::observe` above is the shape; only the weight changes.
        // `f64::exp2` is `2^x`.
        todo!("fill the blank")
    }

    pub fn value(&self) -> Option<Duration> {
        self.state
            .map(|(m, _)| Duration::from_micros(m.round() as u64))
    }
}

const HALF_LIFE: Duration = Duration::from_secs(5);

/// 1000 calls at 10ms, one every 10ms, then a minute of silence, then 900ms.
fn quiet_then_slow() -> (PerSample, TimeDecayed) {
    let mut per_sample = PerSample::new(0.1);
    let mut decayed = TimeDecayed::new(HALF_LIFE);
    let mut at = Duration::ZERO;
    for _ in 0..1000 {
        at += ms(10);
        per_sample.observe(ms(10));
        decayed.observe(ms(10), at);
    }
    at += Duration::from_secs(60);
    per_sample.observe(ms(900));
    decayed.observe(ms(900), at);
    (per_sample, decayed)
}

fn main() {
    let (per_sample, decayed) = quiet_then_slow();
    println!(
        "after a quiet minute and one 900ms call: per-sample {:?}, time-decayed {:?}",
        per_sample.value().unwrap(),
        decayed.value().unwrap()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_sample_is_the_value() {
        let mut ewma = TimeDecayed::new(HALF_LIFE);
        assert_eq!(ewma.value(), None, "no samples, no opinion");

        ewma.observe(ms(42), Duration::from_secs(7));
        assert_eq!(ewma.value(), Some(ms(42)));
    }

    #[test]
    fn one_half_life_later_a_sample_is_worth_half() {
        let mut ewma = TimeDecayed::new(HALF_LIFE);
        ewma.observe(ms(100), Duration::ZERO);
        ewma.observe(ms(200), HALF_LIFE);

        assert_eq!(ewma.value(), Some(ms(150)), "weight 1 - 2^-1 = 0.5");
    }

    #[test]
    fn silence_is_forgetting() {
        let (per_sample, decayed) = quiet_then_slow();

        assert!(
            per_sample.value().unwrap() < ms(100),
            "GIVEN, the trap: one sample after a quiet minute moves a per-sample \
             EWMA by α — it still believes the 10ms from a minute ago"
        );
        assert!(
            decayed.value().unwrap() > ms(800),
            "twelve half-lives of silence: the old value keeps 2^-12 of its weight"
        );
    }

    #[test]
    fn a_burst_at_one_instant_still_counts() {
        let mut ewma = TimeDecayed::new(HALF_LIFE);
        ewma.observe(ms(10), Duration::ZERO);
        for _ in 0..300 {
            ewma.observe(ms(900), Duration::from_secs(1));
        }

        assert!(
            ewma.value().unwrap() > ms(800),
            "dt = 0 makes the pure weight 0: three hundred calls would count for \
             nothing. That is what `MIN_WEIGHT` is for"
        );
    }
}
