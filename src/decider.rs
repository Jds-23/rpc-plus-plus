use std::sync::Arc;

use crate::upstream::{Upstream, UpstreamId};

pub mod least_latency;
pub mod prefer_least_errors;
pub mod round_robin;

pub trait Decider: Send + Sync + 'static {
    /// Returns up to `max` distinct upstreams, best first.
    /// Empty means no upstream is available. `max == 0` returns empty.
    fn decide(&self, max: usize) -> Vec<Arc<Upstream>>;

    fn upstream_len(&self) -> usize;
}

/// The order `decide` hands out, best first.
pub struct Ranking {
    upstreams: Vec<Arc<Upstream>>,
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("upstream must not be empty")]
    EmptyUpstreams,
    #[error("upstream {0} is not tracked by the observer")]
    UnknownUpstream(UpstreamId),
    #[error("the minimum window is 1, given 0")]
    ZeroWindow,
}

#[cfg(test)]
fn upstream(label: &str) -> Upstream {
    Upstream::builder()
        .label(label)
        .url(format!("http://{label}.invalid"))
        .http(reqwest::Client::new())
        .build()
        .expect("upstream build failed")
}
