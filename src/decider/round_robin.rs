use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::{decider::Decider, upstream::Upstream};

pub struct RoundRobin {
    upstreams: Vec<Arc<Upstream>>,
    next: AtomicUsize,
}

impl Decider for RoundRobin {
    fn decide(&self, max: usize) -> Vec<Arc<Upstream>> {
        if self.upstreams.is_empty() || max == 0 {
            return Vec::new();
        }
        let start = self.next.fetch_add(1, Ordering::Relaxed) % self.upstreams.len();
        let (head, tail) = self.upstreams.split_at(start);
        tail.iter().chain(head).take(max).cloned().collect()
    }

    fn upstream_len(&self) -> usize {
        self.upstreams.len()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("upstreams must not be empty")]
    EmptyUpstreams,
}

impl RoundRobin {
    pub fn new(upstreams: impl IntoIterator<Item = Upstream>) -> Result<Self, BuildError> {
        let upstreams: Vec<Arc<Upstream>> = upstreams.into_iter().map(Arc::new).collect();
        if upstreams.is_empty() {
            return Err(BuildError::EmptyUpstreams);
        }
        Ok(RoundRobin {
            upstreams,
            next: AtomicUsize::new(0),
        })
    }
}
