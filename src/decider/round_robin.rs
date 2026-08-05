use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use crate::{decider::Decider, rpc_handler::RpcHandler};

pub struct RoundRobin {
    items: Vec<Arc<RpcHandler>>,
    next: AtomicUsize,
}

impl Decider for RoundRobin {
    fn decide(&self, max: usize) -> Vec<Arc<RpcHandler>> {
        if self.items.is_empty() || max == 0 {
            return Vec::new();
        }
        let start = self.next.fetch_add(1, Ordering::Relaxed) % self.items.len();
        let (head, tail) = self.items.split_at(start);
        tail.iter().chain(head).take(max).cloned().collect()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RoundRobinBuildError {
    #[error("handlers is empty")]
    EmptyHandlers,
}

impl RoundRobin {
    pub fn new(
        handlers: impl IntoIterator<Item = RpcHandler>,
    ) -> Result<Self, RoundRobinBuildError> {
        let handlers: Vec<Arc<RpcHandler>> = handlers.into_iter().map(Arc::new).collect();
        if handlers.is_empty() {
            return Err(RoundRobinBuildError::EmptyHandlers);
        }
        Ok(RoundRobin {
            items: handlers,
            next: AtomicUsize::new(0),
        })
    }
}
