use std::sync::Arc;

use crate::rpc_handler::RpcHandler;

pub mod round_robin;

pub trait Decider: Send + Sync + 'static {
    /// Returns up to `max` distinct upstreams, best first.
    /// Empty means no upstream is available. `max == 0` returns empty.
    fn decide(&self, max: usize) -> Vec<Arc<RpcHandler>>;
}
