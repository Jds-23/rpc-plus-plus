mod evict;

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use futures_util::future::{BoxFuture, FutureExt, Shared};
use uuid::Uuid;

use crate::jsonrpc::DedupKey;
use evict::{Entry, Evict, Inflight, lock};

pub(super) type Run<T> = BoxFuture<'static, T>;

/// One request's run, awaited by every caller that asked the same question.
/// Whoever is still polling drives it; the last drop cancels it.
pub(super) type Flight<T> = Shared<Run<T>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Role {
    Leader,
    Follower { leader: Uuid },
}

pub(super) struct SingleFlight<T> {
    inflight: Inflight<T>,
    next_id: AtomicU64,
}

impl<T> Default for SingleFlight<T> {
    fn default() -> Self {
        Self {
            inflight: Arc::default(),
            next_id: AtomicU64::new(0),
        }
    }
}

impl<T: Clone + Send + Sync + 'static> SingleFlight<T> {
    /// Joins the live flight for `key`, or starts `make()` as a new one led by
    /// `me`. `make` runs only for the leader.
    ///
    /// Not `async`: the lock can never be held across an `.await`.
    pub(super) fn join<F>(
        &self,
        key: DedupKey,
        me: Uuid,
        make: impl FnOnce() -> F,
    ) -> (Flight<T>, Role)
    where
        F: Future<Output = T> + Send + 'static,
    {
        let mut inflight = lock(&self.inflight);
        if let Some(entry) = inflight.get(&key)
            && let Some(flight) = entry.flight.upgrade()
        {
            return (
                flight,
                Role::Follower {
                    leader: entry.leader,
                },
            );
        }

        // None, or a dead entry whose `Evict` has not run yet: start over it.
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let evict = Evict {
            inflight: self.inflight.clone(),
            key,
            id,
        };
        let run = make();
        let flight = async move {
            let _evict = evict;
            run.await
        }
        .boxed()
        .shared();
        let weak = flight
            .downgrade()
            .expect("a flight that was never polled has not completed");
        inflight.insert(
            key,
            Entry {
                id,
                leader: me,
                flight: weak,
            },
        );
        (flight, Role::Leader)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        lock(&self.inflight).len()
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod test;
