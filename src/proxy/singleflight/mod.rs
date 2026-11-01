mod evict;

use std::sync::Arc;

use futures_util::future::{BoxFuture, FutureExt, Shared};

use super::RequestId;
use crate::jsonrpc::DedupKey;
use evict::{Entry, Evict, FlightIds, Inflight, lock};

pub(super) type Run<T> = BoxFuture<'static, T>;

pub(super) type Flight<T> = Shared<Run<T>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Role {
    Leader,
    Follower { leader: RequestId },
}

pub(super) struct SingleFlight<T> {
    inflight: Inflight<T>,
    ids: FlightIds,
}

impl<T> Default for SingleFlight<T> {
    fn default() -> Self {
        Self {
            inflight: Arc::default(),
            ids: FlightIds::default(),
        }
    }
}

impl<T: Clone + Send + Sync + 'static> SingleFlight<T> {
    pub(super) fn join<F>(
        &self,
        key: DedupKey,
        me: RequestId,
        make: impl FnOnce() -> F + Send + 'static,
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

        let id = self.ids.next();
        let evict = Evict {
            inflight: self.inflight.clone(),
            key,
            id,
        };
        let flight = async move {
            let _evict = evict;
            make().await
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
