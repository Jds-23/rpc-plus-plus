use std::{
    collections::HashMap,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use futures_util::future::WeakShared;
use uuid::Uuid;

use super::Run;
use crate::jsonrpc::DedupKey;

pub(super) type Entries<T> = HashMap<DedupKey, Entry<T>>;
pub(super) type Inflight<T> = Arc<Mutex<Entries<T>>>;

pub(super) struct Entry<T> {
    /// Which flight this is, for `Evict`.
    pub(super) id: u64,
    pub(super) leader: Uuid,
    /// Weak on purpose: the map alone must never keep a run alive.
    pub(super) flight: WeakShared<Run<T>>,
}

/// Rides inside the flight, so the entry leaves the map however the flight
/// ends: answered, failed, or dropped by every caller.
pub(super) struct Evict<T> {
    pub(super) inflight: Inflight<T>,
    pub(super) key: DedupKey,
    pub(super) id: u64,
}

impl<T> Drop for Evict<T> {
    fn drop(&mut self) {
        let mut inflight = lock(&self.inflight);
        if inflight
            .get(&self.key)
            .is_some_and(|entry| entry.id == self.id)
        {
            inflight.remove(&self.key);
        }
    }
}

pub(super) fn lock<T>(inflight: &Inflight<T>) -> MutexGuard<'_, Entries<T>> {
    inflight.lock().unwrap_or_else(PoisonError::into_inner)
}
