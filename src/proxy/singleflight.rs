use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
};

use futures_util::future::{BoxFuture, FutureExt, Shared, WeakShared};

use crate::proxy::dedup_key::DedupKey;

pub type Call<T> = BoxFuture<'static, T>;

/// One request's run, awaited by every caller that asked the same question.
/// Whoever is still polling drives it; the last drop cancels it.
pub type Flight<T> = Shared<Call<T>>;

/// Weak on purpose: the map alone must never keep a call alive. The `u64` is
/// which flight the entry is, for `Evict`.
type Entries<T> = HashMap<DedupKey, (u64, WeakShared<Call<T>>)>;
type Inflight<T> = Arc<Mutex<Entries<T>>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Started the flight.
    Leader,
    /// Joined a flight already in the air.
    Follower,
}

pub struct SingleFlight<T> {
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
    /// Joins the live flight for `key`, or starts `make()` as a new one.
    /// `make` runs only for the leader.
    ///
    /// Not `async`: the lock can never be held across an `.await`.
    pub fn join<F>(&self, key: DedupKey, make: impl FnOnce() -> F) -> (Flight<T>, Role)
    where
        F: Future<Output = T> + Send + 'static,
    {
        let mut inflight = lock(&self.inflight);
        if let Some(flight) = inflight.get(&key).and_then(|(_, weak)| weak.upgrade()) {
            return (flight, Role::Follower);
        }

        // None, or a dead entry whose `Evict` has not run yet: start over it.
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let evict = Evict {
            inflight: self.inflight.clone(),
            key,
            id,
        };
        let call = make();
        let flight = async move {
            let _evict = evict;
            call.await
        }
        .boxed()
        .shared();
        let weak = flight
            .downgrade()
            .expect("a flight that was never polled has not completed");
        inflight.insert(key, (id, weak));
        (flight, Role::Leader)
    }

    /// Flights in the map.
    pub fn len(&self) -> usize {
        lock(&self.inflight).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

struct Evict<T> {
    inflight: Inflight<T>,
    key: DedupKey,
    id: u64,
}

impl<T> Drop for Evict<T> {
    fn drop(&mut self) {
        let mut inflight = lock(&self.inflight);
        if inflight
            .get(&self.key)
            .is_some_and(|(id, _)| *id == self.id)
        {
            inflight.remove(&self.key);
        }
    }
}

fn lock<T>(inflight: &Inflight<T>) -> MutexGuard<'_, Entries<T>> {
    inflight.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use std::{sync::atomic::AtomicUsize, time::Duration};

    use axum::body::Bytes;
    use tokio::{task::JoinHandle, time::Instant};

    use super::*;
    use crate::proxy::dedup_key::dedup_key;

    const LATENCY: Duration = Duration::from_millis(100);

    type Answer = Result<&'static str, &'static str>;

    /// Stands in for a request's run: counts calls, sleeps `LATENCY`, answers.
    #[derive(Default)]
    struct Fake {
        entered: AtomicUsize,
        finished: AtomicUsize,
    }

    impl Fake {
        fn call(self: &Arc<Self>, answer: Answer) -> impl Future<Output = Answer> + 'static {
            let fake = self.clone();
            async move {
                fake.entered.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(LATENCY).await;
                fake.finished.fetch_add(1, Ordering::SeqCst);
                answer
            }
        }

        fn entered(&self) -> usize {
            self.entered.load(Ordering::SeqCst)
        }

        fn finished(&self) -> usize {
            self.finished.load(Ordering::SeqCst)
        }
    }

    fn key(method: &str) -> DedupKey {
        dedup_key(&Bytes::from(format!(r#"{{"method":"{method}"}}"#))).unwrap()
    }

    fn callers(
        n: usize,
        method: &'static str,
        flights: &Arc<SingleFlight<Answer>>,
        fake: &Arc<Fake>,
        answer: Answer,
    ) -> Vec<JoinHandle<Answer>> {
        (0..n)
            .map(|_| {
                let (flight, _) = flights.join(key(method), || fake.call(answer));
                tokio::spawn(flight)
            })
            .collect()
    }

    async fn answers(callers: Vec<JoinHandle<Answer>>) -> Vec<Answer> {
        let mut out = Vec::with_capacity(callers.len());
        for caller in callers {
            out.push(caller.await.expect("caller panicked"));
        }
        out
    }

    #[tokio::test(start_paused = true)]
    async fn twenty_callers_make_one_call() {
        let (flights, fake) = (Arc::default(), Arc::<Fake>::default());
        let started = Instant::now();

        let out = answers(callers(20, "eth_blockNumber", &flights, &fake, Ok("0x10"))).await;

        assert_eq!(
            fake.entered(),
            1,
            "20 identical requests, one upstream call"
        );
        assert!(
            out.iter().all(|a| *a == Ok("0x10")),
            "everyone gets the answer"
        );
        assert_eq!(
            started.elapsed(),
            LATENCY,
            "the 20th caller waits for the one call, not for 20 of them"
        );
    }

    #[tokio::test]
    async fn only_the_first_caller_leads() {
        let (flights, fake) = (SingleFlight::<Answer>::default(), Arc::<Fake>::default());

        let (_first, first) = flights.join(key("eth_chainId"), || fake.call(Ok("0x1")));
        let (_second, second) = flights
            .join(key("eth_chainId"), || -> BoxFuture<'static, Answer> {
                panic!("a follower must not build a call")
            });

        assert_eq!((first, second), (Role::Leader, Role::Follower));
    }

    #[tokio::test(start_paused = true)]
    async fn different_keys_do_not_coalesce() {
        let (flights, fake) = (Arc::default(), Arc::<Fake>::default());

        let mut all = callers(5, "eth_blockNumber", &flights, &fake, Ok("block"));
        all.extend(callers(5, "eth_chainId", &flights, &fake, Ok("chain")));
        answers(all).await;

        assert_eq!(fake.entered(), 2, "one call per key, not one call total");
    }

    #[tokio::test(start_paused = true)]
    async fn a_finished_flight_leaves_the_map() {
        let (flights, fake) = (Arc::default(), Arc::<Fake>::default());

        answers(callers(3, "eth_blockNumber", &flights, &fake, Ok("0x10"))).await;

        assert!(
            flights.is_empty(),
            "one entry per distinct request, forever, is a leak"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn dedup_is_not_a_cache() {
        let (flights, fake) = (Arc::default(), Arc::<Fake>::default());

        answers(callers(1, "eth_blockNumber", &flights, &fake, Ok("0x10"))).await;
        tokio::time::sleep(Duration::from_secs(12)).await;
        answers(callers(1, "eth_blockNumber", &flights, &fake, Ok("0x11"))).await;

        assert_eq!(
            fake.entered(),
            2,
            "a block later, a finished flight must not serve the old answer"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_failure_is_shared_but_not_remembered() {
        let (flights, fake) = (Arc::default(), Arc::<Fake>::default());

        let first = answers(callers(5, "eth_call", &flights, &fake, Err("down"))).await;
        assert!(
            first.iter().all(|a| *a == Err("down")),
            "the bad news is coalesced too"
        );
        assert_eq!(fake.entered(), 1);
        assert!(flights.is_empty(), "the error path cleans up too");

        answers(callers(1, "eth_call", &flights, &fake, Err("down"))).await;
        assert_eq!(
            fake.entered(),
            2,
            "the next caller retries; it does not inherit the failure"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_leader_hanging_up_strands_no_one() {
        let (flights, fake) = (Arc::default(), Arc::<Fake>::default());

        let mut all = callers(4, "eth_blockNumber", &flights, &fake, Ok("0x10"));
        let leader = all.remove(0);
        tokio::time::sleep(LATENCY / 2).await;
        leader.abort();

        let out = answers(all).await;
        assert!(
            out.iter().all(|a| *a == Ok("0x10")),
            "any follower still polling drives the call — there is no leader to lose"
        );
        assert_eq!(fake.entered(), 1, "and it is still the one call");
    }

    #[tokio::test(start_paused = true)]
    async fn when_everyone_hangs_up_the_call_and_entry_go_with_them() {
        let (flights, fake) = (Arc::default(), Arc::<Fake>::default());

        let hung_up = callers(5, "eth_blockNumber", &flights, &fake, Ok("0x10"));
        tokio::time::sleep(LATENCY / 2).await;
        for caller in &hung_up {
            caller.abort();
        }
        tokio::time::sleep(LATENCY * 2).await;

        assert_eq!(fake.finished(), 0, "nobody was left to want the answer");
        assert!(flights.is_empty(), "RAII covers the cancel path");

        answers(callers(1, "eth_blockNumber", &flights, &fake, Ok("0x10"))).await;
        assert_eq!(
            fake.entered(),
            2,
            "the next caller starts fresh, not a zombie flight"
        );
    }

    #[tokio::test]
    async fn a_stale_evict_spares_its_replacement() {
        let (flights, fake) = (SingleFlight::<Answer>::default(), Arc::<Fake>::default());
        let (_live, _) = flights.join(key("eth_blockNumber"), || fake.call(Ok("0x10")));

        drop(Evict {
            inflight: flights.inflight.clone(),
            key: key("eth_blockNumber"),
            id: u64::MAX,
        });

        assert_eq!(
            flights.len(),
            1,
            "a late drop of an older flight evicts nothing"
        );
        let (_, role) = flights.join(key("eth_blockNumber"), || fake.call(Ok("0x10")));
        assert_eq!(role, Role::Follower, "the live flight is still joinable");
    }
}
