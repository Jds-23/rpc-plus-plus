use std::{
    future::{Pending, Ready, pending, ready},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

use axum::body::Bytes;
use tokio::{task::JoinHandle, time::Instant};

use super::{evict::Evict, *};
use crate::jsonrpc::dedup_key;

const LATENCY: Duration = Duration::from_millis(100);

type Answer = Result<&'static str, &'static str>;

/// Stands in for a request's run: counts runs, sleeps `LATENCY`, answers.
#[derive(Default)]
struct Fake {
    entered: AtomicUsize,
    finished: AtomicUsize,
}

impl Fake {
    fn run(self: &Arc<Self>, answer: Answer) -> impl Future<Output = Answer> + use<> {
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
            let (flight, _) = flights.join(key(method), Uuid::new_v4(), {
                let fake = fake.clone();
                move || fake.run(answer)
            });
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
        "any follower still polling drives the run — there is no leader to lose"
    );
    assert_eq!(fake.entered(), 1, "and it is still the one run");
}

#[tokio::test(start_paused = true)]
async fn when_everyone_hangs_up_the_run_and_entry_go_with_them() {
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

#[test]
fn only_the_first_caller_leads() {
    let flights = SingleFlight::<()>::default();
    let leader = Uuid::new_v4();

    let (_first, first) = flights.join(key("eth_chainId"), leader, pending);
    let (_second, second) = flights.join(key("eth_chainId"), Uuid::new_v4(), || -> Pending<()> {
        panic!("a follower must not build a run")
    });

    assert_eq!(first, Role::Leader);
    assert_eq!(
        second,
        Role::Follower { leader },
        "a follower learns whose flight it joined"
    );
}

#[tokio::test]
async fn make_runs_outside_the_lock() {
    let flights = SingleFlight::<bool>::default();
    let inflight = flights.inflight.clone();

    let (flight, _) = flights.join(key("eth_blockNumber"), Uuid::new_v4(), move || {
        ready(inflight.try_lock().is_ok())
    });

    assert!(
        flight.await,
        "caller code must not run while the map is locked"
    );
}

#[test]
fn a_panicking_make_does_not_wedge_the_map() {
    let flights = Arc::new(SingleFlight::<()>::default());
    let (tx, rx) = mpsc::channel::<()>();

    thread::spawn({
        let flights = flights.clone();
        move || {
            let _tx = tx;
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(async {
                    let (flight, _) =
                        flights.join(key("eth_blockNumber"), Uuid::new_v4(), || -> Ready<()> {
                            panic!("make blew up")
                        });
                    flight.await;
                });
        }
    });

    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1)),
        Err(mpsc::RecvTimeoutError::Disconnected),
        "a panic in caller code must unwind, not deadlock on the map lock"
    );
    assert!(flights.is_empty(), "the panicked flight leaves no entry");
    let (_, role) = flights.join(key("eth_blockNumber"), Uuid::new_v4(), pending);
    assert_eq!(role, Role::Leader, "the key is usable again");
}

#[test]
fn each_key_gets_its_own_leader() {
    let flights = SingleFlight::<()>::default();

    let (_a, a) = flights.join(key("eth_blockNumber"), Uuid::new_v4(), pending);
    let (_b, b) = flights.join(key("eth_chainId"), Uuid::new_v4(), pending);

    assert_eq!(
        (a, b),
        (Role::Leader, Role::Leader),
        "different questions, different flights"
    );
}

#[test]
fn a_stale_evict_spares_its_replacement() {
    let flights = SingleFlight::<()>::default();
    let (_live, _) = flights.join(key("eth_blockNumber"), Uuid::new_v4(), pending);

    drop(Evict {
        inflight: flights.inflight.clone(),
        key: key("eth_blockNumber"),
        id: flights.ids.next(),
    });

    assert_eq!(
        flights.len(),
        1,
        "a late drop of an older flight evicts nothing"
    );
    let (_, role) = flights.join(key("eth_blockNumber"), Uuid::new_v4(), pending);
    assert!(
        matches!(role, Role::Follower { .. }),
        "the live flight is still joinable"
    );
}
