use std::sync::Arc;

use futures_util::stream::{FuturesUnordered, StreamExt};
use tracing::info;

use crate::{
    observer::snapshot::HedgeSnapshot,
    proxy::Failure,
    upstream::{Upstream, call::CallError},
};

/// Which call the racer is starting, and why.
pub(super) struct Start<'a> {
    pub index: usize,
    /// `Some` when the timer started it: the upstream that ran past its
    /// `hedge_after`, always the one just before it in the chain, since the
    /// timer only ever waits on the latest start. `None` for the first call and
    /// for a retry pulled in by a failure.
    pub overtaken: Option<&'a Upstream>,
}

pub(super) struct Won<T> {
    pub index: usize,
    pub answer: T,
}

/// What a race did, not just what it answered.
pub(super) struct Raced<T> {
    pub result: Result<Won<T>, Failure>,
    /// Calls started, hedges included. Always a prefix of the chain.
    pub started: usize,
    /// For this one race: `won` is 1 when a hedge answered first, else 0.
    pub hedge: HedgeSnapshot,
}

/// Races `chain`, staggered by each upstream's `hedge_after`.
///
/// The first `Ok` wins and everything still in flight is dropped. A retryable
/// failure starts the next upstream at once; a final one ends the race. When
/// the latest call has not answered within its `hedge_after`, the next one is
/// started beside it — only that counts as a hedge. Whether a request may be
/// raced at all is the caller's call; this always races.
///
/// Polled on the caller's task: the calls borrow the chain and never need
/// `'static`.
pub(super) async fn race<'a, T, F, Fut>(
    chain: &'a [Arc<Upstream>],
    max_attempt: usize,
    mut call: F,
) -> Raced<T>
where
    F: FnMut(&'a Upstream, Start<'a>) -> Fut,
    Fut: Future<Output = Result<T, CallError>>,
{
    let limit = chain.len().min(max_attempt);
    let launch = |call: &mut F, index: usize, hedge: bool| {
        let start = Start {
            index,
            overtaken: hedge.then(|| chain[index - 1].as_ref()),
        };
        let pending = call(&chain[index], start);
        async move { (index, hedge, pending.await) }
    };

    let mut inflight = FuturesUnordered::new();
    let mut next = 0;
    let mut hedge = HedgeSnapshot { started: 0, won: 0 };
    let mut last_failure = None;

    if limit > 0 {
        inflight.push(launch(&mut call, 0, false));
        next = 1;
    }

    loop {
        let more = next < limit;
        if inflight.is_empty() && !more {
            return Raced {
                result: Err(last_failure.map_or(Failure::NoUpstream, Failure::Call)),
                started: next,
                hedge,
            };
        }
        // Rebuilt every pass on purpose: patience runs from the latest start,
        // so a failure that pulls the next upstream in also restarts the clock.
        let hedge_after = chain[next - 1].hedge_after();

        tokio::select! {
            biased;
            Some((index, hedged, result)) = inflight.next() => match result {
                Ok(answer) => {
                    hedge.won = u64::from(hedged);
                    return Raced {
                        result: Ok(Won { index, answer }),
                        started: next,
                        hedge,
                    };
                }
                Err(failure) if !failure.is_retryable() => {
                    return Raced {
                        result: Err(Failure::Call(failure)),
                        started: next,
                        hedge,
                    };
                }
                Err(failure) => {
                    last_failure = Some(failure);
                    if more {
                        inflight.push(launch(&mut call, next, false));
                        next += 1;
                    }
                }
            },
            _ = tokio::time::sleep(hedge_after), if more => {
                info!(
                    event = "hedge_started",
                    attempt = next + 1,
                    upstream = %chain[next].id(),
                    after_ms = hedge_after.as_millis() as u64,
                );
                inflight.push(launch(&mut call, next, true));
                next += 1;
                hedge.started += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::Cell, time::Duration};

    use reqwest::{Client, StatusCode};
    use tokio::time::Instant;

    use super::*;

    #[derive(Clone, Copy)]
    enum Verdict {
        Succeeds,
        /// Retryable: the next upstream may do better.
        Fails,
        /// Not retryable: every upstream would answer the same.
        Final,
    }

    struct Fake {
        name: &'static str,
        latency: Duration,
        verdict: Verdict,
        entered: Cell<usize>,
        finished: Cell<usize>,
    }

    impl Fake {
        async fn call(&self) -> Result<&'static str, CallError> {
            self.entered.set(self.entered.get() + 1);
            tokio::time::sleep(self.latency).await;
            self.finished.set(self.finished.get() + 1);
            match self.verdict {
                Verdict::Succeeds => Ok(self.name),
                Verdict::Fails => Err(CallError::Unreachable {
                    error: self.name.to_string(),
                }),
                Verdict::Final => Err(CallError::RpcError {
                    http_status: StatusCode::OK,
                    code: 3,
                    retryable: false,
                }),
            }
        }
    }

    const NAMES: [&str; 3] = ["first", "second", "third"];

    /// One upstream per lane, `(latency_ms, verdict)`, all sharing `hedge_after`.
    /// The upstreams are never called; `Fake` stands in for the HTTP.
    fn chain(lanes: &[(u64, Verdict)], hedge_after: u64) -> (Vec<Arc<Upstream>>, Vec<Fake>) {
        lanes
            .iter()
            .zip(NAMES)
            .map(|(&(latency, verdict), name)| {
                let upstream = Upstream::builder()
                    .label(name)
                    .url(format!("http://{name}.invalid"))
                    .http(Client::new())
                    .hedge_after_in_millis(hedge_after)
                    .build()
                    .expect("upstream build failed");
                let fake = Fake {
                    name,
                    latency: Duration::from_millis(latency),
                    verdict,
                    entered: Cell::new(0),
                    finished: Cell::new(0),
                };
                (Arc::new(upstream), fake)
            })
            .unzip()
    }

    async fn run(
        chain: &[Arc<Upstream>],
        fakes: &[Fake],
        max_attempt: usize,
    ) -> Raced<&'static str> {
        race(chain, max_attempt, |upstream, _| {
            let fake = fakes
                .iter()
                .find(|fake| fake.name == upstream.id().as_str());
            fake.expect("every upstream has a fake").call()
        })
        .await
    }

    fn answer(raced: &Raced<&'static str>) -> Result<&'static str, String> {
        raced
            .result
            .as_ref()
            .map(|won| won.answer)
            .map_err(ToString::to_string)
    }

    #[tokio::test(start_paused = true)]
    async fn a_fast_first_upstream_never_hedges() {
        let (chain, fakes) = chain(&[(10, Verdict::Succeeds), (10, Verdict::Succeeds)], 50);

        let out = run(&chain, &fakes, 3).await;

        assert_eq!(answer(&out), Ok("first"), "the original answered in time");
        assert_eq!(out.hedge.started, 0, "the hedge timer never fired");
        assert_eq!(
            fakes[1].entered.get(),
            0,
            "hedging must cost nothing when the first upstream is healthy"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_first_upstream_is_overtaken() {
        let (chain, fakes) = chain(&[(200, Verdict::Succeeds), (20, Verdict::Succeeds)], 50);
        let started = Instant::now();

        let out = run(&chain, &fakes, 3).await;

        assert_eq!(answer(&out), Ok("second"), "the hedge answered first");
        assert_eq!(out.hedge.started, 1, "exactly one extra call was started");
        assert_eq!(out.hedge.won, 1, "and it was the hedge that won");
        assert_eq!(
            started.elapsed(),
            Duration::from_millis(70),
            "50ms of patience, then a 20ms call — not the 200ms the original needed"
        );
        assert_eq!(fakes[0].entered.get(), 1, "the original was started");
        assert_eq!(
            fakes[0].finished.get(),
            0,
            "and dropped mid-flight when the hedge won"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_failure_pulls_the_next_upstream_in_early() {
        let (chain, fakes) = chain(&[(5, Verdict::Fails), (10, Verdict::Succeeds)], 50);
        let started = Instant::now();

        let out = run(&chain, &fakes, 3).await;

        assert_eq!(answer(&out), Ok("second"), "the retry answered");
        assert_eq!(
            out.hedge.started, 0,
            "a call pulled in by a failure is a retry, not a hedge — the timer never fired"
        );
        assert_eq!(out.hedge.won, 0, "a retry winning is not a hedge winning");
        assert_eq!(
            started.elapsed(),
            Duration::from_millis(15),
            "no waiting for hedge_after once the first upstream has already failed"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn every_upstream_failing_returns_the_last_error() {
        let (chain, fakes) = chain(&[(5, Verdict::Fails), (5, Verdict::Fails)], 50);

        let out = run(&chain, &fakes, 3).await;

        assert_eq!(
            answer(&out),
            Err("second".to_string()),
            "the chain is exhausted"
        );
        assert_eq!(out.started, 2);
        assert_eq!(out.hedge.started, 0, "both were retries");
    }

    #[tokio::test(start_paused = true)]
    async fn max_attempt_caps_the_calls_hedges_included() {
        let (chain, fakes) = chain(
            &[
                (500, Verdict::Succeeds),
                (500, Verdict::Succeeds),
                (10, Verdict::Succeeds),
            ],
            50,
        );

        let out = run(&chain, &fakes, 2).await;

        assert_eq!(
            answer(&out),
            Ok("first"),
            "nothing faster was allowed to start"
        );
        assert_eq!(out.started, 2);
        assert_eq!(out.hedge.started, 1);
        assert_eq!(
            fakes[2].entered.get(),
            0,
            "the third call would break the cap"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_final_error_ends_the_race() {
        let (chain, fakes) = chain(&[(5, Verdict::Final), (10, Verdict::Succeeds)], 50);
        let started = Instant::now();

        let out = run(&chain, &fakes, 3).await;

        assert_eq!(
            answer(&out),
            Err("upstream returned rpc error code 3".to_string()),
            "a revert is the answer every upstream would give"
        );
        assert_eq!(started.elapsed(), Duration::from_millis(5));
        assert_eq!(fakes[1].entered.get(), 0, "nothing else was started");
    }

    #[tokio::test(start_paused = true)]
    async fn an_empty_chain_starts_nothing() {
        let out = run(&[], &[], 3).await;

        assert!(matches!(out.result, Err(Failure::NoUpstream)));
        assert_eq!(out.started, 0);
    }
}
