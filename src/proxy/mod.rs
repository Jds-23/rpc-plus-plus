pub mod attempt;
mod hedge;
mod reply;
mod singleflight;

use axum::{
    body::Bytes,
    response::{IntoResponse, Response},
};
use reqwest::StatusCode;
use std::{
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::{Instrument, Span, error, info, info_span, warn};
use uuid::Uuid;

use crate::{
    decider::Decider,
    jsonrpc::{JSONRPC_INTERNAL_ERROR, Shape, dedup_key, is_write, readdress, request_id, shape},
    observer::{Observer, snapshot::HedgeSnapshot},
    proxy::{
        attempt::try_once,
        hedge::{Won, race},
        reply::Reply,
        singleflight::{Role, SingleFlight},
    },
    upstream::{Upstream, UpstreamId, call::CallError},
};

const DEFAULT_MAX_ATTEMPT: u64 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RequestId(Uuid);

impl RequestId {
    fn random() -> Self {
        RequestId(Uuid::new_v4())
    }
}

impl fmt::Display for RequestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(1);

pub struct Pipeline {
    core: Arc<Core>,
    flights: Option<SingleFlight<Reply>>,
}

/// The pipeline's state, shareable so a request's run can own it (`'static`).
struct Core {
    observer: Arc<dyn Observer>,
    decider: Arc<dyn Decider>,
    max_attempt: usize,
    retry_after: Duration,
    dispatch: Dispatch,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Dispatch {
    #[default]
    Sequential,
    Hedged,
}

/// How a request ended, whichever path ran it.
struct Finished<'c> {
    result: Result<Answered<'c>, Failure>,
    tried: Vec<&'c UpstreamId>,
    /// For this one request: `won` is 1 when a hedge answered first, else 0.
    /// `None` under `Dispatch::Sequential`.
    hedge: Option<HedgeSnapshot>,
}

struct Answered<'c> {
    reply: Reply,
    upstream: &'c UpstreamId,
}

#[derive(Debug, thiserror::Error)]
enum Failure {
    #[error("no upstream available")]
    NoUpstream,
    #[error(transparent)]
    Call(CallError),
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("max_attempt must be at least 1")]
    ZeroMaxAttempt,
}

#[bon::bon]
impl Pipeline {
    #[builder]
    pub fn new(
        decider: Arc<dyn Decider>,
        observer: Arc<dyn Observer>,
        #[builder(default = DEFAULT_MAX_ATTEMPT)] max_attempt: u64,
        #[builder(default = DEFAULT_RETRY_AFTER)] retry_after: Duration,
        #[builder(default)] dispatch: Dispatch,
        #[builder(default)] dedup: bool,
    ) -> Result<Self, BuildError> {
        if max_attempt == 0 {
            return Err(BuildError::ZeroMaxAttempt);
        }
        Ok(Self {
            core: Arc::new(Core {
                decider,
                observer,
                max_attempt: max_attempt as usize,
                retry_after,
                dispatch,
            }),
            flights: dedup.then(SingleFlight::default),
        })
    }
}

impl Pipeline {
    pub async fn proxy(&self, body: Bytes) -> Response {
        let request_id = RequestId::random();
        let span = info_span!("proxy", %request_id);
        self.proxy_inner(body, request_id).instrument(span).await
    }

    async fn proxy_inner(&self, body: Bytes, request_id: Uuid) -> Response {
        let received_at = Instant::now();
        info!(event = "request_received", body_bytes = body.len());
        match shape(&body) {
            Shape::Batch => {
                warn!(event = "batch_rejected", body_bytes = body.len());
                StatusCode::BAD_REQUEST.into_response()
            }
            Shape::Single => {
                let reply = match &self.flights {
                    Some(flights) => self.coalesce(flights, request_id, body, received_at).await,
                    None => self.start(body, received_at).await,
                };
                reply.into_response()
            }
            Shape::Malformed => {
                warn!(event = "batch_malformed", body_bytes = body.len());
                StatusCode::BAD_REQUEST.into_response()
            }
        }
    }

    /// The chain is decided here, so a follower never advances the decider.
    fn start(
        &self,
        body: Bytes,
        received_at: Instant,
    ) -> impl Future<Output = Reply> + Send + use<> {
        let chain = self.core.decider.decide(self.core.max_attempt);
        run(self.core.clone(), chain, body, received_at)
    }

    /// Joins the flight for this request's question, or leads it. A write, an
    /// unknown method or a notification runs on its own.
    async fn coalesce(
        &self,
        flights: &SingleFlight<Reply>,
        me: Uuid,
        body: Bytes,
        received_at: Instant,
    ) -> Reply {
        let (Some(key), Some(id)) = (dedup_key(&body), request_id(&body)) else {
            return self.start(body, received_at).await;
        };

        // Instrumented here, so the flight logs under the leader's span
        // whichever caller ends up polling it.
        let (flight, role) = flights.join(key, me, || {
            self.start(body.clone(), received_at)
                .instrument(Span::current())
        });

        match role {
            // The upstream answered the leader's own body: its id is already right.
            Role::Leader => flight.await,
            Role::Follower { leader } => {
                info!(event = "request_coalesced", leader_request_id = %leader);
                let reply = flight.await;
                match readdress(&reply.body, &id) {
                    Some(body) => Reply { body, ..reply },
                    None => {
                        warn!(event = "readdress_failed", leader_request_id = %leader);
                        self.start(body, received_at).await
                    }
                }
            }
        }
    }
}

/// One request, start to finish. Owns everything it touches, so it can run as
/// a flight that outlives the caller who started it.
async fn run(
    core: Arc<Core>,
    chain: Vec<Arc<Upstream>>,
    body: Bytes,
    received_at: Instant,
) -> Reply {
    let finished = match (core.dispatch, is_write(&body)) {
        (Dispatch::Hedged, false) => core.race(&chain, &body).await,
        (Dispatch::Hedged, true) => core.send_once(&chain, &body).await,
        (Dispatch::Sequential, _) => core.walk(&chain, &body).await,
    };
    finish(finished, received_at)
}

impl Core {
    /// One upstream at a time, `retry_after` between them.
    async fn walk<'c>(&self, chain: &'c [Arc<Upstream>], body: &Bytes) -> Finished<'c> {
        let mut tried: Vec<&UpstreamId> = Vec::with_capacity(chain.len());

        let mut last_failure: Option<CallError> = None;

        for upstream in chain {
            if tried.len() >= self.max_attempt {
                break;
            }
            if !tried.is_empty() {
                tokio::time::sleep(self.retry_after).await;
            }
            tried.push(upstream.id());

            match try_once(
                self.observer.as_ref(),
                upstream,
                body,
                tried.len() as u64,
                false,
            )
            .await
            {
                Ok(reply) => {
                    return Finished {
                        result: Ok(Answered {
                            reply,
                            upstream: upstream.id(),
                        }),
                        tried,
                        hedge: None,
                    };
                }
                Err(failure) => {
                    let retryable = failure.is_retryable();
                    last_failure = Some(failure);
                    if !retryable {
                        break;
                    }
                }
            }
        }

        Finished {
            result: Err(last_failure.map_or(Failure::NoUpstream, Failure::Call)),
            tried,
            hedge: None,
        }
    }

    /// A write under `Dispatch::Hedged`: the head of the chain and nothing else.
    /// The first send may land even when it reports failure, so a second call
    /// risks a double send.
    async fn send_once<'c>(&self, chain: &'c [Arc<Upstream>], body: &Bytes) -> Finished<'c> {
        let no_hedge = HedgeSnapshot { started: 0, won: 0 };
        let Some(upstream) = chain.first() else {
            return Finished {
                result: Err(Failure::NoUpstream),
                tried: Vec::new(),
                hedge: Some(no_hedge),
            };
        };
        let result = try_once(self.observer.as_ref(), upstream, body, 1, false).await;

        Finished {
            result: result
                .map(|reply| Answered {
                    reply,
                    upstream: upstream.id(),
                })
                .map_err(Failure::Call),
            tried: vec![upstream.id()],
            hedge: Some(no_hedge),
        }
    }

    /// Staggered race over the chain. Reads only; writes go to `send_once`.
    async fn race<'c>(&self, chain: &'c [Arc<Upstream>], body: &Bytes) -> Finished<'c> {
        let raced = race(chain, self.max_attempt, |upstream, start| {
            if let Some(overtaken) = start.overtaken {
                self.observer.record_hedge(overtaken.id(), upstream.id());
            }
            try_once(
                self.observer.as_ref(),
                upstream,
                body,
                start.index as u64 + 1,
                start.overtaken.is_some(),
            )
        })
        .await;
        // A hedge always overtakes the upstream just before it (see `Start`).
        if let (1, Ok(Won { index, .. })) = (raced.hedge.won, &raced.result) {
            self.observer
                .record_hedge_win(chain[*index - 1].id(), chain[*index].id());
        }

        Finished {
            result: raced.result.map(|Won { index, answer }| Answered {
                reply: answer,
                upstream: chain[index].id(),
            }),
            tried: chain[..raced.started].iter().map(|u| u.id()).collect(),
            hedge: Some(raced.hedge),
        }
    }
}

fn finish(finished: Finished<'_>, received_at: Instant) -> Reply {
    let Finished {
        result,
        tried,
        hedge,
    } = finished;
    let hedges = hedge.map(|hedge| hedge.started);
    let hedge_won = hedge.map(|hedge| hedge.won > 0);

    match result {
        Ok(Answered { reply, upstream }) => {
            info!(
                event = "request_completed",
                attempts = tried.len(),
                upstream = %upstream,
                duration_ms = elapsed_ms(received_at),
                hedges,
                hedge_won,
            );
            reply
        }
        Err(failure) => {
            let error = failure.to_string();

            error!(
                event = "retries_exhausted",
                attempts = tried.len(),
                tried = ?tried,
                duration_ms = elapsed_ms(received_at),
                error = %error,
            );
            Reply::rpc_error(JSONRPC_INTERNAL_ERROR, &error)
        }
    }
}

fn elapsed_ms(since: Instant) -> u64 {
    since.elapsed().as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn static_send<F: Future + Send + 'static>(_: F) {}

    // Never called: it compiles only while `run` is `Send + 'static`.
    #[allow(dead_code)]
    fn run_can_be_a_flight(core: Arc<Core>) {
        static_send(run(core, Vec::new(), Bytes::new(), Instant::now()));
    }
}
