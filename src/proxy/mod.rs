pub mod attempt;
mod hedge;

use axum::{
    body::Bytes,
    response::{IntoResponse, Response},
};
use reqwest::StatusCode;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::{Instrument, error, info, info_span, warn};
use uuid::Uuid;

use crate::{
    decider::Decider,
    jsonrpc::{JSONRPC_INTERNAL_ERROR, Shape, is_write, rpc_error, shape},
    observer::{Observer, snapshot::HedgeSnapshot},
    proxy::{
        attempt::try_once,
        hedge::{Won, race},
    },
    upstream::{Upstream, UpstreamId, call::CallError},
};

const DEFAULT_MAX_ATTEMPT: u64 = 3;
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(1);

pub struct Pipeline {
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
    response: Response,
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
    ) -> Result<Self, BuildError> {
        if max_attempt == 0 {
            return Err(BuildError::ZeroMaxAttempt);
        }
        Ok(Self {
            decider,
            observer,
            max_attempt: max_attempt as usize,
            retry_after,
            dispatch,
        })
    }
}

impl Pipeline {
    pub async fn proxy(&self, body: Bytes) -> Response {
        let request_id = Uuid::new_v4();
        let span = info_span!("proxy", %request_id);
        self.proxy_inner(body).instrument(span).await
    }

    async fn proxy_inner(&self, body: Bytes) -> Response {
        let received_at = Instant::now();
        info!(event = "request_received", body_bytes = body.len());
        match shape(&body) {
            Shape::Batch => {
                warn!(event = "batch_rejected", body_bytes = body.len());
                StatusCode::BAD_REQUEST.into_response()
            }
            Shape::Single => {
                let chain = self.decider.decide(self.max_attempt);
                let finished = match (self.dispatch, is_write(&body)) {
                    (Dispatch::Hedged, false) => self.race(&chain, &body).await,
                    (Dispatch::Hedged, true) => self.send_once(&chain, &body).await,
                    (Dispatch::Sequential, _) => self.walk(&chain, &body).await,
                };
                finish(finished, received_at)
            }
            Shape::Malformed => {
                warn!(event = "batch_malformed", body_bytes = body.len());
                StatusCode::BAD_REQUEST.into_response()
            }
        }
    }
}

impl Pipeline {
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
                Ok(response) => {
                    return Finished {
                        result: Ok(Answered {
                            response,
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
                .map(|response| Answered {
                    response,
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
                response: answer,
                upstream: chain[index].id(),
            }),
            tried: chain[..raced.started].iter().map(|u| u.id()).collect(),
            hedge: Some(raced.hedge),
        }
    }
}

fn finish(finished: Finished<'_>, received_at: Instant) -> Response {
    let Finished {
        result,
        tried,
        hedge,
    } = finished;
    let hedges = hedge.map(|hedge| hedge.started);
    let hedge_won = hedge.map(|hedge| hedge.won > 0);

    match result {
        Ok(Answered { response, upstream }) => {
            info!(
                event = "request_completed",
                attempts = tried.len(),
                upstream = %upstream,
                duration_ms = elapsed_ms(received_at),
                hedges,
                hedge_won,
            );
            response
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
            rpc_error(JSONRPC_INTERNAL_ERROR, &error)
        }
    }
}

fn elapsed_ms(since: Instant) -> u64 {
    since.elapsed().as_millis() as u64
}
