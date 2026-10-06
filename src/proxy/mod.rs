pub mod attempt;
pub mod dedup_key;
mod hedge;
mod reply;

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
    jsonrpc::{JSONRPC_INTERNAL_ERROR, Shape, is_write, shape},
    observer::Observer,
    proxy::{attempt::try_once, hedge::race, reply::Reply},
    upstream::{Upstream, UpstreamId, call::CallError},
};

const DEFAULT_MAX_ATTEMPT: u64 = 3;
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(1);

pub struct Pipeline {
    observer: Arc<dyn Observer>,
    decider: Arc<dyn Decider>,
    max_attempt: usize,
    retry_after: Duration,
    hedging: bool,
}

/// How a request ended, whichever path ran it.
struct Finished<'c> {
    result: Result<(Reply, &'c UpstreamId), Option<CallError>>,
    tried: Vec<&'c UpstreamId>,
    /// `(hedges, hedge_won)`; `None` when hedging is off.
    hedge: Option<(usize, bool)>,
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
        #[builder(default)] hedging: bool,
    ) -> Result<Self, BuildError> {
        if max_attempt == 0 {
            return Err(BuildError::ZeroMaxAttempt);
        }
        Ok(Self {
            decider,
            observer,
            max_attempt: max_attempt as usize,
            retry_after,
            hedging,
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
                let finished = match (self.hedging, is_write(&body)) {
                    (true, false) => self.race(&chain, &body).await,
                    (true, true) => self.send_once(&chain, &body).await,
                    (false, _) => self.walk(&chain, &body).await,
                };
                finish(finished, received_at).into_response()
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
                Ok(reply) => {
                    return Finished {
                        result: Ok((reply, upstream.id())),
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
            result: Err(last_failure),
            tried,
            hedge: None,
        }
    }

    /// A write while hedging is on: the head of the chain and nothing else.
    /// The first send may land even when it reports failure, so a second call
    /// risks a double send.
    async fn send_once<'c>(&self, chain: &'c [Arc<Upstream>], body: &Bytes) -> Finished<'c> {
        let Some(upstream) = chain.first() else {
            return Finished {
                result: Err(None),
                tried: Vec::new(),
                hedge: Some((0, false)),
            };
        };
        let result = try_once(self.observer.as_ref(), upstream, body, 1, false).await;

        Finished {
            result: result
                .map(|response| (response, upstream.id()))
                .map_err(Some),
            tried: vec![upstream.id()],
            hedge: Some((0, false)),
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
                start.attempt,
                start.overtaken.is_some(),
            )
        })
        .await;
        // A hedge always overtakes the upstream just before it (see `Start`).
        if let (true, Ok((index, _))) = (raced.hedge_won, &raced.result) {
            self.observer
                .record_hedge_win(chain[*index - 1].id(), chain[*index].id());
        }

        Finished {
            result: raced
                .result
                .map(|(index, reply)| (reply, chain[index].id())),
            tried: chain[..raced.attempts].iter().map(|u| u.id()).collect(),
            hedge: Some((raced.hedges, raced.hedge_won)),
        }
    }
}

fn finish(finished: Finished<'_>, received_at: Instant) -> Reply {
    let Finished {
        result,
        tried,
        hedge,
    } = finished;
    let (hedges, hedge_won) = hedge.unzip();

    match result {
        Ok((reply, upstream)) => {
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
        Err(last_failure) => {
            let error = match &last_failure {
                Some(failure) => failure.to_string(),
                None => "no upstream available".to_string(),
            };

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
