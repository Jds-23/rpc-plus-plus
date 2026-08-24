pub mod attempt;
pub mod jsonrpc;

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
    observer::Observer,
    proxy::{
        attempt::try_once,
        jsonrpc::{JSONRPC_INTERNAL_ERROR, is_batch, rpc_error},
    },
    upstream::{UpstreamId, call::CallError},
};

const DEFAULT_MAX_ATTEMPT: u64 = 3;
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(1);

pub struct Pipeline {
    observer: Arc<dyn Observer>,
    decider: Arc<dyn Decider>,
    max_attempt: usize,
    retry_after: Duration,
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
    ) -> Result<Self, BuildError> {
        if max_attempt == 0 {
            return Err(BuildError::ZeroMaxAttempt);
        }
        Ok(Self {
            decider,
            observer,
            max_attempt: max_attempt as usize,
            retry_after,
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

        if is_batch(&body) {
            warn!(event = "batch_rejected", body_bytes = body.len());
            return StatusCode::BAD_REQUEST.into_response();
        }

        let chain = self.decider.decide(self.max_attempt);
        let mut tried: Vec<&UpstreamId> = Vec::with_capacity(chain.len());

        let mut last_failure: Option<CallError> = None;

        for upstream in &chain {
            if tried.len() >= self.max_attempt {
                break;
            }
            if !tried.is_empty() {
                tokio::time::sleep(self.retry_after).await;
            }
            tried.push(upstream.id());

            match try_once(self.observer.as_ref(), upstream, &body, tried.len() as u64).await {
                Ok(response) => {
                    info!(
                        event = "request_completed",
                        attempts = tried.len(),
                        upstream = %upstream.id(),
                        duration_ms = elapsed_ms(received_at),
                    );
                    return response;
                }
                Err(failure) => last_failure = Some(failure),
            }
        }

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
        rpc_error(JSONRPC_INTERNAL_ERROR, &error)
    }
}

fn elapsed_ms(since: Instant) -> u64 {
    since.elapsed().as_millis() as u64
}
