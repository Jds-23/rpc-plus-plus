use axum::{
    body::Bytes,
    response::{IntoResponse, Response},
};
use reqwest::{StatusCode, header};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::{Instrument, error, info, info_span, warn};
use uuid::Uuid;

use crate::{decider::Decider, rpc_handler::RpcHandler};

const DEFAULT_MAX_ATTEMPT: u64 = 3;
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(1);

pub struct Proxy {
    decider: Arc<dyn Decider>,
    max_attempt: usize,
    retry_after: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum ProxyBuildError {
    #[error("no decider was provided")]
    NoDecider,
    #[error("max_attempt must be at least 1")]
    ZeroMaxAttempt,
}

#[derive(Default)]
pub struct ProxyBuilder {
    decider: Option<Arc<dyn Decider>>,
    max_attempt: Option<u64>,
    retry_after: Option<Duration>,
}

impl ProxyBuilder {
    pub fn with_decider(mut self, decider: Arc<dyn Decider>) -> Self {
        self.decider = Some(decider);
        self
    }

    pub fn with_max_attempt(mut self, count: u64) -> Self {
        self.max_attempt = Some(count);
        self
    }

    pub fn with_retry_after(mut self, after: Duration) -> Self {
        self.retry_after = Some(after);
        self
    }

    pub fn with_retry_after_in_secs(self, secs: u64) -> Self {
        self.with_retry_after(Duration::from_secs(secs))
    }

    pub fn build(self) -> Result<Proxy, ProxyBuildError> {
        let decider = self.decider.ok_or(ProxyBuildError::NoDecider)?;

        let max_attempt = self.max_attempt.unwrap_or(DEFAULT_MAX_ATTEMPT);
        if max_attempt == 0 {
            return Err(ProxyBuildError::ZeroMaxAttempt);
        }

        Ok(Proxy {
            decider,
            max_attempt: max_attempt as usize,
            retry_after: self.retry_after.unwrap_or(DEFAULT_RETRY_AFTER),
        })
    }
}

impl Proxy {
    pub async fn proxy(&self, body: Bytes) -> Response {
        let request_id = Uuid::new_v4();
        let span = info_span!("proxy", %request_id);
        self.proxy_inner(body).instrument(span).await
    }

    pub async fn proxy_inner(&self, body: Bytes) -> Response {
        let received_at = Instant::now();
        info!(event = "request_received", body_bytes = body.len());

        if check_if_batch(&body) {
            warn!(event = "batch_rejected", body_bytes = body.len());
            return StatusCode::BAD_REQUEST.into_response();
        }

        let handler_chain = self.decider.decide(self.max_attempt);
        let mut tried: Vec<&str> = Vec::with_capacity(handler_chain.len());

        // An empty chain skips the loop and falls through to `retries_exhausted`
        // with zero attempts, rather than inventing an event name outside the
        // logging schema. `RoundRobin` cannot return empty — the builder rejects
        // `max_attempt == 0` and `RoundRobin::new` rejects an empty rotation — but
        // a health-scored `Decider` legitimately can once every upstream is
        // scored out.
        let mut last_failure = if handler_chain.is_empty() {
            "no upstream available"
        } else {
            // "unreachable" unless a read is what actually failed last
            "upstream unreachable"
        };

        for handler in &handler_chain {
            if tried.len() >= self.max_attempt {
                break;
            }
            if !tried.is_empty() {
                tokio::time::sleep(self.retry_after).await;
            }
            tried.push(handler.label());

            match Self::try_once(handler, &body, tried.len() as u64).await {
                Ok(response) => {
                    info!(
                        event = "request_completed",
                        attempts = tried.len(),
                        upstream = %handler.label(),
                        duration_ms = elapsed_ms(received_at),
                    );
                    return response;
                }
                Err(reason) => last_failure = reason,
            }
        }

        error!(
            event = "retries_exhausted",
            attempts = tried.len(),
            tried = ?tried,
            duration_ms = elapsed_ms(received_at),
            error = last_failure,
        );
        rpc_error(-32603, last_failure)
    }

    async fn try_once(
        handler: &RpcHandler,
        body: &Bytes,
        attempt: u64,
    ) -> Result<Response, &'static str> {
        let attempt_start = Instant::now();
        let upstream = handler.label();

        info!(event = "attempt_started", attempt, upstream = %upstream);

        let res = match handler.proxy(body).await {
            Ok(res) => res,
            Err(err) => {
                // `without_url` strips the upstream URL, which embeds the API key.
                warn!(
                    event = "attempt_failed",
                    attempt,
                    upstream = %upstream,
                    duration_ms = elapsed_ms(attempt_start),
                    error = %err.without_url(),
                );
                return Err("upstream unreachable");
            }
        };

        let http_status = res.status();

        let body = match res.bytes().await {
            Ok(body) => body,
            Err(err) => {
                warn!(
                    event = "attempt_failed",
                    attempt,
                    upstream = %upstream,
                    duration_ms = elapsed_ms(attempt_start),
                    http_status = http_status.as_u16(),
                    error = %err.without_url(),
                );
                return Err("upstream read failed");
            }
        };

        let duration_ms = elapsed_ms(attempt_start);

        if http_status != StatusCode::OK {
            warn!(
                event = "attempt_failed",
                attempt,
                upstream = %upstream,
                duration_ms,
                http_status = http_status.as_u16(),
                error = "upstream returned error status",
            );
            return Err("upstream returned error status");
        }

        info!(
            event = "attempt_succeeded",
            attempt,
            upstream = %upstream,
            duration_ms,
            http_status = http_status.as_u16(),
            response_bytes = body.len(),
        );
        Ok((
            http_status,
            [(header::CONTENT_TYPE, "application/json")],
            body,
        )
            .into_response())
    }
}

fn elapsed_ms(since: Instant) -> u64 {
    since.elapsed().as_millis() as u64
}

/// Peeks at the first non-whitespace byte rather than deserialising, so the body
/// stays raw passthrough.
fn check_if_batch(body: &Bytes) -> bool {
    body.iter()
        .find(|byte| !byte.is_ascii_whitespace())
        .is_some_and(|byte| *byte == b'[')
}

fn rpc_error(code: i64, msg: &str) -> Response {
    let body =
        format!(r#"{{"jsonrpc":"2.0","error":{{"code":{code},"message":"{msg}"}},"id":null}}"#);
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        body,
    )
        .into_response()
}
