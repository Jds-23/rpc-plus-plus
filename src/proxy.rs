use axum::{
    Json,
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

use crate::{
    decider::Decider,
    observer::Observer,
    upstream::{CallError, CallOutcome, Upstream, UpstreamId},
};

const DEFAULT_MAX_ATTEMPT: u64 = 3;
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(1);

pub struct ProxyState {
    observer: Arc<dyn Observer>,
    decider: Arc<dyn Decider>,
    max_attempt: usize,
    retry_after: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum ProxyStateBuildError {
    #[error("no decider was provided")]
    NoDecider,
    #[error("no observer was provided")]
    NoObserver,
    #[error("max_attempt must be at least 1")]
    ZeroMaxAttempt,
}

#[derive(Default)]
pub struct ProxyStateBuilder {
    observer: Option<Arc<dyn Observer>>,
    decider: Option<Arc<dyn Decider>>,
    max_attempt: Option<u64>,
    retry_after: Option<Duration>,
}

impl ProxyStateBuilder {
    pub fn with_decider(mut self, decider: Arc<dyn Decider>) -> Self {
        self.decider = Some(decider);
        self
    }

    pub fn with_observer(mut self, observer: Arc<dyn Observer>) -> Self {
        self.observer = Some(observer);
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

    pub fn build(self) -> Result<ProxyState, ProxyStateBuildError> {
        let decider = self.decider.ok_or(ProxyStateBuildError::NoDecider)?;
        let observer = self.observer.ok_or(ProxyStateBuildError::NoObserver)?;

        let max_attempt = self.max_attempt.unwrap_or(DEFAULT_MAX_ATTEMPT);
        if max_attempt == 0 {
            return Err(ProxyStateBuildError::ZeroMaxAttempt);
        }

        Ok(ProxyState {
            decider,
            observer,
            max_attempt: max_attempt as usize,
            retry_after: self.retry_after.unwrap_or(DEFAULT_RETRY_AFTER),
        })
    }
}

impl ProxyState {
    pub async fn proxy(&self, body: Bytes) -> Response {
        let request_id = Uuid::new_v4();
        let span = info_span!("proxy", %request_id);
        self.proxy_inner(body).instrument(span).await
    }

    async fn proxy_inner(&self, body: Bytes) -> Response {
        let received_at = Instant::now();
        info!(event = "request_received", body_bytes = body.len());

        if check_if_batch(&body) {
            warn!(event = "batch_rejected", body_bytes = body.len());
            return StatusCode::BAD_REQUEST.into_response();
        }

        let chain = self.decider.decide(self.max_attempt);
        let mut tried: Vec<&UpstreamId> = Vec::with_capacity(chain.len());

        // An empty chain skips the loop and falls through to `retries_exhausted`
        // with zero attempts, rather than inventing an event name outside the
        // logging schema. `RoundRobin` cannot return empty — the builder rejects
        // `max_attempt == 0` and `RoundRobin::new` rejects an empty rotation — but
        // a health-scored `Decider` legitimately can once every upstream is
        // scored out.
        let mut last_failure = if chain.is_empty() {
            "no upstream available".to_string()
        } else {
            // "unreachable" unless a read is what actually failed last
            "upstream unreachable".to_string()
        };

        for upstream in &chain {
            if tried.len() >= self.max_attempt {
                break;
            }
            if !tried.is_empty() {
                tokio::time::sleep(self.retry_after).await;
            }
            tried.push(upstream.id());

            match Self::try_once(upstream, &body, tried.len() as u64).await {
                Ok(response) => {
                    info!(
                        event = "request_completed",
                        attempts = tried.len(),
                        upstream = %upstream.id(),
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
            error = %last_failure,
        );
        rpc_error(-32603, &last_failure)
    }

    async fn try_once(upstream: &Upstream, body: &Bytes, attempt: u64) -> Result<Response, String> {
        let attempt_start = Instant::now();
        let id = upstream.id();
        match upstream.call(body).await {
            Ok(CallOutcome {
                http_status,
                response_body,
            }) => {
                info!(
                    event = "attempt_succeeded",
                    attempt,
                    upstream = %id,
                    duration_ms = elapsed_ms(attempt_start),
                    http_status = http_status.as_u16(),
                    response_bytes = response_body.len(),
                );
                Ok((
                    http_status,
                    [(header::CONTENT_TYPE, "application/json")],
                    response_body,
                )
                    .into_response())
            }
            Err(CallError::Unreachable { error }) => {
                warn!(
                    event = "attempt_failed",
                    attempt,
                    upstream = %id,
                    duration_ms = elapsed_ms(attempt_start),
                    error = %error,
                );
                Err(error)
            }
            Err(CallError::ReadFailed { http_status, error }) => {
                warn!(
                    event = "attempt_failed",
                    attempt,
                    upstream = %id,
                    duration_ms = elapsed_ms(attempt_start),
                    http_status = http_status.as_u16(),
                    error = %error,
                );
                Err(error)
            }
            Err(CallError::ErrorStatus { http_status }) => {
                warn!(
                    event = "attempt_failed",
                    attempt,
                    upstream = %id,
                    duration_ms = elapsed_ms(attempt_start),
                    http_status = http_status.as_u16(),
                    error = "upstream returned error status",
                );
                Err(format!(
                    "upstream returned error status {}",
                    http_status.as_u16()
                ))
            }
        }
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

/// Built with `json!` rather than string interpolation: `msg` now carries an
/// upstream's own error text, so escaping cannot be left to the call site.
fn rpc_error(code: i64, msg: &str) -> Response {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "error": { "code": code, "message": msg },
        "id": null,
    });
    (StatusCode::OK, Json(body)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    /// An upstream message containing quotes and a newline used to produce a
    /// body that no client could parse.
    #[tokio::test]
    async fn rpc_error_escapes_the_message() {
        let msg = "upstream said \"nope\"\nand hung up";
        let response = rpc_error(-32603, msg);

        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(parsed["error"]["message"], msg);
        assert_eq!(parsed["error"]["code"], -32603);
        assert!(parsed["id"].is_null());
    }
}
