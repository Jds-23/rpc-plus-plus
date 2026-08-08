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
const JSONRPC_INTERNAL_ERROR: i64 = -32603;

pub struct ProxyState {
    observer: Arc<dyn Observer>,
    decider: Arc<dyn Decider>,
    max_attempt: usize,
    retry_after: Duration,
}

#[derive(Debug, thiserror::Error)]
pub enum ProxyStateBuildError {
    #[error("max_attempt must be at least 1")]
    ZeroMaxAttempt,
}

pub struct ProxyStateBuilder {
    observer: Arc<dyn Observer>,
    decider: Arc<dyn Decider>,
    max_attempt: Option<u64>,
    retry_after: Option<Duration>,
}

impl ProxyStateBuilder {
    pub fn new(decider: Arc<dyn Decider>, observer: Arc<dyn Observer>) -> Self {
        Self {
            decider,
            observer,
            max_attempt: None,
            retry_after: None,
        }
    }

    pub fn with_max_attempt(mut self, count: u64) -> Result<Self, ProxyStateBuildError> {
        if count == 0 {
            return Err(ProxyStateBuildError::ZeroMaxAttempt);
        }
        self.max_attempt = Some(count);
        Ok(self)
    }

    pub fn with_retry_after(mut self, after: Duration) -> Self {
        self.retry_after = Some(after);
        self
    }

    pub fn with_retry_after_in_secs(self, secs: u64) -> Self {
        self.with_retry_after(Duration::from_secs(secs))
    }

    pub fn build(self) -> ProxyState {
        let max_attempt = self.max_attempt.unwrap_or(DEFAULT_MAX_ATTEMPT);
        ProxyState {
            decider: self.decider,
            observer: self.observer,
            max_attempt: max_attempt as usize,
            retry_after: self.retry_after.unwrap_or(DEFAULT_RETRY_AFTER),
        }
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

        let mut last_failure: Option<CallError> = None;

        for upstream in &chain {
            if tried.len() >= self.max_attempt {
                break;
            }
            if !tried.is_empty() {
                tokio::time::sleep(self.retry_after).await;
            }
            tried.push(upstream.id());

            match self.try_once(upstream, &body, tried.len() as u64).await {
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

    async fn try_once(
        &self,
        upstream: &Upstream,
        body: &Bytes,
        attempt: u64,
    ) -> Result<Response, CallError> {
        let id = upstream.id();
        info!(
            event = "attempt_started",
            attempt,
            upstream = %id,
        );
        let call = upstream.call(body).await;
        self.observer.record(id, call.record());
        let duration = call.duration.as_millis() as u64;
        match call.result {
            Ok(CallOutcome {
                http_status,
                response_body,
            }) => {
                info!(
                    event = "attempt_succeeded",
                    attempt,
                    upstream = %id,
                    duration_ms = duration,
                    http_status = http_status.as_u16(),
                    response_bytes = response_body.len(),
                );
                Ok((
                    http_status,
                    [(header::CONTENT_TYPE, mime::APPLICATION_JSON.to_string())],
                    response_body,
                )
                    .into_response())
            }
            Err(failure) => {
                match &failure {
                    CallError::Unreachable { error } => warn!(
                        event = "attempt_failed",
                        attempt,
                        upstream = %id,
                        duration_ms = duration,
                        error = %error,
                    ),
                    CallError::ReadFailed { http_status, error } => warn!(
                        event = "attempt_failed",
                        attempt,
                        upstream = %id,
                        duration_ms = duration,
                        http_status = http_status.as_u16(),
                        error = %error,
                    ),
                    CallError::ErrorStatus { http_status } => warn!(
                        event = "attempt_failed",
                        attempt,
                        upstream = %id,
                        duration_ms = duration,
                        http_status = http_status.as_u16(),
                        error = "upstream returned error status",
                    ),
                }
                Err(failure)
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
        let response = rpc_error(JSONRPC_INTERNAL_ERROR, msg);

        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(parsed["error"]["message"], msg);
        assert_eq!(parsed["error"]["code"], JSONRPC_INTERNAL_ERROR);
        assert!(parsed["id"].is_null());
    }
}
