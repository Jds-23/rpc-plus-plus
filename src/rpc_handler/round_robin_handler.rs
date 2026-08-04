use crate::{
    decider::{Decider, RoundRobin},
    rpc_handler::RpcHandler,
    settings::RpcSettings,
    start_up::build_handlers,
};
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

pub type RoundRobinHandler = Arc<Inner>;

const DEFAULT_MAX_ATTEMPT: u64 = 3;
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(1);

#[derive(Debug, thiserror::Error)]
pub enum StateBuildError {
    #[error("no rpc handlers were provided")]
    NoHandlers,
}

#[derive(Debug, Default)]
pub struct RoundRobinHandlerBuilder {
    handlers: Vec<RpcHandler>,
    max_attempt: Option<u64>,
    retry_after: Option<Duration>,
}

impl RoundRobinHandlerBuilder {
    pub fn with_rpc_settings<I>(mut self, rpc_settings: I, rpc_timeout_in_secs: u64) -> Self
    where
        I: IntoIterator<Item = RpcSettings>,
    {
        let handlers = build_handlers(rpc_settings, rpc_timeout_in_secs);
        self.handlers.extend(handlers);
        self
    }

    pub fn with_handlers<I>(mut self, handlers: I) -> Self
    where
        I: IntoIterator<Item = RpcHandler>,
    {
        self.handlers.extend(handlers);
        self
    }

    pub fn with_handler(mut self, handler: RpcHandler) -> Self {
        self.handlers.push(handler);
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

    pub fn build(self) -> Result<RoundRobinHandler, StateBuildError> {
        if self.handlers.is_empty() {
            return Err(StateBuildError::NoHandlers);
        }

        Ok(Arc::new(Inner::new(
            self.handlers,
            self.max_attempt.unwrap_or(DEFAULT_MAX_ATTEMPT),
            self.retry_after.unwrap_or(DEFAULT_RETRY_AFTER),
        )))
    }
}

pub struct Inner {
    handler: RoundRobin<RpcHandler>,
    max_attempt: u64,
    retry_after: Duration,
}

impl Inner {
    pub fn new(rpc_handlers: Vec<RpcHandler>, max_attempt: u64, retry_after: Duration) -> Self {
        Inner {
            handler: RoundRobin::new(rpc_handlers),
            max_attempt,
            retry_after,
        }
    }
    pub async fn proxy(&self, body: Bytes) -> Response {
        let request_id = Uuid::new_v4();
        let span = info_span!("proxy", 
        %request_id,
        body_bytes=body.len());
        self.proxy_inner(body).instrument(span).await
    }

    pub async fn proxy_inner(&self, body: Bytes) -> Response {
        let received_at = Instant::now();
        info!("request_received");

        if check_if_batch(&body) {
            warn!("batch_rejected");
            return StatusCode::BAD_REQUEST.into_response();
        }

        // "unreachable" unless a read is what actually failed last
        let mut last_failure = "upstream unreachable";
        let mut tried: Vec<&str> = Vec::with_capacity(self.max_attempt as usize);

        for attempt in 1..=self.max_attempt {
            if attempt > 1 {
                tokio::time::sleep(self.retry_after).await;
            }

            let Some(handler) = self.handler.decide() else {
                error!(
                    attempts = attempt - 1,
                    duration_ms = elapsed_ms(received_at),
                    "no upstream available"
                );
                return rpc_error(-32603, "no upstream available");
            };
            tried.push(&handler.label);

            match Self::try_once(handler, &body, attempt).await {
                Ok(response) => {
                    info!(
                        attempts = attempt,
                        upstream = %handler.label,
                        duration_ms = elapsed_ms(received_at),
                        "request_completed"
                    );
                    return response;
                }
                Err(reason) => last_failure = reason,
            }
        }

        error!(
            attempts = self.max_attempt,
            tried = ?tried,
            duration_ms = elapsed_ms(received_at),
            error = last_failure,
            "retries_exhausted"
        );
        rpc_error(-32603, last_failure)
    }

    async fn try_once(
        handler: &RpcHandler,
        body: &Bytes,
        attempt: u64,
    ) -> Result<Response, &'static str> {
        let attempt_start = Instant::now();
        let upstream = handler.label.as_str();

        info!(attempt, upstream = %upstream, "attempt_started");

        let res = match handler.proxy(body).await {
            Ok(res) => res,
            Err(err) => {
                // `without_url` strips the upstream URL, which embeds the API key.
                warn!(
                    attempt,
                    upstream = %upstream,
                    duration_ms = elapsed_ms(attempt_start),
                    error = %err.without_url(),
                    "attempt_failed"
                );
                return Err("upstream unreachable");
            }
        };

        let http_status = res.status();

        let body = match res.bytes().await {
            Ok(body) => body,
            Err(err) => {
                warn!(
                    attempt,
                    upstream = %upstream,
                    duration_ms = elapsed_ms(attempt_start),
                    http_status = http_status.as_u16(),
                    error = %err.without_url(),
                    "attempt_failed"
                );
                return Err("upstream read failed");
            }
        };

        let duration_ms = elapsed_ms(attempt_start);

        if http_status != StatusCode::OK {
            warn!(
                attempt,
                upstream = %upstream,
                duration_ms,
                http_status = http_status.as_u16(),
                error = "upstream returned error status",
                "attempt_failed"
            );
            return Err("upstream returned error status");
        }

        info!(
            attempt,
            upstream = %upstream,
            duration_ms,
            http_status = http_status.as_u16(),
            response_bytes = body.len(),
            "attempt_succeeded"
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
