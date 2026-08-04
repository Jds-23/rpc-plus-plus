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
use tracing::{Instrument, Span, error, field::Empty, info, info_span, warn};
use uuid::Uuid;

pub type RoundRobinHandler = Arc<Inner>;

const DEFAULT_MAX_ATTEMPT: u64 = 2;
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
    pub fn with_rpc_setttings<I>(mut self, rpc_settings: I) -> Self
    where
        I: IntoIterator<Item = RpcSettings>,
    {
        let handlers = build_handlers(rpc_settings);
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
    retry_after_in_secs: Duration,
}

impl Inner {
    pub fn new(
        rpc_handlers: Vec<RpcHandler>,
        max_attempt: u64,
        retry_after_in_secs: Duration,
    ) -> Self {
        Inner {
            handler: RoundRobin::new(rpc_handlers),
            max_attempt,
            retry_after_in_secs,
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
        let span = Span::current();
        info!("request_received");

        if check_if_batch(&body) {
            span.record("outcome", "rejected");
            warn!("batch_rejected");
            return StatusCode::BAD_REQUEST.into_response();
        }

        // "unreachable" unless a read is what actually failed last
        let mut last_failure = "upstream unreachable";

        for attempt in 0..=self.max_attempt {
            if attempt > 0 {
                tokio::time::sleep(self.retry_after_in_secs).await;
            }

            let Some(handler) = self.handler.decide() else {
                span.record("attempts", attempt);
                span.record("outcome", "no_upstream");
                error!("no upstream available");
                return rpc_error(-32603, "no upstream available");
            };

            match Self::try_once(handler, &body, attempt).await {
                Ok(response) => {
                    span.record("attempts", attempt + 1);
                    span.record("outcome", "success");
                    info!("request completed");
                    return response;
                }
                Err(reason) => last_failure = reason,
            }
        }

        span.record("attempts", self.max_attempt + 1);
        span.record("outcome", "exhausted");
        error!(error = last_failure, "retries_exhausted");
        rpc_error(-32603, last_failure)
    }

    #[tracing::instrument(
        name = "attempt",
        skip_all,
        fields(attempt, upstream = %handler.label, duration_ms = Empty, http_status = Empty),
    )]
    async fn try_once(
        handler: &RpcHandler,
        body: &Bytes,
        attempt: u64,
    ) -> Result<Response, &'static str> {
        let span = Span::current();
        let attempt_start = Instant::now();

        info!(
            attempt=%attempt,
            upstream=%&handler.label,
            "attempt_started"
        );

        let res = handler.proxy(body).await.map_err(|err| {
            span.record("duration_ms", attempt_start.elapsed().as_millis() as u64);
            error!(error = %err, "upstream request failed");
            "upstream unreachable"
        })?;

        let status = res.status();
        span.record("http_status", status.as_u16());

        let body = res.bytes().await.map_err(|err| {
            span.record("duration_ms", attempt_start.elapsed().as_millis() as u64);
            error!(error = %err, "reading upstream body failed");
            "upstream read failed"
        })?;

        span.record("duration_ms", attempt_start.elapsed().as_millis() as u64);
        span.record("response_bytes", body.len());

        if !status.is_success() {
            warn!("upstream returned error status");
            return Err("upstream returned error status");
        } else {
            info!("attempt succeeded");
            Ok((status, [(header::CONTENT_TYPE, "application/json")], body).into_response())
        }
    }
}

fn check_if_batch(body: &Bytes) -> bool {
    matches!(
        serde_json::from_slice::<serde_json::Value>(body),
        Ok(serde_json::Value::Array(_))
    )
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
