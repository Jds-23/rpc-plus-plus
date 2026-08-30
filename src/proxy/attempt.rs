use axum::{
    body::Bytes,
    response::{IntoResponse, Response},
};
use reqwest::header;
use tracing::{info, warn};

use crate::{
    observer::Observer,
    upstream::{Upstream, call::CallError, call::CallOutcome},
};

pub(super) async fn try_once(
    observer: &dyn Observer,
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
    observer.record(id, call.record());
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
                CallError::RpcError {
                    http_status,
                    code,
                    retryable,
                } => warn!(
                    event = "attempt_failed",
                    attempt,
                    upstream = %id,
                    duration_ms = duration,
                    http_status = http_status.as_u16(),
                    rpc_error_code = code,
                    retryable,
                    error = "upstream returned a json-rpc error",
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
