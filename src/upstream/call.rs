use std::time::Duration;

use axum::body::Bytes;
use reqwest::StatusCode;

pub struct CallResult {
    pub duration: Duration,
    pub result: Result<CallOutcome, CallError>,
}

impl CallResult {
    pub fn record(&self) -> CallRecord<'_> {
        CallRecord {
            duration: self.duration,
            outcome: match &self.result {
                Ok(outcome) => Ok(outcome.http_status),
                Err(error) => Err(error),
            },
        }
    }
}

pub struct CallRecord<'a> {
    pub duration: Duration,
    pub outcome: Result<StatusCode, &'a CallError>,
}

#[derive(Debug)]
pub struct CallOutcome {
    pub http_status: StatusCode,
    pub response_body: Bytes,
}

#[derive(Debug, thiserror::Error)]
pub enum CallError {
    #[error("{error}")]
    Unreachable { error: String },
    #[error("{error}")]
    ReadFailed {
        http_status: StatusCode,
        error: String,
    },
    #[error("upstream returned rpc error code {}", code)]
    RpcError {
        http_status: StatusCode,
        code: i64,
        retryable: bool,
    },
    #[error("upstream returned error status {}", http_status.as_u16())]
    ErrorStatus { http_status: StatusCode },
}

impl CallError {
    pub const UNREACHABLE: &'static str = "unreachable";
    pub const READ_FAILED: &'static str = "read_failed";
    pub const ERROR_STATUS: &'static str = "error_status";
    pub const RPC_ERROR: &'static str = "rpc_error";

    /// `None` for `Unreachable`: no response arrived to carry a status.
    pub fn http_status(&self) -> Option<StatusCode> {
        match self {
            CallError::Unreachable { .. } => None,
            CallError::ReadFailed { http_status, .. }
            | CallError::ErrorStatus { http_status }
            | CallError::RpcError { http_status, .. } => Some(*http_status),
        }
    }

    pub fn is_retryable(&self) -> bool {
        match self {
            // No response arrived. A different upstream is a different connection.
            CallError::Unreachable { .. } => true,
            // The body died mid-read; the same request may land fine on a peer.
            CallError::ReadFailed { .. } => true,
            // 429 and 5xx are the upstream's problem. A 4xx is the caller's, and
            // every upstream will answer it the same way.
            CallError::ErrorStatus { http_status } => {
                *http_status == StatusCode::TOO_MANY_REQUESTS || http_status.is_server_error()
            }
            // Already decided by the error code, in `jsonrpc::is_retryable`.
            CallError::RpcError { retryable, .. } => *retryable,
        }
    }
}

/// `reqwest::Error`'s own `Display` stops at `error sending request`, which names
/// no cause. Walking `source()` yields the transport frame that actually failed.
///
/// Only ever called on an error already passed through `without_url`; the
/// remaining frames are transport-level and carry no path, which is where the API
/// key lives.
pub(super) fn error_chain(err: &dyn std::error::Error) -> String {
    let mut out = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        out.push_str(": ");
        out.push_str(&cause.to_string());
        source = cause.source();
    }
    out
}
