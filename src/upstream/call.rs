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
    #[error("upstream returned error status {}", http_status.as_u16())]
    ErrorStatus { http_status: StatusCode },
}

impl CallError {
    pub const UNREACHABLE: &'static str = "unreachable";
    pub const READ_FAILED: &'static str = "read_failed";
    pub const ERROR_STATUS: &'static str = "error_status";

    /// `None` for `Unreachable`: no response arrived to carry a status.
    pub fn http_status(&self) -> Option<StatusCode> {
        match self {
            CallError::Unreachable { .. } => None,
            CallError::ReadFailed { http_status, .. } | CallError::ErrorStatus { http_status } => {
                Some(*http_status)
            }
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
