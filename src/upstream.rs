use std::{
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::body::Bytes;
use reqwest::{StatusCode, header};

const DEFAULT_RPC_TIMEOUT_IN_SECS: u64 = 3;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct UpstreamId(Arc<str>);

impl UpstreamId {
    pub fn new(label: impl Into<Arc<str>>) -> Self {
        UpstreamId(label.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for UpstreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&*self.0, f)
    }
}

impl fmt::Display for UpstreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

pub struct Upstream {
    http: reqwest::Client,
    url: String,
    id: UpstreamId,
}

impl Upstream {
    pub fn new(url: String, id: UpstreamId) -> Self {
        Upstream {
            http: reqwest::Client::new(),
            url,
            id,
        }
    }
}

impl fmt::Debug for Upstream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.id)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum UpstreamBuildError {
    #[error("label must not be empty")]
    EmptyLabel,
    #[error("url must not be empty")]
    EmptyUrl,
    #[error("rpc_timeout_in_secs must be at least 1")]
    ZeroTimeout,
    #[error("failed to build HTTP client: {0}")]
    HttpClient(#[from] reqwest::Error),
}

pub struct UpstreamBuilder {
    label: String,
    url: String,
    rpc_timeout_in_secs: u64,
}

impl UpstreamBuilder {
    pub fn new(
        label: impl Into<String>,
        url: impl Into<String>,
    ) -> Result<Self, UpstreamBuildError> {
        let label = label.into();
        if label.trim().is_empty() {
            return Err(UpstreamBuildError::EmptyLabel);
        }
        let url = url.into();
        if url.trim().is_empty() {
            return Err(UpstreamBuildError::EmptyUrl);
        }

        Ok(UpstreamBuilder {
            label,
            url,
            rpc_timeout_in_secs: DEFAULT_RPC_TIMEOUT_IN_SECS,
        })
    }

    pub fn with_rpc_timeout_in_secs(mut self, rpc_timeout_in_secs: u64) -> Self {
        self.rpc_timeout_in_secs = rpc_timeout_in_secs;
        self
    }

    pub fn build(self) -> Result<Upstream, UpstreamBuildError> {
        let rpc_timeout_in_secs = self.rpc_timeout_in_secs;
        if rpc_timeout_in_secs == 0 {
            return Err(UpstreamBuildError::ZeroTimeout);
        }
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(rpc_timeout_in_secs))
            .build()?;

        Ok(Upstream {
            http,
            url: self.url,
            id: UpstreamId::new(self.label),
        })
    }
}

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

    pub fn as_str(&self) -> &'static str {
        match self {
            CallError::Unreachable { .. } => Self::UNREACHABLE,
            CallError::ReadFailed { .. } => Self::READ_FAILED,
            CallError::ErrorStatus { .. } => Self::ERROR_STATUS,
        }
    }
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

impl Upstream {
    /// Identity — what per-attempt records key on.
    pub fn id(&self) -> &UpstreamId {
        &self.id
    }

    async fn send(&self, body: &Bytes) -> Result<reqwest::Response, reqwest::Error> {
        self.http
            .post(&self.url)
            .header(header::CONTENT_TYPE, mime::APPLICATION_JSON.to_string())
            .body(body.to_owned())
            .send()
            .await
    }

    pub async fn call(&self, body: &Bytes) -> CallResult {
        let attempt_start = Instant::now();
        let result = self.call_inner(body).await;
        CallResult {
            result,
            duration: attempt_start.elapsed(),
        }
    }

    async fn call_inner(&self, body: &Bytes) -> Result<CallOutcome, CallError> {
        let res = self.send(body).await.map_err(|err| {
            let error = error_chain(&err.without_url());
            CallError::Unreachable { error }
        })?;

        let http_status = res.status();

        let body = res.bytes().await.map_err(|err| {
            let error = error_chain(&err.without_url());
            CallError::ReadFailed { http_status, error }
        })?;

        if http_status != StatusCode::OK {
            return Err(CallError::ErrorStatus { http_status });
        }

        Ok(CallOutcome {
            http_status,
            response_body: body,
        })
    }
}

/// `reqwest::Error`'s own `Display` stops at `error sending request`, which names
/// no cause. Walking `source()` yields the transport frame that actually failed.
///
/// Only ever called on an error already passed through `without_url`; the
/// remaining frames are transport-level and carry no path, which is where the API
/// key lives.
fn error_chain(err: &dyn std::error::Error) -> String {
    let mut out = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        out.push_str(": ");
        out.push_str(&cause.to_string());
        source = cause.source();
    }
    out
}
