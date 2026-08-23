pub mod call;

use std::{
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::body::Bytes;
use reqwest::{StatusCode, header};

use crate::{
    config::UpstreamSettings,
    upstream::call::{CallError, CallOutcome, CallResult, error_chain},
};

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

impl fmt::Debug for Upstream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.id)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("label must not be empty")]
    EmptyLabel,
    #[error("url must not be empty")]
    EmptyUrl,
    #[error("rpc_timeout_in_secs must be at least 1")]
    ZeroTimeout,
    #[error("failed to build HTTP client: {0}")]
    HttpClient(#[from] reqwest::Error),
}

#[bon::bon]
impl Upstream {
    #[builder]
    pub fn new(
        #[builder(into)] label: String,
        #[builder(into)] url: String,
        #[builder(default = DEFAULT_RPC_TIMEOUT_IN_SECS)] rpc_timeout_in_secs: u64,
    ) -> Result<Self, BuildError> {
        if label.trim().is_empty() {
            return Err(BuildError::EmptyLabel);
        }
        if url.trim().is_empty() {
            return Err(BuildError::EmptyUrl);
        }
        if rpc_timeout_in_secs == 0 {
            return Err(BuildError::ZeroTimeout);
        }

        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(rpc_timeout_in_secs))
            .build()?;

        Ok(Upstream {
            http,
            url,
            id: UpstreamId::new(label),
        })
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

pub fn build_all<I>(upstreams: I, rpc_timeout_in_secs: u64) -> Vec<Upstream>
where
    I: IntoIterator<Item = UpstreamSettings>,
{
    upstreams
        .into_iter()
        .filter_map(|item| {
            Upstream::builder()
                .label(item.label.clone())
                .url(item.url)
                .rpc_timeout_in_secs(rpc_timeout_in_secs)
                .build()
                .map_err(|err| {
                    tracing::warn!(
                        event = "upstream_skipped",
                        upstream = %item.label,
                        error = %err,
                    );
                })
                .ok()
        })
        .collect()
}
