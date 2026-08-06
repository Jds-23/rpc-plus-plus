use std::{fmt::Debug, time::Duration};

use anyhow::{Context, Result};
use axum::body::Bytes;
use reqwest::{StatusCode, header};

pub struct Upstream {
    http: reqwest::Client,
    url: String,
    label: String,
}

impl Debug for Upstream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label)
    }
}
#[derive(Default)]
pub struct UpstreamBuilder {
    label: Option<String>,
    url: Option<String>,
    rpc_timeout_in_secs: Option<u64>,
}

impl UpstreamBuilder {
    pub fn new() -> Self {
        UpstreamBuilder::default()
    }

    pub fn with_label(mut self, label: String) -> Self {
        self.label = Some(label);
        self
    }

    pub fn with_url(mut self, url_string: String) -> Self {
        self.url = Some(url_string);
        self
    }

    pub fn with_rpc_timeout_in_secs(mut self, rpc_timeout_in_secs: u64) -> Self {
        self.rpc_timeout_in_secs = Some(rpc_timeout_in_secs);
        self
    }

    pub fn build(self) -> Result<Upstream> {
        let url = self.url.context("url is not set")?;
        let label = self.label.context("label is not set")?;
        let rpc_timeout_in_secs = self
            .rpc_timeout_in_secs
            .context("rpc_timeout_in_secs is not set")?;

        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(rpc_timeout_in_secs))
            .build()
            .context("failed to build HTTP client")?;

        Ok(Upstream { http, url, label })
    }
}

// #[derive(Debug, Clone)]
pub struct CallOutcome {
    pub http_status: StatusCode,
    pub response_body: Bytes,
}

pub enum CallError {
    Unreachable {
        error: String,
    },
    ReadFailed {
        http_status: StatusCode,
        error: String,
    },
    ErrorStatus {
        http_status: StatusCode,
    },
}

impl Upstream {
    pub fn label(&self) -> &str {
        &self.label
    }

    async fn send(&self, body: &Bytes) -> Result<reqwest::Response, reqwest::Error> {
        self.http
            .post(&self.url)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body.to_owned())
            .send()
            .await
    }

    pub async fn call(&self, body: &Bytes) -> Result<CallOutcome, CallError> {
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
