pub mod round_robin_handler;

use std::{fmt::Debug, time::Duration};

use anyhow::{Context, Result};
use axum::body::Bytes;
use reqwest::header;

pub struct RpcHandler {
    http: reqwest::Client,
    url: String,
    label: String,
}

impl Debug for RpcHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label)
    }
}
#[derive(Default)]
pub struct RpcHandlerBuilder {
    label: Option<String>,
    url: Option<String>,
    rpc_timeout_in_secs: Option<u64>,
}

impl RpcHandlerBuilder {
    pub fn new() -> Self {
        RpcHandlerBuilder::default()
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

    pub fn build(self) -> Result<RpcHandler> {
        let url = self.url.context("url is not set")?;
        let label = self.label.context("label is not set")?;
        let rpc_timeout_in_secs = self
            .rpc_timeout_in_secs
            .context("rpc_timeout_in_secs is not set")?;

        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(rpc_timeout_in_secs))
            .build()
            .context("failed to build HTTP client")?;

        Ok(RpcHandler { http, url, label })
    }
}

impl RpcHandler {
    pub async fn proxy(&self, body: &Bytes) -> Result<reqwest::Response, reqwest::Error> {
        self.http
            .post(&self.url)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body.to_owned())
            .send()
            .await
    }
}
