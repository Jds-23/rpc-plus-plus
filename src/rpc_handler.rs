pub mod round_robin_handler;

use std::fmt::Debug;

use anyhow::{Result, anyhow};
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
pub struct RpcHandlerBuilder {
    label: Option<String>,
    url: Option<String>,
    timeout_in_secs: u64,
}

impl Default for RpcHandlerBuilder {
    fn default() -> Self {
        RpcHandlerBuilder {
            label: None,
            url: None,
            timeout_in_secs: 10,
        }
    }
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

    pub fn with_timeout_in_secs(mut self, timeout_in_secs: u64) -> Self {
        self.timeout_in_secs = timeout_in_secs;
        self
    }

    pub fn build(self) -> Result<RpcHandler> {
        if let Some(url) = self.url {
            let http = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(self.timeout_in_secs))
                .build()
                .map_err(|e| anyhow!("client build failed: {}", e))?;
            let label = self.label.unwrap_or_else(|| url.clone());
            Ok(RpcHandler { http, url, label })
        } else {
            Err(anyhow!("url is not present"))
        }
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
