use std::sync::Arc;

use anyhow::{Result, anyhow};
use axum::{
    body::Bytes,
    response::{IntoResponse, Response},
};
use reqwest::{StatusCode, header};

use crate::decider::RoundRobin;

pub struct RpcHandler {
    http: reqwest::Client,
    url: String,
}

pub type RoundRobinHandler=Arc<RoundRobin<RpcHandler>>;

pub struct RpcHandlerBuilder {
    url: Option<String>,
    timeout_in_secs: u64,
}

impl RpcHandlerBuilder {
    pub fn default() -> Self {
        RpcHandlerBuilder {
            url: None,
            timeout_in_secs: 10,
        }
    }

    pub fn new() -> Self {
        RpcHandlerBuilder::default()
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
            Ok(RpcHandler { http, url })
        } else {
            Err(anyhow!("url is not present"))
        }
    }
}

impl RpcHandler {
    pub async fn proxy(&self, body: Bytes) -> Response {
        let res = self
            .http
            .post(&self.url)
            .header(header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await;

        let res = match res {
            Ok(r) => r,
            Err(_) => return rpc_error(-32603, "upstream unreachable"),
        };

        let status = res.status();

        match res.bytes().await {
            Ok(b) => (status, [(header::CONTENT_TYPE, "application/json")], b).into_response(),
            Err(_) => rpc_error(-32603, "upstream read failed"),
        }
    }
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
