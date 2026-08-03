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
use std::{sync::Arc, time::Duration};

pub type RoundRobinHandler = Arc<Inner>;

const DEFAULT_MAX_RETRY_COUNT: u64 = 2;
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(1);

#[derive(Debug, thiserror::Error)]
pub enum StateBuildError {
    #[error("no rpc handlers were provided")]
    NoHandlers,
}

#[derive(Debug, Default)]
pub struct RoundRobinHandlerBuilder {
    handlers: Vec<RpcHandler>,
    max_retry_count: Option<u64>,
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

    pub fn with_max_retry_count(mut self, count: u64) -> Self {
        self.max_retry_count = Some(count);
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
            self.max_retry_count.unwrap_or(DEFAULT_MAX_RETRY_COUNT),
            self.retry_after.unwrap_or(DEFAULT_RETRY_AFTER),
        )))
    }
}

pub struct Inner {
    handler: RoundRobin<RpcHandler>,
    max_retry_count: u64,
    retry_after_in_secs: Duration,
}

impl Inner {
    pub fn new(
        rpc_handlers: Vec<RpcHandler>,
        max_retry_count: u64,
        retry_after_in_secs: Duration,
    ) -> Self {
        Inner {
            handler: RoundRobin::new(rpc_handlers),
            max_retry_count,
            retry_after_in_secs,
        }
    }
    pub async fn proxy(&self, body: Bytes) -> Response {
        // "unreachable" unless a read is what actually failed last
        let mut last_failure = "upstream unreachable";

        for retry_count in 0..=self.max_retry_count {
            if retry_count > 0 {
                tokio::time::sleep(self.retry_after_in_secs).await;
            }

            let handler = match self.handler.decide() {
                Some(h) => h,
                None => return rpc_error(-32603, "no upstream available"),
            };

            let res = match handler.proxy(&body).await {
                Ok(r) => r,
                Err(_) => {
                    last_failure = "upstream unreachable";
                    continue;
                }
            };

            let status = res.status();

            if !status.is_success() {
                last_failure = "upstream read failed";
                continue;
            }

            match res.bytes().await {
                Ok(b) => {
                    return (status, [(header::CONTENT_TYPE, "application/json")], b)
                        .into_response();
                }
                Err(_) => {
                    last_failure = "upstream read failed";
                    continue;
                }
            }
        }

        rpc_error(-32603, last_failure)
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
