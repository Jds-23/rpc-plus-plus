pub mod call;

use std::{
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::body::Bytes;
use reqwest::{Client, ClientBuilder, StatusCode, header};

use crate::{
    config::{ProxySettings, UpstreamSettings},
    jsonrpc::rpc_fault_in,
    upstream::call::{CallError, CallOutcome, CallResult, error_chain},
};

const DEFAULT_RPC_TIMEOUT_IN_SECS: u64 = 3;
const DEFAULT_HEDGE_AFTER_IN_MILLIS: u64 = 250;

/// Pool knobs for the one shared client. Idle caps are per-host, and every
/// upstream shares this pool once `Upstream` stops building its own.
const POOL_MAX_IDLE_PER_HOST: usize = 32;
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(90);

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
    timeout: Duration,
    hedge_after: Duration,
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
    #[error("hedge_after_in_millis must be at least 1")]
    ZeroHedgeAfter,
    #[error("failed to build HTTP client: {0}")]
    HttpClient(#[from] reqwest::Error),
}

#[bon::bon]
impl Upstream {
    #[builder]
    pub fn new(
        #[builder(into)] label: String,
        #[builder(into)] url: String,
        #[builder(into)] http: reqwest::Client,
        #[builder(default = DEFAULT_RPC_TIMEOUT_IN_SECS)] rpc_timeout_in_secs: u64,
        #[builder(default = DEFAULT_HEDGE_AFTER_IN_MILLIS)] hedge_after_in_millis: u64,
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
        if hedge_after_in_millis == 0 {
            return Err(BuildError::ZeroHedgeAfter);
        }

        Ok(Upstream {
            http,
            url,
            id: UpstreamId::new(label),
            timeout: Duration::from_secs(rpc_timeout_in_secs),
            hedge_after: Duration::from_millis(hedge_after_in_millis),
        })
    }
}

impl Upstream {
    /// Identity — what per-attempt records key on.
    pub fn id(&self) -> &UpstreamId {
        &self.id
    }

    /// How long to wait on this upstream before hedging to the next one.
    pub fn hedge_after(&self) -> Duration {
        self.hedge_after
    }

    async fn send(&self, body: &Bytes) -> Result<reqwest::Response, reqwest::Error> {
        self.http
            .post(&self.url)
            .header(header::CONTENT_TYPE, mime::APPLICATION_JSON.to_string())
            .body(body.to_owned())
            .timeout(self.timeout)
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

        if let Some(fault) = rpc_fault_in(&body) {
            return Err(CallError::RpcError {
                http_status,
                code: fault.code,
                retryable: fault.retryable,
            });
        }

        Ok(CallOutcome {
            http_status,
            response_body: body,
        })
    }
}

// TODO: refactor this API — it takes loose settings pieces and silently skips
// upstreams that fail to build.
pub fn build_all<I>(upstreams: I, http: reqwest::Client, proxy: &ProxySettings) -> Vec<Upstream>
where
    I: IntoIterator<Item = UpstreamSettings>,
{
    upstreams
        .into_iter()
        .filter_map(|item| {
            Upstream::builder()
                .http(http.clone())
                .label(item.label.clone())
                .url(item.url)
                .rpc_timeout_in_secs(proxy.rpc_timeout_in_secs)
                .hedge_after_in_millis(
                    item.hedge_after_in_millis
                        .unwrap_or(proxy.hedge.after_in_millis),
                )
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

pub fn build_http_client(settings: &ProxySettings) -> Result<Client, reqwest::Error> {
    ClientBuilder::new()
        .pool_max_idle_per_host(POOL_MAX_IDLE_PER_HOST)
        .pool_idle_timeout(POOL_IDLE_TIMEOUT)
        .connect_timeout(Duration::from_secs(settings.rpc_timeout_in_secs))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HedgeSettings;

    fn builder_upstream(hedge_after_in_millis: Option<u64>) -> Result<Upstream, BuildError> {
        Upstream::builder()
            .label("one")
            .url("http://one.invalid")
            .http(Client::new())
            .maybe_hedge_after_in_millis(hedge_after_in_millis)
            .build()
    }

    fn proxy(after_in_millis: u64) -> ProxySettings {
        ProxySettings {
            max_attempt: 3,
            retry_after_in_secs: 1,
            rpc_timeout_in_secs: 3,
            hedge: HedgeSettings {
                enabled: true,
                after_in_millis,
            },
        }
    }

    fn settings(label: &str, hedge_after_in_millis: Option<u64>) -> UpstreamSettings {
        UpstreamSettings {
            label: label.to_string(),
            url: format!("http://{label}.invalid"),
            hedge_after_in_millis,
        }
    }

    #[test]
    fn the_hedge_threshold_defaults_to_250ms() {
        let upstream = builder_upstream(None).expect("upstream build failed");

        assert_eq!(upstream.hedge_after(), Duration::from_millis(250));
    }

    #[test]
    fn an_explicit_hedge_threshold_is_kept() {
        let upstream = builder_upstream(Some(400)).expect("upstream build failed");

        assert_eq!(upstream.hedge_after(), Duration::from_millis(400));
    }

    #[test]
    fn a_zero_hedge_threshold_is_rejected() {
        let error = builder_upstream(Some(0)).expect_err("zero should be rejected");

        assert!(matches!(error, BuildError::ZeroHedgeAfter), "{error}");
    }

    #[test]
    fn an_upstream_override_wins_over_the_global_threshold() {
        let upstreams = build_all([settings("one", Some(400))], Client::new(), &proxy(100));

        assert_eq!(upstreams[0].hedge_after(), Duration::from_millis(400));
    }

    #[test]
    fn an_upstream_without_override_falls_back_to_the_global_threshold() {
        let upstreams = build_all(
            [settings("one", Some(400)), settings("two", None)],
            Client::new(),
            &proxy(100),
        );

        assert_eq!(upstreams[1].id().as_str(), "two");
        assert_eq!(upstreams[1].hedge_after(), Duration::from_millis(100));
    }
}
