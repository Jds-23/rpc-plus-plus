use reqwest::StatusCode;
use serde_json::{Value, json};
use wiremock::{
    Mock, MockServer, Respond, ResponseTemplate,
    matchers::{method, path},
};

pub enum RpcReply {
    Ok(Value),
    Err(Value),
    Http(StatusCode),
}

impl RpcReply {
    pub fn error(code: i64, message: &str) -> Self {
        RpcReply::Err(json!({ "code": code, "message": message }))
    }

    fn into_template(self, id: Value) -> ResponseTemplate {
        match self {
            RpcReply::Err(err) => ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0", "id": id, "error": err
            })),
            RpcReply::Ok(result) => ResponseTemplate::new(200).set_body_json(json!({
                "jsonrpc": "2.0", "id": id, "result": result
            })),
            RpcReply::Http(status) => ResponseTemplate::new(status.as_u16()),
        }
    }
}

impl From<Value> for RpcReply {
    fn from(v: Value) -> Self {
        RpcReply::Ok(v)
    }
}

impl From<StatusCode> for RpcReply {
    fn from(c: StatusCode) -> Self {
        RpcReply::Http(c)
    }
}

struct RpcResponder<F>(F);

impl<F, R> Respond for RpcResponder<F>
where
    F: Fn(&Value) -> R + Send + Sync + 'static,
    R: Into<RpcReply>,
{
    fn respond(&self, request: &wiremock::Request) -> ResponseTemplate {
        let body: Value = match serde_json::from_slice(&request.body) {
            Ok(v) => v,
            Err(_) => {
                return RpcReply::error(-32700, "Parse error").into_template(Value::Null);
            }
        };
        let id = body.get("id").cloned().unwrap_or(Value::Null);
        (self.0)(&body).into().into_template(id)
    }
}

pub async fn mock_rpc<F, R>(handler: F) -> MockServer
where
    F: Fn(&Value) -> R + Send + Sync + 'static,
    R: Into<RpcReply>,
{
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(RpcResponder(handler))
        .mount(&server)
        .await;
    server
}

pub async fn ok(result: impl Into<Value>) -> MockServer {
    let result = result.into();
    mock_rpc(move |_| result.clone()).await
}

pub async fn failing(status: StatusCode) -> MockServer {
    mock_rpc(move |_| status).await
}

pub async fn rpc_erroring(code: i64, message: &'static str) -> MockServer {
    mock_rpc(move |_| RpcReply::error(code, message)).await
}

pub async fn raw(body: &'static str) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(body, "application/json"))
        .mount(&server)
        .await;
    server
}

struct Delayed<R> {
    inner: R,
    delay: std::time::Duration,
}

impl<R: Respond> Respond for Delayed<R> {
    fn respond(&self, request: &wiremock::Request) -> ResponseTemplate {
        self.inner.respond(request).set_delay(self.delay)
    }
}

pub async fn slow(result: impl Into<Value>, delay: std::time::Duration) -> MockServer {
    let server = MockServer::start().await;
    slow_down(&server, result, delay).await;
    server
}

pub async fn slow_down(server: &MockServer, result: impl Into<Value>, delay: std::time::Duration) {
    let result = result.into();
    server.reset().await;
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(Delayed {
            inner: RpcResponder(move |_: &Value| result.clone()),
            delay,
        })
        .mount(server)
        .await;
}
