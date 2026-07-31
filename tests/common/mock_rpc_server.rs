use serde_json::json;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::{method, path}};

pub async fn get(result: &str)-> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jsonrpc": "2.0", "id": 1, "result": result
        })))
        .mount(&server)
        .await;
    server
}