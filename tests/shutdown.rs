use std::time::Duration;

use reqwest::StatusCode;

use crate::common::{mock_rpc_server, rpc, spawn_app_with_handle, test_settings};

mod common;

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::test]
async fn cancelling_the_token_stops_the_server() {
    let upstream = mock_rpc_server::ok("0x1").await;
    let app = spawn_app_with_handle(test_settings(vec![rpc("one", upstream.uri())])).await;

    let res = reqwest::Client::new()
        .get(format!("{}/healthz", app.addr))
        .send()
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    app.shutdown.cancel();

    let served = tokio::time::timeout(SHUTDOWN_TIMEOUT, app.server)
        .await
        .expect("server did not stop within the timeout")
        .expect("server task panicked");
    assert!(served.is_ok(), "graceful shutdown reported an error");
}

#[tokio::test]
async fn the_port_is_free_once_the_server_stops() {
    let app = spawn_app_with_handle(test_settings(vec![rpc("one", "http://127.0.0.1:1")])).await;
    let port = app
        .addr
        .rsplit(':')
        .next()
        .and_then(|port| port.parse::<u16>().ok())
        .expect("address should end in a port");

    app.shutdown.cancel();
    tokio::time::timeout(SHUTDOWN_TIMEOUT, app.server)
        .await
        .expect("server did not stop within the timeout")
        .expect("server task panicked")
        .expect("graceful shutdown reported an error");

    tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .expect("the port should be bindable again");
}
