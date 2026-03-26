mod common;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use reqwest::Client;
use tokio::time::{Duration, sleep};

use common::{MockHelix, TestHarness, privmsg};

#[tokio::test]
async fn http_server_round_trip_works_over_loopback() {
    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let server = harness.spawn_http_server().await;
    let client = Client::new();

    let response = client
        .get(format!("{}/healthz", server.base_url))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), "");
}

#[tokio::test]
async fn local_helix_mock_is_reachable_over_http() {
    let helix = MockHelix::start(vec![common::user("1", "channelone")]).await;
    let client = Client::new();

    let token = client
        .post(format!("http://{}/oauth2/token", helix.address))
        .send()
        .await
        .unwrap();
    assert_eq!(token.status(), reqwest::StatusCode::OK);
    assert!(token.text().await.unwrap().contains("access_token"));

    let users = client
        .get(format!(
            "http://{}/helix/users?login=channelone",
            helix.address
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(users.status(), reqwest::StatusCode::OK);
    assert!(users.text().await.unwrap().contains("channelone"));
}

#[tokio::test]
async fn irc_loopback_exercises_join_part_and_reconnect() {
    let harness = TestHarness::start(vec!["1".to_string()]).await;
    harness.irc.wait_for_connections(2).await;

    let response = harness
        .request(
            Request::builder()
                .method(Method::POST)
                .uri("/admin/channels")
                .header("X-Api-Key", "secret")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"channels":["2"]}"#))
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), StatusCode::OK);

    sleep(Duration::from_millis(250)).await;
    harness.irc.reconnect_first().await;
    harness
        .irc
        .broadcast_line(&privmsg(
            "net-1",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_067_250_000,
            "network hello",
        ))
        .await;

    sleep(Duration::from_millis(600)).await;
    harness.irc.wait_for_connections(3).await;
    let outgoing = harness.irc.client_lines().await.join("\n");
    assert!(outgoing.contains("JOIN #channelone"));
    assert!(outgoing.contains("JOIN #channeltwo"));
    assert!(harness.state.store.event_count().unwrap() >= 1);
}
