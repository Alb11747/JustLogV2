mod common;

use std::fs;

use axum::body::{Body, to_bytes};
use axum::http::Request;
use justlog::config::Config;
use justlog::model::CanonicalEvent;
use serde_json::Value;
use tempfile::TempDir;

use common::{TestHarness, assert_status_ok, privmsg};

#[tokio::test]
async fn basic_routes_are_healthy() {
    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;

    assert_status_ok(&harness, "/").await;
    assert_status_ok(&harness, "/healthz").await;
    assert_status_ok(&harness, "/readyz").await;
}

#[tokio::test]
async fn channels_route_lists_resolved_channels() {
    let harness = TestHarness::start(vec!["1".to_string()]).await;

    let response = harness
        .request(
            Request::builder()
                .uri("/channels")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), http::StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(json["channels"][0]["userID"], "1");
    assert_eq!(json["channels"][0]["name"], "channelone");
}

#[tokio::test]
async fn uppercase_log_paths_redirect_to_lowercase() {
    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;

    let response = harness
        .request(
            Request::builder()
                .uri("/CHANNELID/1/2024/1/2")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

    assert_eq!(response.status(), http::StatusCode::FOUND);
    assert_eq!(
        response.headers().get("location").unwrap(),
        "/channelid/1/2024/1/2"
    );
}

#[tokio::test]
async fn seeded_archived_raw_route_returns_brotli_payload() {
    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let event = CanonicalEvent::from_raw(&privmsg(
        "smoke-archive-1",
        "1",
        "200",
        "viewer",
        "viewer",
        "channelone",
        1_704_153_600_000,
        "archived smoke",
    ))
    .unwrap()
    .unwrap();
    harness.seed_channel_event(&event);
    harness.compact_channel_day("1", 2024, 1, 2);

    let response = harness
        .request(
            Request::builder()
                .uri("/channelid/1/2024/1/2?raw=1")
                .header("accept-encoding", "br")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

    assert_eq!(response.status(), http::StatusCode::OK);
    assert_eq!(response.headers().get("content-encoding").unwrap(), "br");
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let mut output = String::new();
    let mut decoder = brotli::Decompressor::new(body.as_ref(), 16 * 1024);
    use std::io::Read as _;
    decoder.read_to_string(&mut output).unwrap();
    assert!(output.contains("archived smoke"));
}

#[test]
fn config_load_normalizes_and_persists_defaults() {
    let temp = TempDir::new().unwrap();
    let config_path = temp.path().join("config.json");
    fs::write(
        &config_path,
        r#"{
  "clientID": "client",
  "clientSecret": "secret",
  "logsDirectory": ".\\logs\\",
  "oauth": "oauth:test-token",
  "logLevel": "INFO",
  "admins": ["AdminUser"],
  "channels": ["2", "1", "2"]
}"#,
    )
    .unwrap();

    let config = Config::load(&config_path).unwrap();
    assert_eq!(config.logs_directory.to_string_lossy(), "./logs");
    assert_eq!(
        config.storage.sqlite_path.to_string_lossy(),
        "./logs/justlog.sqlite3"
    );
    assert_eq!(config.oauth, "test-token");
    assert_eq!(config.log_level, "info");
    assert_eq!(config.admins, vec!["adminuser"]);
    assert_eq!(config.channels, vec!["2", "1"]);

    config.persist().unwrap();

    let persisted = Config::load(&config_path).unwrap();
    assert_eq!(persisted.oauth, "test-token");
    assert_eq!(persisted.log_level, "info");
    assert_eq!(persisted.admins, vec!["adminuser"]);
    assert_eq!(persisted.channels, vec!["2", "1"]);
}
