mod common;

use std::fs;
use std::sync::OnceLock;

use axum::body::{Body, to_bytes};
use axum::http::Request;
use justlog::config::Config;
use justlog::model::CanonicalEvent;
use serde_json::Value;
use tempfile::TempDir;

use common::{TestHarness, assert_status_ok, privmsg};

fn env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

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

#[tokio::test]
async fn legacy_txt_missing_only_uses_txt_when_native_missing() {
    let _guard = env_lock().lock().await;
    let temp = TempDir::new().unwrap();
    let legacy_root = temp.path().join("legacy");
    fs::create_dir_all(legacy_root.join("1/2024/1")).unwrap();
    fs::write(
        legacy_root.join("1/2024/1/2.txt"),
        "[0:00:04] Foo-Bar!: hello legacy\n[0:00:05] AnotherUser: second",
    )
    .unwrap();
    unsafe {
        std::env::set_var("JUSTLOG_LEGACY_TXT_ENABLED", "1");
        std::env::set_var("JUSTLOG_LEGACY_TXT_ROOT", legacy_root.as_os_str());
        std::env::remove_var("JUSTLOG_LEGACY_TXT_CHECK_EACH_REQUEST");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_MODE");
    }

    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let body = harness
        .response_text(
            Request::builder()
                .uri("/channelid/1/2024/1/2")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

    assert!(body.contains("#channelone foobar: hello legacy"));
    assert!(body.contains("#channelone anotheruser: second"));

    unsafe {
        std::env::remove_var("JUSTLOG_LEGACY_TXT_ENABLED");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_ROOT");
    }
}

#[tokio::test]
async fn legacy_txt_merge_mode_merges_with_native_and_list_exposes_txt_days() {
    let _guard = env_lock().lock().await;
    let temp = TempDir::new().unwrap();
    let legacy_root = temp.path().join("legacy");
    fs::create_dir_all(legacy_root.join("1/2024/1")).unwrap();
    fs::write(
        legacy_root.join("1/2024/1/2.txt"),
        "[0:00:03] LegacyUser: earliest\n[0:00:05] LegacyUser: latest",
    )
    .unwrap();
    fs::write(
        legacy_root.join("1/2024/1/3.txt"),
        "[0:00:01] LegacyUser: txt only day",
    )
    .unwrap();
    unsafe {
        std::env::set_var("JUSTLOG_LEGACY_TXT_ENABLED", "1");
        std::env::set_var("JUSTLOG_LEGACY_TXT_ROOT", legacy_root.as_os_str());
        std::env::set_var("JUSTLOG_LEGACY_TXT_MODE", "merge");
    }

    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let event = CanonicalEvent::from_raw(&privmsg(
        "native-merge-1",
        "1",
        "200",
        "viewer",
        "viewer",
        "channelone",
        1_704_153_604_000,
        "native middle",
    ))
    .unwrap()
    .unwrap();
    harness.seed_channel_event(&event);

    let body = harness
        .response_text(
            Request::builder()
                .uri("/channelid/1/2024/1/2")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    let earliest = body.find("earliest").unwrap();
    let middle = body.find("native middle").unwrap();
    let latest = body.find("latest").unwrap();
    assert!(earliest < middle && middle < latest);

    let response = harness
        .request(
            Request::builder()
                .uri("/list?channelid=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert!(
        json["availableLogs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["day"] == "3")
    );

    unsafe {
        std::env::remove_var("JUSTLOG_LEGACY_TXT_ENABLED");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_ROOT");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_MODE");
    }
}

#[tokio::test]
async fn legacy_txt_recurses_under_root_and_prunes_empty_dirs() {
    let _guard = env_lock().lock().await;
    let temp = TempDir::new().unwrap();
    let legacy_root = temp.path().join("legacy");
    fs::create_dir_all(legacy_root.join("copied/justlog/tree/1/2024/1")).unwrap();
    fs::create_dir_all(legacy_root.join("copied/empty/a/b")).unwrap();
    fs::write(
        legacy_root.join("copied/justlog/tree/1/2024/1/2.txt"),
        "[0:00:04] RecurseUser: found recursively",
    )
    .unwrap();
    unsafe {
        std::env::set_var("JUSTLOG_LEGACY_TXT_ENABLED", "1");
        std::env::set_var("JUSTLOG_LEGACY_TXT_ROOT", legacy_root.as_os_str());
        std::env::set_var("JUSTLOG_LEGACY_TXT_MODE", "missing_only");
        std::env::set_var("JUSTLOG_LEGACY_TXT_CHECK_EACH_REQUEST", "1");
    }

    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let body = harness
        .response_text(
            Request::builder()
                .uri("/channelid/1/2024/1/2")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

    assert!(body.contains("found recursively"));
    assert!(!legacy_root.join("copied/empty/a/b").exists());
    assert!(!legacy_root.join("copied/empty/a").exists());

    unsafe {
        std::env::remove_var("JUSTLOG_LEGACY_TXT_ENABLED");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_ROOT");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_MODE");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_CHECK_EACH_REQUEST");
    }
}

#[tokio::test]
async fn legacy_txt_off_and_parse_failures_do_not_break_requests() {
    let _guard = env_lock().lock().await;
    let temp = TempDir::new().unwrap();
    let legacy_root = temp.path().join("legacy");
    fs::create_dir_all(legacy_root.join("1/2024/1")).unwrap();
    fs::write(legacy_root.join("1/2024/1/2.txt"), "not a parseable line").unwrap();
    unsafe {
        std::env::set_var("JUSTLOG_LEGACY_TXT_ENABLED", "1");
        std::env::set_var("JUSTLOG_LEGACY_TXT_ROOT", legacy_root.as_os_str());
        std::env::set_var("JUSTLOG_LEGACY_TXT_MODE", "off");
        std::env::set_var("JUSTLOG_LEGACY_TXT_CHECK_EACH_REQUEST", "1");
    }

    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let body = harness
        .response_text(
            Request::builder()
                .uri("/channelid/1/2024/1/2")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(body, "");

    unsafe {
        std::env::set_var("JUSTLOG_LEGACY_TXT_MODE", "merge");
    }
    let body = harness
        .response_text(
            Request::builder()
                .uri("/channelid/1/2024/1/2")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(body, "");

    unsafe {
        std::env::remove_var("JUSTLOG_LEGACY_TXT_ENABLED");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_ROOT");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_MODE");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_CHECK_EACH_REQUEST");
    }
}
