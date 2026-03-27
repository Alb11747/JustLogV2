mod common;

use std::fs;
use std::io::Write;
use std::sync::OnceLock;

use axum::body::{Body, to_bytes};
use axum::http::Request;
use justlog::app::resolve_channel_logins;
use justlog::config::Config;
use justlog::helix::HelixClient;
use justlog::model::CanonicalEvent;
use serde_json::Value;
use tempfile::TempDir;

use common::{TestHarness, assert_status_ok, privmsg};

fn env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn write_gzip(path: &std::path::Path, content: &str) {
    let file = fs::File::create(path).unwrap();
    let mut encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
    encoder.write_all(content.as_bytes()).unwrap();
    encoder.finish().unwrap();
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
    let _guard = env_lock().lock().await;
    unsafe {
        std::env::remove_var("JUSTLOG_IMPORT_FOLDER");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_MODE");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_CHECK_EACH_REQUEST");
        std::env::remove_var("JUSTLOG_IMPORT_DELETE_RAW");
        std::env::remove_var("JUSTLOG_IMPORT_DELETE_RECONSTRUCTED");
    }
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

    unsafe {
        std::env::remove_var("JUSTLOG_IMPORT_FOLDER");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_MODE");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_CHECK_EACH_REQUEST");
        std::env::remove_var("JUSTLOG_IMPORT_DELETE_RAW");
        std::env::remove_var("JUSTLOG_IMPORT_DELETE_RECONSTRUCTED");
    }
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
async fn startup_channel_logins_can_use_plain_logins_without_helix_credentials() {
    let temp = TempDir::new().unwrap();
    let config_path = temp.path().join("config.json");
    fs::write(
        &config_path,
        r#"{
  "logsDirectory": ".\\logs\\",
  "channels": ["channelone", "channeltwo", "12345"]
}"#,
    )
    .unwrap();

    let config = Config::load(&config_path).unwrap();
    let helix = HelixClient::new(&config);
    let logins = resolve_channel_logins(&helix, &config.channels)
        .await
        .unwrap();

    assert_eq!(logins, vec!["channelone", "channeltwo"]);
}

#[tokio::test]
async fn import_folder_raw_irc_txt_imports_into_native_store_without_duplication() {
    let _guard = env_lock().lock().await;
    let temp = TempDir::new().unwrap();
    let import_root = temp.path().join("imports");
    fs::create_dir_all(import_root.join("nested/1/2024/1")).unwrap();
    fs::write(
        import_root.join("nested/1/2024/1/2.txt"),
        privmsg(
            "import-raw-1",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_153_604_000,
            "raw imported",
        ),
    )
    .unwrap();
    unsafe {
        std::env::set_var("JUSTLOG_IMPORT_FOLDER", import_root.as_os_str());
        std::env::set_var("JUSTLOG_LEGACY_TXT_MODE", "missing_only");
    }

    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let first = harness
        .response_text(
            Request::builder()
                .uri("/channelid/1/2024/1/2")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    let second = harness
        .response_text(
            Request::builder()
                .uri("/channelid/1/2024/1/2")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

    assert!(first.contains("raw imported"));
    assert!(second.contains("raw imported"));
    assert_eq!(harness.state.store.event_count().unwrap(), 0);
    let archived = harness
        .state
        .store
        .read_archived_channel_segment_strict("1", 2024, 1, 2)
        .unwrap();
    assert_eq!(archived.len(), 1);
    assert_eq!(archived[0].text, "raw imported");

    unsafe {
        std::env::remove_var("JUSTLOG_IMPORT_FOLDER");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_MODE");
    }
}

#[tokio::test]
async fn import_folder_raw_import_merges_into_existing_archived_day() {
    let _guard = env_lock().lock().await;
    let temp = TempDir::new().unwrap();
    let import_root = temp.path().join("imports");
    let import_file = import_root.join("logs/archive-late.log");
    fs::create_dir_all(import_file.parent().unwrap()).unwrap();
    fs::write(
        &import_file,
        privmsg(
            "archived-import-2",
            "1",
            "201",
            "lateviewer",
            "lateviewer",
            "channelone",
            1_704_153_605_000,
            "late imported archived line",
        ),
    )
    .unwrap();
    unsafe {
        std::env::set_var("JUSTLOG_IMPORT_FOLDER", import_root.as_os_str());
        std::env::set_var("JUSTLOG_LEGACY_TXT_MODE", "missing_only");
    }

    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    harness.seed_channel_event(
        &CanonicalEvent::from_raw(&privmsg(
            "archived-import-1",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_153_604_000,
            "existing archived line",
        ))
        .unwrap()
        .unwrap(),
    );
    harness.compact_channel_day("1", 2024, 1, 2);
    assert_eq!(harness.state.store.event_count().unwrap(), 0);

    let body = harness
        .response_text(
            Request::builder()
                .uri("/channelid/1/2024/1/2")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

    assert!(body.contains("existing archived line"));
    assert!(body.contains("late imported archived line"));
    assert_eq!(harness.state.store.event_count().unwrap(), 0);

    let archived = harness
        .state
        .store
        .read_archived_channel_segment_strict("1", 2024, 1, 2)
        .unwrap();
    assert_eq!(archived.len(), 2);
    assert!(
        archived
            .iter()
            .any(|event| event.text == "existing archived line")
    );
    assert!(
        archived
            .iter()
            .any(|event| event.text == "late imported archived line")
    );

    unsafe {
        std::env::remove_var("JUSTLOG_IMPORT_FOLDER");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_MODE");
    }
}

#[tokio::test]
async fn admin_bulk_import_raw_dry_run_reports_v1_candidates() {
    let _guard = env_lock().lock().await;
    let temp = TempDir::new().unwrap();
    let import_root = temp.path().join("imports");
    let import_file = import_root.join("v1/1/2024/1/2/channel.txt.gz");
    fs::create_dir_all(import_file.parent().unwrap()).unwrap();
    write_gzip(
        &import_file,
        &privmsg(
            "bulk-dry-run-1",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_153_604_000,
            "bulk dry run",
        ),
    );
    unsafe {
        std::env::set_var("JUSTLOG_IMPORT_FOLDER", import_root.as_os_str());
    }

    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let response = harness
        .request(
            Request::builder()
                .method("POST")
                .uri("/admin/import/raw")
                .header("X-Api-Key", "secret")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"channel_id":"1","dry_run":true}"#))
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), http::StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["summary"]["dry_run"], true);
    assert_eq!(json["summary"]["files_selected"], 1);
    assert_eq!(json["summary"]["files_pending"], 1);
    assert_eq!(harness.state.store.event_count().unwrap(), 0);

    unsafe {
        std::env::remove_var("JUSTLOG_IMPORT_FOLDER");
    }
}

#[tokio::test]
async fn admin_bulk_import_raw_imports_v1_channel_tree() {
    let _guard = env_lock().lock().await;
    let temp = TempDir::new().unwrap();
    let import_root = temp.path().join("imports");
    let import_file = import_root.join("v1/1/2024/1/2/channel.txt.gz");
    fs::create_dir_all(import_file.parent().unwrap()).unwrap();
    write_gzip(
        &import_file,
        &privmsg(
            "bulk-import-1",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_153_604_000,
            "bulk imported route",
        ),
    );
    unsafe {
        std::env::set_var("JUSTLOG_IMPORT_FOLDER", import_root.as_os_str());
    }

    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let response = harness
        .request(
            Request::builder()
                .method("POST")
                .uri("/admin/import/raw")
                .header("X-Api-Key", "secret")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"channel_id":"1"}"#))
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), http::StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["summary"]["files_imported"], 1);
    assert_eq!(json["summary"]["affected_channel_days"], 1);

    let archived = harness
        .state
        .store
        .read_archived_channel_segment_strict("1", 2024, 1, 2)
        .unwrap();
    assert_eq!(archived.len(), 1);
    assert_eq!(archived[0].text, "bulk imported route");

    unsafe {
        std::env::remove_var("JUSTLOG_IMPORT_FOLDER");
    }
}

#[tokio::test]
async fn admin_bulk_import_raw_prefers_month_user_shard_skip_by_suffix_shape() {
    let _guard = env_lock().lock().await;
    let temp = TempDir::new().unwrap();
    let import_root = temp.path().join("imports");
    let channel_file = import_root.join("arbitrary/root/name/1/2024/1/2/channel.txt.gz");
    let user_file = import_root.join("arbitrary/root/name/1/2024/1/200.txt.gz");
    fs::create_dir_all(channel_file.parent().unwrap()).unwrap();
    write_gzip(
        &channel_file,
        &privmsg(
            "bulk-import-pref-channel",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_153_604_000,
            "preferred channel file",
        ),
    );
    write_gzip(
        &user_file,
        &privmsg(
            "bulk-import-pref-channel",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_153_604_000,
            "preferred channel file",
        ),
    );
    unsafe {
        std::env::set_var("JUSTLOG_IMPORT_FOLDER", import_root.as_os_str());
        std::env::set_var("JUSTLOG_IMPORT_V1_SKIP_SAMPLE_LINES", "1");
    }

    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let response = harness
        .request(
            Request::builder()
                .method("POST")
                .uri("/admin/import/raw")
                .header("X-Api-Key", "secret")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"channel_id":"1","dry_run":true}"#))
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), http::StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["summary"]["raw_candidates"], 2);
    assert_eq!(json["summary"]["files_selected"], 1);
    assert_eq!(json["summary"]["files_skipped_v1_preferred"], 1);

    let response = harness
        .request(
            Request::builder()
                .method("POST")
                .uri("/admin/import/raw")
                .header("X-Api-Key", "secret")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"channel_id":"1"}"#))
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), http::StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["summary"]["files_imported"], 1);
    assert_eq!(json["summary"]["files_skipped_v1_preferred"], 1);

    let archived = harness
        .state
        .store
        .read_archived_channel_segment_strict("1", 2024, 1, 2)
        .unwrap();
    assert_eq!(archived.len(), 1);
    assert_eq!(archived[0].text, "preferred channel file");

    unsafe {
        std::env::remove_var("JUSTLOG_IMPORT_FOLDER");
        std::env::remove_var("JUSTLOG_IMPORT_V1_SKIP_SAMPLE_LINES");
    }
}

#[tokio::test]
async fn admin_bulk_import_raw_keeps_month_user_shard_when_sample_id_missing_from_channel_data() {
    let _guard = env_lock().lock().await;
    let temp = TempDir::new().unwrap();
    let import_root = temp.path().join("imports");
    let channel_file = import_root.join("other/prefix/1/2024/1/2/channel.txt");
    let user_file = import_root.join("other/prefix/1/2024/1/200.txt.gz");
    fs::create_dir_all(channel_file.parent().unwrap()).unwrap();
    fs::write(
        &channel_file,
        privmsg(
            "bulk-import-keep-channel",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_153_604_000,
            "channel day line",
        ),
    )
    .unwrap();
    write_gzip(
        &user_file,
        &privmsg(
            "bulk-import-keep-user",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_153_605_000,
            "user shard unique line",
        ),
    );
    unsafe {
        std::env::set_var("JUSTLOG_IMPORT_FOLDER", import_root.as_os_str());
        std::env::set_var("JUSTLOG_IMPORT_V1_SKIP_SAMPLE_LINES", "1");
    }

    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let response = harness
        .request(
            Request::builder()
                .method("POST")
                .uri("/admin/import/raw")
                .header("X-Api-Key", "secret")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"channel_id":"1","dry_run":true}"#))
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), http::StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["summary"]["raw_candidates"], 2);
    assert_eq!(json["summary"]["files_selected"], 2);
    assert_eq!(json["summary"]["files_skipped_v1_preferred"], 0);

    unsafe {
        std::env::remove_var("JUSTLOG_IMPORT_FOLDER");
        std::env::remove_var("JUSTLOG_IMPORT_V1_SKIP_SAMPLE_LINES");
    }
}

#[tokio::test]
async fn admin_bulk_import_raw_can_disable_v1_month_shard_skip_optimization() {
    let _guard = env_lock().lock().await;
    let temp = TempDir::new().unwrap();
    let import_root = temp.path().join("imports");
    let channel_file = import_root.join("any/root/1/2024/1/2/channel.txt.gz");
    let user_file = import_root.join("any/root/1/2024/1/200.txt.gz");
    fs::create_dir_all(channel_file.parent().unwrap()).unwrap();
    write_gzip(
        &channel_file,
        &privmsg(
            "bulk-import-disable-skip",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_153_604_000,
            "same line",
        ),
    );
    write_gzip(
        &user_file,
        &privmsg(
            "bulk-import-disable-skip",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_153_604_000,
            "same line",
        ),
    );
    unsafe {
        std::env::set_var("JUSTLOG_IMPORT_FOLDER", import_root.as_os_str());
        std::env::set_var("JUSTLOG_IMPORT_V1_SKIP_SAMPLE_LINES", "1");
        std::env::set_var("JUSTLOG_IMPORT_V1_SKIP_OPTIMIZATION", "0");
    }

    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let response = harness
        .request(
            Request::builder()
                .method("POST")
                .uri("/admin/import/raw")
                .header("X-Api-Key", "secret")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"channel_id":"1","dry_run":true}"#))
                .unwrap(),
        )
        .await;
    assert_eq!(response.status(), http::StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["summary"]["raw_candidates"], 2);
    assert_eq!(json["summary"]["files_selected"], 2);
    assert_eq!(json["summary"]["files_skipped_v1_preferred"], 0);

    unsafe {
        std::env::remove_var("JUSTLOG_IMPORT_FOLDER");
        std::env::remove_var("JUSTLOG_IMPORT_V1_SKIP_SAMPLE_LINES");
        std::env::remove_var("JUSTLOG_IMPORT_V1_SKIP_OPTIMIZATION");
    }
}

#[tokio::test]
async fn import_folder_can_delete_raw_files_after_successful_import() {
    let _guard = env_lock().lock().await;
    let temp = TempDir::new().unwrap();
    let import_root = temp.path().join("imports");
    let import_file = import_root.join("nested/1/2024/1/2.txt");
    fs::create_dir_all(import_file.parent().unwrap()).unwrap();
    fs::write(
        &import_file,
        privmsg(
            "import-raw-delete-1",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_153_604_000,
            "raw imported then deleted",
        ),
    )
    .unwrap();
    unsafe {
        std::env::set_var("JUSTLOG_IMPORT_FOLDER", import_root.as_os_str());
        std::env::set_var("JUSTLOG_LEGACY_TXT_MODE", "missing_only");
        std::env::set_var("JUSTLOG_IMPORT_DELETE_RAW", "1");
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

    assert!(body.contains("raw imported then deleted"));
    assert!(!import_file.exists());
    assert!(!import_root.join("nested/1/2024/1").exists());

    unsafe {
        std::env::remove_var("JUSTLOG_IMPORT_FOLDER");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_MODE");
        std::env::remove_var("JUSTLOG_IMPORT_DELETE_RAW");
    }
}

#[tokio::test]
async fn import_folder_retries_files_left_in_importing_state() {
    let _guard = env_lock().lock().await;
    let temp = TempDir::new().unwrap();
    let import_root = temp.path().join("imports");
    let import_file = import_root.join("nested/1/2024/1/2.txt");
    fs::create_dir_all(import_file.parent().unwrap()).unwrap();
    fs::write(
        &import_file,
        privmsg(
            "import-raw-retry-1",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_153_604_000,
            "raw imported after retry",
        ),
    )
    .unwrap();
    unsafe {
        std::env::set_var("JUSTLOG_IMPORT_FOLDER", import_root.as_os_str());
        std::env::set_var("JUSTLOG_LEGACY_TXT_MODE", "missing_only");
    }

    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let fingerprint = format!(
        "{}:{}",
        fs::metadata(&import_file).unwrap().len(),
        fs::metadata(&import_file)
            .unwrap()
            .modified()
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    );
    harness
        .state
        .store
        .record_imported_raw_file(
            import_file.to_string_lossy().as_ref(),
            &fingerprint,
            "importing",
        )
        .unwrap();

    let body = harness
        .response_text(
            Request::builder()
                .uri("/channelid/1/2024/1/2")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

    assert!(body.contains("raw imported after retry"));
    assert_eq!(harness.state.store.event_count().unwrap(), 0);
    let archived = harness
        .state
        .store
        .read_archived_channel_segment_strict("1", 2024, 1, 2)
        .unwrap();
    assert_eq!(archived.len(), 1);
    assert_eq!(archived[0].text, "raw imported after retry");

    unsafe {
        std::env::remove_var("JUSTLOG_IMPORT_FOLDER");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_MODE");
    }
}

#[tokio::test]
async fn import_folder_can_delete_reconstructed_files_after_read() {
    let _guard = env_lock().lock().await;
    let temp = TempDir::new().unwrap();
    let import_root = temp.path().join("imports");
    let import_file = import_root.join("overlay/1/2024/1/2.json");
    fs::create_dir_all(import_file.parent().unwrap()).unwrap();
    fs::write(
        &import_file,
        r##"{"streamer":{"name":"channelone","id":1},"video":{"title":"vod title","id":"vod-1"},"comments":[{"_id":"json-delete-1","created_at":"2024-01-02T00:00:04Z","channel_id":"1","content_id":"vod-1","commenter":{"display_name":"JsonDeleteUser","_id":"300","name":"jsondeleteuser","logo":"https://example.com/logo.png"},"message":{"body":"json deleted after read","user_color":"#FF0000","user_badges":[],"emoticons":[]}}]}"##,
    )
    .unwrap();
    unsafe {
        std::env::set_var("JUSTLOG_IMPORT_FOLDER", import_root.as_os_str());
        std::env::set_var("JUSTLOG_LEGACY_TXT_MODE", "missing_only");
        std::env::set_var("JUSTLOG_IMPORT_DELETE_RECONSTRUCTED", "1");
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

    assert!(body.contains("json deleted after read"));
    assert!(!import_file.exists());
    assert!(!import_root.join("overlay/1/2024/1").exists());

    unsafe {
        std::env::remove_var("JUSTLOG_IMPORT_FOLDER");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_MODE");
        std::env::remove_var("JUSTLOG_IMPORT_DELETE_RECONSTRUCTED");
    }
}

#[tokio::test]
async fn import_folder_can_delete_already_consumed_reconstructed_files_on_later_request() {
    let _guard = env_lock().lock().await;
    let temp = TempDir::new().unwrap();
    let import_root = temp.path().join("imports");
    let import_file = import_root.join("overlay/1/2024/1/2.json");
    fs::create_dir_all(import_file.parent().unwrap()).unwrap();
    fs::write(
        &import_file,
        r##"{"streamer":{"name":"channelone","id":1},"video":{"title":"vod title","id":"vod-1"},"comments":[{"_id":"json-delete-later-1","created_at":"2024-01-02T00:00:04Z","channel_id":"1","content_id":"vod-1","commenter":{"display_name":"JsonDeleteLaterUser","_id":"300","name":"jsondeletelateruser","logo":"https://example.com/logo.png"},"message":{"body":"json deleted later","user_color":"#FF0000","user_badges":[],"emoticons":[]}}]}"##,
    )
    .unwrap();
    unsafe {
        std::env::set_var("JUSTLOG_IMPORT_FOLDER", import_root.as_os_str());
        std::env::set_var("JUSTLOG_LEGACY_TXT_MODE", "missing_only");
        std::env::set_var("JUSTLOG_IMPORT_DELETE_RECONSTRUCTED", "0");
        std::env::set_var("JUSTLOG_IMPORT_DELETE_ALREADY_IMPORTED_RECONSTRUCTED", "1");
    }

    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let first_body = harness
        .response_text(
            Request::builder()
                .uri("/channelid/1/2024/1/2")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

    assert!(first_body.contains("json deleted later"));
    assert!(import_file.exists());

    let second_body = harness
        .response_text(
            Request::builder()
                .uri("/channelid/1/2024/1/2")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

    assert!(!second_body.contains("json deleted later"));
    assert!(!import_file.exists());
    assert!(!import_root.join("overlay/1/2024/1").exists());

    unsafe {
        std::env::remove_var("JUSTLOG_IMPORT_FOLDER");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_MODE");
        std::env::remove_var("JUSTLOG_IMPORT_DELETE_RECONSTRUCTED");
        std::env::remove_var("JUSTLOG_IMPORT_DELETE_ALREADY_IMPORTED_RECONSTRUCTED");
    }
}

#[tokio::test]
async fn import_folder_reprocesses_changed_reconstructed_files() {
    let _guard = env_lock().lock().await;
    let temp = TempDir::new().unwrap();
    let import_root = temp.path().join("imports");
    let import_file = import_root.join("overlay/1/2024/1/2.txt");
    fs::create_dir_all(import_file.parent().unwrap()).unwrap();
    fs::write(&import_file, "[0:00:04] FirstUser: first value").unwrap();
    unsafe {
        std::env::set_var("JUSTLOG_IMPORT_FOLDER", import_root.as_os_str());
        std::env::set_var("JUSTLOG_LEGACY_TXT_MODE", "merge");
        std::env::set_var("JUSTLOG_IMPORT_DELETE_ALREADY_IMPORTED_RECONSTRUCTED", "1");
    }

    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let first_body = harness
        .response_text(
            Request::builder()
                .uri("/channelid/1/2024/1/2")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert!(first_body.contains("first value"));
    assert!(import_file.exists());

    std::thread::sleep(std::time::Duration::from_secs(1));
    fs::write(&import_file, "[0:00:05] SecondUser: second value").unwrap();

    let second_body = harness
        .response_text(
            Request::builder()
                .uri("/channelid/1/2024/1/2")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert!(second_body.contains("second value"));
    assert!(import_file.exists());

    unsafe {
        std::env::remove_var("JUSTLOG_IMPORT_FOLDER");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_MODE");
        std::env::remove_var("JUSTLOG_IMPORT_DELETE_ALREADY_IMPORTED_RECONSTRUCTED");
    }
}

#[tokio::test]
async fn import_folder_can_import_log_files_from_arbitrary_subdirs() {
    let _guard = env_lock().lock().await;
    let temp = TempDir::new().unwrap();
    let import_root = temp.path().join("imports");
    let import_file = import_root.join("logs.2/debug.1.log");
    fs::create_dir_all(import_file.parent().unwrap()).unwrap();
    fs::write(
        &import_file,
        format!(
            "2024-03-21 14:09:30.126 irc.client 333 DEBUG: FROM SERVER: {}\n2024-03-21 14:09:30.127 irc.client 1221 DEBUG: _dispatcher: all_raw_messages\n",
            privmsg(
                "import-log-anywhere-1",
                "1",
                "200",
                "viewer",
                "viewer",
                "channelone",
                1_710_990_570_007,
                "log import anywhere",
            )
        ),
    )
    .unwrap();
    unsafe {
        std::env::set_var("JUSTLOG_IMPORT_FOLDER", import_root.as_os_str());
        std::env::set_var("JUSTLOG_LEGACY_TXT_MODE", "missing_only");
    }

    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let body = harness
        .response_text(
            Request::builder()
                .uri("/channelid/1/2024/3/21")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

    assert!(body.contains("log import anywhere"));
    assert_eq!(harness.state.store.event_count().unwrap(), 0);
    let archived = harness
        .state
        .store
        .read_archived_channel_segment_strict("1", 2024, 3, 21)
        .unwrap();
    assert_eq!(archived.len(), 1);
    assert_eq!(archived[0].text, "log import anywhere");

    unsafe {
        std::env::remove_var("JUSTLOG_IMPORT_FOLDER");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_MODE");
    }
}

#[tokio::test]
async fn import_folder_merge_mode_merges_native_json_and_simple_text() {
    let _guard = env_lock().lock().await;
    let temp = TempDir::new().unwrap();
    let import_root = temp.path().join("imports");
    fs::create_dir_all(import_root.join("a/b/1/2024/1")).unwrap();
    fs::write(
        import_root.join("a/b/1/2024/1/2.txt"),
        "[0:00:03] LegacyUser: earliest\n[0:00:05] LegacyUser: latest",
    )
    .unwrap();
    fs::write(
        import_root.join("a/b/1/2024/1/2.json"),
        r##"{"streamer":{"name":"channelone","id":1},"video":{"title":"vod title","id":"vod-1"},"comments":[{"_id":"json-1","created_at":"2024-01-02T00:00:04Z","channel_id":"1","content_id":"vod-1","commenter":{"display_name":"JsonUser","_id":"300","name":"jsonuser","logo":"https://example.com/logo.png"},"message":{"body":"json middle","user_color":"#FF0000","user_badges":[{"_id":"vip","version":"1"}],"emoticons":[]}}]}"##,
    ).unwrap();
    unsafe {
        std::env::set_var("JUSTLOG_IMPORT_FOLDER", import_root.as_os_str());
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
    let json_middle = body.find("json middle").unwrap();
    let latest = body.find("latest").unwrap();
    assert!(earliest < middle && middle < latest);
    assert!(earliest < json_middle && json_middle < latest);
    assert!(body.contains("channelone jsonuser: json middle"));

    unsafe {
        std::env::remove_var("JUSTLOG_IMPORT_FOLDER");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_MODE");
    }
}

#[tokio::test]
async fn import_folder_list_and_gzip_support_work() {
    let _guard = env_lock().lock().await;
    let temp = TempDir::new().unwrap();
    let import_root = temp.path().join("imports");
    fs::create_dir_all(import_root.join("copied/raw/1/2024/1")).unwrap();
    fs::create_dir_all(import_root.join("copied/reconstructed/1/2024/1")).unwrap();
    fs::create_dir_all(import_root.join("copied/empty/a/b")).unwrap();
    write_gzip(
        &import_root.join("copied/raw/1/2024/1/2.txt.gz"),
        &privmsg(
            "import-raw-gz-1",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_153_605_000,
            "raw gz imported",
        ),
    );
    write_gzip(
        &import_root.join("copied/reconstructed/1/2024/1/3.json.gz"),
        r##"{"streamer":{"name":"channelone","id":1},"video":{"title":"vod title","id":"vod-2"},"comments":[{"_id":"json-gz-1","created_at":"2024-01-03T00:00:04Z","channel_id":"1","content_id":"vod-2","commenter":{"display_name":"JsonGzUser","_id":"301","name":"jsongzuser","logo":"https://example.com/logo.png"},"message":{"body":"json gz only","user_color":"#00FF00","user_badges":[],"emoticons":[]}}]}"##,
    );
    unsafe {
        std::env::set_var("JUSTLOG_IMPORT_FOLDER", import_root.as_os_str());
        std::env::set_var("JUSTLOG_LEGACY_TXT_MODE", "missing_only");
        std::env::set_var("JUSTLOG_LEGACY_TXT_CHECK_EACH_REQUEST", "1");
    }

    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let raw_body = harness
        .response_text(
            Request::builder()
                .uri("/channelid/1/2024/1/2")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert!(raw_body.contains("raw gz imported"));

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
            .any(|entry| entry["day"] == "2")
    );
    assert!(
        json["availableLogs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["day"] == "3")
    );
    assert!(!import_root.join("copied/empty/a/b").exists());
    assert!(!import_root.join("copied/empty/a").exists());

    unsafe {
        std::env::remove_var("JUSTLOG_IMPORT_FOLDER");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_MODE");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_CHECK_EACH_REQUEST");
    }
}

#[tokio::test]
async fn legacy_txt_mode_off_ignores_simple_text_but_keeps_complete_json() {
    let _guard = env_lock().lock().await;
    let temp = TempDir::new().unwrap();
    let import_root = temp.path().join("imports");
    fs::create_dir_all(import_root.join("1/2024/1")).unwrap();
    fs::write(
        import_root.join("1/2024/1/2.txt"),
        "[0:00:04] HiddenUser: simple hidden",
    )
    .unwrap();
    fs::write(
        import_root.join("1/2024/1/2.json"),
        r##"{"streamer":{"name":"channelone","id":1},"video":{"title":"vod title","id":"vod-3"},"comments":[{"_id":"json-visible-1","created_at":"2024-01-02T00:00:05Z","channel_id":"1","content_id":"vod-3","commenter":{"display_name":"VisibleUser","_id":"302","name":"visibleuser","logo":"https://example.com/logo.png"},"message":{"body":"json visible","user_color":"#0000FF","user_badges":[],"emoticons":[]}}]}"##,
    ).unwrap();
    unsafe {
        std::env::set_var("JUSTLOG_IMPORT_FOLDER", import_root.as_os_str());
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
    assert!(!body.contains("simple hidden"));
    assert!(body.contains("json visible"));

    unsafe {
        std::env::remove_var("JUSTLOG_IMPORT_FOLDER");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_MODE");
        std::env::remove_var("JUSTLOG_LEGACY_TXT_CHECK_EACH_REQUEST");
    }
}
