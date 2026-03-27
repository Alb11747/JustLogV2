mod common;

use std::fs;
use std::path::Path;
use std::sync::Arc;

use axum::body::Body;
use axum::http::Request;
use chrono::{Duration, Utc};
use justlog::debug_sync::{DebugRuntime, process_due_reconciliation_jobs, run_startup_validation};
use justlog::store::ReconciliationOutcome;
use serde_json::json;
use tempfile::TempDir;

use common::{MockJustLogServer, TestHarness, privmsg};

fn trusted_runtime(base_url: &str, log_dir: &Path) -> Arc<DebugRuntime> {
    let mut vars = std::collections::HashMap::new();
    vars.insert("JUSTLOG_DEBUG".to_string(), "1".to_string());
    vars.insert(
        "JUSTLOG_DEBUG_TRUSTED_COMPARE_URL".to_string(),
        base_url.to_string(),
    );
    vars.insert(
        "JUSTLOG_DEBUG_FALLBACK_TRUSTED_API".to_string(),
        "1".to_string(),
    );
    Arc::new(DebugRuntime::from_summary(log_dir, DebugRuntime::summary_from_map(&vars)).unwrap())
}

fn compare_runtime(base_url: &str, log_dir: &Path) -> Arc<DebugRuntime> {
    let mut vars = std::collections::HashMap::new();
    vars.insert("JUSTLOG_DEBUG".to_string(), "1".to_string());
    vars.insert(
        "JUSTLOG_DEBUG_COMPARE_URL".to_string(),
        base_url.to_string(),
    );
    Arc::new(DebugRuntime::from_summary(log_dir, DebugRuntime::summary_from_map(&vars)).unwrap())
}

fn startup_runtime(log_dir: &Path, value: &str) -> Arc<DebugRuntime> {
    let mut vars = std::collections::HashMap::new();
    vars.insert(
        "JUSTLOG_DEBUG_VALIDATE_CONSISTENCY_ON_STARTUP".to_string(),
        value.to_string(),
    );
    Arc::new(DebugRuntime::from_summary(log_dir, DebugRuntime::summary_from_map(&vars)).unwrap())
}

#[tokio::test]
async fn compaction_schedules_reconciliation_after_delay() {
    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let event = justlog::model::CanonicalEvent::from_raw(&privmsg(
        "schedule-1",
        "1",
        "200",
        "viewer",
        "viewer",
        "channelone",
        1_704_153_600_000,
        "scheduled",
    ))
    .unwrap()
    .unwrap();
    harness.seed_channel_event(&event);
    harness.compact_channel_day("1", 2024, 1, 2);

    let due_now = harness
        .state
        .store
        .due_reconciliation_jobs(Utc::now())
        .unwrap();
    assert!(due_now.is_empty());

    let due_later = harness
        .state
        .store
        .due_reconciliation_jobs(Utc::now() + Duration::hours(5))
        .unwrap();
    assert_eq!(due_later.len(), 1);
    assert_eq!(due_later[0].channel_id, "1");
    assert_eq!(due_later[0].day, 2);
}

#[tokio::test]
async fn trusted_reconciliation_repairs_archived_segment_and_logs_it() {
    let remote = MockJustLogServer::start().await;
    let log_dir = TempDir::new().unwrap();
    let runtime = trusted_runtime(&remote.base_url(), log_dir.path());
    let harness =
        TestHarness::start_with_debug_runtime(vec!["1".to_string()], false, runtime.clone()).await;
    let msg1 = privmsg(
        "repair-1",
        "1",
        "200",
        "viewer",
        "viewer",
        "channelone",
        1_704_153_600_000,
        "first",
    );
    let msg2 = privmsg(
        "repair-2",
        "1",
        "200",
        "viewer",
        "viewer",
        "channelone",
        1_704_153_601_000,
        "second",
    );
    let event = justlog::model::CanonicalEvent::from_raw(&msg1)
        .unwrap()
        .unwrap();
    harness.seed_channel_event(&event);
    harness.compact_channel_day("1", 2024, 1, 2);
    harness
        .state
        .store
        .schedule_reconciliation(
            "1",
            2024,
            1,
            2,
            "segments/channel/1/2024/1/2.br",
            Utc::now(),
        )
        .unwrap();
    remote
        .set_json(
            "/channelid/1/2024/1/2?json=1",
            json!({ "messages": [{ "raw": msg1 }, { "raw": msg2 }] }),
        )
        .await;

    process_due_reconciliation_jobs(runtime, harness.state.store.clone(), Utc::now())
        .await
        .unwrap();

    let repaired = harness
        .state
        .store
        .read_channel_logs("1", 2024, 1, 2)
        .unwrap();
    assert_eq!(repaired.len(), 2);
    assert!(
        !harness
            .state
            .store
            .channel_day_is_unhealthy("1", 2024, 1, 2)
            .unwrap()
    );

    let log = fs::read_to_string(log_dir.path().join("reconciliation.log")).unwrap();
    assert!(log.contains("repaired channel-day 1/2024/1/2"));
}

#[tokio::test]
async fn compare_only_reconciliation_logs_conflicts_without_mutating_archive() {
    let remote = MockJustLogServer::start().await;
    let log_dir = TempDir::new().unwrap();
    let runtime = compare_runtime(&remote.base_url(), log_dir.path());
    let harness =
        TestHarness::start_with_debug_runtime(vec!["1".to_string()], false, runtime.clone()).await;
    let msg1 = privmsg(
        "compare-1",
        "1",
        "200",
        "viewer",
        "viewer",
        "channelone",
        1_704_153_600_000,
        "first",
    );
    let msg2 = privmsg(
        "compare-2",
        "1",
        "200",
        "viewer",
        "viewer",
        "channelone",
        1_704_153_601_000,
        "second",
    );
    let event = justlog::model::CanonicalEvent::from_raw(&msg1)
        .unwrap()
        .unwrap();
    harness.seed_channel_event(&event);
    harness.compact_channel_day("1", 2024, 1, 2);
    harness
        .state
        .store
        .schedule_reconciliation(
            "1",
            2024,
            1,
            2,
            "segments/channel/1/2024/1/2.br",
            Utc::now(),
        )
        .unwrap();
    remote
        .set_json(
            "/channelid/1/2024/1/2?json=1",
            json!({ "messages": [{ "raw": msg1 }, { "raw": msg2 }] }),
        )
        .await;

    process_due_reconciliation_jobs(runtime, harness.state.store.clone(), Utc::now())
        .await
        .unwrap();

    let archived = harness
        .state
        .store
        .read_channel_logs("1", 2024, 1, 2)
        .unwrap();
    assert_eq!(archived.len(), 1);
    assert!(
        harness
            .state
            .store
            .channel_day_is_unhealthy("1", 2024, 1, 2)
            .unwrap()
    );
    let log = fs::read_to_string(log_dir.path().join("reconciliation.log")).unwrap();
    assert!(log.contains("missing locally"));
    assert!(log.contains("compare-2"));
}

#[tokio::test]
async fn fallback_proxies_dated_range_random_and_user_reads() {
    let remote = MockJustLogServer::start().await;
    let log_dir = TempDir::new().unwrap();
    let runtime = trusted_runtime(&remote.base_url(), log_dir.path());
    let harness =
        TestHarness::start_with_debug_runtime(vec!["1".to_string()], false, runtime).await;

    let msg = justlog::model::CanonicalEvent::from_raw(&privmsg(
        "fallback-1",
        "1",
        "200",
        "viewer",
        "viewer",
        "channelone",
        1_704_153_600_000,
        "local",
    ))
    .unwrap()
    .unwrap();
    harness.seed_channel_event(&msg);
    harness.compact_channel_day("1", 2024, 1, 2);
    let user_only = justlog::model::CanonicalEvent::from_raw(&privmsg(
        "fallback-user-1",
        "1",
        "200",
        "viewer",
        "viewer",
        "channelone",
        1_704_153_602_000,
        "local user",
    ))
    .unwrap()
    .unwrap();
    harness.seed_channel_event(&user_only);
    harness.compact_user_month("1", "200", 2024, 1);

    let logs_dir = harness.state.config.read().await.logs_directory.clone();
    fs::remove_file(logs_dir.join("segments/channel/1/2024/1/2.br")).unwrap();
    fs::remove_file(logs_dir.join("segments/user/1/200/2024/1.br")).unwrap();
    harness
        .state
        .store
        .record_reconciliation_outcome(
            &harness
                .state
                .store
                .due_reconciliation_jobs(Utc::now() + Duration::hours(5))
                .unwrap()[0]
                .clone(),
            &ReconciliationOutcome {
                checked_at: Utc::now(),
                status: "conflict".to_string(),
                conflict_count: 1,
                repair_status: "none".to_string(),
                unhealthy: 1,
                last_error: None,
            },
        )
        .unwrap();

    remote
        .set_text("/channelid/1/2024/1/2", "dated fallback")
        .await;
    remote
        .set_text(
            "/channelid/1/2024/1/2?from=1704153600&to=1704240000",
            "range fallback",
        )
        .await;
    remote
        .set_text("/channelid/1/random", "random fallback")
        .await;
    remote
        .set_json(
            "/channelid/1/userid/200/2024/1?json=1",
            json!({ "messages": [{ "raw": privmsg(
                "remote-user-1",
                "1",
                "200",
                "viewer",
                "viewer",
                "channelone",
                1_704_153_600_000,
                "remote user",
            ) }] }),
        )
        .await;

    let dated = harness
        .response_text(
            Request::builder()
                .uri("/channelid/1/2024/1/2")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(dated, "dated fallback");

    let range = harness
        .response_text(
            Request::builder()
                .uri("/channelid/1/2024/1/2?from=1704153600&to=1704240000")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(range, "range fallback");

    let random = harness
        .response_text(
            Request::builder()
                .uri("/channelid/1/random")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(random, "random fallback");

    let user = harness
        .response_text(
            Request::builder()
                .uri("/channelid/1/userid/200/2024/1?json=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert!(user.contains("remote-user-1"));
}

#[tokio::test]
async fn startup_validation_repairs_missing_user_month_data_and_updates_watermark() {
    let log_dir = TempDir::new().unwrap();
    let runtime = startup_runtime(log_dir.path(), "true");
    let harness =
        TestHarness::start_with_debug_runtime(vec!["1".to_string()], false, runtime.clone()).await;

    let msg1 = justlog::model::CanonicalEvent::from_raw(&privmsg(
        "startup-user-1",
        "1",
        "200",
        "viewer",
        "viewer",
        "channelone",
        1_704_153_600_000,
        "one",
    ))
    .unwrap()
    .unwrap();
    let msg2 = justlog::model::CanonicalEvent::from_raw(&privmsg(
        "startup-user-2",
        "1",
        "200",
        "viewer",
        "viewer",
        "channelone",
        1_704_153_601_000,
        "two",
    ))
    .unwrap()
    .unwrap();
    harness.seed_channel_event(&msg1);
    harness.seed_channel_event(&msg2);
    harness.compact_channel_day("1", 2024, 1, 2);
    harness.seed_channel_event(&msg1);
    harness.compact_user_month("1", "200", 2024, 1);

    run_startup_validation(runtime.clone(), harness.state.store.clone(), Utc::now())
        .await
        .unwrap();

    let repaired_user = harness
        .state
        .store
        .read_user_logs("1", "200", 2024, 1)
        .unwrap();
    assert_eq!(repaired_user.len(), 2);
    assert!(runtime.read_last_validated_time().unwrap().is_some());
}

#[tokio::test]
async fn startup_validation_repairs_missing_channel_data_from_user_month() {
    let log_dir = TempDir::new().unwrap();
    let runtime = startup_runtime(log_dir.path(), "true");
    let harness =
        TestHarness::start_with_debug_runtime(vec!["1".to_string()], false, runtime.clone()).await;

    let msg1 = justlog::model::CanonicalEvent::from_raw(&privmsg(
        "startup-channel-1",
        "1",
        "200",
        "viewer",
        "viewer",
        "channelone",
        1_704_153_600_000,
        "one",
    ))
    .unwrap()
    .unwrap();
    let msg2 = justlog::model::CanonicalEvent::from_raw(&privmsg(
        "startup-channel-2",
        "1",
        "200",
        "viewer",
        "viewer",
        "channelone",
        1_704_153_601_000,
        "two",
    ))
    .unwrap()
    .unwrap();
    harness.seed_channel_event(&msg1);
    harness.compact_channel_day("1", 2024, 1, 2);
    harness.seed_channel_event(&msg1);
    harness.seed_channel_event(&msg2);
    harness.compact_user_month("1", "200", 2024, 1);

    run_startup_validation(runtime, harness.state.store.clone(), Utc::now())
        .await
        .unwrap();

    let repaired_channel = harness
        .state
        .store
        .read_channel_logs("1", 2024, 1, 2)
        .unwrap();
    assert_eq!(repaired_channel.len(), 2);
}
