mod common;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};

use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use justlog::model::CanonicalEvent;
use justlog::recent_messages::RecentMessagesRuntime;
use tokio::time::{Duration, sleep};

use common::{MockJustLogServer, TestHarness, privmsg};

fn recent_messages_runtime(base_url: &str) -> RecentMessagesRuntime {
    let mut vars = HashMap::new();
    vars.insert("JUSTLOG_RECENT_MESSAGES_ENABLED".to_string(), "1".to_string());
    vars.insert("JUSTLOG_RECENT_MESSAGES_URL".to_string(), base_url.to_string());
    RecentMessagesRuntime::from_summary(RecentMessagesRuntime::summary_from_map(&vars))
}

async fn wait_for_event_count(harness: &TestHarness, expected: i64) {
    for _ in 0..40 {
        if harness.state.store.event_count().unwrap() == expected {
            return;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(harness.state.store.event_count().unwrap(), expected);
}

#[tokio::test]
async fn mirrored_ingest_deduplicates_and_recovers_from_reconnect() {
    let harness = TestHarness::start(vec!["1".to_string()]).await;
    harness.irc.wait_for_connections(2).await;

    harness
        .irc
        .broadcast_line(&privmsg(
            "m1",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_067_200_000,
            "hello",
        ))
        .await;
    sleep(Duration::from_millis(250)).await;
    assert_eq!(harness.state.store.event_count().unwrap(), 1);

    harness.irc.reconnect_first().await;
    harness
        .irc
        .broadcast_line(&privmsg(
            "m2",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_067_201_000,
            "after reconnect",
        ))
        .await;
    sleep(Duration::from_millis(500)).await;
    harness.irc.wait_for_connections(3).await;

    harness
        .irc
        .broadcast_line(&privmsg(
            "m3",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_067_202_000,
            "after recovery",
        ))
        .await;
    sleep(Duration::from_millis(500)).await;

    assert_eq!(harness.state.store.event_count().unwrap(), 3);
}

#[tokio::test]
async fn reconnect_backfill_loads_missed_messages_from_robotty() {
    let remote = MockJustLogServer::start().await;
    remote
        .set_json(
            "/api/v2/recent-messages/channelone?limit=800",
            serde_json::json!({ "messages": [], "error": null, "error_code": null }),
        )
        .await;
    let harness = TestHarness::start_with_recent_messages_runtime(
        vec!["1".to_string()],
        true,
        Arc::new(recent_messages_runtime(&format!("{}/api/v2/recent-messages", remote.base_url()))),
    )
    .await;
    harness.irc.wait_for_connections(2).await;

    remote
        .set_json(
            "/api/v2/recent-messages/channelone?limit=800",
            serde_json::json!({
                "messages": [privmsg(
                    "robotty-1",
                    "1",
                    "200",
                    "viewer",
                    "viewer",
                    "channelone",
                    1_704_067_250_000,
                    "missed from robotty"
                )],
                "error": null,
                "error_code": null
            }),
        )
        .await;

    harness.irc.reconnect_first().await;
    harness.irc.wait_for_connections(3).await;
    wait_for_event_count(&harness, 1).await;
}

#[tokio::test]
async fn runtime_join_triggers_recent_message_backfill() {
    let remote = MockJustLogServer::start().await;
    remote
        .set_json(
            "/api/v2/recent-messages/channelone?limit=800",
            serde_json::json!({ "messages": [], "error": null, "error_code": null }),
        )
        .await;
    remote
        .set_json(
            "/api/v2/recent-messages/channeltwo?limit=800",
            serde_json::json!({
                "messages": [privmsg(
                    "robotty-join",
                    "2",
                    "200",
                    "viewer",
                    "viewer",
                    "channeltwo",
                    1_704_067_260_000,
                    "joined backfill"
                )],
                "error": null,
                "error_code": null
            }),
        )
        .await;
    let harness = TestHarness::start_with_recent_messages_runtime(
        vec!["1".to_string()],
        true,
        Arc::new(recent_messages_runtime(&format!("{}/api/v2/recent-messages", remote.base_url()))),
    )
    .await;
    harness.irc.wait_for_connections(2).await;

    harness
        .state
        .ingest
        .read()
        .await
        .clone()
        .unwrap()
        .join_channels(&["channeltwo".to_string()])
        .await;

    wait_for_event_count(&harness, 1).await;
}

#[tokio::test]
async fn robotty_backfill_does_not_duplicate_existing_events() {
    let remote = MockJustLogServer::start().await;
    let duplicate_raw = privmsg(
        "robotty-dup",
        "1",
        "200",
        "viewer",
        "viewer",
        "channelone",
        1_704_067_270_000,
        "duplicate message",
    );
    remote
        .set_json(
            "/api/v2/recent-messages/channelone?limit=800",
            serde_json::json!({ "messages": [], "error": null, "error_code": null }),
        )
        .await;
    let harness = TestHarness::start_with_recent_messages_runtime(
        vec!["1".to_string()],
        true,
        Arc::new(recent_messages_runtime(&format!("{}/api/v2/recent-messages", remote.base_url()))),
    )
    .await;
    harness.irc.wait_for_connections(2).await;

    let event = CanonicalEvent::from_raw(&duplicate_raw).unwrap().unwrap();
    harness.seed_channel_event(&event);
    remote
        .set_json(
            "/api/v2/recent-messages/channelone?limit=800",
            serde_json::json!({ "messages": [duplicate_raw], "error": null, "error_code": null }),
        )
        .await;

    harness.irc.reconnect_first().await;
    harness.irc.wait_for_connections(3).await;
    wait_for_event_count(&harness, 1).await;
}

#[tokio::test]
async fn robotty_api_errors_do_not_break_live_ingest() {
    let remote = MockJustLogServer::start().await;
    remote
        .set_json(
            "/api/v2/recent-messages/channelone?limit=800",
            serde_json::json!({
                "messages": [],
                "error": "The bot is currently not joined to this channel",
                "error_code": "channel_not_joined"
            }),
        )
        .await;
    let harness = TestHarness::start_with_recent_messages_runtime(
        vec!["1".to_string()],
        true,
        Arc::new(recent_messages_runtime(&format!("{}/api/v2/recent-messages", remote.base_url()))),
    )
    .await;
    harness.irc.wait_for_connections(2).await;

    harness
        .irc
        .broadcast_line(&privmsg(
            "live-after-error",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_067_280_000,
            "live still works",
        ))
        .await;
    wait_for_event_count(&harness, 1).await;
}

#[tokio::test]
async fn malformed_robotty_payload_lines_are_ignored_without_crashing_ingest() {
    let remote = MockJustLogServer::start().await;
    remote
        .set_json(
            "/api/v2/recent-messages/channelone?limit=800",
            serde_json::json!({
                "messages": [
                    "not raw irc",
                    ":tmi.twitch.tv NOTICE * :maintenance",
                    privmsg(
                        "robotty-good",
                        "1",
                        "200",
                        "viewer",
                        "viewer",
                        "channelone",
                        1_704_067_290_000,
                        "good one"
                    )
                ],
                "error": null,
                "error_code": null
            }),
        )
        .await;
    let harness = TestHarness::start_with_recent_messages_runtime(
        vec!["1".to_string()],
        true,
        Arc::new(recent_messages_runtime(&format!("{}/api/v2/recent-messages", remote.base_url()))),
    )
    .await;
    harness.irc.wait_for_connections(2).await;
    wait_for_event_count(&harness, 1).await;

    harness
        .irc
        .broadcast_line(&privmsg(
            "live-after-malformed",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_067_291_000,
            "still alive",
        ))
        .await;
    wait_for_event_count(&harness, 2).await;
}

#[tokio::test]
async fn ingest_stop_prevents_reconnect_after_disconnect() {
    let harness = TestHarness::start(vec!["1".to_string()]).await;
    harness.irc.wait_for_connections(2).await;

    harness.irc.reconnect_first().await;
    sleep(Duration::from_millis(100)).await;
    harness
        .state
        .ingest
        .read()
        .await
        .clone()
        .expect("ingest should be running")
        .stop();

    sleep(Duration::from_millis(1_000)).await;
    assert_eq!(harness.irc.connection_count().await, 2);
}

#[tokio::test]
async fn anonymous_ingest_uses_placeholder_pass_and_generated_justinfan_nick() {
    let harness = TestHarness::start_anonymous(vec!["1".to_string()]).await;
    harness.irc.wait_for_connections(2).await;
    sleep(Duration::from_millis(250)).await;

    let outgoing = harness.irc.client_lines().await.join("\n");
    assert!(outgoing.contains("PASS _"));
    assert!(outgoing.contains("JOIN #channelone"));

    let nick_line = harness
        .irc
        .client_lines()
        .await
        .into_iter()
        .find(|line| line.starts_with("NICK justinfan"))
        .expect("anonymous ingest should send a generated justinfan nick");
    let suffix = nick_line.trim_start_matches("NICK justinfan");
    assert!(!suffix.is_empty());
    assert!(suffix.chars().all(|ch| ch.is_ascii_digit()));
    assert!(!outgoing.contains("PASS oauth:"));
}

#[tokio::test]
async fn channels_list_and_lowercase_redirect_are_compatible() {
    let harness = TestHarness::start(vec!["1".to_string()]).await;
    harness.irc.wait_for_connections(2).await;

    let channels = harness
        .request(
            Request::builder()
                .uri("/channels")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(channels.status(), StatusCode::OK);
    let body = to_bytes(channels.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["channels"][0]["name"], "channelone");

    let redirect = harness
        .request(
            Request::builder()
                .uri("/CHANNELID/1")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(redirect.status(), StatusCode::FOUND);
    assert_eq!(redirect.headers().get("location").unwrap(), "/channelid/1");
}

#[tokio::test]
async fn admin_and_chat_commands_update_config_and_emit_join_part() {
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
    assert!(
        harness
            .state
            .config
            .read()
            .await
            .channels
            .contains(&"2".to_string())
    );

    harness
        .irc
        .broadcast_line(&privmsg(
            "cmd-join",
            "1",
            "100",
            "adminuser",
            "adminuser",
            "channelone",
            1_704_067_210_000,
            "!justlog part channeltwo",
        ))
        .await;
    sleep(Duration::from_millis(500)).await;
    assert!(
        !harness
            .state
            .config
            .read()
            .await
            .channels
            .contains(&"2".to_string())
    );
    let outgoing = harness.irc.client_lines().await.join("\n");
    assert!(outgoing.contains("JOIN #channeltwo"));
    assert!(outgoing.contains("PART #channeltwo"));
}

#[tokio::test]
async fn optout_flow_updates_config_and_blocks_queries() {
    let harness = TestHarness::start(vec!["1".to_string()]).await;
    harness.irc.wait_for_connections(2).await;

    let optout = harness
        .request(
            Request::builder()
                .method(Method::POST)
                .uri("/optout")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    let code = String::from_utf8(
        to_bytes(optout.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec(),
    )
    .unwrap();
    let code = serde_json::from_str::<String>(&code).unwrap();

    harness
        .irc
        .broadcast_line(&privmsg(
            "optout",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_067_220_000,
            &format!("!justlog optout {code}"),
        ))
        .await;
    sleep(Duration::from_millis(500)).await;

    assert!(
        harness
            .state
            .config
            .read()
            .await
            .opt_out
            .contains_key("200")
    );
    let list = harness
        .request(
            Request::builder()
                .uri("/list?channelid=1&userid=200")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(list.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn expired_optout_codes_cannot_be_redeemed() {
    let harness = TestHarness::start(vec!["1".to_string()]).await;
    harness.state.optout_codes.lock().await.insert(
        "expired1".to_string(),
        Instant::now() - StdDuration::from_secs(1),
    );

    harness
        .irc
        .broadcast_line(&privmsg(
            "expired-optout",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_067_221_000,
            "!justlog optout expired1",
        ))
        .await;
    sleep(Duration::from_millis(500)).await;

    assert!(
        !harness
            .state
            .config
            .read()
            .await
            .opt_out
            .contains_key("200")
    );
}

#[tokio::test]
async fn user_month_reads_include_messages_from_compacted_channel_days() {
    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let first = CanonicalEvent::from_raw(&privmsg(
        "user-month-1",
        "1",
        "200",
        "viewer",
        "viewer",
        "channelone",
        1_704_153_600_000,
        "from compacted day",
    ))
    .unwrap()
    .unwrap();
    let second = CanonicalEvent::from_raw(&privmsg(
        "user-month-2",
        "1",
        "200",
        "viewer",
        "viewer",
        "channelone",
        1_704_240_000_000,
        "still hot",
    ))
    .unwrap()
    .unwrap();
    harness.seed_channel_event(&first);
    harness.seed_channel_event(&second);
    harness.compact_channel_day("1", 2024, 1, 2);

    let response = harness
        .request(
            Request::builder()
                .uri("/channelid/1/userid/200/2024/1?json=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await;

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let messages = json["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 2);
    assert!(
        messages
            .iter()
            .any(|message| message["text"] == "from compacted day")
    );
    assert!(
        messages
            .iter()
            .any(|message| message["text"] == "still hot")
    );
}

#[tokio::test]
async fn raw_route_can_passthrough_brotli_or_fallback_to_transformed_brotli() {
    let harness = TestHarness::start(vec!["1".to_string()]).await;
    let event = CanonicalEvent::from_raw(&privmsg(
        "archive-1",
        "1",
        "200",
        "viewer",
        "viewer",
        "channelone",
        1_704_153_600_000,
        "archived",
    ))
    .unwrap()
    .unwrap();
    harness.seed_channel_event(&event);
    harness.compact_channel_day("1", 2024, 1, 2);

    let raw = harness
        .request(
            Request::builder()
                .uri("/channelid/1/2024/1/2?raw=1")
                .header("accept-encoding", "br")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(raw.status(), StatusCode::OK);
    assert_eq!(raw.headers().get("content-encoding").unwrap(), "br");
    let raw_bytes = to_bytes(raw.into_body(), usize::MAX).await.unwrap();
    let decoded = {
        let mut output = String::new();
        let mut decoder = brotli::Decompressor::new(raw_bytes.as_ref(), 16 * 1024);
        use std::io::Read as _;
        decoder.read_to_string(&mut output).unwrap();
        output
    };
    assert!(decoded.contains("archived"));

    let fallback = harness
        .request(
            Request::builder()
                .uri("/channelid/1/2024/1/2?raw=1&reverse=1")
                .header("accept-encoding", "br")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(fallback.status(), StatusCode::OK);
    assert_eq!(fallback.headers().get("content-encoding").unwrap(), "br");
}

#[tokio::test]
async fn admin_channel_mutation_requires_valid_api_key() {
    let harness = TestHarness::start(vec!["1".to_string()]).await;

    let response = harness
        .request(
            Request::builder()
                .method(Method::POST)
                .uri("/admin/channels")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"channels":["2"]}"#))
                .unwrap(),
        )
        .await;

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    assert_eq!(harness.state.config.read().await.channels, vec!["1"]);
}

#[tokio::test]
async fn admin_channel_mutation_rejects_unknown_channel_ids() {
    let harness = TestHarness::start(vec!["1".to_string()]).await;

    let response = harness
        .request(
            Request::builder()
                .method(Method::POST)
                .uri("/admin/channels")
                .header("X-Api-Key", "secret")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"channels":["2","999"]}"#))
                .unwrap(),
        )
        .await;

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert_eq!(harness.state.config.read().await.channels, vec!["1"]);
}
