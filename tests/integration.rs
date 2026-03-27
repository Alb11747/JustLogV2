mod common;

use std::time::{Duration as StdDuration, Instant};

use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use justlog::model::CanonicalEvent;
use tokio::time::{Duration, sleep};

use common::{TestHarness, privmsg};

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
