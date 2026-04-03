mod common;
// Core non-import integration coverage lives here.

use std::io::Read as _;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::http::{Method, Request, StatusCode};
use justlog::model::CanonicalEvent;
use justlog::recent_messages::RecentMessagesRuntime;
use serde_json::Value;
use tokio::time::timeout;

use common::{MockJustLogServer, TestHarness, TestHarnessOptions, privmsg};

fn env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn decode_brotli(bytes: &[u8]) -> String {
    let mut output = String::new();
    let mut decoder = brotli::Decompressor::new(bytes, 16 * 1024);
    decoder.read_to_string(&mut output).unwrap();
    output
}

fn recent_messages_runtime(base_url: &str) -> RecentMessagesRuntime {
    let mut vars = std::collections::HashMap::new();
    vars.insert(
        "JUSTLOG_RECENT_MESSAGES_ENABLED".to_string(),
        "1".to_string(),
    );
    vars.insert(
        "JUSTLOG_RECENT_MESSAGES_URL".to_string(),
        base_url.to_string(),
    );
    RecentMessagesRuntime::from_summary(RecentMessagesRuntime::summary_from_map(&vars))
}

#[tokio::test(start_paused = true)]
async fn meta_docs_and_metrics_routes_are_served() {
    let harness = TestHarness::start_with_options(TestHarnessOptions {
        start_ingest: false,
        metrics_enabled: true,
        ..Default::default()
    })
    .await;

    let root = harness
        .request(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await;
    assert_eq!(root.status(), StatusCode::FOUND);
    assert_eq!(root.headers().get("location").unwrap(), "/docs");

    for path in [
        "/healthz",
        "/readyz",
        "/openapi.yaml",
        "/docs",
        "/docs/rapidoc-min.js",
        "/metrics",
    ] {
        let response = harness
            .request(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await;
        assert_eq!(response.status(), StatusCode::OK, "{path}");
    }

    let openapi = harness
        .request(
            Request::builder()
                .uri("/openapi.yaml")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(
        openapi.headers().get("content-type").unwrap(),
        "application/yaml; charset=utf-8"
    );

    let docs = harness
        .response_text(Request::builder().uri("/docs").body(Body::empty()).unwrap())
        .await;
    assert!(docs.contains("spec-url=\"/openapi.yaml\""));
    assert!(docs.contains("src=\"/docs/rapidoc-min.js\""));

    let metrics = harness
        .response_text(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert!(metrics.contains("justlog_events_total"));
}

#[tokio::test(start_paused = true)]
async fn channel_admin_list_and_optout_routes_cover_core_controls() {
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

    let add = harness
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
    assert_eq!(add.status(), StatusCode::OK);
    harness
        .irc
        .wait_for_client_line_contains("JOIN #channeltwo")
        .await;

    let list_by_login = harness
        .request(
            Request::builder()
                .uri("/list?channel=channelone")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(list_by_login.status(), StatusCode::OK);

    let list_by_user = harness
        .request(
            Request::builder()
                .uri("/list?channelid=1&userid=200")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(list_by_user.status(), StatusCode::OK);

    let unknown = harness
        .request(
            Request::builder()
                .method(Method::POST)
                .uri("/admin/channels")
                .header("X-Api-Key", "secret")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"channels":["999"]}"#))
                .unwrap(),
        )
        .await;
    assert_eq!(unknown.status(), StatusCode::BAD_REQUEST);

    let forbidden = harness
        .request(
            Request::builder()
                .method(Method::DELETE)
                .uri("/admin/channels")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"channels":["2"]}"#))
                .unwrap(),
        )
        .await;
    assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);

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
            "optout-1",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_067_220_000,
            &format!("!justlog optout {code}"),
        ))
        .await;
    harness
        .irc
        .wait_for_client_line_contains("opted you out")
        .await;

    let blocked = harness
        .request(
            Request::builder()
                .uri("/list?channelid=1&userid=200")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(blocked.status(), StatusCode::FORBIDDEN);

    let remove = harness
        .request(
            Request::builder()
                .method(Method::DELETE)
                .uri("/admin/channels")
                .header("X-Api-Key", "secret")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"channels":["2"]}"#))
                .unwrap(),
        )
        .await;
    assert_eq!(remove.status(), StatusCode::OK);
    harness
        .irc
        .wait_for_client_line_contains("PART #channeltwo")
        .await;
}

#[tokio::test(start_paused = true)]
async fn cors_defaults_allow_any_origin_and_preflight() {
    let _guard = env_lock().lock().await;
    unsafe {
        std::env::remove_var("JUSTLOG_CORS_ENABLED");
        std::env::remove_var("JUSTLOG_CORS_ALLOW_ORIGINS");
        std::env::remove_var("JUSTLOG_CORS_ALLOW_METHODS");
        std::env::remove_var("JUSTLOG_CORS_ALLOW_HEADERS");
        std::env::remove_var("JUSTLOG_CORS_EXPOSE_HEADERS");
        std::env::remove_var("JUSTLOG_CORS_MAX_AGE_SECONDS");
    }

    let harness = TestHarness::start_with_options(TestHarnessOptions {
        start_ingest: false,
        ..Default::default()
    })
    .await;

    let get_response = harness
        .request(
            Request::builder()
                .uri("/healthz")
                .header("Origin", "https://app.example")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(
        get_response
            .headers()
            .get("access-control-allow-origin")
            .unwrap(),
        "*"
    );

    let preflight = harness
        .request(
            Request::builder()
                .method(Method::OPTIONS)
                .uri("/admin/channels")
                .header("Origin", "https://app.example")
                .header("Access-Control-Request-Method", "POST")
                .header("Access-Control-Request-Headers", "content-type,x-api-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(preflight.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        preflight
            .headers()
            .get("access-control-allow-origin")
            .unwrap(),
        "*"
    );
    assert_eq!(
        preflight
            .headers()
            .get("access-control-allow-methods")
            .unwrap(),
        "GET,POST,DELETE,OPTIONS"
    );
    assert_eq!(
        preflight
            .headers()
            .get("access-control-allow-headers")
            .unwrap(),
        "Content-Type,X-Api-Key"
    );
}

#[tokio::test(start_paused = true)]
async fn cors_can_be_disabled_or_restricted_by_env() {
    let _guard = env_lock().lock().await;
    unsafe {
        std::env::set_var("JUSTLOG_CORS_ENABLED", "0");
    }
    let disabled = TestHarness::start_with_options(TestHarnessOptions {
        start_ingest: false,
        ..Default::default()
    })
    .await;
    let disabled_response = disabled
        .request(
            Request::builder()
                .uri("/healthz")
                .header("Origin", "https://app.example")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert!(
        disabled_response
            .headers()
            .get("access-control-allow-origin")
            .is_none()
    );

    unsafe {
        std::env::set_var(
            "JUSTLOG_CORS_ALLOW_ORIGINS",
            "https://allowed.example, https://other.example",
        );
        std::env::set_var("JUSTLOG_CORS_ENABLED", "1");
        std::env::set_var("JUSTLOG_CORS_EXPOSE_HEADERS", "ETag");
    }
    let restricted = TestHarness::start_with_options(TestHarnessOptions {
        start_ingest: false,
        ..Default::default()
    })
    .await;

    let allowed = restricted
        .request(
            Request::builder()
                .uri("/healthz")
                .header("Origin", "https://allowed.example")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(
        allowed
            .headers()
            .get("access-control-allow-origin")
            .unwrap(),
        "https://allowed.example"
    );
    assert_eq!(allowed.headers().get("vary").unwrap(), "Origin");
    assert_eq!(
        allowed
            .headers()
            .get("access-control-expose-headers")
            .unwrap(),
        "ETag"
    );

    let denied = restricted
        .request(
            Request::builder()
                .uri("/healthz")
                .header("Origin", "https://denied.example")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert!(
        denied
            .headers()
            .get("access-control-allow-origin")
            .is_none()
    );
    assert_eq!(denied.headers().get("vary").unwrap(), "Origin");

    unsafe {
        std::env::remove_var("JUSTLOG_CORS_ENABLED");
        std::env::remove_var("JUSTLOG_CORS_ALLOW_ORIGINS");
        std::env::remove_var("JUSTLOG_CORS_ALLOW_METHODS");
        std::env::remove_var("JUSTLOG_CORS_ALLOW_HEADERS");
        std::env::remove_var("JUSTLOG_CORS_EXPOSE_HEADERS");
        std::env::remove_var("JUSTLOG_CORS_MAX_AGE_SECONDS");
    }
}

#[tokio::test(start_paused = true)]
async fn log_routes_cover_login_id_user_random_and_compressed_reads() {
    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let archived = CanonicalEvent::from_raw(&privmsg(
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
    let hot = CanonicalEvent::from_raw(&privmsg(
        "hot-1",
        "1",
        "200",
        "viewer",
        "viewer",
        "channelone",
        1_705_276_800_000,
        "hot",
    ))
    .unwrap()
    .unwrap();
    harness.seed_channel_event(&archived);
    harness.seed_channel_event(&hot);
    harness.compact_channel_day("1", 2024, 1, 2);

    let uppercase = harness
        .request(
            Request::builder()
                .uri("/CHANNELID/1")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(uppercase.status(), StatusCode::FOUND);
    assert_eq!(uppercase.headers().get("location").unwrap(), "/channelid/1");

    let channel_redirect = harness
        .request(
            Request::builder()
                .uri("/channel/channelone")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(channel_redirect.status(), StatusCode::FOUND);

    let user_redirect = harness
        .request(
            Request::builder()
                .uri("/channel/channelone/user/viewer")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(user_redirect.status(), StatusCode::FOUND);

    let channel_json = harness
        .request(
            Request::builder()
                .uri("/channel/channelone/2024/1/2?json=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(channel_json.status(), StatusCode::OK);

    let range_json = harness
        .request(
            Request::builder()
                .uri("/channelid/1/2024/1/15?json=1&from=1705276800&to=1705276800")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    assert_eq!(range_json.status(), StatusCode::OK);

    let user_month = harness
        .request(
            Request::builder()
                .uri("/channel/channelone/user/viewer/2024/1?json=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    let user_month_body = to_bytes(user_month.into_body(), usize::MAX).await.unwrap();
    let user_month_json: Value = serde_json::from_slice(&user_month_body).unwrap();
    assert_eq!(user_month_json["messages"].as_array().unwrap().len(), 2);

    for path in [
        "/channel/channelone/random?json=1",
        "/channel/channelone/user/viewer/random?json=1",
        "/channelid/1/random?json=1",
        "/channelid/1/userid/200/random?json=1",
        "/channelid/1/userid/200/2024/1?json=1",
    ] {
        let response = harness
            .request(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await;
        assert_eq!(response.status(), StatusCode::OK, "{path}");
    }

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
    let raw_body = to_bytes(raw.into_body(), usize::MAX).await.unwrap();
    assert!(decode_brotli(&raw_body).contains("archived"));

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
    let fallback_body = to_bytes(fallback.into_body(), usize::MAX).await.unwrap();
    assert!(decode_brotli(&fallback_body).contains("archived"));
}

#[tokio::test(start_paused = true)]
async fn ingest_is_event_driven_and_recovers_with_backfill() {
    let remote = MockJustLogServer::start().await;
    remote
        .set_json(
            "/api/v2/recent-messages/channelone?limit=800",
            serde_json::json!({ "messages": [], "error": null, "error_code": null }),
        )
        .await;
    let harness = TestHarness::start_with_options(TestHarnessOptions {
        recent_messages_runtime: Arc::new(recent_messages_runtime(&format!(
            "{}/api/v2/recent-messages",
            remote.base_url()
        ))),
        ..Default::default()
    })
    .await;

    harness.irc.wait_for_connections(2).await;
    harness
        .irc
        .wait_for_client_line_contains("PASS oauth:")
        .await;
    harness
        .irc
        .wait_for_client_line_contains("NICK justinfan777777")
        .await;
    harness.irc.wait_for_client_line_contains("CAP REQ").await;
    harness
        .irc
        .wait_for_client_line_contains("JOIN #channelone")
        .await;

    harness
        .irc
        .send_to_connection(0, "PING :tmi.twitch.tv")
        .await;
    harness
        .irc
        .wait_for_client_line_contains("PONG :tmi.twitch.tv")
        .await;

    harness
        .irc
        .broadcast_line(&privmsg(
            "live-1",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_067_200_000,
            "hello",
        ))
        .await;
    harness.wait_for_event_count(1).await;

    remote
        .set_json(
            "/api/v2/recent-messages/channelone?limit=800",
            serde_json::json!({
                "messages": [
                    "not raw irc",
                    privmsg(
                        "robotty-1",
                        "1",
                        "200",
                        "viewer",
                        "viewer",
                        "channelone",
                        1_704_067_250_000,
                        "missed"
                    )
                ],
                "error": null,
                "error_code": null
            }),
        )
        .await;

    harness.irc.reconnect_first().await;
    harness.irc.wait_for_connections(3).await;
    harness.wait_for_event_count(2).await;

    let join = harness
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
    assert_eq!(join.status(), StatusCode::OK);
    harness
        .irc
        .wait_for_client_line_contains("JOIN #channeltwo")
        .await;

    harness
        .irc
        .broadcast_line(&privmsg(
            "cmd-part",
            "1",
            "100",
            "adminuser",
            "adminuser",
            "channelone",
            1_704_067_260_000,
            "!justlog part channeltwo",
        ))
        .await;
    harness
        .irc
        .wait_for_client_line_contains("PART #channeltwo")
        .await;
}

#[tokio::test(start_paused = true)]
async fn anonymous_ingest_stop_and_optout_expiry_do_not_wait_on_wall_clock() {
    let harness = TestHarness::start_anonymous(vec!["1".to_string()]).await;
    harness.irc.wait_for_connections(2).await;
    harness.irc.wait_for_client_line_contains("PASS _").await;
    let nick_line = harness
        .irc
        .wait_for_client_line_contains("NICK justinfan")
        .await;
    assert!(
        nick_line
            .trim_start_matches("NICK justinfan")
            .chars()
            .all(|ch| ch.is_ascii_digit())
    );

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
    assert!(harness.state.optout_codes.lock().await.contains_key(&code));
    tokio::task::yield_now().await;
    harness.advance_time(Duration::from_secs(61)).await;
    timeout(Duration::from_secs(1), async {
        loop {
            if !harness.state.optout_codes.lock().await.contains_key(&code) {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    harness.irc.reconnect_first().await;
    harness.state.ingest.read().await.clone().unwrap().stop();
    harness.advance_time(Duration::from_secs(1)).await;
    assert_eq!(harness.irc.connection_count().await, 2);
}

#[tokio::test(start_paused = true)]
async fn compactor_uses_fast_forwarded_time_for_archive_compression() {
    let harness = TestHarness::start_with_options(TestHarnessOptions {
        start_ingest: false,
        start_compactor: true,
        compact_interval_seconds: 60,
        compact_after_channel_days: 7,
        ..Default::default()
    })
    .await;

    let event = CanonicalEvent::from_raw(&privmsg(
        "compress-1",
        "1",
        "200",
        "viewer",
        "viewer",
        "channelone",
        1_705_104_000_000,
        "compress me",
    ))
    .unwrap()
    .unwrap();
    harness.seed_channel_event(&event);

    harness
        .advance_time(Duration::from_secs(8 * 24 * 60 * 60 + 60))
        .await;
    harness.wait_for_channel_segment("1", 2024, 1, 13).await;

    let raw = harness
        .request(
            Request::builder()
                .uri("/channelid/1/2024/1/13?raw=1")
                .header("accept-encoding", "br")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
    let raw_body = to_bytes(raw.into_body(), usize::MAX).await.unwrap();
    assert!(decode_brotli(&raw_body).contains("compress me"));
}
