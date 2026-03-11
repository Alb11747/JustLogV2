use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::body::{Body, to_bytes};
use axum::extract::Request as AxumRequest;
use axum::http::{Method, Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use justlog::api;
use justlog::app::{AppState, CommandService, resolve_channel_logins};
use justlog::config::Config;
use justlog::helix::{HelixClient, UserData};
use justlog::ingest::IngestManager;
use justlog::model::CanonicalEvent;
use justlog::store::Store;
use serde_json::json;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::time::{Duration, sleep};
use tower::util::ServiceExt;

#[derive(Clone)]
struct MockHelix {
    address: SocketAddr,
}

impl MockHelix {
    async fn start(users: Vec<UserData>) -> Self {
        #[derive(Clone)]
        struct HelixState {
            users: Arc<HashMap<String, UserData>>,
            by_login: Arc<HashMap<String, UserData>>,
        }

        async fn token() -> impl IntoResponse {
            Json(json!({ "access_token": "test-token" }))
        }

        async fn users_handler(
            axum::extract::State(state): axum::extract::State<HelixState>,
            request: AxumRequest,
        ) -> impl IntoResponse {
            let mut data = Vec::new();
            let query = request.uri().query().unwrap_or_default();
            let values = url::form_urlencoded::parse(query.as_bytes()).into_owned().collect::<Vec<_>>();
            let ids = values
                .iter()
                .filter_map(|(key, value)| if key == "id" { Some(value.clone()) } else { None })
                .collect::<Vec<_>>();
            let logins = values
                .iter()
                .filter_map(|(key, value)| if key == "login" { Some(value.clone()) } else { None })
                .collect::<Vec<_>>();
            if !ids.is_empty() {
                for id in ids {
                    if let Some(user) = state.users.get(&id) {
                        data.push(user.clone());
                    }
                }
            }
            if !logins.is_empty() {
                for login in logins {
                    if let Some(user) = state.by_login.get(&login.to_lowercase()) {
                        data.push(user.clone());
                    }
                }
            }
            Json(json!({ "data": data }))
        }

        let state = HelixState {
            users: Arc::new(users.iter().cloned().map(|user| (user.id.clone(), user)).collect()),
            by_login: Arc::new(users.iter().cloned().map(|user| (user.login.to_lowercase(), user)).collect()),
        };
        let app = Router::new()
            .route("/oauth2/token", post(token))
            .route("/helix/users", get(users_handler))
            .with_state(state);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self { address }
    }

    fn base_url(&self) -> String {
        format!("http://{}/helix", self.address)
    }
}

#[derive(Clone)]
struct MockIrc {
    address: SocketAddr,
    connections: Arc<Mutex<Vec<mpsc::UnboundedSender<ServerEvent>>>>,
    client_lines: Arc<Mutex<Vec<String>>>,
}

#[derive(Clone)]
enum ServerEvent {
    Line(String),
    Close,
}

impl MockIrc {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let connections = Arc::new(Mutex::new(Vec::new()));
        let client_lines = Arc::new(Mutex::new(Vec::new()));
        let connections_clone = connections.clone();
        let client_lines_clone = client_lines.clone();
        tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let (tx, rx) = mpsc::unbounded_channel();
                connections_clone.lock().await.push(tx);
                let client_lines = client_lines_clone.clone();
                tokio::spawn(handle_irc_connection(socket, rx, client_lines));
            }
        });
        Self {
            address,
            connections,
            client_lines,
        }
    }

    async fn wait_for_connections(&self, count: usize) {
        for _ in 0..50 {
            if self.connections.lock().await.len() >= count {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
        panic!("timed out waiting for {count} IRC connections");
    }

    async fn broadcast_line(&self, line: &str) {
        for connection in self.connections.lock().await.iter() {
            let _ = connection.send(ServerEvent::Line(line.to_string()));
        }
    }

    async fn reconnect_first(&self) {
        if let Some(connection) = self.connections.lock().await.first().cloned() {
            let _ = connection.send(ServerEvent::Line(":tmi.twitch.tv RECONNECT".to_string()));
            let _ = connection.send(ServerEvent::Close);
        }
    }

    async fn client_lines(&self) -> Vec<String> {
        self.client_lines.lock().await.clone()
    }
}

async fn handle_irc_connection(
    socket: TcpStream,
    mut rx: mpsc::UnboundedReceiver<ServerEvent>,
    client_lines: Arc<Mutex<Vec<String>>>,
) {
    let (reader_half, mut writer_half) = socket.into_split();
    let mut reader = BufReader::new(reader_half).lines();
    loop {
        tokio::select! {
            line = reader.next_line() => {
                match line {
                    Ok(Some(line)) => client_lines.lock().await.push(line),
                    _ => return,
                }
            }
            event = rx.recv() => {
                match event {
                    Some(ServerEvent::Line(line)) => {
                        let _ = writer_half.write_all(line.as_bytes()).await;
                        let _ = writer_half.write_all(b"\r\n").await;
                        let _ = writer_half.flush().await;
                    }
                    Some(ServerEvent::Close) | None => return,
                }
            }
        }
    }
}

struct TestHarness {
    _temp: TempDir,
    state: AppState,
    router: Router,
    irc: MockIrc,
}

impl TestHarness {
    async fn start(channel_ids: Vec<String>) -> Self {
        let users = vec![
            user("1", "channelone"),
            user("2", "channeltwo"),
            user("100", "adminuser"),
            user("200", "viewer"),
        ];
        let helix_server = MockHelix::start(users.clone()).await;
        let irc = MockIrc::start().await;
        let temp = TempDir::new().unwrap();
        let config_path = temp.path().join("config.json");
        let logs_dir = temp.path().join("logs");
        std::fs::write(
            &config_path,
            serde_json::to_string_pretty(&json!({
                "admins": ["adminuser"],
                "logsDirectory": logs_dir.to_string_lossy(),
                "adminAPIKey": "secret",
                "username": "justinfan777777",
                "oauth": "oauth:777777777",
                "botVerified": false,
                "clientID": "client",
                "clientSecret": "secret",
                "logLevel": "info",
                "channels": channel_ids,
                "archive": true,
                "listenAddress": ":0",
                "helix": { "baseUrl": helix_server.base_url() },
                "irc": { "server": "127.0.0.1", "port": irc.address.port(), "tls": false }
            }))
            .unwrap(),
        )
        .unwrap();
        let config = Config::load(&config_path).unwrap();
        let shared_config = Arc::new(RwLock::new(config.clone()));
        let store = Store::open(&config).unwrap();
        let helix = HelixClient::new(&config);
        let ingest_slot = Arc::new(RwLock::new(None));
        let state = AppState {
            config: shared_config.clone(),
            store: store.clone(),
            helix: helix.clone(),
            ingest: ingest_slot.clone(),
            start_time: Instant::now(),
            optout_codes: Arc::new(Mutex::new(HashMap::new())),
        };
        let command_service = Arc::new(CommandService::new(state.clone()));
        let ingest = IngestManager::new(shared_config.clone(), store.clone(), command_service);
        *ingest_slot.write().await = Some(ingest.clone());
        let initial_logins = resolve_channel_logins(&helix, &config.channels).await.unwrap();
        ingest.start(initial_logins).await;
        let router = api::router(state.clone());
        Self {
            _temp: temp,
            state,
            router,
            irc,
        }
    }

    async fn request(&self, request: Request<Body>) -> axum::response::Response {
        self.router.clone().oneshot(request).await.unwrap()
    }
}

fn user(id: &str, login: &str) -> UserData {
    UserData {
        id: id.to_string(),
        login: login.to_string(),
        display_name: login.to_string(),
        r#type: String::new(),
        broadcaster_type: String::new(),
        description: String::new(),
        profile_image_url: String::new(),
        offline_image_url: String::new(),
        view_count: 0,
        email: String::new(),
    }
}

fn privmsg(id: &str, room_id: &str, user_id: &str, login: &str, display_name: &str, channel: &str, timestamp_ms: i64, text: &str) -> String {
    format!(
        "@badge-info=;badges=;color=#0000FF;display-name={display_name};emotes=;flags=;id={id};mod=0;room-id={room_id};subscriber=0;tmi-sent-ts={timestamp_ms};turbo=0;user-id={user_id};user-type= :{login}!{login}@{login}.tmi.twitch.tv PRIVMSG #{channel} :{text}"
    )
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
async fn channels_list_and_lowercase_redirect_are_compatible() {
    let harness = TestHarness::start(vec!["1".to_string()]).await;
    harness.irc.wait_for_connections(2).await;

    let channels = harness
        .request(Request::builder().uri("/channels").body(Body::empty()).unwrap())
        .await;
    assert_eq!(channels.status(), StatusCode::OK);
    let body = to_bytes(channels.into_body(), usize::MAX).await.unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["channels"][0]["name"], "channelone");

    let redirect = harness
        .request(Request::builder().uri("/CHANNELID/1").body(Body::empty()).unwrap())
        .await;
    assert_eq!(redirect.status(), StatusCode::FOUND);
    assert_eq!(
        redirect.headers().get("location").unwrap(),
        "/channelid/1"
    );
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
    assert!(harness.state.config.read().await.channels.contains(&"2".to_string()));

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
    assert!(!harness.state.config.read().await.channels.contains(&"2".to_string()));
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
    let code = String::from_utf8(to_bytes(optout.into_body(), usize::MAX).await.unwrap().to_vec()).unwrap();
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

    assert!(harness.state.config.read().await.opt_out.contains_key("200"));
    let list = harness
        .request(Request::builder().uri("/list?channelid=1&userid=200").body(Body::empty()).unwrap())
        .await;
    assert_eq!(list.status(), StatusCode::FORBIDDEN);
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
    harness.state.store.insert_event(&event).unwrap();
    harness
        .state
        .store
        .compact_channel_partition(&event.channel_day_key())
        .unwrap();

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
