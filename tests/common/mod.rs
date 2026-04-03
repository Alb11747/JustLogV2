#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use axum::body::{Body, to_bytes};
use axum::extract::Request as AxumRequest;
use axum::http::{Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{TimeZone, Utc};
use justlog::api;
use justlog::app::{AppState, CommandService, resolve_channel_logins};
use justlog::clock::FakeClock;
use justlog::compact::spawn_compactor;
use justlog::config::Config;
use justlog::cors::CorsRuntime;
use justlog::debug_sync::DebugRuntime;
use justlog::helix::{HelixClient, UserData};
use justlog::ingest::IngestManager;
use justlog::legacy_txt::LegacyTxtRuntime;
use justlog::model::CanonicalEvent;
use justlog::recent_messages::RecentMessagesRuntime;
use justlog::store::Store;
use serde_json::json;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, Notify, RwLock, mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tower::util::ServiceExt;

#[derive(Clone)]
pub struct MockHelix {
    pub address: SocketAddr,
}

impl MockHelix {
    pub async fn start(users: Vec<UserData>) -> Self {
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
            let values = url::form_urlencoded::parse(query.as_bytes())
                .into_owned()
                .collect::<Vec<_>>();
            let ids = values
                .iter()
                .filter_map(|(key, value)| (key == "id").then_some(value.clone()))
                .collect::<Vec<_>>();
            let logins = values
                .iter()
                .filter_map(|(key, value)| (key == "login").then_some(value.clone()))
                .collect::<Vec<_>>();
            for id in ids {
                if let Some(user) = state.users.get(&id) {
                    data.push(user.clone());
                }
            }
            for login in logins {
                if let Some(user) = state.by_login.get(&login.to_lowercase()) {
                    data.push(user.clone());
                }
            }
            Json(json!({ "data": data }))
        }

        let state = HelixState {
            users: Arc::new(
                users
                    .iter()
                    .cloned()
                    .map(|user| (user.id.clone(), user))
                    .collect(),
            ),
            by_login: Arc::new(
                users
                    .iter()
                    .cloned()
                    .map(|user| (user.login.to_lowercase(), user))
                    .collect(),
            ),
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

    pub fn base_url(&self) -> String {
        format!("http://{}/helix", self.address)
    }
}

#[derive(Clone)]
pub struct MockJustLogServer {
    pub address: SocketAddr,
    routes: Arc<Mutex<HashMap<String, MockRouteResponse>>>,
}

#[derive(Clone)]
struct MockRouteResponse {
    status: StatusCode,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl MockJustLogServer {
    pub async fn start() -> Self {
        async fn handler(
            axum::extract::State(routes): axum::extract::State<
                Arc<Mutex<HashMap<String, MockRouteResponse>>>,
            >,
            request: AxumRequest,
        ) -> impl IntoResponse {
            let key = request
                .uri()
                .path_and_query()
                .map(|value| value.as_str().to_string())
                .unwrap_or_else(|| request.uri().path().to_string());
            let route = routes.lock().await.get(&key).cloned();
            let Some(route) = route else {
                return StatusCode::NOT_FOUND.into_response();
            };
            let mut response = axum::response::Response::new(Body::from(route.body));
            *response.status_mut() = route.status;
            for (name, value) in route.headers {
                response.headers_mut().insert(
                    axum::http::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                    axum::http::HeaderValue::from_str(&value).unwrap(),
                );
            }
            response
        }

        let routes = Arc::new(Mutex::new(HashMap::new()));
        let app = Router::new()
            .route("/{*path}", get(handler))
            .with_state(routes.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self { address, routes }
    }

    pub fn base_url(&self) -> String {
        format!("http://{}", self.address)
    }

    pub async fn set_json(&self, path_and_query: &str, value: serde_json::Value) {
        self.routes.lock().await.insert(
            path_and_query.to_string(),
            MockRouteResponse {
                status: StatusCode::OK,
                headers: vec![("content-type".to_string(), "application/json".to_string())],
                body: serde_json::to_vec(&value).unwrap(),
            },
        );
    }

    pub async fn set_text(&self, path_and_query: &str, body: &str) {
        self.routes.lock().await.insert(
            path_and_query.to_string(),
            MockRouteResponse {
                status: StatusCode::OK,
                headers: vec![(
                    "content-type".to_string(),
                    "text/plain; charset=utf-8".to_string(),
                )],
                body: body.as_bytes().to_vec(),
            },
        );
    }
}

#[derive(Clone)]
pub struct MockIrc {
    pub address: SocketAddr,
    connections: Arc<Mutex<Vec<mpsc::UnboundedSender<ServerEvent>>>>,
    connection_notify: Arc<Notify>,
    client_lines: Arc<Mutex<Vec<String>>>,
    line_notify: Arc<Notify>,
}

#[derive(Clone)]
enum ServerEvent {
    Line(String),
    Close,
}

impl MockIrc {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let connections = Arc::new(Mutex::new(Vec::new()));
        let connection_notify = Arc::new(Notify::new());
        let client_lines = Arc::new(Mutex::new(Vec::new()));
        let line_notify = Arc::new(Notify::new());
        let connections_clone = connections.clone();
        let connection_notify_clone = connection_notify.clone();
        let client_lines_clone = client_lines.clone();
        let line_notify_clone = line_notify.clone();
        tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let (tx, rx) = mpsc::unbounded_channel();
                connections_clone.lock().await.push(tx);
                connection_notify_clone.notify_waiters();
                let client_lines = client_lines_clone.clone();
                let line_notify = line_notify_clone.clone();
                tokio::spawn(handle_irc_connection(socket, rx, client_lines, line_notify));
            }
        });
        Self {
            address,
            connections,
            connection_notify,
            client_lines,
            line_notify,
        }
    }

    pub async fn wait_for_connections(&self, count: usize) {
        timeout(StdDuration::from_secs(5), async {
            loop {
                if self.connections.lock().await.len() >= count {
                    return;
                }
                self.connection_notify.notified().await;
            }
        })
        .await
        .unwrap();
    }

    pub async fn wait_for_connection_count(&self, count: usize) {
        self.wait_for_connections(count).await;
    }

    pub async fn wait_for_client_line_contains(&self, needle: &str) -> String {
        timeout(StdDuration::from_secs(5), async {
            loop {
                if let Some(line) = self
                    .client_lines
                    .lock()
                    .await
                    .iter()
                    .find(|line| line.contains(needle))
                    .cloned()
                {
                    return line;
                }
                self.line_notify.notified().await;
            }
        })
        .await
        .unwrap()
    }

    pub async fn broadcast_line(&self, line: &str) {
        for connection in self.connections.lock().await.iter() {
            let _ = connection.send(ServerEvent::Line(line.to_string()));
        }
    }

    pub async fn send_to_connection(&self, index: usize, line: &str) {
        if let Some(connection) = self.connections.lock().await.get(index).cloned() {
            let _ = connection.send(ServerEvent::Line(line.to_string()));
        }
    }

    pub async fn close_connection(&self, index: usize) {
        if let Some(connection) = self.connections.lock().await.get(index).cloned() {
            let _ = connection.send(ServerEvent::Close);
        }
    }

    pub async fn reconnect_first(&self) {
        if let Some(connection) = self.connections.lock().await.first().cloned() {
            let _ = connection.send(ServerEvent::Line(":tmi.twitch.tv RECONNECT".to_string()));
            let _ = connection.send(ServerEvent::Close);
        }
    }

    pub async fn client_lines(&self) -> Vec<String> {
        self.client_lines.lock().await.clone()
    }

    pub async fn connection_count(&self) -> usize {
        self.connections.lock().await.len()
    }
}

async fn handle_irc_connection(
    socket: TcpStream,
    mut rx: mpsc::UnboundedReceiver<ServerEvent>,
    client_lines: Arc<Mutex<Vec<String>>>,
    line_notify: Arc<Notify>,
) {
    let (reader_half, mut writer_half) = socket.into_split();
    let mut reader = BufReader::new(reader_half).lines();
    loop {
        tokio::select! {
            line = reader.next_line() => {
                match line {
                    Ok(Some(line)) => {
                        client_lines.lock().await.push(line);
                        line_notify.notify_waiters();
                    }
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

pub struct TestHarnessOptions {
    pub channel_ids: Vec<String>,
    pub start_ingest: bool,
    pub start_compactor: bool,
    pub debug_runtime: Arc<DebugRuntime>,
    pub recent_messages_runtime: Arc<RecentMessagesRuntime>,
    pub oauth: String,
    pub metrics_enabled: bool,
    pub initial_time: chrono::DateTime<Utc>,
    pub compact_interval_seconds: u64,
    pub compact_after_channel_days: i64,
    pub compact_after_user_months: i64,
}

impl Default for TestHarnessOptions {
    fn default() -> Self {
        Self {
            channel_ids: vec!["1".to_string()],
            start_ingest: true,
            start_compactor: false,
            debug_runtime: Arc::new(DebugRuntime::disabled()),
            recent_messages_runtime: Arc::new(RecentMessagesRuntime::disabled()),
            oauth: "oauth:777777777".to_string(),
            metrics_enabled: false,
            initial_time: Utc.with_ymd_and_hms(2024, 1, 15, 12, 0, 0).unwrap(),
            compact_interval_seconds: 60,
            compact_after_channel_days: 7,
            compact_after_user_months: 3,
        }
    }
}

pub struct TestHarness {
    pub _temp: TempDir,
    pub state: AppState,
    pub router: Router,
    pub irc: MockIrc,
    pub clock: FakeClock,
    _shutdown: watch::Sender<bool>,
}

impl TestHarness {
    pub async fn start(channel_ids: Vec<String>) -> Self {
        Self::start_with_options(TestHarnessOptions {
            channel_ids,
            ..Default::default()
        })
        .await
    }

    pub async fn start_anonymous(channel_ids: Vec<String>) -> Self {
        Self::start_with_options(TestHarnessOptions {
            channel_ids,
            oauth: String::new(),
            ..Default::default()
        })
        .await
    }

    pub async fn start_without_ingest(channel_ids: Vec<String>) -> Self {
        Self::start_with_options(TestHarnessOptions {
            channel_ids,
            start_ingest: false,
            ..Default::default()
        })
        .await
    }

    pub async fn start_with_debug_runtime(
        channel_ids: Vec<String>,
        start_ingest: bool,
        debug_runtime: Arc<DebugRuntime>,
    ) -> Self {
        Self::start_with_options(TestHarnessOptions {
            channel_ids,
            start_ingest,
            debug_runtime,
            ..Default::default()
        })
        .await
    }

    pub async fn start_with_recent_messages_runtime(
        channel_ids: Vec<String>,
        start_ingest: bool,
        recent_messages_runtime: Arc<RecentMessagesRuntime>,
    ) -> Self {
        Self::start_with_options(TestHarnessOptions {
            channel_ids,
            start_ingest,
            recent_messages_runtime,
            ..Default::default()
        })
        .await
    }

    pub async fn start_with_options(options: TestHarnessOptions) -> Self {
        let users = vec![
            user("1", "channelone"),
            user("2", "channeltwo"),
            user("100", "adminuser"),
            user("200", "viewer"),
        ];
        let helix_server = MockHelix::start(users).await;
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
                "oauth": options.oauth,
                "botVerified": false,
                "clientID": "client",
                "clientSecret": "secret",
                "logLevel": "info",
                "channels": options.channel_ids,
                "archive": true,
                "listenAddress": ":0",
                "helix": { "baseUrl": helix_server.base_url() },
                "irc": { "server": "127.0.0.1", "port": irc.address.port(), "tls": false },
                "storage": {
                    "compactIntervalSeconds": options.compact_interval_seconds,
                    "compactAfterChannelDays": options.compact_after_channel_days,
                    "compactAfterUserMonths": options.compact_after_user_months
                },
                "ops": {
                    "metricsEnabled": options.metrics_enabled,
                    "metricsRoute": "/metrics"
                }
            }))
            .unwrap(),
        )
        .unwrap();
        let config = Config::load(&config_path).unwrap();
        let shared_config = Arc::new(RwLock::new(config.clone()));
        let store = Store::open(&config).unwrap();
        let helix = HelixClient::new(&config);
        let ingest_slot = Arc::new(RwLock::new(None));
        let clock = FakeClock::new(options.initial_time);
        let shared_clock = clock.shared();
        let state = AppState {
            config: shared_config.clone(),
            store: store.clone(),
            helix: helix.clone(),
            legacy_txt: Arc::new(LegacyTxtRuntime::from_env(&config.logs_directory)),
            debug_runtime: options.debug_runtime,
            cors: Arc::new(CorsRuntime::from_env()),
            ingest: ingest_slot.clone(),
            clock: shared_clock.clone(),
            start_time: shared_clock.now_instant(),
            optout_codes: Arc::new(Mutex::new(HashMap::new())),
        };

        if options.start_ingest {
            let command_service = Arc::new(CommandService::new(state.clone()));
            let ingest = IngestManager::new(
                shared_config.clone(),
                store.clone(),
                command_service,
                options.recent_messages_runtime,
                shared_clock.clone(),
            );
            *ingest_slot.write().await = Some(ingest.clone());
            let initial_logins = resolve_channel_logins(&helix, &config.channels)
                .await
                .unwrap();
            ingest.start(initial_logins).await;
        }

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        if options.start_compactor {
            spawn_compactor(
                shared_config.clone(),
                store.clone(),
                state.debug_runtime.clone(),
                shared_clock,
                shutdown_rx,
            );
        }

        let router = api::router(state.clone());
        Self {
            _temp: temp,
            state,
            router,
            irc,
            clock,
            _shutdown: shutdown_tx,
        }
    }

    pub async fn request(&self, request: Request<Body>) -> axum::response::Response {
        self.router.clone().oneshot(request).await.unwrap()
    }

    pub async fn response_text(&self, request: Request<Body>) -> String {
        let response = self.request(request).await;
        String::from_utf8(
            to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap()
                .to_vec(),
        )
        .unwrap()
    }

    pub async fn response_bytes(&self, request: Request<Body>) -> Vec<u8> {
        let response = self.request(request).await;
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec()
    }

    pub fn seed_channel_event(&self, event: &CanonicalEvent) {
        self.state.store.insert_event(event).unwrap();
    }

    pub fn compact_channel_day(&self, channel_id: &str, year: i32, month: u32, day: u32) {
        self.state
            .store
            .compact_channel_partition(&justlog::model::ChannelDayKey {
                channel_id: channel_id.to_string(),
                year,
                month,
                day,
            })
            .unwrap();
    }

    pub fn compact_user_month(&self, channel_id: &str, user_id: &str, year: i32, month: u32) {
        self.state
            .store
            .compact_user_partition(&justlog::model::UserMonthKey {
                channel_id: channel_id.to_string(),
                user_id: user_id.to_string(),
                year,
                month,
            })
            .unwrap();
    }

    pub async fn wait_for_event_count(&self, expected: i64) {
        timeout(StdDuration::from_secs(5), async {
            loop {
                if self.state.store.event_count().unwrap() == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    pub async fn wait_for_channel_segment(
        &self,
        channel_id: &str,
        year: i32,
        month: u32,
        day: u32,
    ) {
        timeout(StdDuration::from_secs(5), async {
            loop {
                if self
                    .state
                    .store
                    .channel_raw_plan(channel_id, year, month, day)
                    .unwrap()
                    .segment_path
                    .is_some()
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    pub async fn advance_time(&self, duration: StdDuration) {
        self.clock.advance(duration);
        tokio::time::advance(duration).await;
        tokio::task::yield_now().await;
    }

    pub async fn spawn_http_server(&self) -> HttpServerHandle {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = self.router.clone();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        HttpServerHandle {
            base_url: format!("http://{address}"),
            task,
        }
    }
}

pub struct HttpServerHandle {
    pub base_url: String,
    task: JoinHandle<()>,
}

impl Drop for HttpServerHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub fn user(id: &str, login: &str) -> UserData {
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

pub fn privmsg(
    id: &str,
    room_id: &str,
    user_id: &str,
    login: &str,
    display_name: &str,
    channel: &str,
    timestamp_ms: i64,
    text: &str,
) -> String {
    format!(
        "@badge-info=;badges=;color=#0000FF;display-name={display_name};emotes=;flags=;id={id};mod=0;room-id={room_id};subscriber=0;tmi-sent-ts={timestamp_ms};turbo=0;user-id={user_id};user-type= :{login}!{login}@{login}.tmi.twitch.tv PRIVMSG #{channel} :{text}"
    )
}

pub async fn assert_status_ok(harness: &TestHarness, path: &str) {
    let response = harness
        .request(Request::builder().uri(path).body(Body::empty()).unwrap())
        .await;
    assert_eq!(response.status(), StatusCode::OK);
}
