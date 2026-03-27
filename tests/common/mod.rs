#![allow(dead_code)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use axum::body::{Body, to_bytes};
use axum::extract::Request as AxumRequest;
use axum::http::{Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use justlog::api;
use justlog::app::{AppState, CommandService, resolve_channel_logins};
use justlog::config::Config;
use justlog::debug_sync::DebugRuntime;
use justlog::helix::{HelixClient, UserData};
use justlog::ingest::IngestManager;
use justlog::legacy_txt::LegacyTxtRuntime;
use justlog::model::CanonicalEvent;
use justlog::store::Store;
use serde_json::json;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio::task::JoinHandle;
use tokio::time::{Duration, sleep};
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
                .filter_map(|(key, value)| {
                    if key == "id" {
                        Some(value.clone())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>();
            let logins = values
                .iter()
                .filter_map(|(key, value)| {
                    if key == "login" {
                        Some(value.clone())
                    } else {
                        None
                    }
                })
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
        sleep(Duration::from_millis(50)).await;
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
        sleep(Duration::from_millis(50)).await;
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
    client_lines: Arc<Mutex<Vec<String>>>,
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

    pub async fn wait_for_connections(&self, count: usize) {
        for _ in 0..50 {
            if self.connections.lock().await.len() >= count {
                return;
            }
            sleep(Duration::from_millis(100)).await;
        }
        panic!("timed out waiting for {count} IRC connections");
    }

    pub async fn broadcast_line(&self, line: &str) {
        for connection in self.connections.lock().await.iter() {
            let _ = connection.send(ServerEvent::Line(line.to_string()));
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

pub struct TestHarness {
    pub _temp: TempDir,
    pub state: AppState,
    pub router: Router,
    pub irc: MockIrc,
}

impl TestHarness {
    pub async fn start(channel_ids: Vec<String>) -> Self {
        Self::start_with_options(channel_ids, true, Arc::new(DebugRuntime::disabled())).await
    }

    pub async fn start_anonymous(channel_ids: Vec<String>) -> Self {
        Self::start_with_options_and_oauth(
            channel_ids,
            true,
            Arc::new(DebugRuntime::disabled()),
            "",
        )
        .await
    }

    pub async fn start_without_ingest(channel_ids: Vec<String>) -> Self {
        Self::start_with_options(channel_ids, false, Arc::new(DebugRuntime::disabled())).await
    }

    pub async fn start_with_debug_runtime(
        channel_ids: Vec<String>,
        start_ingest: bool,
        debug_runtime: Arc<DebugRuntime>,
    ) -> Self {
        Self::start_with_options(channel_ids, start_ingest, debug_runtime).await
    }

    async fn start_with_options(
        channel_ids: Vec<String>,
        start_ingest: bool,
        debug_runtime: Arc<DebugRuntime>,
    ) -> Self {
        Self::start_with_options_and_oauth(
            channel_ids,
            start_ingest,
            debug_runtime,
            "oauth:777777777",
        )
        .await
    }

    async fn start_with_options_and_oauth(
        channel_ids: Vec<String>,
        start_ingest: bool,
        debug_runtime: Arc<DebugRuntime>,
        oauth: &str,
    ) -> Self {
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
                "oauth": oauth,
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
            legacy_txt: Arc::new(LegacyTxtRuntime::from_env(&config.logs_directory)),
            debug_runtime,
            ingest: ingest_slot.clone(),
            start_time: Instant::now(),
            optout_codes: Arc::new(Mutex::new(HashMap::new())),
        };
        if start_ingest {
            let command_service = Arc::new(CommandService::new(state.clone()));
            let ingest = IngestManager::new(shared_config.clone(), store.clone(), command_service);
            *ingest_slot.write().await = Some(ingest.clone());
            let initial_logins = resolve_channel_logins(&helix, &config.channels)
                .await
                .unwrap();
            ingest.start(initial_logins).await;
        }
        let router = api::router(state.clone());
        Self {
            _temp: temp,
            state,
            router,
            irc,
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

    pub async fn spawn_http_server(&self) -> HttpServerHandle {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = self.router.clone();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        sleep(Duration::from_millis(50)).await;
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
