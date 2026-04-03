use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use clap::{Parser, Subcommand};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as HyperServerBuilder;
use tokio::net::TcpListener;
use tokio::sync::{Mutex, RwLock, watch};
use tokio::task::JoinSet;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::api;
use crate::clock::SharedClock;
use crate::compact::{CompactorTasks, spawn_compactor};
use crate::config::{Config, SharedConfig};
use crate::cors::CorsRuntime;
use crate::debug_sync::{DebugRuntime, run_startup_validation};
use crate::helix::HelixClient;
use crate::import::import_legacy_logs;
use crate::ingest::{ChatCommandService, IngestManager};
use crate::legacy_txt::LegacyTxtRuntime;
use crate::model::CanonicalEvent;
use crate::recent_messages::RecentMessagesRuntime;
use crate::store::Store;

const SERVER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(25);
const CONNECTION_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
const BACKGROUND_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(25);
const STORE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Parser)]
#[command(name = "justlog")]
struct Cli {
    #[arg(long, default_value = "config.json")]
    config: PathBuf,
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    ImportLegacy { source: PathBuf },
}

#[derive(Clone)]
pub struct AppState {
    pub config: SharedConfig,
    pub store: Store,
    pub helix: HelixClient,
    pub legacy_txt: Arc<LegacyTxtRuntime>,
    pub debug_runtime: Arc<DebugRuntime>,
    pub cors: Arc<CorsRuntime>,
    pub ingest: Arc<RwLock<Option<IngestManager>>>,
    pub clock: SharedClock,
    pub start_time: Instant,
    pub optout_codes: Arc<Mutex<HashMap<String, Instant>>>,
}

pub async fn run_cli() -> Result<()> {
    let cli = Cli::parse();
    let config = Config::load(&cli.config)?;
    init_tracing(&config.log_level);
    let debug_runtime = Arc::new(DebugRuntime::from_env(&config.logs_directory)?);
    let cors = Arc::new(CorsRuntime::from_env());
    let legacy_txt = Arc::new(LegacyTxtRuntime::from_env(&config.logs_directory));
    let recent_messages = Arc::new(RecentMessagesRuntime::from_env());
    let clock = SharedClock::real();

    let shared_config = Arc::new(RwLock::new(config.clone()));
    let store = Store::open(&config)?;
    run_startup_validation(debug_runtime.clone(), store.clone(), clock.now_utc()).await?;

    if let Some(Commands::ImportLegacy { source }) = cli.command {
        let imported = import_legacy_logs(&store, &source)?;
        info!("imported {imported} legacy events");
        return Ok(());
    }

    let helix = HelixClient::new(&config);
    let ingest_slot = Arc::new(RwLock::new(None));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let state = AppState {
        config: shared_config.clone(),
        store: store.clone(),
        helix: helix.clone(),
        legacy_txt,
        debug_runtime: debug_runtime.clone(),
        cors,
        ingest: ingest_slot.clone(),
        clock: clock.clone(),
        start_time: clock.now_instant(),
        optout_codes: Arc::new(Mutex::new(HashMap::new())),
    };

    if config.oauth.trim().is_empty() {
        info!("oauth not configured; starting ingest in anonymous justinfan mode");
    }
    let command_service = Arc::new(CommandService::new(state.clone()));
    let ingest = IngestManager::new(
        shared_config.clone(),
        store.clone(),
        command_service,
        recent_messages,
        clock.clone(),
    );
    *ingest_slot.write().await = Some(ingest.clone());

    let initial_channels = resolve_channel_logins(&helix, &config.channels).await?;
    ingest.start(initial_channels).await;
    let compactor_tasks = spawn_compactor(
        shared_config.clone(),
        store.clone(),
        debug_runtime,
        clock.clone(),
        shutdown_rx.clone(),
    );

    let state_for_server = state.clone();
    let listen_address = config.listen_address.clone();
    let mut server =
        tokio::spawn(async move { serve(state_for_server, &listen_address, shutdown_rx).await });

    tokio::select! {
        result = &mut server => {
            result.map_err(|error| anyhow::anyhow!("server join error: {error}"))??;
            initiate_shutdown(
                &store,
                &shutdown_tx,
                &ingest,
                compactor_tasks,
                None,
            )
            .await;
        }
        _ = wait_for_shutdown_signal() => {
            info!("shutdown signal received");
            initiate_shutdown(
                &store,
                &shutdown_tx,
                &ingest,
                compactor_tasks,
                Some(&mut server),
            )
            .await;
        }
    }

    Ok(())
}

async fn initiate_shutdown(
    store: &Store,
    shutdown_tx: &watch::Sender<bool>,
    ingest: &IngestManager,
    compactor_tasks: CompactorTasks,
    server: Option<&mut tokio::task::JoinHandle<Result<()>>>,
) {
    store.request_shutdown();
    let _ = shutdown_tx.send(true);

    if let Some(server) = server {
        match tokio::time::timeout(SERVER_SHUTDOWN_TIMEOUT, server).await {
            Ok(result) => {
                if let Err(error) = result {
                    warn!("server join error during shutdown: {error}");
                } else if let Ok(Err(error)) = result {
                    warn!("server shutdown returned error: {error}");
                }
            }
            Err(_) => {
                warn!("graceful shutdown timeout reached while waiting for HTTP server");
            }
        }
    }

    if tokio::time::timeout(BACKGROUND_SHUTDOWN_TIMEOUT, ingest.stop())
        .await
        .is_err()
    {
        warn!("graceful shutdown timeout reached while waiting for ingest tasks");
    }

    if tokio::time::timeout(BACKGROUND_SHUTDOWN_TIMEOUT, compactor_tasks.shutdown())
        .await
        .is_err()
    {
        warn!("graceful shutdown timeout reached while waiting for compactor tasks");
    }

    let store = store.clone();
    if tokio::time::timeout(
        STORE_SHUTDOWN_TIMEOUT,
        tokio::task::spawn_blocking(move || store.shutdown()),
    )
    .await
    .is_err()
    {
        warn!("graceful shutdown timeout reached while waiting for store workers");
    }
}

async fn serve(
    state: AppState,
    listen_address: &str,
    shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let bind_address = normalize_listen_address(listen_address);
    let listener = TcpListener::bind(bind_address).await?;
    serve_with_listener(state, listener, shutdown).await
}

async fn serve_with_listener(
    state: AppState,
    listener: TcpListener,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let local_address = listener.local_addr()?;
    info!("Listening on {local_address}");
    let mut connections = JoinSet::new();
    loop {
        let accept = listener.accept();
        tokio::pin!(accept);
        let maybe_connection = tokio::select! {
            result = &mut accept => Some(result?),
            _ = shutdown.changed() => None,
        };
        let Some((stream, remote_address)) = maybe_connection else {
            info!("HTTP server stopping accepts; draining active connections");
            break;
        };
        let state = state.clone();
        connections.spawn(async move {
            let io = TokioIo::new(stream);
            let service = service_fn(move |request| {
                let state = state.clone();
                async move {
                    Ok::<_, std::convert::Infallible>(
                        api::dispatch(state, request.map(axum::body::Body::new)).await,
                    )
                }
            });
            let builder = HyperServerBuilder::new(TokioExecutor::new());
            if let Err(error) = builder.serve_connection_with_upgrades(io, service).await {
                warn!("failed to serve HTTP connection from {remote_address}: {error}");
            }
        });
    }
    match tokio::time::timeout(CONNECTION_DRAIN_TIMEOUT, async {
        while let Some(result) = connections.join_next().await {
            if let Err(error) = result {
                warn!("connection task join error during shutdown: {error}");
            }
        }
    })
    .await
    {
        Ok(()) => {}
        Err(_) => {
            warn!("HTTP connection drain timeout reached; aborting in-flight connections");
            connections.abort_all();
            while let Some(result) = connections.join_next().await {
                if let Err(error) = result {
                    warn!("connection task join error after abort during shutdown: {error}");
                }
            }
        }
    }
    Ok(())
}

async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut terminate =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

pub async fn resolve_channel_logins(
    helix: &HelixClient,
    channel_ids: &[String],
) -> Result<Vec<String>> {
    if !helix.is_enabled() {
        let mut logins = channel_ids
            .iter()
            .filter(|value| !looks_like_channel_id(value))
            .cloned()
            .collect::<Vec<_>>();
        logins.sort();
        if logins.len() != channel_ids.len() {
            let skipped = channel_ids
                .iter()
                .filter(|value| looks_like_channel_id(value))
                .cloned()
                .collect::<Vec<_>>();
            warn!(
                "skipping startup channels that require Helix lookup because clientID/clientSecret are not configured: {:?}",
                skipped
            );
        }
        return Ok(logins);
    }
    let users = helix.get_users_by_ids(channel_ids).await?;
    let mut logins = users
        .into_values()
        .map(|user| user.login)
        .collect::<Vec<_>>();
    logins.sort();
    Ok(logins)
}

fn looks_like_channel_id(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn normalize_listen_address(listen_address: &str) -> SocketAddr {
    if let Some(port) = listen_address.strip_prefix(':') {
        format!("0.0.0.0:{port}").parse().unwrap()
    } else {
        listen_address.parse().unwrap()
    }
}

fn init_tracing(log_level: &str) {
    let filter = EnvFilter::try_new(log_level).unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
}

pub struct CommandService {
    state: AppState,
}

impl CommandService {
    pub fn new(state: AppState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl ChatCommandService for CommandService {
    async fn handle_privmsg_command(&self, event: &CanonicalEvent) -> Result<()> {
        let lower = event.text.to_lowercase();
        if !lower.starts_with("!justlog") {
            return Ok(());
        }
        let mut parts = event.text.split_whitespace();
        let _prefix = parts.next();
        let Some(command) = parts.next().map(|value| value.to_lowercase()) else {
            return Ok(());
        };
        let args = parts.map(|value| value.to_string()).collect::<Vec<_>>();
        match command.as_str() {
            "status" => {
                if !self.is_admin(&event.username).await {
                    return Ok(());
                }
                let uptime = format_duration(
                    self.state
                        .clock
                        .now_instant()
                        .saturating_duration_since(self.state.start_time),
                );
                self.say(
                    &event.channel_login,
                    &format!("{}, uptime: {}", event.display_name, uptime),
                )
                .await;
            }
            "join" => {
                if !self.is_admin(&event.username).await {
                    return Ok(());
                }
                self.handle_join_part(event, &args, true).await?;
            }
            "part" => {
                if !self.is_admin(&event.username).await {
                    return Ok(());
                }
                self.handle_join_part(event, &args, false).await?;
            }
            "optout" => {
                self.handle_opt_out(event, &args).await?;
            }
            "optin" => {
                if !self.is_admin(&event.username).await {
                    return Ok(());
                }
                self.handle_opt_in(event, &args).await?;
            }
            _ => {}
        }
        Ok(())
    }
}

impl CommandService {
    async fn is_admin(&self, username: &str) -> bool {
        self.state
            .config
            .read()
            .await
            .admins
            .iter()
            .any(|admin| admin == &username.to_lowercase())
    }

    async fn say(&self, channel: &str, text: &str) {
        if let Some(ingest) = self.state.ingest.read().await.clone() {
            ingest.say(channel, text).await;
        }
    }

    async fn handle_join_part(
        &self,
        event: &CanonicalEvent,
        args: &[String],
        join: bool,
    ) -> Result<()> {
        if args.is_empty() {
            self.say(&event.channel_login, &format!("{}, at least 1 username has to be provided. multiple usernames have to be separated with a space", event.display_name)).await;
            return Ok(());
        }
        let users = self.state.helix.get_users_by_logins(args).await?;
        let mut ids = Vec::new();
        let mut logins = Vec::new();
        for user in users.into_values() {
            ids.push(user.id);
            logins.push(user.login);
        }
        {
            let mut config = self.state.config.write().await;
            if join {
                config.add_channels(&ids);
            } else {
                config.remove_channels(&ids);
            }
            config.persist()?;
        }
        if let Some(ingest) = self.state.ingest.read().await.clone() {
            if join {
                ingest.join_channels(&logins).await;
            } else {
                ingest.part_channels(&logins).await;
            }
        }
        let action = if join { "added" } else { "removed" };
        self.say(
            &event.channel_login,
            &format!("{}, {action} channels: {:?}", event.display_name, ids),
        )
        .await;
        Ok(())
    }

    async fn handle_opt_out(&self, event: &CanonicalEvent, args: &[String]) -> Result<()> {
        if args.is_empty() {
            self.say(&event.channel_login, &format!("{}, at least 1 username has to be provided. multiple usernames have to be separated with a space", event.display_name)).await;
            return Ok(());
        }
        {
            let mut codes = self.state.optout_codes.lock().await;
            if let Some(expires_at) = codes.remove(&args[0]) {
                if expires_at <= self.state.clock.now_instant() {
                    return Ok(());
                }
                if let Some(user_id) = event.user_id.clone() {
                    let mut config = self.state.config.write().await;
                    config.opt_out_users(&[user_id]);
                    config.persist()?;
                    self.say(
                        &event.channel_login,
                        &format!("{}, opted you out", event.display_name),
                    )
                    .await;
                    return Ok(());
                }
            }
        }
        if !self.is_admin(&event.username).await {
            return Ok(());
        }
        let users = self.state.helix.get_users_by_logins(args).await?;
        let ids = users.into_values().map(|user| user.id).collect::<Vec<_>>();
        let mut config = self.state.config.write().await;
        config.opt_out_users(&ids);
        config.persist()?;
        self.say(
            &event.channel_login,
            &format!("{}, opted out channels: {:?}", event.display_name, ids),
        )
        .await;
        Ok(())
    }

    async fn handle_opt_in(&self, event: &CanonicalEvent, args: &[String]) -> Result<()> {
        if args.is_empty() {
            self.say(&event.channel_login, &format!("{}, at least 1 username has to be provided. multiple usernames have to be separated with a space", event.display_name)).await;
            return Ok(());
        }
        let users = self.state.helix.get_users_by_logins(args).await?;
        let ids = users.into_values().map(|user| user.id).collect::<Vec<_>>();
        let mut config = self.state.config.write().await;
        config.remove_opt_out(&ids);
        config.persist()?;
        self.say(
            &event.channel_login,
            &format!("{}, opted in channels: {:?}", event.display_name, ids),
        )
        .await;
        Ok(())
    }
}

fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let remaining = seconds % 60;
    format!("{hours}h {minutes}m {remaining}s")
}

#[cfg(test)]
mod tests {
    use super::{AppState, serve_with_listener};
    use crate::clock::SharedClock;
    use crate::config::Config;
    use crate::cors::CorsRuntime;
    use crate::debug_sync::DebugRuntime;
    use crate::helix::HelixClient;
    use crate::legacy_txt::LegacyTxtRuntime;
    use crate::store::Store;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::{Mutex, RwLock, watch};

    fn test_state() -> AppState {
        let temp = TempDir::new().unwrap();
        let root = temp.keep();
        let logs_directory = root.join("logs");
        let config_path = root.join("config.json");
        let mut config = Config {
            config_path,
            config_file_permissions: None,
            bot_verified: false,
            logs_directory,
            archive: false,
            admin_api_key: String::new(),
            username: "justinfan0".to_string(),
            oauth: String::new(),
            listen_address: "127.0.0.1:0".to_string(),
            admins: Vec::new(),
            channels: Vec::new(),
            client_id: String::new(),
            client_secret: String::new(),
            log_level: "info".to_string(),
            opt_out: HashMap::new(),
            compression: Default::default(),
            http: Default::default(),
            ingest: Default::default(),
            helix: Default::default(),
            irc: Default::default(),
            storage: Default::default(),
            ops: Default::default(),
        };
        config.storage.sqlite_path = PathBuf::from(root.join("justlog.sqlite3"));
        config.normalize().unwrap();
        let shared_config = Arc::new(RwLock::new(config.clone()));
        let store = Store::open(&config).unwrap();
        let clock = SharedClock::real();
        AppState {
            config: shared_config,
            store,
            helix: HelixClient::new(&config),
            legacy_txt: Arc::new(LegacyTxtRuntime::from_env(&config.logs_directory)),
            debug_runtime: Arc::new(DebugRuntime::disabled()),
            cors: Arc::new(CorsRuntime::disabled()),
            ingest: Arc::new(RwLock::new(None)),
            clock: clock.clone(),
            start_time: clock.now_instant(),
            optout_codes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[tokio::test]
    async fn serve_shutdown_aborts_idle_connections_after_timeout() {
        let state = test_state();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let server =
            tokio::spawn(async move { serve_with_listener(state, listener, shutdown_rx).await });

        let mut client = TcpStream::connect(local).await.unwrap();
        client.write_all(b"GET /healthz HTTP/1.1\r\n").await.unwrap();

        let _ = shutdown_tx.send(true);
        tokio::time::timeout(Duration::from_secs(15), server)
            .await
            .expect("server should exit after bounded drain timeout")
            .unwrap()
            .unwrap();
    }
}
