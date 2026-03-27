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
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

use crate::api;
use crate::compact::spawn_compactor;
use crate::config::{Config, SharedConfig};
use crate::debug_sync::{DebugRuntime, run_startup_validation};
use crate::helix::HelixClient;
use crate::import::import_legacy_logs;
use crate::ingest::{ChatCommandService, IngestManager};
use crate::legacy_txt::LegacyTxtRuntime;
use crate::model::CanonicalEvent;
use crate::store::Store;

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
    pub ingest: Arc<RwLock<Option<IngestManager>>>,
    pub start_time: Instant,
    pub optout_codes: Arc<Mutex<HashMap<String, Instant>>>,
}

pub async fn run_cli() -> Result<()> {
    let cli = Cli::parse();
    let config = Config::load(&cli.config)?;
    init_tracing(&config.log_level);
    let debug_runtime = Arc::new(DebugRuntime::from_env(&config.logs_directory)?);
    let legacy_txt = Arc::new(LegacyTxtRuntime::from_env(&config.logs_directory));

    let shared_config = Arc::new(RwLock::new(config.clone()));
    let store = Store::open(&config)?;
    run_startup_validation(debug_runtime.clone(), store.clone(), chrono::Utc::now()).await?;

    if let Some(Commands::ImportLegacy { source }) = cli.command {
        let imported = import_legacy_logs(&store, &source)?;
        info!("imported {imported} legacy events");
        return Ok(());
    }

    let helix = HelixClient::new(&config);
    let ingest_slot = Arc::new(RwLock::new(None));
    let state = AppState {
        config: shared_config.clone(),
        store: store.clone(),
        helix: helix.clone(),
        legacy_txt,
        debug_runtime: debug_runtime.clone(),
        ingest: ingest_slot.clone(),
        start_time: Instant::now(),
        optout_codes: Arc::new(Mutex::new(HashMap::new())),
    };

    if config.oauth.trim().is_empty() {
        info!("oauth not configured; starting ingest in anonymous justinfan mode");
    }
    let command_service = Arc::new(CommandService::new(state.clone()));
    let ingest = IngestManager::new(shared_config.clone(), store.clone(), command_service);
    *ingest_slot.write().await = Some(ingest.clone());

    let initial_channels = resolve_channel_logins(&helix, &config.channels).await?;
    ingest.start(initial_channels).await;
    spawn_compactor(shared_config.clone(), store.clone(), debug_runtime);

    serve(state, &config.listen_address).await
}

async fn serve(state: AppState, listen_address: &str) -> Result<()> {
    let bind_address = normalize_listen_address(listen_address);
    let listener = TcpListener::bind(bind_address).await?;
    let local_address = listener.local_addr()?;
    info!("Listening on {local_address}");
    loop {
        let (stream, remote_address) = listener.accept().await?;
        let state = state.clone();
        tokio::spawn(async move {
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
                let uptime = format_duration(self.state.start_time.elapsed());
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
                if expires_at <= Instant::now() {
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
