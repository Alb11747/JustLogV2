use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(skip)]
    pub config_path: PathBuf,
    #[serde(skip)]
    pub config_file_permissions: Option<u32>,
    #[serde(default)]
    pub bot_verified: bool,
    #[serde(default = "default_logs_directory", rename = "logsDirectory")]
    pub logs_directory: PathBuf,
    #[serde(default = "default_archive")]
    pub archive: bool,
    #[serde(default, rename = "adminAPIKey")]
    pub admin_api_key: String,
    #[serde(default = "default_username")]
    pub username: String,
    #[serde(default = "default_oauth")]
    pub oauth: String,
    #[serde(default = "default_listen_address", rename = "listenAddress")]
    pub listen_address: String,
    #[serde(default = "default_admins")]
    pub admins: Vec<String>,
    #[serde(default)]
    pub channels: Vec<String>,
    #[serde(default, rename = "clientID")]
    pub client_id: String,
    #[serde(default, rename = "clientSecret")]
    pub client_secret: String,
    #[serde(default = "default_log_level", rename = "logLevel")]
    pub log_level: String,
    #[serde(default, rename = "optOut")]
    pub opt_out: HashMap<String, bool>,
    #[serde(default)]
    pub compression: CompressionConfig,
    #[serde(default, rename = "http")]
    pub http: HttpConfig,
    #[serde(default)]
    pub ingest: IngestConfig,
    #[serde(default)]
    pub helix: HelixConfig,
    #[serde(default)]
    pub irc: IrcConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub ops: OpsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionConfig {
    #[serde(default = "default_algorithm")]
    pub algorithm: String,
    #[serde(default = "default_quality")]
    pub quality: u32,
    #[serde(default = "default_lgwin")]
    pub lgwin: u32,
    #[serde(default = "default_mode")]
    pub mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpConfig {
    #[serde(default = "default_true", rename = "precompressedStreaming")]
    pub precompressed_streaming: bool,
    #[serde(default = "default_true", rename = "onTheFlyCompression")]
    pub on_the_fly_compression: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestConfig {
    #[serde(default = "default_redundancy_factor", rename = "redundancyFactor")]
    pub redundancy_factor: usize,
    #[serde(
        default = "default_max_channels_per_connection",
        rename = "maxChannelsPerConnection"
    )]
    pub max_channels_per_connection: usize,
    #[serde(default = "default_connect_timeout_ms", rename = "connectTimeoutMs")]
    pub connect_timeout_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelixConfig {
    #[serde(default, rename = "baseUrl")]
    pub base_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrcConfig {
    #[serde(default = "default_irc_server")]
    pub server: String,
    #[serde(default = "default_irc_port")]
    pub port: u16,
    #[serde(default = "default_irc_tls")]
    pub tls: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    #[serde(default, rename = "sqlitePath")]
    pub sqlite_path: PathBuf,
    #[serde(
        default = "default_compact_interval_seconds",
        rename = "compactIntervalSeconds"
    )]
    pub compact_interval_seconds: u64,
    #[serde(
        default = "default_compact_after_channel_days",
        rename = "compactAfterChannelDays"
    )]
    pub compact_after_channel_days: i64,
    #[serde(
        default = "default_compact_after_user_months",
        rename = "compactAfterUserMonths"
    )]
    pub compact_after_user_months: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpsConfig {
    #[serde(default, rename = "metricsEnabled")]
    pub metrics_enabled: bool,
    #[serde(default = "default_metrics_route", rename = "metricsRoute")]
    pub metrics_route: String,
}

pub type SharedConfig = Arc<RwLock<Config>>;

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            algorithm: default_algorithm(),
            quality: default_quality(),
            lgwin: default_lgwin(),
            mode: default_mode(),
        }
    }
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            precompressed_streaming: default_true(),
            on_the_fly_compression: default_true(),
        }
    }
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            redundancy_factor: default_redundancy_factor(),
            max_channels_per_connection: default_max_channels_per_connection(),
            connect_timeout_ms: default_connect_timeout_ms(),
        }
    }
}

impl Default for HelixConfig {
    fn default() -> Self {
        Self {
            base_url: String::new(),
        }
    }
}

impl Default for IrcConfig {
    fn default() -> Self {
        Self {
            server: default_irc_server(),
            port: default_irc_port(),
            tls: default_irc_tls(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            sqlite_path: PathBuf::new(),
            compact_interval_seconds: default_compact_interval_seconds(),
            compact_after_channel_days: default_compact_after_channel_days(),
            compact_after_user_months: default_compact_after_user_months(),
        }
    }
}

impl Default for OpsConfig {
    fn default() -> Self {
        Self {
            metrics_enabled: false,
            metrics_route: default_metrics_route(),
        }
    }
}

impl Config {
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let content = fs::read_to_string(&path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        let mut config: Config = serde_json::from_str(&content)
            .with_context(|| format!("failed to parse config {}", path.display()))?;
        config.config_path = path.clone();
        config.config_file_permissions = fs::metadata(&path)
            .ok()
            .map(|metadata| metadata.permissions().readonly())
            .map(|readonly| if readonly { 0o444 } else { 0o644 });
        config.normalize()?;
        Ok(config)
    }

    pub fn normalize(&mut self) -> Result<()> {
        self.logs_directory = normalize_path(&self.logs_directory);
        if self.storage.sqlite_path.as_os_str().is_empty() {
            self.storage.sqlite_path = self.logs_directory.join("justlog.sqlite3");
        }
        self.storage.sqlite_path = normalize_path(&self.storage.sqlite_path);
        self.oauth = self.oauth.trim_start_matches("oauth:").to_string();
        self.log_level = self.log_level.to_lowercase();
        self.admins = self
            .admins
            .iter()
            .map(|value| value.to_lowercase())
            .collect::<Vec<_>>();
        self.channels = dedupe(
            self.channels
                .iter()
                .map(|value| value.to_string())
                .collect(),
        );
        Ok(())
    }

    pub fn is_opted_out(&self, user_id: &str) -> bool {
        self.opt_out.contains_key(user_id)
    }

    pub fn add_channels(&mut self, channel_ids: &[String]) {
        let mut channel_set = self.channels.iter().cloned().collect::<HashSet<_>>();
        for channel_id in channel_ids {
            channel_set.insert(channel_id.clone());
        }
        self.channels = channel_set.into_iter().collect();
        self.channels.sort();
    }

    pub fn remove_channels(&mut self, channel_ids: &[String]) {
        let remove = channel_ids.iter().cloned().collect::<HashSet<_>>();
        self.channels.retain(|channel| !remove.contains(channel));
    }

    pub fn opt_out_users(&mut self, user_ids: &[String]) {
        for user_id in user_ids {
            self.opt_out.insert(user_id.clone(), true);
        }
    }

    pub fn remove_opt_out(&mut self, user_ids: &[String]) {
        for user_id in user_ids {
            self.opt_out.remove(user_id);
        }
    }

    pub fn persist(&self) -> Result<()> {
        let parent = self
            .config_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        fs::create_dir_all(&parent)
            .with_context(|| format!("failed to create config directory {}", parent.display()))?;
        let temp_path = self.config_path.with_extension("json.tmp");
        let mut persisted = self.clone();
        persisted.config_path = PathBuf::new();
        persisted.config_file_permissions = None;
        let serialized = serde_json::to_string_pretty(&persisted)?;
        fs::write(&temp_path, serialized)?;
        fs::rename(&temp_path, &self.config_path)?;
        Ok(())
    }
}

fn dedupe(values: Vec<String>) -> Vec<String> {
    let mut set = HashSet::new();
    let mut deduped = Vec::new();
    for value in values {
        if set.insert(value.clone()) {
            deduped.push(value);
        }
    }
    deduped
}

fn normalize_path(path: &Path) -> PathBuf {
    let mut rendered = path.to_string_lossy().replace('\\', "/");
    while rendered.ends_with('/') && rendered.len() > 1 {
        rendered.pop();
    }
    PathBuf::from(rendered)
}

fn default_logs_directory() -> PathBuf {
    PathBuf::from("./logs")
}

fn default_archive() -> bool {
    true
}

fn default_username() -> String {
    "justinfan777777".to_string()
}

fn default_oauth() -> String {
    "oauth:777777777".to_string()
}

fn default_listen_address() -> String {
    ":8025".to_string()
}

fn default_admins() -> Vec<String> {
    vec!["gempir".to_string()]
}

fn default_log_level() -> String {
    "info".to_string()
}

fn default_algorithm() -> String {
    "brotli".to_string()
}

fn default_quality() -> u32 {
    11
}

fn default_lgwin() -> u32 {
    22
}

fn default_mode() -> String {
    "text".to_string()
}

fn default_true() -> bool {
    true
}

fn default_redundancy_factor() -> usize {
    2
}

fn default_max_channels_per_connection() -> usize {
    100
}

fn default_connect_timeout_ms() -> u64 {
    15_000
}

fn default_irc_server() -> String {
    "irc.chat.twitch.tv".to_string()
}

fn default_irc_port() -> u16 {
    6697
}

fn default_irc_tls() -> bool {
    true
}

fn default_compact_interval_seconds() -> u64 {
    60
}

fn default_compact_after_channel_days() -> i64 {
    1
}

fn default_compact_after_user_months() -> i64 {
    1
}

fn default_metrics_route() -> String {
    "/metrics".to_string()
}
