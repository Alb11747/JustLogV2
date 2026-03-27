use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use moka::sync::Cache;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{RwLock, broadcast, watch};
use tokio::time::sleep;
use tokio_native_tls::{TlsConnector, native_tls};
use tracing::{error, info, warn};
use twitch_irc::message::{IRCMessage, ServerMessage};

use crate::config::Config;
use crate::model::{CanonicalEvent, OutboundMessage};
use crate::store::Store;

trait IoStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T> IoStream for T where T: AsyncRead + AsyncWrite + Unpin + Send {}

type BoxedStream = Box<dyn IoStream>;

#[async_trait]
pub trait ChatCommandService: Send + Sync {
    async fn handle_privmsg_command(&self, event: &CanonicalEvent) -> Result<()>;
}

#[derive(Clone)]
pub struct IngestManager {
    config: Arc<RwLock<Config>>,
    store: Store,
    commands: Arc<dyn ChatCommandService>,
    outbound: broadcast::Sender<OutboundCommand>,
    shutdown: watch::Sender<bool>,
    desired_channels: Arc<RwLock<HashSet<String>>>,
    rr_counter: Arc<AtomicUsize>,
    dedupe: Cache<String, ()>,
}

#[derive(Clone, Debug)]
enum OutboundCommand {
    Join(String),
    Part(String),
    Say(OutboundMessage),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct IrcIdentity {
    pass_line: String,
    nick: String,
    anonymous: bool,
}

impl IngestManager {
    pub fn new(
        config: Arc<RwLock<Config>>,
        store: Store,
        commands: Arc<dyn ChatCommandService>,
    ) -> Self {
        let (outbound, _) = broadcast::channel(1024);
        let (shutdown, _) = watch::channel(false);
        Self {
            config,
            store,
            commands,
            outbound,
            shutdown,
            desired_channels: Arc::new(RwLock::new(HashSet::new())),
            rr_counter: Arc::new(AtomicUsize::new(0)),
            dedupe: Cache::builder()
                .time_to_live(Duration::from_secs(5))
                .max_capacity(50_000)
                .build(),
        }
    }

    pub async fn start(&self, initial_channels: Vec<String>) {
        {
            let mut desired = self.desired_channels.write().await;
            *desired = initial_channels
                .into_iter()
                .map(|value| value.to_lowercase())
                .collect();
        }
        let redundancy = self.config.read().await.ingest.redundancy_factor.max(1);
        for lane_index in 0..redundancy {
            let lane = self.clone();
            tokio::spawn(async move {
                lane.run_lane(lane_index).await;
            });
        }
    }

    pub fn stop(&self) {
        let _ = self.shutdown.send(true);
    }

    pub async fn join_channels(&self, channels: &[String]) {
        let mut desired = self.desired_channels.write().await;
        for channel in channels {
            let channel = channel.to_lowercase();
            desired.insert(channel.clone());
            let _ = self.outbound.send(OutboundCommand::Join(channel));
        }
    }

    pub async fn part_channels(&self, channels: &[String]) {
        let mut desired = self.desired_channels.write().await;
        for channel in channels {
            let channel = channel.to_lowercase();
            desired.remove(&channel);
            let _ = self.outbound.send(OutboundCommand::Part(channel));
        }
    }

    pub async fn say(&self, channel: &str, text: &str) {
        let _ = self.outbound.send(OutboundCommand::Say(OutboundMessage {
            channel: channel.to_lowercase(),
            text: text.to_string(),
        }));
        self.rr_counter.fetch_add(1, Ordering::Relaxed);
    }

    async fn run_lane(&self, lane_index: usize) {
        let mut shutdown = self.shutdown.subscribe();
        loop {
            match self.connect_once(lane_index).await {
                Ok(()) => warn!("ingest lane {lane_index} disconnected cleanly"),
                Err(error) => warn!("ingest lane {lane_index} disconnected: {error}"),
            }
            if *shutdown.borrow() {
                info!("ingest lane {lane_index} stopping");
                return;
            }
            tokio::select! {
                _ = shutdown.changed() => {
                    info!("ingest lane {lane_index} stopping");
                    return;
                }
                _ = sleep(Duration::from_millis(750)) => {}
            }
        }
    }

    async fn connect_once(&self, lane_index: usize) -> Result<()> {
        let mut shutdown = self.shutdown.subscribe();
        let config = self.config.read().await.clone();
        let stream = connect_stream(&config).await?;
        let (reader_half, mut writer_half) = tokio::io::split(stream);
        let mut reader = BufReader::new(reader_half).lines();
        let mut outbound_rx = self.outbound.subscribe();
        let identity = irc_identity(&config);

        write_irc_line(&mut writer_half, &identity.pass_line).await?;
        write_irc_line(&mut writer_half, &format!("NICK {}", identity.nick)).await?;
        write_irc_line(
            &mut writer_half,
            "CAP REQ :twitch.tv/commands twitch.tv/tags twitch.tv/membership",
        )
        .await?;

        let desired_channels = self.desired_channels.read().await.clone();
        for channel in desired_channels {
            write_irc_line(&mut writer_half, &format!("JOIN #{channel}")).await?;
        }
        if identity.anonymous {
            info!(
                "ingest lane {lane_index} connected anonymously as {}",
                identity.nick
            );
        } else {
            info!("ingest lane {lane_index} connected");
        }

        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    return Ok(());
                }
                maybe_line = reader.next_line() => {
                    let line = maybe_line?;
                    let Some(line) = line else {
                        return Err(anyhow!("EOF"));
                    };
                    if line.starts_with("PING ") {
                        write_irc_line(&mut writer_half, &line.replacen("PING", "PONG", 1)).await?;
                        continue;
                    }

                    let irc = match IRCMessage::parse(&line) {
                        Ok(message) => message,
                        Err(error) => {
                            warn!("failed to parse IRC line: {error}");
                            continue;
                        }
                    };

                    let server_message = match ServerMessage::try_from(irc) {
                        Ok(message) => message,
                        Err(error) => {
                            warn!("failed to parse server message: {error}");
                            continue;
                        }
                    };

                    if matches!(server_message, ServerMessage::Reconnect(_)) {
                        return Err(anyhow!("server requested reconnect"));
                    }

                    if let Some(event) = CanonicalEvent::from_server_message(&line, server_message) {
                        if self.dedupe.get(&event.event_uid).is_some() {
                            continue;
                        }
                        self.dedupe.insert(event.event_uid.clone(), ());
                        if event.kind == crate::model::PRIVMSG_TYPE {
                            self.commands.handle_privmsg_command(&event).await?;
                        }
                        let cfg = self.config.read().await;
                        if cfg.is_opted_out(&event.room_id)
                            || event.user_id.as_deref().is_some_and(|user_id| cfg.is_opted_out(user_id))
                            || event.target_user_id.as_deref().is_some_and(|user_id| cfg.is_opted_out(user_id))
                        {
                            continue;
                        }
                        drop(cfg);
                        let store = self.store.clone();
                        let event_to_store = event.clone();
                        let insert_result = tokio::task::spawn_blocking(move || {
                            store.insert_event(&event_to_store)
                        })
                        .await;
                        match insert_result {
                            Ok(Ok(_)) => {}
                            Ok(Err(error)) => {
                                error!("failed to persist event: {error}");
                            }
                            Err(error) => {
                                error!("failed to join blocking event persistence task: {error}");
                            }
                        }
                    }
                }
                outbound = outbound_rx.recv() => {
                    match outbound {
                        Ok(OutboundCommand::Join(channel)) => {
                            write_irc_line(&mut writer_half, &format!("JOIN #{channel}")).await?;
                        }
                        Ok(OutboundCommand::Part(channel)) => {
                            write_irc_line(&mut writer_half, &format!("PART #{channel}")).await?;
                        }
                        Ok(OutboundCommand::Say(message)) => {
                            write_irc_line(&mut writer_half, &format!("PRIVMSG #{} :{}", message.channel, message.text)).await?;
                        }
                        Err(broadcast::error::RecvError::Lagged(skipped)) => {
                            warn!("ingest lane {lane_index} lagged on outbound channel by {skipped}");
                        }
                        Err(broadcast::error::RecvError::Closed) => return Ok(()),
                    }
                }
            }
        }
    }
}

async fn connect_stream(config: &Config) -> Result<BoxedStream> {
    let connect_timeout = Duration::from_millis(config.ingest.connect_timeout_ms);
    let address = format!("{}:{}", config.irc.server, config.irc.port);
    let tcp_stream = tokio::time::timeout(connect_timeout, TcpStream::connect(address)).await??;
    if config.irc.tls {
        let connector = native_tls::TlsConnector::builder().build()?;
        let connector = TlsConnector::from(connector);
        let stream = connector.connect(&config.irc.server, tcp_stream).await?;
        Ok(Box::new(stream))
    } else {
        Ok(Box::new(tcp_stream))
    }
}

fn irc_identity(config: &Config) -> IrcIdentity {
    if config.oauth.trim().is_empty() {
        IrcIdentity {
            pass_line: "PASS _".to_string(),
            nick: generate_justinfan_username(),
            anonymous: true,
        }
    } else {
        IrcIdentity {
            pass_line: format!("PASS oauth:{}", config.oauth),
            nick: config.username.clone(),
            anonymous: false,
        }
    }
}

fn generate_justinfan_username() -> String {
    let digits = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() % 1_000_000_000)
        .unwrap_or(0);
    format!("justinfan{digits}")
}

async fn write_irc_line<W>(writer: &mut W, line: &str) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\r\n").await?;
    writer.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{generate_justinfan_username, irc_identity};
    use crate::config::Config;

    #[test]
    fn anonymous_identity_uses_placeholder_pass_and_generated_justinfan_nick() {
        let mut config = serde_json::from_str::<Config>(
            r#"{
                "logsDirectory": "./logs",
                "oauth": ""
            }"#,
        )
        .unwrap();
        config.normalize().unwrap();

        let identity = irc_identity(&config);

        assert_eq!(identity.pass_line, "PASS _");
        assert!(identity.anonymous);
        assert!(identity.nick.starts_with("justinfan"));
        assert!(identity.nick[9..].chars().all(|ch| ch.is_ascii_digit()));
    }

    #[test]
    fn authenticated_identity_preserves_existing_pass_and_username() {
        let mut config = serde_json::from_str::<Config>(
            r#"{
                "logsDirectory": "./logs",
                "username": "loggerbot",
                "oauth": "oauth:test-token"
            }"#,
        )
        .unwrap();
        config.normalize().unwrap();

        let identity = irc_identity(&config);

        assert_eq!(identity.pass_line, "PASS oauth:test-token");
        assert_eq!(identity.nick, "loggerbot");
        assert!(!identity.anonymous);
    }

    #[test]
    fn generated_justinfan_username_has_numeric_suffix() {
        let nick = generate_justinfan_username();
        assert!(nick.starts_with("justinfan"));
        assert!(nick[9..].chars().all(|ch| ch.is_ascii_digit()));
    }
}
