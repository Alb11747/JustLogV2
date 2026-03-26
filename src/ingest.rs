use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use moka::sync::Cache;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::{RwLock, broadcast};
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

impl IngestManager {
    pub fn new(
        config: Arc<RwLock<Config>>,
        store: Store,
        commands: Arc<dyn ChatCommandService>,
    ) -> Self {
        let (outbound, _) = broadcast::channel(1024);
        Self {
            config,
            store,
            commands,
            outbound,
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
        loop {
            match self.connect_once(lane_index).await {
                Ok(()) => warn!("ingest lane {lane_index} disconnected cleanly"),
                Err(error) => warn!("ingest lane {lane_index} disconnected: {error}"),
            }
            sleep(Duration::from_millis(750)).await;
        }
    }

    async fn connect_once(&self, lane_index: usize) -> Result<()> {
        let config = self.config.read().await.clone();
        let stream = connect_stream(&config).await?;
        let (reader_half, mut writer_half) = tokio::io::split(stream);
        let mut reader = BufReader::new(reader_half).lines();
        let mut outbound_rx = self.outbound.subscribe();

        write_irc_line(&mut writer_half, &format!("PASS oauth:{}", config.oauth)).await?;
        write_irc_line(&mut writer_half, &format!("NICK {}", config.username)).await?;
        write_irc_line(
            &mut writer_half,
            "CAP REQ :twitch.tv/commands twitch.tv/tags twitch.tv/membership",
        )
        .await?;

        let desired_channels = self.desired_channels.read().await.clone();
        for channel in desired_channels {
            write_irc_line(&mut writer_half, &format!("JOIN #{channel}")).await?;
        }

        info!("ingest lane {lane_index} connected");

        loop {
            tokio::select! {
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
                        if let Err(error) = self.store.insert_event(&event) {
                            error!("failed to persist event: {error}");
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

async fn write_irc_line<W>(writer: &mut W, line: &str) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(line.as_bytes()).await?;
    writer.write_all(b"\r\n").await?;
    writer.flush().await?;
    Ok(())
}
