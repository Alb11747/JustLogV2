use std::collections::HashMap;
use std::convert::TryFrom;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Datelike, Utc};
use serde::{Deserialize, Serialize};
use twitch_irc::message::{ClearChatAction, IRCMessage, ServerMessage};

pub const PRIVMSG_TYPE: i32 = 1;
pub const CLEARCHAT_TYPE: i32 = 2;
pub const USERNOTICE_TYPE: i32 = 4;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalEvent {
    pub event_uid: String,
    pub room_id: String,
    pub channel_login: String,
    pub username: String,
    pub display_name: String,
    pub user_id: Option<String>,
    pub target_user_id: Option<String>,
    pub text: String,
    pub system_text: String,
    pub timestamp: DateTime<Utc>,
    pub raw: String,
    pub tags: HashMap<String, String>,
    pub kind: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredEvent {
    pub seq: i64,
    pub event_uid: String,
    pub room_id: String,
    pub channel_login: String,
    pub username: String,
    pub display_name: String,
    pub user_id: Option<String>,
    pub target_user_id: Option<String>,
    pub text: String,
    pub system_text: String,
    pub timestamp: DateTime<Utc>,
    pub raw: String,
    pub tags: HashMap<String, String>,
    pub kind: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub text: String,
    #[serde(rename = "systemText")]
    pub system_text: String,
    pub username: String,
    #[serde(rename = "displayName")]
    pub display_name: String,
    pub channel: String,
    pub timestamp: DateTime<Utc>,
    pub id: String,
    #[serde(rename = "type")]
    pub message_type: i32,
    pub raw: String,
    pub tags: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChatLog {
    pub messages: Vec<ChatMessage>,
}

#[derive(Debug, Clone, Serialize)]
pub struct AllChannelsJson {
    pub channels: Vec<ChannelInfo>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChannelInfo {
    #[serde(rename = "userID")]
    pub user_id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct UserLogList {
    #[serde(rename = "availableLogs")]
    pub available_logs: Vec<UserLogFile>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ChannelLogList {
    #[serde(rename = "availableLogs")]
    pub available_logs: Vec<ChannelLogFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserLogFile {
    pub year: String,
    pub month: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChannelLogFile {
    pub year: String,
    pub month: String,
    pub day: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ErrorResponse {
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChannelDayKey {
    pub channel_id: String,
    pub year: i32,
    pub month: u32,
    pub day: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UserMonthKey {
    pub channel_id: String,
    pub user_id: String,
    pub year: i32,
    pub month: u32,
}

#[derive(Debug, Clone)]
pub struct ChannelPartitionSummary {
    pub key: ChannelDayKey,
    pub count: i64,
}

#[derive(Debug, Clone)]
pub struct UserPartitionSummary {
    pub key: UserMonthKey,
    pub count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentRecord {
    pub id: i64,
    pub scope: String,
    pub channel_id: String,
    pub user_id: Option<String>,
    pub year: i32,
    pub month: u32,
    pub day: Option<u32>,
    pub path: String,
    pub line_count: i64,
    pub start_ts: i64,
    pub end_ts: i64,
    pub compression: String,
    pub passthrough_raw: bool,
}

#[derive(Debug, Clone)]
pub struct OutboundMessage {
    pub channel: String,
    pub text: String,
}

impl CanonicalEvent {
    pub fn from_raw(raw: &str) -> Result<Option<Self>> {
        let irc =
            IRCMessage::parse(raw).with_context(|| format!("failed to parse IRC line: {raw}"))?;
        let server_message = ServerMessage::try_from(irc).map_err(|error| anyhow!(error))?;
        Ok(Self::from_server_message(raw, server_message))
    }

    pub fn from_server_message(raw: &str, server_message: ServerMessage) -> Option<Self> {
        match server_message {
            ServerMessage::Privmsg(message) => Some(Self {
                event_uid: format!("privmsg:{}:{}", message.channel_id, message.message_id),
                room_id: message.channel_id.clone(),
                channel_login: message.channel_login.clone(),
                username: message.sender.login.clone(),
                display_name: message.sender.name.clone(),
                user_id: Some(message.sender.id.clone()),
                target_user_id: None,
                text: message.message_text.clone(),
                system_text: String::new(),
                timestamp: message.server_timestamp,
                raw: raw.to_string(),
                tags: tags_to_map(&message.source.tags),
                kind: PRIVMSG_TYPE,
            }),
            ServerMessage::ClearChat(message) => {
                let (username, target_user_id, system_text) = match message.action {
                    ClearChatAction::ChatCleared => {
                        (String::new(), None, "chat has been cleared".to_string())
                    }
                    ClearChatAction::UserBanned {
                        user_login,
                        user_id,
                    } => (
                        user_login.clone(),
                        Some(user_id.clone()),
                        format!("{user_login} has been banned"),
                    ),
                    ClearChatAction::UserTimedOut {
                        user_login,
                        user_id,
                        timeout_length,
                    } => (
                        user_login.clone(),
                        Some(user_id.clone()),
                        build_timeout_text(&user_login, timeout_length),
                    ),
                };
                Some(Self {
                    event_uid: format!("clearchat:{}", blake3::hash(raw.as_bytes()).to_hex()),
                    room_id: message.channel_id.clone(),
                    channel_login: message.channel_login.clone(),
                    username,
                    display_name: String::new(),
                    user_id: None,
                    target_user_id,
                    text: String::new(),
                    system_text,
                    timestamp: message.server_timestamp,
                    raw: raw.to_string(),
                    tags: tags_to_map(&message.source.tags),
                    kind: CLEARCHAT_TYPE,
                })
            }
            ServerMessage::UserNotice(message) => {
                let target_user_id = message
                    .source
                    .tags
                    .0
                    .get("msg-param-recipient-id")
                    .and_then(Clone::clone);
                Some(Self {
                    event_uid: format!("usernotice:{}:{}", message.channel_id, message.message_id),
                    room_id: message.channel_id.clone(),
                    channel_login: message.channel_login.clone(),
                    username: message.sender.login.clone(),
                    display_name: message.sender.name.clone(),
                    user_id: Some(message.sender.id.clone()),
                    target_user_id,
                    text: message.message_text.unwrap_or_default(),
                    system_text: message.system_message,
                    timestamp: message.server_timestamp,
                    raw: raw.to_string(),
                    tags: tags_to_map(&message.source.tags),
                    kind: USERNOTICE_TYPE,
                })
            }
            _ => None,
        }
    }

    pub fn channel_day_key(&self) -> ChannelDayKey {
        ChannelDayKey {
            channel_id: self.room_id.clone(),
            year: self.timestamp.year(),
            month: self.timestamp.month(),
            day: self.timestamp.day(),
        }
    }

    pub fn user_month_keys(&self) -> Vec<UserMonthKey> {
        let mut keys = Vec::new();
        for user_id in [self.user_id.clone(), self.target_user_id.clone()]
            .into_iter()
            .flatten()
        {
            keys.push(UserMonthKey {
                channel_id: self.room_id.clone(),
                user_id,
                year: self.timestamp.year(),
                month: self.timestamp.month(),
            });
        }
        keys
    }
}

impl From<StoredEvent> for ChatMessage {
    fn from(event: StoredEvent) -> Self {
        ChatMessage {
            text: event.text,
            system_text: event.system_text,
            username: event.username,
            display_name: event.display_name,
            channel: event.channel_login,
            timestamp: event.timestamp,
            id: event.event_uid,
            message_type: event.kind,
            raw: event.raw,
            tags: event.tags,
        }
    }
}

fn tags_to_map(tags: &twitch_irc::message::IRCTags) -> HashMap<String, String> {
    tags.0
        .iter()
        .map(|(key, value)| (key.clone(), value.clone().unwrap_or_default()))
        .collect()
}

fn build_timeout_text(username: &str, timeout_length: Duration) -> String {
    format!(
        "{username} has been timed out for {} seconds",
        timeout_length.as_secs()
    )
}
