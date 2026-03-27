use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};
use chrono::{DateTime, TimeZone, Utc};
use flate2::read::GzDecoder;
use regex::Regex;
use serde::Deserialize;
use walkdir::WalkDir;

use crate::model::{CanonicalEvent, ChannelLogFile, ChatMessage, PRIVMSG_TYPE};
use crate::store::{Store, load_lines_from_text_file};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyTxtMode {
    Off,
    MissingOnly,
    Merge,
}

#[derive(Debug, Clone)]
pub struct LegacyTxtRuntime {
    import_folder: Option<PathBuf>,
    check_each_request: bool,
    mode: LegacyTxtMode,
    import_folder_exists_at_startup: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ImportKind {
    RawIrc,
    SimpleText,
    JsonExport,
}

#[derive(Debug, Clone)]
struct ImportFile {
    path: PathBuf,
    fingerprint: String,
    kind: ImportKind,
    year: i32,
    month: u32,
    day: u32,
}

#[derive(Debug, Clone, Default)]
pub struct ChannelDayImport {
    pub complete_messages: Vec<ChatMessage>,
    pub simple_messages: Vec<ChatMessage>,
}

impl LegacyTxtRuntime {
    pub fn from_env(_default_logs_directory: &Path) -> Self {
        let import_folder = env::var("JUSTLOG_IMPORT_FOLDER")
            .ok()
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
            .map(PathBuf::from);
        let import_folder_exists_at_startup =
            import_folder.as_ref().is_some_and(|path| path.exists());
        let mode = match env::var("JUSTLOG_LEGACY_TXT_MODE")
            .unwrap_or_else(|_| "missing_only".to_string())
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "off" => LegacyTxtMode::Off,
            "merge" => LegacyTxtMode::Merge,
            _ => LegacyTxtMode::MissingOnly,
        };
        Self {
            import_folder,
            check_each_request: env_flag("JUSTLOG_LEGACY_TXT_CHECK_EACH_REQUEST", false),
            mode,
            import_folder_exists_at_startup,
        }
    }

    pub fn mode(&self) -> LegacyTxtMode {
        self.mode
    }

    pub fn is_import_enabled(&self) -> bool {
        self.import_folder_path().is_some()
    }

    pub fn import_raw_channel_day(
        &self,
        store: &Store,
        channel_id: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<()> {
        for file in self.discover_channel_day_files(channel_id, year, month, day)? {
            if file.kind != ImportKind::RawIrc {
                continue;
            }
            let path_key = file.path.to_string_lossy().to_string();
            if store.imported_raw_file_is_current(&path_key, &file.fingerprint)? {
                continue;
            }
            let mut imported = 0usize;
            for line in load_lines_from_supported_file(&file.path)? {
                match CanonicalEvent::from_raw(&line) {
                    Ok(Some(event)) => {
                        if store.insert_event(&event)? {
                            imported += 1;
                        }
                    }
                    Ok(None) => {}
                    Err(_) => {}
                }
            }
            let status = if imported > 0 { "imported" } else { "seen" };
            store.record_imported_raw_file(&path_key, &file.fingerprint, status)?;
        }
        Ok(())
    }

    pub fn import_raw_channel(&self, store: &Store, channel_id: &str) -> Result<()> {
        for file in self.discover_channel_files(channel_id)? {
            if file.kind != ImportKind::RawIrc {
                continue;
            }
            let path_key = file.path.to_string_lossy().to_string();
            if store.imported_raw_file_is_current(&path_key, &file.fingerprint)? {
                continue;
            }
            let mut imported = 0usize;
            for line in load_lines_from_supported_file(&file.path)? {
                match CanonicalEvent::from_raw(&line) {
                    Ok(Some(event)) => {
                        if store.insert_event(&event)? {
                            imported += 1;
                        }
                    }
                    Ok(None) => {}
                    Err(_) => {}
                }
            }
            let status = if imported > 0 { "imported" } else { "seen" };
            store.record_imported_raw_file(&path_key, &file.fingerprint, status)?;
        }
        Ok(())
    }

    pub fn load_channel_day_import(
        &self,
        channel_id: &str,
        channel_login: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<ChannelDayImport> {
        let mut result = ChannelDayImport::default();
        for file in self.discover_channel_day_files(channel_id, year, month, day)? {
            match file.kind {
                ImportKind::RawIrc => {}
                ImportKind::SimpleText => {
                    if let Ok(messages) =
                        parse_sparse_txt_file(&file.path, channel_login, year, month, day)
                    {
                        result.simple_messages.extend(messages);
                    }
                }
                ImportKind::JsonExport => {
                    if let Ok(messages) = parse_json_export_file(&file.path, channel_login) {
                        result.complete_messages.extend(messages);
                    }
                }
            }
        }
        result
            .complete_messages
            .sort_by(|left, right| left.timestamp.cmp(&right.timestamp));
        result
            .simple_messages
            .sort_by(|left, right| left.timestamp.cmp(&right.timestamp));
        Ok(result)
    }

    pub fn available_channel_logs(&self, channel_id: &str) -> Result<Vec<ChannelLogFile>> {
        let mut logs = Vec::new();
        let mut seen = HashSet::new();
        for file in self.discover_channel_files(channel_id)? {
            if file.kind == ImportKind::SimpleText && self.mode == LegacyTxtMode::Off {
                continue;
            }
            let key = (file.year, file.month, file.day);
            if seen.insert(key) {
                logs.push(ChannelLogFile {
                    year: file.year.to_string(),
                    month: file.month.to_string(),
                    day: file.day.to_string(),
                });
            }
        }
        logs.sort_by(|left, right| {
            right
                .year
                .cmp(&left.year)
                .then_with(|| right.month.cmp(&left.month))
                .then_with(|| right.day.cmp(&left.day))
        });
        Ok(logs)
    }

    fn discover_channel_day_files(
        &self,
        channel_id: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<Vec<ImportFile>> {
        let Some(root) = self.import_folder_path() else {
            return Ok(Vec::new());
        };
        let mut files = Vec::new();
        for entry in WalkDir::new(&root).into_iter().filter_map(Result::ok) {
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry.into_path();
            let Some(details) = match_import_path(&root, &path) else {
                continue;
            };
            if details.0 != channel_id
                || details.1 != year
                || details.2 != month
                || details.3 != day
            {
                continue;
            }
            if let Some(kind) = classify_import_file(&path)? {
                files.push(ImportFile {
                    path: path.clone(),
                    fingerprint: file_fingerprint(&path)?,
                    kind,
                    year,
                    month,
                    day,
                });
            }
        }
        files.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(files)
    }

    fn discover_channel_files(&self, channel_id: &str) -> Result<Vec<ImportFile>> {
        let Some(root) = self.import_folder_path() else {
            return Ok(Vec::new());
        };
        let mut files = Vec::new();
        for entry in WalkDir::new(&root).into_iter().filter_map(Result::ok) {
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry.into_path();
            let Some((matched_channel_id, year, month, day)) = match_import_path(&root, &path)
            else {
                continue;
            };
            if matched_channel_id != channel_id {
                continue;
            }
            if let Some(kind) = classify_import_file(&path)? {
                files.push(ImportFile {
                    path: path.clone(),
                    fingerprint: file_fingerprint(&path)?,
                    kind,
                    year,
                    month,
                    day,
                });
            }
        }
        files.sort_by(|left, right| left.path.cmp(&right.path));
        Ok(files)
    }

    fn import_folder_path(&self) -> Option<PathBuf> {
        let root = self.import_folder.clone()?;
        if self.check_each_request {
            if !root.exists() {
                return None;
            }
            prune_empty_dirs(&root);
            return Some(root);
        }
        if self.import_folder_exists_at_startup && root.exists() {
            prune_empty_dirs(&root);
            return Some(root);
        }
        if self.import_folder_exists_at_startup {
            return Some(root);
        }
        None
    }
}

pub fn merge_messages(
    mut native: Vec<ChatMessage>,
    imported: Vec<ChatMessage>,
) -> Vec<ChatMessage> {
    native.extend(imported);
    native.sort_by(|left, right| left.timestamp.cmp(&right.timestamp));
    native
}

fn classify_import_file(path: &Path) -> Result<Option<ImportKind>> {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if name.ends_with(".json") || name.ends_with(".json.gz") {
        return Ok(Some(ImportKind::JsonExport));
    }
    if !(name.ends_with(".txt") || name.ends_with(".txt.gz")) {
        return Ok(None);
    }
    let lines = load_lines_from_supported_file(path)?;
    if behaves_like_raw_irc(&lines) {
        Ok(Some(ImportKind::RawIrc))
    } else {
        Ok(Some(ImportKind::SimpleText))
    }
}

fn behaves_like_raw_irc(lines: &[String]) -> bool {
    let mut saw_supported = false;
    for line in lines.iter().filter(|line| !line.trim().is_empty()) {
        match CanonicalEvent::from_raw(line) {
            Ok(Some(_)) => saw_supported = true,
            Ok(None) => {}
            Err(_) => return false,
        }
    }
    saw_supported
}

fn load_lines_from_supported_file(path: &Path) -> Result<Vec<String>> {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if name.ends_with(".gz") {
        let file = File::open(path)?;
        let decoder = GzDecoder::new(file);
        let reader = BufReader::new(decoder);
        return reader
            .lines()
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(Into::into);
    }
    load_lines_from_text_file(path)
}

fn load_string_from_supported_file(path: &Path) -> Result<String> {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let mut output = String::new();
    if name.ends_with(".gz") {
        let file = File::open(path)?;
        let mut decoder = GzDecoder::new(file);
        decoder.read_to_string(&mut output)?;
    } else {
        File::open(path)?.read_to_string(&mut output)?;
    }
    Ok(output)
}

fn parse_sparse_txt_file(
    path: &Path,
    channel_login: &str,
    year: i32,
    month: u32,
    day: u32,
) -> Result<Vec<ChatMessage>> {
    let lines = load_lines_from_supported_file(path)?;
    let base = Utc
        .with_ymd_and_hms(year, month, day, 0, 0, 0)
        .single()
        .ok_or_else(|| anyhow!("invalid legacy txt date"))?;
    let regex = Regex::new(r"^\[(?:(\d+):)?(\d{1,2}):(\d{2})\]\s+([^:]+):\s?(.*)$")?;
    let mut messages = Vec::new();
    for line in lines {
        let Some(captures) = regex.captures(&line) else {
            continue;
        };
        let hours = captures
            .get(1)
            .map(|value| value.as_str().parse::<i64>())
            .transpose()?
            .unwrap_or_default();
        let minutes = captures[2].parse::<i64>()?;
        let seconds = captures[3].parse::<i64>()?;
        let display_name = captures[4].trim().to_string();
        let username = normalize_username(&display_name);
        let text = captures[5].to_string();
        let timestamp = base
            + chrono::Duration::hours(hours)
            + chrono::Duration::minutes(minutes)
            + chrono::Duration::seconds(seconds);
        messages.push(ChatMessage {
            text: text.clone(),
            system_text: String::new(),
            username: username.clone(),
            display_name,
            channel: channel_login.to_string(),
            timestamp,
            id: fallback_message_id(timestamp, &username, &text),
            message_type: PRIVMSG_TYPE,
            raw: build_surrogate_raw(channel_login, &username, timestamp, &text),
            tags: HashMap::new(),
        });
    }
    Ok(messages)
}

fn parse_json_export_file(path: &Path, channel_login: &str) -> Result<Vec<ChatMessage>> {
    let content = load_string_from_supported_file(path)?;
    let export: JsonExport = serde_json::from_str(&content)?;
    let fallback_channel = export
        .streamer
        .as_ref()
        .map(|streamer| streamer.name.clone())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| channel_login.to_string());
    let video_id = export
        .video
        .as_ref()
        .map(|video| video.id.clone())
        .unwrap_or_default();
    let video_title = export
        .video
        .as_ref()
        .map(|video| video.title.clone())
        .unwrap_or_default();
    let streamer_name = export
        .streamer
        .as_ref()
        .map(|streamer| streamer.name.clone())
        .unwrap_or_default();
    let mut messages = Vec::new();
    for comment in export.comments {
        let Some(timestamp) = parse_rfc3339_utc(comment.created_at.as_deref()) else {
            continue;
        };
        let Some(message_body) = comment
            .message
            .as_ref()
            .and_then(|message| message.body.clone())
            .filter(|value| !value.is_empty())
        else {
            continue;
        };
        let display_name = comment
            .commenter
            .as_ref()
            .and_then(|commenter| commenter.display_name.clone())
            .unwrap_or_default();
        let username = comment
            .commenter
            .as_ref()
            .and_then(|commenter| commenter.name.clone())
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| normalize_username(&display_name));
        let mut tags = HashMap::new();
        if let Some(channel_id) = comment.channel_id.clone() {
            tags.insert("room-id".to_string(), channel_id);
        } else if let Some(streamer) = export.streamer.as_ref() {
            tags.insert("room-id".to_string(), streamer.id.to_string());
        }
        if let Some(commenter) = comment.commenter.as_ref() {
            if let Some(user_id) = commenter.id.clone() {
                tags.insert("user-id".to_string(), user_id);
            }
            if let Some(name) = commenter.display_name.clone() {
                tags.insert("display-name".to_string(), name);
            }
            if let Some(logo) = commenter.logo.clone() {
                tags.insert("commenter-logo".to_string(), logo);
            }
        }
        if let Some(message) = comment.message.as_ref() {
            if let Some(color) = message.user_color.clone() {
                tags.insert("color".to_string(), color);
            }
            if !message.user_badges.is_empty() {
                tags.insert(
                    "user-badges-json".to_string(),
                    serde_json::to_string(&message.user_badges)?,
                );
            }
            if !message.emoticons.is_empty() {
                tags.insert(
                    "emoticons-json".to_string(),
                    serde_json::to_string(&message.emoticons)?,
                );
            }
        }
        if !video_id.is_empty() {
            tags.insert("video-id".to_string(), video_id.clone());
        }
        if !video_title.is_empty() {
            tags.insert("video-title".to_string(), video_title.clone());
        }
        if !streamer_name.is_empty() {
            tags.insert("streamer-name".to_string(), streamer_name.clone());
        }
        if let Some(content_id) = comment.content_id.clone() {
            tags.insert("content-id".to_string(), content_id);
        }
        let id = comment
            .id
            .clone()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| fallback_message_id(timestamp, &username, &message_body));
        let channel = if channel_login.is_empty() {
            fallback_channel.clone()
        } else {
            channel_login.to_string()
        };
        messages.push(ChatMessage {
            text: message_body.clone(),
            system_text: String::new(),
            username: username.clone(),
            display_name,
            channel: channel.clone(),
            timestamp,
            id: id.clone(),
            message_type: PRIVMSG_TYPE,
            raw: build_surrogate_raw(&channel, &username, timestamp, &message_body),
            tags,
        });
    }
    Ok(messages)
}

fn parse_rfc3339_utc(input: Option<&str>) -> Option<DateTime<Utc>> {
    let input = input?;
    DateTime::parse_from_rfc3339(input)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn build_surrogate_raw(
    channel_login: &str,
    username: &str,
    timestamp: DateTime<Utc>,
    text: &str,
) -> String {
    let message_id = fallback_message_id(timestamp, username, text);
    format!(
        "@display-name={username};id={message_id};room-id=;tmi-sent-ts={};user-id= :{username}!{username}@{username}.tmi.twitch.tv PRIVMSG #{channel_login} :{text}",
        timestamp.timestamp_millis()
    )
}

fn fallback_message_id(timestamp: DateTime<Utc>, username: &str, text: &str) -> String {
    format!(
        "legacy:{}",
        blake3::hash(format!("{}:{username}:{text}", timestamp.timestamp_millis()).as_bytes())
            .to_hex()
    )
}

fn normalize_username(value: &str) -> String {
    let normalized = value
        .chars()
        .filter_map(|char| {
            let lower = char.to_ascii_lowercase();
            if lower == '_' || lower.is_ascii_alphanumeric() {
                Some(lower)
            } else {
                None
            }
        })
        .collect::<String>();
    if normalized.is_empty() {
        "unknown".to_string()
    } else {
        normalized
    }
}

fn file_fingerprint(path: &Path) -> Result<String> {
    let metadata = fs::metadata(path)?;
    let modified = metadata
        .modified()
        .ok()
        .and_then(|value| value.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|value| value.as_secs())
        .unwrap_or_default();
    Ok(format!("{}:{modified}", metadata.len()))
}

fn match_import_path(root: &Path, path: &Path) -> Option<(String, i32, u32, u32)> {
    let suffix = path
        .strip_prefix(root)
        .ok()?
        .components()
        .map(|component| component.as_os_str().to_string_lossy().to_string())
        .collect::<Vec<_>>();
    if suffix.len() < 4 {
        return None;
    }
    let len = suffix.len();
    let channel_id = suffix[len - 4].clone();
    let year = suffix[len - 3].parse::<i32>().ok()?;
    let month = suffix[len - 2].parse::<u32>().ok()?;
    let file_name = &suffix[len - 1];
    let day_stem = if let Some(stripped) = file_name.strip_suffix(".txt.gz") {
        stripped
    } else if let Some(stripped) = file_name.strip_suffix(".json.gz") {
        stripped
    } else if let Some(stripped) = file_name.strip_suffix(".txt") {
        stripped
    } else if let Some(stripped) = file_name.strip_suffix(".json") {
        stripped
    } else {
        return None;
    };
    let day = day_stem.parse::<u32>().ok()?;
    Some((channel_id, year, month, day))
}

fn env_flag(name: &str, default: bool) -> bool {
    match env::var(name) {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => default,
    }
}

fn prune_empty_dirs(root: &Path) {
    let mut directories = WalkDir::new(root)
        .min_depth(1)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_dir())
        .map(|entry| entry.into_path())
        .collect::<Vec<_>>();
    directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
    for directory in directories {
        remove_empty_dir_and_parents(root, &directory);
    }
}

fn remove_empty_dir_and_parents(root: &Path, start: &Path) {
    let mut current = start.to_path_buf();
    loop {
        if current == root {
            break;
        }
        let is_empty = fs::read_dir(&current)
            .map(|mut entries| entries.next().is_none())
            .unwrap_or(false);
        if !is_empty {
            break;
        }
        if fs::remove_dir(&current).is_err() {
            break;
        }
        let Some(parent) = current.parent() else {
            break;
        };
        if parent == root {
            break;
        }
        current = parent.to_path_buf();
    }
}

#[derive(Debug, Deserialize)]
struct JsonExport {
    #[serde(default)]
    streamer: Option<JsonStreamer>,
    #[serde(default)]
    video: Option<JsonVideo>,
    #[serde(default)]
    comments: Vec<JsonComment>,
}

#[derive(Debug, Deserialize)]
struct JsonStreamer {
    name: String,
    id: i64,
}

#[derive(Debug, Deserialize)]
struct JsonVideo {
    id: String,
    title: String,
}

#[derive(Debug, Deserialize)]
struct JsonComment {
    #[serde(rename = "_id")]
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    created_at: Option<String>,
    #[serde(default)]
    channel_id: Option<String>,
    #[serde(default)]
    content_id: Option<String>,
    #[serde(default)]
    commenter: Option<JsonCommenter>,
    #[serde(default)]
    message: Option<JsonCommentMessage>,
}

#[derive(Debug, Deserialize)]
struct JsonCommenter {
    #[serde(default)]
    display_name: Option<String>,
    #[serde(rename = "_id")]
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    logo: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JsonCommentMessage {
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    user_color: Option<String>,
    #[serde(default)]
    user_badges: Vec<serde_json::Value>,
    #[serde(default)]
    emoticons: Vec<serde_json::Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sparse_parser_builds_messages() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("2.txt");
        fs::write(
            &path,
            "[0:00:04] Foo-Bar!: hello\n[1:02:03] Another_User: hi",
        )
        .unwrap();
        let messages = parse_sparse_txt_file(&path, "channelone", 2024, 1, 2).unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].username, "foobar");
        assert_eq!(messages[1].username, "another_user");
    }

    #[test]
    fn recursive_lookup_and_prune_work() {
        let temp = tempfile::TempDir::new().unwrap();
        let root = temp.path().join("imports");
        fs::create_dir_all(root.join("nested/copy/1/2024/1")).unwrap();
        fs::create_dir_all(root.join("nested/empty/a/b")).unwrap();
        fs::write(
            root.join("nested/copy/1/2024/1/2.txt"),
            "[0:00:04] User: hello",
        )
        .unwrap();
        let runtime = LegacyTxtRuntime {
            import_folder: Some(root.clone()),
            check_each_request: true,
            mode: LegacyTxtMode::MissingOnly,
            import_folder_exists_at_startup: true,
        };
        let files = runtime.discover_channel_day_files("1", 2024, 1, 2).unwrap();
        assert_eq!(files.len(), 1);
        assert!(!root.join("nested/empty/a/b").exists());
    }
}
