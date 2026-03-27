use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, TimeZone, Utc};
use regex::Regex;
use walkdir::WalkDir;

use crate::model::{ChannelLogFile, ChatMessage, PRIVMSG_TYPE};
use crate::store::load_lines_from_text_file;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyTxtMode {
    Off,
    MissingOnly,
    Merge,
}

#[derive(Debug, Clone)]
pub struct LegacyTxtRuntime {
    enabled: bool,
    check_each_request: bool,
    root: PathBuf,
    root_exists_at_startup: bool,
    mode: LegacyTxtMode,
}

impl LegacyTxtRuntime {
    pub fn from_env(default_logs_directory: &Path) -> Self {
        let enabled = env_flag("JUSTLOG_LEGACY_TXT_ENABLED", false);
        let check_each_request = env_flag("JUSTLOG_LEGACY_TXT_CHECK_EACH_REQUEST", false);
        let root = env::var("JUSTLOG_LEGACY_TXT_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|_| default_logs_directory.join("legacy-txt"));
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
        let root_exists_at_startup = root.exists();
        Self {
            enabled,
            check_each_request,
            root,
            root_exists_at_startup,
            mode,
        }
    }

    pub fn mode(&self) -> LegacyTxtMode {
        if !self.enabled {
            LegacyTxtMode::Off
        } else {
            self.mode
        }
    }

    pub fn is_request_enabled(&self) -> bool {
        self.mode() != LegacyTxtMode::Off && self.root_is_available()
    }

    pub fn should_check_each_request(&self) -> bool {
        self.check_each_request
    }

    pub fn root_is_available(&self) -> bool {
        if !self.enabled {
            return false;
        }
        if self.check_each_request {
            self.prepare_root()
        } else {
            self.root_exists_at_startup
        }
    }

    pub fn channel_day_paths(
        &self,
        channel_id: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Vec<PathBuf> {
        if !self.prepare_root() {
            return Vec::new();
        }
        let mut matches = WalkDir::new(&self.root)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_file())
            .filter_map(|entry| {
                let path = entry.into_path();
                if !path
                    .extension()
                    .and_then(|value| value.to_str())
                    .is_some_and(|value| value.eq_ignore_ascii_case("txt"))
                {
                    return None;
                }
                let suffix = path
                    .strip_prefix(&self.root)
                    .ok()?
                    .components()
                    .map(|component| component.as_os_str().to_string_lossy().to_string())
                    .collect::<Vec<_>>();
                if suffix.len() < 4 {
                    return None;
                }
                let len = suffix.len();
                let file_name = &suffix[len - 1];
                let month_name = &suffix[len - 2];
                let year_name = &suffix[len - 3];
                let channel_name = &suffix[len - 4];
                let file_stem = Path::new(file_name)
                    .file_stem()
                    .and_then(|value| value.to_str())?;
                if channel_name != channel_id
                    || year_name != &year.to_string()
                    || month_name != &month.to_string()
                    || file_stem != day.to_string()
                {
                    return None;
                }
                Some(path)
            })
            .collect::<Vec<_>>();
        matches.sort();
        matches
    }

    pub fn available_channel_logs(&self, channel_id: &str) -> Vec<ChannelLogFile> {
        let Some(channel_root) = self.channel_root() else {
            return Vec::new();
        };
        let mut logs = Vec::new();
        for entry in WalkDir::new(channel_root)
            .into_iter()
            .filter_map(Result::ok)
        {
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry.into_path();
            if !path
                .extension()
                .and_then(|value| value.to_str())
                .is_some_and(|value| value.eq_ignore_ascii_case("txt"))
            {
                continue;
            }
            let suffix = match path.strip_prefix(&self.root) {
                Ok(suffix) => suffix,
                Err(_) => continue,
            };
            let components = suffix
                .components()
                .map(|component| component.as_os_str().to_string_lossy().to_string())
                .collect::<Vec<_>>();
            if components.len() < 4 {
                continue;
            }
            let len = components.len();
            if components[len - 4] != channel_id {
                continue;
            }
            let Ok(year) = components[len - 3].parse::<i32>() else {
                continue;
            };
            let Ok(month) = components[len - 2].parse::<u32>() else {
                continue;
            };
            let Some(stem) = Path::new(&components[len - 1])
                .file_stem()
                .and_then(|value| value.to_str())
            else {
                continue;
            };
            let Ok(day) = stem.parse::<u32>() else {
                continue;
            };
            logs.push(ChannelLogFile {
                year: year.to_string(),
                month: month.to_string(),
                day: day.to_string(),
            });
        }
        logs.sort_by(|left, right| {
            right
                .year
                .cmp(&left.year)
                .then_with(|| right.month.cmp(&left.month))
                .then_with(|| right.day.cmp(&left.day))
        });
        logs.dedup();
        logs
    }

    pub fn load_channel_day_messages(
        &self,
        channel_id: &str,
        channel_login: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Option<Vec<ChatMessage>> {
        let paths = self.channel_day_paths(channel_id, year, month, day);
        if paths.is_empty() {
            return None;
        }
        let mut messages = Vec::new();
        for path in paths {
            if let Ok(parsed) = parse_sparse_txt_file(&path, channel_login, year, month, day) {
                messages.extend(parsed);
            }
        }
        if messages.is_empty() {
            return None;
        }
        messages.sort_by(|left, right| left.timestamp.cmp(&right.timestamp));
        Some(messages)
    }

    fn channel_root(&self) -> Option<PathBuf> {
        if !self.prepare_root() {
            return None;
        }
        Some(self.root.clone())
    }

    fn prepare_root(&self) -> bool {
        if !self.root.exists() {
            return false;
        }
        self.prune_empty_dirs();
        self.root.exists()
    }

    fn prune_empty_dirs(&self) {
        let mut directories = WalkDir::new(&self.root)
            .min_depth(1)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_dir())
            .map(|entry| entry.into_path())
            .collect::<Vec<_>>();
        directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
        for directory in directories {
            remove_empty_dir_and_parents(&self.root, &directory);
        }
    }
}

pub fn merge_messages(mut native: Vec<ChatMessage>, legacy: Vec<ChatMessage>) -> Vec<ChatMessage> {
    native.extend(legacy);
    native.sort_by(|left, right| left.timestamp.cmp(&right.timestamp));
    native
}

fn parse_sparse_txt_file(
    path: &Path,
    channel_login: &str,
    year: i32,
    month: u32,
    day: u32,
) -> anyhow::Result<Vec<ChatMessage>> {
    let lines = load_lines_from_text_file(path)?;
    let base = Utc
        .with_ymd_and_hms(year, month, day, 0, 0, 0)
        .single()
        .ok_or_else(|| anyhow::anyhow!("invalid legacy txt date"))?;
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

fn env_flag(name: &str, default: bool) -> bool {
    match env::var(name) {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => default,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_builds_messages_from_sparse_txt() {
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
        assert_eq!(messages[0].text, "hello");
        assert_eq!(messages[0].timestamp.timestamp(), 1_704_153_604);
        assert_eq!(messages[1].username, "another_user");
    }

    #[test]
    fn recursive_lookup_and_empty_dir_prune_work() {
        let temp = tempfile::TempDir::new().unwrap();
        let root = temp.path().join("legacy");
        fs::create_dir_all(root.join("nested/copy/1/2024/1")).unwrap();
        fs::create_dir_all(root.join("nested/empty/a/b")).unwrap();
        fs::write(
            root.join("nested/copy/1/2024/1/2.txt"),
            "[0:00:04] User: hello",
        )
        .unwrap();

        let runtime = LegacyTxtRuntime {
            enabled: true,
            check_each_request: true,
            root: root.clone(),
            root_exists_at_startup: true,
            mode: LegacyTxtMode::MissingOnly,
        };

        let paths = runtime.channel_day_paths("1", 2024, 1, 2);

        assert_eq!(paths.len(), 1);
        assert!(!root.join("nested/empty/a/b").exists());
    }
}
