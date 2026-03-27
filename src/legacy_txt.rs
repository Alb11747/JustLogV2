use std::collections::{BTreeSet, HashMap, HashSet};
use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Result, anyhow};
use chrono::{DateTime, Datelike, TimeZone, Utc};
use flate2::read::GzDecoder;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};
use walkdir::WalkDir;

use crate::model::{
    CanonicalEvent, ChannelDayKey, ChannelLogFile, ChatMessage, PRIVMSG_TYPE, UserMonthKey,
};
use crate::store::Store;

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
    delete_raw_after_import: bool,
    delete_current_raw_on_discovery: bool,
    delete_reconstructed_after_read: bool,
    delete_current_reconstructed_on_discovery: bool,
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
}

#[derive(Debug, Clone, Default)]
pub struct ChannelDayImport {
    pub complete_messages: Vec<ChatMessage>,
    pub simple_messages: Vec<ChatMessage>,
}

#[derive(Debug, Clone, Default)]
pub struct RawImportOutcome {
    pub affected_channel_days: BTreeSet<ChannelDayKey>,
    pub affected_user_months: BTreeSet<UserMonthKey>,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct BulkRawImportSummary {
    pub files_scanned: usize,
    pub raw_candidates: usize,
    pub files_selected: usize,
    pub files_current: usize,
    pub files_pending: usize,
    pub files_imported: usize,
    pub files_failed: usize,
    pub files_deleted: usize,
    pub affected_channel_days: usize,
    pub affected_user_months: usize,
    pub elapsed_ms: u128,
    pub dry_run: bool,
    pub channel_id: Option<String>,
}

impl RawImportOutcome {
    fn record_event(&mut self, event: &CanonicalEvent) {
        self.affected_channel_days.insert(event.channel_day_key());
        for key in event.user_month_keys() {
            self.affected_user_months.insert(key);
        }
    }

    fn extend(&mut self, other: Self) {
        self.affected_channel_days
            .extend(other.affected_channel_days);
        self.affected_user_months.extend(other.affected_user_months);
    }
}

const IMPORT_PROGRESS_LINE_INTERVAL: usize = 100_000;

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
            delete_raw_after_import: env_flag("JUSTLOG_IMPORT_DELETE_RAW", false),
            delete_current_raw_on_discovery: env_flag(
                "JUSTLOG_IMPORT_DELETE_ALREADY_IMPORTED_RAW",
                true,
            ),
            delete_reconstructed_after_read: env_flag("JUSTLOG_IMPORT_DELETE_RECONSTRUCTED", false),
            delete_current_reconstructed_on_discovery: env_flag(
                "JUSTLOG_IMPORT_DELETE_ALREADY_IMPORTED_RECONSTRUCTED",
                false,
            ),
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
    ) -> Result<RawImportOutcome> {
        let files = self.discover_import_files()?;
        let (raw_count, simple_count, json_count) = import_kind_counts(&files);
        info!(
            "Import-folder scan for channel-day {channel_id}/{year}/{month}/{day}: {} candidate file(s), raw={}, simple={}, json={}",
            files.len(),
            raw_count,
            simple_count,
            json_count
        );
        self.import_raw_files(
            store,
            files,
            &format!("channel-day {channel_id}/{year}/{month}/{day}"),
        )
    }

    pub fn import_raw_channel(&self, store: &Store, channel_id: &str) -> Result<RawImportOutcome> {
        let files = self.discover_import_files()?;
        let (raw_count, simple_count, json_count) = import_kind_counts(&files);
        info!(
            "Import-folder scan for channel {channel_id}: {} candidate file(s), raw={}, simple={}, json={}",
            files.len(),
            raw_count,
            simple_count,
            json_count
        );
        self.import_raw_files(store, files, &format!("channel {channel_id}"))
    }

    pub fn load_channel_day_import(
        &self,
        store: &Store,
        channel_id: &str,
        channel_login: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<ChannelDayImport> {
        let mut result = ChannelDayImport::default();
        let files = self.discover_import_files()?;
        let (raw_count, simple_count, json_count) = import_kind_counts(&files);
        let mut matched_simple_files = 0usize;
        let mut matched_json_files = 0usize;
        for file in files {
            match file.kind {
                ImportKind::RawIrc => {}
                ImportKind::SimpleText => {
                    if self.mode == LegacyTxtMode::Off {
                        continue;
                    }
                    if !simple_text_file_matches_request(
                        &file.path,
                        channel_id,
                        channel_login,
                        year,
                        month,
                        day,
                    ) {
                        continue;
                    }
                    if self.handle_already_consumed_reconstructed_file(store, &file)? {
                        continue;
                    }
                    if let Ok(messages) =
                        parse_sparse_txt_file(&file.path, channel_login, year, month, day)
                    {
                        if !messages.is_empty() {
                            matched_simple_files += 1;
                            result.simple_messages.extend(messages);
                            self.record_consumed_reconstructed_file(store, &file)?;
                            self.delete_import_file_if_configured(&file, false);
                        }
                    }
                }
                ImportKind::JsonExport => {
                    if self.handle_already_consumed_reconstructed_file(store, &file)? {
                        continue;
                    }
                    if let Ok(messages) = parse_json_export_file(&file.path, channel_login) {
                        let matching = messages
                            .into_iter()
                            .filter(|message| {
                                chat_message_matches_channel_day(
                                    message, channel_id, year, month, day,
                                )
                            })
                            .collect::<Vec<_>>();
                        if !matching.is_empty() {
                            matched_json_files += 1;
                            result.complete_messages.extend(matching);
                            self.record_consumed_reconstructed_file(store, &file)?;
                            self.delete_import_file_if_configured(&file, false);
                        }
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
        info!(
            "Overlay scan for channel-day {channel_id}/{year}/{month}/{day}: candidate file(s) raw={}, simple={}, json={}; matched file(s) simple={}, json={}; message(s) simple={}, complete={}",
            raw_count,
            simple_count,
            json_count,
            matched_simple_files,
            matched_json_files,
            result.simple_messages.len(),
            result.complete_messages.len()
        );
        Ok(result)
    }

    pub fn available_channel_logs(
        &self,
        store: &Store,
        channel_id: &str,
    ) -> Result<Vec<ChannelLogFile>> {
        let mut logs = Vec::new();
        let mut seen = HashSet::new();
        let files = self.discover_import_files()?;
        let (raw_count, simple_count, json_count) = import_kind_counts(&files);
        for file in files {
            match file.kind {
                ImportKind::RawIrc => {}
                ImportKind::SimpleText => {
                    if self.mode == LegacyTxtMode::Off {
                        continue;
                    }
                    if self.handle_already_consumed_reconstructed_file(store, &file)? {
                        continue;
                    }
                    if let Some((year, month, day)) =
                        infer_simple_text_channel_day(&file.path, channel_id, None)
                    {
                        let key = (year, month, day);
                        if seen.insert(key) {
                            logs.push(ChannelLogFile {
                                year: year.to_string(),
                                month: month.to_string(),
                                day: day.to_string(),
                            });
                        }
                        self.record_consumed_reconstructed_file(store, &file)?;
                    }
                }
                ImportKind::JsonExport => {
                    if self.handle_already_consumed_reconstructed_file(store, &file)? {
                        continue;
                    }
                    if let Ok(messages) = parse_json_export_file(&file.path, "") {
                        let mut file_contributed = false;
                        for message in messages {
                            let room_id = message.tags.get("room-id").cloned().unwrap_or_default();
                            if room_id != channel_id {
                                continue;
                            }
                            let key = (
                                message.timestamp.year(),
                                message.timestamp.month(),
                                message.timestamp.day(),
                            );
                            if seen.insert(key) {
                                logs.push(ChannelLogFile {
                                    year: key.0.to_string(),
                                    month: key.1.to_string(),
                                    day: key.2.to_string(),
                                });
                            }
                            file_contributed = true;
                        }
                        if file_contributed {
                            self.record_consumed_reconstructed_file(store, &file)?;
                        }
                    }
                }
            }
        }
        logs.sort_by(|left, right| {
            right
                .year
                .cmp(&left.year)
                .then_with(|| right.month.cmp(&left.month))
                .then_with(|| right.day.cmp(&left.day))
        });
        info!(
            "Import-folder availability scan for channel {channel_id}: candidate file(s) raw={}, simple={}, json={}; discovered day(s)={}",
            raw_count,
            simple_count,
            json_count,
            logs.len()
        );
        Ok(logs)
    }

    pub fn bulk_import_raw(
        &self,
        store: &Store,
        channel_id: Option<&str>,
        limit_files: Option<usize>,
        dry_run: bool,
    ) -> Result<BulkRawImportSummary> {
        let started = Instant::now();
        let files = self.discover_import_files()?;
        let raw_candidates = files
            .iter()
            .filter(|file| file.kind == ImportKind::RawIrc)
            .count();
        let mut selected = files
            .into_iter()
            .filter(|file| file.kind == ImportKind::RawIrc)
            .collect::<Vec<_>>();
        if let Some(channel_id) = channel_id {
            selected.retain(|file| raw_file_matches_channel(file, channel_id));
        }
        if let Some(limit) = limit_files {
            selected.truncate(limit);
        }

        let mut summary = BulkRawImportSummary {
            files_scanned: raw_candidates,
            raw_candidates,
            files_selected: selected.len(),
            dry_run,
            channel_id: channel_id.map(str::to_string),
            ..BulkRawImportSummary::default()
        };

        info!(
            "Starting bulk raw import: channel_filter={:?}, dry_run={}, selected_files={}",
            channel_id,
            dry_run,
            selected.len()
        );

        let mut outcome = RawImportOutcome::default();
        let total_files = selected.len();
        for (index, file) in selected.into_iter().enumerate() {
            let path_key = file.path.to_string_lossy().to_string();
            let status_check_started = Instant::now();
            info!(
                "Bulk import status check {}/{} for {}",
                index + 1,
                total_files,
                file.path.display()
            );
            let is_current = store.imported_raw_file_is_current(&path_key, &file.fingerprint)?;
            info!(
                "Completed bulk import status check for {} in {:?}: current={is_current}",
                file.path.display(),
                status_check_started.elapsed()
            );
            if is_current {
                summary.files_current += 1;
                if !dry_run && self.delete_current_raw_if_configured(&file) {
                    summary.files_deleted += 1;
                }
                continue;
            }

            summary.files_pending += 1;
            if dry_run {
                continue;
            }

            match import_raw_file_with_progress(store, &file, index + 1, total_files) {
                Ok(file_outcome) => {
                    summary.files_imported += 1;
                    outcome.extend(file_outcome);
                    if self.delete_import_file_if_configured(&file, true) {
                        summary.files_deleted += 1;
                    }
                }
                Err(error) => {
                    let failed_status = format!("failed:{error}");
                    let _ = store.record_imported_raw_file(
                        &path_key,
                        &file.fingerprint,
                        &failed_status,
                    );
                    summary.files_failed += 1;
                    warn!(
                        "Failed bulk raw import for {}: {error:#}",
                        file.path.display()
                    );
                }
            }
        }

        if !dry_run && !outcome.affected_channel_days.is_empty() {
            let day_keys = outcome
                .affected_channel_days
                .iter()
                .cloned()
                .collect::<Vec<_>>();
            store.merge_imported_channel_days_into_archives(&day_keys)?;
        }
        summary.affected_channel_days = outcome.affected_channel_days.len();
        summary.affected_user_months = outcome.affected_user_months.len();
        summary.elapsed_ms = started.elapsed().as_millis();
        info!(
            "Completed bulk raw import: scanned={}, selected={}, current={}, pending={}, imported={}, failed={}, deleted={}, channel_days={}, user_months={}, elapsed_ms={}",
            summary.files_scanned,
            summary.files_selected,
            summary.files_current,
            summary.files_pending,
            summary.files_imported,
            summary.files_failed,
            summary.files_deleted,
            summary.affected_channel_days,
            summary.affected_user_months,
            summary.elapsed_ms
        );
        Ok(summary)
    }

    fn discover_import_files(&self) -> Result<Vec<ImportFile>> {
        let Some(root) = self.import_folder_path() else {
            return Ok(Vec::new());
        };
        let scan_started = Instant::now();
        let mut files = Vec::new();
        for entry in WalkDir::new(&root).into_iter().filter_map(Result::ok) {
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry.into_path();
            if !is_supported_import_path(&path) {
                continue;
            }
            let kind = match classify_import_file(&path) {
                Ok(kind) => kind,
                Err(error) => {
                    warn!("Skipping import candidate {}: {error:#}", path.display());
                    None
                }
            };
            if let Some(kind) = kind {
                files.push(ImportFile {
                    path: path.clone(),
                    fingerprint: file_fingerprint(&path)?,
                    kind,
                });
            }
        }
        files.sort_by(|left, right| left.path.cmp(&right.path));
        info!(
            "Import-folder discovery completed for {}: {} supported file(s) in {:?}",
            root.display(),
            files.len(),
            scan_started.elapsed()
        );
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

    fn import_raw_files(
        &self,
        store: &Store,
        files: Vec<ImportFile>,
        scope: &str,
    ) -> Result<RawImportOutcome> {
        info!("Preparing raw import candidates for {scope}");
        let pending = files
            .into_iter()
            .filter(|file| file.kind == ImportKind::RawIrc)
            .collect::<Vec<_>>();
        if pending.is_empty() {
            info!("Raw import scan for {scope}: no raw candidate files found");
            return Ok(RawImportOutcome::default());
        }

        let mut remaining = Vec::new();
        let mut total_bytes = 0u64;
        for file in pending {
            let path_key = file.path.to_string_lossy().to_string();
            let status_check_started = Instant::now();
            info!("Checking raw import status for {}", file.path.display());
            let is_current = store.imported_raw_file_is_current(&path_key, &file.fingerprint)?;
            info!(
                "Completed raw import status check for {} in {:?}: current={is_current}",
                file.path.display(),
                status_check_started.elapsed()
            );
            if is_current {
                self.delete_current_raw_if_configured(&file);
                continue;
            }
            let metadata_started = Instant::now();
            let file_size = fs::metadata(&file.path)
                .map(|metadata| metadata.len())
                .unwrap_or(0);
            info!(
                "Read raw import metadata for {} in {:?}: {} bytes",
                file.path.display(),
                metadata_started.elapsed(),
                file_size
            );
            total_bytes += file_size;
            remaining.push(file);
        }
        if remaining.is_empty() {
            info!("Raw import scan for {scope}: all raw candidate files already current");
            return Ok(RawImportOutcome::default());
        }

        info!(
            "Starting raw import scan for {scope}: {} file(s), {:.2} MiB pending",
            remaining.len(),
            total_bytes as f64 / (1024.0 * 1024.0)
        );

        let total_files = remaining.len();
        let mut outcome = RawImportOutcome::default();
        for (index, file) in remaining.into_iter().enumerate() {
            match import_raw_file_with_progress(store, &file, index + 1, total_files) {
                Ok(file_outcome) => outcome.extend(file_outcome),
                Err(error) => {
                    let path_key = file.path.to_string_lossy().to_string();
                    let failed_status = format!("failed:{error}");
                    let _ = store.record_imported_raw_file(
                        &path_key,
                        &file.fingerprint,
                        &failed_status,
                    );
                    warn!("Failed raw import for {}: {error:#}", file.path.display());
                    continue;
                }
            }
            self.delete_import_file_if_configured(&file, true);
        }
        Ok(outcome)
    }

    fn delete_import_file_if_configured(&self, file: &ImportFile, is_raw: bool) -> bool {
        let should_delete = if is_raw {
            self.delete_raw_after_import
        } else {
            self.delete_reconstructed_after_read
        };
        if !should_delete {
            return false;
        }
        if fs::remove_file(&file.path).is_ok() {
            if let Some(root) = self.import_folder.as_deref() {
                if let Some(parent) = file.path.parent() {
                    remove_empty_dir_and_parents(root, parent);
                }
            }
            info!(
                "Deleted consumed {} import file {}",
                if is_raw { "raw" } else { "reconstructed" },
                file.path.display()
            );
            true
        } else {
            warn!(
                "Failed to delete consumed {} import file {}",
                if is_raw { "raw" } else { "reconstructed" },
                file.path.display()
            );
            false
        }
    }

    fn delete_current_raw_if_configured(&self, file: &ImportFile) -> bool {
        if !self.delete_current_raw_on_discovery {
            return false;
        }
        if fs::remove_file(&file.path).is_ok() {
            if let Some(root) = self.import_folder.as_deref() {
                if let Some(parent) = file.path.parent() {
                    remove_empty_dir_and_parents(root, parent);
                }
            }
            info!("Deleted already-imported raw file {}", file.path.display());
            true
        } else {
            warn!(
                "Failed to delete already-imported raw file {}",
                file.path.display()
            );
            false
        }
    }

    fn handle_already_consumed_reconstructed_file(
        &self,
        store: &Store,
        file: &ImportFile,
    ) -> Result<bool> {
        let path_key = file.path.to_string_lossy().to_string();
        let status_check_started = Instant::now();
        info!(
            "Checking reconstructed import status for {}",
            file.path.display()
        );
        let is_current =
            store.imported_reconstructed_file_is_current(&path_key, &file.fingerprint)?;
        info!(
            "Completed reconstructed import status check for {} in {:?}: current={is_current}",
            file.path.display(),
            status_check_started.elapsed()
        );
        if is_current {
            self.delete_current_reconstructed_if_configured(file);
            return Ok(true);
        }
        Ok(false)
    }

    fn record_consumed_reconstructed_file(&self, store: &Store, file: &ImportFile) -> Result<()> {
        let path_key = file.path.to_string_lossy().to_string();
        let record_started = Instant::now();
        info!(
            "Recording reconstructed import completion for {}",
            file.path.display()
        );
        store.record_imported_reconstructed_file(&path_key, &file.fingerprint, "consumed")?;
        info!(
            "Recorded reconstructed import completion for {} in {:?}",
            file.path.display(),
            record_started.elapsed()
        );
        Ok(())
    }

    fn delete_current_reconstructed_if_configured(&self, file: &ImportFile) {
        if !self.delete_current_reconstructed_on_discovery {
            return;
        }
        if fs::remove_file(&file.path).is_ok() {
            if let Some(root) = self.import_folder.as_deref() {
                if let Some(parent) = file.path.parent() {
                    remove_empty_dir_and_parents(root, parent);
                }
            }
            info!(
                "Deleted already-consumed reconstructed file {}",
                file.path.display()
            );
        } else {
            warn!(
                "Failed to delete already-consumed reconstructed file {}",
                file.path.display()
            );
        }
    }
}

fn import_kind_counts(files: &[ImportFile]) -> (usize, usize, usize) {
    let mut raw = 0usize;
    let mut simple = 0usize;
    let mut json = 0usize;
    for file in files {
        match file.kind {
            ImportKind::RawIrc => raw += 1,
            ImportKind::SimpleText => simple += 1,
            ImportKind::JsonExport => json += 1,
        }
    }
    (raw, simple, json)
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
    if !(name.ends_with(".txt")
        || name.ends_with(".txt.gz")
        || name.ends_with(".log")
        || name.ends_with(".log.gz"))
    {
        return Ok(None);
    }
    if behaves_like_raw_irc_path(path)? {
        Ok(Some(ImportKind::RawIrc))
    } else {
        Ok(Some(ImportKind::SimpleText))
    }
}

fn raw_file_matches_channel(file: &ImportFile, channel_id: &str) -> bool {
    if let Some((matched_channel_id, _, _, _)) = infer_legacy_path_channel_day(&file.path) {
        if matched_channel_id == channel_id {
            return true;
        }
    }
    let mut matched = false;
    let _ = for_each_line_in_supported_file(&file.path, |line| {
        let Some(raw) = extract_raw_irc_line(&line) else {
            return Ok(());
        };
        if let Ok(Some(event)) = CanonicalEvent::from_raw(&raw) {
            if event.room_id == channel_id {
                matched = true;
            }
        }
        Ok(())
    });
    matched
}

fn behaves_like_raw_irc_path(path: &Path) -> Result<bool> {
    let mut saw_supported = false;
    let mut saw_raw_line = false;
    for_each_line_in_supported_file(path, |line| {
        let Some(raw) = extract_raw_irc_line(&line) else {
            return Ok(());
        };
        saw_raw_line = true;
        if let Ok(Some(_)) = CanonicalEvent::from_raw(&raw) {
            saw_supported = true;
        }
        Ok(())
    })?;
    Ok(saw_raw_line && saw_supported)
}

#[cfg(test)]
fn behaves_like_raw_irc(lines: &[String]) -> bool {
    let raw_lines = extract_raw_irc_lines(lines);
    let mut saw_supported = false;
    for line in &raw_lines {
        match CanonicalEvent::from_raw(line) {
            Ok(Some(_)) => saw_supported = true,
            Ok(None) => {}
            Err(_) => return false,
        }
    }
    saw_supported && !raw_lines.is_empty()
}

#[cfg(test)]
fn extract_raw_irc_lines(lines: &[String]) -> Vec<String> {
    lines
        .iter()
        .filter_map(|line| extract_raw_irc_line(line))
        .collect()
}

fn extract_raw_irc_line(line: &str) -> Option<String> {
    let trimmed = line.trim();
    if trimmed.starts_with('@') || trimmed.starts_with(':') {
        return Some(trimmed.to_string());
    }
    let (_, raw) = trimmed.split_once("FROM SERVER: ")?;
    let raw = raw.trim();
    if raw.starts_with('@') || raw.starts_with(':') {
        Some(raw.to_string())
    } else {
        None
    }
}

fn load_lines_from_supported_file(path: &Path) -> Result<Vec<String>> {
    let mut lines = Vec::new();
    for_each_line_in_supported_file(path, |line| {
        lines.push(line);
        Ok(())
    })?;
    Ok(lines)
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

fn for_each_line_in_supported_file<F>(path: &Path, mut f: F) -> Result<()>
where
    F: FnMut(String) -> Result<()>,
{
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if name.ends_with(".gz") {
        let file = File::open(path)?;
        let decoder = GzDecoder::new(file);
        let reader = BufReader::new(decoder);
        for line in reader.lines() {
            f(line?)?;
        }
        return Ok(());
    }
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    for line in reader.lines() {
        f(line?)?;
    }
    Ok(())
}

fn import_raw_file_with_progress(
    store: &Store,
    file: &ImportFile,
    index: usize,
    total_files: usize,
) -> Result<RawImportOutcome> {
    let path_key = file.path.to_string_lossy().to_string();
    let import_marker_started = Instant::now();
    info!("Recording raw import start for {}", file.path.display());
    store.record_imported_raw_file(&path_key, &file.fingerprint, "importing")?;
    info!(
        "Recorded raw import start for {} in {:?}",
        file.path.display(),
        import_marker_started.elapsed()
    );

    let file_size = fs::metadata(&file.path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    info!(
        "Importing raw file {index}/{total_files}: {} ({:.2} MiB)",
        file.path.display(),
        file_size as f64 / (1024.0 * 1024.0)
    );

    let mut scanned_lines = 0usize;
    let mut candidate_lines = 0usize;
    let mut imported = 0usize;
    let mut parse_errors = 0usize;
    let mut skipped_events = 0usize;
    let mut outcome = RawImportOutcome::default();
    for_each_line_in_supported_file(&file.path, |line| {
        scanned_lines += 1;
        let Some(raw) = extract_raw_irc_line(&line) else {
            if scanned_lines % IMPORT_PROGRESS_LINE_INTERVAL == 0 {
                let status = format!("importing:{scanned_lines}:{imported}:{parse_errors}");
                let _ = store.record_imported_raw_file(&path_key, &file.fingerprint, &status);
                info!(
                    "Raw import progress {}: scanned {} lines, imported {}, parse_errors {}",
                    file.path.display(),
                    scanned_lines,
                    imported,
                    parse_errors
                );
            }
            return Ok(());
        };

        candidate_lines += 1;
        match CanonicalEvent::from_raw(&raw) {
            Ok(Some(event)) => {
                if store.insert_event(&event)? {
                    imported += 1;
                    outcome.record_event(&event);
                } else {
                    skipped_events += 1;
                }
            }
            Ok(None) => {
                skipped_events += 1;
            }
            Err(_) => {
                parse_errors += 1;
            }
        }

        if scanned_lines % IMPORT_PROGRESS_LINE_INTERVAL == 0 {
            let status = format!("importing:{scanned_lines}:{imported}:{parse_errors}");
            let _ = store.record_imported_raw_file(&path_key, &file.fingerprint, &status);
            info!(
                "Raw import progress {}: scanned {} lines, imported {}, parse_errors {}",
                file.path.display(),
                scanned_lines,
                imported,
                parse_errors
            );
        }
        Ok(())
    })?;

    let status = if imported > 0 { "imported" } else { "seen" };
    let completion_marker_started = Instant::now();
    info!(
        "Recording raw import completion for {} with status {status}",
        file.path.display()
    );
    store.record_imported_raw_file(&path_key, &file.fingerprint, status)?;
    info!(
        "Recorded raw import completion for {} in {:?}",
        file.path.display(),
        completion_marker_started.elapsed()
    );
    info!(
        "Completed raw import {}: scanned {} lines, raw_candidates {}, imported {}, skipped {}, parse_errors {}",
        file.path.display(),
        scanned_lines,
        candidate_lines,
        imported,
        skipped_events,
        parse_errors
    );
    Ok(outcome)
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

fn is_supported_import_path(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    name.ends_with(".txt")
        || name.ends_with(".txt.gz")
        || name.ends_with(".json")
        || name.ends_with(".json.gz")
        || name.ends_with(".log")
        || name.ends_with(".log.gz")
}

fn infer_filename_channel_day(path: &Path, channel_login: Option<&str>) -> Option<(i32, u32, u32)> {
    let file_name = path.file_name()?.to_string_lossy();
    let regex = Regex::new(r"\[(\d{1,2})-(\d{1,2})-(\d{2,4})\]").ok()?;
    let captures = regex.captures(&file_name)?;
    let month = captures.get(1)?.as_str().parse::<u32>().ok()?;
    let day = captures.get(2)?.as_str().parse::<u32>().ok()?;
    let mut year = captures.get(3)?.as_str().parse::<i32>().ok()?;
    if year < 100 {
        year += 2000;
    }
    if let Some(channel_login) = channel_login {
        let channel_login = channel_login.trim().to_ascii_lowercase();
        if !channel_login.is_empty() && !file_name.to_ascii_lowercase().contains(&channel_login) {
            return None;
        }
    }
    Some((year, month, day))
}

fn infer_legacy_path_channel_day(path: &Path) -> Option<(String, i32, u32, u32)> {
    let components = path
        .components()
        .map(|component| component.as_os_str().to_string_lossy().to_string())
        .collect::<Vec<_>>();
    if components.len() < 4 {
        return None;
    }
    let len = components.len();
    let channel_id = components[len - 4].clone();
    let year = components[len - 3].parse::<i32>().ok()?;
    let month = components[len - 2].parse::<u32>().ok()?;
    let file_name = &components[len - 1];
    let stem = file_name
        .strip_suffix(".txt.gz")
        .or_else(|| file_name.strip_suffix(".log.gz"))
        .or_else(|| file_name.strip_suffix(".json.gz"))
        .or_else(|| file_name.strip_suffix(".txt"))
        .or_else(|| file_name.strip_suffix(".log"))
        .or_else(|| file_name.strip_suffix(".json"))?;
    let day = stem.parse::<u32>().ok()?;
    Some((channel_id, year, month, day))
}

fn infer_simple_text_channel_day(
    path: &Path,
    channel_id: &str,
    channel_login: Option<&str>,
) -> Option<(i32, u32, u32)> {
    if let Some((matched_channel_id, year, month, day)) = infer_legacy_path_channel_day(path) {
        if !channel_id.is_empty() && matched_channel_id == channel_id {
            return Some((year, month, day));
        }
    }
    infer_filename_channel_day(path, channel_login)
}

fn simple_text_file_matches_request(
    path: &Path,
    channel_id: &str,
    channel_login: &str,
    year: i32,
    month: u32,
    day: u32,
) -> bool {
    matches!(
        infer_simple_text_channel_day(
            path,
            channel_id,
            if channel_login.trim().is_empty() {
                None
            } else {
                Some(channel_login)
            }
        ),
        Some((matched_year, matched_month, matched_day))
            if matched_year == year && matched_month == month && matched_day == day
    )
}

fn chat_message_matches_channel_day(
    message: &ChatMessage,
    channel_id: &str,
    year: i32,
    month: u32,
    day: u32,
) -> bool {
    let room_id = message.tags.get("room-id").cloned().unwrap_or_default();
    room_id == channel_id
        && message.timestamp.year() == year
        && message.timestamp.month() == month
        && message.timestamp.day() == day
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
        fs::create_dir_all(root.join("nested/copy")).unwrap();
        fs::create_dir_all(root.join("nested/empty/a/b")).unwrap();
        fs::write(
            root.join("nested/copy/[1-2-24] channelone - Chat.txt"),
            "[0:00:04] User: hello",
        )
        .unwrap();
        let runtime = LegacyTxtRuntime {
            import_folder: Some(root.clone()),
            check_each_request: true,
            mode: LegacyTxtMode::MissingOnly,
            delete_raw_after_import: false,
            delete_current_raw_on_discovery: true,
            delete_reconstructed_after_read: false,
            delete_current_reconstructed_on_discovery: false,
            import_folder_exists_at_startup: true,
        };
        let files = runtime.discover_import_files().unwrap();
        assert_eq!(files.len(), 1);
        assert!(!root.join("nested/empty/a/b").exists());
    }

    #[test]
    fn debug_log_lines_extract_raw_irc_messages() {
        let lines = vec![
            "2024-03-21 14:08:32.304 irc.client 402 DEBUG: command: pubmsg, source: kochayuyo!kochayuyo@kochayuyo.tmi.twitch.tv, target: #fobm4ster, arguments: ['annytfErmDying'], tags: [...]".to_string(),
            "2024-03-21 14:09:30.126 irc.client 333 DEBUG: FROM SERVER: @badge-info=;badges=twitch-recap-2023/1;color=#008000;display-name=FireflyHairOrnament;emotes=;first-msg=0;flags=;id=669ecf91-aa9f-4f7c-bdef-a5d51d190b81;mod=0;returning-chatter=0;room-id=79202256;subscriber=0;tmi-sent-ts=1711030170007;turbo=0;user-id=716048414;user-type= :fireflyhairornament!fireflyhairornament@fireflyhairornament.tmi.twitch.tv PRIVMSG #zy0xxx :ZlAY".to_string(),
            "2024-03-21 14:10:08.534 twitchbot.autosongrequest 363 INFO: Checking Song Queue".to_string(),
        ];
        let extracted = extract_raw_irc_lines(&lines);
        assert_eq!(extracted.len(), 1);
        assert!(extracted[0].contains("PRIVMSG #zy0xxx :ZlAY"));
        assert!(behaves_like_raw_irc(&lines));
    }
}
