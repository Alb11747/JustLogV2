use std::collections::{BTreeSet, HashMap, HashSet};
use std::env;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::thread;
use std::time::Instant;

use anyhow::{Result, anyhow};
use chrono::{DateTime, Datelike, TimeZone, Utc};
use flate2::read::GzDecoder;
use regex::Regex;
use serde::Deserialize;
use walkdir::WalkDir;

use crate::legacy_txt::{BulkRawImportSummary, LegacyTxtMode};
use crate::model::{
    CanonicalEvent, ChannelDayKey, ChatMessage, PRIVMSG_TYPE, StoredEvent, UserMonthKey,
};
use crate::store::Store;

const DISCOVERY_QUEUE_CAPACITY: usize = 256;
const PARSE_QUEUE_CAPACITY: usize = 64;
const PROGRESS_INTERVAL: usize = 100;

#[derive(Debug, Clone)]
pub struct ImportPipelineConfig {
    pub import_folder: PathBuf,
    pub mode: LegacyTxtMode,
    pub delete_raw_after_import: bool,
    pub delete_current_raw_on_discovery: bool,
    pub delete_reconstructed_after_import: bool,
    pub delete_current_reconstructed_on_discovery: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportFamily {
    Raw,
    SimpleText,
    JsonExport,
}

#[derive(Debug, Clone)]
pub struct ImportTarget {
    pub channel_id: Option<String>,
    pub day: Option<ChannelDayKey>,
}

#[derive(Debug, Clone)]
struct ImportCandidate {
    path: PathBuf,
    fingerprint: String,
    family: ImportFamily,
}

#[derive(Debug)]
struct ParsedImportFile {
    candidate: ImportCandidate,
    all_matching: bool,
    events: Vec<CanonicalEvent>,
}

#[derive(Debug, Clone, Default)]
pub struct ChannelDayImportResult {
    pub affected_channel_days: BTreeSet<ChannelDayKey>,
    pub affected_user_months: BTreeSet<UserMonthKey>,
}

pub fn run_import(
    store: &Store,
    config: &ImportPipelineConfig,
    target: ImportTarget,
    limit_files: Option<usize>,
    dry_run: bool,
) -> Result<BulkRawImportSummary> {
    if !config.import_folder.exists() {
        return Ok(BulkRawImportSummary {
            dry_run,
            channel_id: target.channel_id.clone(),
            ..BulkRawImportSummary::default()
        });
    }

    let started = Instant::now();
    let run_id = store.start_import_run(target.channel_id.as_deref(), limit_files, dry_run)?;
    let worker_count = default_worker_count();
    let (candidate_tx, candidate_rx) =
        mpsc::sync_channel::<Option<ImportCandidate>>(DISCOVERY_QUEUE_CAPACITY);
    let (parse_tx, parse_rx) = mpsc::sync_channel::<Option<ImportCandidate>>(PARSE_QUEUE_CAPACITY);
    let (result_tx, result_rx) = mpsc::sync_channel::<Result<ParsedImportFile>>(PARSE_QUEUE_CAPACITY);

    let discovery_root = config.import_folder.clone();
    let discovery_target = target.clone();
    let discovery_handle = thread::spawn(move || {
        discover_candidates(
            &discovery_root,
            discovery_target,
            limit_files,
            candidate_tx,
        )
    });

    let mut parser_handles = Vec::new();
    let shared_parse_rx = std::sync::Arc::new(std::sync::Mutex::new(parse_rx));
    for _ in 0..worker_count {
        let rx = shared_parse_rx.clone();
        let tx = result_tx.clone();
        let target = target.clone();
        parser_handles.push(thread::spawn(move || parse_worker_loop(rx, tx, target)));
    }
    drop(result_tx);

    let mut summary = BulkRawImportSummary {
        dry_run,
        channel_id: target.channel_id.clone(),
        ..BulkRawImportSummary::default()
    };
    let mut in_flight = 0usize;
    let mut completed = 0usize;
    let mut failures = 0usize;
    let mut affected_channel_days = BTreeSet::new();
    let mut affected_user_months = BTreeSet::new();

    loop {
        match candidate_rx.recv() {
            Ok(Some(candidate)) => {
                process_discovered_candidate(
                    store,
                    config,
                    run_id,
                    dry_run,
                    &candidate,
                    &mut summary,
                    &parse_tx,
                    &mut in_flight,
                )?;
            }
            Ok(None) => break,
            Err(_) => break,
        }
    }

    for _ in 0..worker_count {
        parse_tx.send(None)?;
    }

    while in_flight > 0 {
        let parsed = result_rx
            .recv()
            .map_err(|_| anyhow!("import parser channel closed unexpectedly"))??;
        in_flight = in_flight.saturating_sub(1);
        let path_key = parsed.candidate.path.to_string_lossy().to_string();
        match commit_or_finalize_parsed_file(store, run_id, config, parsed, &mut summary) {
            Ok(result) => {
                completed += 1;
                affected_channel_days.extend(result.affected_channel_days);
                affected_user_months.extend(result.affected_user_months);
            }
            Err(error) => {
                failures += 1;
                summary.files_failed += 1;
                let _ = store.upsert_import_file_state(
                    run_id,
                    &path_key,
                    "",
                    "unknown",
                    "failed",
                    Some(&error.to_string()),
                );
            }
        }
        if completed % PROGRESS_INTERVAL == 0 || in_flight == 0 {
            store.update_import_run_progress(
                run_id,
                summary.directories_visited,
                summary.files_discovered,
                completed,
                failures,
                Some(&path_key),
            )?;
        }
    }

    let discovery_result = discovery_handle
        .join()
        .map_err(|_| anyhow!("import discovery thread panicked"))??;
    summary.directories_visited = discovery_result.0;
    summary.files_discovered = discovery_result.1;
    summary.affected_channel_days = affected_channel_days.len();
    summary.affected_user_months = affected_user_months.len();
    summary.elapsed_ms = started.elapsed().as_millis();
    store.update_import_run_progress(
        run_id,
        summary.directories_visited,
        summary.files_discovered,
        completed,
        failures,
        None,
    )?;
    store.finish_import_run(run_id, if failures == 0 { "completed" } else { "completed_with_errors" })?;

    for handle in parser_handles {
        let _ = handle.join();
    }

    Ok(summary)
}

fn process_discovered_candidate(
    store: &Store,
    config: &ImportPipelineConfig,
    run_id: i64,
    dry_run: bool,
    candidate: &ImportCandidate,
    summary: &mut BulkRawImportSummary,
    parse_tx: &SyncSender<Option<ImportCandidate>>,
    in_flight: &mut usize,
) -> Result<()> {
    summary.files_discovered += 1;
    summary.files_scanned += 1;
    summary.files_selected += 1;
    if candidate.family == ImportFamily::Raw {
        summary.raw_candidates += 1;
    }
    if candidate.family == ImportFamily::SimpleText && config.mode == LegacyTxtMode::Off {
        return Ok(());
    }

    let path_key = candidate.path.to_string_lossy().to_string();
    if let Some((stored_fingerprint, state)) = store.import_file_state(&path_key)? {
        if stored_fingerprint == candidate.fingerprint && state == "done" {
            summary.files_current += 1;
            maybe_delete_current_source(config, candidate, summary);
            return Ok(());
        }
        if stored_fingerprint == candidate.fingerprint && state != "done" {
            summary.files_resumed += 1;
        }
    }

    if dry_run {
        summary.files_pending += 1;
        return Ok(());
    }

    store.upsert_import_file_state(
        run_id,
        &path_key,
        &candidate.fingerprint,
        family_name(candidate.family),
        "queued",
        None,
    )?;
    summary.files_pending += 1;
    parse_tx.send(Some(candidate.clone()))?;
    *in_flight += 1;
    Ok(())
}

fn commit_or_finalize_parsed_file(
    store: &Store,
    run_id: i64,
    config: &ImportPipelineConfig,
    parsed: ParsedImportFile,
    summary: &mut BulkRawImportSummary,
) -> Result<ChannelDayImportResult> {
    let path_key = parsed.candidate.path.to_string_lossy().to_string();
    if parsed.events.is_empty() {
        if parsed.all_matching {
            store.upsert_import_file_state(
                run_id,
                &path_key,
                &parsed.candidate.fingerprint,
                family_name(parsed.candidate.family),
                "done",
                None,
            )?;
        }
        return Ok(ChannelDayImportResult::default());
    }

    let result = commit_parsed_file(store, run_id, config, &parsed, summary)?;
    summary.files_imported += 1;
    maybe_delete_consumed_source(config, &parsed.candidate, summary)?;
    Ok(result)
}

fn commit_parsed_file(
    store: &Store,
    run_id: i64,
    _config: &ImportPipelineConfig,
    parsed: &ParsedImportFile,
    summary: &mut BulkRawImportSummary,
) -> Result<ChannelDayImportResult> {
    let path_key = parsed.candidate.path.to_string_lossy().to_string();
    store.upsert_import_file_state(
        run_id,
        &path_key,
        &parsed.candidate.fingerprint,
        family_name(parsed.candidate.family),
        "committing",
        None,
    )?;

    let mut archive_by_day = HashMap::<ChannelDayKey, Vec<CanonicalEvent>>::new();
    let mut hot_events = Vec::new();
    let now = Utc::now();
    for event in &parsed.events {
        let day_key = event.channel_day_key();
        if store.should_archive_channel_day(&day_key, now) {
            archive_by_day.entry(day_key).or_default().push(event.clone());
        } else {
            hot_events.push(event.clone());
        }
    }

    let mut result = ChannelDayImportResult::default();
    if !hot_events.is_empty() {
        let outcome = store.insert_events_batch_low_priority(&hot_events)?;
        summary.hot_partitions_inserted += outcome.affected_channel_days.len();
        result
            .affected_channel_days
            .extend(outcome.affected_channel_days);
        result
            .affected_user_months
            .extend(outcome.affected_user_months);
    }

    if !archive_by_day.is_empty() {
        let mut archive_user_months = BTreeSet::new();
        for (day_key, events) in archive_by_day {
            store.record_import_partition_state(
                "channel",
                &day_key.channel_id,
                None,
                day_key.year,
                day_key.month,
                Some(day_key.day),
                "committing",
                None,
                true,
            )?;
            store.record_import_file_partition(
                &path_key,
                "channel",
                &day_key.channel_id,
                None,
                day_key.year,
                day_key.month,
                Some(day_key.day),
            )?;
            let mut merged = store.read_channel_logs(
                &day_key.channel_id,
                day_key.year,
                day_key.month,
                day_key.day,
            )?;
            merged.extend(events.into_iter().map(stored_from_canonical));
            dedupe_and_sort(&mut merged);
            store.import_replace_or_create_channel_segment(
                &day_key.channel_id,
                day_key.year,
                day_key.month,
                day_key.day,
                &merged,
            )?;
            store.record_import_partition_state(
                "channel",
                &day_key.channel_id,
                None,
                day_key.year,
                day_key.month,
                Some(day_key.day),
                "committed",
                None,
                false,
            )?;
            summary.archive_partitions_committed += 1;
            result.affected_channel_days.insert(day_key.clone());
            for event in &merged {
                for key in stored_user_month_keys(event) {
                    archive_user_months.insert(key);
                }
            }
        }

        for month_key in archive_user_months {
            store.record_import_partition_state(
                "user",
                &month_key.channel_id,
                Some(&month_key.user_id),
                month_key.year,
                month_key.month,
                None,
                "committing",
                None,
                true,
            )?;
            store.record_import_file_partition(
                &path_key,
                "user",
                &month_key.channel_id,
                Some(&month_key.user_id),
                month_key.year,
                month_key.month,
                None,
            )?;
            let mut merged = store.read_user_logs(
                &month_key.channel_id,
                &month_key.user_id,
                month_key.year,
                month_key.month,
            )?;
            dedupe_and_sort(&mut merged);
            store.import_replace_or_create_user_segment(
                &month_key.channel_id,
                &month_key.user_id,
                month_key.year,
                month_key.month,
                &merged,
            )?;
            store.record_import_partition_state(
                "user",
                &month_key.channel_id,
                Some(&month_key.user_id),
                month_key.year,
                month_key.month,
                None,
                "committed",
                None,
                false,
            )?;
            result.affected_user_months.insert(month_key);
        }
    }

    store.upsert_import_file_state(
        run_id,
        &path_key,
        &parsed.candidate.fingerprint,
        family_name(parsed.candidate.family),
        "done",
        None,
    )?;
    match parsed.candidate.family {
        ImportFamily::Raw => summary.files_parsed_raw += 1,
        ImportFamily::SimpleText => summary.files_parsed_simple += 1,
        ImportFamily::JsonExport => summary.files_parsed_json += 1,
    }
    Ok(result)
}

fn discover_candidates(
    root: &Path,
    target: ImportTarget,
    limit_files: Option<usize>,
    sender: SyncSender<Option<ImportCandidate>>,
) -> Result<(usize, usize)> {
    let mut directories = 0usize;
    let mut files = 0usize;
    for entry in WalkDir::new(root).sort_by_file_name().into_iter().filter_map(Result::ok) {
        if entry.file_type().is_dir() {
            directories += 1;
            continue;
        }
        if !entry.file_type().is_file() {
            continue;
        }
        if limit_files.is_some_and(|limit| files >= limit) {
            break;
        }
        let path = entry.into_path();
        let Some(family) = classify_import_file(&path)? else {
            continue;
        };
        if let Some(channel_id) = target.channel_id.as_deref() {
            if !candidate_might_match_channel(&path, family, channel_id) {
                continue;
            }
        }
        let fingerprint = file_fingerprint(&path)?;
        sender.send(Some(ImportCandidate {
            path,
            fingerprint,
            family,
        }))?;
        files += 1;
    }
    let _ = sender.send(None);
    Ok((directories, files))
}

fn parse_worker_loop(
    receiver: std::sync::Arc<std::sync::Mutex<Receiver<Option<ImportCandidate>>>>,
    sender: SyncSender<Result<ParsedImportFile>>,
    target: ImportTarget,
) {
    loop {
        let next = {
            let guard = receiver.lock().unwrap();
            guard.recv()
        };
        let Ok(message) = next else {
            return;
        };
        let Some(candidate) = message else {
            return;
        };
        let result = parse_candidate(candidate, &target);
        let _ = sender.send(result);
    }
}

fn parse_candidate(candidate: ImportCandidate, target: &ImportTarget) -> Result<ParsedImportFile> {
    let all_matching = target.day.is_none();
    let mut events = match candidate.family {
        ImportFamily::Raw => parse_raw_file(&candidate.path)?,
        ImportFamily::SimpleText => parse_simple_text_file(&candidate.path)?,
        ImportFamily::JsonExport => parse_json_file(&candidate.path)?,
    };

    if let Some(channel_id) = target.channel_id.as_deref() {
        events.retain(|event| event.room_id == channel_id);
    }
    if let Some(day) = target.day.as_ref() {
        events.retain(|event| {
            let key = event.channel_day_key();
            key.channel_id == day.channel_id
                && key.year == day.year
                && key.month == day.month
                && key.day == day.day
        });
    }

    Ok(ParsedImportFile {
        candidate,
        all_matching,
        events,
    })
}

fn maybe_delete_current_source(
    config: &ImportPipelineConfig,
    candidate: &ImportCandidate,
    summary: &mut BulkRawImportSummary,
) {
    let delete = match candidate.family {
        ImportFamily::Raw => config.delete_current_raw_on_discovery,
        ImportFamily::SimpleText | ImportFamily::JsonExport => {
            config.delete_current_reconstructed_on_discovery
        }
    };
    if delete && fs::remove_file(&candidate.path).is_ok() {
        summary.files_deleted += 1;
        remove_empty_dir_and_parents(&config.import_folder, candidate.path.parent());
    }
}

fn maybe_delete_consumed_source(
    config: &ImportPipelineConfig,
    candidate: &ImportCandidate,
    summary: &mut BulkRawImportSummary,
) -> Result<()> {
    let delete = match candidate.family {
        ImportFamily::Raw => config.delete_raw_after_import,
        ImportFamily::SimpleText | ImportFamily::JsonExport => {
            config.delete_reconstructed_after_import
        }
    };
    if delete && candidate.path.exists() {
        fs::remove_file(&candidate.path)?;
        summary.files_deleted += 1;
        remove_empty_dir_and_parents(&config.import_folder, candidate.path.parent());
    }
    Ok(())
}

fn family_name(family: ImportFamily) -> &'static str {
    match family {
        ImportFamily::Raw => "raw",
        ImportFamily::SimpleText => "simple_text",
        ImportFamily::JsonExport => "json_export",
    }
}

fn default_worker_count() -> usize {
    env::var("JUSTLOG_IMPORT_MAX_RAW_WORKERS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| {
            thread::available_parallelism()
                .map(|value| value.get())
                .unwrap_or(4)
                .min(8)
        })
}

fn candidate_might_match_channel(path: &Path, family: ImportFamily, channel_id: &str) -> bool {
    if let Some((matched_channel_id, _, _, _)) = infer_legacy_path_channel_day(path) {
        return matched_channel_id == channel_id;
    }
    family == ImportFamily::Raw
}

fn stored_from_canonical(event: CanonicalEvent) -> StoredEvent {
    StoredEvent {
        seq: 0,
        event_uid: event.event_uid,
        room_id: event.room_id,
        channel_login: event.channel_login,
        username: event.username,
        display_name: event.display_name,
        user_id: event.user_id,
        target_user_id: event.target_user_id,
        text: event.text,
        system_text: event.system_text,
        timestamp: event.timestamp,
        raw: event.raw,
        tags: event.tags,
        kind: event.kind,
    }
}

fn stored_user_month_keys(event: &StoredEvent) -> Vec<UserMonthKey> {
    let mut keys = Vec::new();
    for user_id in [event.user_id.clone(), event.target_user_id.clone()]
        .into_iter()
        .flatten()
    {
        keys.push(UserMonthKey {
            channel_id: event.room_id.clone(),
            user_id,
            year: event.timestamp.year(),
            month: event.timestamp.month(),
        });
    }
    keys
}

fn dedupe_and_sort(events: &mut Vec<StoredEvent>) {
    events.sort_by_key(|event| {
        (
            event.timestamp.timestamp_millis(),
            event.seq,
            event.event_uid.clone(),
        )
    });
    let mut seen = HashSet::new();
    events.retain(|event| seen.insert(event.event_uid.clone()));
    events.sort_by_key(|event| event.timestamp.timestamp_millis());
}

fn classify_import_file(path: &Path) -> Result<Option<ImportFamily>> {
    let name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if name.ends_with(".json") || name.ends_with(".json.gz") {
        return Ok(Some(ImportFamily::JsonExport));
    }
    if !(name.ends_with(".txt")
        || name.ends_with(".txt.gz")
        || name.ends_with(".log")
        || name.ends_with(".log.gz"))
    {
        return Ok(None);
    }
    if behaves_like_raw_irc_path(path)? {
        Ok(Some(ImportFamily::Raw))
    } else {
        Ok(Some(ImportFamily::SimpleText))
    }
}

fn parse_raw_file(path: &Path) -> Result<Vec<CanonicalEvent>> {
    let mut events = Vec::new();
    for_each_line_in_supported_file(path, |line| {
        let Some(raw) = extract_raw_irc_line(&line) else {
            return Ok(());
        };
        if let Some(event) = CanonicalEvent::from_raw(&raw)? {
            events.push(event);
        }
        Ok(())
    })?;
    Ok(events)
}

fn parse_simple_text_file(path: &Path) -> Result<Vec<CanonicalEvent>> {
    let (channel_id, year, month, day) = infer_legacy_path_channel_day(path)
        .ok_or_else(|| anyhow!("simple text import path must end with <channel>/<year>/<month>/<day>"))?;
    let channel_login = infer_channel_login_from_rawish_name(path).unwrap_or_default();
    let messages = parse_sparse_txt_file(path, &channel_login, year, month, day)?;
    Ok(messages
        .into_iter()
        .map(|message| CanonicalEvent::from_chat_message(&channel_id, &channel_login, &message))
        .collect())
}

fn parse_json_file(path: &Path) -> Result<Vec<CanonicalEvent>> {
    let inferred = infer_legacy_path_channel_day(path);
    let default_channel_login = infer_channel_login_from_rawish_name(path).unwrap_or_default();
    let messages = parse_json_export_file(path, &default_channel_login)?;
    let default_channel_id = inferred
        .as_ref()
        .map(|value| value.0.clone())
        .unwrap_or_default();
    Ok(messages
        .into_iter()
        .map(|message| {
            let room_id = message
                .tags
                .get("room-id")
                .cloned()
                .filter(|value| !value.is_empty())
                .unwrap_or_else(|| default_channel_id.clone());
            let channel_login = if message.channel.is_empty() {
                default_channel_login.clone()
            } else {
                message.channel.clone()
            };
            CanonicalEvent::from_chat_message(&room_id, &channel_login, &message)
        })
        .collect())
}

fn behaves_like_raw_irc_path(path: &Path) -> Result<bool> {
    let mut matched = false;
    for_each_line_in_supported_file(path, |line| {
        let Some(raw) = extract_raw_irc_line(&line) else {
            return Ok(());
        };
        if CanonicalEvent::from_raw(&raw)?.is_some() {
            matched = true;
            return Err(anyhow!("classification-match"));
        }
        Ok(())
    })
    .or_else(|error| {
        if error.to_string() == "classification-match" {
            Ok(())
        } else {
            Err(error)
        }
    })?;
    Ok(matched)
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
            channel,
            timestamp,
            id,
            message_type: PRIVMSG_TYPE,
            raw: build_surrogate_raw(channel_login, &username, timestamp, &message_body),
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
    if stem.eq_ignore_ascii_case("channel") {
        return None;
    }
    let day = stem.parse::<u32>().ok()?;
    Some((channel_id, year, month, day))
}

fn infer_channel_login_from_rawish_name(path: &Path) -> Option<String> {
    path.file_stem()
        .and_then(|value| value.to_str())
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty() && value != "channel")
}

fn remove_empty_dir_and_parents(root: &Path, start: Option<&Path>) {
    let Some(start) = start else {
        return;
    };
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
    comments: Vec<JsonComment>,
}

#[derive(Debug, Deserialize)]
struct JsonStreamer {
    name: String,
    id: i64,
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
}

#[derive(Debug, Deserialize)]
struct JsonCommentMessage {
    #[serde(default)]
    body: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_text_import_normalizes_to_canonical_events() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("imports/1/2024/1/2.txt");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, "[0:00:04] SomeUser: hello").unwrap();

        let events = parse_simple_text_file(&path).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].room_id, "1");
        assert_eq!(events[0].text, "hello");
    }

    #[test]
    fn json_import_uses_path_channel_id_when_missing() {
        let temp = tempfile::TempDir::new().unwrap();
        let path = temp.path().join("imports/1/2024/1/2.json");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(
            &path,
            r##"{"comments":[{"_id":"json-1","created_at":"2024-01-02T00:00:04Z","commenter":{"display_name":"JsonUser","name":"jsonuser"},"message":{"body":"json imported"}}]}"##,
        )
        .unwrap();

        let events = parse_json_file(&path).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].room_id, "1");
        assert_eq!(events[0].text, "json imported");
    }
}
