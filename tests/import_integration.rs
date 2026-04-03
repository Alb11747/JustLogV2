mod common;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use anyhow::{Result, anyhow};
use brotli::{CompressorWriter, Decompressor};
use chrono::{Datelike, Duration, TimeZone, Utc};
use flate2::Compression;
use flate2::write::GzEncoder;
use justlog::import_pipeline::{
    ImportArchive, ImportHooks, ImportInstrumentation, ImportIo, ImportPipelineConfig,
    ImportTarget, ImportWalkEntry, run_import_with_hooks,
};
use justlog::legacy_txt::LegacyTxtMode;
use justlog::model::{ChannelDayKey, StoredEvent, UserMonthKey};
use justlog::store::Store;
use serde_json::json;
use tempfile::TempDir;

use common::{TestHarness, TestHarnessOptions, privmsg};

fn env_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

fn pipeline_config(root: &Path) -> ImportPipelineConfig {
    ImportPipelineConfig {
        import_folder: root.to_path_buf(),
        mode: LegacyTxtMode::MissingOnly,
        delete_raw_after_import: true,
        delete_current_raw_on_discovery: false,
        delete_reconstructed_after_import: true,
        delete_current_reconstructed_on_discovery: false,
    }
}

fn gzip_bytes_fast(content: &str) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(content.as_bytes()).unwrap();
    encoder.finish().unwrap()
}

fn write_fixture(path: &Path, content: &str) {
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    if path.extension().and_then(|ext| ext.to_str()) == Some("gz") {
        fs::write(path, gzip_bytes_fast(content)).unwrap();
    } else {
        fs::write(path, content).unwrap();
    }
}

fn tiny_simple_text(messages: &[(&str, &str)]) -> String {
    messages
        .iter()
        .map(|(time, line)| format!("[{time}] {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn tiny_json_export(day: u32, messages: &[(&str, &str, &str, &str)]) -> String {
    let comments = messages
        .iter()
        .enumerate()
        .map(|(index, (id, display_name, login, body))| {
            json!({
                "_id": id,
                "created_at": format!("2024-01-{day:02}T00:00:{:02}Z", index + 4),
                "channel_id": "1",
                "content_id": "vod-1",
                "commenter": {
                    "display_name": display_name,
                    "_id": format!("user-{index}"),
                    "name": login,
                    "logo": "https://example.com/logo.png"
                },
                "message": {
                    "body": body,
                    "user_color": "#8A2BE2",
                    "user_badges": [],
                    "emoticons": []
                }
            })
        })
        .collect::<Vec<_>>();
    serde_json::to_string(&json!({
        "streamer": { "name": "channelone", "id": 1 },
        "video": { "title": "tiny vod", "id": "vod-1" },
        "comments": comments
    }))
    .unwrap()
}

fn tiny_debug_raw_log(messages: &[(&str, i64, &str)]) -> String {
    let mut lines = vec![
        "2024-03-21 14:09:30.120 httpx 1 DEBUG: unrelated line".to_string(),
        "2024-03-21 14:09:30.121 irc.client 1221 DEBUG: _dispatcher: all_raw_messages".to_string(),
    ];
    lines.extend(messages.iter().map(|(id, timestamp_ms, text)| {
        format!(
            "2024-03-21 14:09:30.126 irc.client 333 DEBUG: FROM SERVER: {}",
            privmsg(
                id,
                "1",
                "200",
                "viewer",
                "viewer",
                "channelone",
                *timestamp_ms,
                text,
            )
        )
    }));
    lines.push("2024-03-21 14:09:30.127 irc.client 402 DEBUG: command: pubmsg".to_string());
    lines.join("\n")
}

fn update_max(maximum: &AtomicUsize, value: usize) {
    let mut current = maximum.load(Ordering::SeqCst);
    while value > current
        && maximum
            .compare_exchange(current, value, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
    {
        current = maximum.load(Ordering::SeqCst);
    }
}

#[derive(Default)]
struct ImportMetrics {
    discovered: AtomicUsize,
    discovered_after_parse_started: AtomicUsize,
    parse_active: AtomicUsize,
    parse_max: AtomicUsize,
    commit_active: AtomicUsize,
    commit_max: AtomicUsize,
    queue_backpressure: AtomicUsize,
    parse_started: AtomicBool,
}

impl ImportMetrics {
    fn max_parse_active(&self) -> usize {
        self.parse_max.load(Ordering::SeqCst)
    }

    fn max_commit_active(&self) -> usize {
        self.commit_max.load(Ordering::SeqCst)
    }
}

impl ImportInstrumentation for ImportMetrics {
    fn on_discovered(&self, _path: &Path) {
        self.discovered.fetch_add(1, Ordering::SeqCst);
        if self.parse_started.load(Ordering::SeqCst) {
            self.discovered_after_parse_started
                .fetch_add(1, Ordering::SeqCst);
        }
    }

    fn on_parse_queue_backpressure(&self, _path: &Path) {
        self.queue_backpressure.fetch_add(1, Ordering::SeqCst);
    }

    fn on_parse_start(&self, _path: &Path) {
        self.parse_started.store(true, Ordering::SeqCst);
        let active = self.parse_active.fetch_add(1, Ordering::SeqCst) + 1;
        update_max(&self.parse_max, active);
    }

    fn on_parse_finish(&self, _path: &Path) {
        self.parse_active.fetch_sub(1, Ordering::SeqCst);
    }

    fn on_commit_start(&self, _scope: &str, _key: &str) {
        let active = self.commit_active.fetch_add(1, Ordering::SeqCst) + 1;
        update_max(&self.commit_max, active);
    }

    fn on_commit_finish(&self, _scope: &str, _key: &str) {
        self.commit_active.fetch_sub(1, Ordering::SeqCst);
    }
}

#[derive(Default)]
struct MockFsState {
    files: BTreeMap<PathBuf, Vec<u8>>,
    dirs: BTreeSet<PathBuf>,
}

struct MockFs {
    root: PathBuf,
    state: Mutex<MockFsState>,
    fail_once: Mutex<HashSet<(String, PathBuf)>>,
    fail_read_on_attempt: Mutex<BTreeMap<PathBuf, usize>>,
    read_attempts: Mutex<BTreeMap<PathBuf, usize>>,
    write_counts: Mutex<BTreeMap<PathBuf, usize>>,
    replace_counts: Mutex<BTreeMap<PathBuf, usize>>,
    coordinated_reads: usize,
    started_reads: AtomicUsize,
    active_reads: AtomicUsize,
    max_active_reads: AtomicUsize,
}

impl MockFs {
    fn new(root: PathBuf, coordinated_reads: usize) -> Self {
        let mut dirs = BTreeSet::new();
        dirs.insert(root.clone());
        Self {
            root,
            state: Mutex::new(MockFsState {
                files: BTreeMap::new(),
                dirs,
            }),
            fail_once: Mutex::new(HashSet::new()),
            fail_read_on_attempt: Mutex::new(BTreeMap::new()),
            read_attempts: Mutex::new(BTreeMap::new()),
            write_counts: Mutex::new(BTreeMap::new()),
            replace_counts: Mutex::new(BTreeMap::new()),
            coordinated_reads,
            started_reads: AtomicUsize::new(0),
            active_reads: AtomicUsize::new(0),
            max_active_reads: AtomicUsize::new(0),
        }
    }

    fn add_file(&self, path: PathBuf, bytes: Vec<u8>) {
        let mut state = self.state.lock().unwrap();
        self.ensure_parent_dirs(&mut state, &path);
        state.files.insert(path, bytes);
    }

    fn ensure_parent_dirs(&self, state: &mut MockFsState, path: &Path) {
        let mut current = path.parent();
        while let Some(dir) = current {
            if !dir.starts_with(&self.root) {
                break;
            }
            state.dirs.insert(dir.to_path_buf());
            current = dir.parent();
        }
        state.dirs.insert(self.root.clone());
    }

    fn fail_once(&self, op: &str, path: &Path) {
        self.fail_once
            .lock()
            .unwrap()
            .insert((op.to_string(), path.to_path_buf()));
    }

    fn fail_read_on_attempt(&self, path: &Path, attempt: usize) {
        self.fail_read_on_attempt
            .lock()
            .unwrap()
            .insert(path.to_path_buf(), attempt);
    }

    fn maybe_fail(&self, op: &str, path: &Path) -> Result<()> {
        let key = (op.to_string(), path.to_path_buf());
        if self.fail_once.lock().unwrap().remove(&key) {
            Err(anyhow!("mock fs injected failure for {op} {}", path.display()))
        } else {
            Ok(())
        }
    }

    fn read_bytes(&self, path: &Path) -> Result<Vec<u8>> {
        self.maybe_fail("read", path)?;
        {
            let mut attempts = self.read_attempts.lock().unwrap();
            let attempt = attempts.entry(path.to_path_buf()).or_insert(0);
            *attempt += 1;
            if self
                .fail_read_on_attempt
                .lock()
                .unwrap()
                .get(path)
                .copied()
                .is_some_and(|fail_at| fail_at == *attempt)
            {
                return Err(anyhow!(
                    "mock fs injected failure for read {}",
                    path.display()
                ));
            }
        }
        let active = self.active_reads.fetch_add(1, Ordering::SeqCst) + 1;
        update_max(&self.max_active_reads, active);
        let started = self.started_reads.fetch_add(1, Ordering::SeqCst) + 1;
        if started <= self.coordinated_reads {
            for _ in 0..10_000 {
                if self.active_reads.load(Ordering::SeqCst) >= self.coordinated_reads
                    || self.started_reads.load(Ordering::SeqCst) >= self.coordinated_reads
                {
                    break;
                }
                std::thread::yield_now();
            }
        }
        let bytes = self
            .state
            .lock()
            .unwrap()
            .files
            .get(path)
            .cloned()
            .ok_or_else(|| anyhow!("mock fs missing file {}", path.display()));
        self.active_reads.fetch_sub(1, Ordering::SeqCst);
        bytes
    }

    fn write_bytes(&self, path: &Path, bytes: Vec<u8>) -> Result<()> {
        self.maybe_fail("write", path)?;
        *self
            .write_counts
            .lock()
            .unwrap()
            .entry(path.to_path_buf())
            .or_insert(0) += 1;
        let mut state = self.state.lock().unwrap();
        self.ensure_parent_dirs(&mut state, path);
        state.files.insert(path.to_path_buf(), bytes);
        Ok(())
    }

    fn atomic_replace(&self, temp: &Path, final_path: &Path) -> Result<()> {
        self.maybe_fail("replace", final_path)?;
        *self
            .replace_counts
            .lock()
            .unwrap()
            .entry(final_path.to_path_buf())
            .or_insert(0) += 1;
        let mut state = self.state.lock().unwrap();
        let bytes = state
            .files
            .remove(temp)
            .ok_or_else(|| anyhow!("mock fs missing temp {}", temp.display()))?;
        self.ensure_parent_dirs(&mut state, final_path);
        state.files.insert(final_path.to_path_buf(), bytes);
        Ok(())
    }

    fn file_bytes(&self, path: &Path) -> Option<Vec<u8>> {
        self.state.lock().unwrap().files.get(path).cloned()
    }

    fn file_exists(&self, path: &Path) -> bool {
        self.state.lock().unwrap().files.contains_key(path)
    }

    fn source_file_count(&self) -> usize {
        self.state
            .lock()
            .unwrap()
            .files
            .keys()
            .filter(|path| path.starts_with(&self.root))
            .count()
    }

    fn write_count(&self, path: &Path) -> usize {
        self.write_counts
            .lock()
            .unwrap()
            .get(path)
            .copied()
            .unwrap_or_default()
    }

    fn replace_count(&self, path: &Path) -> usize {
        self.replace_counts
            .lock()
            .unwrap()
            .get(path)
            .copied()
            .unwrap_or_default()
    }
}

impl ImportIo for MockFs {
    fn path_exists(&self, path: &Path) -> bool {
        let state = self.state.lock().unwrap();
        state.files.contains_key(path) || state.dirs.contains(path)
    }

    fn walk(
        &self,
        root: &Path,
        visitor: &mut dyn FnMut(ImportWalkEntry) -> Result<()>,
    ) -> Result<()> {
        let state = self.state.lock().unwrap();
        let mut entries = Vec::new();
        for dir in &state.dirs {
            if dir.starts_with(root) {
                entries.push(ImportWalkEntry {
                    path: dir.clone(),
                    is_dir: true,
                    is_file: false,
                });
            }
        }
        for file in state.files.keys() {
            if file.starts_with(root) {
                entries.push(ImportWalkEntry {
                    path: file.clone(),
                    is_dir: false,
                    is_file: true,
                });
            }
        }
        entries.sort_by(|left, right| left.path.cmp(&right.path));
        drop(state);
        for entry in entries {
            visitor(entry)?;
        }
        Ok(())
    }

    fn fingerprint(&self, path: &Path) -> Result<String> {
        let bytes = self
            .state
            .lock()
            .unwrap()
            .files
            .get(path)
            .cloned()
            .ok_or_else(|| anyhow!("mock fs missing file {}", path.display()))?;
        Ok(blake3::hash(&bytes).to_hex().to_string())
    }

    fn read_lines(&self, path: &Path) -> Result<Vec<String>> {
        let bytes = self.read_bytes(path)?;
        let text = if path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or_default()
            .ends_with(".gz")
        {
            let mut text = String::new();
            flate2::read::GzDecoder::new(bytes.as_slice()).read_to_string(&mut text)?;
            text
        } else {
            String::from_utf8(bytes)?
        };
        Ok(text.lines().map(|line| line.to_string()).collect())
    }

    fn read_to_string(&self, path: &Path) -> Result<String> {
        let bytes = self.read_bytes(path)?;
        if path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or_default()
            .ends_with(".gz")
        {
            let mut text = String::new();
            flate2::read::GzDecoder::new(bytes.as_slice()).read_to_string(&mut text)?;
            Ok(text)
        } else {
            Ok(String::from_utf8(bytes)?)
        }
    }

    fn remove_file(&self, path: &Path) -> Result<()> {
        self.maybe_fail("remove", path)?;
        let mut state = self.state.lock().unwrap();
        state.files.remove(path);
        Ok(())
    }

    fn prune_empty_dir_and_parents(&self, root: &Path, start: Option<&Path>) {
        let Some(mut current) = start.map(Path::to_path_buf) else {
            return;
        };
        let mut state = self.state.lock().unwrap();
        loop {
            if current == root {
                break;
            }
            let has_child_dir = state
                .dirs
                .iter()
                .any(|dir| dir.parent() == Some(current.as_path()));
            let has_child_file = state
                .files
                .keys()
                .any(|file| file.parent() == Some(current.as_path()));
            if has_child_dir || has_child_file {
                break;
            }
            state.dirs.remove(&current);
            let Some(parent) = current.parent() else {
                break;
            };
            current = parent.to_path_buf();
        }
    }
}

#[derive(Default)]
struct MockArchiveState {
    channel_events: BTreeMap<String, Vec<StoredEvent>>,
    user_events: BTreeMap<String, Vec<StoredEvent>>,
    channel_write_attempts: BTreeMap<String, usize>,
    channel_write_successes: BTreeMap<String, usize>,
    user_write_attempts: BTreeMap<String, usize>,
    user_write_successes: BTreeMap<String, usize>,
}

struct MockArchive {
    fs: Arc<MockFs>,
    root: PathBuf,
    state: Mutex<MockArchiveState>,
    active_keys: Mutex<HashSet<String>>,
    same_key_overlap_detected: AtomicBool,
}

impl MockArchive {
    fn new(fs: Arc<MockFs>, root: PathBuf) -> Self {
        Self {
            fs,
            root,
            state: Mutex::new(MockArchiveState::default()),
            active_keys: Mutex::new(HashSet::new()),
            same_key_overlap_detected: AtomicBool::new(false),
        }
    }

    fn channel_storage_key(&self, key: &ChannelDayKey) -> String {
        format!(
            "channel/{}/{}/{}/{}",
            key.channel_id, key.year, key.month, key.day
        )
    }

    fn user_storage_key(&self, key: &UserMonthKey) -> String {
        format!(
            "user/{}/{}/{}/{}",
            key.channel_id, key.user_id, key.year, key.month
        )
    }

    fn channel_final_path(&self, key: &ChannelDayKey) -> PathBuf {
        self.root.join(format!(
            "channels/{}/{}/{}/{}.br",
            key.channel_id, key.year, key.month, key.day
        ))
    }

    fn user_final_path(&self, key: &UserMonthKey) -> PathBuf {
        self.root.join(format!(
            "users/{}/{}/{}/{}.br",
            key.channel_id, key.user_id, key.year, key.month
        ))
    }

    fn encode_events(events: &[StoredEvent]) -> Vec<u8> {
        let body = events
            .iter()
            .map(|event| {
                if !event.raw.is_empty() {
                    event.raw.clone()
                } else {
                    event.text.clone()
                }
            })
            .collect::<Vec<_>>()
            .join("\n");
        let mut encoded = Vec::new();
        {
            let mut writer = CompressorWriter::new(&mut encoded, 16 * 1024, 0, 20);
            writer.write_all(body.as_bytes()).unwrap();
        }
        encoded
    }

    fn begin_commit(&self, key: &str) {
        let mut active = self.active_keys.lock().unwrap();
        if !active.insert(key.to_string()) {
            self.same_key_overlap_detected.store(true, Ordering::SeqCst);
        }
    }

    fn end_commit(&self, key: &str) {
        self.active_keys.lock().unwrap().remove(key);
    }

    fn channel_event_count(&self) -> usize {
        self.state
            .lock()
            .unwrap()
            .channel_events
            .values()
            .map(Vec::len)
            .sum()
    }

    fn decode_channel_segment(&self, key: &ChannelDayKey) -> String {
        let path = self.channel_final_path(key);
        let bytes = self.fs.file_bytes(&path).unwrap();
        let mut output = String::new();
        Decompressor::new(bytes.as_slice(), 16 * 1024)
            .read_to_string(&mut output)
            .unwrap();
        output
    }

    fn channel_write_attempts(&self, key: &ChannelDayKey) -> usize {
        self.state
            .lock()
            .unwrap()
            .channel_write_attempts
            .get(&self.channel_storage_key(key))
            .copied()
            .unwrap_or_default()
    }

    fn channel_write_successes(&self, key: &ChannelDayKey) -> usize {
        self.state
            .lock()
            .unwrap()
            .channel_write_successes
            .get(&self.channel_storage_key(key))
            .copied()
            .unwrap_or_default()
    }

    fn user_write_successes(&self, key: &UserMonthKey) -> usize {
        self.state
            .lock()
            .unwrap()
            .user_write_successes
            .get(&self.user_storage_key(key))
            .copied()
            .unwrap_or_default()
    }

    fn total_channel_write_successes(&self) -> usize {
        self.state
            .lock()
            .unwrap()
            .channel_write_successes
            .values()
            .sum()
    }

    fn total_user_write_successes(&self) -> usize {
        self.state
            .lock()
            .unwrap()
            .user_write_successes
            .values()
            .sum()
    }
}

impl ImportArchive for MockArchive {
    fn read_channel_day(&self, _store: &Store, key: &ChannelDayKey) -> Result<Vec<StoredEvent>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .channel_events
            .get(&self.channel_storage_key(key))
            .cloned()
            .unwrap_or_default())
    }

    fn write_channel_day(
        &self,
        _store: &Store,
        key: &ChannelDayKey,
        events: &[StoredEvent],
    ) -> Result<()> {
        let storage_key = self.channel_storage_key(key);
        let final_path = self.channel_final_path(key);
        let temp_path = final_path.with_extension("tmp.br");
        self.begin_commit(&storage_key);
        *self
            .state
            .lock()
            .unwrap()
            .channel_write_attempts
            .entry(storage_key.clone())
            .or_insert(0) += 1;
        let result = (|| {
            self.fs.write_bytes(&temp_path, Self::encode_events(events))?;
            self.fs.atomic_replace(&temp_path, &final_path)?;
            let mut state = self.state.lock().unwrap();
            state
                .channel_events
                .insert(storage_key.clone(), events.to_vec());
            *state
                .channel_write_successes
                .entry(storage_key.clone())
                .or_insert(0) += 1;
            Ok(())
        })();
        self.end_commit(&storage_key);
        result
    }

    fn read_user_month(&self, _store: &Store, key: &UserMonthKey) -> Result<Vec<StoredEvent>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .user_events
            .get(&self.user_storage_key(key))
            .cloned()
            .unwrap_or_default())
    }

    fn write_user_month(
        &self,
        _store: &Store,
        key: &UserMonthKey,
        events: &[StoredEvent],
    ) -> Result<()> {
        let storage_key = self.user_storage_key(key);
        let final_path = self.user_final_path(key);
        let temp_path = final_path.with_extension("tmp.br");
        self.begin_commit(&storage_key);
        *self
            .state
            .lock()
            .unwrap()
            .user_write_attempts
            .entry(storage_key.clone())
            .or_insert(0) += 1;
        let result = (|| {
            self.fs.write_bytes(&temp_path, Self::encode_events(events))?;
            self.fs.atomic_replace(&temp_path, &final_path)?;
            let mut state = self.state.lock().unwrap();
            state.user_events.insert(storage_key.clone(), events.to_vec());
            *state.user_write_successes.entry(storage_key.clone()).or_insert(0) += 1;
            Ok(())
        })();
        self.end_commit(&storage_key);
        result
    }
}

fn run_mock_import(
    store: &Store,
    root: &Path,
    io: Arc<MockFs>,
    archive: Arc<MockArchive>,
    metrics: Arc<ImportMetrics>,
    limit_files: Option<usize>,
) -> justlog::legacy_txt::BulkRawImportSummary {
    run_import_with_hooks(
        store,
        &pipeline_config(root),
        ImportTarget {
            channel_id: Some("1".to_string()),
            day: None,
        },
        limit_files,
        false,
        ImportHooks {
            io: Some(io),
            archive: Some(archive),
            instrumentation: Some(metrics),
            discovery_queue_capacity: Some(256),
            parse_queue_capacity: Some(1),
        },
    )
    .unwrap()
}

#[derive(Default)]
struct CountingRealArchiveState {
    channel_write_successes: HashMap<String, usize>,
    user_write_successes: HashMap<String, usize>,
}

struct CountingRealArchive {
    state: Mutex<CountingRealArchiveState>,
}

impl CountingRealArchive {
    fn new() -> Self {
        Self {
            state: Mutex::new(CountingRealArchiveState::default()),
        }
    }

    fn channel_key(key: &ChannelDayKey) -> String {
        format!(
            "channel/{}/{}/{}/{}",
            key.channel_id, key.year, key.month, key.day
        )
    }

    fn user_key(key: &UserMonthKey) -> String {
        format!(
            "user/{}/{}/{}/{}",
            key.channel_id, key.user_id, key.year, key.month
        )
    }

    fn channel_write_successes(&self, key: &ChannelDayKey) -> usize {
        self.state
            .lock()
            .unwrap()
            .channel_write_successes
            .get(&Self::channel_key(key))
            .copied()
            .unwrap_or_default()
    }
}

impl ImportArchive for CountingRealArchive {
    fn read_channel_day(&self, store: &Store, key: &ChannelDayKey) -> Result<Vec<StoredEvent>> {
        Ok(store
            .read_archived_channel_segment_strict(&key.channel_id, key.year, key.month, key.day)
            .unwrap_or_default())
    }

    fn write_channel_day(
        &self,
        store: &Store,
        key: &ChannelDayKey,
        events: &[StoredEvent],
    ) -> Result<()> {
        store.import_replace_or_create_channel_segment(
            &key.channel_id,
            key.year,
            key.month,
            key.day,
            events,
        )?;
        *self
            .state
            .lock()
            .unwrap()
            .channel_write_successes
            .entry(Self::channel_key(key))
            .or_insert(0) += 1;
        Ok(())
    }

    fn read_user_month(&self, store: &Store, key: &UserMonthKey) -> Result<Vec<StoredEvent>> {
        Ok(store
            .read_archived_user_segment_strict(
                &key.channel_id,
                &key.user_id,
                key.year,
                key.month,
            )
            .unwrap_or_default())
    }

    fn write_user_month(
        &self,
        store: &Store,
        key: &UserMonthKey,
        events: &[StoredEvent],
    ) -> Result<()> {
        store.import_replace_or_create_user_segment(
            &key.channel_id,
            &key.user_id,
            key.year,
            key.month,
            events,
        )?;
        *self
            .state
            .lock()
            .unwrap()
            .user_write_successes
            .entry(Self::user_key(key))
            .or_insert(0) += 1;
        Ok(())
    }
}

#[tokio::test]
async fn real_filesystem_admin_import_end_to_end_handles_sample_data() {
    let _guard = env_lock().lock().await;
    let temp = TempDir::new().unwrap();
    let import_root = temp.path().join("imports");

    let old_raw = import_root.join("raw/1/2024/1/2/channel.txt");
    let old_raw_gz = import_root.join("raw/1/2024/1/3/channel.txt.gz");
    let old_debug = import_root.join("debug/1/2024/1/4/channel.log");
    let hot_date = (Utc::now() - Duration::days(1)).date_naive();
    let hot_timestamp = Utc
        .with_ymd_and_hms(hot_date.year(), hot_date.month(), hot_date.day(), 0, 0, 1)
        .unwrap()
        .timestamp_millis();
    let hot_raw = import_root.join(format!(
        "raw/1/{}/{}/{}/channel.txt",
        hot_date.year(),
        hot_date.month(),
        hot_date.day()
    ));

    write_fixture(
        &old_raw,
        &privmsg(
            "sample-raw-1",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_153_604_000,
            "sample raw line",
        ),
    );
    write_fixture(
        &old_raw_gz,
        &privmsg(
            "sample-raw-2",
            "1",
            "201",
            "viewer2",
            "viewer2",
            "channelone",
            1_704_240_005_000,
            "sample gz raw line",
        ),
    );
    write_fixture(
        &old_debug,
        &tiny_debug_raw_log(&[("sample-debug-1", 1_704_326_406_000, "sample debug line")]),
    );
    write_fixture(
        &hot_raw,
        &privmsg(
            "sample-hot-1",
            "1",
            "205",
            "viewer5",
            "viewer5",
            "channelone",
            hot_timestamp,
            "hot line",
        ),
    );

    let harness = TestHarness::start_with_options(TestHarnessOptions {
        channel_ids: vec!["1".to_string()],
        start_ingest: false,
        compact_after_channel_days: 7,
        ..Default::default()
    })
    .await;

    let summary = run_import_with_hooks(
        &harness.state.store,
        &pipeline_config(&import_root),
        ImportTarget {
            channel_id: Some("1".to_string()),
            day: None,
        },
        None,
        false,
        ImportHooks::default(),
    )
    .unwrap();
    assert_eq!(summary.files_imported, 4);
    assert_eq!(summary.files_parsed_raw, 4);
    assert_eq!(summary.files_parsed_simple, 0);
    assert_eq!(summary.files_parsed_json, 0);
    assert_eq!(summary.archive_partitions_committed, 3);
    assert_eq!(summary.hot_partitions_inserted, 1);

    assert!(harness
        .state
        .store
        .read_archived_channel_segment_strict("1", 2024, 1, 2)
        .unwrap()
        .iter()
        .any(|event| event.text == "sample raw line"));
    assert!(harness
        .state
        .store
        .read_archived_channel_segment_strict("1", 2024, 1, 3)
        .unwrap()
        .iter()
        .any(|event| event.text == "sample gz raw line"));
    assert!(harness
        .state
        .store
        .read_archived_channel_segment_strict("1", 2024, 1, 4)
        .unwrap()
        .iter()
        .any(|event| event.text == "sample debug line"));
    assert!(harness
        .state
        .store
        .read_channel_logs("1", hot_date.year(), hot_date.month(), hot_date.day())
        .unwrap()
        .iter()
        .any(|event| event.text == "hot line"));

    assert_eq!(harness.state.store.event_count().unwrap(), 1);
    assert!(!old_raw.exists());
    assert!(!old_raw_gz.exists());
    assert!(!old_debug.exists());
    assert!(!hot_raw.exists());

}

#[tokio::test]
async fn real_filesystem_import_rewrites_shared_archived_day_once() {
    let _guard = env_lock().lock().await;
    let temp = TempDir::new().unwrap();
    let import_root = temp.path().join("imports");
    let archived_day = ChannelDayKey {
        channel_id: "1".to_string(),
        year: 2024,
        month: 1,
        day: 2,
    };
    let first = import_root.join("raw/a/1/2024/1/2/channel.txt");
    let second = import_root.join("raw/b/1/2024/1/2/channel.txt.gz");

    write_fixture(
        &first,
        &privmsg(
            "real-shared-day-1",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_153_604_000,
            "real shared raw",
        ),
    );
    write_fixture(
        &second,
        &privmsg(
            "real-shared-day-2",
            "1",
            "201",
            "viewer2",
            "viewer2",
            "channelone",
            1_704_153_605_000,
            "real shared raw gz",
        ),
    );

    let harness = TestHarness::start_with_options(TestHarnessOptions {
        channel_ids: vec!["1".to_string()],
        start_ingest: false,
        compact_after_channel_days: 7,
        ..Default::default()
    })
    .await;
    let archive = Arc::new(CountingRealArchive::new());

    let summary = run_import_with_hooks(
        &harness.state.store,
        &pipeline_config(&import_root),
        ImportTarget {
            channel_id: Some("1".to_string()),
            day: None,
        },
        None,
        false,
        ImportHooks {
            archive: Some(archive.clone()),
            ..ImportHooks::default()
        },
    )
    .unwrap();

    assert_eq!(summary.files_imported, 2);
    assert_eq!(summary.archive_partitions_committed, 1);
    assert_eq!(archive.channel_write_successes(&archived_day), 1);
    let archived = harness
        .state
        .store
        .read_archived_channel_segment_strict("1", 2024, 1, 2)
        .unwrap();
    assert_eq!(archived.len(), 2);
    assert!(archived.iter().any(|event| event.text == "real shared raw"));
    assert!(archived.iter().any(|event| event.text == "real shared raw gz"));
}

#[tokio::test]
#[ignore = "20k-file stress test; run explicitly when profiling import throughput"]
async fn mock_fs_large_generated_import_uses_parallel_parse_and_serial_archive_writes() {
    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let root = PathBuf::from(r"C:\mock\imports");
    let archive_root = PathBuf::from(r"C:\mock\archive");
    let io = Arc::new(MockFs::new(root.clone(), 4));
    let archive = Arc::new(MockArchive::new(io.clone(), archive_root));
    let metrics = Arc::new(ImportMetrics::default());
    let hot_base = (Utc::now() - Duration::days(2)).date_naive();
    let parse_subset = 120usize;

    for index in 0..20_000usize {
        let old = index % 3 == 0;
        let hot_date = hot_base + Duration::days((index % 2) as i64);
        let bucket = index % 64;
        let timestamp = if old {
            1_704_153_600_000 + index as i64
        } else {
            Utc.with_ymd_and_hms(
                hot_date.year(),
                hot_date.month(),
                hot_date.day(),
                0,
                0,
                1,
            )
            .unwrap()
            .timestamp_millis()
                + index as i64
        };
        let path = match index % 4 {
            0 => {
                if old {
                    root.join(format!("raw/{bucket}/1/2024/1/{}/file-{index}.txt", 2 + (index % 3) as u32))
                } else {
                    root.join(format!(
                        "raw/{bucket}/1/{}/{}/{}/file-{index}.txt",
                        hot_date.year(),
                        hot_date.month(),
                        hot_date.day()
                    ))
                }
            }
            1 => {
                if old {
                    root.join(format!("raw/{bucket}/1/2024/1/{}/file-{index}.txt.gz", 2 + (index % 3) as u32))
                } else {
                    root.join(format!(
                        "raw/{bucket}/1/{}/{}/{}/file-{index}.txt.gz",
                        hot_date.year(),
                        hot_date.month(),
                        hot_date.day()
                    ))
                }
            }
            2 => {
                if old {
                    root.join(format!("simple/{bucket}/{index}/1/2024/1/{}.txt", 2 + (index % 3) as u32))
                } else {
                    root.join(format!(
                        "simple/{bucket}/{index}/1/{}/{}/{}.txt",
                        hot_date.year(),
                        hot_date.month(),
                        hot_date.day()
                    ))
                }
            }
            _ => {
                if old {
                    root.join(format!("json/{bucket}/{index}/1/2024/1/{}.json.gz", 2 + (index % 3) as u32))
                } else {
                    root.join(format!(
                        "json/{bucket}/{index}/1/{}/{}/{}.json.gz",
                        hot_date.year(),
                        hot_date.month(),
                        hot_date.day()
                    ))
                }
            }
        };
        let content = match index % 4 {
            0 | 1 => privmsg(
                &format!("bulk-raw-{index}"),
                "1",
                "200",
                "viewer",
                "viewer",
                "channelone",
                timestamp,
                &format!("bulk raw {index}"),
            ),
            2 => tiny_simple_text(&[("00:01", &format!("Viewer {index}: bulk simple {index}"))]),
            _ => tiny_json_export(
                if old { 2 + (index % 3) as u32 } else { hot_date.day() },
                &[(
                    &format!("bulk-json-{index}"),
                    &format!("Viewer {index}"),
                    &format!("viewer{index}"),
                    &format!("bulk json {index}"),
                )],
            ),
        };
        let bytes = if path.extension().and_then(|ext| ext.to_str()) == Some("gz") {
            gzip_bytes_fast(&content)
        } else {
            content.into_bytes()
        };
        io.add_file(path, bytes);
        if index >= parse_subset {
            let family = match index % 4 {
                0 | 1 => "raw",
                2 => "simple_text",
                _ => "json_export",
            };
            let path_key = match index % 4 {
                0 => {
                    if old {
                        root.join(format!("raw/{bucket}/1/2024/1/{}/file-{index}.txt", 2 + (index % 3) as u32))
                    } else {
                        root.join(format!(
                            "raw/{bucket}/1/{}/{}/{}/file-{index}.txt",
                            hot_date.year(),
                            hot_date.month(),
                            hot_date.day()
                        ))
                    }
                }
                1 => {
                    if old {
                        root.join(format!("raw/{bucket}/1/2024/1/{}/file-{index}.txt.gz", 2 + (index % 3) as u32))
                    } else {
                        root.join(format!(
                            "raw/{bucket}/1/{}/{}/{}/file-{index}.txt.gz",
                            hot_date.year(),
                            hot_date.month(),
                            hot_date.day()
                        ))
                    }
                }
                2 => {
                    if old {
                        root.join(format!("simple/{bucket}/{index}/1/2024/1/{}.txt", 2 + (index % 3) as u32))
                    } else {
                        root.join(format!(
                            "simple/{bucket}/{index}/1/{}/{}/{}.txt",
                            hot_date.year(),
                            hot_date.month(),
                            hot_date.day()
                        ))
                    }
                }
                _ => {
                    if old {
                        root.join(format!("json/{bucket}/{index}/1/2024/1/{}.json.gz", 2 + (index % 3) as u32))
                    } else {
                        root.join(format!(
                            "json/{bucket}/{index}/1/{}/{}/{}.json.gz",
                            hot_date.year(),
                            hot_date.month(),
                            hot_date.day()
                        ))
                    }
                }
            };
            let fingerprint = io.fingerprint(&path_key).unwrap();
            harness
                .state
                .store
                .upsert_import_file_state(
                    0,
                    &path_key.to_string_lossy(),
                    &fingerprint,
                    family,
                    "done",
                    None,
                )
                .unwrap();
        }
    }

    let summary = run_import_with_hooks(
        &harness.state.store,
        &ImportPipelineConfig {
            import_folder: root.clone(),
            mode: LegacyTxtMode::MissingOnly,
            delete_raw_after_import: false,
            delete_current_raw_on_discovery: false,
            delete_reconstructed_after_import: false,
            delete_current_reconstructed_on_discovery: false,
        },
        ImportTarget {
            channel_id: Some("1".to_string()),
            day: None,
        },
        None,
        false,
        ImportHooks {
            io: Some(io.clone()),
            archive: Some(archive.clone()),
            instrumentation: Some(metrics.clone()),
            discovery_queue_capacity: Some(256),
            parse_queue_capacity: Some(8),
        },
    )
    .unwrap();

    assert_eq!(summary.files_current, 20_000 - parse_subset);
    assert_eq!(summary.files_imported, parse_subset);
    assert_eq!(summary.files_failed, 0);
    assert!(summary.files_parsed_raw > 0);
    assert!(summary.files_parsed_simple > 0);
    assert!(summary.files_parsed_json > 0);
    assert!(summary.archive_partitions_committed > 0);
    assert!(summary.hot_partitions_inserted > 0);

    assert!(metrics.max_parse_active() > 1, "expected parser parallelism");
    assert!(metrics.discovered_after_parse_started.load(Ordering::SeqCst) > 0);
    assert!(metrics.max_commit_active() >= 1);
    assert!(!archive.same_key_overlap_detected.load(Ordering::SeqCst));
    assert!(io.max_active_reads.load(Ordering::SeqCst) > 1);

    let hot_events = harness.state.store.event_count().unwrap();
    let archived_events = archive.channel_event_count();
    assert!(hot_events > 0);
    assert!(archived_events > 0);
    assert_eq!(hot_events + archived_events as i64, parse_subset as i64);
    assert_eq!(
        archive.total_channel_write_successes(),
        summary.archive_partitions_committed
    );
    assert!(archive.total_user_write_successes() > 0);
    assert!(archive.total_user_write_successes() <= summary.affected_user_months);
}

#[tokio::test]
async fn mock_fs_shared_archived_day_commits_once_across_multiple_files() {
    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let root = PathBuf::from(r"C:\mock\shared-day");
    let archive_root = PathBuf::from(r"C:\mock\shared-day-archive");
    let io = Arc::new(MockFs::new(root.clone(), 0));
    let archive = Arc::new(MockArchive::new(io.clone(), archive_root));
    let day_key = ChannelDayKey {
        channel_id: "1".to_string(),
        year: 2024,
        month: 1,
        day: 2,
    };
    let user_key = UserMonthKey {
        channel_id: "1".to_string(),
        user_id: "200".to_string(),
        year: 2024,
        month: 1,
    };

    io.add_file(
        root.join("raw/a/1/2024/1/2/one.txt"),
        privmsg(
            "shared-day-raw-1",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_153_604_000,
            "shared archive raw",
        )
        .into_bytes(),
    );
    io.add_file(
        root.join("simple/b/1/2024/1/2.txt"),
        tiny_simple_text(&[("00:00:05", "Viewer: shared archive simple")]).into_bytes(),
    );
    io.add_file(
        root.join("json/c/1/2024/1/2.json.gz"),
        gzip_bytes_fast(&tiny_json_export(
            2,
            &[(
                "shared-day-json-1",
                "Viewer Json",
                "viewerjson",
                "shared archive json",
            )],
        )),
    );

    let summary = run_mock_import(
        &harness.state.store,
        &root,
        io.clone(),
        archive.clone(),
        Arc::new(ImportMetrics::default()),
        None,
    );

    assert_eq!(summary.files_imported, 3);
    assert_eq!(summary.archive_partitions_committed, 1);
    assert_eq!(archive.channel_write_successes(&day_key), 1);
    assert_eq!(archive.channel_write_attempts(&day_key), 1);
    assert_eq!(archive.user_write_successes(&user_key), 1);
    assert!(!archive.same_key_overlap_detected.load(Ordering::SeqCst));
    assert_eq!(io.replace_count(&archive.channel_final_path(&day_key)), 1);
    assert_eq!(
        io.write_count(&archive.channel_final_path(&day_key).with_extension("tmp.br")),
        1
    );
    let contents = archive.decode_channel_segment(&day_key);
    assert!(contents.contains("shared archive raw"));
    assert!(contents.contains("shared archive simple"));
    assert!(contents.contains("shared archive json"));
}

#[tokio::test]
async fn mock_fs_resume_after_parse_failure_imports_remaining_files_without_duplicates() {
    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let root = PathBuf::from(r"C:\mock\parse-failure");
    let archive_root = PathBuf::from(r"C:\mock\parse-archive");
    let io = Arc::new(MockFs::new(root.clone(), 0));
    let archive = Arc::new(MockArchive::new(io.clone(), archive_root));
    let metrics = Arc::new(ImportMetrics::default());
    let hot_date = (Utc::now() - Duration::days(1)).date_naive();
    let hot_timestamp = Utc
        .with_ymd_and_hms(hot_date.year(), hot_date.month(), hot_date.day(), 0, 0, 1)
        .unwrap()
        .timestamp_millis();

    let failing = root.join(format!(
        "raw/1/{}/{}/{}/failing.txt",
        hot_date.year(),
        hot_date.month(),
        hot_date.day()
    ));
    let good = root.join(format!(
        "simple/1/{}/{}/{}.txt",
        hot_date.year(),
        hot_date.month(),
        hot_date.day()
    ));
    io.add_file(
        failing.clone(),
        privmsg(
            "parse-failure-1",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            hot_timestamp,
            "failing first run",
        )
        .into_bytes(),
    );
    io.add_file(
        good.clone(),
        tiny_simple_text(&[("00:03", "Viewer Good: good first run")]).into_bytes(),
    );
    io.fail_read_on_attempt(&failing, 2);

    let first = run_mock_import(
        &harness.state.store,
        &root,
        io.clone(),
        archive.clone(),
        metrics.clone(),
        None,
    );
    assert_eq!(first.files_failed, 1);
    assert_eq!(first.files_imported, 1);
    assert_eq!(harness.state.store.event_count().unwrap(), 1);
    assert!(io.file_exists(&failing));
    assert!(!io.file_exists(&good));

    let second = run_mock_import(
        &harness.state.store,
        &root,
        io.clone(),
        archive.clone(),
        Arc::new(ImportMetrics::default()),
        None,
    );
    assert_eq!(second.files_failed, 0);
    assert_eq!(second.files_imported, 1);
    assert_eq!(harness.state.store.event_count().unwrap(), 2);
    assert!(!io.file_exists(&failing));
}

#[tokio::test]
async fn mock_fs_retries_source_deletion_after_successful_commit() {
    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let root = PathBuf::from(r"C:\mock\delete-failure");
    let archive_root = PathBuf::from(r"C:\mock\delete-archive");
    let io = Arc::new(MockFs::new(root.clone(), 0));
    let archive = Arc::new(MockArchive::new(io.clone(), archive_root));
    let hot_date = (Utc::now() - Duration::days(1)).date_naive();
    let hot_timestamp = Utc
        .with_ymd_and_hms(hot_date.year(), hot_date.month(), hot_date.day(), 0, 0, 1)
        .unwrap()
        .timestamp_millis();
    let file = root.join(format!(
        "raw/1/{}/{}/{}/channel.txt",
        hot_date.year(),
        hot_date.month(),
        hot_date.day()
    ));
    io.add_file(
        file.clone(),
        privmsg(
            "delete-failure-1",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            hot_timestamp,
            "delete retried",
        )
        .into_bytes(),
    );
    io.fail_once("remove", &file);

    let first = run_mock_import(
        &harness.state.store,
        &root,
        io.clone(),
        archive.clone(),
        Arc::new(ImportMetrics::default()),
        None,
    );
    assert_eq!(first.files_failed, 1);
    assert_eq!(harness.state.store.event_count().unwrap(), 1);
    assert!(io.file_exists(&file));

    let second = run_mock_import(
        &harness.state.store,
        &root,
        io.clone(),
        archive,
        Arc::new(ImportMetrics::default()),
        None,
    );
    assert_eq!(second.files_failed, 0);
    assert_eq!(harness.state.store.event_count().unwrap(), 1);
    assert!(!io.file_exists(&file));
    assert_eq!(harness.state.store.event_count().unwrap(), 1);
}

#[tokio::test]
async fn mock_fs_mixed_sample_data_writes_archive_segments_and_hot_storage() {
    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let root = PathBuf::from(r"C:\mock\mixed-samples");
    let archive_root = PathBuf::from(r"C:\mock\mixed-samples-archive");
    let io = Arc::new(MockFs::new(root.clone(), 0));
    let archive = Arc::new(MockArchive::new(io.clone(), archive_root));
    let metrics = Arc::new(ImportMetrics::default());
    let hot_date = (Utc::now() - Duration::days(1)).date_naive();

    let old_raw = root.join("raw/1/2024/1/2/channel.txt");
    let old_simple = root.join("simple/1/2024/1/3.txt");
    let old_json = root.join("json/1/2024/1/4.json.gz");
    let hot_raw = root.join(format!(
        "raw/1/{}/{}/{}/channel.txt",
        hot_date.year(),
        hot_date.month(),
        hot_date.day()
    ));

    io.add_file(
        old_raw.clone(),
        privmsg(
            "mock-family-raw-1",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_153_604_000,
            "mock archived raw",
        )
        .into_bytes(),
    );
    io.add_file(
        old_simple.clone(),
        tiny_simple_text(&[("00:01:02", "Viewer Simple: mock archived simple")]).into_bytes(),
    );
    io.add_file(
        old_json.clone(),
        gzip_bytes_fast(&tiny_json_export(
            4,
            &[(
                "mock-family-json-1",
                "Viewer Json",
                "viewerjson",
                "mock archived json",
            )],
        )),
    );
    io.add_file(
        hot_raw.clone(),
        privmsg(
            "mock-family-hot-1",
            "1",
            "201",
            "viewerhot",
            "viewerhot",
            "channelone",
            Utc.with_ymd_and_hms(
                hot_date.year(),
                hot_date.month(),
                hot_date.day(),
                0,
                0,
                9,
            )
            .unwrap()
            .timestamp_millis(),
            "mock hot raw",
        )
        .into_bytes(),
    );

    let summary = run_mock_import(
        &harness.state.store,
        &root,
        io.clone(),
        archive.clone(),
        metrics,
        None,
    );

    assert_eq!(summary.files_imported, 4);
    assert_eq!(summary.files_parsed_raw, 2);
    assert_eq!(summary.files_parsed_simple, 1);
    assert_eq!(summary.files_parsed_json, 1);
    assert_eq!(summary.archive_partitions_committed, 3);
    assert_eq!(summary.hot_partitions_inserted, 1);
    assert_eq!(harness.state.store.event_count().unwrap(), 1);
    assert_eq!(archive.channel_event_count(), 3);
    assert_eq!(io.source_file_count(), 0);

    let raw_day = ChannelDayKey {
        channel_id: "1".to_string(),
        year: 2024,
        month: 1,
        day: 2,
    };
    let simple_day = ChannelDayKey {
        channel_id: "1".to_string(),
        year: 2024,
        month: 1,
        day: 3,
    };
    let json_day = ChannelDayKey {
        channel_id: "1".to_string(),
        year: 2024,
        month: 1,
        day: 4,
    };
    assert!(archive.decode_channel_segment(&raw_day).contains("mock archived raw"));
    assert!(archive
        .decode_channel_segment(&simple_day)
        .contains("mock archived simple"));
    assert!(archive
        .decode_channel_segment(&json_day)
        .contains("mock archived json"));
    assert!(io.file_exists(&archive.channel_final_path(&raw_day)));
    assert!(io.file_exists(&archive.channel_final_path(&simple_day)));
    assert!(io.file_exists(&archive.channel_final_path(&json_day)));
    assert_eq!(archive.channel_write_successes(&raw_day), 1);
    assert_eq!(archive.channel_write_successes(&simple_day), 1);
    assert_eq!(archive.channel_write_successes(&json_day), 1);
    assert!(!io.file_exists(&old_raw));
    assert!(!io.file_exists(&old_simple));
    assert!(!io.file_exists(&old_json));
    assert!(!io.file_exists(&hot_raw));
}

#[tokio::test]
async fn mock_fs_retries_archive_replace_failure_without_losing_source_data() {
    let harness = TestHarness::start_without_ingest(vec!["1".to_string()]).await;
    let root = PathBuf::from(r"C:\mock\replace-failure");
    let archive_root = PathBuf::from(r"C:\mock\replace-failure-archive");
    let io = Arc::new(MockFs::new(root.clone(), 0));
    let archive = Arc::new(MockArchive::new(io.clone(), archive_root));
    let archived_file = root.join("raw/1/2024/1/2/channel.txt");
    let day_key = ChannelDayKey {
        channel_id: "1".to_string(),
        year: 2024,
        month: 1,
        day: 2,
    };
    let final_path = archive.channel_final_path(&day_key);
    let temp_path = final_path.with_extension("tmp.br");

    io.add_file(
        archived_file.clone(),
        privmsg(
            "replace-failure-1",
            "1",
            "200",
            "viewer",
            "viewer",
            "channelone",
            1_704_153_604_000,
            "replace archive payload",
        )
        .into_bytes(),
    );
    io.fail_once("replace", &final_path);

    let first = run_mock_import(
        &harness.state.store,
        &root,
        io.clone(),
        archive.clone(),
        Arc::new(ImportMetrics::default()),
        None,
    );
    assert_eq!(first.files_imported, 0);
    assert_eq!(first.files_failed, 1);
    assert!(io.file_exists(&archived_file));
    assert!(io.file_exists(&temp_path));
    assert!(!io.file_exists(&final_path));
    assert_eq!(archive.channel_event_count(), 0);
    assert_eq!(archive.channel_write_attempts(&day_key), 1);
    assert_eq!(archive.channel_write_successes(&day_key), 0);
    assert_eq!(io.write_count(&temp_path), 1);
    assert_eq!(io.replace_count(&final_path), 0);

    let second = run_mock_import(
        &harness.state.store,
        &root,
        io.clone(),
        archive.clone(),
        Arc::new(ImportMetrics::default()),
        None,
    );
    assert_eq!(second.files_failed, 0);
    assert_eq!(second.files_imported, 1);
    assert!(!io.file_exists(&archived_file));
    assert!(!io.file_exists(&temp_path));
    assert!(io.file_exists(&final_path));
    assert!(archive
        .decode_channel_segment(&day_key)
        .contains("replace archive payload"));
    assert_eq!(archive.channel_event_count(), 1);
    assert_eq!(archive.channel_write_attempts(&day_key), 2);
    assert_eq!(archive.channel_write_successes(&day_key), 1);
    assert_eq!(io.write_count(&temp_path), 2);
    assert_eq!(io.replace_count(&final_path), 1);
}
