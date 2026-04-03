use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread;
use std::time::Instant;
use std::{
    cell::Cell,
    ops::{Deref, DerefMut},
};

use anyhow::{Context, Result, anyhow};
use brotli::{CompressorWriter, Decompressor};
use chrono::{DateTime, Datelike, Duration, TimeZone, Utc};
use rand::Rng;
use rusqlite::{Connection, OptionalExtension, params};
use rusqlite::{ToSql, params_from_iter};

use crate::config::Config;
use crate::debug_sync::RECONCILIATION_DELAY_SECONDS;
use crate::model::{
    CanonicalEvent, ChannelDayKey, ChannelLogFile, ChannelPartitionSummary, SegmentRecord,
    StoredEvent, UserLogFile, UserMonthKey, UserPartitionSummary,
};

thread_local! {
    static STORE_DB_LOCK_DEPTH: Cell<usize> = const { Cell::new(0) };
    static STORE_IMPORT_DB_LOCK_DEPTH: Cell<usize> = const { Cell::new(0) };
    static STORE_DB_PRIORITY_DEPTH: Cell<usize> = const { Cell::new(0) };
}

#[derive(Clone)]
pub struct Store {
    db: Arc<Mutex<Connection>>,
    import_db: Arc<Mutex<Connection>>,
    root_dir: PathBuf,
    sqlite_path: PathBuf,
    archive_enabled: bool,
    compact_after_channel_days: i64,
    compact_after_user_months: i64,
    compression_executor: Arc<CompressionExecutor>,
    compression_paths: Arc<CompressionPathLocks>,
    db_priority: Arc<DbPriorityScheduler>,
    shutdown_requested: Arc<AtomicBool>,
}

struct StoreDbGuard<'a> {
    guard: MutexGuard<'a, Connection>,
    depth: &'static std::thread::LocalKey<Cell<usize>>,
    _priority: Option<DbPriorityGuard>,
}

impl Deref for StoreDbGuard<'_> {
    type Target = Connection;

    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl DerefMut for StoreDbGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl Drop for StoreDbGuard<'_> {
    fn drop(&mut self) {
        self.depth.with(|depth| {
            let current = depth.get();
            debug_assert!(current > 0, "store db lock depth underflow");
            depth.set(current.saturating_sub(1));
        });
    }
}

#[derive(Debug, Clone)]
pub struct RawResponsePlan {
    pub segment_path: Option<PathBuf>,
    pub events: Vec<StoredEvent>,
}

#[derive(Debug, Clone, Default)]
pub struct InsertEventsBatchOutcome {
    pub inserted: usize,
    pub skipped: usize,
    pub affected_channel_days: BTreeSet<ChannelDayKey>,
    pub affected_user_months: BTreeSet<UserMonthKey>,
}

#[derive(Debug, Clone, Default)]
pub struct IndexedInsertEventsBatchOutcome {
    pub totals: InsertEventsBatchOutcome,
    pub per_file: HashMap<usize, InsertEventsBatchOutcome>,
}

#[derive(Debug, Clone)]
pub struct ReconciliationJob {
    pub id: i64,
    pub channel_id: String,
    pub year: i32,
    pub month: u32,
    pub day: u32,
    pub segment_path: String,
    pub scheduled_at: DateTime<Utc>,
    pub checked_at: Option<DateTime<Utc>>,
    pub status: String,
    pub conflict_count: i64,
    pub repair_status: String,
    pub unhealthy: i64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ReconciliationOutcome {
    pub checked_at: DateTime<Utc>,
    pub status: String,
    pub conflict_count: i64,
    pub repair_status: String,
    pub unhealthy: i64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DatabaseSizeStats {
    pub page_size: i64,
    pub page_count: i64,
    pub freelist_count: i64,
    pub live_pages: i64,
    pub main_bytes: u64,
    pub free_bytes: u64,
    pub live_bytes: u64,
    pub wal_bytes: u64,
}

const DB_RECLAIM_INCREMENTAL_FREE_BYTES: u64 = 4 * 1024 * 1024;
const DB_RECLAIM_FULL_VACUUM_FREE_BYTES: u64 = 16 * 1024 * 1024;
const DB_RECLAIM_FREE_RATIO_NUMERATOR: u64 = 1;
const DB_RECLAIM_FREE_RATIO_DENOMINATOR: u64 = 5;

impl CompressionExecutor {
    fn new(worker_count: usize, quality: u32, lgwin: u32) -> Self {
        let bounded = worker_count.max(1);
        let (sender, receiver) = mpsc::sync_channel::<CompressionRequest>(bounded * 2);
        let shared_receiver = Arc::new(Mutex::new(receiver));
        for index in 0..bounded {
            let receiver = Arc::clone(&shared_receiver);
            thread::Builder::new()
                .name(format!("justlog-compress-{index}"))
                .spawn(move || compression_worker_loop(receiver, quality, lgwin))
                .expect("failed to spawn compression worker");
        }
        Self { sender }
    }

    fn submit(
        &self,
        temp_path: PathBuf,
        raws: Vec<String>,
    ) -> Receiver<Result<CompressionResult, String>> {
        let (response_tx, response_rx) = mpsc::sync_channel(1);
        self.sender
            .send(CompressionRequest {
                temp_path,
                raws,
                response: response_tx,
            })
            .expect("compression executor stopped unexpectedly");
        response_rx
    }
}

impl CompressionPathLocks {
    fn acquire(self: &Arc<Self>, path: PathBuf) -> CompressionPathGuard {
        let mut locked = self.paths.lock().unwrap();
        while locked.contains(&path) {
            locked = self.ready.wait(locked).unwrap();
        }
        locked.insert(path.clone());
        CompressionPathGuard {
            locks: Arc::clone(self),
            path,
        }
    }
}

impl Drop for CompressionPathGuard {
    fn drop(&mut self) {
        let mut locked = self.locks.paths.lock().unwrap();
        locked.remove(&self.path);
        self.locks.ready.notify_all();
    }
}

#[derive(Debug)]
struct CompressionRequest {
    temp_path: PathBuf,
    raws: Vec<String>,
    response: SyncSender<Result<CompressionResult, String>>,
}

#[derive(Debug)]
struct CompressionResult {
    temp_path: PathBuf,
    output_bytes: u64,
    elapsed: std::time::Duration,
}

#[derive(Debug)]
struct CompressionExecutor {
    sender: SyncSender<CompressionRequest>,
}

#[derive(Debug, Default)]
struct CompressionPathLocks {
    paths: Mutex<BTreeSet<PathBuf>>,
    ready: Condvar,
}

#[derive(Debug)]
struct CompressionPathGuard {
    locks: Arc<CompressionPathLocks>,
    path: PathBuf,
}

#[derive(Debug)]
struct PreparedSegmentWrite {
    final_path: PathBuf,
    relative_path: String,
    line_count: usize,
    start_ts: i64,
    end_ts: i64,
    response: Receiver<Result<CompressionResult, String>>,
    _path_guard: CompressionPathGuard,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DbWorkPriority {
    Live,
    Import,
}

#[derive(Debug, Default)]
struct DbPriorityState {
    pending_live: usize,
    active_live: usize,
    waiting_import: usize,
    active_import: usize,
}

#[derive(Debug, Default)]
struct DbPriorityScheduler {
    state: Mutex<DbPriorityState>,
    ready: Condvar,
}

#[derive(Debug)]
struct DbPriorityGuard {
    scheduler: Arc<DbPriorityScheduler>,
    priority: DbWorkPriority,
    active: bool,
}

impl Drop for DbPriorityGuard {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let mut state = self.scheduler.state.lock().unwrap();
        match self.priority {
            DbWorkPriority::Live => {
                state.active_live = state.active_live.saturating_sub(1);
            }
            DbWorkPriority::Import => {
                state.active_import = state.active_import.saturating_sub(1);
            }
        }
        self.scheduler.ready.notify_all();
        STORE_DB_PRIORITY_DEPTH.with(|depth| {
            let current = depth.get();
            debug_assert!(current > 0, "store db priority depth underflow");
            depth.set(current.saturating_sub(1));
        });
    }
}

impl Store {
    fn enter_db_priority(
        &self,
        priority: DbWorkPriority,
        label: &'static str,
    ) -> Option<DbPriorityGuard> {
        let already_inside = STORE_DB_PRIORITY_DEPTH.with(|depth| {
            let current = depth.get();
            if current > 0 {
                true
            } else {
                depth.set(current + 1);
                false
            }
        });
        if already_inside {
            return None;
        }

        let mut state = self.db_priority.state.lock().unwrap();
        match priority {
            DbWorkPriority::Live => {
                state.pending_live += 1;
                if state.waiting_import > 0 || state.active_import > 0 {
                    tracing::info!(
                        "Live DB work preempted pending import work: {label} (waiting_import={}, active_import={})",
                        state.waiting_import,
                        state.active_import
                    );
                }
                state.pending_live = state.pending_live.saturating_sub(1);
                state.active_live += 1;
            }
            DbWorkPriority::Import => {
                state.waiting_import += 1;
                let mut logged_wait = false;
                while state.pending_live > 0 || state.active_live > 0 || state.active_import > 0 {
                    if !logged_wait {
                        tracing::info!(
                            "Import batch waiting for low-priority DB slot: {label} (pending_live={}, active_live={}, active_import={})",
                            state.pending_live,
                            state.active_live,
                            state.active_import
                        );
                        logged_wait = true;
                    }
                    state = self.db_priority.ready.wait(state).unwrap();
                }
                state.waiting_import = state.waiting_import.saturating_sub(1);
                state.active_import += 1;
                if logged_wait {
                    tracing::info!(
                        "Import batch started after waiting for low-priority DB slot: {label}"
                    );
                }
            }
        }

        Some(DbPriorityGuard {
            scheduler: Arc::clone(&self.db_priority),
            priority,
            active: true,
        })
    }

    fn lock_db(&self) -> StoreDbGuard<'_> {
        self.lock_db_with_priority(DbWorkPriority::Live, "live db")
    }

    fn lock_db_with_priority(
        &self,
        priority: DbWorkPriority,
        label: &'static str,
    ) -> StoreDbGuard<'_> {
        let priority_guard = self.enter_db_priority(priority, label);
        STORE_DB_LOCK_DEPTH.with(|depth| {
            assert!(
                depth.get() == 0,
                "re-entrant Store DB lock on the same thread; drop the outer db guard before calling another Store method"
            );
        });
        let guard = self.db.lock().unwrap();
        STORE_DB_LOCK_DEPTH.with(|depth| depth.set(depth.get() + 1));
        StoreDbGuard {
            guard,
            depth: &STORE_DB_LOCK_DEPTH,
            _priority: priority_guard,
        }
    }

    fn lock_import_db(&self) -> StoreDbGuard<'_> {
        self.lock_import_db_with_priority(DbWorkPriority::Live, "live import db")
    }

    fn lock_import_db_low_priority(&self, label: &'static str) -> StoreDbGuard<'_> {
        self.lock_import_db_with_priority(DbWorkPriority::Import, label)
    }

    fn lock_import_db_with_priority(
        &self,
        priority: DbWorkPriority,
        label: &'static str,
    ) -> StoreDbGuard<'_> {
        let priority_guard = self.enter_db_priority(priority, label);
        STORE_IMPORT_DB_LOCK_DEPTH.with(|depth| {
            assert!(
                depth.get() == 0,
                "re-entrant Store import DB lock on the same thread; drop the outer import db guard before calling another Store method"
            );
        });
        let guard = self.import_db.lock().unwrap();
        STORE_IMPORT_DB_LOCK_DEPTH.with(|depth| depth.set(depth.get() + 1));
        StoreDbGuard {
            guard,
            depth: &STORE_IMPORT_DB_LOCK_DEPTH,
            _priority: priority_guard,
        }
    }

    pub fn open(config: &Config) -> Result<Self> {
        fs::create_dir_all(&config.logs_directory)?;
        let connection = Connection::open(&config.storage.sqlite_path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.busy_timeout(std::time::Duration::from_secs(30))?;
        let import_connection = Connection::open(&config.storage.sqlite_path)?;
        import_connection.pragma_update(None, "journal_mode", "WAL")?;
        import_connection.pragma_update(None, "synchronous", "FULL")?;
        import_connection.busy_timeout(std::time::Duration::from_secs(30))?;
        let compression_threads = read_max_compress_threads_from_env();

        let store = Self {
            db: Arc::new(Mutex::new(connection)),
            import_db: Arc::new(Mutex::new(import_connection)),
            root_dir: config.logs_directory.clone(),
            sqlite_path: config.storage.sqlite_path.clone(),
            archive_enabled: config.archive,
            compact_after_channel_days: config.storage.compact_after_channel_days,
            compact_after_user_months: config.storage.compact_after_user_months,
            compression_executor: Arc::new(CompressionExecutor::new(
                compression_threads,
                config.compression.quality,
                config.compression.lgwin,
            )),
            compression_paths: Arc::new(CompressionPathLocks::default()),
            db_priority: Arc::new(DbPriorityScheduler::default()),
            shutdown_requested: Arc::new(AtomicBool::new(false)),
        };
        store.initialize()?;
        store.recover_pending_segments()?;
        Ok(store)
    }

    fn initialize(&self) -> Result<()> {
        let db = self.lock_db();
        db.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS events (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                event_uid TEXT NOT NULL UNIQUE,
                kind INTEGER NOT NULL,
                room_id TEXT NOT NULL,
                channel_login TEXT NOT NULL,
                username TEXT NOT NULL,
                display_name TEXT NOT NULL,
                user_id TEXT,
                target_user_id TEXT,
                text TEXT NOT NULL,
                system_text TEXT NOT NULL,
                raw TEXT NOT NULL,
                timestamp_unix INTEGER NOT NULL,
                timestamp_rfc3339 TEXT NOT NULL,
                tags_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS events_channel_time_idx
                ON events(room_id, timestamp_unix, seq);
            CREATE INDEX IF NOT EXISTS events_user_time_idx
                ON events(room_id, user_id, timestamp_unix, seq);
            CREATE INDEX IF NOT EXISTS events_target_user_time_idx
                ON events(room_id, target_user_id, timestamp_unix, seq);

            CREATE TABLE IF NOT EXISTS segments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                user_id TEXT,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                day INTEGER,
                path TEXT NOT NULL UNIQUE,
                line_count INTEGER NOT NULL,
                start_ts INTEGER NOT NULL,
                end_ts INTEGER NOT NULL,
                compression TEXT NOT NULL,
                passthrough_raw INTEGER NOT NULL
            );

            CREATE UNIQUE INDEX IF NOT EXISTS segments_scope_partition_idx
                ON segments(scope, channel_id, COALESCE(user_id, ''), year, month, COALESCE(day, 0));

            CREATE TABLE IF NOT EXISTS pending_segments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                user_id TEXT,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                day INTEGER,
                temp_path TEXT NOT NULL,
                final_path TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS reconciliation_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id TEXT NOT NULL,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                day INTEGER NOT NULL,
                segment_path TEXT NOT NULL,
                scheduled_at INTEGER NOT NULL,
                checked_at INTEGER,
                status TEXT NOT NULL DEFAULT 'pending',
                conflict_count INTEGER NOT NULL DEFAULT 0,
                repair_status TEXT NOT NULL DEFAULT 'none',
                unhealthy INTEGER NOT NULL DEFAULT 0,
                last_error TEXT
            );

            CREATE UNIQUE INDEX IF NOT EXISTS reconciliation_jobs_partition_idx
                ON reconciliation_jobs(channel_id, year, month, day);

            CREATE TABLE IF NOT EXISTS imported_raw_files (
                path TEXT PRIMARY KEY,
                fingerprint TEXT NOT NULL,
                imported_at INTEGER NOT NULL,
                status TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS imported_reconstructed_files (
                path TEXT PRIMARY KEY,
                fingerprint TEXT NOT NULL,
                imported_at INTEGER NOT NULL,
                status TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS import_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                completed_at INTEGER,
                status TEXT NOT NULL,
                channel_filter TEXT,
                limit_files INTEGER,
                dry_run INTEGER NOT NULL DEFAULT 0,
                directories_visited INTEGER NOT NULL DEFAULT 0,
                files_discovered INTEGER NOT NULL DEFAULT 0,
                files_completed INTEGER NOT NULL DEFAULT 0,
                files_failed INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS import_files (
                path TEXT PRIMARY KEY,
                run_id INTEGER,
                fingerprint TEXT NOT NULL,
                kind TEXT NOT NULL,
                state TEXT NOT NULL,
                discovered_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                last_error TEXT
            );

            CREATE TABLE IF NOT EXISTS import_partition_outputs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scope TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                user_id TEXT,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                day INTEGER,
                state TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                source_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT
            );

            CREATE TABLE IF NOT EXISTS import_file_partitions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT NOT NULL,
                scope TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                user_id TEXT,
                year INTEGER NOT NULL,
                month INTEGER NOT NULL,
                day INTEGER
            );

            CREATE TABLE IF NOT EXISTS import_checkpoints (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                run_id INTEGER,
                updated_at INTEGER NOT NULL,
                directories_visited INTEGER NOT NULL DEFAULT 0,
                files_discovered INTEGER NOT NULL DEFAULT 0,
                last_path TEXT
            );

            CREATE UNIQUE INDEX IF NOT EXISTS import_partition_outputs_partition_idx
                ON import_partition_outputs(scope, channel_id, COALESCE(user_id, ''), year, month, COALESCE(day, 0));

            CREATE UNIQUE INDEX IF NOT EXISTS import_file_partitions_unique_idx
                ON import_file_partitions(path, scope, channel_id, COALESCE(user_id, ''), year, month, COALESCE(day, 0));
            "#,
        )?;
        Ok(())
    }

    pub fn insert_event(&self, event: &CanonicalEvent) -> Result<bool> {
        let db = self.lock_db();
        let inserted = db.execute(
            r#"
            INSERT OR IGNORE INTO events(
                event_uid, kind, room_id, channel_login, username, display_name, user_id,
                target_user_id, text, system_text, raw, timestamp_unix, timestamp_rfc3339, tags_json
            ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
            "#,
            params![
                event.event_uid,
                event.kind,
                event.room_id,
                event.channel_login,
                event.username,
                event.display_name,
                event.user_id,
                event.target_user_id,
                event.text,
                event.system_text,
                event.raw,
                event.timestamp.timestamp(),
                event.timestamp.to_rfc3339(),
                serde_json::to_string(&event.tags)?
            ],
        )?;
        Ok(inserted > 0)
    }

    pub fn insert_events_batch(
        &self,
        events: &[CanonicalEvent],
    ) -> Result<InsertEventsBatchOutcome> {
        self.insert_events_batch_inner(events, false)
    }

    pub fn insert_events_batch_low_priority(
        &self,
        events: &[CanonicalEvent],
    ) -> Result<InsertEventsBatchOutcome> {
        self.insert_events_batch_inner(events, true)
    }

    fn insert_events_batch_inner(
        &self,
        events: &[CanonicalEvent],
        low_priority: bool,
    ) -> Result<InsertEventsBatchOutcome> {
        if events.is_empty() {
            return Ok(InsertEventsBatchOutcome::default());
        }

        let mut db = if low_priority {
            self.lock_import_db_low_priority("bulk raw event batch")
        } else {
            self.lock_import_db()
        };
        let tx = db.transaction()?;
        let mut statement = tx.prepare(
            r#"
            INSERT OR IGNORE INTO events(
                event_uid, kind, room_id, channel_login, username, display_name, user_id,
                target_user_id, text, system_text, raw, timestamp_unix, timestamp_rfc3339, tags_json
            ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
            "#,
        )?;

        let mut outcome = InsertEventsBatchOutcome::default();
        for event in events {
            let inserted = statement.execute(params![
                event.event_uid,
                event.kind,
                event.room_id,
                event.channel_login,
                event.username,
                event.display_name,
                event.user_id,
                event.target_user_id,
                event.text,
                event.system_text,
                event.raw,
                event.timestamp.timestamp(),
                event.timestamp.to_rfc3339(),
                serde_json::to_string(&event.tags)?
            ])?;
            if inserted > 0 {
                outcome.inserted += 1;
                outcome
                    .affected_channel_days
                    .insert(event.channel_day_key());
                for key in event.user_month_keys() {
                    outcome.affected_user_months.insert(key);
                }
            } else {
                outcome.skipped += 1;
            }
        }
        drop(statement);
        tx.commit()?;
        Ok(outcome)
    }

    pub fn insert_indexed_events_batch(
        &self,
        events: &[(usize, CanonicalEvent)],
    ) -> Result<IndexedInsertEventsBatchOutcome> {
        self.insert_indexed_events_batch_inner(events, false)
    }

    pub fn insert_indexed_events_batch_low_priority(
        &self,
        events: &[(usize, CanonicalEvent)],
    ) -> Result<IndexedInsertEventsBatchOutcome> {
        self.insert_indexed_events_batch_inner(events, true)
    }

    fn insert_indexed_events_batch_inner(
        &self,
        events: &[(usize, CanonicalEvent)],
        low_priority: bool,
    ) -> Result<IndexedInsertEventsBatchOutcome> {
        if events.is_empty() {
            return Ok(IndexedInsertEventsBatchOutcome::default());
        }

        let mut db = if low_priority {
            self.lock_import_db_low_priority("bulk indexed raw event batch")
        } else {
            self.lock_import_db()
        };
        let tx = db.transaction()?;
        let mut statement = tx.prepare(
            r#"
            INSERT OR IGNORE INTO events(
                event_uid, kind, room_id, channel_login, username, display_name, user_id,
                target_user_id, text, system_text, raw, timestamp_unix, timestamp_rfc3339, tags_json
            ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
            "#,
        )?;

        let mut outcome = IndexedInsertEventsBatchOutcome::default();
        for (file_index, event) in events {
            let inserted = statement.execute(params![
                event.event_uid,
                event.kind,
                event.room_id,
                event.channel_login,
                event.username,
                event.display_name,
                event.user_id,
                event.target_user_id,
                event.text,
                event.system_text,
                event.raw,
                event.timestamp.timestamp(),
                event.timestamp.to_rfc3339(),
                serde_json::to_string(&event.tags)?
            ])?;
            let file_outcome = outcome.per_file.entry(*file_index).or_default();
            if inserted > 0 {
                outcome.totals.inserted += 1;
                file_outcome.inserted += 1;
                let day_key = event.channel_day_key();
                outcome.totals.affected_channel_days.insert(day_key.clone());
                file_outcome.affected_channel_days.insert(day_key);
                for key in event.user_month_keys() {
                    outcome.totals.affected_user_months.insert(key.clone());
                    file_outcome.affected_user_months.insert(key);
                }
            } else {
                outcome.totals.skipped += 1;
                file_outcome.skipped += 1;
            }
        }
        drop(statement);
        tx.commit()?;
        Ok(outcome)
    }

    pub fn read_channel_logs(
        &self,
        channel_id: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<Vec<StoredEvent>> {
        let mut events = self.read_channel_segment(channel_id, year, month, day)?;
        events.extend(self.read_hot_channel_events(channel_id, year, month, day)?);
        events.sort_by_key(|event| (event.timestamp.timestamp(), event.seq));
        Ok(events)
    }

    pub fn channel_raw_plan(
        &self,
        channel_id: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<RawResponsePlan> {
        let segment = self.segment_for_channel_day(channel_id, year, month, day)?;
        let hot_events = self.read_hot_channel_events(channel_id, year, month, day)?;
        if hot_events.is_empty() {
            if let Some(segment) = segment {
                if segment.passthrough_raw {
                    return Ok(RawResponsePlan {
                        segment_path: Some(self.root_dir.join(segment.path)),
                        events: Vec::new(),
                    });
                }
            }
        }
        let mut events = self.read_channel_logs(channel_id, year, month, day)?;
        events.sort_by_key(|event| (event.timestamp.timestamp(), event.seq));
        Ok(RawResponsePlan {
            segment_path: None,
            events,
        })
    }

    pub fn read_user_logs(
        &self,
        channel_id: &str,
        user_id: &str,
        year: i32,
        month: u32,
    ) -> Result<Vec<StoredEvent>> {
        let mut events = self.read_user_segment(channel_id, user_id, year, month)?;
        events.extend(self.read_channel_segments_for_user(channel_id, user_id, year, month)?);
        events.extend(self.read_hot_user_events(channel_id, user_id, year, month)?);
        events.sort_by_key(|event| (event.timestamp.timestamp(), event.seq));
        events.dedup_by(|left, right| left.event_uid == right.event_uid);
        Ok(events)
    }

    pub fn read_channel_range(
        &self,
        channel_id: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<StoredEvent>> {
        let mut events = Vec::new();
        for key in enumerate_channel_days(channel_id, from, to) {
            events.extend(self.read_channel_logs(&key.channel_id, key.year, key.month, key.day)?);
        }
        events.retain(|event| event.timestamp >= from && event.timestamp <= to);
        events.sort_by_key(|event| (event.timestamp.timestamp(), event.seq));
        Ok(events)
    }

    pub fn read_user_range(
        &self,
        channel_id: &str,
        user_id: &str,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
    ) -> Result<Vec<StoredEvent>> {
        let mut events = Vec::new();
        for key in enumerate_user_months(channel_id, user_id, from, to) {
            events.extend(self.read_user_logs(
                &key.channel_id,
                &key.user_id,
                key.year,
                key.month,
            )?);
        }
        events.retain(|event| event.timestamp >= from && event.timestamp <= to);
        events.sort_by_key(|event| (event.timestamp.timestamp(), event.seq));
        Ok(events)
    }

    pub fn get_available_logs_for_user(
        &self,
        channel_id: &str,
        user_id: &str,
    ) -> Result<Vec<UserLogFile>> {
        let db = self.lock_db();
        let mut statement = db.prepare(
            r#"
            SELECT DISTINCT year, month FROM (
                SELECT year, month FROM segments
                WHERE scope = 'user' AND channel_id = ?1 AND user_id = ?2
                UNION
                SELECT CAST(strftime('%Y', timestamp_rfc3339) AS INTEGER) AS year,
                       CAST(strftime('%m', timestamp_rfc3339) AS INTEGER) AS month
                FROM events
                WHERE room_id = ?1 AND (user_id = ?2 OR target_user_id = ?2)
            )
            ORDER BY year DESC, month DESC
            "#,
        )?;
        let rows = statement.query_map(params![channel_id, user_id], |row| {
            Ok(UserLogFile {
                year: row.get::<_, i32>(0)?.to_string(),
                month: row.get::<_, i32>(1)?.to_string(),
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn get_available_logs_for_channel(&self, channel_id: &str) -> Result<Vec<ChannelLogFile>> {
        let db = self.lock_db();
        let mut statement = db.prepare(
            r#"
            SELECT DISTINCT year, month, day FROM (
                SELECT year, month, day FROM segments
                WHERE scope = 'channel' AND channel_id = ?1
                UNION
                SELECT CAST(strftime('%Y', timestamp_rfc3339) AS INTEGER) AS year,
                       CAST(strftime('%m', timestamp_rfc3339) AS INTEGER) AS month,
                       CAST(strftime('%d', timestamp_rfc3339) AS INTEGER) AS day
                FROM events
                WHERE room_id = ?1
            )
            ORDER BY year DESC, month DESC, day DESC
            "#,
        )?;
        let rows = statement.query_map(params![channel_id], |row| {
            Ok(ChannelLogFile {
                year: row.get::<_, i32>(0)?.to_string(),
                month: row.get::<_, i32>(1)?.to_string(),
                day: row.get::<_, i32>(2)?.to_string(),
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn latest_user_log_month(
        &self,
        channel_id: &str,
        user_id: &str,
    ) -> Result<Option<(i32, u32)>> {
        let logs = self.get_available_logs_for_user(channel_id, user_id)?;
        let Some(first) = logs.first() else {
            return Ok(None);
        };
        Ok(Some((first.year.parse()?, first.month.parse()?)))
    }

    pub fn random_user_message(
        &self,
        channel_id: &str,
        user_id: &str,
    ) -> Result<Option<StoredEvent>> {
        let events = self.get_all_user_events(channel_id, user_id)?;
        choose_random(events)
    }

    pub fn random_channel_message(&self, channel_id: &str) -> Result<Option<StoredEvent>> {
        let events = self.get_all_channel_events(channel_id)?;
        choose_random(events)
    }

    pub fn recover_pending_segments(&self) -> Result<()> {
        let pending = {
            let db = self.lock_db();
            let mut statement = db.prepare(
                "SELECT id, temp_path, final_path FROM pending_segments ORDER BY id ASC",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                ))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        let db = self.lock_db();
        for (id, temp_path, final_path) in pending {
            let _ = fs::remove_file(self.root_dir.join(&temp_path));
            let _ = fs::remove_file(self.root_dir.join(&final_path));
            db.execute("DELETE FROM pending_segments WHERE id = ?1", params![id])?;
        }
        Ok(())
    }

    pub fn compact_old_partitions(
        &self,
        now: DateTime<Utc>,
        compact_after_channel_days: i64,
        compact_after_user_months: i64,
    ) -> Result<()> {
        if !self.archive_enabled {
            return Ok(());
        }
        let channel_keys = self
            .compactable_channel_days(now - Duration::days(compact_after_channel_days))?
            .into_iter()
            .map(|partition| partition.key)
            .collect::<Vec<_>>();
        if self.shutdown_requested() {
            tracing::info!("store shutdown requested; stopping channel compaction scheduling");
            return Ok(());
        }
        self.compact_channel_partitions_parallel(&channel_keys)?;

        let user_keys = self
            .compactable_user_months(now, compact_after_user_months)?
            .into_iter()
            .map(|partition| partition.key)
            .collect::<Vec<_>>();
        if self.shutdown_requested() {
            tracing::info!("store shutdown requested; stopping user compaction scheduling");
            return Ok(());
        }
        self.compact_user_partitions_parallel(&user_keys)?;
        self.reclaim_database_space_low_priority()?;
        Ok(())
    }

    pub fn merge_imported_channel_days_into_archives(
        &self,
        day_keys: &[ChannelDayKey],
    ) -> Result<()> {
        if !self.archive_enabled {
            return Ok(());
        }
        if self.shutdown_requested() {
            tracing::info!("store shutdown requested; skipping imported archive merge");
            return Ok(());
        }
        self.merge_imported_channel_days_into_archives_parallel(day_keys)?;
        Ok(())
    }

    pub fn merge_imported_channel_days_into_archives_low_priority(
        &self,
        day_keys: &[ChannelDayKey],
    ) -> Result<()> {
        if !self.archive_enabled {
            return Ok(());
        }
        if self.shutdown_requested() {
            tracing::info!(
                "store shutdown requested; skipping low-priority imported archive merge"
            );
            return Ok(());
        }
        let _priority =
            self.enter_db_priority(DbWorkPriority::Import, "bulk imported archive merge");
        tracing::info!(
            "Import archive merge started for {} channel-day partition(s)",
            day_keys.len()
        );
        self.merge_imported_channel_days_into_archives_parallel(day_keys)?;
        tracing::info!(
            "Import archive merge completed for {} channel-day partition(s)",
            day_keys.len()
        );
        Ok(())
    }

    pub fn event_count(&self) -> Result<i64> {
        let db = self.lock_db();
        Ok(db.query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))?)
    }

    pub fn database_size_stats(&self) -> Result<DatabaseSizeStats> {
        let db = self.lock_db();
        let page_size = db.pragma_query_value(None, "page_size", |row| row.get::<_, i64>(0))?;
        let page_count = db.pragma_query_value(None, "page_count", |row| row.get::<_, i64>(0))?;
        let freelist_count =
            db.pragma_query_value(None, "freelist_count", |row| row.get::<_, i64>(0))?;
        drop(db);

        let live_pages = page_count.saturating_sub(freelist_count);
        let main_bytes = fs::metadata(&self.sqlite_path)
            .map(|meta| meta.len())
            .unwrap_or_default();
        let wal_path = self.sqlite_path.with_extension(format!(
            "{}-wal",
            self.sqlite_path
                .extension()
                .and_then(|value| value.to_str())
                .unwrap_or_default()
        ));
        let wal_bytes = fs::metadata(&wal_path)
            .map(|meta| meta.len())
            .unwrap_or_default();
        Ok(DatabaseSizeStats {
            page_size,
            page_count,
            freelist_count,
            live_pages,
            main_bytes,
            free_bytes: (page_size.max(0) as u64) * (freelist_count.max(0) as u64),
            live_bytes: (page_size.max(0) as u64) * (live_pages.max(0) as u64),
            wal_bytes,
        })
    }

    pub fn reclaim_database_space_low_priority(&self) -> Result<()> {
        let mut db = self.lock_import_db_low_priority("database maintenance");
        self.reclaim_database_space_with_connection(&mut db)
    }

    pub fn request_shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::SeqCst);
    }

    pub fn shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::SeqCst)
    }

    pub fn imported_raw_file_is_current(&self, path: &str, fingerprint: &str) -> Result<bool> {
        self.imported_raw_file_is_current_inner(path, fingerprint, false)
    }

    pub fn imported_raw_file_is_current_low_priority(
        &self,
        path: &str,
        fingerprint: &str,
    ) -> Result<bool> {
        self.imported_raw_file_is_current_inner(path, fingerprint, true)
    }

    fn imported_raw_file_is_current_inner(
        &self,
        path: &str,
        fingerprint: &str,
        low_priority: bool,
    ) -> Result<bool> {
        let lock_started = Instant::now();
        let db = if low_priority {
            self.lock_import_db_low_priority("bulk raw status lookup")
        } else {
            self.lock_db()
        };
        let lock_elapsed = lock_started.elapsed();
        let stored = db
            .query_row(
                "SELECT fingerprint, status FROM imported_raw_files WHERE path = ?1",
                params![path],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let is_current = matches!(
            stored.as_ref(),
            Some((stored_fingerprint, status))
                if stored_fingerprint == fingerprint && matches!(status.as_str(), "imported" | "seen")
        );
        if lock_elapsed > std::time::Duration::from_secs(1) {
            tracing::warn!(
                "Waited {:?} for imported_raw_files status lock: path={path}",
                lock_elapsed
            );
        }
        Ok(is_current)
    }

    pub fn imported_raw_files_current_map(
        &self,
        entries: &[(String, String)],
    ) -> Result<HashMap<String, bool>> {
        if entries.is_empty() {
            return Ok(HashMap::new());
        }

        let lock_started = Instant::now();
        let db = self.lock_import_db();
        let lock_elapsed = lock_started.elapsed();
        let mut stored = HashMap::<String, (String, String)>::new();

        for chunk in entries.chunks(500) {
            let placeholders = std::iter::repeat_n("?", chunk.len())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT path, fingerprint, status FROM imported_raw_files WHERE path IN ({placeholders})"
            );
            let params_vec = chunk
                .iter()
                .map(|(path, _)| path.as_str())
                .collect::<Vec<_>>();
            let mut statement = db.prepare(&sql)?;
            let rows = statement.query_map(
                params_from_iter(params_vec.iter().map(|value| value as &dyn ToSql)),
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )?;
            for row in rows {
                let (path, fingerprint, status) = row?;
                stored.insert(path, (fingerprint, status));
            }
        }

        if lock_elapsed > std::time::Duration::from_secs(1) {
            tracing::warn!(
                "Waited {:?} for imported_raw_files batch status lock: paths={}",
                lock_elapsed,
                entries.len()
            );
        }

        Ok(entries
            .iter()
            .map(|(path, fingerprint)| {
                let is_current = matches!(
                    stored.get(path),
                    Some((stored_fingerprint, status))
                        if stored_fingerprint == fingerprint
                            && matches!(status.as_str(), "imported" | "seen")
                );
                (path.clone(), is_current)
            })
            .collect())
    }

    pub fn record_imported_raw_file(
        &self,
        path: &str,
        fingerprint: &str,
        status: &str,
    ) -> Result<()> {
        self.record_imported_raw_file_inner(path, fingerprint, status, false)
    }

    pub fn record_imported_raw_file_low_priority(
        &self,
        path: &str,
        fingerprint: &str,
        status: &str,
    ) -> Result<()> {
        self.record_imported_raw_file_inner(path, fingerprint, status, true)
    }

    fn record_imported_raw_file_inner(
        &self,
        path: &str,
        fingerprint: &str,
        status: &str,
        low_priority: bool,
    ) -> Result<()> {
        let lock_started = Instant::now();
        let db = if low_priority {
            self.lock_import_db_low_priority("bulk raw status write")
        } else {
            self.lock_import_db()
        };
        let lock_elapsed = lock_started.elapsed();
        db.execute(
            r#"
            INSERT INTO imported_raw_files(path, fingerprint, imported_at, status)
            VALUES(?1, ?2, ?3, ?4)
            ON CONFLICT(path) DO UPDATE SET
                fingerprint = excluded.fingerprint,
                imported_at = excluded.imported_at,
                status = excluded.status
            "#,
            params![path, fingerprint, Utc::now().timestamp(), status],
        )?;
        if lock_elapsed > std::time::Duration::from_secs(1) {
            tracing::warn!(
                "Waited {:?} for imported_raw_files write lock: path={}, status={}",
                lock_elapsed,
                path,
                status
            );
        }
        Ok(())
    }

    pub fn imported_raw_file_status(&self, path: &str) -> Result<Option<String>> {
        let db = self.lock_import_db();
        Ok(db
            .query_row(
                "SELECT status FROM imported_raw_files WHERE path = ?1",
                params![path],
                |row| row.get::<_, String>(0),
            )
            .optional()?)
    }

    pub fn imported_reconstructed_file_is_current(
        &self,
        path: &str,
        fingerprint: &str,
    ) -> Result<bool> {
        let lock_started = Instant::now();
        let db = self.lock_db();
        let lock_elapsed = lock_started.elapsed();
        let stored = db
            .query_row(
                "SELECT fingerprint, status FROM imported_reconstructed_files WHERE path = ?1",
                params![path],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        let is_current = matches!(
            stored.as_ref(),
            Some((stored_fingerprint, status))
                if stored_fingerprint == fingerprint && status == "consumed"
        );
        if lock_elapsed > std::time::Duration::from_secs(1) {
            tracing::warn!(
                "Waited {:?} for imported_reconstructed_files status lock: path={path}",
                lock_elapsed
            );
        }
        Ok(is_current)
    }

    pub fn record_imported_reconstructed_file(
        &self,
        path: &str,
        fingerprint: &str,
        status: &str,
    ) -> Result<()> {
        let lock_started = Instant::now();
        let db = self.lock_db();
        let lock_elapsed = lock_started.elapsed();
        db.execute(
            r#"
            INSERT INTO imported_reconstructed_files(path, fingerprint, imported_at, status)
            VALUES(?1, ?2, ?3, ?4)
            ON CONFLICT(path) DO UPDATE SET
                fingerprint = excluded.fingerprint,
                imported_at = excluded.imported_at,
                status = excluded.status
            "#,
            params![path, fingerprint, Utc::now().timestamp(), status],
        )?;
        if lock_elapsed > std::time::Duration::from_secs(1) {
            tracing::warn!(
                "Waited {:?} for imported_reconstructed_files write lock: path={}, status={}",
                lock_elapsed,
                path,
                status
            );
        }
        Ok(())
    }

    pub fn start_import_run(
        &self,
        channel_filter: Option<&str>,
        limit_files: Option<usize>,
        dry_run: bool,
    ) -> Result<i64> {
        let now = Utc::now().timestamp();
        let db = self.lock_import_db();
        db.execute(
            r#"
            INSERT INTO import_runs(
                started_at, updated_at, completed_at, status, channel_filter, limit_files, dry_run
            ) VALUES(?1, ?1, NULL, 'running', ?2, ?3, ?4)
            "#,
            params![
                now,
                channel_filter,
                limit_files.map(|value| value as i64),
                if dry_run { 1 } else { 0 }
            ],
        )?;
        Ok(db.last_insert_rowid())
    }

    pub fn update_import_run_progress(
        &self,
        run_id: i64,
        directories_visited: usize,
        files_discovered: usize,
        files_completed: usize,
        files_failed: usize,
        last_path: Option<&str>,
    ) -> Result<()> {
        let now = Utc::now().timestamp();
        let db = self.lock_import_db_low_priority("import run progress");
        db.execute(
            r#"
            UPDATE import_runs
            SET updated_at = ?2,
                directories_visited = ?3,
                files_discovered = ?4,
                files_completed = ?5,
                files_failed = ?6
            WHERE id = ?1
            "#,
            params![
                run_id,
                now,
                directories_visited as i64,
                files_discovered as i64,
                files_completed as i64,
                files_failed as i64
            ],
        )?;
        db.execute(
            r#"
            INSERT INTO import_checkpoints(id, run_id, updated_at, directories_visited, files_discovered, last_path)
            VALUES(1, ?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(id) DO UPDATE SET
                run_id = excluded.run_id,
                updated_at = excluded.updated_at,
                directories_visited = excluded.directories_visited,
                files_discovered = excluded.files_discovered,
                last_path = excluded.last_path
            "#,
            params![
                run_id,
                now,
                directories_visited as i64,
                files_discovered as i64,
                last_path
            ],
        )?;
        Ok(())
    }

    pub fn finish_import_run(&self, run_id: i64, status: &str) -> Result<()> {
        let now = Utc::now().timestamp();
        let db = self.lock_import_db();
        db.execute(
            r#"
            UPDATE import_runs
            SET updated_at = ?2, completed_at = ?2, status = ?3
            WHERE id = ?1
            "#,
            params![run_id, now, status],
        )?;
        Ok(())
    }

    pub fn import_file_state(&self, path: &str) -> Result<Option<(String, String)>> {
        let db = self.lock_import_db_low_priority("import file read");
        db.query_row(
            "SELECT fingerprint, state FROM import_files WHERE path = ?1",
            params![path],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn upsert_import_file_state(
        &self,
        run_id: i64,
        path: &str,
        fingerprint: &str,
        kind: &str,
        state: &str,
        last_error: Option<&str>,
    ) -> Result<()> {
        let now = Utc::now().timestamp();
        let db = self.lock_import_db_low_priority("import file write");
        db.execute(
            r#"
            INSERT INTO import_files(path, run_id, fingerprint, kind, state, discovered_at, updated_at, last_error)
            VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?6, ?7)
            ON CONFLICT(path) DO UPDATE SET
                run_id = excluded.run_id,
                fingerprint = excluded.fingerprint,
                kind = excluded.kind,
                state = excluded.state,
                updated_at = excluded.updated_at,
                last_error = excluded.last_error
            "#,
            params![path, run_id, fingerprint, kind, state, now, last_error],
        )?;
        Ok(())
    }

    pub fn record_import_partition_state(
        &self,
        scope: &str,
        channel_id: &str,
        user_id: Option<&str>,
        year: i32,
        month: u32,
        day: Option<u32>,
        state: &str,
        last_error: Option<&str>,
        increment_source_count: bool,
    ) -> Result<()> {
        let now = Utc::now().timestamp();
        let db = self.lock_import_db_low_priority("import partition write");
        db.execute(
            r#"
            INSERT INTO import_partition_outputs(
                scope, channel_id, user_id, year, month, day, state, updated_at, source_count, last_error
            ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
            ON CONFLICT(scope, channel_id, COALESCE(user_id, ''), year, month, COALESCE(day, 0)) DO UPDATE SET
                state = excluded.state,
                updated_at = excluded.updated_at,
                source_count = import_partition_outputs.source_count + excluded.source_count,
                last_error = excluded.last_error
            "#,
            params![
                scope,
                channel_id,
                user_id,
                year,
                month,
                day,
                state,
                now,
                if increment_source_count { 1 } else { 0 },
                last_error
            ],
        )?;
        Ok(())
    }

    pub fn record_import_file_partition(
        &self,
        path: &str,
        scope: &str,
        channel_id: &str,
        user_id: Option<&str>,
        year: i32,
        month: u32,
        day: Option<u32>,
    ) -> Result<()> {
        let db = self.lock_import_db_low_priority("import file partition write");
        db.execute(
            r#"
            INSERT OR IGNORE INTO import_file_partitions(path, scope, channel_id, user_id, year, month, day)
            VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
            params![path, scope, channel_id, user_id, year, month, day],
        )?;
        Ok(())
    }

    fn compactable_channel_days(
        &self,
        threshold: DateTime<Utc>,
    ) -> Result<Vec<ChannelPartitionSummary>> {
        let db = self.lock_db();
        let mut statement = db.prepare(
            r#"
            SELECT room_id,
                   CAST(strftime('%Y', timestamp_rfc3339) AS INTEGER),
                   CAST(strftime('%m', timestamp_rfc3339) AS INTEGER),
                   CAST(strftime('%d', timestamp_rfc3339) AS INTEGER),
                   COUNT(*)
            FROM events
            WHERE timestamp_unix < ?1
            GROUP BY room_id, strftime('%Y', timestamp_rfc3339), strftime('%m', timestamp_rfc3339), strftime('%d', timestamp_rfc3339)
            ORDER BY room_id, MIN(timestamp_unix)
            "#,
        )?;
        let rows = statement.query_map(params![threshold.timestamp()], |row| {
            Ok(ChannelPartitionSummary {
                key: ChannelDayKey {
                    channel_id: row.get(0)?,
                    year: row.get(1)?,
                    month: row.get::<_, i64>(2)? as u32,
                    day: row.get::<_, i64>(3)? as u32,
                },
                count: row.get(4)?,
            })
        })?;
        let rows = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        drop(db);
        Ok(rows
            .into_iter()
            .filter(|summary| {
                self.segment_for_channel_day(
                    &summary.key.channel_id,
                    summary.key.year,
                    summary.key.month,
                    summary.key.day,
                )
                .ok()
                .flatten()
                .is_none()
            })
            .collect())
    }

    fn compactable_user_months(
        &self,
        now: DateTime<Utc>,
        compact_after_user_months: i64,
    ) -> Result<Vec<UserPartitionSummary>> {
        let cutoff = now - Duration::days(31 * compact_after_user_months);
        let db = self.lock_db();
        let mut statement = db.prepare(
            r#"
            SELECT room_id, user_ref,
                   CAST(strftime('%Y', timestamp_rfc3339) AS INTEGER),
                   CAST(strftime('%m', timestamp_rfc3339) AS INTEGER),
                   COUNT(*)
            FROM (
                SELECT room_id, user_id AS user_ref, timestamp_rfc3339
                FROM events WHERE user_id IS NOT NULL
                UNION ALL
                SELECT room_id, target_user_id AS user_ref, timestamp_rfc3339
                FROM events WHERE target_user_id IS NOT NULL
            )
            WHERE strftime('%s', timestamp_rfc3339) < ?1
            GROUP BY room_id, user_ref, strftime('%Y', timestamp_rfc3339), strftime('%m', timestamp_rfc3339)
            ORDER BY room_id, user_ref
            "#,
        )?;
        let rows = statement.query_map(params![cutoff.timestamp()], |row| {
            Ok(UserPartitionSummary {
                key: UserMonthKey {
                    channel_id: row.get(0)?,
                    user_id: row.get(1)?,
                    year: row.get(2)?,
                    month: row.get::<_, i64>(3)? as u32,
                },
                count: row.get(4)?,
            })
        })?;
        let rows = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        drop(db);
        Ok(rows
            .into_iter()
            .filter(|summary| {
                self.segment_for_user_month(
                    &summary.key.channel_id,
                    &summary.key.user_id,
                    summary.key.year,
                    summary.key.month,
                )
                .ok()
                .flatten()
                .is_none()
            })
            .collect())
    }

    pub fn compact_channel_partition(&self, key: &ChannelDayKey) -> Result<()> {
        let events = self.read_hot_channel_events(&key.channel_id, key.year, key.month, key.day)?;
        if events.is_empty() {
            return Ok(());
        }
        let relative = format!(
            "segments/channel/{}/{}/{}/{}.br",
            key.channel_id, key.year, key.month, key.day
        );
        let temp_path = self.root_dir.join(format!("{relative}.tmp"));
        let final_path = self.root_dir.join(&relative);
        let prepared = self.prepare_segment_write(&final_path, &temp_path, &events)?;
        self.install_channel_segment_write(
            prepared,
            &key.channel_id,
            key.year,
            key.month,
            key.day,
            &relative,
        )?;
        Ok(())
    }

    pub fn compact_user_partition(&self, key: &UserMonthKey) -> Result<()> {
        let events =
            self.read_hot_user_events(&key.channel_id, &key.user_id, key.year, key.month)?;
        if events.is_empty() {
            return Ok(());
        }
        let relative = format!(
            "segments/user/{}/{}/{}/{}.br",
            key.channel_id, key.user_id, key.year, key.month
        );
        let temp_path = self.root_dir.join(format!("{relative}.tmp"));
        let final_path = self.root_dir.join(&relative);
        let prepared = self.prepare_segment_write(&final_path, &temp_path, &events)?;
        self.install_user_segment_write(
            prepared,
            &key.channel_id,
            &key.user_id,
            key.year,
            key.month,
            &relative,
        )?;
        Ok(())
    }

    fn write_pending_segment(
        &self,
        scope: &str,
        channel_id: &str,
        user_id: Option<&str>,
        year: i32,
        month: u32,
        day: Option<u32>,
        temp_path: &Path,
        final_path: &Path,
    ) -> Result<()> {
        if let Some(parent) = final_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let relative_temp = temp_path
            .strip_prefix(&self.root_dir)
            .unwrap_or(temp_path)
            .to_string_lossy()
            .to_string();
        let relative_final = final_path
            .strip_prefix(&self.root_dir)
            .unwrap_or(final_path)
            .to_string_lossy()
            .to_string();
        let db = self.lock_db();
        db.execute(
            r#"
            INSERT INTO pending_segments(scope, channel_id, user_id, year, month, day, temp_path, final_path)
            VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "#,
            params![scope, channel_id, user_id, year, month, day, relative_temp, relative_final],
        )?;
        Ok(())
    }

    fn write_segment_file(
        &self,
        final_path: &Path,
        temp_path: &Path,
        events: &[StoredEvent],
    ) -> Result<()> {
        let prepared = self.prepare_segment_write(final_path, temp_path, events)?;
        self.finish_segment_write(prepared)
    }

    fn prepare_segment_write(
        &self,
        final_path: &Path,
        temp_path: &Path,
        events: &[StoredEvent],
    ) -> Result<PreparedSegmentWrite> {
        let path_guard = self.compression_paths.acquire(final_path.to_path_buf());
        if let Some(parent) = temp_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let raws = events
            .iter()
            .map(|event| event.raw.clone())
            .collect::<Vec<_>>();
        let line_count = events.len();
        let start_ts = events
            .first()
            .map(|event| event.timestamp.timestamp())
            .unwrap_or_default();
        let end_ts = events
            .last()
            .map(|event| event.timestamp.timestamp())
            .unwrap_or_default();
        let response = self
            .compression_executor
            .submit(temp_path.to_path_buf(), raws);
        tracing::info!(
            "Compression job queued: target={}, lines={}",
            final_path.display(),
            line_count
        );
        Ok(PreparedSegmentWrite {
            final_path: final_path.to_path_buf(),
            relative_path: final_path
                .strip_prefix(&self.root_dir)
                .unwrap_or(final_path)
                .to_string_lossy()
                .to_string(),
            line_count,
            start_ts,
            end_ts,
            response,
            _path_guard: path_guard,
        })
    }

    fn finish_segment_write(&self, prepared: PreparedSegmentWrite) -> Result<()> {
        let result = prepared
            .response
            .recv()
            .map_err(|_| anyhow!("compression worker stopped unexpectedly"))?
            .map_err(|error| anyhow!(error))?;
        tracing::info!(
            "Compression install started: target={}, bytes={}, elapsed={:?}",
            prepared.relative_path,
            result.output_bytes,
            result.elapsed
        );
        safe_replace_file(&result.temp_path, &prepared.final_path)?;
        tracing::info!(
            "Compression install completed: target={}",
            prepared.relative_path
        );
        Ok(())
    }

    fn reclaim_database_space_with_connection(&self, db: &mut Connection) -> Result<()> {
        db.execute_batch("PRAGMA optimize; PRAGMA wal_checkpoint(TRUNCATE);")?;

        let page_size = db.pragma_query_value(None, "page_size", |row| row.get::<_, i64>(0))?;
        let freelist_count =
            db.pragma_query_value(None, "freelist_count", |row| row.get::<_, i64>(0))?;
        let page_count = db.pragma_query_value(None, "page_count", |row| row.get::<_, i64>(0))?;
        let auto_vacuum =
            db.pragma_query_value(None, "auto_vacuum", |row| row.get::<_, i64>(0))?;

        let free_bytes = (page_size.max(0) as u64) * (freelist_count.max(0) as u64);
        let page_count_u64 = page_count.max(0) as u64;
        let free_pages_u64 = freelist_count.max(0) as u64;
        let high_free_ratio = page_count_u64 > 0
            && free_pages_u64.saturating_mul(DB_RECLAIM_FREE_RATIO_DENOMINATOR)
                >= page_count_u64.saturating_mul(DB_RECLAIM_FREE_RATIO_NUMERATOR);

        if free_bytes < DB_RECLAIM_INCREMENTAL_FREE_BYTES {
            return Ok(());
        }

        if auto_vacuum == 2 {
            db.execute_batch("PRAGMA incremental_vacuum; PRAGMA wal_checkpoint(TRUNCATE);")?;
            return Ok(());
        }

        if free_bytes >= DB_RECLAIM_FULL_VACUUM_FREE_BYTES && high_free_ratio {
            db.pragma_update(None, "auto_vacuum", "INCREMENTAL")?;
            db.execute_batch("VACUUM; PRAGMA wal_checkpoint(TRUNCATE); PRAGMA optimize;")?;
        }
        Ok(())
    }

    fn install_channel_segment_write(
        &self,
        prepared: PreparedSegmentWrite,
        channel_id: &str,
        year: i32,
        month: u32,
        day: u32,
        relative: &str,
    ) -> Result<()> {
        let line_count = prepared.line_count;
        let start_ts = prepared.start_ts;
        let end_ts = prepared.end_ts;
        self.finish_segment_write(prepared)?;
        let mut db = self.lock_db();
        let tx = db.transaction()?;
        tx.execute(
            r#"
            INSERT OR REPLACE INTO segments(scope, channel_id, user_id, year, month, day, path, line_count, start_ts, end_ts, compression, passthrough_raw)
            VALUES('channel', ?1, NULL, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'brotli', 1)
            "#,
            params![channel_id, year, month, day, relative, line_count as i64, start_ts, end_ts],
        )?;
        tx.execute(
            r#"
            DELETE FROM events
            WHERE room_id = ?1
              AND strftime('%Y', timestamp_rfc3339) = printf('%04d', ?2)
              AND strftime('%m', timestamp_rfc3339) = printf('%02d', ?3)
              AND strftime('%d', timestamp_rfc3339) = printf('%02d', ?4)
            "#,
            params![channel_id, year, month, day],
        )?;
        tx.execute(
            "DELETE FROM pending_segments WHERE final_path = ?1",
            params![relative],
        )?;
        tx.commit()?;
        drop(db);
        self.schedule_reconciliation(
            channel_id,
            year,
            month,
            day,
            relative,
            Utc::now() + Duration::seconds(RECONCILIATION_DELAY_SECONDS),
        )?;
        Ok(())
    }

    fn install_user_segment_write(
        &self,
        prepared: PreparedSegmentWrite,
        channel_id: &str,
        user_id: &str,
        year: i32,
        month: u32,
        relative: &str,
    ) -> Result<()> {
        let line_count = prepared.line_count;
        let start_ts = prepared.start_ts;
        let end_ts = prepared.end_ts;
        self.finish_segment_write(prepared)?;
        let mut db = self.lock_db();
        let tx = db.transaction()?;
        tx.execute(
            r#"
            INSERT OR REPLACE INTO segments(scope, channel_id, user_id, year, month, day, path, line_count, start_ts, end_ts, compression, passthrough_raw)
            VALUES('user', ?1, ?2, ?3, ?4, NULL, ?5, ?6, ?7, ?8, 'brotli', 1)
            "#,
            params![channel_id, user_id, year, month, relative, line_count as i64, start_ts, end_ts],
        )?;
        tx.execute(
            r#"
            DELETE FROM events
            WHERE room_id = ?1
              AND (user_id = ?2 OR target_user_id = ?2)
              AND strftime('%Y', timestamp_rfc3339) = printf('%04d', ?3)
              AND strftime('%m', timestamp_rfc3339) = printf('%02d', ?4)
            "#,
            params![channel_id, user_id, year, month],
        )?;
        tx.execute(
            "DELETE FROM pending_segments WHERE final_path = ?1",
            params![relative],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn schedule_reconciliation(
        &self,
        channel_id: &str,
        year: i32,
        month: u32,
        day: u32,
        segment_path: &str,
        scheduled_at: DateTime<Utc>,
    ) -> Result<()> {
        let db = self.lock_db();
        db.execute(
            r#"
            INSERT INTO reconciliation_jobs(
                channel_id, year, month, day, segment_path, scheduled_at, checked_at, status,
                conflict_count, repair_status, unhealthy, last_error
            ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, NULL, 'pending', 0, 'none', 0, NULL)
            ON CONFLICT(channel_id, year, month, day)
            DO UPDATE SET
                segment_path = excluded.segment_path,
                scheduled_at = excluded.scheduled_at,
                checked_at = NULL,
                status = 'pending',
                conflict_count = 0,
                repair_status = 'none',
                unhealthy = 0,
                last_error = NULL
            "#,
            params![
                channel_id,
                year,
                month,
                day,
                segment_path,
                scheduled_at.timestamp(),
            ],
        )?;
        Ok(())
    }

    pub fn due_reconciliation_jobs(&self, now: DateTime<Utc>) -> Result<Vec<ReconciliationJob>> {
        let db = self.lock_db();
        let mut statement = db.prepare(
            r#"
            SELECT id, channel_id, year, month, day, segment_path, scheduled_at, checked_at,
                   status, conflict_count, repair_status, unhealthy, last_error
            FROM reconciliation_jobs
            WHERE scheduled_at <= ?1 AND checked_at IS NULL
            ORDER BY scheduled_at ASC, id ASC
            "#,
        )?;
        let rows = statement.query_map(params![now.timestamp()], map_reconciliation_job_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn record_reconciliation_outcome(
        &self,
        job: &ReconciliationJob,
        outcome: &ReconciliationOutcome,
    ) -> Result<()> {
        let db = self.lock_db();
        db.execute(
            r#"
            UPDATE reconciliation_jobs
            SET checked_at = ?2,
                status = ?3,
                conflict_count = ?4,
                repair_status = ?5,
                unhealthy = ?6,
                last_error = ?7
            WHERE id = ?1
            "#,
            params![
                job.id,
                outcome.checked_at.timestamp(),
                outcome.status,
                outcome.conflict_count,
                outcome.repair_status,
                outcome.unhealthy,
                outcome.last_error,
            ],
        )?;
        Ok(())
    }

    pub fn upsert_reconciliation_status(
        &self,
        channel_id: &str,
        year: i32,
        month: u32,
        day: u32,
        segment_path: &str,
        outcome: &ReconciliationOutcome,
    ) -> Result<()> {
        let db = self.lock_db();
        db.execute(
            r#"
            INSERT INTO reconciliation_jobs(
                channel_id, year, month, day, segment_path, scheduled_at, checked_at, status,
                conflict_count, repair_status, unhealthy, last_error
            ) VALUES(?1, ?2, ?3, ?4, ?5, ?6, ?6, ?7, ?8, ?9, ?10, ?11)
            ON CONFLICT(channel_id, year, month, day)
            DO UPDATE SET
                segment_path = excluded.segment_path,
                checked_at = excluded.checked_at,
                status = excluded.status,
                conflict_count = excluded.conflict_count,
                repair_status = excluded.repair_status,
                unhealthy = excluded.unhealthy,
                last_error = excluded.last_error
            "#,
            params![
                channel_id,
                year,
                month,
                day,
                segment_path,
                outcome.checked_at.timestamp(),
                outcome.status,
                outcome.conflict_count,
                outcome.repair_status,
                outcome.unhealthy,
                outcome.last_error,
            ],
        )?;
        Ok(())
    }

    pub fn mark_reconciliation_error(&self, job: &ReconciliationJob, error: &str) -> Result<()> {
        self.record_reconciliation_outcome(
            job,
            &ReconciliationOutcome {
                checked_at: Utc::now(),
                status: "error".to_string(),
                conflict_count: 0,
                repair_status: "none".to_string(),
                unhealthy: 1,
                last_error: Some(error.to_string()),
            },
        )
    }

    pub fn channel_day_is_unhealthy(
        &self,
        channel_id: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<bool> {
        let db = self.lock_db();
        let unhealthy = db
            .query_row(
                r#"
                SELECT unhealthy
                FROM reconciliation_jobs
                WHERE channel_id = ?1 AND year = ?2 AND month = ?3 AND day = ?4
                "#,
                params![channel_id, year, month, day],
                |row| row.get::<_, i64>(0),
            )
            .optional()?;
        Ok(unhealthy.unwrap_or_default() == 1)
    }

    pub fn replace_channel_segment(
        &self,
        channel_id: &str,
        year: i32,
        month: u32,
        day: u32,
        events: &[StoredEvent],
    ) -> Result<()> {
        let segment = self
            .segment_for_channel_day(channel_id, year, month, day)?
            .ok_or_else(|| anyhow!("missing channel segment for repair"))?;
        let final_path = self.root_dir.join(&segment.path);
        let temp_path = self.root_dir.join(format!("{}.repair.tmp", segment.path));
        self.write_segment_file(&final_path, &temp_path, events)?;
        let db = self.lock_db();
        db.execute(
            r#"
            UPDATE segments
            SET line_count = ?1, start_ts = ?2, end_ts = ?3
            WHERE id = ?4
            "#,
            params![
                events.len() as i64,
                events
                    .first()
                    .map(|event| event.timestamp.timestamp())
                    .unwrap_or_default(),
                events
                    .last()
                    .map(|event| event.timestamp.timestamp())
                    .unwrap_or_default(),
                segment.id,
            ],
        )?;
        Ok(())
    }

    pub fn replace_or_create_channel_segment(
        &self,
        channel_id: &str,
        year: i32,
        month: u32,
        day: u32,
        events: &[StoredEvent],
    ) -> Result<String> {
        let relative = format!(
            "segments/channel/{}/{}/{}/{}.br",
            channel_id, year, month, day
        );
        let final_path = self.root_dir.join(&relative);
        let temp_path = self.root_dir.join(format!("{}.repair.tmp", relative));
        self.write_segment_file(&final_path, &temp_path, events)?;
        let db = self.lock_db();
        db.execute(
            r#"
            INSERT OR REPLACE INTO segments(scope, channel_id, user_id, year, month, day, path, line_count, start_ts, end_ts, compression, passthrough_raw)
            VALUES('channel', ?1, NULL, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'brotli', 1)
            "#,
            params![
                channel_id,
                year,
                month,
                day,
                relative,
                events.len() as i64,
                events.first().map(|event| event.timestamp.timestamp()).unwrap_or_default(),
                events.last().map(|event| event.timestamp.timestamp()).unwrap_or_default(),
            ],
        )?;
        Ok(relative)
    }

    pub fn replace_or_create_user_segment(
        &self,
        channel_id: &str,
        user_id: &str,
        year: i32,
        month: u32,
        events: &[StoredEvent],
    ) -> Result<String> {
        let relative = format!(
            "segments/user/{}/{}/{}/{}.br",
            channel_id, user_id, year, month
        );
        let final_path = self.root_dir.join(&relative);
        let temp_path = self.root_dir.join(format!("{}.repair.tmp", relative));
        self.write_segment_file(&final_path, &temp_path, events)?;
        let db = self.lock_db();
        db.execute(
            r#"
            INSERT OR REPLACE INTO segments(scope, channel_id, user_id, year, month, day, path, line_count, start_ts, end_ts, compression, passthrough_raw)
            VALUES('user', ?1, ?2, ?3, ?4, NULL, ?5, ?6, ?7, ?8, 'brotli', 1)
            "#,
            params![
                channel_id,
                user_id,
                year,
                month,
                relative,
                events.len() as i64,
                events.first().map(|event| event.timestamp.timestamp()).unwrap_or_default(),
                events.last().map(|event| event.timestamp.timestamp()).unwrap_or_default(),
            ],
        )?;
        Ok(relative)
    }

    pub fn import_replace_or_create_channel_segment(
        &self,
        channel_id: &str,
        year: i32,
        month: u32,
        day: u32,
        events: &[StoredEvent],
    ) -> Result<String> {
        let relative = format!(
            "segments/channel/{}/{}/{}/{}.br",
            channel_id, year, month, day
        );
        let final_path = self.root_dir.join(&relative);
        let temp_path = self.root_dir.join(format!("{relative}.import.tmp"));
        self.write_pending_segment(
            "channel",
            channel_id,
            None,
            year,
            month,
            Some(day),
            &temp_path,
            &final_path,
        )?;
        let prepared = self.prepare_segment_write(&final_path, &temp_path, events)?;
        let line_count = prepared.line_count;
        let start_ts = prepared.start_ts;
        let end_ts = prepared.end_ts;
        self.finish_segment_write(prepared)?;
        let mut db = self.lock_db();
        let tx = db.transaction()?;
        tx.execute(
            r#"
            INSERT OR REPLACE INTO segments(scope, channel_id, user_id, year, month, day, path, line_count, start_ts, end_ts, compression, passthrough_raw)
            VALUES('channel', ?1, NULL, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'brotli', 1)
            "#,
            params![channel_id, year, month, day, relative, line_count as i64, start_ts, end_ts],
        )?;
        tx.execute(
            r#"
            DELETE FROM events
            WHERE room_id = ?1
              AND strftime('%Y', timestamp_rfc3339) = printf('%04d', ?2)
              AND strftime('%m', timestamp_rfc3339) = printf('%02d', ?3)
              AND strftime('%d', timestamp_rfc3339) = printf('%02d', ?4)
            "#,
            params![channel_id, year, month, day],
        )?;
        tx.execute(
            "DELETE FROM pending_segments WHERE final_path = ?1",
            params![relative],
        )?;
        tx.commit()?;
        drop(db);
        self.schedule_reconciliation(
            channel_id,
            year,
            month,
            day,
            &relative,
            Utc::now() + Duration::seconds(RECONCILIATION_DELAY_SECONDS),
        )?;
        Ok(relative)
    }

    pub fn import_replace_or_create_user_segment(
        &self,
        channel_id: &str,
        user_id: &str,
        year: i32,
        month: u32,
        events: &[StoredEvent],
    ) -> Result<String> {
        let relative = format!(
            "segments/user/{}/{}/{}/{}.br",
            channel_id, user_id, year, month
        );
        let final_path = self.root_dir.join(&relative);
        let temp_path = self.root_dir.join(format!("{relative}.import.tmp"));
        self.write_pending_segment(
            "user",
            channel_id,
            Some(user_id),
            year,
            month,
            None,
            &temp_path,
            &final_path,
        )?;
        let prepared = self.prepare_segment_write(&final_path, &temp_path, events)?;
        let line_count = prepared.line_count;
        let start_ts = prepared.start_ts;
        let end_ts = prepared.end_ts;
        self.finish_segment_write(prepared)?;
        let mut db = self.lock_db();
        let tx = db.transaction()?;
        tx.execute(
            r#"
            INSERT OR REPLACE INTO segments(scope, channel_id, user_id, year, month, day, path, line_count, start_ts, end_ts, compression, passthrough_raw)
            VALUES('user', ?1, ?2, ?3, ?4, NULL, ?5, ?6, ?7, ?8, 'brotli', 1)
            "#,
            params![channel_id, user_id, year, month, relative, line_count as i64, start_ts, end_ts],
        )?;
        tx.execute(
            r#"
            DELETE FROM events
            WHERE room_id = ?1
              AND (user_id = ?2 OR target_user_id = ?2)
              AND strftime('%Y', timestamp_rfc3339) = printf('%04d', ?3)
              AND strftime('%m', timestamp_rfc3339) = printf('%02d', ?4)
            "#,
            params![channel_id, user_id, year, month],
        )?;
        tx.execute(
            "DELETE FROM pending_segments WHERE final_path = ?1",
            params![relative],
        )?;
        tx.commit()?;
        Ok(relative)
    }

    pub fn should_archive_channel_day(&self, key: &ChannelDayKey, now: DateTime<Utc>) -> bool {
        let cutoff = now - Duration::days(self.compact_after_channel_days.max(0));
        match Utc
            .with_ymd_and_hms(key.year, key.month, key.day, 23, 59, 59)
            .single()
        {
            Some(end_of_day) => end_of_day <= cutoff,
            None => false,
        }
    }

    fn merge_imported_channel_days_into_archives_parallel(
        &self,
        day_keys: &[ChannelDayKey],
    ) -> Result<()> {
        let mut prepared_days = Vec::new();
        for key in day_keys {
            if self.shutdown_requested() {
                tracing::info!("store shutdown requested; stopping channel-day merge preparation");
                break;
            }
            let mut day_events =
                self.read_channel_logs(&key.channel_id, key.year, key.month, key.day)?;
            if day_events.is_empty() {
                continue;
            }
            dedupe_and_sort_stored_events(&mut day_events);
            let relative = format!(
                "segments/channel/{}/{}/{}/{}.br",
                key.channel_id, key.year, key.month, key.day
            );
            let final_path = self.root_dir.join(&relative);
            let temp_path = self.root_dir.join(format!("{relative}.repair.tmp"));
            self.write_pending_segment(
                "channel",
                &key.channel_id,
                None,
                key.year,
                key.month,
                Some(key.day),
                &temp_path,
                &final_path,
            )?;
            let prepared = self.prepare_segment_write(&final_path, &temp_path, &day_events)?;
            prepared_days.push((key.clone(), relative, day_events, prepared));
        }

        let mut user_months = BTreeSet::new();
        for (key, relative, day_events, prepared) in prepared_days {
            if self.shutdown_requested() {
                tracing::info!("store shutdown requested; stopping channel-day merge installs");
                break;
            }
            self.install_channel_segment_write(
                prepared,
                &key.channel_id,
                key.year,
                key.month,
                key.day,
                &relative,
            )?;
            for event in &day_events {
                if let Some(user_id) = event.user_id.clone() {
                    user_months.insert(UserMonthKey {
                        channel_id: event.room_id.clone(),
                        user_id,
                        year: event.timestamp.year(),
                        month: event.timestamp.month(),
                    });
                }
                if let Some(user_id) = event.target_user_id.clone() {
                    user_months.insert(UserMonthKey {
                        channel_id: event.room_id.clone(),
                        user_id,
                        year: event.timestamp.year(),
                        month: event.timestamp.month(),
                    });
                }
            }
        }

        self.compact_user_partitions_parallel(&user_months.into_iter().collect::<Vec<_>>())
    }

    fn compact_channel_partitions_parallel(&self, keys: &[ChannelDayKey]) -> Result<()> {
        let mut prepared = Vec::new();
        for key in keys {
            if self.shutdown_requested() {
                tracing::info!("store shutdown requested; stopping channel compaction preparation");
                break;
            }
            let events =
                self.read_hot_channel_events(&key.channel_id, key.year, key.month, key.day)?;
            if events.is_empty() {
                continue;
            }
            let relative = format!(
                "segments/channel/{}/{}/{}/{}.br",
                key.channel_id, key.year, key.month, key.day
            );
            let temp_path = self.root_dir.join(format!("{relative}.tmp"));
            let final_path = self.root_dir.join(&relative);
            self.write_pending_segment(
                "channel",
                &key.channel_id,
                None,
                key.year,
                key.month,
                Some(key.day),
                &temp_path,
                &final_path,
            )?;
            let prepared_write = self.prepare_segment_write(&final_path, &temp_path, &events)?;
            prepared.push((key.clone(), relative, prepared_write));
        }
        for (key, relative, prepared_write) in prepared {
            if self.shutdown_requested() {
                tracing::info!("store shutdown requested; stopping channel compaction installs");
                break;
            }
            self.install_channel_segment_write(
                prepared_write,
                &key.channel_id,
                key.year,
                key.month,
                key.day,
                &relative,
            )?;
        }
        Ok(())
    }

    fn compact_user_partitions_parallel(&self, keys: &[UserMonthKey]) -> Result<()> {
        let mut prepared = Vec::new();
        for key in keys {
            if self.shutdown_requested() {
                tracing::info!("store shutdown requested; stopping user compaction preparation");
                break;
            }
            let events =
                self.read_hot_user_events(&key.channel_id, &key.user_id, key.year, key.month)?;
            if events.is_empty() {
                continue;
            }
            let relative = format!(
                "segments/user/{}/{}/{}/{}.br",
                key.channel_id, key.user_id, key.year, key.month
            );
            let temp_path = self.root_dir.join(format!("{relative}.tmp"));
            let final_path = self.root_dir.join(&relative);
            self.write_pending_segment(
                "user",
                &key.channel_id,
                Some(&key.user_id),
                key.year,
                key.month,
                None,
                &temp_path,
                &final_path,
            )?;
            let prepared_write = self.prepare_segment_write(&final_path, &temp_path, &events)?;
            prepared.push((key.clone(), relative, prepared_write));
        }
        for (key, relative, prepared_write) in prepared {
            if self.shutdown_requested() {
                tracing::info!("store shutdown requested; stopping user compaction installs");
                break;
            }
            self.install_user_segment_write(
                prepared_write,
                &key.channel_id,
                &key.user_id,
                key.year,
                key.month,
                &relative,
            )?;
        }
        Ok(())
    }

    pub fn list_channel_segments_since(
        &self,
        start: Option<DateTime<Utc>>,
    ) -> Result<Vec<SegmentRecord>> {
        self.list_segments("channel", start, None)
    }

    pub fn list_user_segments_since(
        &self,
        start: Option<DateTime<Utc>>,
    ) -> Result<Vec<SegmentRecord>> {
        self.list_segments("user", start, None)
    }

    pub fn read_archived_channel_segment_strict(
        &self,
        channel_id: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<Vec<StoredEvent>> {
        let Some(segment) = self.segment_for_channel_day(channel_id, year, month, day)? else {
            return Ok(Vec::new());
        };
        self.load_segment_events_strict(&segment)
    }

    pub fn read_archived_user_segment_strict(
        &self,
        channel_id: &str,
        user_id: &str,
        year: i32,
        month: u32,
    ) -> Result<Vec<StoredEvent>> {
        let Some(segment) = self.segment_for_user_month(channel_id, user_id, year, month)? else {
            return Ok(Vec::new());
        };
        self.load_segment_events_strict(&segment)
            .map(|events| filter_user_events(events, user_id))
    }

    fn segment_for_channel_day(
        &self,
        channel_id: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<Option<SegmentRecord>> {
        let db = self.lock_db();
        db.query_row(
            r#"
            SELECT id, scope, channel_id, user_id, year, month, day, path, line_count, start_ts, end_ts, compression, passthrough_raw
            FROM segments
            WHERE scope = 'channel' AND channel_id = ?1 AND year = ?2 AND month = ?3 AND day = ?4
            "#,
            params![channel_id, year, month, day],
            map_segment_row,
        )
        .optional()
        .map_err(Into::into)
    }

    fn segment_for_user_month(
        &self,
        channel_id: &str,
        user_id: &str,
        year: i32,
        month: u32,
    ) -> Result<Option<SegmentRecord>> {
        let db = self.lock_db();
        db.query_row(
            r#"
            SELECT id, scope, channel_id, user_id, year, month, day, path, line_count, start_ts, end_ts, compression, passthrough_raw
            FROM segments
            WHERE scope = 'user' AND channel_id = ?1 AND user_id = ?2 AND year = ?3 AND month = ?4
            "#,
            params![channel_id, user_id, year, month],
            map_segment_row,
        )
        .optional()
        .map_err(Into::into)
    }

    fn read_channel_segment(
        &self,
        channel_id: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<Vec<StoredEvent>> {
        let segment = match self.segment_for_channel_day(channel_id, year, month, day)? {
            Some(segment) => segment,
            None => return Ok(Vec::new()),
        };
        self.load_segment_events(&segment)
    }

    fn read_user_segment(
        &self,
        channel_id: &str,
        user_id: &str,
        year: i32,
        month: u32,
    ) -> Result<Vec<StoredEvent>> {
        let segment = match self.segment_for_user_month(channel_id, user_id, year, month)? {
            Some(segment) => segment,
            None => return Ok(Vec::new()),
        };
        self.load_segment_events(&segment)
            .map(|events| filter_user_events(events, user_id))
    }

    fn read_channel_segments_for_user(
        &self,
        channel_id: &str,
        user_id: &str,
        year: i32,
        month: u32,
    ) -> Result<Vec<StoredEvent>> {
        let mut events = Vec::new();
        let db = self.lock_db();
        let mut statement = db.prepare(
            r#"
            SELECT id, scope, channel_id, user_id, year, month, day, path, line_count, start_ts, end_ts, compression, passthrough_raw
            FROM segments
            WHERE scope = 'channel' AND channel_id = ?1 AND year = ?2 AND month = ?3
            ORDER BY day ASC
            "#,
        )?;
        let segments = statement
            .query_map(params![channel_id, year, month], map_segment_row)?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        drop(statement);
        drop(db);
        for segment in segments {
            events.extend(filter_user_events(
                self.load_segment_events(&segment)?,
                user_id,
            ));
        }
        Ok(events)
    }

    fn load_segment_events(&self, segment: &SegmentRecord) -> Result<Vec<StoredEvent>> {
        self.load_segment_events_internal(segment, false)
    }

    fn load_segment_events_strict(&self, segment: &SegmentRecord) -> Result<Vec<StoredEvent>> {
        self.load_segment_events_internal(segment, true)
    }

    fn load_segment_events_internal(
        &self,
        segment: &SegmentRecord,
        strict: bool,
    ) -> Result<Vec<StoredEvent>> {
        let file = File::open(self.root_dir.join(&segment.path))
            .with_context(|| format!("failed to open segment {}", segment.path))?;
        let mut decoder = Decompressor::new(file, 64 * 1024);
        let mut bytes = Vec::new();
        decoder.read_to_end(&mut bytes)?;
        let mut events = Vec::new();
        for (index, line) in bytes.split(|byte| *byte == b'\n').enumerate() {
            if line.is_empty() {
                continue;
            }
            let raw = String::from_utf8(line.to_vec())?;
            match CanonicalEvent::from_raw(&raw)? {
                Some(event) => {
                    events.push(StoredEvent {
                        seq: index as i64,
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
                    });
                }
                None if strict => {
                    return Err(anyhow!(
                        "segment {} contains a line that could not be parsed as a supported event",
                        segment.path
                    ));
                }
                None => {}
            }
        }
        Ok(events)
    }

    fn read_hot_channel_events(
        &self,
        channel_id: &str,
        year: i32,
        month: u32,
        day: u32,
    ) -> Result<Vec<StoredEvent>> {
        let db = self.lock_db();
        let mut statement = db.prepare(
            r#"
            SELECT seq, event_uid, room_id, channel_login, username, display_name, user_id, target_user_id,
                   text, system_text, raw, timestamp_rfc3339, tags_json, kind
            FROM events
            WHERE room_id = ?1
              AND strftime('%Y', timestamp_rfc3339) = printf('%04d', ?2)
              AND strftime('%m', timestamp_rfc3339) = printf('%02d', ?3)
              AND strftime('%d', timestamp_rfc3339) = printf('%02d', ?4)
            ORDER BY timestamp_unix ASC, seq ASC
            "#,
        )?;
        let rows = statement.query_map(params![channel_id, year, month, day], map_event_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    fn read_hot_user_events(
        &self,
        channel_id: &str,
        user_id: &str,
        year: i32,
        month: u32,
    ) -> Result<Vec<StoredEvent>> {
        let db = self.lock_db();
        let mut statement = db.prepare(
            r#"
            SELECT seq, event_uid, room_id, channel_login, username, display_name, user_id, target_user_id,
                   text, system_text, raw, timestamp_rfc3339, tags_json, kind
            FROM events
            WHERE room_id = ?1
              AND (user_id = ?2 OR target_user_id = ?2)
              AND strftime('%Y', timestamp_rfc3339) = printf('%04d', ?3)
              AND strftime('%m', timestamp_rfc3339) = printf('%02d', ?4)
            ORDER BY timestamp_unix ASC, seq ASC
            "#,
        )?;
        let rows = statement.query_map(params![channel_id, user_id, year, month], map_event_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map(|events| filter_user_events(events, user_id))
            .map_err(Into::into)
    }

    fn get_all_user_events(&self, channel_id: &str, user_id: &str) -> Result<Vec<StoredEvent>> {
        let mut events = Vec::new();
        for log in self.get_available_logs_for_user(channel_id, user_id)? {
            events.extend(self.read_user_logs(
                channel_id,
                user_id,
                log.year.parse()?,
                log.month.parse()?,
            )?);
        }
        Ok(events)
    }

    fn get_all_channel_events(&self, channel_id: &str) -> Result<Vec<StoredEvent>> {
        let mut events = Vec::new();
        for log in self.get_available_logs_for_channel(channel_id)? {
            events.extend(self.read_channel_logs(
                channel_id,
                log.year.parse()?,
                log.month.parse()?,
                log.day.parse()?,
            )?);
        }
        Ok(events)
    }

    fn list_segments(
        &self,
        scope: &str,
        start: Option<DateTime<Utc>>,
        channel_id: Option<&str>,
    ) -> Result<Vec<SegmentRecord>> {
        let db = self.lock_db();
        let mut rows = Vec::new();
        if let Some(start) = start {
            if let Some(channel_id) = channel_id {
                let mut statement = db.prepare(
                    r#"
                    SELECT id, scope, channel_id, user_id, year, month, day, path, line_count, start_ts, end_ts, compression, passthrough_raw
                    FROM segments
                    WHERE scope = ?1 AND channel_id = ?2 AND end_ts >= ?3
                    ORDER BY start_ts ASC
                    "#,
                )?;
                rows.extend(
                    statement
                        .query_map(
                            params![scope, channel_id, start.timestamp()],
                            map_segment_row,
                        )?
                        .collect::<rusqlite::Result<Vec<_>>>()?,
                );
            } else {
                let mut statement = db.prepare(
                    r#"
                    SELECT id, scope, channel_id, user_id, year, month, day, path, line_count, start_ts, end_ts, compression, passthrough_raw
                    FROM segments
                    WHERE scope = ?1 AND end_ts >= ?2
                    ORDER BY start_ts ASC
                    "#,
                )?;
                rows.extend(
                    statement
                        .query_map(params![scope, start.timestamp()], map_segment_row)?
                        .collect::<rusqlite::Result<Vec<_>>>()?,
                );
            }
        } else if let Some(channel_id) = channel_id {
            let mut statement = db.prepare(
                r#"
                SELECT id, scope, channel_id, user_id, year, month, day, path, line_count, start_ts, end_ts, compression, passthrough_raw
                FROM segments
                WHERE scope = ?1 AND channel_id = ?2
                ORDER BY start_ts ASC
                "#,
            )?;
            rows.extend(
                statement
                    .query_map(params![scope, channel_id], map_segment_row)?
                    .collect::<rusqlite::Result<Vec<_>>>()?,
            );
        } else {
            let mut statement = db.prepare(
                r#"
                SELECT id, scope, channel_id, user_id, year, month, day, path, line_count, start_ts, end_ts, compression, passthrough_raw
                FROM segments
                WHERE scope = ?1
                ORDER BY start_ts ASC
                "#,
            )?;
            rows.extend(
                statement
                    .query_map(params![scope], map_segment_row)?
                    .collect::<rusqlite::Result<Vec<_>>>()?,
            );
        }
        Ok(rows)
    }
}

fn filter_user_events(events: Vec<StoredEvent>, user_id: &str) -> Vec<StoredEvent> {
    events
        .into_iter()
        .filter(|event| {
            event.user_id.as_deref() == Some(user_id)
                || event.target_user_id.as_deref() == Some(user_id)
        })
        .collect()
}

fn dedupe_and_sort_stored_events(events: &mut Vec<StoredEvent>) {
    let mut seen = BTreeSet::new();
    events.retain(|event| seen.insert(event.event_uid.clone()));
    events.sort_by_key(|event| (event.timestamp.timestamp(), event.event_uid.clone()));
    for (index, event) in events.iter_mut().enumerate() {
        event.seq = index as i64;
    }
}

fn read_max_compress_threads_from_env() -> usize {
    std::env::var("JUSTLOG_IMPORT_MAX_COMPRESS_THREADS")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(default_max_compress_threads)
}

fn default_max_compress_threads() -> usize {
    thread::available_parallelism()
        .map(|value| value.get())
        .unwrap_or(2)
        .min(4)
        .max(1)
}

fn compression_worker_loop(
    receiver: Arc<Mutex<Receiver<CompressionRequest>>>,
    quality: u32,
    lgwin: u32,
) {
    loop {
        let request = {
            let guard = receiver.lock().unwrap();
            guard.recv()
        };
        let Ok(request) = request else {
            return;
        };
        let started = Instant::now();
        tracing::info!(
            "Compression job started: target={}, lines={}",
            request.temp_path.display(),
            request.raws.len()
        );
        let result =
            write_compressed_segment_file(&request.temp_path, &request.raws, quality, lgwin)
                .map(|output_bytes| CompressionResult {
                    temp_path: request.temp_path.clone(),
                    output_bytes,
                    elapsed: started.elapsed(),
                })
                .map_err(|error| error.to_string());
        match &result {
            Ok(done) => tracing::info!(
                "Compression job completed: target={}, bytes={}, elapsed={:?}",
                done.temp_path.display(),
                done.output_bytes,
                done.elapsed
            ),
            Err(error) => tracing::warn!(
                "Compression job failed: target={}, error={}",
                request.temp_path.display(),
                error
            ),
        }
        let _ = request.response.send(result);
    }
}

fn write_compressed_segment_file(
    temp_path: &Path,
    raws: &[String],
    quality: u32,
    lgwin: u32,
) -> Result<u64> {
    if let Some(parent) = temp_path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(temp_path)?;
    let mut writer = CompressorWriter::new(file, 64 * 1024, quality, lgwin);
    for raw in raws {
        writer.write_all(raw.as_bytes())?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    drop(writer);
    Ok(fs::metadata(temp_path)?.len())
}

fn choose_random(events: Vec<StoredEvent>) -> Result<Option<StoredEvent>> {
    if events.is_empty() {
        return Ok(None);
    }
    let mut rng = rand::rng();
    let index = rng.random_range(0..events.len());
    Ok(events.into_iter().nth(index))
}

fn safe_replace_file(temp_path: &Path, final_path: &Path) -> Result<()> {
    if let Some(parent) = final_path.parent() {
        fs::create_dir_all(parent)?;
    }

    #[cfg(not(windows))]
    {
        fs::rename(temp_path, final_path)?;
        return Ok(());
    }

    #[cfg(windows)]
    {
        if !final_path.exists() {
            fs::rename(temp_path, final_path)?;
            return Ok(());
        }

        let backup_path = final_path.with_extension(format!(
            "{}swap.bak",
            final_path
                .extension()
                .and_then(|value| value.to_str())
                .map(|value| format!("{value}."))
                .unwrap_or_default()
        ));
        if backup_path.exists() {
            let _ = fs::remove_file(&backup_path);
        }
        fs::rename(final_path, &backup_path)?;
        match fs::rename(temp_path, final_path) {
            Ok(()) => {
                let _ = fs::remove_file(&backup_path);
                Ok(())
            }
            Err(error) => {
                let _ = fs::rename(&backup_path, final_path);
                Err(error.into())
            }
        }
    }
}

fn enumerate_channel_days(
    channel_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Vec<ChannelDayKey> {
    let mut keys = Vec::new();
    let mut cursor = from.date_naive();
    let end = to.date_naive();
    while cursor <= end {
        keys.push(ChannelDayKey {
            channel_id: channel_id.to_string(),
            year: cursor.year(),
            month: cursor.month(),
            day: cursor.day(),
        });
        cursor = cursor.succ_opt().unwrap();
    }
    keys
}

fn enumerate_user_months(
    channel_id: &str,
    user_id: &str,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Vec<UserMonthKey> {
    let mut keys = Vec::new();
    let mut year = from.year();
    let mut month = from.month();
    loop {
        keys.push(UserMonthKey {
            channel_id: channel_id.to_string(),
            user_id: user_id.to_string(),
            year,
            month,
        });
        if year == to.year() && month == to.month() {
            break;
        }
        if month == 12 {
            year += 1;
            month = 1;
        } else {
            month += 1;
        }
    }
    keys
}

fn map_event_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredEvent> {
    let timestamp = row.get::<_, String>(11)?;
    let timestamp = DateTime::parse_from_rfc3339(&timestamp)
        .map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                11,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?
        .with_timezone(&Utc);
    let tags_json = row.get::<_, String>(12)?;
    let tags = serde_json::from_str(&tags_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(12, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(StoredEvent {
        seq: row.get(0)?,
        event_uid: row.get(1)?,
        room_id: row.get(2)?,
        channel_login: row.get(3)?,
        username: row.get(4)?,
        display_name: row.get(5)?,
        user_id: row.get(6)?,
        target_user_id: row.get(7)?,
        text: row.get(8)?,
        system_text: row.get(9)?,
        raw: row.get(10)?,
        timestamp,
        tags,
        kind: row.get(13)?,
    })
}

fn map_segment_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<SegmentRecord> {
    Ok(SegmentRecord {
        id: row.get(0)?,
        scope: row.get(1)?,
        channel_id: row.get(2)?,
        user_id: row.get(3)?,
        year: row.get(4)?,
        month: row.get::<_, i64>(5)? as u32,
        day: row.get::<_, Option<i64>>(6)?.map(|value| value as u32),
        path: row.get(7)?,
        line_count: row.get(8)?,
        start_ts: row.get(9)?,
        end_ts: row.get(10)?,
        compression: row.get(11)?,
        passthrough_raw: row.get::<_, i64>(12)? == 1,
    })
}

fn map_reconciliation_job_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<ReconciliationJob> {
    let scheduled_at = timestamp_from_sql(6, row.get::<_, i64>(6)?)?;
    let checked_at = row
        .get::<_, Option<i64>>(7)?
        .map(|value| timestamp_from_sql(7, value))
        .transpose()?;
    Ok(ReconciliationJob {
        id: row.get(0)?,
        channel_id: row.get(1)?,
        year: row.get(2)?,
        month: row.get::<_, i64>(3)? as u32,
        day: row.get::<_, i64>(4)? as u32,
        segment_path: row.get(5)?,
        scheduled_at,
        checked_at,
        status: row.get(8)?,
        conflict_count: row.get(9)?,
        repair_status: row.get(10)?,
        unhealthy: row.get(11)?,
        last_error: row.get(12)?,
    })
}

fn timestamp_from_sql(index: usize, timestamp: i64) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(timestamp, 0).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            rusqlite::types::Type::Integer,
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("invalid unix timestamp {timestamp}"),
            )),
        )
    })
}

pub fn decode_segment_lines(path: &Path) -> Result<Vec<String>> {
    let file = File::open(path)?;
    let mut decoder = Decompressor::new(file, 64 * 1024);
    let mut output = String::new();
    decoder.read_to_string(&mut output)?;
    Ok(output.lines().map(ToString::to_string).collect())
}

pub fn load_lines_from_text_file(path: &Path) -> Result<Vec<String>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    reader
        .lines()
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|error| anyhow!(error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::CanonicalEvent;
    use std::sync::mpsc;
    use std::time::Duration;
    use tempfile::TempDir;

    fn test_store() -> (Store, TempDir) {
        let temp = TempDir::new().unwrap();
        let sqlite_path = temp.path().join("test.sqlite3");
        let root_dir = temp.path().join("logs");
        fs::create_dir_all(&root_dir).unwrap();
        let store = Store {
            db: Arc::new(Mutex::new(Connection::open(&sqlite_path).unwrap())),
            import_db: Arc::new(Mutex::new(Connection::open(&sqlite_path).unwrap())),
            root_dir,
            sqlite_path,
            archive_enabled: true,
            compact_after_channel_days: 30,
            compact_after_user_months: 6,
            compression_executor: Arc::new(CompressionExecutor::new(1, 5, 22)),
            compression_paths: Arc::new(CompressionPathLocks::default()),
            db_priority: Arc::new(DbPriorityScheduler::default()),
            shutdown_requested: Arc::new(AtomicBool::new(false)),
        };
        store.initialize().unwrap();
        (store, temp)
    }

    #[test]
    #[should_panic(
        expected = "re-entrant Store DB lock on the same thread; drop the outer db guard before calling another Store method"
    )]
    fn nested_db_lock_panics_instead_of_deadlocking() {
        let (store, _temp) = test_store();

        let _outer = store.lock_db();
        let _inner = store.lock_db();
    }

    #[test]
    fn low_priority_db_work_waits_for_live_work() {
        let (store, _temp) = test_store();
        let (started_tx, started_rx) = mpsc::channel();
        let shared = store.clone();
        let handle = thread::spawn(move || {
            let _guard = shared.lock_db();
            started_tx.send(()).unwrap();
            thread::sleep(Duration::from_millis(150));
        });
        started_rx.recv().unwrap();

        let started = Instant::now();
        let _guard = store.lock_import_db_low_priority("test low-priority wait");
        assert!(
            started.elapsed() >= Duration::from_millis(100),
            "low-priority db work should wait for active live work"
        );
        handle.join().unwrap();
    }

    #[test]
    fn live_db_work_does_not_wait_for_low_priority_gate() {
        let (store, _temp) = test_store();
        let (started_tx, started_rx) = mpsc::channel();
        let shared = store.clone();
        let handle = thread::spawn(move || {
            let _guard = shared.lock_import_db_low_priority("test held low-priority slot");
            started_tx.send(()).unwrap();
            thread::sleep(Duration::from_millis(150));
        });
        started_rx.recv().unwrap();

        let started = Instant::now();
        let _guard = store.lock_db();
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "live db work should not wait for the low-priority scheduler slot"
        );
        handle.join().unwrap();
    }

    #[test]
    fn imported_raw_status_lookup_low_priority_waits_for_live_work() {
        let (store, _temp) = test_store();
        store
            .record_imported_raw_file("path.txt", "fingerprint", "imported")
            .unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let shared = store.clone();
        let handle = thread::spawn(move || {
            let _guard = shared.lock_db();
            started_tx.send(()).unwrap();
            thread::sleep(Duration::from_millis(150));
        });
        started_rx.recv().unwrap();

        let started = Instant::now();
        let is_current = store
            .imported_raw_file_is_current_low_priority("path.txt", "fingerprint")
            .unwrap();
        assert!(is_current);
        assert!(
            started.elapsed() >= Duration::from_millis(100),
            "low-priority status lookup should wait for live work"
        );
        handle.join().unwrap();
    }

    #[test]
    fn record_imported_raw_file_low_priority_waits_for_live_work() {
        let (store, _temp) = test_store();
        let (started_tx, started_rx) = mpsc::channel();
        let shared = store.clone();
        let handle = thread::spawn(move || {
            let _guard = shared.lock_db();
            started_tx.send(()).unwrap();
            thread::sleep(Duration::from_millis(150));
        });
        started_rx.recv().unwrap();

        let started = Instant::now();
        store
            .record_imported_raw_file_low_priority("path.txt", "fingerprint", "imported")
            .unwrap();
        assert!(
            started.elapsed() >= Duration::from_millis(100),
            "low-priority status write should wait for live work"
        );
        handle.join().unwrap();
    }

    #[test]
    fn insert_events_batch_low_priority_waits_for_live_work() {
        let (store, _temp) = test_store();
        let event = CanonicalEvent::from_raw("@badge-info=;badges=;color=#1E90FF;display-name=viewer;emotes=;id=batch-low-pri-1;room-id=1;subscriber=0;tmi-sent-ts=1704153604000;turbo=0;user-id=200;user-type= :viewer!viewer@viewer.tmi.twitch.tv PRIVMSG #channelone :hello").unwrap().unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let shared = store.clone();
        let handle = thread::spawn(move || {
            let _guard = shared.lock_db();
            started_tx.send(()).unwrap();
            thread::sleep(Duration::from_millis(150));
        });
        started_rx.recv().unwrap();

        let started = Instant::now();
        let outcome = store.insert_events_batch_low_priority(&[event]).unwrap();
        assert_eq!(outcome.inserted, 1);
        assert!(
            started.elapsed() >= Duration::from_millis(100),
            "low-priority event batch should wait for live work"
        );
        handle.join().unwrap();
    }

    #[test]
    fn insert_indexed_events_batch_low_priority_waits_for_live_work() {
        let (store, _temp) = test_store();
        let event = CanonicalEvent::from_raw("@badge-info=;badges=;color=#1E90FF;display-name=viewer;emotes=;id=batch-low-pri-2;room-id=1;subscriber=0;tmi-sent-ts=1704153605000;turbo=0;user-id=200;user-type= :viewer!viewer@viewer.tmi.twitch.tv PRIVMSG #channelone :hello indexed").unwrap().unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let shared = store.clone();
        let handle = thread::spawn(move || {
            let _guard = shared.lock_db();
            started_tx.send(()).unwrap();
            thread::sleep(Duration::from_millis(150));
        });
        started_rx.recv().unwrap();

        let started = Instant::now();
        let outcome = store
            .insert_indexed_events_batch_low_priority(&[(0, event)])
            .unwrap();
        assert_eq!(outcome.totals.inserted, 1);
        assert!(
            started.elapsed() >= Duration::from_millis(100),
            "low-priority indexed event batch should wait for live work"
        );
        handle.join().unwrap();
    }

    #[test]
    fn merge_imported_channel_days_low_priority_waits_for_live_work_and_completes() {
        let (store, _temp) = test_store();
        let event = CanonicalEvent::from_raw("@badge-info=;badges=;color=#1E90FF;display-name=viewer;emotes=;id=archive-low-pri-1;room-id=1;subscriber=0;tmi-sent-ts=1704153606000;turbo=0;user-id=200;user-type= :viewer!viewer@viewer.tmi.twitch.tv PRIVMSG #channelone :archive me").unwrap().unwrap();
        store.insert_event(&event).unwrap();
        let key = ChannelDayKey {
            channel_id: "1".to_string(),
            year: 2024,
            month: 1,
            day: 2,
        };

        let (started_tx, started_rx) = mpsc::channel();
        let shared = store.clone();
        let handle = thread::spawn(move || {
            let _guard = shared.lock_db();
            started_tx.send(()).unwrap();
            thread::sleep(Duration::from_millis(150));
        });
        started_rx.recv().unwrap();

        let started = Instant::now();
        store
            .merge_imported_channel_days_into_archives_low_priority(std::slice::from_ref(&key))
            .unwrap();
        assert!(
            started.elapsed() >= Duration::from_millis(100),
            "low-priority archive merge should wait for live work"
        );
        let archived = store
            .read_archived_channel_segment_strict("1", 2024, 1, 2)
            .unwrap();
        assert_eq!(archived.len(), 1);
        assert_eq!(archived[0].text, "archive me");
        handle.join().unwrap();
    }

    #[test]
    fn reclaim_database_space_reduces_free_pages_after_archival_delete() {
        let (store, _temp) = test_store();
        let key = ChannelDayKey {
            channel_id: "1".to_string(),
            year: 2024,
            month: 1,
            day: 2,
        };
        let payload = "x".repeat(4096);
        let mut events = Vec::new();
        for index in 0..6000usize {
            let raw = format!(
                "@badge-info=;badges=;color=#1E90FF;display-name=viewer;emotes=;id=db-size-{index};room-id=1;subscriber=0;tmi-sent-ts={};turbo=0;user-id=200;user-type= :viewer!viewer@viewer.tmi.twitch.tv PRIVMSG #channelone :{}",
                1_704_153_600_000i64 + index as i64,
                payload
            );
            events.push(CanonicalEvent::from_raw(&raw).unwrap().unwrap());
        }
        let outcome = store.insert_events_batch(&events).unwrap();
        assert_eq!(outcome.inserted, events.len());

        store.compact_channel_partition(&key).unwrap();
        let before = store.database_size_stats().unwrap();
        assert_eq!(store.event_count().unwrap(), 0);
        assert!(
            before.free_bytes >= DB_RECLAIM_INCREMENTAL_FREE_BYTES,
            "expected meaningful free space after compaction, stats={before:?}"
        );

        store.reclaim_database_space_low_priority().unwrap();
        let after = store.database_size_stats().unwrap();
        assert!(
            after.free_bytes < before.free_bytes || after.main_bytes < before.main_bytes,
            "database maintenance should reclaim space: before={before:?} after={after:?}"
        );
    }
}
