use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;
use std::{
    cell::Cell,
    ops::{Deref, DerefMut},
};

use anyhow::{Context, Result, anyhow};
use brotli::{CompressorWriter, Decompressor};
use chrono::{DateTime, Datelike, Duration, Utc};
use rand::Rng;
use rusqlite::{Connection, OptionalExtension, params};

use crate::config::Config;
use crate::debug_sync::RECONCILIATION_DELAY_SECONDS;
use crate::model::{
    CanonicalEvent, ChannelDayKey, ChannelLogFile, ChannelPartitionSummary, SegmentRecord,
    StoredEvent, UserLogFile, UserMonthKey, UserPartitionSummary,
};

thread_local! {
    static STORE_DB_LOCK_DEPTH: Cell<usize> = const { Cell::new(0) };
}

#[derive(Clone)]
pub struct Store {
    db: Arc<Mutex<Connection>>,
    root_dir: PathBuf,
    compression_quality: u32,
    compression_lgwin: u32,
    archive_enabled: bool,
}

struct StoreDbGuard<'a> {
    guard: MutexGuard<'a, Connection>,
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
        STORE_DB_LOCK_DEPTH.with(|depth| {
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

impl Store {
    fn lock_db(&self) -> StoreDbGuard<'_> {
        STORE_DB_LOCK_DEPTH.with(|depth| {
            assert!(
                depth.get() == 0,
                "re-entrant Store DB lock on the same thread; drop the outer db guard before calling another Store method"
            );
        });
        let guard = self.db.lock().unwrap();
        STORE_DB_LOCK_DEPTH.with(|depth| depth.set(depth.get() + 1));
        StoreDbGuard { guard }
    }

    pub fn open(config: &Config) -> Result<Self> {
        fs::create_dir_all(&config.logs_directory)?;
        let connection = Connection::open(&config.storage.sqlite_path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.busy_timeout(std::time::Duration::from_secs(30))?;

        let store = Self {
            db: Arc::new(Mutex::new(connection)),
            root_dir: config.logs_directory.clone(),
            compression_quality: config.compression.quality,
            compression_lgwin: config.compression.lgwin,
            archive_enabled: config.archive,
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
        for partition in
            self.compactable_channel_days(now - Duration::days(compact_after_channel_days))?
        {
            self.compact_channel_partition(&partition.key)?;
        }
        for partition in self.compactable_user_months(now, compact_after_user_months)? {
            self.compact_user_partition(&partition.key)?;
        }
        Ok(())
    }

    pub fn event_count(&self) -> Result<i64> {
        let db = self.lock_db();
        Ok(db.query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))?)
    }

    pub fn imported_raw_file_is_current(&self, path: &str, fingerprint: &str) -> Result<bool> {
        let lock_started = Instant::now();
        let db = self.lock_db();
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

    pub fn record_imported_raw_file(
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
        self.write_segment_file(&final_path, &temp_path, &events)?;
        let mut db = self.lock_db();
        let tx = db.transaction()?;
        tx.execute(
            r#"
            INSERT OR REPLACE INTO segments(scope, channel_id, user_id, year, month, day, path, line_count, start_ts, end_ts, compression, passthrough_raw)
            VALUES('channel', ?1, NULL, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'brotli', 1)
            "#,
            params![
                key.channel_id,
                key.year,
                key.month,
                key.day,
                relative,
                events.len() as i64,
                events.first().map(|event| event.timestamp.timestamp()).unwrap_or_default(),
                events.last().map(|event| event.timestamp.timestamp()).unwrap_or_default(),
            ],
        )?;
        tx.execute(
            r#"
            DELETE FROM events
            WHERE room_id = ?1
              AND strftime('%Y', timestamp_rfc3339) = printf('%04d', ?2)
              AND strftime('%m', timestamp_rfc3339) = printf('%02d', ?3)
              AND strftime('%d', timestamp_rfc3339) = printf('%02d', ?4)
            "#,
            params![key.channel_id, key.year, key.month, key.day],
        )?;
        tx.execute(
            "DELETE FROM pending_segments WHERE final_path = ?1",
            params![relative],
        )?;
        tx.commit()?;
        drop(db);
        self.schedule_reconciliation(
            &key.channel_id,
            key.year,
            key.month,
            key.day,
            &relative,
            Utc::now() + Duration::seconds(RECONCILIATION_DELAY_SECONDS),
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
        self.write_segment_file(&final_path, &temp_path, &events)?;
        let mut db = self.lock_db();
        let tx = db.transaction()?;
        tx.execute(
            r#"
            INSERT OR REPLACE INTO segments(scope, channel_id, user_id, year, month, day, path, line_count, start_ts, end_ts, compression, passthrough_raw)
            VALUES('user', ?1, ?2, ?3, ?4, NULL, ?5, ?6, ?7, ?8, 'brotli', 1)
            "#,
            params![
                key.channel_id,
                key.user_id,
                key.year,
                key.month,
                relative,
                events.len() as i64,
                events.first().map(|event| event.timestamp.timestamp()).unwrap_or_default(),
                events.last().map(|event| event.timestamp.timestamp()).unwrap_or_default(),
            ],
        )?;
        tx.execute(
            r#"
            DELETE FROM events
            WHERE room_id = ?1
              AND (user_id = ?2 OR target_user_id = ?2)
              AND strftime('%Y', timestamp_rfc3339) = printf('%04d', ?3)
              AND strftime('%m', timestamp_rfc3339) = printf('%02d', ?4)
            "#,
            params![key.channel_id, key.user_id, key.year, key.month],
        )?;
        tx.execute(
            "DELETE FROM pending_segments WHERE final_path = ?1",
            params![relative],
        )?;
        tx.commit()?;
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
        if let Some(parent) = temp_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = File::create(temp_path)?;
        let mut writer = CompressorWriter::new(
            file,
            64 * 1024,
            self.compression_quality,
            self.compression_lgwin,
        );
        for event in events {
            writer.write_all(event.raw.as_bytes())?;
            writer.write_all(b"\n")?;
        }
        writer.flush()?;
        drop(writer);
        if final_path.exists() {
            fs::remove_file(final_path)?;
        }
        fs::rename(temp_path, final_path)?;
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

fn choose_random(events: Vec<StoredEvent>) -> Result<Option<StoredEvent>> {
    if events.is_empty() {
        return Ok(None);
    }
    let mut rng = rand::rng();
    let index = rng.random_range(0..events.len());
    Ok(events.into_iter().nth(index))
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

    #[test]
    #[should_panic(
        expected = "re-entrant Store DB lock on the same thread; drop the outer db guard before calling another Store method"
    )]
    fn nested_db_lock_panics_instead_of_deadlocking() {
        let store = Store {
            db: Arc::new(Mutex::new(Connection::open_in_memory().unwrap())),
            root_dir: PathBuf::new(),
            compression_quality: 5,
            compression_lgwin: 22,
            archive_enabled: false,
        };

        let _outer = store.lock_db();
        let _inner = store.lock_db();
    }
}
