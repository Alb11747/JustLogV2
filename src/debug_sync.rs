use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow, bail};
use axum::body::Body;
use axum::http::header::{CACHE_CONTROL, CONTENT_ENCODING, CONTENT_TYPE, VARY};
use axum::http::{HeaderValue, Response, StatusCode, Uri};
use chrono::{DateTime, Datelike, Duration, Utc};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::model::{ChannelDayKey, StoredEvent, UserMonthKey};
use crate::store::{ReconciliationJob, ReconciliationOutcome, Store};

pub const RECONCILIATION_DELAY_SECONDS: i64 = 4 * 60 * 60;
const RECONCILIATION_SMALL_CONFLICT_LIMIT: usize = 5;
const WATERMARK_OVERLAP_DAYS: i64 = 1;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconciliationMode {
    Disabled,
    Compare,
    Trusted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StartupValidationMode {
    Disabled,
    Full,
    Relative(Duration),
}

#[derive(Clone)]
pub struct DebugRuntime {
    enabled: bool,
    compare_url: Option<String>,
    trusted_compare_url: Option<String>,
    fallback_trusted_api: bool,
    startup_validation_mode: StartupValidationMode,
    ignore_last_validated: bool,
    logger: Option<Arc<ReconciliationLogger>>,
    client: reqwest::Client,
    watermark_path: PathBuf,
}

#[derive(Clone)]
struct ReconciliationLogger {
    file: Arc<Mutex<File>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DebugRuntimeSummary {
    pub enabled: bool,
    pub compare_url: Option<String>,
    pub trusted_compare_url: Option<String>,
    pub fallback_trusted_api: bool,
    pub startup_validation_mode: StartupValidationMode,
    pub ignore_last_validated: bool,
    pub warnings: Vec<String>,
}

#[derive(Deserialize)]
struct RemoteChatLog {
    messages: Vec<RemoteChatMessage>,
}

#[derive(Deserialize)]
struct RemoteChatMessage {
    raw: String,
}

#[derive(Debug)]
struct ReconciliationDiff {
    missing_local: Vec<StoredEvent>,
    extra_local: Vec<StoredEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct LocalDayKey {
    channel_id: String,
    year: i32,
    month: u32,
    day: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ValidationWatermark {
    last_successful_validation_unix: i64,
}

impl DebugRuntime {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            compare_url: None,
            trusted_compare_url: None,
            fallback_trusted_api: false,
            startup_validation_mode: StartupValidationMode::Disabled,
            ignore_last_validated: false,
            logger: None,
            client: reqwest::Client::new(),
            watermark_path: PathBuf::from("./consistency-validation-watermark.json"),
        }
    }

    pub fn from_env(logs_directory: &Path) -> Result<Self> {
        let vars = env::vars().collect::<HashMap<_, _>>();
        let summary = Self::summary_from_map(&vars);
        Self::from_summary(logs_directory, summary)
    }

    pub fn from_summary(logs_directory: &Path, summary: DebugRuntimeSummary) -> Result<Self> {
        for warning in &summary.warnings {
            warn!("{warning}");
        }
        if summary.enabled && summary.compare_url.is_some() && summary.trusted_compare_url.is_some()
        {
            info!("JUSTLOG_DEBUG_TRUSTED_COMPARE_URL is set; trusted reconciliation mode wins");
        }
        let logger = if (summary.enabled && summary.compare_url.is_some())
            || summary.trusted_compare_url.is_some()
            || summary.startup_validation_mode != StartupValidationMode::Disabled
        {
            Some(Arc::new(ReconciliationLogger::open(
                &logs_directory.join("reconciliation.log"),
            )?))
        } else {
            None
        };
        Ok(Self {
            enabled: summary.enabled,
            compare_url: summary.compare_url,
            trusted_compare_url: summary.trusted_compare_url,
            fallback_trusted_api: summary.fallback_trusted_api,
            startup_validation_mode: summary.startup_validation_mode,
            ignore_last_validated: summary.ignore_last_validated,
            logger,
            client: reqwest::Client::new(),
            watermark_path: logs_directory.join("consistency-validation-watermark.json"),
        })
    }

    pub fn summary_from_map(vars: &HashMap<String, String>) -> DebugRuntimeSummary {
        let enabled = env_flag(vars.get("JUSTLOG_DEBUG").map(String::as_str));
        let compare_url = normalize_base_url(vars.get("JUSTLOG_DEBUG_COMPARE_URL").cloned());
        let trusted_compare_url =
            normalize_base_url(vars.get("JUSTLOG_DEBUG_TRUSTED_COMPARE_URL").cloned());
        let fallback_requested = env_flag(
            vars.get("JUSTLOG_DEBUG_FALLBACK_TRUSTED_API")
                .map(String::as_str),
        );
        let startup_validation_mode = parse_startup_validation_mode(
            vars.get("JUSTLOG_DEBUG_VALIDATE_CONSISTENCY_ON_STARTUP")
                .map(String::as_str),
        );
        let ignore_last_validated = env_flag(
            vars.get("JUSTLOG_DEBUG_VALIDATE_CONSISTENCY_ON_STARTUP_IGNORE_LAST_VALIDATED")
                .map(String::as_str),
        );
        let mut warnings = Vec::new();
        let fallback_trusted_api = if fallback_requested && trusted_compare_url.is_none() {
            warnings.push(
                "JUSTLOG_DEBUG_FALLBACK_TRUSTED_API is enabled but JUSTLOG_DEBUG_TRUSTED_COMPARE_URL is missing; fallback is disabled".to_string(),
            );
            false
        } else {
            fallback_requested
        };
        if vars
            .get("JUSTLOG_DEBUG_VALIDATE_CONSISTENCY_ON_STARTUP")
            .is_some()
            && startup_validation_mode == StartupValidationMode::Disabled
        {
            warnings.push(
                "JUSTLOG_DEBUG_VALIDATE_CONSISTENCY_ON_STARTUP is invalid; startup validation is disabled".to_string(),
            );
        }
        DebugRuntimeSummary {
            enabled,
            compare_url,
            trusted_compare_url,
            fallback_trusted_api,
            startup_validation_mode,
            ignore_last_validated,
            warnings,
        }
    }

    pub fn reconciliation_mode(&self) -> ReconciliationMode {
        if !self.enabled {
            return ReconciliationMode::Disabled;
        }
        if self.trusted_compare_url.is_some() {
            ReconciliationMode::Trusted
        } else if self.compare_url.is_some() {
            ReconciliationMode::Compare
        } else {
            ReconciliationMode::Disabled
        }
    }

    pub fn reconciliation_enabled(&self) -> bool {
        self.reconciliation_mode() != ReconciliationMode::Disabled
    }

    pub fn fallback_enabled(&self) -> bool {
        self.fallback_trusted_api && self.trusted_compare_url.is_some()
    }

    pub fn trusted_base_url(&self) -> Option<&str> {
        self.trusted_compare_url.as_deref()
    }

    pub fn startup_validation_enabled(&self) -> bool {
        self.startup_validation_mode != StartupValidationMode::Disabled
    }

    pub fn startup_validation_mode(&self) -> &StartupValidationMode {
        &self.startup_validation_mode
    }

    pub fn ignore_last_validated(&self) -> bool {
        self.ignore_last_validated
    }

    fn compare_base_url(&self) -> Option<&str> {
        self.trusted_compare_url
            .as_deref()
            .or(self.compare_url.as_deref())
    }

    pub fn log_check(&self, level: &str, message: &str) {
        if let Some(logger) = &self.logger {
            let _ = logger.write_line(level, message);
        }
    }

    pub fn read_last_validated_time(&self) -> Result<Option<DateTime<Utc>>> {
        if !self.watermark_path.exists() {
            return Ok(None);
        }
        let content = fs::read_to_string(&self.watermark_path)
            .with_context(|| format!("failed to read {}", self.watermark_path.display()))?;
        let watermark: ValidationWatermark = serde_json::from_str(&content)?;
        Ok(DateTime::<Utc>::from_timestamp(
            watermark.last_successful_validation_unix,
            0,
        ))
    }

    pub fn persist_last_validated_time(&self, now: DateTime<Utc>) -> Result<()> {
        if let Some(parent) = self.watermark_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let payload = ValidationWatermark {
            last_successful_validation_unix: now.timestamp(),
        };
        fs::write(&self.watermark_path, serde_json::to_vec_pretty(&payload)?)?;
        Ok(())
    }

    pub fn effective_validation_start(&self, now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>> {
        let requested_start = match self.startup_validation_mode {
            StartupValidationMode::Disabled => return Ok(None),
            StartupValidationMode::Full => None,
            StartupValidationMode::Relative(duration) => Some(now - duration),
        };
        if self.ignore_last_validated {
            return Ok(requested_start);
        }
        let watermark_start = self
            .read_last_validated_time()?
            .map(|value| value - Duration::days(WATERMARK_OVERLAP_DAYS));
        Ok(match (requested_start, watermark_start) {
            (Some(requested), Some(watermark)) => Some(if requested > watermark {
                requested
            } else {
                watermark
            }),
            (Some(requested), None) => Some(requested),
            (None, Some(watermark)) => Some(watermark),
            (None, None) => None,
        })
    }

    pub async fn proxy_fallback_request(
        &self,
        uri: &Uri,
        accept_encoding: &str,
        content_type: &str,
    ) -> Result<Response<Body>> {
        let Some(base_url) = self.trusted_base_url() else {
            return Err(anyhow!("trusted fallback URL is not configured"));
        };
        let path = uri
            .path_and_query()
            .map(|value| value.as_str())
            .unwrap_or_else(|| uri.path());
        let response = self
            .client
            .get(format!("{base_url}{path}"))
            .header("accept-encoding", accept_encoding)
            .header("content-type", content_type)
            .send()
            .await
            .with_context(|| format!("failed to proxy request to trusted justlog for {path}"))?;
        let status = StatusCode::from_u16(response.status().as_u16())?;
        let headers = response.headers().clone();
        let body = response.bytes().await?;
        let mut proxied = Response::new(Body::from(body));
        *proxied.status_mut() = status;
        copy_header_if_present(&headers, &mut proxied, CONTENT_TYPE);
        copy_header_if_present(&headers, &mut proxied, CONTENT_ENCODING);
        copy_header_if_present(&headers, &mut proxied, CACHE_CONTROL);
        copy_header_if_present(&headers, &mut proxied, VARY);
        Ok(proxied)
    }
}

impl ReconciliationLogger {
    fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            file: Arc::new(Mutex::new(file)),
        })
    }

    fn write_line(&self, level: &str, message: &str) -> Result<()> {
        let timestamp = Utc::now().to_rfc3339();
        let mut file = self.file.lock().unwrap();
        writeln!(file, "{timestamp} [{level}] {message}")?;
        file.flush()?;
        Ok(())
    }
}

pub async fn run_startup_validation(
    runtime: Arc<DebugRuntime>,
    store: Store,
    now: DateTime<Utc>,
) -> Result<()> {
    if !runtime.startup_validation_enabled() {
        return Ok(());
    }
    let effective_start = runtime.effective_validation_start(now)?;
    runtime.log_check(
        "INFO",
        &format!(
            "startup consistency validation begin: mode={:?} effective_start={:?} ignore_last_validated={}",
            runtime.startup_validation_mode(),
            effective_start,
            runtime.ignore_last_validated()
        ),
    );

    let candidate_days = collect_candidate_days(&store, effective_start)?;
    for key in &candidate_days {
        if let Err(error) = validate_single_day(runtime.clone(), store.clone(), key).await {
            runtime.log_check(
                "ERROR",
                &format!(
                    "startup validation failed for channel-day {}/{}/{}/{}: {}",
                    key.channel_id, key.year, key.month, key.day, error
                ),
            );
            return Err(error);
        }
    }

    runtime.persist_last_validated_time(now)?;
    runtime.log_check(
        "INFO",
        &format!(
            "startup consistency validation complete: validated {} channel-day partitions",
            candidate_days.len()
        ),
    );
    Ok(())
}

pub async fn process_due_reconciliation_jobs(
    runtime: Arc<DebugRuntime>,
    store: Store,
    now: DateTime<Utc>,
) -> Result<()> {
    if !runtime.reconciliation_enabled() {
        return Ok(());
    }
    for job in store.due_reconciliation_jobs(now)? {
        if let Err(error) =
            process_reconciliation_job(runtime.clone(), store.clone(), job.clone()).await
        {
            let message = error.to_string();
            warn!("reconciliation job failed: {error}");
            runtime.log_check(
                "ERROR",
                &format!(
                    "reconciliation error for channel-day {}/{}/{}/{}: {}",
                    job.channel_id, job.year, job.month, job.day, message
                ),
            );
            let _ = store.mark_reconciliation_error(&job, &message);
        }
    }
    Ok(())
}

pub async fn process_reconciliation_job(
    runtime: Arc<DebugRuntime>,
    store: Store,
    job: ReconciliationJob,
) -> Result<()> {
    let mode = runtime.reconciliation_mode();
    if mode == ReconciliationMode::Disabled {
        return Ok(());
    }
    let base_url = runtime
        .compare_base_url()
        .ok_or_else(|| anyhow!("compare URL is not configured"))?;
    let local_events = store.read_channel_logs(&job.channel_id, job.year, job.month, job.day)?;
    let remote_events = fetch_remote_day_events(
        &runtime.client,
        base_url,
        &job.channel_id,
        job.year,
        job.month,
        job.day,
    )
    .await?;
    let diff = diff_events(&local_events, &remote_events);
    let mut status = "clean".to_string();
    let mut repair_status = "none".to_string();
    let mut unhealthy = 0_i64;
    let conflict_count = (diff.missing_local.len() + diff.extra_local.len()) as i64;

    runtime.log_check(
        "INFO",
        &format!(
            "checked channel-day {}/{}/{}/{} from {}",
            job.channel_id, job.year, job.month, job.day, job.segment_path
        ),
    );

    if !diff.missing_local.is_empty() || !diff.extra_local.is_empty() {
        status = "conflict".to_string();
        unhealthy = if mode == ReconciliationMode::Trusted {
            0
        } else {
            1
        };
        let summary = build_conflict_summary(&diff);
        runtime.log_check(
            "WARN",
            &format!(
                "conflict for channel-day {}/{}/{}/{}: {}",
                job.channel_id, job.year, job.month, job.day, summary
            ),
        );
        warn!(
            "reconciliation conflict for channel-day {}/{}/{}/{}: {}",
            job.channel_id, job.year, job.month, job.day, summary
        );
        if mode == ReconciliationMode::Trusted && !diff.missing_local.is_empty() {
            sync_channel_and_users_from_events(&store, &remote_events)?;
            repair_status = "repaired".to_string();
            status = "repaired".to_string();
            runtime.log_check(
                "WARN",
                &format!(
                    "repaired channel-day {}/{}/{}/{} with {} remote events",
                    job.channel_id,
                    job.year,
                    job.month,
                    job.day,
                    remote_events.len()
                ),
            );
        }
    }

    store.record_reconciliation_outcome(
        &job,
        &ReconciliationOutcome {
            checked_at: Utc::now(),
            status,
            conflict_count,
            repair_status,
            unhealthy,
            last_error: None,
        },
    )?;
    Ok(())
}

async fn validate_single_day(
    runtime: Arc<DebugRuntime>,
    store: Store,
    key: &LocalDayKey,
) -> Result<()> {
    let channel_key = ChannelDayKey {
        channel_id: key.channel_id.clone(),
        year: key.year,
        month: key.month,
        day: key.day,
    };
    let mut authoritative = BTreeMap::new();
    let mut corruption_detected = false;

    match store.read_archived_channel_segment_strict(&key.channel_id, key.year, key.month, key.day)
    {
        Ok(events) => {
            for event in events {
                authoritative.insert(event_identity(&event), event);
            }
        }
        Err(error) => {
            corruption_detected = true;
            runtime.log_check(
                "WARN",
                &format!(
                    "channel-day {}/{}/{}/{} failed local sanity check: {}",
                    key.channel_id, key.year, key.month, key.day, error
                ),
            );
        }
    }

    let user_segments = store.list_user_segments_since(None)?;
    for segment in user_segments
        .into_iter()
        .filter(|segment| segment.channel_id == key.channel_id)
    {
        let Some(user_id) = segment.user_id.clone() else {
            continue;
        };
        match store.read_archived_user_segment_strict(
            &segment.channel_id,
            &user_id,
            segment.year,
            segment.month,
        ) {
            Ok(events) => {
                for event in events.into_iter().filter(|event| {
                    event.timestamp.year() == key.year
                        && event.timestamp.month() == key.month
                        && event.timestamp.day() == key.day
                }) {
                    authoritative.insert(event_identity(&event), event);
                }
            }
            Err(error) => {
                if segment.year == key.year && segment.month == key.month {
                    corruption_detected = true;
                    runtime.log_check(
                        "WARN",
                        &format!(
                            "user-month {}/{}/{}/{} failed local sanity check: {}",
                            segment.channel_id, user_id, segment.year, segment.month, error
                        ),
                    );
                }
            }
        }
    }

    let authoritative_events = authoritative.into_values().collect::<Vec<_>>();
    if authoritative_events.is_empty() {
        return Ok(());
    }

    let local_channel_events = store
        .read_channel_logs(&key.channel_id, key.year, key.month, key.day)
        .unwrap_or_default();
    let local_channel_diff = diff_events(&local_channel_events, &authoritative_events);
    let channel_mismatch =
        !local_channel_diff.missing_local.is_empty() || !local_channel_diff.extra_local.is_empty();

    let repaired_any =
        if corruption_detected && runtime.reconciliation_mode() == ReconciliationMode::Trusted {
            let remote_events = fetch_remote_day_events(
                &runtime.client,
                runtime
                    .compare_base_url()
                    .ok_or_else(|| anyhow!("compare URL is not configured"))?,
                &key.channel_id,
                key.year,
                key.month,
                key.day,
            )
            .await?;
            sync_channel_and_users_from_events(&store, &remote_events)?;
            true
        } else if corruption_detected || channel_mismatch {
            sync_channel_and_users_from_events(&store, &authoritative_events)?;
            true
        } else {
            sync_user_months_from_authoritative(&store, &channel_key, &authoritative_events)?;
            false
        };

    let segment_path = format!(
        "segments/channel/{}/{}/{}/{}.br",
        key.channel_id, key.year, key.month, key.day
    );
    store.upsert_reconciliation_status(
        &key.channel_id,
        key.year,
        key.month,
        key.day,
        &segment_path,
        &ReconciliationOutcome {
            checked_at: Utc::now(),
            status: if repaired_any {
                "startup-repaired".to_string()
            } else {
                "startup-clean".to_string()
            },
            conflict_count: if repaired_any { 1 } else { 0 },
            repair_status: if repaired_any {
                "startup-repaired".to_string()
            } else {
                "none".to_string()
            },
            unhealthy: 0,
            last_error: None,
        },
    )?;
    runtime.log_check(
        "INFO",
        &format!(
            "startup validated channel-day {}/{}/{}/{} repaired={}",
            key.channel_id, key.year, key.month, key.day, repaired_any
        ),
    );
    Ok(())
}

fn collect_candidate_days(store: &Store, start: Option<DateTime<Utc>>) -> Result<Vec<LocalDayKey>> {
    let mut keys = BTreeSet::new();
    for segment in store.list_channel_segments_since(start)? {
        if let Some(day) = segment.day {
            keys.insert(LocalDayKey {
                channel_id: segment.channel_id,
                year: segment.year,
                month: segment.month,
                day,
            });
        }
    }
    for segment in store.list_user_segments_since(start)? {
        let Some(user_id) = segment.user_id.clone() else {
            continue;
        };
        let events = match store.read_archived_user_segment_strict(
            &segment.channel_id,
            &user_id,
            segment.year,
            segment.month,
        ) {
            Ok(events) => events,
            Err(_) => continue,
        };
        for event in events {
            keys.insert(LocalDayKey {
                channel_id: event.room_id.clone(),
                year: event.timestamp.year(),
                month: event.timestamp.month(),
                day: event.timestamp.day(),
            });
        }
    }
    Ok(keys.into_iter().collect())
}

fn sync_channel_and_users_from_events(
    store: &Store,
    authoritative_events: &[StoredEvent],
) -> Result<()> {
    let Some(first) = authoritative_events.first() else {
        return Ok(());
    };
    store.replace_or_create_channel_segment(
        &first.room_id,
        first.timestamp.year(),
        first.timestamp.month(),
        first.timestamp.day(),
        authoritative_events,
    )?;
    sync_user_months_from_authoritative(
        store,
        &ChannelDayKey {
            channel_id: first.room_id.clone(),
            year: first.timestamp.year(),
            month: first.timestamp.month(),
            day: first.timestamp.day(),
        },
        authoritative_events,
    )?;
    Ok(())
}

fn sync_user_months_from_authoritative(
    store: &Store,
    day_key: &ChannelDayKey,
    authoritative_events: &[StoredEvent],
) -> Result<()> {
    let mut by_user = BTreeMap::<UserMonthKey, Vec<StoredEvent>>::new();
    for event in authoritative_events {
        for user_key in user_month_keys_for_event(event) {
            by_user.entry(user_key).or_default().push(event.clone());
        }
    }
    for (user_key, day_events) in by_user {
        let mut month_events = store
            .read_archived_user_segment_strict(
                &user_key.channel_id,
                &user_key.user_id,
                user_key.year,
                user_key.month,
            )
            .unwrap_or_default()
            .into_iter()
            .filter(|event| {
                !(event.timestamp.year() == day_key.year
                    && event.timestamp.month() == day_key.month
                    && event.timestamp.day() == day_key.day)
            })
            .collect::<Vec<_>>();
        month_events.extend(
            day_events
                .into_iter()
                .filter(|event| event_applies_to_user(event, &user_key.user_id)),
        );
        dedupe_and_sort(&mut month_events);
        if !month_events.is_empty() {
            store.replace_or_create_user_segment(
                &user_key.channel_id,
                &user_key.user_id,
                user_key.year,
                user_key.month,
                &month_events,
            )?;
        }
    }
    Ok(())
}

fn dedupe_and_sort(events: &mut Vec<StoredEvent>) {
    let mut seen = BTreeSet::new();
    events.retain(|event| seen.insert(event_identity(event)));
    events.sort_by_key(|event| (event.timestamp.timestamp(), event.event_uid.clone()));
    for (index, event) in events.iter_mut().enumerate() {
        event.seq = index as i64;
    }
}

fn user_month_keys_for_event(event: &StoredEvent) -> Vec<UserMonthKey> {
    let mut keys = Vec::new();
    if let Some(user_id) = event.user_id.clone() {
        keys.push(UserMonthKey {
            channel_id: event.room_id.clone(),
            user_id,
            year: event.timestamp.year(),
            month: event.timestamp.month(),
        });
    }
    if let Some(user_id) = event.target_user_id.clone() {
        keys.push(UserMonthKey {
            channel_id: event.room_id.clone(),
            user_id,
            year: event.timestamp.year(),
            month: event.timestamp.month(),
        });
    }
    keys.sort_by(|left, right| left.user_id.cmp(&right.user_id));
    keys.dedup_by(|left, right| left.user_id == right.user_id);
    keys
}

fn event_applies_to_user(event: &StoredEvent, user_id: &str) -> bool {
    event.user_id.as_deref() == Some(user_id) || event.target_user_id.as_deref() == Some(user_id)
}

fn build_conflict_summary(diff: &ReconciliationDiff) -> String {
    let mut parts = Vec::new();
    if !diff.missing_local.is_empty() {
        parts.push(format!(
            "missing locally: {}",
            summarize_events(&diff.missing_local)
        ));
    }
    if !diff.extra_local.is_empty() {
        parts.push(format!(
            "extra locally: {}",
            summarize_events(&diff.extra_local)
        ));
    }
    parts.join("; ")
}

fn diff_events(local_events: &[StoredEvent], remote_events: &[StoredEvent]) -> ReconciliationDiff {
    let local_keys = local_events
        .iter()
        .map(event_identity)
        .collect::<BTreeSet<_>>();
    let remote_keys = remote_events
        .iter()
        .map(event_identity)
        .collect::<BTreeSet<_>>();
    let missing_local = remote_events
        .iter()
        .filter(|event| !local_keys.contains(&event_identity(event)))
        .cloned()
        .collect();
    let extra_local = local_events
        .iter()
        .filter(|event| !remote_keys.contains(&event_identity(event)))
        .cloned()
        .collect();
    ReconciliationDiff {
        missing_local,
        extra_local,
    }
}

fn summarize_events(events: &[StoredEvent]) -> String {
    if events.len() <= RECONCILIATION_SMALL_CONFLICT_LIMIT {
        return events
            .iter()
            .map(describe_event)
            .collect::<Vec<_>>()
            .join(", ");
    }
    let mut groups = Vec::new();
    let mut start = 0_usize;
    while start < events.len() {
        let mut end = start;
        while end + 1 < events.len() && events[end + 1].seq == events[end].seq + 1 {
            end += 1;
        }
        if start == end {
            groups.push(describe_event(&events[start]));
        } else {
            groups.push(format!(
                "{} through {}",
                describe_event(&events[start]),
                describe_event(&events[end])
            ));
        }
        start = end + 1;
    }
    groups.join(", ")
}

fn describe_event(event: &StoredEvent) -> String {
    format!(
        "{}@{}",
        event.event_uid,
        event.timestamp.format("%Y-%m-%d %H:%M:%S")
    )
}

fn event_identity(event: &StoredEvent) -> String {
    if !event.event_uid.is_empty() {
        return event.event_uid.clone();
    }
    format!(
        "{}:{}:{}:{}",
        event.timestamp.timestamp(),
        event.kind,
        event.room_id,
        event.raw
    )
}

async fn fetch_remote_day_events(
    client: &reqwest::Client,
    base_url: &str,
    channel_id: &str,
    year: i32,
    month: u32,
    day: u32,
) -> Result<Vec<StoredEvent>> {
    let response = client
        .get(format!(
            "{base_url}/channelid/{channel_id}/{year}/{month}/{day}?json=1"
        ))
        .send()
        .await
        .with_context(|| {
            format!("failed to fetch remote logs for channel-day {channel_id}/{year}/{month}/{day}")
        })?
        .error_for_status()?;
    let body = response.json::<RemoteChatLog>().await?;
    body.messages
        .into_iter()
        .enumerate()
        .map(|(index, message)| {
            let Some(event) = crate::model::CanonicalEvent::from_raw(&message.raw)? else {
                return Err(anyhow!(
                    "remote message could not be parsed as a canonical event"
                ));
            };
            Ok(StoredEvent {
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
            })
        })
        .collect()
}

fn parse_startup_validation_mode(value: Option<&str>) -> StartupValidationMode {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return StartupValidationMode::Disabled;
    };
    if value.eq_ignore_ascii_case("true") {
        return StartupValidationMode::Full;
    }
    match parse_relative_duration(value) {
        Ok(duration) => StartupValidationMode::Relative(duration),
        Err(_) => StartupValidationMode::Disabled,
    }
}

fn parse_relative_duration(value: &str) -> Result<Duration> {
    let suffixes = [("mo", 31_i64), ("y", 365_i64), ("d", 1_i64), ("h", 0_i64)];
    for (suffix, days) in suffixes {
        if let Some(number) = value.strip_suffix(suffix) {
            let amount = number.parse::<i64>()?;
            if amount <= 0 {
                bail!("relative duration must be positive");
            }
            return if suffix == "h" {
                Ok(Duration::hours(amount))
            } else {
                Ok(Duration::days(amount * days))
            };
        }
    }
    bail!("unsupported relative duration")
}

fn normalize_base_url(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().trim_end_matches('/').to_string())
        .filter(|value| !value.is_empty())
}

fn env_flag(value: Option<&str>) -> bool {
    matches!(value, Some("1" | "true" | "TRUE" | "yes" | "YES"))
}

fn copy_header_if_present(
    headers: &reqwest::header::HeaderMap,
    response: &mut Response<Body>,
    header: axum::http::header::HeaderName,
) {
    if let Some(value) = headers.get(header.as_str()) {
        if let Ok(value) = HeaderValue::from_bytes(value.as_bytes()) {
            response.headers_mut().insert(header, value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DebugRuntime, StartupValidationMode, parse_relative_duration};
    use chrono::{Duration, Utc};
    use std::collections::HashMap;
    use tempfile::TempDir;

    #[test]
    fn summary_defaults_to_disabled() {
        let vars = HashMap::new();
        let summary = DebugRuntime::summary_from_map(&vars);
        assert!(!summary.enabled);
        assert!(summary.compare_url.is_none());
        assert!(summary.trusted_compare_url.is_none());
        assert!(!summary.fallback_trusted_api);
        assert_eq!(
            summary.startup_validation_mode,
            StartupValidationMode::Disabled
        );
    }

    #[test]
    fn summary_supports_compare_mode() {
        let mut vars = HashMap::new();
        vars.insert("JUSTLOG_DEBUG".to_string(), "1".to_string());
        vars.insert(
            "JUSTLOG_DEBUG_COMPARE_URL".to_string(),
            "http://example.test/".to_string(),
        );
        let summary = DebugRuntime::summary_from_map(&vars);
        assert!(summary.enabled);
        assert_eq!(summary.compare_url.as_deref(), Some("http://example.test"));
    }

    #[test]
    fn summary_supports_trusted_mode() {
        let mut vars = HashMap::new();
        vars.insert("JUSTLOG_DEBUG".to_string(), "1".to_string());
        vars.insert(
            "JUSTLOG_DEBUG_TRUSTED_COMPARE_URL".to_string(),
            "http://trusted.test/".to_string(),
        );
        let summary = DebugRuntime::summary_from_map(&vars);
        assert!(summary.enabled);
        assert_eq!(
            summary.trusted_compare_url.as_deref(),
            Some("http://trusted.test")
        );
    }

    #[test]
    fn fallback_requires_trusted_url() {
        let mut vars = HashMap::new();
        vars.insert(
            "JUSTLOG_DEBUG_FALLBACK_TRUSTED_API".to_string(),
            "1".to_string(),
        );
        let summary = DebugRuntime::summary_from_map(&vars);
        assert!(!summary.fallback_trusted_api);
        assert_eq!(summary.warnings.len(), 1);
    }

    #[test]
    fn startup_validation_supports_true_and_relative_values() {
        let mut vars = HashMap::new();
        vars.insert(
            "JUSTLOG_DEBUG_VALIDATE_CONSISTENCY_ON_STARTUP".to_string(),
            "true".to_string(),
        );
        let summary = DebugRuntime::summary_from_map(&vars);
        assert_eq!(summary.startup_validation_mode, StartupValidationMode::Full);

        vars.insert(
            "JUSTLOG_DEBUG_VALIDATE_CONSISTENCY_ON_STARTUP".to_string(),
            "7d".to_string(),
        );
        let summary = DebugRuntime::summary_from_map(&vars);
        assert_eq!(
            summary.startup_validation_mode,
            StartupValidationMode::Relative(Duration::days(7))
        );
    }

    #[test]
    fn startup_validation_ignore_last_validated_flag_parses() {
        let mut vars = HashMap::new();
        vars.insert(
            "JUSTLOG_DEBUG_VALIDATE_CONSISTENCY_ON_STARTUP_IGNORE_LAST_VALIDATED".to_string(),
            "1".to_string(),
        );
        let summary = DebugRuntime::summary_from_map(&vars);
        assert!(summary.ignore_last_validated);
    }

    #[test]
    fn relative_duration_parser_supports_all_units() {
        assert_eq!(parse_relative_duration("24h").unwrap(), Duration::hours(24));
        assert_eq!(parse_relative_duration("7d").unwrap(), Duration::days(7));
        assert_eq!(parse_relative_duration("3mo").unwrap(), Duration::days(93));
        assert_eq!(parse_relative_duration("1y").unwrap(), Duration::days(365));
    }

    #[test]
    fn effective_validation_start_uses_watermark_overlap() {
        let temp = TempDir::new().unwrap();
        let mut vars = HashMap::new();
        vars.insert(
            "JUSTLOG_DEBUG_VALIDATE_CONSISTENCY_ON_STARTUP".to_string(),
            "7d".to_string(),
        );
        let runtime =
            DebugRuntime::from_summary(temp.path(), DebugRuntime::summary_from_map(&vars)).unwrap();
        let now = Utc::now();
        runtime
            .persist_last_validated_time(now - Duration::hours(12))
            .unwrap();
        let effective = runtime.effective_validation_start(now).unwrap().unwrap();
        assert!(effective > now - Duration::days(7));
        assert!(effective <= now - Duration::hours(12) - Duration::hours(23));
    }
}
