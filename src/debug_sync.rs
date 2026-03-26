use std::collections::{BTreeSet, HashMap};
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, anyhow};
use axum::body::Body;
use axum::http::header::{CACHE_CONTROL, CONTENT_ENCODING, CONTENT_TYPE, VARY};
use axum::http::{HeaderValue, Response, StatusCode, Uri};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use tracing::{info, warn};

use crate::model::StoredEvent;
use crate::store::{ReconciliationJob, ReconciliationOutcome, Store};

pub const RECONCILIATION_DELAY_SECONDS: i64 = 4 * 60 * 60;
const RECONCILIATION_SMALL_CONFLICT_LIMIT: usize = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReconciliationMode {
    Disabled,
    Compare,
    Trusted,
}

#[derive(Clone)]
pub struct DebugRuntime {
    enabled: bool,
    compare_url: Option<String>,
    trusted_compare_url: Option<String>,
    fallback_trusted_api: bool,
    logger: Option<Arc<ReconciliationLogger>>,
    client: reqwest::Client,
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

impl DebugRuntime {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            compare_url: None,
            trusted_compare_url: None,
            fallback_trusted_api: false,
            logger: None,
            client: reqwest::Client::new(),
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
        if summary.enabled
            && summary.compare_url.is_some()
            && summary.trusted_compare_url.is_some()
        {
            info!("JUSTLOG_DEBUG_TRUSTED_COMPARE_URL is set; trusted reconciliation mode wins");
        }
        let logger = if summary.enabled && summary.compare_url.is_some() || summary.trusted_compare_url.is_some() {
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
            logger,
            client: reqwest::Client::new(),
        })
    }

    pub fn summary_from_map(vars: &HashMap<String, String>) -> DebugRuntimeSummary {
        let enabled = env_flag(vars.get("JUSTLOG_DEBUG").map(String::as_str));
        let compare_url = normalize_base_url(vars.get("JUSTLOG_DEBUG_COMPARE_URL").cloned());
        let trusted_compare_url =
            normalize_base_url(vars.get("JUSTLOG_DEBUG_TRUSTED_COMPARE_URL").cloned());
        let fallback_requested =
            env_flag(vars.get("JUSTLOG_DEBUG_FALLBACK_TRUSTED_API").map(String::as_str));
        let mut warnings = Vec::new();
        let fallback_trusted_api = if fallback_requested && trusted_compare_url.is_none() {
            warnings.push(
                "JUSTLOG_DEBUG_FALLBACK_TRUSTED_API is enabled but JUSTLOG_DEBUG_TRUSTED_COMPARE_URL is missing; fallback is disabled".to_string(),
            );
            false
        } else {
            fallback_requested
        };
        DebugRuntimeSummary {
            enabled,
            compare_url,
            trusted_compare_url,
            fallback_trusted_api,
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
    let local_events = store.read_channel_logs(
        &job.channel_id,
        job.year,
        job.month,
        job.day,
    )?;
    let remote_events =
        fetch_remote_day_events(&runtime.client, base_url, &job.channel_id, job.year, job.month, job.day)
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
        unhealthy = if mode == ReconciliationMode::Trusted { 0 } else { 1 };
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
            let repaired = merge_events(&local_events, &remote_events);
            store.replace_channel_segment(
                &job.channel_id,
                job.year,
                job.month,
                job.day,
                &repaired,
            )?;
            repair_status = "repaired".to_string();
            status = "repaired".to_string();
            runtime.log_check(
                "WARN",
                &format!(
                    "repaired channel-day {}/{}/{}/{} with {} merged events",
                    job.channel_id,
                    job.year,
                    job.month,
                    job.day,
                    repaired.len()
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

fn build_conflict_summary(diff: &ReconciliationDiff) -> String {
    let mut parts = Vec::new();
    if !diff.missing_local.is_empty() {
        parts.push(format!(
            "missing locally: {}",
            summarize_events(&diff.missing_local)
        ));
    }
    if !diff.extra_local.is_empty() {
        parts.push(format!("extra locally: {}", summarize_events(&diff.extra_local)));
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

fn merge_events(local_events: &[StoredEvent], remote_events: &[StoredEvent]) -> Vec<StoredEvent> {
    let mut merged = Vec::new();
    let mut seen = BTreeSet::new();
    for event in local_events.iter().chain(remote_events.iter()) {
        let key = event_identity(event);
        if seen.insert(key) {
            merged.push(event.clone());
        }
    }
    merged.sort_by_key(|event| (event.timestamp.timestamp(), event.event_uid.clone()));
    for (index, event) in merged.iter_mut().enumerate() {
        event.seq = index as i64;
    }
    merged
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
            format!(
                "failed to fetch remote logs for channel-day {channel_id}/{year}/{month}/{day}"
            )
        })?
        .error_for_status()?;
    let body = response.json::<RemoteChatLog>().await?;
    body.messages
        .into_iter()
        .enumerate()
        .map(|(index, message)| {
            let Some(event) = crate::model::CanonicalEvent::from_raw(&message.raw)? else {
                return Err(anyhow!("remote message could not be parsed as a canonical event"));
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
    use super::DebugRuntime;
    use std::collections::HashMap;

    #[test]
    fn summary_defaults_to_disabled() {
        let vars = HashMap::new();
        let summary = DebugRuntime::summary_from_map(&vars);
        assert!(!summary.enabled);
        assert!(summary.compare_url.is_none());
        assert!(summary.trusted_compare_url.is_none());
        assert!(!summary.fallback_trusted_api);
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
        assert!(summary.trusted_compare_url.is_none());
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
}
