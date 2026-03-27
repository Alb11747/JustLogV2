use std::collections::HashMap;
use std::env;

use reqwest::Url;
use tracing::warn;

const DEFAULT_RECENT_MESSAGES_URL: &str = "https://recent-messages.robotty.de/api/v2/recent-messages";
const DEFAULT_RECENT_MESSAGES_LIMIT: usize = 800;

#[derive(Clone)]
pub struct RecentMessagesRuntime {
    enabled: bool,
    base_url: Option<String>,
    limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecentMessagesRuntimeSummary {
    pub enabled: bool,
    pub base_url: Option<String>,
    pub limit: usize,
    pub warnings: Vec<String>,
}

impl RecentMessagesRuntime {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            base_url: None,
            limit: DEFAULT_RECENT_MESSAGES_LIMIT,
        }
    }

    pub fn from_env() -> Self {
        let vars = env::vars().collect::<HashMap<_, _>>();
        Self::from_summary(Self::summary_from_map(&vars))
    }

    pub fn from_summary(summary: RecentMessagesRuntimeSummary) -> Self {
        for warning in &summary.warnings {
            warn!("{warning}");
        }
        Self {
            enabled: summary.enabled && summary.base_url.is_some(),
            base_url: summary.base_url,
            limit: summary.limit,
        }
    }

    pub fn summary_from_map(vars: &HashMap<String, String>) -> RecentMessagesRuntimeSummary {
        let enabled = env_flag(vars.get("JUSTLOG_RECENT_MESSAGES_ENABLED").map(String::as_str));
        let configured_url = vars
            .get("JUSTLOG_RECENT_MESSAGES_URL")
            .cloned()
            .unwrap_or_else(|| DEFAULT_RECENT_MESSAGES_URL.to_string());
        let mut warnings = Vec::new();
        let base_url = match normalize_base_url(&configured_url) {
            Ok(url) => Some(url),
            Err(error) => {
                warnings.push(format!(
                    "JUSTLOG_RECENT_MESSAGES_URL is invalid; recent-message backfill is disabled: {error}"
                ));
                None
            }
        };
        let limit = vars
            .get("JUSTLOG_RECENT_MESSAGES_LIMIT")
            .and_then(|value| value.trim().parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_RECENT_MESSAGES_LIMIT);
        if enabled && base_url.is_none() {
            warnings.push(
                "JUSTLOG_RECENT_MESSAGES_ENABLED is set but JUSTLOG_RECENT_MESSAGES_URL is unusable; recent-message backfill is disabled".to_string(),
            );
        }
        RecentMessagesRuntimeSummary {
            enabled,
            base_url,
            limit,
            warnings,
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn base_url(&self) -> Option<&str> {
        self.base_url.as_deref()
    }

    pub fn limit(&self) -> usize {
        self.limit
    }
}

fn normalize_base_url(value: &str) -> Result<String, String> {
    let trimmed = value.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err("empty URL".to_string());
    }
    let parsed = Url::parse(trimmed).map_err(|error| error.to_string())?;
    match parsed.scheme() {
        "http" | "https" => Ok(parsed.to_string().trim_end_matches('/').to_string()),
        scheme => Err(format!("unsupported scheme {scheme}")),
    }
}

fn env_flag(value: Option<&str>) -> bool {
    matches!(value, Some("1" | "true" | "TRUE" | "yes" | "YES"))
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_RECENT_MESSAGES_LIMIT, RecentMessagesRuntime};
    use std::collections::HashMap;

    #[test]
    fn summary_defaults_to_disabled_with_robotty_url() {
        let vars = HashMap::new();
        let summary = RecentMessagesRuntime::summary_from_map(&vars);
        assert!(!summary.enabled);
        assert_eq!(
            summary.base_url.as_deref(),
            Some("https://recent-messages.robotty.de/api/v2/recent-messages")
        );
        assert_eq!(summary.limit, DEFAULT_RECENT_MESSAGES_LIMIT);
        assert!(summary.warnings.is_empty());
    }

    #[test]
    fn summary_normalizes_url_and_limit() {
        let mut vars = HashMap::new();
        vars.insert("JUSTLOG_RECENT_MESSAGES_ENABLED".to_string(), "1".to_string());
        vars.insert(
            "JUSTLOG_RECENT_MESSAGES_URL".to_string(),
            " https://example.test/api/v2/recent-messages/ ".to_string(),
        );
        vars.insert("JUSTLOG_RECENT_MESSAGES_LIMIT".to_string(), "25".to_string());

        let summary = RecentMessagesRuntime::summary_from_map(&vars);
        assert!(summary.enabled);
        assert_eq!(
            summary.base_url.as_deref(),
            Some("https://example.test/api/v2/recent-messages")
        );
        assert_eq!(summary.limit, 25);
    }

    #[test]
    fn invalid_url_disables_runtime_with_warning() {
        let mut vars = HashMap::new();
        vars.insert("JUSTLOG_RECENT_MESSAGES_ENABLED".to_string(), "1".to_string());
        vars.insert("JUSTLOG_RECENT_MESSAGES_URL".to_string(), "notaurl".to_string());

        let summary = RecentMessagesRuntime::summary_from_map(&vars);
        assert!(summary.enabled);
        assert!(summary.base_url.is_none());
        assert!(!summary.warnings.is_empty());

        let runtime = RecentMessagesRuntime::from_summary(summary);
        assert!(!runtime.enabled());
    }

    #[test]
    fn disabled_runtime_stays_disabled() {
        let runtime = RecentMessagesRuntime::disabled();
        assert!(!runtime.enabled());
        assert!(runtime.base_url().is_none());
        assert_eq!(runtime.limit(), DEFAULT_RECENT_MESSAGES_LIMIT);
    }
}
