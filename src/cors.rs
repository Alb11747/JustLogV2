use std::collections::HashMap;
use std::env;

use axum::body::Body;
use axum::http::header::{
    ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
    ACCESS_CONTROL_EXPOSE_HEADERS, ACCESS_CONTROL_MAX_AGE, ACCESS_CONTROL_REQUEST_HEADERS,
    ACCESS_CONTROL_REQUEST_METHOD, ORIGIN, VARY,
};
use axum::http::{HeaderMap, HeaderValue, Method, Response, StatusCode};
use tracing::warn;

const DEFAULT_ALLOW_METHODS: &str = "GET,POST,DELETE,OPTIONS";
const DEFAULT_ALLOW_HEADERS: &str = "Content-Type,X-Api-Key";
const DEFAULT_MAX_AGE_SECONDS: u64 = 86400;

#[derive(Clone)]
pub struct CorsRuntime {
    enabled: bool,
    allow_origins: AllowOrigins,
    allow_methods: HeaderValue,
    allow_headers: AllowHeaders,
    expose_headers: Option<HeaderValue>,
    max_age: HeaderValue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorsRuntimeSummary {
    pub enabled: bool,
    pub allow_origins: CorsOriginsSummary,
    pub allow_methods: String,
    pub allow_headers: CorsHeadersSummary,
    pub expose_headers: Option<String>,
    pub max_age_seconds: u64,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CorsOriginsSummary {
    Any,
    List(Vec<String>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CorsHeadersSummary {
    Any,
    List(Vec<String>),
}

#[derive(Clone)]
enum AllowOrigins {
    Any,
    List(Vec<String>),
}

#[derive(Clone)]
enum AllowHeaders {
    Any,
    List(HeaderValue),
}

impl CorsRuntime {
    pub fn disabled() -> Self {
        Self::from_summary(CorsRuntimeSummary {
            enabled: false,
            allow_origins: CorsOriginsSummary::Any,
            allow_methods: DEFAULT_ALLOW_METHODS.to_string(),
            allow_headers: CorsHeadersSummary::List(vec![
                "Content-Type".to_string(),
                "X-Api-Key".to_string(),
            ]),
            expose_headers: None,
            max_age_seconds: DEFAULT_MAX_AGE_SECONDS,
            warnings: Vec::new(),
        })
    }

    pub fn from_env() -> Self {
        let vars = env::vars().collect::<HashMap<_, _>>();
        Self::from_summary(Self::summary_from_map(&vars))
    }

    pub fn from_summary(summary: CorsRuntimeSummary) -> Self {
        for warning in &summary.warnings {
            warn!("{warning}");
        }

        let allow_origins = match summary.allow_origins {
            CorsOriginsSummary::Any => AllowOrigins::Any,
            CorsOriginsSummary::List(values) => AllowOrigins::List(values),
        };
        let allow_headers = match summary.allow_headers {
            CorsHeadersSummary::Any => AllowHeaders::Any,
            CorsHeadersSummary::List(values) => {
                AllowHeaders::List(HeaderValue::from_str(&values.join(",")).unwrap())
            }
        };

        Self {
            enabled: summary.enabled,
            allow_origins,
            allow_methods: HeaderValue::from_str(&summary.allow_methods).unwrap(),
            allow_headers,
            expose_headers: summary
                .expose_headers
                .map(|value| HeaderValue::from_str(&value).unwrap()),
            max_age: HeaderValue::from_str(&summary.max_age_seconds.to_string()).unwrap(),
        }
    }

    pub fn summary_from_map(vars: &HashMap<String, String>) -> CorsRuntimeSummary {
        let mut warnings = Vec::new();
        let enabled = env_flag_default_true(vars.get("JUSTLOG_CORS_ENABLED").map(String::as_str));

        let allow_origins = match parse_csv(
            vars.get("JUSTLOG_CORS_ALLOW_ORIGINS")
                .map(String::as_str)
                .unwrap_or("*"),
            true,
        ) {
            ParsedList::Any => CorsOriginsSummary::Any,
            ParsedList::List(values) => {
                if values.is_empty() {
                    warnings.push(
                        "JUSTLOG_CORS_ALLOW_ORIGINS is empty after parsing; defaulting to *"
                            .to_string(),
                    );
                    CorsOriginsSummary::Any
                } else {
                    CorsOriginsSummary::List(values)
                }
            }
        };

        let allow_methods = match parse_csv(
            vars.get("JUSTLOG_CORS_ALLOW_METHODS")
                .map(String::as_str)
                .unwrap_or(DEFAULT_ALLOW_METHODS),
            false,
        ) {
            ParsedList::Any => DEFAULT_ALLOW_METHODS.to_string(),
            ParsedList::List(values) => {
                if values.is_empty() {
                    warnings.push(
                        "JUSTLOG_CORS_ALLOW_METHODS is empty after parsing; defaulting to GET,POST,DELETE,OPTIONS".to_string(),
                    );
                    DEFAULT_ALLOW_METHODS.to_string()
                } else {
                    values.join(",")
                }
            }
        };

        let allow_headers = match parse_csv(
            vars.get("JUSTLOG_CORS_ALLOW_HEADERS")
                .map(String::as_str)
                .unwrap_or(DEFAULT_ALLOW_HEADERS),
            true,
        ) {
            ParsedList::Any => CorsHeadersSummary::Any,
            ParsedList::List(values) => {
                if values.is_empty() {
                    warnings.push(
                        "JUSTLOG_CORS_ALLOW_HEADERS is empty after parsing; defaulting to Content-Type,X-Api-Key".to_string(),
                    );
                    CorsHeadersSummary::List(vec![
                        "Content-Type".to_string(),
                        "X-Api-Key".to_string(),
                    ])
                } else {
                    CorsHeadersSummary::List(values)
                }
            }
        };

        let expose_headers = match parse_csv(
            vars.get("JUSTLOG_CORS_EXPOSE_HEADERS")
                .map(String::as_str)
                .unwrap_or(""),
            false,
        ) {
            ParsedList::Any => None,
            ParsedList::List(values) if values.is_empty() => None,
            ParsedList::List(values) => Some(values.join(",")),
        };

        let max_age_seconds = vars
            .get("JUSTLOG_CORS_MAX_AGE_SECONDS")
            .and_then(|value| value.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_MAX_AGE_SECONDS);

        CorsRuntimeSummary {
            enabled,
            allow_origins,
            allow_methods,
            allow_headers,
            expose_headers,
            max_age_seconds,
            warnings,
        }
    }

    pub fn preflight_response(
        &self,
        method: &Method,
        request_headers: &HeaderMap,
    ) -> Option<Response<Body>> {
        if !self.enabled
            || method != Method::OPTIONS
            || request_headers.get(ORIGIN).is_none()
            || request_headers.get(ACCESS_CONTROL_REQUEST_METHOD).is_none()
        {
            return None;
        }

        let mut response = Response::builder()
            .status(StatusCode::NO_CONTENT)
            .body(Body::empty())
            .unwrap();
        self.apply_headers(request_headers, response.headers_mut());
        response
            .headers_mut()
            .insert(ACCESS_CONTROL_ALLOW_METHODS, self.allow_methods.clone());

        match &self.allow_headers {
            AllowHeaders::Any => {
                if let Some(request_headers) = request_headers.get(ACCESS_CONTROL_REQUEST_HEADERS) {
                    response
                        .headers_mut()
                        .insert(ACCESS_CONTROL_ALLOW_HEADERS, request_headers.clone());
                } else {
                    response
                        .headers_mut()
                        .insert(ACCESS_CONTROL_ALLOW_HEADERS, HeaderValue::from_static("*"));
                }
            }
            AllowHeaders::List(value) => {
                response
                    .headers_mut()
                    .insert(ACCESS_CONTROL_ALLOW_HEADERS, value.clone());
            }
        }

        response
            .headers_mut()
            .insert(ACCESS_CONTROL_MAX_AGE, self.max_age.clone());
        Some(response)
    }

    pub fn apply_response_headers(
        &self,
        request_headers: &HeaderMap,
        response: &mut Response<Body>,
    ) {
        if self.enabled {
            self.apply_headers(request_headers, response.headers_mut());
        }
    }

    fn apply_headers(&self, request_headers: &HeaderMap, response_headers: &mut HeaderMap) {
        let Some(origin) = request_headers.get(ORIGIN) else {
            return;
        };

        match &self.allow_origins {
            AllowOrigins::Any => {
                response_headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
            }
            AllowOrigins::List(values) => {
                append_vary(response_headers, "Origin");
                if values.iter().any(|value| origin_matches(origin, value)) {
                    response_headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, origin.clone());
                } else {
                    return;
                }
            }
        }

        if let Some(expose_headers) = &self.expose_headers {
            response_headers.insert(ACCESS_CONTROL_EXPOSE_HEADERS, expose_headers.clone());
        }
    }
}

enum ParsedList {
    Any,
    List(Vec<String>),
}

fn parse_csv(value: &str, allow_wildcard: bool) -> ParsedList {
    let trimmed = value.trim();
    if allow_wildcard && trimmed == "*" {
        return ParsedList::Any;
    }

    let values = trimmed
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.trim_end_matches('/').to_string())
        .collect::<Vec<_>>();
    ParsedList::List(values)
}

fn env_flag_default_true(value: Option<&str>) -> bool {
    match value {
        None => true,
        Some("0" | "false" | "FALSE" | "no" | "NO") => false,
        Some(_) => true,
    }
}

fn origin_matches(origin: &HeaderValue, allowed_origin: &str) -> bool {
    origin
        .to_str()
        .ok()
        .map(str::trim)
        .map(|value| value.trim_end_matches('/'))
        == Some(allowed_origin)
}

fn append_vary(headers: &mut HeaderMap, value: &str) {
    if let Some(existing) = headers.get(VARY).and_then(|current| current.to_str().ok()) {
        if existing
            .split(',')
            .map(str::trim)
            .any(|current| current.eq_ignore_ascii_case(value))
        {
            return;
        }
        headers.insert(
            VARY,
            HeaderValue::from_str(&format!("{existing}, {value}")).unwrap(),
        );
    } else {
        headers.insert(VARY, HeaderValue::from_str(value).unwrap());
    }
}

#[cfg(test)]
mod tests {
    use super::{CorsHeadersSummary, CorsOriginsSummary, CorsRuntime};
    use std::collections::HashMap;

    #[test]
    fn summary_defaults_to_enabled_open_cors() {
        let vars = HashMap::new();
        let summary = CorsRuntime::summary_from_map(&vars);
        assert!(summary.enabled);
        assert_eq!(summary.allow_origins, CorsOriginsSummary::Any);
        assert_eq!(summary.allow_methods, "GET,POST,DELETE,OPTIONS");
        assert_eq!(
            summary.allow_headers,
            CorsHeadersSummary::List(vec!["Content-Type".to_string(), "X-Api-Key".to_string()])
        );
        assert_eq!(summary.expose_headers, None);
        assert_eq!(summary.max_age_seconds, 86400);
        assert!(summary.warnings.is_empty());
    }

    #[test]
    fn summary_parses_lists_and_disabled_flag() {
        let mut vars = HashMap::new();
        vars.insert("JUSTLOG_CORS_ENABLED".to_string(), "0".to_string());
        vars.insert(
            "JUSTLOG_CORS_ALLOW_ORIGINS".to_string(),
            "https://a.test, https://b.test/".to_string(),
        );
        vars.insert(
            "JUSTLOG_CORS_ALLOW_METHODS".to_string(),
            "GET, OPTIONS".to_string(),
        );
        vars.insert("JUSTLOG_CORS_ALLOW_HEADERS".to_string(), "*".to_string());
        vars.insert(
            "JUSTLOG_CORS_EXPOSE_HEADERS".to_string(),
            "ETag, X-Trace-Id".to_string(),
        );
        vars.insert("JUSTLOG_CORS_MAX_AGE_SECONDS".to_string(), "60".to_string());

        let summary = CorsRuntime::summary_from_map(&vars);
        assert!(!summary.enabled);
        assert_eq!(
            summary.allow_origins,
            CorsOriginsSummary::List(vec![
                "https://a.test".to_string(),
                "https://b.test".to_string()
            ])
        );
        assert_eq!(summary.allow_methods, "GET,OPTIONS");
        assert_eq!(summary.allow_headers, CorsHeadersSummary::Any);
        assert_eq!(summary.expose_headers.as_deref(), Some("ETag,X-Trace-Id"));
        assert_eq!(summary.max_age_seconds, 60);
    }
}
