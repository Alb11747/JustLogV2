use std::collections::HashMap;
use std::str::FromStr;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_ENCODING, CONTENT_TYPE, LOCATION, VARY};
use axum::http::{HeaderValue, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use chrono::{DateTime, Datelike, TimeZone, Utc};
use regex::Regex;
use serde::Deserialize;
use serde::Serialize;
use tokio::fs::File;
use tokio::task;
use tokio::time::sleep;
use tokio_util::io::ReaderStream;
use tracing::{info, warn};

use crate::app::AppState;
use crate::legacy_txt::{BulkRawImportSummary, LegacyTxtMode, merge_messages};
use crate::model::{
    AllChannelsJson, ChannelInfo, ChannelLogList, ChatLog, ChatMessage, ErrorResponse, UserLogList,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ResponseType {
    Json,
    Text,
    Raw,
}

#[derive(Default)]
struct LogTime {
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    year: Option<i32>,
    month: Option<u32>,
    day: Option<u32>,
    random: bool,
}

struct LogRequest {
    channel_id: String,
    user_id: Option<String>,
    reverse: bool,
    response_type: ResponseType,
    redirect_path: Option<String>,
    time: LogTime,
}

#[derive(Deserialize)]
struct ChannelsPayload {
    channels: Vec<String>,
}

#[derive(Default, Deserialize)]
struct BulkImportRawPayload {
    #[serde(default)]
    channel_id: Option<String>,
    #[serde(default)]
    limit_files: Option<usize>,
    #[serde(default)]
    dry_run: bool,
}

#[derive(Serialize)]
struct BulkImportRawResponse {
    summary: BulkRawImportSummary,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", any(dispatch_router))
        .route("/{*path}", any(dispatch_router))
        .with_state(state)
}

async fn dispatch_router(State(state): State<AppState>, request: Request) -> Response {
    dispatch(state, request).await
}

pub async fn dispatch(state: AppState, request: Request) -> Response {
    let path = request.uri().path().to_string();
    info!("HTTP request start: {} {}", request.method(), path);
    {
        let config = state.config.read().await;
        if config.ops.metrics_enabled && path == config.ops.metrics_route {
            let count = state.store.event_count().unwrap_or_default();
            return Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, "text/plain; charset=utf-8")
                .body(Body::from(format!("justlog_events_total {count}\n")))
                .unwrap();
        }
    }
    match (request.method().clone(), path.as_str()) {
        (_, "/healthz") => StatusCode::OK.into_response(),
        (_, "/readyz") => StatusCode::OK.into_response(),
        (_, "/") => root_response().into_response(),
        (_, "/channels") => match channels_handler(state).await {
            Ok(response) => response,
            Err(error) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
        },
        (_, "/list") => match list_handler(state, request.uri()).await {
            Ok(response) => response,
            Err(error) => error_response(StatusCode::NOT_FOUND, &error.to_string()),
        },
        (Method::POST, "/optout") => match optout_handler(state).await {
            Ok(response) => response,
            Err(error) => error_response(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
        },
        (Method::POST, "/admin/channels") => {
            match admin_channels_handler(state, request, true).await {
                Ok(response) => response,
                Err(error) => error_response(StatusCode::BAD_REQUEST, &error.to_string()),
            }
        }
        (Method::DELETE, "/admin/channels") => {
            match admin_channels_handler(state, request, false).await {
                Ok(response) => response,
                Err(error) => error_response(StatusCode::BAD_REQUEST, &error.to_string()),
            }
        }
        (Method::POST, "/admin/import/raw") => match admin_import_raw_handler(state, request).await
        {
            Ok(response) => response,
            Err(error) => error_response(StatusCode::BAD_REQUEST, &error.to_string()),
        },
        _ => match log_handler(state, request).await {
            Ok(response) => response,
            Err(error) => {
                if error.to_string() == "route not found" {
                    root_response().into_response()
                } else {
                    error_response(StatusCode::NOT_FOUND, &error.to_string())
                }
            }
        },
    }
}

async fn channels_handler(state: AppState) -> Result<Response> {
    let config = state.config.read().await.clone();
    let users = state.helix.get_users_by_ids(&config.channels).await?;
    let mut channels = users
        .into_values()
        .map(|user| ChannelInfo {
            user_id: user.id,
            name: user.login,
        })
        .collect::<Vec<_>>();
    channels.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(Json(AllChannelsJson { channels }).into_response())
}

async fn list_handler(state: AppState, uri: &Uri) -> Result<Response> {
    let query = query_map(uri);
    let channel_id = resolve_channel_id(&state, &query).await?;
    let user_id = resolve_optional_user_id(&state, &query).await?;
    {
        let config = state.config.read().await;
        if config.is_opted_out(&channel_id)
            || user_id
                .as_deref()
                .is_some_and(|user_id| config.is_opted_out(user_id))
        {
            return Ok(error_response(
                StatusCode::FORBIDDEN,
                "User or channel has opted out",
            ));
        }
    }
    if let Some(user_id) = user_id {
        let store = state.store.clone();
        let channel_id_for_logs = channel_id.clone();
        let user_id_for_logs = user_id.clone();
        let logs = run_blocking(move || {
            store.get_available_logs_for_user(&channel_id_for_logs, &user_id_for_logs)
        })
        .await?;
        let mut response = Json(UserLogList {
            available_logs: logs,
        })
        .into_response();
        response.headers_mut().insert(
            CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=3600"),
        );
        Ok(response)
    } else {
        let legacy_txt = state.legacy_txt.clone();
        let store = state.store.clone();
        let channel_id_for_import = channel_id.clone();
        let import_outcome =
            run_blocking(move || legacy_txt.import_raw_channel(&store, &channel_id_for_import))
                .await?;
        let archive_enabled = state.config.read().await.archive;
        if archive_enabled && !import_outcome.affected_channel_days.is_empty() {
            let store = state.store.clone();
            let affected = import_outcome
                .affected_channel_days
                .into_iter()
                .collect::<Vec<_>>();
            run_blocking(move || store.merge_imported_channel_days_into_archives(&affected))
                .await?;
        }

        let store = state.store.clone();
        let channel_id_for_logs = channel_id.clone();
        let mut logs =
            run_blocking(move || store.get_available_logs_for_channel(&channel_id_for_logs))
                .await?;
        if state.legacy_txt.is_import_enabled() {
            let legacy_txt = state.legacy_txt.clone();
            let store = state.store.clone();
            let channel_id_for_available = channel_id.clone();
            logs.extend(
                run_blocking(move || {
                    legacy_txt.available_channel_logs(&store, &channel_id_for_available)
                })
                .await?,
            );
            logs.sort_by(|left, right| {
                right
                    .year
                    .cmp(&left.year)
                    .then_with(|| right.month.cmp(&left.month))
                    .then_with(|| right.day.cmp(&left.day))
            });
            logs.dedup();
        }
        let mut response = Json(ChannelLogList {
            available_logs: logs,
        })
        .into_response();
        response.headers_mut().insert(
            CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=3600"),
        );
        Ok(response)
    }
}

async fn optout_handler(state: AppState) -> Result<Response> {
    let code = random_string(6);
    {
        state
            .optout_codes
            .lock()
            .await
            .insert(code.clone(), Instant::now() + Duration::from_secs(60));
    }
    let state_clone = state.clone();
    let code_clone = code.clone();
    tokio::spawn(async move {
        sleep(Duration::from_secs(60)).await;
        state_clone.optout_codes.lock().await.remove(&code_clone);
    });
    Ok(Json(code).into_response())
}

async fn admin_channels_handler(state: AppState, request: Request, join: bool) -> Result<Response> {
    let api_key = request
        .headers()
        .get("X-Api-Key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let expected = state.config.read().await.admin_api_key.clone();
    if api_key.is_empty() || api_key != expected {
        return Ok(error_response(
            StatusCode::FORBIDDEN,
            "No I don't think so.",
        ));
    }

    let body = axum::body::to_bytes(request.into_body(), usize::MAX).await?;
    let payload: ChannelsPayload = serde_json::from_slice(&body)?;
    let users = state.helix.get_users_by_ids(&payload.channels).await?;
    let missing = payload
        .channels
        .iter()
        .filter(|channel_id| !users.contains_key(*channel_id))
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(anyhow!("unknown channel ids: {}", missing.join(", ")));
    }
    let logins = payload
        .channels
        .iter()
        .filter_map(|channel_id| users.get(channel_id).map(|user| user.login.clone()))
        .collect::<Vec<_>>();
    {
        let mut config = state.config.write().await;
        if join {
            config.add_channels(&payload.channels);
        } else {
            config.remove_channels(&payload.channels);
        }
        config.persist()?;
    }
    if let Some(ingest) = state.ingest.read().await.clone() {
        if join {
            ingest.join_channels(&logins).await;
        } else {
            ingest.part_channels(&logins).await;
        }
    }
    let text = if join {
        format!(
            "Doubters? Joined channels or already in: {:?}",
            payload.channels
        )
    } else {
        format!("Doubters? Removed channels {:?}", payload.channels)
    };
    Ok(Json(text).into_response())
}

async fn admin_import_raw_handler(state: AppState, request: Request) -> Result<Response> {
    let api_key = request
        .headers()
        .get("X-Api-Key")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let expected = state.config.read().await.admin_api_key.clone();
    if api_key.is_empty() || api_key != expected {
        return Ok(error_response(
            StatusCode::FORBIDDEN,
            "No I don't think so.",
        ));
    }

    let body = axum::body::to_bytes(request.into_body(), usize::MAX).await?;
    let payload = if body.is_empty() {
        BulkImportRawPayload::default()
    } else {
        serde_json::from_slice::<BulkImportRawPayload>(&body)?
    };
    info!(
        "Admin bulk raw import requested: channel_filter={:?}, limit_files={:?}, dry_run={}",
        payload.channel_id, payload.limit_files, payload.dry_run
    );
    let legacy_txt = state.legacy_txt.clone();
    let store = state.store.clone();
    let channel_id = payload.channel_id.clone();
    let limit_files = payload.limit_files;
    let dry_run = payload.dry_run;
    let summary = run_blocking(move || {
        legacy_txt.bulk_import_raw(&store, channel_id.as_deref(), limit_files, dry_run)
    })
    .await?;
    Ok(Json(BulkImportRawResponse { summary }).into_response())
}

async fn log_handler(state: AppState, request: Request) -> Result<Response> {
    let accept_encoding = request
        .headers()
        .get("accept-encoding")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let content_type = request
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let parse_started = Instant::now();
    let log_request = parse_log_request(&state, request.uri(), &content_type).await?;
    info!(
        "Parsed log request in {:?}: channel_id={}, user_id={:?}, year={:?}, month={:?}, day={:?}, random={}, response_type={:?}",
        parse_started.elapsed(),
        log_request.channel_id,
        log_request.user_id,
        log_request.time.year,
        log_request.time.month,
        log_request.time.day,
        log_request.time.random,
        log_request.response_type
    );
    if let Some(redirect_path) = log_request.redirect_path {
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::FOUND;
        response
            .headers_mut()
            .insert(LOCATION, HeaderValue::from_str(&redirect_path)?);
        return Ok(response);
    }

    {
        let opt_out_started = Instant::now();
        let config = state.config.read().await;
        if config.is_opted_out(&log_request.channel_id)
            || log_request
                .user_id
                .as_deref()
                .is_some_and(|user_id| config.is_opted_out(user_id))
        {
            return Ok(error_response(
                StatusCode::FORBIDDEN,
                "User or channel has opted out",
            ));
        }
        info!(
            "Completed opt-out checks in {:?}",
            opt_out_started.elapsed()
        );
    }

    let fallback_check_started = Instant::now();
    let should_fallback = should_force_trusted_fallback(&state, &log_request).await?;
    info!(
        "Completed trusted fallback check in {:?}: should_fallback={should_fallback}",
        fallback_check_started.elapsed()
    );
    if should_fallback {
        return fallback_response(&state, request.uri(), &accept_encoding, &content_type).await;
    }

    let local = if log_request.time.random {
        random_response(state.clone(), log_request).await
    } else if log_request.time.from.is_some() || log_request.time.to.is_some() {
        range_response(state.clone(), log_request, &accept_encoding).await
    } else {
        dated_response(state.clone(), log_request, &accept_encoding).await
    };

    match local {
        Ok(response) => Ok(response),
        Err(error) if state.debug_runtime.fallback_enabled() => {
            warn!("local log read failed; falling back to trusted justlog: {error}");
            fallback_response(&state, request.uri(), &accept_encoding, &content_type).await
        }
        Err(error) => Err(error),
    }
}

async fn random_response(state: AppState, request: LogRequest) -> Result<Response> {
    let event = if let Some(user_id) = request.user_id.as_deref() {
        let store = state.store.clone();
        let channel_id = request.channel_id.clone();
        let user_id = user_id.to_string();
        run_blocking(move || store.random_user_message(&channel_id, &user_id)).await?
    } else {
        let store = state.store.clone();
        let channel_id = request.channel_id.clone();
        run_blocking(move || store.random_channel_message(&channel_id)).await?
    };
    let Some(event) = event else {
        return Ok(error_response(StatusCode::NOT_FOUND, "could not load logs"));
    };
    respond_with_events(vec![event.into()], request.response_type, false)
}

async fn range_response(
    state: AppState,
    request: LogRequest,
    accept_encoding: &str,
) -> Result<Response> {
    let from = request
        .time
        .from
        .unwrap_or_else(|| Utc::now() - chrono::Duration::days(30));
    let to = request.time.to.unwrap_or_else(Utc::now);
    let mut events = if let Some(user_id) = request.user_id.as_deref() {
        let store = state.store.clone();
        let channel_id = request.channel_id.clone();
        let user_id = user_id.to_string();
        run_blocking(move || store.read_user_range(&channel_id, &user_id, from, to)).await?
    } else {
        let store = state.store.clone();
        let channel_id = request.channel_id.clone();
        run_blocking(move || store.read_channel_range(&channel_id, from, to)).await?
    };
    if request.reverse {
        events.reverse();
    }
    let messages = events
        .into_iter()
        .map(ChatMessage::from)
        .collect::<Vec<_>>();
    respond_with_events(
        messages,
        request.response_type,
        accept_encoding.contains("br"),
    )
}

async fn dated_response(
    state: AppState,
    request: LogRequest,
    accept_encoding: &str,
) -> Result<Response> {
    info!(
        "Entering dated_response: channel_id={}, user_id={:?}, year={:?}, month={:?}, day={:?}, response_type={:?}",
        request.channel_id,
        request.user_id,
        request.time.year,
        request.time.month,
        request.time.day,
        request.response_type
    );
    let year = request.time.year.ok_or_else(|| anyhow!("invalid year"))?;
    let month = request.time.month.ok_or_else(|| anyhow!("invalid month"))?;
    let mut events = if let Some(user_id) = request.user_id.as_deref() {
        let store = state.store.clone();
        let channel_id = request.channel_id.clone();
        let user_id = user_id.to_string();
        run_blocking(move || store.read_user_logs(&channel_id, &user_id, year, month)).await?
    } else {
        let day = request.time.day.ok_or_else(|| anyhow!("invalid day"))?;
        let legacy_mode = state.legacy_txt.mode();
        if request.response_type == ResponseType::Raw
            && accept_encoding.contains("br")
            && !state.legacy_txt.is_import_enabled()
        {
            let store = state.store.clone();
            let channel_id = request.channel_id.clone();
            let plan =
                run_blocking(move || store.channel_raw_plan(&channel_id, year, month, day)).await?;
            if let Some(path) = plan.segment_path {
                return direct_brotli_response(path);
            }
        }
        let legacy_txt = state.legacy_txt.clone();
        let store = state.store.clone();
        let channel_id = request.channel_id.clone();
        info!(
            "Starting request-driven raw import for channel-day {channel_id}/{year}/{month}/{day}"
        );
        let import_outcome = run_blocking(move || {
            legacy_txt.import_raw_channel_day(&store, &channel_id, year, month, day)
        })
        .await?;
        let archive_enabled = state.config.read().await.archive;
        if archive_enabled && !import_outcome.affected_channel_days.is_empty() {
            let store = state.store.clone();
            let affected = import_outcome
                .affected_channel_days
                .into_iter()
                .collect::<Vec<_>>();
            run_blocking(move || store.merge_imported_channel_days_into_archives(&affected))
                .await?;
        }

        let store = state.store.clone();
        let channel_id_for_native = request.channel_id.clone();
        let native =
            run_blocking(move || store.read_channel_logs(&channel_id_for_native, year, month, day))
                .await?;
        let channel_login = resolve_channel_login(&state, &request.channel_id).await;
        let legacy_txt = state.legacy_txt.clone();
        let store = state.store.clone();
        let channel_id_for_import = request.channel_id.clone();
        let imported = run_blocking(move || {
            legacy_txt.load_channel_day_import(
                &store,
                &channel_id_for_import,
                &channel_login,
                year,
                month,
                day,
            )
        })
        .await?;
        return respond_with_channel_day_messages(
            request,
            native.into_iter().map(ChatMessage::from).collect(),
            imported.complete_messages,
            imported.simple_messages,
            accept_encoding.contains("br"),
            legacy_mode,
        );
    };
    if request.reverse {
        events.reverse();
    }
    let messages = events
        .into_iter()
        .map(ChatMessage::from)
        .collect::<Vec<_>>();
    respond_with_events(
        messages,
        request.response_type,
        accept_encoding.contains("br"),
    )
}

fn respond_with_channel_day_messages(
    request: LogRequest,
    native: Vec<ChatMessage>,
    complete_imported: Vec<ChatMessage>,
    simple_imported: Vec<ChatMessage>,
    compress_brotli: bool,
    legacy_mode: LegacyTxtMode,
) -> Result<Response> {
    let native_is_empty = native.is_empty();
    let native_count = native.len();
    let complete_count = complete_imported.len();
    let simple_count = simple_imported.len();
    let with_complete = merge_messages(native, complete_imported);
    let mut messages = match legacy_mode {
        LegacyTxtMode::Off => with_complete,
        LegacyTxtMode::MissingOnly => {
            if native_is_empty {
                merge_messages(with_complete, simple_imported)
            } else {
                with_complete
            }
        }
        LegacyTxtMode::Merge => merge_messages(with_complete, simple_imported),
    };
    if request.reverse {
        messages.reverse();
    }
    info!(
        "Preparing channel-day response: type={:?}, native_messages={}, complete_imported={}, simple_imported={}, final_messages={}, reverse={}, brotli={}",
        request.response_type,
        native_count,
        complete_count,
        simple_count,
        messages.len(),
        request.reverse,
        compress_brotli
    );
    respond_with_events(messages, request.response_type, compress_brotli)
}

async fn parse_log_request(state: &AppState, uri: &Uri, content_type: &str) -> Result<LogRequest> {
    let path = uri.path().to_string();
    if path != path.to_lowercase() {
        let redirect = if let Some(query) = uri.query() {
            format!("{}?{}", path.to_lowercase(), query)
        } else {
            path.to_lowercase()
        };
        return Ok(LogRequest {
            channel_id: String::new(),
            user_id: None,
            reverse: false,
            response_type: ResponseType::Text,
            redirect_path: Some(redirect),
            time: LogTime::default(),
        });
    }
    if !path.starts_with("/channel") {
        return Err(anyhow!("route not found"));
    }
    let regex = Regex::new(
        r"^/(channel|channelid)/([^/]+)(?:/(user|userid)/([^/]+))?(?:/(\d{4})/(\d{1,2})(?:/(\d{1,2}))?|/(random))?$",
    )?;
    let captures = regex
        .captures(path.trim_end_matches('/'))
        .ok_or_else(|| anyhow!("route not found"))?;
    let query = query_map(uri);
    let response_type = if query.contains_key("json")
        || query.get("type").is_some_and(|value| value == "json")
        || content_type == "application/json"
    {
        ResponseType::Json
    } else if query.contains_key("raw") || query.get("type").is_some_and(|value| value == "raw") {
        ResponseType::Raw
    } else {
        ResponseType::Text
    };
    let reverse = query.contains_key("reverse");
    let channel_id = if &captures[1] == "channelid" {
        captures[2].to_string()
    } else {
        resolve_login_to_id(&state.helix, &captures[2]).await?
    };
    let is_user_request = captures.get(3).is_some();
    let user_id = if captures.get(3).map(|capture| capture.as_str()) == Some("userid") {
        captures.get(4).map(|capture| capture.as_str().to_string())
    } else if let Some(user) = captures.get(4) {
        Some(resolve_login_to_id(&state.helix, user.as_str()).await?)
    } else {
        None
    };

    let mut time = LogTime::default();
    if captures.get(8).is_some() {
        time.random = true;
    } else if let Some(year) = captures.get(5) {
        time.year = Some(i32::from_str(year.as_str())?);
        time.month = Some(u32::from_str(captures.get(6).unwrap().as_str())?);
        time.day = captures
            .get(7)
            .map(|capture| u32::from_str(capture.as_str()))
            .transpose()?;
    } else {
        let now = Utc::now();
        time.year = Some(now.year());
        time.month = Some(now.month());
        if !is_user_request {
            time.day = Some(now.day());
        } else if let Some(user_id) = user_id.as_deref() {
            if let Some((year, month)) = state.store.latest_user_log_month(&channel_id, user_id)? {
                time.year = Some(year);
                time.month = Some(month);
            }
        }
        let redirect = if is_user_request {
            format!(
                "{}/{}/{}{}",
                path.trim_end_matches('/'),
                time.year.unwrap(),
                time.month.unwrap(),
                uri.query()
                    .map(|query| format!("?{query}"))
                    .unwrap_or_default()
            )
        } else {
            format!(
                "{}/{}/{}/{}{}",
                path.trim_end_matches('/'),
                time.year.unwrap(),
                time.month.unwrap(),
                time.day.unwrap(),
                uri.query()
                    .map(|query| format!("?{query}"))
                    .unwrap_or_default()
            )
        };
        return Ok(LogRequest {
            channel_id,
            user_id,
            reverse,
            response_type,
            redirect_path: Some(redirect),
            time,
        });
    }

    if let Some(from) = query.get("from") {
        time.from = Some(parse_timestamp(from)?);
    }
    if let Some(to) = query.get("to") {
        time.to = Some(parse_timestamp(to)?);
    }

    Ok(LogRequest {
        channel_id,
        user_id,
        reverse,
        response_type,
        redirect_path: None,
        time,
    })
}

fn query_map(uri: &Uri) -> HashMap<String, String> {
    url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
        .into_owned()
        .collect()
}

async fn should_force_trusted_fallback(state: &AppState, request: &LogRequest) -> Result<bool> {
    if !state.debug_runtime.fallback_enabled() {
        return Ok(false);
    }
    if request.time.random || request.user_id.is_some() {
        return Ok(false);
    }
    if request.time.from.is_some() || request.time.to.is_some() {
        let from = request
            .time
            .from
            .unwrap_or_else(|| Utc::now() - chrono::Duration::days(30));
        let to = request.time.to.unwrap_or_else(Utc::now);
        let mut cursor = from.date_naive();
        let end = to.date_naive();
        while cursor <= end {
            if state.store.channel_day_is_unhealthy(
                &request.channel_id,
                cursor.year(),
                cursor.month(),
                cursor.day(),
            )? {
                return Ok(true);
            }
            cursor = cursor.succ_opt().unwrap();
        }
        return Ok(false);
    }
    match (request.time.year, request.time.month, request.time.day) {
        (Some(year), Some(month), Some(day)) => {
            state
                .store
                .channel_day_is_unhealthy(&request.channel_id, year, month, day)
        }
        _ => Ok(false),
    }
}

async fn fallback_response(
    state: &AppState,
    uri: &Uri,
    accept_encoding: &str,
    content_type: &str,
) -> Result<Response> {
    state
        .debug_runtime
        .proxy_fallback_request(uri, accept_encoding, content_type)
        .await
        .map_err(Into::into)
}

async fn resolve_channel_id(state: &AppState, query: &HashMap<String, String>) -> Result<String> {
    if let Some(channel_id) = query.get("channelid") {
        return Ok(channel_id.to_string());
    }
    if let Some(channel) = query.get("channel") {
        return resolve_login_to_id(&state.helix, channel).await;
    }
    Err(anyhow!("missing channel"))
}

async fn resolve_optional_user_id(
    state: &AppState,
    query: &HashMap<String, String>,
) -> Result<Option<String>> {
    if let Some(user_id) = query.get("userid") {
        return Ok(Some(user_id.to_string()));
    }
    if let Some(user) = query.get("user") {
        return Ok(Some(resolve_login_to_id(&state.helix, user).await?));
    }
    Ok(None)
}

async fn resolve_login_to_id(helix: &crate::helix::HelixClient, login: &str) -> Result<String> {
    let users = helix.get_users_by_logins(&[login.to_lowercase()]).await?;
    users
        .get(&login.to_lowercase())
        .map(|user| user.id.clone())
        .ok_or_else(|| anyhow!("could not find users"))
}

async fn resolve_channel_login(state: &AppState, channel_id: &str) -> String {
    state
        .helix
        .get_users_by_ids(&[channel_id.to_string()])
        .await
        .ok()
        .and_then(|users| users.get(channel_id).map(|user| user.login.clone()))
        .unwrap_or_else(|| channel_id.to_string())
}

fn parse_timestamp(input: &str) -> Result<DateTime<Utc>> {
    let ts = i64::from_str(input)?;
    Utc.timestamp_opt(ts, 0)
        .single()
        .ok_or_else(|| anyhow!("invalid timestamp"))
}

fn respond_with_events(
    messages: Vec<ChatMessage>,
    response_type: ResponseType,
    compress_brotli: bool,
) -> Result<Response> {
    let render_started = Instant::now();
    let message_count = messages.len();
    info!(
        "Starting response render: type={:?}, messages={}, brotli={}",
        response_type, message_count, compress_brotli
    );
    match response_type {
        ResponseType::Json => {
            let mut response = Json(ChatLog { messages }).into_response();
            response
                .headers_mut()
                .insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
            info!(
                "Completed response render: type={:?}, messages={}, elapsed={:?}",
                response_type,
                message_count,
                render_started.elapsed()
            );
            Ok(response)
        }
        ResponseType::Raw => {
            let rendered = messages
                .into_iter()
                .map(|message| message.raw)
                .collect::<Vec<_>>()
                .join("\n");
            let response = bytes_response(rendered + "\n", compress_brotli)?;
            info!(
                "Completed response render: type={:?}, messages={}, elapsed={:?}",
                response_type,
                message_count,
                render_started.elapsed()
            );
            Ok(response)
        }
        ResponseType::Text => {
            let rendered = messages
                .into_iter()
                .map(render_text_line)
                .collect::<Vec<_>>()
                .join("");
            let response = bytes_response(rendered, compress_brotli)?;
            info!(
                "Completed response render: type={:?}, messages={}, elapsed={:?}",
                response_type,
                message_count,
                render_started.elapsed()
            );
            Ok(response)
        }
    }
}

fn bytes_response(rendered: String, compress_brotli: bool) -> Result<Response> {
    let render_bytes = rendered.len();
    let mut response = if compress_brotli {
        let compression_started = Instant::now();
        let mut output = Vec::new();
        {
            let mut writer = brotli::CompressorWriter::new(&mut output, 16 * 1024, 5, 22);
            std::io::Write::write_all(&mut writer, rendered.as_bytes())?;
            std::io::Write::flush(&mut writer)?;
        }
        info!(
            "Completed Brotli compression: input_bytes={}, output_bytes={}, elapsed={:?}",
            render_bytes,
            output.len(),
            compression_started.elapsed()
        );
        Response::new(Body::from(output))
    } else {
        info!("Response body ready without compression: bytes={render_bytes}");
        Response::new(Body::from(rendered))
    };
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    if compress_brotli {
        response
            .headers_mut()
            .insert(CONTENT_ENCODING, HeaderValue::from_static("br"));
        response
            .headers_mut()
            .insert(VARY, HeaderValue::from_static("Accept-Encoding"));
    }
    Ok(response)
}

fn render_text_line(message: ChatMessage) -> String {
    let text = if message.message_type == crate::model::CLEARCHAT_TYPE {
        message.system_text
    } else if !message.text.is_empty() {
        message.text
    } else {
        message.system_text
    };
    if message.message_type == crate::model::PRIVMSG_TYPE {
        format!(
            "[{}] #{} {}: {}\n",
            message.timestamp.format("%Y-%m-%-d %H:%M:%S"),
            message.channel,
            message.username,
            text
        )
    } else {
        format!(
            "[{}] #{} {}\n",
            message.timestamp.format("%Y-%m-%-d %H:%M:%S"),
            message.channel,
            text
        )
    }
}

fn direct_brotli_response(path: std::path::PathBuf) -> Result<Response> {
    let stream = ReaderStream::new(File::from_std(std::fs::File::open(path)?));
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    response
        .headers_mut()
        .insert(CONTENT_ENCODING, HeaderValue::from_static("br"));
    response
        .headers_mut()
        .insert(VARY, HeaderValue::from_static("Accept-Encoding"));
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    Ok(response)
}

fn error_response(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(ErrorResponse {
            message: message.to_string(),
        }),
    )
        .into_response()
}

fn root_response() -> &'static str {
    "justlog v2\n\nRoutes: /channels /list /channel/... /channelid/... /optout /admin/channels /admin/import/raw\n"
}

fn random_string(len: usize) -> String {
    use rand::Rng as _;
    let charset = b"abcdefghijklmnopqrstuvwxyz1234567890";
    let mut rng = rand::rng();
    (0..len)
        .map(|_| charset[rng.random_range(0..charset.len())] as char)
        .collect()
}

async fn run_blocking<T, F>(f: F) -> Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T> + Send + 'static,
{
    task::spawn_blocking(f)
        .await
        .map_err(|error| anyhow!("blocking task join error: {error}"))?
}

#[cfg(test)]
mod tests {
    use super::dispatch;
    use crate::app::AppState;
    use crate::config::Config;
    use crate::debug_sync::DebugRuntime;
    use crate::helix::HelixClient;
    use crate::ingest::{ChatCommandService, IngestManager};
    use crate::legacy_txt::LegacyTxtRuntime;
    use crate::model::CanonicalEvent;
    use crate::store::Store;
    use anyhow::Result;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Instant;
    use tempfile::TempDir;
    use tokio::sync::{Mutex, RwLock};

    struct NoopCommands;

    #[async_trait::async_trait]
    impl ChatCommandService for NoopCommands {
        async fn handle_privmsg_command(&self, _event: &CanonicalEvent) -> Result<()> {
            Ok(())
        }
    }

    fn test_state() -> AppState {
        let temp = TempDir::new().unwrap();
        let root = temp.keep();
        let logs_directory = root.join("logs");
        let config_path = root.join("config.json");
        let mut config = Config {
            config_path,
            config_file_permissions: None,
            bot_verified: false,
            logs_directory,
            archive: false,
            admin_api_key: String::new(),
            username: "justinfan0".to_string(),
            oauth: String::new(),
            listen_address: "127.0.0.1:0".to_string(),
            admins: Vec::new(),
            channels: Vec::new(),
            client_id: String::new(),
            client_secret: String::new(),
            log_level: "info".to_string(),
            opt_out: HashMap::new(),
            compression: Default::default(),
            http: Default::default(),
            ingest: Default::default(),
            helix: Default::default(),
            irc: Default::default(),
            storage: Default::default(),
            ops: Default::default(),
        };
        config.storage.sqlite_path = PathBuf::from(root.join("justlog.sqlite3"));
        config.normalize().unwrap();
        let shared_config = Arc::new(RwLock::new(config.clone()));
        let store = Store::open(&config).unwrap();
        AppState {
            config: shared_config,
            store,
            helix: HelixClient::new(&config),
            legacy_txt: Arc::new(LegacyTxtRuntime::from_env(&config.logs_directory)),
            debug_runtime: Arc::new(DebugRuntime::disabled()),
            ingest: Arc::new(RwLock::new(None)),
            start_time: Instant::now(),
            optout_codes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn start_test_ingest(state: &AppState) {
        {
            let mut config = state.config.write().await;
            config.ingest.connect_timeout_ms = 50;
            config.irc.server = "127.0.0.1".to_string();
            config.irc.port = 1;
            config.irc.tls = false;
        }
        let ingest = IngestManager::new(
            state.config.clone(),
            state.store.clone(),
            Arc::new(NoopCommands),
        );
        *state.ingest.write().await = Some(ingest.clone());
        ingest.start(Vec::new()).await;
    }

    #[tokio::test]
    async fn healthz_route_returns_ok_in_process() {
        let response = dispatch(
            test_state(),
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn healthz_route_returns_ok_over_tcp_with_ingest_started() {
        let state = test_state();
        start_test_ingest(&state).await;
        let response = dispatch(
            state,
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }
}
