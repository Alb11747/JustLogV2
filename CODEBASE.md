# JustLogV2 Codebase Guide

## Overview

JustLogV2 is a Rust service that does two jobs:

1. It connects to Twitch IRC, normalizes incoming chat events, and stores them.
2. It exposes an HTTP API for listing channels and serving archived chat logs.

The project is built as a library crate with a very small binary entry point. Runtime behavior is centered around four areas:

- application bootstrap and CLI handling
- Twitch metadata lookup and IRC ingestion
- SQLite-backed hot storage plus Brotli-compressed archived segments
- Axum HTTP routes for log retrieval and administration

## High-Level Architecture

```text
src/main.rs
  -> justlog::run_cli()
     -> load config
     -> open store
     -> create Helix client
     -> create ingest manager
     -> resolve channel IDs to logins
     -> start IRC ingest lanes
     -> start background compactor
     -> serve Axum API
```

At runtime:

- `HelixClient` resolves Twitch user IDs and logins through the Twitch Helix API.
- `IngestManager` maintains one or more IRC connections and turns raw IRC lines into canonical events.
- `Store` persists recent events in SQLite and archives older partitions into Brotli segment files.
- `api` reads from the store and returns logs as JSON, plain text, or raw IRC text.
- `CommandService` listens for `!justlog ...` chat commands and updates config or channel membership.

## Repository Layout

### Root

- `Cargo.toml`: crate metadata and dependencies.
- `README.md`: short usage guide for local and Docker runs.
- `Dockerfile`: container image build for the service.
- `tests/smoke.rs`: fast deterministic smoke tests for config, API basics, and archived reads.
- `tests/integration.rs`: end-to-end style tests using mock Helix and IRC servers.
- `tests/network.rs`: loopback HTTP/TCP coverage using real local sockets.
- `tests/live_network.rs`: explicitly gated live network smoke checks.
- `tests/common/mod.rs`: shared test harness for mock Helix, mock IRC, seeded state, and HTTP server setup.
- `target/`: Cargo build output.

### Source Modules

- `src/main.rs`: binary entry point; delegates directly to `justlog::run_cli()`.
- `src/lib.rs`: exports the crate modules and re-exports `run_cli`.
- `src/app.rs`: application bootstrap, CLI parsing, shared app state, and chat command handling.
- `src/api.rs`: the entire HTTP surface, including log route parsing and response formatting.
- `src/config.rs`: JSON config schema, defaults, normalization, and persistence.
- `src/helix.rs`: Twitch Helix client for access tokens and user lookups with in-memory caching.
- `src/ingest.rs`: Twitch IRC connection management, message parsing, deduplication, and outbound join/part/say handling.
- `src/store.rs`: SQLite schema, inserts, queries, compaction, and segment file I/O.
- `src/model.rs`: canonical event model, API response structs, and raw IRC parsing helpers.
- `src/compact.rs`: background loop that periodically archives old partitions.
- `src/import.rs`: CLI import path for legacy plain-text or gzip-compressed log files.

## Startup and Lifecycle

`src/app.rs` is the main orchestration layer.

### CLI

The binary supports:

- normal server mode: `cargo run -- --config config.json`
- legacy import mode: `cargo run -- --config config.json import-legacy <path>`

### Boot Sequence

`run_cli()` performs these steps:

1. Parse CLI arguments with `clap`.
2. Load and normalize config from JSON.
3. Initialize tracing from the configured log level.
4. Open the store and initialize SQLite tables.
5. If `ImportLegacy` was requested, import old log files and exit.
6. Build shared application state.
7. Create the chat command service and ingest manager.
8. Resolve configured channel IDs into channel logins through Helix.
9. Start ingestion lanes.
10. Start the background compactor.
11. Build the Axum router and bind the HTTP listener.

## Shared State

`AppState` holds the long-lived state used across routes and chat commands:

- `config`: shared mutable config behind `Arc<RwLock<Config>>`
- `store`: cloned handle to the SQLite/segment storage layer
- `helix`: cached Twitch API client
- `ingest`: optional ingest manager handle used for runtime join/part/say actions
- `start_time`: used for uptime reporting
- `optout_codes`: temporary one-minute confirmation codes for self-service opt-out

## Configuration

The config file is JSON-based and modeled in `src/config.rs`.

### Important Top-Level Fields

- `clientID`, `clientSecret`: required for Helix user resolution and token creation.
- `channels`: Twitch channel IDs to ingest on startup.
- `logsDirectory`: root directory for archived logs and the SQLite file.
- `listenAddress`: HTTP bind address; `:8025` becomes `0.0.0.0:8025`.
- `adminAPIKey`: required for `/admin/channels`.
- `admins`: usernames allowed to use privileged `!justlog` chat commands.
- `optOut`: map of opted-out user IDs.
- `archive`: enables or disables background compaction.

### Nested Config Groups

- `compression`: Brotli quality/window settings used when writing segment files.
- `http`: feature flags for precompressed streaming and on-the-fly compression.
- `ingest`: redundancy factor, max channels per connection, and connect timeout.
- `helix`: optional base URL override, mainly useful for tests.
- `irc`: server/port/TLS settings for Twitch IRC.
- `storage`: SQLite path and compaction thresholds.
- `ops`: metrics toggle and route path.

### Normalization Behavior

On load, config normalization:

- normalizes path separators
- defaults `storage.sqlitePath` to `<logsDirectory>/justlog.sqlite3`
- strips the `oauth:` prefix from the configured token
- lowercases log levels and admin usernames
- deduplicates configured channels
- requires `clientID` to be present

`Config::persist()` writes the normalized config back to disk after admin/chat command changes.

## Ingestion Pipeline

`src/ingest.rs` owns the Twitch IRC connection logic.

### Connections

- The ingest manager starts `redundancy_factor` independent lanes.
- Each lane connects to the configured IRC endpoint.
- On connect, it sends `PASS`, `NICK`, and Twitch capability requests.
- It joins every desired channel currently tracked in memory.

### Message Handling

For each incoming IRC line:

- `PING` receives an immediate `PONG`.
- raw IRC is parsed into a `twitch_irc` `ServerMessage`
- only supported messages are converted into `CanonicalEvent`
- a short TTL cache deduplicates mirrored traffic across redundant lanes
- `PRIVMSG` events are passed to the chat command service
- opted-out channels/users are skipped
- remaining events are inserted into the store

### Outbound Commands

The ingest layer also listens on a broadcast channel for:

- `JOIN #channel`
- `PART #channel`
- `PRIVMSG #channel :text`

Those commands are triggered by HTTP admin routes or `!justlog` chat commands.

## Canonical Event Model

`src/model.rs` converts raw IRC messages into a storage-friendly format.

Supported message types:

- `PRIVMSG`
- `CLEARCHAT`
- `USERNOTICE`

Each `CanonicalEvent` stores:

- Twitch room/channel identifiers
- actor and target user IDs where applicable
- text and system text
- normalized timestamp
- original raw IRC line
- parsed tags
- stable event UID for deduplication

This model is used both for live ingestion and for rehydrating archived segment files later.

## Storage Design

`src/store.rs` implements a two-tier storage model.

### Hot Storage

Recent events are stored in SQLite in the `events` table.

The store uses:

- WAL journal mode
- full sync mode
- indexes by channel/time and user/time
- `INSERT OR IGNORE` on `event_uid` to avoid duplicates

### Archived Storage

Older data is compacted into Brotli-compressed segment files under the logs directory:

- channel partitions: `segments/channel/<channel_id>/<year>/<month>/<day>.br`
- user partitions: `segments/user/<channel_id>/<user_id>/<year>/<month>.br`

Segment metadata is stored in the `segments` table, and in-progress writes are tracked in `pending_segments` so incomplete files can be cleaned up on restart.

### Read Path

When serving logs, the store merges:

- archived segment contents
- still-hot SQLite rows for the same partition

This lets the service keep recent writes queryable immediately while still archiving older data.

### Compaction

The compactor periodically:

- finds channel-day partitions older than `compact_after_channel_days`
- finds user-month partitions older than `compact_after_user_months`
- writes a `.br` segment file
- records segment metadata
- deletes compacted rows from the `events` table

If a channel-day query requests `raw` output and the entire partition is already archived with passthrough enabled, the API can stream the Brotli file directly without reconstructing the response.

## HTTP API

`src/api.rs` exposes all routes through a catch-all Axum handler.

### Basic Routes

- `GET /healthz`: liveness check.
- `GET /readyz`: readiness check.
- `GET /`: short plaintext route summary.
- `GET /channels`: list configured channels, resolved to login names.
- `GET /list?channel=<login>` or `channelid=<id>`: list available log partitions.

### Log Routes

Supported path patterns:

- `/channel/<login>/<year>/<month>/<day>`
- `/channelid/<id>/<year>/<month>/<day>`
- `/channel/<login>/user/<login>/<year>/<month>`
- `/channelid/<id>/userid/<id>/<year>/<month>`
- `/channel/.../random`

Query options:

- `json=1` or `type=json`: JSON response
- `raw=1` or `type=raw`: raw IRC lines
- default: human-readable text
- `reverse=1`: reverse event order
- `from=<unix_ts>` and `to=<unix_ts>`: range filtering

Behavior notes:

- uppercase paths redirect to lowercase equivalents
- missing date segments redirect to a canonical path using the latest/default period
- opted-out users and channels return `403`
- Brotli can be used either as direct file passthrough or on-the-fly response compression

### Admin and Opt-Out Routes

- `POST /optout`: create a short-lived confirmation code for chat-based self opt-out
- `POST /admin/channels`: add channel IDs, requires `X-Api-Key`
- `DELETE /admin/channels`: remove channel IDs, requires `X-Api-Key`

### Metrics

If enabled, the app exposes a simple event counter on the configured metrics route, defaulting to `/metrics`.

## Chat Commands

`CommandService` in `src/app.rs` reacts to `!justlog` commands received from chat:

- `!justlog status`: admin-only uptime response
- `!justlog join <login...>`: admin-only add channels
- `!justlog part <login...>`: admin-only remove channels
- `!justlog optout <code|login...>`: self opt-out by code or admin opt-out by login
- `!justlog optin <login...>`: admin-only remove opt-outs

Most command handlers:

- resolve provided logins to Twitch user IDs through Helix
- update the in-memory/shared config
- persist the config JSON
- instruct the ingest manager to join or part channels immediately

## Helix Integration

`src/helix.rs` wraps the Twitch Helix API.

Responsibilities:

- fetch app access tokens with the client credentials flow
- resolve users by ID or login
- cache user lookups in memory by both ID and login

Tests override the Helix base URL so integration tests can run against a local mock server.

## Legacy Import Path

`src/import.rs` provides a one-time importer for old logs.

It:

- walks a source directory recursively
- reads either plain text files or `.gz` files
- parses each line as raw IRC
- converts supported messages into `CanonicalEvent`
- inserts unique events into the store

## Testing Strategy

The repository uses a small test pyramid instead of a single all-purpose suite.

### Smoke tests

`tests/smoke.rs` stays fast and deterministic. It focuses on:

- root, health, and readiness routes
- `/channels` output with mocked Helix resolution
- lowercase redirect behavior
- one representative archived raw log read path
- config load, normalization, and persistence defaults

These are the tests intended for normal push and pull request CI.

### Integration tests

`tests/integration.rs` keeps broad end-to-end coverage using local mock Helix and IRC servers.

Current integration coverage includes:

- redundant ingest deduplication and reconnect recovery
- `/channels` output and lowercase redirects
- admin channel management plus chat-based join/part commands
- opt-out flow and blocked queries
- raw route Brotli passthrough and fallback compression behavior
- an auth failure case for admin channel mutation

The integration harness constructs a full `AppState`, `Store`, `HelixClient`, `IngestManager`, and Axum router, so it validates behavior close to production wiring.

### Local network tests

`tests/network.rs` exercises the same behavior over real loopback sockets instead of only in-process service calls.

It covers:

- localhost HTTP round-trips against a bound Axum server
- direct reachability of the local mock Helix HTTP server
- IRC connect/join/reconnect behavior over TCP

These deterministic network checks run on tagged releases and manual release workflow dispatches.

### Live network tests

`tests/live_network.rs` is intentionally narrow and opt-in.

It includes:

- a basic outbound HTTPS smoke check
- an optional Twitch Helix auth and user lookup smoke check when credentials are available

This suite skips unless `JUSTLOG_RUN_LIVE_NETWORK_TESTS=1` is set. In CI it is reserved for the dedicated live-network workflow so release tags do not depend on public network stability or secret availability.

## Operational Notes

- SQLite is the authoritative hot store; segment files are the archived cold store.
- The app depends on Twitch Helix for channel/user resolution even when ingestion is already running.
- Channel config stores Twitch user IDs, but IRC joins use resolved login names.
- Redundant ingest lanes improve resilience but require event deduplication.
- Opt-out checks are enforced both during ingestion and during HTTP reads.

## Suggested Onboarding Path

If you are new to the codebase, read the modules in this order:

1. `src/app.rs`
2. `src/config.rs`
3. `src/ingest.rs`
4. `src/model.rs`
5. `src/store.rs`
6. `src/api.rs`
7. `tests/integration.rs`

That order follows the main runtime flow from startup to event ingestion to data serving.
