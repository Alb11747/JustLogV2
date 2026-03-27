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

Operationally, the documented deployment path is Docker Compose on Ubuntu/Linux with `./data` mounted to `/data`. Windows is still a common development and operator environment, but any personal sync or deploy scripts remain local workflow tooling rather than a tracked repo interface.

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
     -> run optional startup validation
     -> start background compactor and reconciliation worker
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
- `README.md`: short usage guide for local development and Docker Compose deployment.
- `docker-compose.yml`: canonical container runtime setup for local Docker use and Ubuntu deployments.
- `.env.template`: starter compose env file for creating a local `.env` with optional env-only runtime flags.
- `data/config.template.json`: starter JSON app config for creating an untracked `data/config.json`.
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
- `src/legacy_txt.rs`: import-folder compatibility layer for raw IRC imports plus reconstructed TXT/JSON overlays during API reads.
- `src/debug_sync.rs`: reconciliation jobs, trusted/compare debug runtime, startup consistency validation, and trusted API fallback helpers.

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
10. Run optional startup consistency validation.
11. Start the background compactor and reconciliation worker.
12. Build the Axum router and bind the HTTP listener.

## Shared State

`AppState` holds the long-lived state used across routes and chat commands:

- `config`: shared mutable config behind `Arc<RwLock<Config>>`
- `store`: cloned handle to the SQLite/segment storage layer
- `helix`: cached Twitch API client
- `debug_runtime`: env-driven reconciliation, startup validation, and trusted fallback runtime
- `ingest`: optional ingest manager handle used for runtime join/part/say actions
- `start_time`: used for uptime reporting
- `optout_codes`: temporary one-minute confirmation codes for self-service opt-out

## Configuration

The config file is JSON-based and modeled in `src/config.rs`.

### Important Top-Level Fields

- `clientID`, `clientSecret`: required for Helix user resolution and token creation.
- `channels`: Twitch channel IDs to ingest on startup.
- `logsDirectory`: root directory for archived logs and the SQLite file.
- `listenAddress`: HTTP bind address; `:8026` becomes `0.0.0.0:8026`.
- `adminAPIKey`: required for `/admin/channels` and `/admin/import/raw`.
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

### Env-Only Import Folder Compatibility

Import-folder support is intentionally not part of the JSON config schema. It is controlled only through env flags so it stays operationally optional and architecturally separate from the main ingest/store path.

Supported flags:

- `JUSTLOG_IMPORT_FOLDER=<path>`
- `JUSTLOG_LEGACY_TXT_MODE=missing_only|merge|off`
- `JUSTLOG_LEGACY_TXT_CHECK_EACH_REQUEST=1`
- `JUSTLOG_IMPORT_DELETE_RAW=1`
- `JUSTLOG_IMPORT_DELETE_ALREADY_IMPORTED_RAW=0|1`
- `JUSTLOG_IMPORT_DELETE_RECONSTRUCTED=1`
- `JUSTLOG_IMPORT_DELETE_ALREADY_IMPORTED_RECONSTRUCTED=0|1`

Behavior summary:

- Raw IRC `.txt` and `.txt.gz` files found under the import folder are imported into native storage.
- Wrapped debug-log TXT files are also accepted when they contain embedded raw IRC payloads, such as `FROM SERVER: @badge-info=... PRIVMSG ...`. Non-IRC wrapper lines are ignored.
- Simple sparse TXT and JSON exports stay separate and are merged at read time.
- `JUSTLOG_LEGACY_TXT_MODE` only controls reconstructed overlays.
- `JUSTLOG_LEGACY_TXT_CHECK_EACH_REQUEST=1` only affects reconstructed-file discovery freshness.
- `JUSTLOG_IMPORT_DELETE_RAW=1` removes successfully imported raw source files.
- `JUSTLOG_IMPORT_DELETE_ALREADY_IMPORTED_RAW=1` also removes raw source files whose path and fingerprint are already marked current in SQLite. This defaults to on.
- `JUSTLOG_IMPORT_DELETE_RECONSTRUCTED=1` removes successfully consumed reconstructed TXT / JSON files.
- `JUSTLOG_IMPORT_DELETE_ALREADY_IMPORTED_RECONSTRUCTED=1` also removes reconstructed TXT / JSON files whose path and fingerprint are already marked consumed in SQLite. This defaults to off.

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
- a single process-local `Arc<Mutex<rusqlite::Connection>>` shared by reads, writes, compaction, import bookkeeping, and reconciliation metadata

Because the store uses one non-reentrant mutex around the SQLite connection, store methods must not call other store methods that also lock `self.db` while the first guard is still alive. If a method needs a follow-up store lookup after collecting query rows, it must drop the active `Statement` and mutex guard first.

This is not hypothetical. A production HTTP/import stall on March 27, 2026 was caused by the compactor holding the DB mutex in `compactable_channel_days()` and `compactable_user_months()` while calling `segment_for_channel_day()` / `segment_for_user_month()`, which attempted to lock the same mutex again. The visible symptom was hanging request-time raw-import bookkeeping in `legacy_txt`, even though the true owner was background compaction.

Reconstructed import-folder overlays do not enter SQLite hot storage. Raw IRC files from the import folder can be imported into SQLite because they already carry stable native message identities.

### Archived Storage

Older data is compacted into Brotli-compressed segment files under the logs directory:

- channel partitions: `segments/channel/<channel_id>/<year>/<month>/<day>.br`
- user partitions: `segments/user/<channel_id>/<user_id>/<year>/<month>.br`

Segment metadata is stored in the `segments` table, and in-progress writes are tracked in `pending_segments` so incomplete files can be cleaned up on restart.

Channel-day archives and user-month archives are intended to represent the same underlying event set. If consistency validation or reconciliation repairs one side, it must update the affected counterpart archives too.

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

Implementation guardrail:

- treat `src/store.rs` methods as lock-scoped units of work
- do not call `segment_for_*`, `read_*`, or other `Store` helpers from inside a block that still holds `let db = self.db.lock().unwrap()`
- if a method needs two phases, collect rows first, then explicitly `drop(statement)` and `drop(db)` before phase two
- when adding loops over query results, be careful that `query_map(...)` and prepared statements can keep the lock alive longer than expected

If a channel-day query requests `raw` output and the entire partition is already archived with passthrough enabled, the API can stream the Brotli file directly without reconstructing the response.

### Reconciliation And Startup Validation

The debug reconciliation layer in `src/debug_sync.rs` adds two related safety workflows:

- post-compaction reconciliation jobs for archived channel-day segments
- startup consistency validation across archived channel-day and user-month segments

Reconciliation is env-driven:

- `JUSTLOG_DEBUG=1` enables compare/trusted reconciliation modes.
- `JUSTLOG_DEBUG_COMPARE_URL=<base>` enables compare-only checks against another JustLog instance.
- `JUSTLOG_DEBUG_TRUSTED_COMPARE_URL=<base>` enables trusted reconciliation and wins if both compare URLs are present.
- `JUSTLOG_DEBUG_FALLBACK_TRUSTED_API=1` allows trusted API proxy fallback for unhealthy archived channel-day reads and local read failures.

When a channel-day partition is compacted into a segment, the store records a reconciliation job in SQLite and schedules it roughly 4 hours in the future. A second background loop in `src/compact.rs` polls due jobs every minute and processes them.

Per-job behavior:

- compare mode fetches the remote `/channelid/<id>/<year>/<month>/<day>?json=1` view, logs diffs to `reconciliation.log`, and marks the partition unhealthy without mutating local archives
- trusted mode uses the trusted remote day as authoritative when local archived events are missing, then rewrites the channel-day archive and affected user-month archives so both archive views stay aligned

Reconciliation status is stored in the `reconciliation_jobs` table. That status is also consulted by API reads so unhealthy archived channel-day requests can be proxied to a trusted instance when trusted fallback is enabled.

Startup validation can also run before the app starts serving traffic:

- `JUSTLOG_DEBUG_VALIDATE_CONSISTENCY_ON_STARTUP=true` scans all archived data, subject to the cached watermark limit.
- Relative values such as `24h`, `7d`, `3mo`, and `1y` scan recent archived data.
- The last successful startup validation time is cached in `consistency-validation-watermark.json` under the logs directory, and future startups reuse it with up to one day of overlap so stable data is not revalidated unnecessarily.
- `JUSTLOG_DEBUG_VALIDATE_CONSISTENCY_ON_STARTUP_IGNORE_LAST_VALIDATED=1` disables that limit and rechecks the full requested scope.

Startup validation walks archived channel-day partitions plus user-month segments, builds an authoritative per-day event set from sane local archives, and then:

- repairs missing or stale channel-day data from local user-month data when possible
- repairs missing or stale user-month data from the channel-day archive when possible
- in trusted mode, refetches a day from the trusted remote instance if local archive corruption prevents a safe local repair

Validation outcomes are logged to `reconciliation.log` and also upserted into `reconciliation_jobs` so later reads see the partition as healthy after repair.

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
- channel-day reads can consult the import-folder compatibility layer
- `/list` can include channel-days discovered from import-folder files
### Import Folder Compatibility Reads

`src/legacy_txt.rs` is deliberately separate from `store.rs`, `ingest.rs`, and compaction. It acts as a compatibility edge around API reads plus raw IRC import bookkeeping.

Current scope:

- channel-day responses
- channel-day discovery in `/list`
- raw IRC import bookkeeping

Current non-scope:

- user-month reads
- random reads
- range reads
- archive generation

The module searches recursively under `JUSTLOG_IMPORT_FOLDER` for supported files anywhere in the tree. Legacy path suffixes such as:

```text
.../<channel_id>/<year>/<month>/<day>.txt
.../<channel_id>/<year>/<month>/<day>.txt.gz
.../<channel_id>/<year>/<month>/<day>.json
.../<channel_id>/<year>/<month>/<day>.json.gz
```

Supported import families:

- raw IRC TXT or TXT.GZ: inserted into native storage with full parsed metadata
- debug-wrapper TXT or TXT.GZ with embedded raw IRC lines: inserted into native storage using only extracted IRC payloads
- simple sparse TXT: reconstructed overlay messages
- JSON chat exports: reconstructed overlay messages with useful metadata preserved in tags

If multiple matching files exist, imported and reconstructed messages are stable-sorted by timestamp. Parse failures are ignored and do not fail requests.

For large migrations, `POST /admin/import/raw` is the preferred path. It reuses the same raw-file classification, imports raw IRC files in bulk, merges affected channel-day archives immediately, refreshes related user-month archives, and returns a summary JSON payload.

For large import folders, raw IRC imports are streamed line-by-line instead of buffering full files in memory. The module logs start, periodic progress, and completion summaries through tracing, writes an `importing` status before each raw-file import begins, and only treats a file as current when its fingerprint matches a terminal status (`imported` or `seen`). If the process crashes or Docker stops mid-import, unfinished raw files are retried on the next matching request. When the delete flags are enabled, consumed files are removed after successful raw import, already-current raw files can also be removed during preflight, and reconstructed files are removed after successful overlay parsing, then empty parent directories are pruned.

Whenever the import folder is checked, the module also prunes empty directories below that root and removes empty parent layers upward when possible.

Debugging note:

- if request logs reach `Checking raw import status for ...` but not `Completed raw import status check for ...`, the importer is usually waiting on the store mutex rather than parsing a file
- in that situation, inspect `src/compact.rs`, reconciliation work, and any recent `src/store.rs` changes before assuming the raw importer is the root cause

### Admin and Opt-Out Routes

- `POST /optout`: create a short-lived confirmation code for chat-based self opt-out
- `POST /admin/channels`: add channel IDs, requires `X-Api-Key`
- `DELETE /admin/channels`: remove channel IDs, requires `X-Api-Key`
- `POST /admin/import/raw`: bulk raw import under `JUSTLOG_IMPORT_FOLDER`, requires `X-Api-Key`

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

This import path is distinct from the import-folder compatibility layer:

- `src/import.rs` imports raw IRC-style history into normal storage
- `src/legacy_txt.rs` handles recursive dropped import files during API reads, importing raw IRC files and overlaying reconstructed TXT/JSON data

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
