# JustLogV2

JustLogV2 is a Twitch chat logger with an HTTP API for serving stored logs. Its primary mode is anonymous Twitch IRC ingest using a generated `justinfan<digits>` account.

The repo is intended to be platform agnostic. A common workflow is Windows for development and operator tooling, with Ubuntu/Linux as the production host.

Archived channel-day data and archived user-month data are expected to stay consistent. When reconciliation or startup consistency validation repairs one side, it also repairs the affected counterpart archives.

## Test suites

This repo now uses a layered test strategy:

- `cargo test --lib`: crate-level unit coverage when source modules add small focused tests.
- `cargo test --test smoke`: fast deterministic smoke coverage for core config, API, and archived-read behavior.
- `cargo test --test integration`: local end-to-end coverage using mock Helix and IRC services.
- `cargo test --test network`: local socket-based networking checks over real loopback HTTP/TCP.
- `cargo test --test live_network -- --nocapture`: opt-in live network smoke checks.

Live network tests are gated and skip unless `JUSTLOG_RUN_LIVE_NETWORK_TESTS=1` is set. The optional Twitch Helix smoke test also requires `TWITCH_CLIENT_ID` and `TWITCH_CLIENT_SECRET`.

## Run locally

PowerShell examples are shown below, but the same commands work from any shell with equivalent path syntax.

```powershell
cargo run -- --config config.json
```

The app listens on port `8026` by default and expects a JSON config file. The default and recommended setup is anonymous chat ingest, and the minimum useful config looks like this:

```json
{
    "adminAPIKey": "replace-me",
    "channels": ["channel_login_here"]
}
```

IRC auth is anonymous by default. If `oauth` is omitted or empty, JustLog connects with `PASS _` and a generated `NICK justinfan<digits>`. In that mode, `username` is ignored for the IRC session. If you provide `oauth`, JustLog switches to authenticated bot-style IRC auth and uses the configured `username`.

`clientID` and `clientSecret` are optional unless you want Twitch Helix-backed features such as login/id resolution, `/channels` name resolution from stored IDs, or admin/chat commands that translate logins into channel IDs. Helix credentials are separate from IRC auth. If Helix credentials are omitted, startup ingestion can still join channels listed by login name directly, but numeric channel IDs cannot be resolved at startup.

For the Docker and upload workflow in this repo, keep the runtime file at `./data/config.json` instead.

## Recent Message Backfill

JustLogV2 can optionally fetch short-gap missed chat from Robotty's recent-messages API and store it through the normal ingest pipeline. This path is env-driven, off by default, and best-effort only.

Supported env flags:

- `JUSTLOG_RECENT_MESSAGES_ENABLED=0|1`: enable or disable Robotty backfill. Default is `0`.
- `JUSTLOG_RECENT_MESSAGES_URL=<base-url>`: recent-messages base URL. Default is `https://recent-messages.robotty.de/api/v2/recent-messages`.
- `JUSTLOG_RECENT_MESSAGES_LIMIT=<n>`: number of recent messages to request per fetch. Default is `800`.

Behavior summary:

- Fetches run after successful IRC connects and reconnects for the currently desired channels.
- Fetches also run after runtime channel joins for newly joined logins.
- Remote API errors, malformed payloads, and unsupported IRC lines are logged and skipped.
- Backfill never tears down live ingest if the remote API fails.
- Fetched messages are replayed into storage only and do not re-run chat commands such as `!justlog ...`.

## Import Folder Compatibility

JustLogV2 supports an optional recursive import folder for channel-day reads, `/list`, and a dedicated admin-triggered bulk raw import path. This is a compatibility feature layered around API reads and migration workflows; it is not part of live ingest.

Supported env flags:

- `JUSTLOG_IMPORT_HOST_DIR=<host-path>`: Docker Compose host folder to mount. Default is `./data/import-folder`.
- `JUSTLOG_IMPORT_FOLDER=<path>`: in-container or local folder to search recursively.
- `JUSTLOG_LEGACY_TXT_MODE=missing_only|merge|off`: applies only to reconstructed data. Default is `missing_only`.
- `JUSTLOG_LEGACY_TXT_CHECK_EACH_REQUEST=1`: re-check reconstructed-file availability on each request.
- `JUSTLOG_IMPORT_DELETE_RAW=1`: delete raw IRC source files after they are successfully imported into native storage.
- `JUSTLOG_IMPORT_DELETE_ALREADY_IMPORTED_RAW=0|1`: delete raw IRC source files that are already marked current in native storage. Default is `1`.
- `JUSTLOG_IMPORT_DELETE_RECONSTRUCTED=1`: delete reconstructed TXT / JSON source files after they are successfully consumed on read.
- `JUSTLOG_IMPORT_DELETE_ALREADY_IMPORTED_RECONSTRUCTED=0|1`: delete reconstructed TXT / JSON source files that are already marked consumed with the same fingerprint. Default is `0`.
- `JUSTLOG_IMPORT_MAX_COMPRESS_THREADS=<n>`: global cap for concurrent archive compression jobs. Default is `min(available_parallelism, 4)`.
- `JUSTLOG_IMPORT_MAX_RAW_WORKERS=<n>`: global cap for concurrent raw-file parsing workers during bulk raw import. Default is `min(available_parallelism, 8)`.

Behavior summary:

- Raw IRC `.txt` and `.txt.gz` files are imported into the native store when found.
- Wrapped debug logs that contain embedded IRC payloads such as `... FROM SERVER: @badge-info=... PRIVMSG ...` are also treated as raw IRC sources. Other log lines in those files are ignored.
- Simple sparse TXT files like `[0:04:26] user: msg` stay separate and are controlled by `JUSTLOG_LEGACY_TXT_MODE`.
- JSON `.json` and `.json.gz` chat exports stay separate and are merged at read time.
- `off` only disables reconstructed overlays. It does not disable raw IRC imports.
- Large raw migrations should use `POST /admin/import/raw` instead of relying on a normal log request to do the work.

The current scope is channel-day reads and `/list`. User-month, random, and range reads still use native data only.

### How to add files

For local non-Docker runs, point `JUSTLOG_IMPORT_FOLDER` at any folder and drop files there.

For the committed Docker Compose setup, the default host path is:

```text
./data/import-folder
```

That folder is mounted into the container as:

```text
/import-folder
```

The easiest workflow is:

1. Set `JUSTLOG_IMPORT_FOLDER=/import-folder` in `.env`.
2. Choose `JUSTLOG_LEGACY_TXT_MODE` for reconstructed simple TXT and JSON overlays.
3. Optionally set `JUSTLOG_IMPORT_DELETE_RAW=1`, adjust `JUSTLOG_IMPORT_DELETE_ALREADY_IMPORTED_RAW`, and/or set `JUSTLOG_IMPORT_DELETE_RECONSTRUCTED=1` plus `JUSTLOG_IMPORT_DELETE_ALREADY_IMPORTED_RECONSTRUCTED=1` if you want consumed source files removed automatically.
4. Copy files anywhere under `./data/import-folder`.
5. Restart the service with `docker compose up -d --build` or `docker compose restart`.

For large v1 migrations, prefer:

```text
POST /admin/import/raw
```

with the `X-Api-Key` header instead of waiting for a normal `/channelid/...` request to import everything.

Example host paths:

```text
./data/import-folder/copied/raw/31062476/2024/10/20.txt
./data/import-folder/copied/simple/31062476/2024/10/20.txt
./data/import-folder/copied/json/31062476/2024/10/20.json.gz
```

The service recurses under the import folder, so only the trailing `.../<channel_id>/<year>/<month>/<day>.*` suffix matters.

It also supports v1-style raw layouts such as:

```text
./data/import-folder/v1/<channel_id>/<year>/<month>/<day>/channel.txt.gz
./data/import-folder/v1/<channel_id>/<year>/<month>/<user_id>.txt.gz
```

### Supported file families

Supported file types:

- `.txt`
- `.txt.gz`
- `.json`
- `.json.gz`

TXT files are classified per file:

- Raw IRC text with stable Twitch metadata is imported into native storage and keeps all parsed fields.
- Debug-style wrapper logs are accepted when they contain embedded raw IRC lines; unrelated wrapper/debug lines are skipped.
- Simple sparse text is reconstructed into overlay messages.

JSON exports are reconstructed into overlay messages using fields such as:

- `comments[]._id`
- `comments[].created_at`
- `comments[].channel_id`
- `comments[].commenter.display_name`
- `comments[].commenter._id`
- `comments[].commenter.name`
- `comments[].message.body`

Useful extra JSON metadata is preserved in message tags when available, such as user color, badges, emoticons, content id, video id/title, streamer name, and commenter logo.

### Large import behavior

Large import folders are handled incrementally:

- Raw IRC imports are streamed line-by-line instead of loading full files into memory.
- Bulk raw import now prefers v1 day-level `channel.txt(.gz)` files over same-day numeric shard files, so a canonical day file can skip redundant shard work before status checks and worker scheduling.
- The importer logs start, periodic progress, and completion summaries through normal tracing output.
- Progress logs are emitted every `100000` scanned lines for long-running raw imports.
- Archive compression now runs in parallel across independent segment files, capped by `JUSTLOG_IMPORT_MAX_COMPRESS_THREADS`.
- Raw-file bookkeeping records an `importing` state before work starts and only marks a file complete when it finishes.
- If the process crashes or Docker stops mid-import, unfinished raw files are retried on the next matching request.
- Re-importing an already completed raw file is skipped when its fingerprint is unchanged.
- If `JUSTLOG_IMPORT_DELETE_RAW=1`, successfully imported raw files are deleted after completion.
- If `JUSTLOG_IMPORT_DELETE_ALREADY_IMPORTED_RAW=1`, raw files that are already current in native storage are also deleted during the preflight scan. This flag defaults to `1`.
- If `JUSTLOG_IMPORT_DELETE_RECONSTRUCTED=1`, successfully parsed reconstructed TXT / JSON files are deleted after the request that consumed them.
- If `JUSTLOG_IMPORT_DELETE_ALREADY_IMPORTED_RECONSTRUCTED=1`, reconstructed TXT / JSON files that were already consumed successfully and are still unchanged are deleted during later discovery before reparsing.
- `POST /admin/import/raw` can bulk-import raw IRC files under `JUSTLOG_IMPORT_FOLDER`, optionally filtered by `channel_id`, limited by file count, or run as `dry_run`.

### Import stall troubleshooting

If `/healthz` responds quickly but a real log route such as `/channelid/<id>/<year>/<month>/<day>` hangs, the problem may be outside the importer itself.

Useful signals:

- `HTTP request start: GET ...` confirms the request entered `src/api.rs`.
- `Import-folder discovery completed ...` confirms recursive import-folder scanning finished.
- `Checking raw import status for ...` without a matching `Completed raw import status check for ...` points at store lock contention before the actual file import starts.
- `Starting raw import scan for ...` means preflight completed and the request is inside the file import loop.
- `Completed raw import ...` plus `Deleted consumed import file ...` confirms the import and delete-after-success path both finished.
- `Compression job queued ...`, `Compression job started ...`, `Compression job completed ...`, and `Compression install completed ...` show the archive compression lifecycle after raw import or compaction.
- `Parsed log request ...`, `Completed opt-out checks ...`, `Completed trusted fallback check ...`, and `Entering dated_response ...` show whether a request is stalling before the importer starts.

One real failure pattern in this repo was a self-deadlock in the background compactor. The compactor held the `Store` SQLite mutex while asking the store another question that also tried to lock the same mutex. That blocked request-time calls such as `imported_raw_file_is_current(...)`, which made log routes appear to hang in the import layer even though the real owner was background compaction.

Recommended debugging order:

1. Confirm `GET /healthz` still returns immediately.
2. Trigger one hanging log request with a short curl timeout.
3. Read recent container logs and compare the last emitted line against the import progress markers above.
4. If the last line is a store-status check, inspect background workers rather than only the import code.
5. If needed, capture a thread dump while the route is hung to identify the mutex owner.

On Linux hosts, a useful live snapshot is:

```bash
curl --max-time 25 http://127.0.0.1:8026/channelid/31062476/2024/2/24 >/dev/null 2>&1 &
sleep 2
gdb -batch -ex "info threads" -ex "thread apply all bt" -p "$(docker inspect justlog-v2 --format '{{.State.Pid}}')"
```

If several request threads are blocked in `Store::imported_raw_file_is_current()` or another small store read, look for another thread already inside `src/store.rs` while holding the mutex.

### Empty directory cleanup

Whenever the import folder is checked, the compatibility layer prunes empty directories below that root and removes empty parent directories upward when possible, while leaving non-empty branches alone. This also runs after delete-after-success removes consumed import files.

## Debug Validation

Optional env flags support reconciliation and startup consistency validation:

- `JUSTLOG_DEBUG=1`
- `JUSTLOG_DEBUG_COMPARE_URL=<justlog-base-url>`
- `JUSTLOG_DEBUG_TRUSTED_COMPARE_URL=<justlog-base-url>`
- `JUSTLOG_DEBUG_FALLBACK_TRUSTED_API=1`
- `JUSTLOG_DEBUG_VALIDATE_CONSISTENCY_ON_STARTUP=true|24h|7d|3mo|1y`
- `JUSTLOG_DEBUG_VALIDATE_CONSISTENCY_ON_STARTUP_IGNORE_LAST_VALIDATED=1`

Behavior summary:

- `JUSTLOG_DEBUG_COMPARE_URL` enables compare-only reconciliation. Archived channel-day partitions are checked against another JustLog instance after compaction, conflicts are logged, and mismatches are marked unhealthy without changing local data.
- `JUSTLOG_DEBUG_TRUSTED_COMPARE_URL` enables trusted reconciliation. Missing local archived events are repaired from the trusted remote source, and the affected channel-day plus related user-month archives are rewritten to stay in sync.
- If both compare URLs are set, trusted mode wins.
- Reconciliation work is scheduled when a channel-day archive is written and becomes eligible about 4 hours later, then runs in the background.
- Checks and repairs are appended to `reconciliation.log` under the logs directory.
- `JUSTLOG_DEBUG_FALLBACK_TRUSTED_API=1` only works with `JUSTLOG_DEBUG_TRUSTED_COMPARE_URL`. When enabled, unhealthy archived channel-day reads and local read failures can be proxied to the trusted JustLog instance instead of failing locally.

Startup validation runs before the app starts serving traffic. It scans archived channel-day and user-month data, does local sanity checks, and repairs safe mismatches between the two archive views. In trusted mode it can also recover from corrupted local archive data by refetching the authoritative channel-day from the trusted remote instance.

The last successful startup validation time is cached in `consistency-validation-watermark.json` under the logs directory. By default, future startups only recheck the requested recent scope plus up to one day of overlap. Setting `JUSTLOG_DEBUG_VALIDATE_CONSISTENCY_ON_STARTUP_IGNORE_LAST_VALIDATED=1` forces the full requested scope to be revalidated on every startup.

## Run with Docker Compose

Docker Compose is the standard container path for both local testing and Ubuntu production deployments.

Start from the committed defaults:

```powershell
New-Item -ItemType Directory -Force data | Out-Null
Copy-Item data/config.template.json data/config.json
```

Then edit:

- `.env` if you want to change the published port, data mount, or optional env-only feature flags
- `data/config.json` for startup channels, admin API key, optional Twitch Helix credentials, and optional authenticated IRC bot settings

`.env.template` exists as a clean starting point for a local `.env`.

Start the service:

```powershell
docker compose up -d --build
```

Stop it again:

```powershell
docker compose down
```

`docker stop` and `docker compose down` now trigger graceful shutdown handling so the HTTP listener stops accepting new work and ingest/background loops exit instead of immediately trying to reconnect.

The committed [`docker-compose.yml`](C:\Users\Albert\Sync\Projects\JustLogV2\docker-compose.yml) file builds from the local `Dockerfile`, reads defaults from `.env`, publishes host port `8026` by default through `${JUSTLOG_PUBLIC_PORT}` and forwards it to the app's in-container listener on `8026`, mounts `./data` to `/data`, mounts `${JUSTLOG_IMPORT_HOST_DIR:-./data/import-folder}` to `/import-folder`, keeps logs and SQLite state under the `/data` mount, and uses `restart: unless-stopped`. The container config path stays at the Docker image default of `/data/config.json`.

Keep the published Docker port and `listenAddress` aligned. A mismatched host mapping such as `8026 -> 8026` while the app still listens on `:8025` can look like an HTTP regression even when the app is otherwise healthy.

## Ubuntu Production Setup

Copy or sync the project to the Ubuntu host, make sure Docker Engine and the Docker Compose plugin are installed, then start the service from the project directory:

```bash
mkdir -p data
cp data/config.template.json data/config.json
docker compose up -d --build
```

The container expects `/data/config.json`, so the host-side file should be `./data/config.json`. Logs and SQLite state remain under that same `data/` directory. Review `.env` before the first deployment if you want to change the host port or enable any optional env-driven features.

## Auto-published images

This repo ships a GitHub Actions workflow that publishes a public image to:

`ghcr.io/alb11747/justlogv2`

The workflow runs on:

- pushes to `main` (`latest` and commit SHA tags)
- version tags like `v0.1.0`
- manual dispatch from the Actions tab

If GitHub Container Registry creates the package as private on first publish, change the package visibility once in the repo's package settings and later publishes will keep using that package.

## CI test strategy

GitHub Actions is split so fast feedback and release confidence stay separate:

- `.github/workflows/tests.yml` runs `cargo test --lib` and `cargo test --test smoke` on pushes to `main` and on pull requests.
- `.github/workflows/release-tests.yml` runs `cargo test --locked --test integration`, `cargo test --locked --test network`, and `cargo check --locked --release` on `v*.*.*` tags and manual dispatch.
- `.github/workflows/live-network-tests.yml` runs the opt-in live network suite on manual dispatch and on a weekly schedule. It is the only workflow that passes live-network environment variables and Twitch secrets into tests.
