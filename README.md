# JustLogV2

JustLogV2 is a Twitch chat logger with an HTTP API for serving stored logs.

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

The app listens on port `8025` by default and expects a JSON config file. The minimum useful config looks like this:

```json
{
  "clientID": "your-twitch-client-id",
  "clientSecret": "your-twitch-client-secret",
  "adminAPIKey": "replace-me",
  "channels": ["123456789"]
}
```

For the Docker and upload workflow in this repo, keep the runtime file at `./data/config.json` instead.

## Legacy TXT Compatibility

JustLogV2 also has an optional read-only legacy TXT compatibility layer for sparse chat exports. This is intended as a backward-compatibility feature for API reads, not as part of the main ingest, storage, or compaction pipeline.

Supported env flags:

- `JUSTLOG_LEGACY_TXT_ENABLED=1`: turn the compatibility layer on.
- `JUSTLOG_LEGACY_TXT_ROOT=<path>`: root folder to search for legacy TXT files.
- `JUSTLOG_LEGACY_TXT_MODE=missing_only|merge|off`: request behavior. Default is `missing_only`.
- `JUSTLOG_LEGACY_TXT_CHECK_EACH_REQUEST=1`: re-check the legacy root on every request instead of only trusting startup availability.

Mode behavior:

- `missing_only`: use legacy TXT only when native JustLog data for that channel-day is missing.
- `merge`: merge native and TXT messages by timestamp using a stable sort.
- `off`: completely ignore TXT data.

The current compatibility scope is channel-day reads and `/list` channel-day discovery. User-month, random, and range reads still use native data only.

### Legacy TXT layout

The legacy reader looks for files matching:

```text
.../<channel_id>/<year>/<month>/<day>.txt
```

The search is recursive under `JUSTLOG_LEGACY_TXT_ROOT`, so you can copy an existing JustLog-style folder tree anywhere under that root and the API will still find matching days.

Example:

```text
D:\legacy-root\copied\justlog\31062476\2024\1\2.txt
```

If the requested route is `/channelid/31062476/2024/1/2`, that file is eligible.

### TXT parsing behavior

The parser is intentionally minimal and targets response compatibility for lines like:

```text
[0:00:04] SomeUser: hello
```

It derives:

- absolute timestamps from the requested channel-day plus the per-line offset
- normalized usernames from the visible name
- stable fallback ids from timestamp, username, and message text

Unsupported metadata is left empty, and TXT parse failures never fail the request.

### Empty directory cleanup

Whenever the legacy root is checked, the compatibility layer prunes empty directories under that root and removes empty parent directories upward when possible. This helps clean up copied legacy trees after files are moved or deleted, while leaving non-empty branches untouched.

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
- `data/config.json` for Twitch credentials, admin API key, startup channels, and other JSON config

`.env.template` exists as a clean starting point for a local `.env`.

Start the service:

```powershell
docker compose up -d --build
```

Stop it again:

```powershell
docker compose down
```

The committed [`docker-compose.yml`](C:\Users\Albert\Sync\Projects\JustLogV2\docker-compose.yml) file builds from the local `Dockerfile`, reads defaults from `.env`, publishes `${JUSTLOG_PUBLIC_PORT}`, mounts `${JUSTLOG_DATA_DIR}` to `/data`, keeps logs and SQLite state under that mounted directory, and uses `restart: unless-stopped`. The container config path stays at the Docker image default of `/data/config.json`.

## Ubuntu Production Setup

Copy or sync the project to the Ubuntu host, make sure Docker Engine and the Docker Compose plugin are installed, then start the service from the project directory:

```bash
mkdir -p data
cp data/config.template.json data/config.json
docker compose up -d --build
```

The container expects `/data/config.json`, so the host-side file should be `./data/config.json`. Logs and SQLite state remain under that same `data/` directory. Review `.env` before the first deployment if you want to change the host port or enable any optional env-driven features.

`upload-project-to-server.cmd` now checks for both `.env` and `data/config.json` before syncing so it does not push an incomplete deployment by accident.

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
