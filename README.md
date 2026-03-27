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

Startup validation caches the last successful validation time in the logs directory and, by default, only rechecks recent data plus up to one day of overlap. Setting `JUSTLOG_DEBUG_VALIDATE_CONSISTENCY_ON_STARTUP_IGNORE_LAST_VALIDATED=1` forces the full requested scope to be revalidated on every startup.

## Run with Docker Compose

Docker Compose is the standard container path for both local testing and Ubuntu production deployments.

Create a persistent data directory and put your config at `./data/config.json`:

```powershell
New-Item -ItemType Directory -Force data | Out-Null
```

Start the service:

```powershell
docker compose up -d --build
```

Stop it again:

```powershell
docker compose down
```

The committed [`compose.yaml`](C:\Users\Albert\Sync\Projects\JustLogV2\compose.yaml) file builds from the local `Dockerfile`, publishes port `8025`, mounts `./data` to `/data`, keeps logs and SQLite state under `./data`, and uses `restart: unless-stopped`.

## Ubuntu Production Setup

Copy or sync the project to the Ubuntu host, make sure Docker Engine and the Docker Compose plugin are installed, then start the service from the project directory:

```bash
mkdir -p data
docker compose up -d --build
```

The container expects `/data/config.json`, so the host-side file should be `./data/config.json`. Logs and SQLite state remain under that same `data/` directory.

You can use personal scripts or sync helpers to move the repo onto the server, but those helpers are local workflow choices rather than part of the repository interface.

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
