# JustLogV2

JustLogV2 is a Twitch chat logger with an HTTP API for serving stored logs.

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

## Debug Validation

Optional env flags support reconciliation and startup consistency validation:

- `JUSTLOG_DEBUG=1`
- `JUSTLOG_DEBUG_COMPARE_URL=<justlog-base-url>`
- `JUSTLOG_DEBUG_TRUSTED_COMPARE_URL=<justlog-base-url>`
- `JUSTLOG_DEBUG_FALLBACK_TRUSTED_API=1`
- `JUSTLOG_DEBUG_VALIDATE_CONSISTENCY_ON_STARTUP=true|24h|7d|3mo|1y`
- `JUSTLOG_DEBUG_VALIDATE_CONSISTENCY_ON_STARTUP_IGNORE_LAST_VALIDATED=1`

Startup validation caches the last successful validation time in the logs directory and, by default, only rechecks recent data plus up to one day of overlap. Setting `JUSTLOG_DEBUG_VALIDATE_CONSISTENCY_ON_STARTUP_IGNORE_LAST_VALIDATED=1` forces the full requested scope to be revalidated on every startup.

## Run with Docker

Build the image locally:

```powershell
docker build -t justlogv2 .
```

Start the container with a persistent data directory:

```powershell
docker run --rm -p 8025:8025 -v ${PWD}/data:/data justlogv2
```

Put your config at `./data/config.json`. Logs and SQLite data stay under `/data`.

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
