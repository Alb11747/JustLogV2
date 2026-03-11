# JustLogV2

JustLogV2 is a Twitch chat logger with an HTTP API for serving stored logs.

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
