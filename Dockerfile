# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1.85

FROM rust:${RUST_VERSION}-bookworm AS chef

WORKDIR /app

RUN cargo install cargo-chef --version 0.1.71

FROM chef AS planner

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder

RUN apt-get update \
    && apt-get install --yes --no-install-recommends pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY --from=planner /app/recipe.json recipe.json

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo chef cook --release --locked --recipe-path recipe.json

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked \
    && cp /app/target/release/justlog /tmp/justlog

FROM debian:bookworm-slim

ARG APP_UID=1000
ARG APP_GID=1000

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /tmp/justlog /usr/local/bin/justlog

RUN groupadd --gid ${APP_GID} justlog \
    && useradd --uid ${APP_UID} --gid ${APP_GID} --create-home --shell /usr/sbin/nologin justlog \
    && mkdir -p /data/logs \
    && chown -R ${APP_UID}:${APP_GID} /data /home/justlog

USER justlog

EXPOSE 8026

VOLUME ["/data"]

ENTRYPOINT ["justlog"]
CMD ["--config", "/data/config.json"]
