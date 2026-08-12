# syntax=docker/dockerfile:1

# --- builder -----------------------------------------------------------------
# The full `rust` image rather than `-slim`: reqwest pulls rustls, whose crypto
# provider is aws-lc-rs, which compiles C. That needs a toolchain and cmake.
FROM rust:1.96-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

# The cache mounts do not survive into the layer, so the binary has to be copied
# out of the target dir inside the same RUN. `--locked` because Cargo.lock is
# committed -- the image should build the versions that were tested.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked \
    && cp target/release/rpc-plus-plus /usr/local/bin/rpc-plus-plus

# --- runtime -----------------------------------------------------------------
FROM debian:bookworm-slim

# rustls-platform-verifier reads the OS trust store. Without these, every HTTPS
# upstream fails to connect.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin app

# settings.yaml is looked up relative to the working directory, and it is never
# baked in -- provider URLs carry the API key in the path. Mount it at run time:
#
#   docker run --rm -p 8080:8080 \
#     -v "$PWD/settings.yaml:/app/settings.yaml:ro" rpc-plus-plus
#
# and set `application_host: 0.0.0.0` in it, or the bind stays on loopback
# inside the container and the published port reaches nothing.
WORKDIR /app

COPY --from=builder /usr/local/bin/rpc-plus-plus /usr/local/bin/rpc-plus-plus

USER app

# Documentation only. The port actually bound is `application_port`.
EXPOSE 8080

# Exec form, so the process is PID 1 and receives the SIGTERM that main.rs
# already listens for.
ENTRYPOINT ["rpc-plus-plus"]
