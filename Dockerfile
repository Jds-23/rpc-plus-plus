FROM rust:1.96-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked \
    && cp target/release/rpc-plus-plus /usr/local/bin/rpc-plus-plus

# --- runtime -----------------------------------------------------------------
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin app

WORKDIR /app

COPY --from=builder /usr/local/bin/rpc-plus-plus /usr/local/bin/rpc-plus-plus

USER app

EXPOSE 8080

ENTRYPOINT ["rpc-plus-plus"]
