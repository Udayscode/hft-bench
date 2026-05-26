# =========================================================
# BUILD STAGE
# =========================================================
FROM rust:1.87-slim-bookworm AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    protobuf-compiler \
    clang \
    cmake \
    make \
    g++ \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ---------------------------------------------------------
# Dependency cache layer — copy manifests + proto first
# ---------------------------------------------------------

COPY services/bot-fleet/Cargo.toml services/bot-fleet/Cargo.lock ./services/bot-fleet/
COPY proto/ ./proto/

WORKDIR /app/services/bot-fleet

# Create dummy source for dependency compilation cache
RUN mkdir -p src && echo "fn main() {}" > src/main.rs

RUN cargo build --release

# Cleanup dummy build artifacts
RUN rm -rf src target/release/deps/bot-fleet*

# ---------------------------------------------------------
# Real source copy
# ---------------------------------------------------------

COPY services/bot-fleet/src ./src
COPY services/bot-fleet/build.rs ./build.rs

# Production optimized release build
RUN cargo build --release --locked

# Strip symbols to reduce binary size
RUN strip target/release/bot-fleet

# =========================================================
# RUNTIME STAGE
# =========================================================
FROM debian:bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV RUST_LOG=info

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    tini \
    && rm -rf /var/lib/apt/lists/*

RUN useradd --system --create-home --uid 10001 appuser

WORKDIR /app

COPY --from=builder /app/services/bot-fleet/target/release/bot-fleet /app/bot-fleet

RUN chown -R appuser:appuser /app

USER appuser

EXPOSE 50052

ENTRYPOINT ["/usr/bin/tini", "--"]

CMD ["./bot-fleet"]