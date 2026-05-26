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

# Dependency cache layer
COPY services/shadow-engine/Cargo.toml services/shadow-engine/Cargo.lock ./services/shadow-engine/
COPY proto/ ./proto/

WORKDIR /app/services/shadow-engine

# Create dummy source for dependency compilation cache
RUN mkdir -p src && echo "fn main() {}" > src/main.rs

RUN cargo build --release

# Cleanup dummy build artifacts
RUN rm -rf src target/release/deps/shadow-engine*

# Real source copy
COPY services/shadow-engine/src ./src
COPY services/shadow-engine/build.rs ./build.rs

# Production optimized release build
RUN cargo build --release --locked

# Strip symbols to reduce binary size
RUN strip target/release/shadow-engine

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

RUN useradd --system --create-home --uid 10002 appuser

WORKDIR /app

COPY --from=builder /app/services/shadow-engine/target/release/shadow-engine /app/shadow-engine

RUN chown -R appuser:appuser /app

USER appuser

EXPOSE 50053

ENTRYPOINT ["/usr/bin/tini", "--"]

CMD ["./shadow-engine"]
