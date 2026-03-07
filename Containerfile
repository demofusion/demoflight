# syntax=docker/dockerfile:1
# Multi-arch Containerfile for demoflight using cargo-chef for dependency caching
# Supports: linux/amd64, linux/arm64 (native builds via GitHub Actions matrix)

# ==============================================================================
# Chef stage - base image with cargo-chef installed
# ==============================================================================
FROM docker.io/lukemathwalker/cargo-chef:0.1.77-rust-1.93-bookworm AS chef
WORKDIR /build

# ==============================================================================
# Planner stage - analyze project and create recipe
# ==============================================================================
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ==============================================================================
# Builder stage - cook dependencies then build application
# ==============================================================================
FROM chef AS builder

ARG RUST_TARGET

# Install build dependencies and add musl target
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    libprotobuf-dev \
    musl-tools \
    && rm -rf /var/lib/apt/lists/* \
    && rustup target add "$RUST_TARGET"

# Cook dependencies (this layer is cached if Cargo.toml/Cargo.lock unchanged)
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --release --target "$RUST_TARGET" --recipe-path recipe.json

# Copy source and build application
COPY . .
RUN cargo build --release --target "$RUST_TARGET" && \
    cp "target/$RUST_TARGET/release/demoflight" /demoflight && \
    strip /demoflight

# ==============================================================================
# Runtime stage - distroless static image (nonroot)
# ==============================================================================
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /demoflight /usr/local/bin/demoflight

EXPOSE 50051
EXPOSE 9090

ENTRYPOINT ["/usr/local/bin/demoflight"]
