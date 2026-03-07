# syntax=docker/dockerfile:1
# Multi-arch Containerfile for demoflight using cargo-chef for dependency caching
# Supports: linux/amd64, linux/arm64 (native builds via GitHub Actions matrix)

# ==============================================================================
# Chef stage - base image with cargo-chef installed
# ==============================================================================
FROM docker.io/lukemathwalker/cargo-chef:latest-rust-1.93-bookworm AS chef
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

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    libprotobuf-dev \
    musl-tools \
    && rm -rf /var/lib/apt/lists/*

# Add musl target for current architecture
RUN case "$(uname -m)" in \
        x86_64)  rustup target add x86_64-unknown-linux-musl ;; \
        aarch64) rustup target add aarch64-unknown-linux-musl ;; \
        *) echo "Unsupported architecture: $(uname -m)" && exit 1 ;; \
    esac

# Cook dependencies (this layer is cached if Cargo.toml/Cargo.lock unchanged)
COPY --from=planner /build/recipe.json recipe.json
RUN case "$(uname -m)" in \
        x86_64)  TARGET="x86_64-unknown-linux-musl" ;; \
        aarch64) TARGET="aarch64-unknown-linux-musl" ;; \
    esac && \
    cargo chef cook --release --target "$TARGET" --recipe-path recipe.json

# Copy source and build application
COPY . .
RUN case "$(uname -m)" in \
        x86_64)  TARGET="x86_64-unknown-linux-musl" ;; \
        aarch64) TARGET="aarch64-unknown-linux-musl" ;; \
    esac && \
    cargo build --release --target "$TARGET" && \
    cp "target/$TARGET/release/demoflight" /demoflight && \
    strip /demoflight

# ==============================================================================
# Runtime stage - distroless static image (nonroot)
# ==============================================================================
FROM gcr.io/distroless/static-debian12:nonroot

LABEL org.opencontainers.image.title="demoflight"
LABEL org.opencontainers.image.description="Arrow Flight server for streaming SQL queries over GOTV broadcasts"
LABEL org.opencontainers.image.source="https://github.com/demofusion/demoflight"
LABEL org.opencontainers.image.licenses="Apache-2.0"

COPY --from=builder /demoflight /usr/local/bin/demoflight

EXPOSE 50051
EXPOSE 9090

ENTRYPOINT ["/usr/local/bin/demoflight"]
