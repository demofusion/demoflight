# syntax=docker/dockerfile:1
# Multi-arch Containerfile for demoflight
# Supports: linux/amd64, linux/arm64 (native builds on matching arch)
# Static musl binary, distroless runtime

# ==============================================================================
# Builder stage - compile static Rust binary with musl
# ==============================================================================
FROM docker.io/library/rust:1.93-bookworm AS builder

WORKDIR /build

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

# Copy source code
COPY . .

# Build static release binary
ENV CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse
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

# OCI labels
LABEL org.opencontainers.image.title="demoflight"
LABEL org.opencontainers.image.description="Arrow Flight server for streaming SQL queries over GOTV broadcasts"
LABEL org.opencontainers.image.source="https://github.com/demofusion/demoflight"
LABEL org.opencontainers.image.licenses="Apache-2.0"

COPY --from=builder /demoflight /usr/local/bin/demoflight

# gRPC Flight port
EXPOSE 50051
# Prometheus metrics port
EXPOSE 9090

ENTRYPOINT ["/usr/local/bin/demoflight"]
