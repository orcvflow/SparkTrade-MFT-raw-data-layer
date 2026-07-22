# ─── Stage 1: Builder ────────────────────────────────────────────────────────
FROM golang:1.23-bookworm AS builder

# Install ZeroMQ development library (required by pebbe/zmq4)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libzmq3-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Cache dependencies before copying full source
COPY go.mod go.sum ./
RUN go mod download

# Copy source
COPY . .

# Build with optimizations; CGO required for ZeroMQ
RUN CGO_ENABLED=1 GOOS=linux go build \
    -ldflags="-s -w -X main.version=$(git describe --tags --always --dirty 2>/dev/null || echo dev)" \
    -o /build/raw-data-layer \
    ./cmd/raw-data-layer

# ─── Stage 2: Runtime ────────────────────────────────────────────────────────
FROM debian:bookworm-slim

# Install ZeroMQ runtime library + CA certificates (for WebSocket TLS)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libzmq5 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -r -u 1001 -g root quant

WORKDIR /app

# Copy binary and assets
COPY --from=builder /build/raw-data-layer .
COPY --chown=quant:root mappings/ ./mappings/
COPY --chown=quant:root config/config.yaml ./config/

# Create data directories
RUN mkdir -p /var/log/raw_data/wal /app/logs && \
    chown -R quant:root /var/log/raw_data /app/logs

USER quant

# Ports: 5555=ZeroMQ PUB, 8080=Health HTTP
EXPOSE 5555 8080

# Health check
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:8080/live || exit 1

ENTRYPOINT ["/app/raw-data-layer"]
CMD ["--mappings", "/app/mappings", "--wal-dir", "/var/log/raw_data/wal", "--log-level", "info"]
