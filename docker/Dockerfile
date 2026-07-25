# ─── Stage 1: Builder ────────────────────────────────────────────────────────
# Builds ALL FIVE binaries: the monolith (cmd/raw-data-layer, single-process
# fallback) + the 4 Addım C split processes (adapter/canonicalizer/publisher/
# storage). The runtime image carries all of them; docker-compose.multi picks one
# per service via `command:`.
FROM golang:1.25-bookworm AS builder

# Install ZeroMQ development library (required by pebbe/zmq4 cgo binding)
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

# Build all 5 binaries with CGO (ZeroMQ) + stripped symbols. The same image serves
# both single-process (monolith) and multi-process (Addım C) deployments.
RUN for bin in raw-data-layer adapter canonicalizer publisher storage; do \
        CGO_ENABLED=1 GOOS=linux go build -ldflags="-s -w" -o /build/$bin ./cmd/$bin ; \
    done && \
    test -x /build/raw-data-layer && test -x /build/adapter && test -x /build/canonicalizer && \
    test -x /build/publisher && test -x /build/storage

# ─── Stage 2: Runtime ────────────────────────────────────────────────────────
FROM debian:bookworm-slim

# Install ZeroMQ runtime library + CA certificates (for WebSocket TLS) + wget (healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libzmq5 \
    ca-certificates \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -r -u 1001 -g root quant

WORKDIR /app

# Copy all 5 binaries + assets
COPY --from=builder /build/raw-data-layer /build/adapter /build/canonicalizer /build/publisher /build/storage ./
COPY --chown=quant:root mappings/ ./mappings/
COPY --chown=quant:root config/ ./config/

# Create data directories (WAL + logs + the shared UDS socket dir for multi-process)
RUN mkdir -p /var/log/raw_data/wal /app/logs /run/rdl && \
    chown -R quant:root /var/log/raw_data /app/logs /run/rdl /app/config

USER quant

# Ports: 5555=ZeroMQ PUB, 8080=monolith health, 8081-8084=multi-process health/metrics
EXPOSE 5555 8080 8081 8082 8083 8084

# Default = monolith single-process (override `command:` for the split processes).
# Healthcheck probes the monolith's :8080/live; multi-process services set their
# own healthcheck in docker-compose.multi.yml.
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:8080/live || exit 1

ENTRYPOINT ["/app/raw-data-layer"]
CMD ["-config", "/app/config/config.yaml"]
