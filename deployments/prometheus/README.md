# Prometheus — scrape config + ServiceMonitor

## Files

| File | Purpose |
|------|---------|
| `prometheus.yml` | Scrape config: 4 jobs (one per process) hitting `/metrics` on 8081-8084 + prometheus self |
| `servicemonitor.yaml` | Prometheus Operator `ServiceMonitor` CRD (label-based discovery) |

## Metrics (source: `pkg/health/metrics.go`)

| Metric | Type | Labels |
|--------|------|--------|
| `raw_data_messages_received_total` | CounterVec | `source` |
| `raw_data_messages_processed_total` | CounterVec | `source` |
| `raw_data_adapter_latency_microseconds` | HistogramVec | `source` (buckets 100/500/1k/5k/10k µs) |
| `raw_data_queue_depth` | Gauge | — |
| `raw_data_backpressure_total` | Counter | — |
| `raw_data_wal_writes_total` | Counter | — |
| `raw_data_dolphindb_writes_total` | Counter | — |
| `raw_data_dolphindb_write_errors_total` | Counter | — |

## Targets

| Process | Port | Job |
|---------|------|-----|
| adapter | 8081 | rdl-adapter |
| canonicalizer | 8082 | rdl-canonicalizer |
| publisher | 8083 | rdl-publisher |
| storage | 8084 | rdl-storage |

K8s targets use the Service DNS `raw-data-layer:808X`. For docker-compose, swap
the host for the compose service name (`adapter`, `canonicalizer`, ...).

## Validation

Canonical: `promtool check config deployments/prometheus/prometheus.yml`
(promtool ships with prometheus; not always installed locally — the YAML is also
validated by `go run ./scripts/validate_yaml deployments/prometheus/prometheus.yml`).
