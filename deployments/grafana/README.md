# Grafana Dashboard — Raw Data Layer Pipeline

`raw-data-layer.json` — 13-panel dashboard for the 4-process pipeline, driven by
the Prometheus metrics in `pkg/health/metrics.go` (8 custom metrics) plus the Go
runtime/process collectors (`go_*`, `process_*`) registered in `Register()`.

## Panels

| # | Panel | PromQL |
|---|-------|--------|
| 1 | Throughput (processed msg/s) | `sum by (source) (rate(raw_data_messages_processed_total[1m]))` |
| 2 | Messages received (msg/s) | `sum by (source) (rate(raw_data_messages_received_total[1m]))` |
| 3 | Adapter latency p50 (µs) | `histogram_quantile(0.50, sum by (le,source)(rate(raw_data_adapter_latency_microseconds_bucket[5m])))` |
| 4 | Adapter latency p95 (µs) | `histogram_quantile(0.95, ...)` |
| 5 | Adapter latency p99 (µs) | `histogram_quantile(0.99, ...)` |
| 6 | Queue depth | `raw_data_queue_depth` |
| 7 | Backpressure events (1/s) | `rate(raw_data_backpressure_total[5m])` |
| 8 | WAL writes (1/s) | `rate(raw_data_wal_writes_total[5m])` |
| 9 | DolphinDB writes (1/s) | `rate(raw_data_dolphindb_writes_total[5m])` |
| 10 | DolphinDB write errors (1/s) | `rate(raw_data_dolphindb_write_errors_total[5m])` |
| 11 | GC pause max (s) | `go_gc_duration_seconds{quantile="1.0"}` |
| 12 | Heap alloc (MB) | `go_memstats_alloc_bytes / 1024 / 1024` |
| 13 | Goroutines | `go_goroutines` |

## Datasource + variables

- `DS_PROM` — datasource picker (defaults to a Prometheus named "Prometheus").
- `source` — multi-select over `label_values(raw_data_messages_processed_total, source)`.

## Import

1. Grafana → Dashboards → Import → upload `raw-data-layer.json`.
2. Pick the Prometheus datasource when prompted.
3. Ensure Prometheus scrapes the 4 processes (see `deployments/prometheus/`).

## Validation

```bash
jq -e '.title and (.panels | length) >= 9' deployments/grafana/raw-data-layer.json
# exit 0 = titled + ≥9 panels (13 panels here)
jq . deployments/grafana/raw-data-layer.json >/dev/null   # valid JSON / importable
```
