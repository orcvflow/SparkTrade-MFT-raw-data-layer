# ELK Stack — log aggregation

Ships the pipeline's structured JSON logs (produced by `pkg/pipeline/runtime.go` →
`slog.NewJSONHandler`) from container stdout to Elasticsearch, viewed in Kibana.

## Files

| File | Kind | Purpose |
|------|------|---------|
| `filebeat.yaml` | ConfigMap + DaemonSet + ServiceAccount | Tails `/var/log/containers/*.log`, decodes the containerd envelope + the inner slog JSON, ships to ES |
| `elasticsearch.yaml` | Deployment + Service | Single-node Elasticsearch (security disabled — dev only) |
| `kibana.yaml` | Deployment + Service | Kibana pointed at Elasticsearch |

## Log format (source: `pkg/pipeline/runtime.go`)

`NewLogger` uses `slog.NewJSONHandler` (Go stdlib). One log line:

```json
{"time":"2026-07-25T14:30:00.123456789Z","level":"INFO","msg":"WAL started","component":"wal"}
```

Fields: `time` (RFC3339Nano), `level` (DEBUG/INFO/WARN/ERROR), `msg`, plus any
`slog.Attr` keys added at the call site (e.g. `component`, `source`, `error`).
There is **no** `source` file/line field (`AddSource` is false). Output is stdout
by default (`config.multi.yaml` → `logging.output: stdout`).

## Decoding pipeline

1. Container runtime captures each pod's stdout → `/var/log/containers/*.log`,
   one JSON envelope per line: `{"log":"<inner>","stream":"stdout","time":"..."}`.
2. Filebeat `container` input decodes the envelope → inner slog line in `message`.
3. `decode_json_fields` on `message` with `overwrite_keys: true` → `time`/`level`/
   `msg`/attrs become top-level event fields (so Kibana can filter by `level`,
   `msg`, `component`).
4. Ship to Elasticsearch index `raw-data-layer-*` (template auto-created).

## Index pattern (Kibana)

Filebeat's `setup.template` creates the `raw-data-layer-*` index pattern. In Kibana
→ Stack Management → Index Patterns, add `raw-data-layer-*` (or use the auto-created
one) to explore logs in Discover.

## Apply

```bash
kubectl apply -f deployments/elk/elasticsearch.yaml
kubectl apply -f deployments/elk/kibana.yaml
kubectl apply -f deployments/elk/filebeat.yaml
kubectl -n raw-data-layer port-forward svc/kibana 5601:5601
# → http://localhost:5601
```

## Production notes (honest gaps)

- **Security disabled** (xpack.security.enabled=false) — dev validation only.
  Production: enable xpack security + TLS + auth on ES + Kibana.
- **No PVC** on Elasticsearch — indices are lost on pod restart. Add a PVC
  (mirror `raw-data-wal`) for durable storage.
- **include_lines filter** narrows to `raw-data-layer` namespace logs by matching
  the container-log filename. For a stricter guarantee, replace with a
  `kubernetes` autodiscover provider + a label condition (adds a ClusterRole).
