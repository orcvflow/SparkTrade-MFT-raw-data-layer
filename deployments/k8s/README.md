# Raw Data Layer — Kubernetes Manifests

Sidecar deployment of the 4-process market-data pipeline (adapter → canonicalizer
→ publisher → storage) over UDS+Protobuf, mirroring `docker/docker-compose.multi.yml`
and `config/config.multi.yaml`.

## Files

| File | Kind | Purpose |
|------|------|---------|
| `deployment.yaml` | Deployment | One pod, 4 sidecar containers, shared `emptyDir` `/run/rdl` for UDS sockets + shared PVC `/var/log/raw_data/wal` for durable WAL |
| `service.yaml` | Service | Exposes 5555 (ZeroMQ PUB) + 8081-8084 (per-process `/metrics` + `/live` + `/ready`) |
| `configmap.yaml` | ConfigMap | `config.multi.yaml` mounted at `/app/config/` |
| `pvc.yaml` | PVC | Durable WAL volume (RWO, 10 Gi) |
| `secret.yaml` | Secret | **Template** for DolphinDB + Binance + IB creds — never real creds |

## Design decision: sidecar over 4 separate Deployments

The pipeline's inter-process IPC is **Unix Domain Sockets** at
`/run/rdl/raw-{adapter-canonicalizer,canonicalizer-publisher,publisher-storage}.sock`
(see `config.multi.yaml` → `ipc`). UDS requires a **shared filesystem**, which K8s
pods do not have across pod boundaries (`emptyDir` is per-pod).

Therefore the 4 processes **must co-locate in one pod** sharing an `emptyDir` at
`/run/rdl`. This is the correct K8s translation of the docker-compose
`rdl-sock` named volume, and it **preserves the low-latency UDS path** that
Addım D's SIMD/zero-copy work depends on.

**Alternative (not implemented here):** 4 separate Deployments, one process each,
communicating over **TCP** via K8s Services. This would enable independent
horizontal scaling but requires:
1. A TCP IPC path in `pkg/ipc` (currently UDS-only) — a code change.
2. Accepting ~10-100× higher inter-process latency (loopback TCP vs UDS).

That trade-off negates Addım D's latency goals, so it is deferred. If future
throughput demands per-process scaling, implement TCP IPC in `pkg/ipc` first,
then split into 4 Deployments + a `raw-data-wal` RWX PVC.

## Probes

Each container exposes `/live` and `/ready` on its health port (per
`pkg/health/server.go`, port passed to `NewServer(name, port, snapshot)`):

| Container | Health port | Liveness | Readiness |
|-----------|-----------|----------|-----------|
| adapter | 8081 | `/live` | `/ready` |
| canonicalizer | 8082 | `/live` | `/ready` |
| publisher | 8083 | `/live` | `/ready` |
| storage | 8084 | `/live` | `/ready` |

Readiness predicate is set via `SetReady()` in each process binary. `/metrics`
(Prometheus, `promhttp.Handler()`) is served on the same port.

## Startup ordering

K8s does **not** guarantee container start order within a pod. This is safe
because each process connects to the upstream UDS socket with **exponential
backoff reconnect** (1, 2, 4, 8, 16, 30 s — CLAUDE.md "Always recoverable").
The pipeline self-heals regardless of start order; e.g. if `adapter` starts
before `canonicalizer` binds `raw-adapter-canonicalizer.sock`, the adapter
retries until it succeeds.

## Volumes

| Volume | Type | Mount | Why |
|--------|------|-------|-----|
| `rdl-sock` | `emptyDir` | `/run/rdl` (all 4) | UDS sockets — ephemeral, pod-local |
| `rdl-wal` | PVC (RWO) | `/var/log/raw_data/wal` (storage+canonicalizer+adapter) | Durable WAL — survives pod restart |
| `config` | ConfigMap | `/app/config` (all 4, RO) | `config.multi.yaml` |
| `mappings` | ConfigMap (optional) | `/app/mappings` (all 4, RO) | Symbol mappings; absent → mapper returns UNKNOWN (paranoid fallback) |

## Apply

```bash
# 1. Create namespace
kubectl create namespace raw-data-layer

# 2. Create the mappings ConfigMap from the repo's mappings/*.json
kubectl create configmap raw-data-layer-mappings \
  --namespace=raw-data-layer \
  --from-file=mappings/

# 3. Create the real secret (replace placeholders) — OR apply secret.yaml as-is for dry-run
kubectl apply -f deployments/k8s/secret.yaml     # template
# kubectl create secret generic raw-data-layer-secret --namespace=raw-data-layer \
#   --from-literal=dolphindb-host=... --from-literal=dolphindb-password=...

# 4. Apply the rest
kubectl apply -f deployments/k8s/pvc.yaml
kubectl apply -f deployments/k8s/configmap.yaml
kubectl apply -f deployments/k8s/deployment.yaml
kubectl apply -f deployments/k8s/service.yaml

# 5. Verify
kubectl -n raw-data-layer rollout status deployment/raw-data-layer
kubectl -n raw-data-layer get pods
kubectl -n raw-data-layer port-forward svc/raw-data-layer 8081:8081
curl localhost:8081/live   # {"alive":true}
```

## Validation (no cluster — YAML validity)

`kubectl` / `helm` / `yq` are not required to validate structure. A Go-based
YAML parse (using the project's existing `gopkg.in/yaml.v3` dependency) confirms
all manifests are well-formed:

```bash
go run ./scripts/validate_yaml deployments/k8s/*.yaml
```

For a full `kubectl apply --dry-run=server`, point at a real cluster.
