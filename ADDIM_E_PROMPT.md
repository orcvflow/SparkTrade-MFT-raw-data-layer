# Addım E — Production Deployment (Implementation Prompt)

> **Context (stable prefix — cache-friendly):** You are implementing Addım E of the `raw-data-layer` Go market-data ingestion system. Addım A–D are DONE, merged to `main` at `7988f94`, tagged `v0.4.0`, and pushed to `github.com/orcvflow/SparkTrade-MFT-raw-data-layer`. The code is multi-process (4 cmd binaries: adapter/canonicalizer/publisher/storage over UDS+Protobuf), SIMD/zero-copy optimized (Sonic+mmap+pool+lock-free), and the full test suite is green incl. `-race`. This phase is **production deployment + system-level measurement**, not new ingestion logic.
>
> **Roadmap source:** `STEP-D.md` §8 (7 tasks E1–E7, 7-day plan, ready-infra inventory, attention points). Read it before starting.

## Output Contract (top — the evaluable acceptance gate)

Each task below produces **concrete files + a deterministic acceptance check**. A task is "done" iff its check passes. Do not claim completion without running the check and pasting its exit code. The acceptance checks ARE the eval set — see "Eval Set" at the bottom.

**Paranoid principles carry forward from CLAUDE.md (non-negotiable):**
- Never panic — every new goroutine has `defer recover()`; return error/default.
- Never lose data — raw_payload byte-for-byte; WAL-first on any storage write.
- Never hang — bounded queues, explicit backpressure, timeouts on all I/O.
- Always observable — health/metrics endpoints; structured logs.
- Always recoverable — exponential backoff (1,2,4,8,16,30s).
- **Report honestly** — do not assert performance numbers you did not measure; flag spec targets as "targets" until measured.

**When uncertain:** make the value configurable (`config/config.yaml` + env override) with a documented default. Never hardcode a production tuning constant. When uncertain whether a deploy target is met, measure it (E1) rather than assert it.

---

## Task E1 — Production Benchmark CLI

**Objective:** Measure the spec's system-level targets (throughput, latency p50/p95/p99, GC pause, memory) on a 4-process pipeline under load. This is where the unverified "200K msg/s, <500µs p99, <100ms GC, <2GB" claims from the spec become MEASURED numbers (currently only component-level numbers are verified — see `STEP-D.md` §3).

**Files:** `cmd/adapter/main.go` (add `--benchmark` flag), `pkg/benchmark/` (new — harness + reporter).

**Implementation constraints (edge cases enumerated):**
- The `--benchmark` flag MUST NOT alter the normal adapter run path. Gate ALL benchmark behavior behind the flag; default invocation is unchanged. (Risk: breaking the multi-process `cmd/adapter` binary — Addım D deliberately deferred this for that reason.)
- Use the **mock adapter** path (`pkg/adapter/mock.go`) to generate deterministic message load — do NOT depend on Binance testnet (flaky, rate-limited, not CI-reproducible).
- Drive the full 4-process pipeline (adapter→canonicalizer→publisher→storage) over UDS, OR an in-process equivalent for isolation. Report which.
- Capture: throughput (msg/s), latency histogram (p50/p95/p99), GC pause (`runtime.MemStats`), RSS/heap (mem), goroutine count. Run for a configurable `--messages` count (default 1,000,000) and `--warmup` (default 10,000).
- WAL `fsync` per-message is a known bottleneck (CLAUDE.md / PROGRESS.md Step C notes). Benchmark BOTH with sync WAL and with a batched/async WAL variant you add here — report both so the delta is visible. Do not silently switch the production default.

**Acceptance check (evaluable):**
```
./bin/adapter --benchmark --messages=100000 --warmup=1000 > bench_report.json 2>&1
test $? -eq 0 && jq -e '.throughput_msgs_per_sec > 0 and (.latency_p99_ns | tonumber) > 0' bench_report.json
# exit 0 = PASS
```
The report must be valid JSON with non-zero throughput AND a numeric p99. (No absolute floor asserted — see honest-limitations in `STEP-D.md` §6; the gate is "the run completes and emits a well-formed report", not "hits 200K".)

---

## Task E2 — Grafana Dashboard

**Files:** `deployments/grafana/raw-data-layer.json` (dashboard JSON), `deployments/grafana/README.md`.

**Panels (one panel per existing metric — `pkg/health/metrics.go` is the source of truth):** Throughput (msg/s), Latency p50/p95/p99, GC pause, Heap alloc, Goroutine count, Queue depth, Backpressure counter, WAL writes, DolphinDB write errors.

**Constraints:**
- Panels reference the EXISTING Prometheus metric names (`raw_data_messages_received_total`, `raw_data_adapter_latency_microseconds`, `raw_data_queue_depth`, `raw_data_backpressure_total`, etc. — verify against `pkg/health/metrics.go` before writing).
- Dashboard JSON must import cleanly into Grafana (validate with `jq .`).
- No Grafana infra is running yet — that is E3/E5's job; here we only produce a valid, importable dashboard artifact.

**Acceptance check:**
```
jq -e '.title and (.panels | length) >= 9' deployments/grafana/raw-data-layer.json
# exit 0 = PASS (titled, ≥9 panels)
```

---

## Task E3 — Kubernetes Manifests

**Files:** `deployments/k8s/{adapter,canonicalizer,publisher,storage}.yaml` (each: Deployment + Service), `deployments/k8s/configmap.yaml`, `deployments/k8s/secrets.yaml` (template, no real creds), `deployments/k8s/README.md`.

**Constraints:**
- One Deployment per process (4 total), mirroring `docker/docker-compose.multi.yml` topology + `config/config.multi.yaml` IPC paths.
- Liveness probe → `/live`, readiness probe → `/ready` (both exist on ports 8081–8084 per `pkg/health/server.go`).
- WAL volume as a PVC (data must survive pod restart — never lose data).
- Secrets for DolphinDB creds + Binance endpoint — NEVER inline real creds (CLAUDE.md known limitation: hardcoded creds in config.yaml; this task fixes it for k8s).

**Acceptance check:**
```
for f in deployments/k8s/*.yaml; do kubectl apply --dry-run=client -f "$f" >/dev/null 2>&1 || echo "FAIL: $f"; done
# (or, if no kubectl: yq eval '.' "$f" >/dev/null for YAML validity)
# exit 0 = all 6 manifests parse
```

---

## Task E4 — Helm Chart

**Files:** `deployments/helm/raw-data-layer/{Chart.yaml,values.yaml,templates/*.yaml}`.

**Constraints:**
- Templates render from `values.yaml` (image tag, replicas, ports, IPC paths, resource limits).
- `helm lint` passes.
- Default `values.yaml` reflects the 4-process topology from E3.

**Acceptance check:**
```
helm lint deployments/helm/raw-data-layer
# exit 0 = PASS
```

---

## Task E5 — Prometheus + ServiceMonitor

**Files:** `deployments/prometheus/servicemonitor.yaml`, `deployments/prometheus/prometheus.yml` (scrape config referencing 8081–8084 `/metrics`).

**Constraints:**
- Scrape config targets the 4 process `/metrics` endpoints (existing `pkg/health/metrics.go` + `promhttp.Handler()`).
- ServiceMonitor uses the standard monitoring.coreos.com CRD (label-based discovery).

**Acceptance check:**
```
promtool check config deployments/prometheus/prometheus.yml
# exit 0 = config valid
```

---

## Task E6 — ELK Stack (Log aggregation)

**Files:** `deployments/elk/filebeat.yaml` (ship structured JSON logs), `deployments/elk/elasticsearch.yaml`, `deployments/elk/kibana.yaml`, `deployments/elk/README.md`.

**Constraints:**
- Filebeat ships the EXISTING structured JSON logs produced by `pkg/pipeline/runtime.go` (`NewLogger` JSON format). Verify the log format before configuring Filebeat's json decoder.
- Index pattern auto-created in Kibana.

**Acceptance check:**
```
for f in deployments/elk/*.yaml; do yq eval '.' "$f" >/dev/null || echo "FAIL: $f"; done
# exit 0 = all parse
```

---

## Task E7 — CI/CD (GitHub Actions)

**Files:** `.github/workflows/ci.yml`, `.github/workflows/release.yml`.

**Constraints:**
- CI workflow: `go build ./...` → `go test ./... -race -timeout 300s` → `go test -tags=regression ./test/regression/...` (note: regression runs WITHOUT `-race` — see `test/regression/regression_test.go` header) → `golangci-lint run`.
- CI MUST NOT fail on the absence of live Binance/DolphinDB (those are network tests, gated behind a separate optional job).
- Release workflow: on tag `v*`, build 4 binaries (`cmd/*`) + docker image, attach to GitHub Release.

**Acceptance check:**
```
# Lint the workflow YAML + validate job structure
yq eval '.' .github/workflows/ci.yml >/dev/null && yq eval '.' .github/workflows/release.yml >/dev/null
# + act --list (if act installed) to confirm jobs parse
# exit 0 = workflows valid YAML
```

---

## Eval Set (skill Rule #2 — build before optimizing THIS prompt)

Each task's **Acceptance check** above is one eval case. The eval set for THIS prompt = {E1 benchmark report emits valid JSON, E2 dashboard has title+≥9 panels, E3 6 k8s manifests parse, E4 helm lint, E5 promtool check, E6 elk YAML parse, E7 workflow YAML parse}. 7 cases, each binary pass/fail.

**Before optimizing this prompt further** (clarity/structure tweaks, few-shot additions), we run the 7 acceptance checks against the agent's output. A prompt revision ships only if it passes all 7 (no-regression) AND scores ≥ baseline on `prompt_optimizer --analyze`. Baseline for THIS prompt is captured at `/tmp/addim_e_prompt_baseline.json` (run after this draft is written).

**Known prompt risks to revisit after the eval set is real:**
- E1's "measure BOTH WAL variants" could be misread as "change the default" — tighten if the eval shows an agent flipping the production WAL default.
- "Never hardcode a production tuning constant" is a vague verb (`hardcode`) — if eval shows ambiguity, enumerate the specific constants (ports, batch sizes, backoff intervals) that must be configurable.

---

## Order of execution (dependency-respecting)

E1 → (E3 → E4) → E5 → E2 → E6 → E7.
(E1 first because it produces the measured numbers the spec targets depend on; E3/E4 deploy topology; E5 scrapes E3's services; E2 visualizes E5's metrics; E6 independent; E7 wraps everything.)
