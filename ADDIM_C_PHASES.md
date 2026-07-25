# Addım C — Multi-Process Split (Homalos Pattern) — Completion Log

**Scope:** Split the monolith into 4 isolated processes (Adapter / Canonicalizer /
Publisher / Storage) over UDS + Protobuf, with process isolation, auto-restart,
health on ports 8081-8084, /metrics, integration + chaos tests, systemd,
docker-compose.multi, prometheus.

**Delivery:** Phased (5→6→7→8), completed end-to-end with real verification at
each phase (no unverified claims). All changes uncommitted unless the user asks.

---

## Phase 1-2 (prior session) — pkg/ipc + pkg/process + tests
DONE + green. UDS+Protobuf transport, lossless on-disk spool (single FIFO store),
client.Flush for lossless graceful shutdown; process supervisor with auto-restart.
Coverage: ipc 85.4%, process 85.2%.

## Phase 3-4 (prior session) — config loader + 4 cmd binaries + health
DONE + green. `pkg/config` makes config.yaml real (yaml.v3 + env + defaults,
98.9% cov); reusable `pkg/health/server.go` (89.0%); 4 cmd binaries on 8081-8084;
monolith kept as fallback. `go test ./pkg/... -race` → 13 packages green.

---

## Phase 5 (2026-07-25) — Integration tests — ✅ GREEN (5/5)

**Files created:**
- `test/integration/multiprocess_test.go` (`//go:build integration`)

**What it tests (real, not mocked-away):**
Builds the REAL 4 cmd binaries as child processes over isolated UDS+Protobuf IPC
(temp sockets, free health ports, temp WAL dir, per-test config.yaml generated
from `config.DefaultConfig()` + yaml.Marshal so keys always match struct tags).
The WAL on disk is the durable oracle — events are read back and raw_payload is
compared byte-for-byte.

**Tests + real results (`go test -tags=integration ./test/integration/ -run TestMultiprocess -count=1`):**
| Test | Result | Time | What it proves |
|---|---|---|---|
| `TestMultiprocess_ProcessesBoot` | PASS | 6.29s | all 4 binaries boot, bind sockets, /live 200 |
| `TestMultiprocess_FullPipeline_TestInjected` | PASS | 1.12s | raw frame → canon → pub → storage → WAL; raw_payload byte-for-byte intact; price=50000 size=0.5 |
| `TestMultiprocess_PublisherToStorage_Edge` | PASS | 0.14s | canonical frame injected at storage socket lands in WAL; price=12345.67 |
| `TestMultiprocess_LosslessMultipleEvents` | PASS | 1.63s | 20 distinct frames → 20 in WAL (no loss across 3 procs + 2 IPC hops) |
| `TestMultiprocess_RealAdapter_FourProcesses` | PASS | 13.08s | REAL adapter binary + local WS mock → full 4-process chain → WAL; raw_payload intact |

Total: 22.3s, exit 0.

**Honest finding surfaced (pre-existing, NOT introduced by Addım C):**
`canon.Process` calls `ToCanonical("BINANCE", "BTCUSDT")` (uppercase `Source`) but
`SymbolMapper` keys its tables on the lowercase **filename** (`binance.json` →
`"binance"`). Case mismatch → every Binance symbol resolves to `"UNKNOWN"` (raw
payload is still preserved; the pipeline still flows). This is an Addım A/B latent
bug in `pkg/canonicalizer`/`pkg/mapper`. The integration tests deliberately do NOT
assert `CanonicalSymbol=="BTC/USD"` to stay robust to it; they assert raw_payload,
price, size, source, and event count — the lossless-across-IPC guarantees that are
Addım C's actual concern. Fixing the case bug is out of Addım C's scope (it would
touch Addım A/B code); flagged for the user to decide.

**Limitations:**
- The real-adapter test uses a local WS mock (not the Binance testnet) so it needs
  no network and is deterministic. It does NOT exercise the 24h session rotation
  or real Binance auth (those are adapter-level, covered by pkg/adapter tests).
- DolphinDB is disabled in the test config (WAL-only); the storage process's
  DolphinDB reconnect loop still runs and fails fast to 127.0.0.1:1 (harmless,
  non-fatal, WAL unaffected).

---

## Phase 6 (2026-07-25) — Chaos tests — ✅ GREEN (5/5, under -race)

**Files created:**
- `test/chaos/multiprocess_test.go` (`//go:build chaos`)

**What it tests (real kill + auto-restart + lossless spool):**
Starts 3 real binaries (canonicalizer/publisher/storage) as supervised child
processes (`pkg/process` with `MaxRetries=0` = unlimited, short backoff, mirroring
`systemd Restart=always`). SIGKILLs a process mid-stream WITHOUT going through
Stop, so the supervisor treats it as a crash and auto-restarts it. The lossless
on-disk IPC spool (separate process) holds events during the outage; on restart
the drainLoop drains the spool FIFO, so events injected DURING the outage land in
the WAL. This is the real, testable Homalos invariant.

**Tests + real results (`go test -tags=chaos -race ./test/chaos/ -run TestMultiprocessChaos`):**
| Test | Result | Time | What it proves |
|---|---|---|---|
| `StorageKill_AutoRestartAndLossless` | PASS | 13.64s | SIGKILL storage → publisher spools → storage auto-restarts (restarts=1) → 5/5 outage events drained to WAL (lossless) |
| `PublisherKill_AutoRestart` | PASS | 8.14s | SIGKILL publisher → canonicalizer spools → restarts → 4/4 drained to WAL |
| `CanonicalizerKill_AutoRestart` | PASS | 8.32s | SIGKILL source → publisher+storage stay live → canon restarts → flow resumes |
| `AllKill_Recovery` | PASS | 10.60s | SIGKILL all 3 → all auto-restart → full chain resumes → WAL |
| `RaceConcurrentInject` | PASS | 2.52s | 8 goroutines × 5 frames = 40 concurrent → 40/40 lossless in WAL; -race clean |

Total: 44.2s, exit 0, no test-harness data races.

**Honest limitation:** the child binaries are production builds (NOT race-
instrumented); `-race` instruments the test harness + test-side IPC clients only.
To race-check the pipeline internals, rebuild binaries with `go build -race`
(documented option, not done here — keeps the chaos suite fast + reliable).

---

## Phase 7 (2026-07-25) — Deployment + monitoring — ✅ DONE

**Files created/edited:**
- `deployments/systemd/raw-data-adapter.service` (+ canonicalizer/publisher/storage) — `Restart=always`, `RestartSec=5`, ordered `Requires=`/`After=` per the pipeline, `LimitNOFILE=65536`. Mirrors the supervisor contract proven in Phase 6.
- `docker/Dockerfile` — **edited**: now builds ALL 5 binaries (monolith + adapter/canonicalizer/publisher/storage) in the builder stage; runtime copies all 5 + exposes 5555/8080/8081-8084; default ENTRYPOINT = monolith, `command:` override picks the split binary.
- `config/config.multi.yaml` — docker multi-process config: IPC sockets under `/run/rdl` (shared volume), adapters/DolphinDB disabled by default, **no secrets embedded** (password empty).
- `docker/docker-compose.multi.yml` — 4 services + prometheus, shared `rdl-sock` volume at `/run/rdl` (UDS across containers), per-service healthcheck on `/live`, `depends_on: condition: service_healthy` for startup order, `restart: unless-stopped`.
- `config/prometheus.yml` — scrapes 8081-8084 `/metrics`.

**Verified:** `go build ./...` + `go vet ./...` clean (the binaries the Dockerfile builds compile).

**Honest limitation:** the Docker image + `docker compose up` were **NOT actually
built/run here** — this background session has no docker daemon. The Dockerfile,
compose, and prometheus.yml are syntactically verified + the binaries they build
compile via `go build ./cmd/...`; to fully verify the containerized multi-process
topology, run `docker compose -f docker/docker-compose.multi.yml up --build`
(needs docker). UDS across containers requires the shared `/run/rdl` volume
(documented in the compose); a TCP IPC option for network-isolated services is a
future extension (pkg/ipc is UDS-only today).

---

## Phase 8 (2026-07-25) — Final verification — ✅ GREEN

| Check | Command | Result |
|---|---|---|
| Full unit + existing tests, race | `go test ./... -race -count=1` | **EXIT 0** (13 pkg + test/unit 203s + test/integration + test/chaos non-tagged) |
| Integration (multi-process) | `go test -tags=integration ./test/integration/ -run TestMultiprocess` | **5/5 PASS** (22.3s) |
| Chaos (multi-process, race) | `go test -tags=chaos -race ./test/chaos/ -run TestMultiprocessChaos` | **5/5 PASS** (44.2s) |
| Vet | `go vet ./...` | clean |
| Build | `go build ./...` | clean (5 binaries: monolith + 4 split) |

**Data-race fix applied (pre-existing Addım A/B, fixed to satisfy Phase 8's no-race criterion):**
`pkg/storage/wal.go` `Stats()` read `currentFilePath` (a plain string field) with
NO lock, while `rotateLocked()` writes it under `w.mu` (called from the
`rotationChecker` goroutine spawned by `Start()` and from `Write`'s
`shouldRotate`). Under concurrent rotation + `Stats`, the race detector flagged it
(flaky — only fired under full-suite load, not in isolation). Fix: read
`currentFilePath` under `w.mu` in `Stats()`. Verified: `pkg/storage -race -count=8`
clean (42.7s) + full suite `-race` green. This race pre-existed Addım C (the
`M pkg/storage/wal.go` was modified in Addım A/B); Addım C's Phase 8 simply
requires a green `-race` suite, so it was fixed here.

---

## Summary — Addım C COMPLETE

All 8 phases done with real verification at each step (no unverified claims).
The monolith is split into 4 isolated processes over UDS+Protobuf with lossless
on-disk spooling; `pkg/process` + systemd auto-restart one crash without taking
down peers; the lossless-via-spool property is genuinely tested (events injected
during a real process outage survive + drain on restart). 10 new tests
(5 integration + 5 chaos) all green; full `-race` suite green; 5 binaries build
clean; deployment artifacts (systemd/docker/prometheus) provided.

**Outstanding — CLOSED (2026-07-25):**
1. ✅ FIXED — `canon.Process` → `ToCanonical("BINANCE", …)` case-mismatch. The
   mapper now collapses source casing via `normalizeSource` (lowercases the
   source key at load + every lookup), so "BINANCE"/"Binance"/"binance" all
   resolve to the same `"binance"` table. Binance symbols now map to "BTC/USD"
   instead of "UNKNOWN", verified end-to-end through the 4-process IPC pipeline
   (`TestMultiprocess_FullPipeline_TestInjected` asserts `CanonicalSymbol ==
   "BTC/USD"`, log: `symbol="BTC/USD"`). Regression tests added:
   `TestSymbolMapper_SourceCaseInsensitive` + `TestCanonicalizer_ParseBinance_SymbolMapped`.
   Full `-race` suite green (17 pkgs + integration + chaos + unit, exit 0).
2. ✅ VALIDATED (to the extent the environment allows) — Docker compose:
   `docker-compose -f docker/docker-compose.multi.yml config` exits 0 (all 5
   services parse, healthchecks/volumes/depends_on resolve, variable
   interpolation defaults work); all 5 cmd binaries compile with
   `CGO_ENABLED=1` (the Dockerfile `RUN` step succeeds). Live
   `docker compose up --build` NOT run — no Docker daemon on this host
   (`/var/run/docker.sock` absent; needs `sudo systemctl start docker`).
   Run: `docker-compose -f docker/docker-compose.multi.yml up --build`.
3. ✅ COMMITTED — all Addım A+B+C changes committed on branch
   `addim-c-multiprocess` (mapper fix + regression tests included).
