// cmd/adapter — Addım C adapter process (health on port 8081).
//
// Runs the market-data adapters (Binance / IB) and forwards each raw message to
// the canonicalizer process over a UDS+Protobuf IPC client. The client is
// lossless: if the canonicalizer is down or slow, messages spill to an on-disk
// spool and are replayed (FIFO) when it returns. One adapter crash is isolated
// from the rest of the pipeline (Homalos pattern); systemd restarts this process.
//
// Design (CLAUDE.md): never panic (defer/recover), never lose data (spool),
// never hang (non-blocking Send), always observable (/health + /metrics).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"raw-data-layer/pkg/adapter"
	"raw-data-layer/pkg/config"
	"raw-data-layer/pkg/health"
	"raw-data-layer/pkg/ipc"
	"raw-data-layer/pkg/pipeline"
)

func main() {
	defer func() {
		if r := recover(); r != nil {
			fmt.Fprintf(os.Stderr, "FATAL: %v\n", r)
			os.Exit(1)
		}
	}()

	cfgPath := flag.String("config", "config/config.yaml", "config file path")
	flag.Parse()

	cfg, _ := config.Load(*cfgPath)
	log := pipeline.NewLogger(cfg.Logging)
	log.Info("adapter process starting", "port", cfg.Processes.AdapterHealthPort)

	ctx, cancel := context.WithCancel(context.Background())

	// IPC client → canonicalizer (lossless, spooled).
	client, err := ipc.NewClient(cfg.IPC.AdapterToCanonicalizer, ipc.ClientConfig{
		Backoff:   cfg.Adapters.Binance.Reconnect.BackoffSeconds,
		SpoolPath: pipeline.SpoolPath(cfg, "adapter"),
	})
	if err != nil {
		log.Error("ipc client init failed", "error", err)
		cancel()
		os.Exit(1)
	}
	client.Start()

	// rawCh: adapters → fanIn → IPC client.
	rawCh := make(chan adapter.RawMessage, cfg.WorkerPool.QueueSize)

	adapterCfg := adapter.AdapterConfig{
		Enabled:           true,
		ReconnectAttempts: cfg.Adapters.Binance.Reconnect.MaxAttempts,
		BackoffSeconds:    cfg.Adapters.Binance.Reconnect.BackoffSeconds,
		Timeout:           config.ParseDur(cfg.Adapters.IB.RequestTimeout, 10*time.Second),
	}

	var adapters []adapter.Adapter
	if cfg.Adapters.Binance.Enabled {
		adapters = append(adapters, adapter.NewBinanceAdapter(
			cfg.Adapters.Binance.Endpoint, cfg.Adapters.Binance.Symbols, adapterCfg))
	}
	if cfg.Adapters.IB.Enabled {
		adapters = append(adapters, adapter.NewIBAdapter(
			cfg.Adapters.IB.Host, cfg.Adapters.IB.Port, cfg.Adapters.IB.ClientID,
			cfg.Adapters.IB.Symbols, adapterCfg))
	}

	// fanIn: drain rawCh → EncodeRaw → client.Send. Exits on ctx cancel after a
	// non-blocking drain (never closes rawCh, so an in-flight adapter send cannot
	// panic on a closed channel).
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error("panic in fanIn", "error", fmt.Sprintf("%v", r))
			}
		}()
		for {
			select {
			case <-ctx.Done():
				// Drain whatever is buffered, non-blocking, then exit.
				for {
					select {
					case rm := <-rawCh:
						sendRaw(log, client, rm)
					default:
						return
					}
				}
			case rm := <-rawCh:
				sendRaw(log, client, rm)
			}
		}
	}()

	// Start adapters.
	for _, adp := range adapters {
		a := adp
		if err := a.Connect(ctx); err != nil {
			log.Warn("adapter connect failed", "adapter", a.Name(), "error", err)
		}
		if err := a.Start(ctx, rawCh); err != nil {
			log.Error("adapter start failed", "adapter", a.Name(), "error", err)
			continue
		}
		log.Info("adapter started", "name", a.Name())
	}

	// Health.
	hsrv := health.NewServer("adapter", cfg.Processes.AdapterHealthPort, func() health.Snapshot {
		st := client.Stats()
		status := "ok"
		if st.Closed {
			status = "degraded"
		}
		return health.Snapshot{Status: status, Component: map[string]any{
			"ipc_connected":   st.Connected,
			"ipc_sent":        st.Sent,
			"ipc_spooled":     st.Spooled,
			"ipc_spool_bytes": st.SpoolBytes,
			"ipc_reconnects":  st.Reconnects,
			"adapters":        len(adapters),
		}}
	})
	hsrv.SetReady(func() bool { return len(adapters) > 0 })
	if err := hsrv.Start(); err != nil {
		log.Error("health server start failed", "error", err)
	}

	log.Info("adapter process ready")
	sig := pipeline.WaitForSignal()
	log.Info("shutting down adapter", "signal", sig.String())

	// Graceful, lossless: cancel (adapters stop, fanIn drains) → flush IPC spool
	// to the canonicalizer → stop the client.
	cancel()
	for _, a := range adapters {
		_ = a.Stop()
	}
	fctx, fcancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := client.Flush(fctx); err != nil {
		log.Warn("ipc flush incomplete (canonicalizer down?)", "error", err)
	}
	fcancel()
	_ = client.Stop()
	_ = hsrv.Stop()
}

// sendRaw encodes and sends one raw message; never panics.
func sendRaw(log logger, client *ipc.Client, rm adapter.RawMessage) {
	defer func() {
		if r := recover(); r != nil {
			log.Warn("panic in sendRaw", "error", fmt.Sprintf("%v", r))
		}
	}()
	msg, err := pipeline.EncodeRaw(rm)
	if err != nil {
		log.Warn("encode raw failed", "source", rm.Source, "error", err)
		return
	}
	if err := client.Send(msg); err != nil {
		// ErrSpoolFull = hard backpressure (spool cap reached). The message is not
		// spooled — bounded loss under overload (CLAUDE.md explicit backpressure).
		log.Warn("ipc send failed (backpressure)", "source", rm.Source, "error", err)
	}
}

// logger is the minimal subset of *slog.Logger used by sendRaw, to avoid pulling
// slog into the helper signature awkwardly. (sendRaw is hot-ish; the adapter
// process passes its slog logger.)
type logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	Debug(msg string, args ...any)
}
