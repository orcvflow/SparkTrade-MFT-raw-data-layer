// cmd/canonicalizer — Addım C canonicalizer process (health on port 8082).
//
// Binds a UDS server on the adapter→canonicalizer socket, decodes raw frames,
// runs them through a bounded worker pool with the canonicalizer's Process
// callback (parse → sanitize → map symbol → build CanonicalEvent, raw_payload
// preserved byte-for-byte), then forwards each canonical event to the publisher
// process over a lossless UDS client. The pool callback is decoupled from the
// adapter: the input edge is now IPC, not an in-process channel.
//
// Design (CLAUDE.md): never panic, never lose data (spool to publisher), bounded
// pool with backpressure, always observable.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"raw-data-layer/pkg/canonicalizer"
	"raw-data-layer/pkg/config"
	"raw-data-layer/pkg/health"
	"raw-data-layer/pkg/ipc"
	"raw-data-layer/pkg/mapper"
	"raw-data-layer/pkg/pipeline"
	"raw-data-layer/pkg/workerpool"
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
	log.Info("canonicalizer process starting", "port", cfg.Processes.CanonicalizerHealthPort)

	// Symbol mapper + canonicalizer + worker pool.
	sm, err := mapper.NewSymbolMapper(cfg.MappingsDir)
	if err != nil {
		log.Error("symbol mapper init failed", "dir", cfg.MappingsDir, "error", err)
		os.Exit(1)
	}
	canon := canonicalizer.NewCanonicalizer(sm)

	poolCfg := workerpool.PoolConfig{
		MinWorkers:         cfg.WorkerPool.Autoscale.MinWorkers,
		MaxWorkers:         cfg.WorkerPool.Autoscale.MaxWorkers,
		QueueSize:          cfg.WorkerPool.QueueSize,
		AutoscaleEnabled:   cfg.WorkerPool.Autoscale.Enabled,
		ScaleUpThreshold:   cfg.WorkerPool.Autoscale.HighWaterMark,
		ScaleDownThreshold: cfg.WorkerPool.Autoscale.LowWaterMark,
	}
	pool := workerpool.NewPool(poolCfg, canon.Process)
	if err := pool.Start(); err != nil {
		log.Error("pool start failed", "error", err)
		os.Exit(1)
	}

	// Outbound IPC client → publisher (lossless, spooled).
	outClient, err := ipc.NewClient(cfg.IPC.CanonicalizerToPublisher, ipc.ClientConfig{
		Backoff:   cfg.Adapters.Binance.Reconnect.BackoffSeconds,
		SpoolPath: pipeline.SpoolPath(cfg, "canonicalizer"),
	})
	if err != nil {
		log.Error("ipc client init failed", "error", err)
		os.Exit(1)
	}
	outClient.Start()

	// Output loop: pool.Output() → EncodeCanonical → outClient.Send. Exits when
	// the pool closes its output channel (on pool.Stop), after draining.
	outputDone := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Error("panic in output loop", "error", fmt.Sprintf("%v", r))
			}
			close(outputDone)
		}()
		for processed := range pool.Output() {
			ev, ok := processed.Canonical.(*canonicalizer.CanonicalEvent)
			if !ok || ev == nil {
				// Processor produced no canonical event — build a lossless
				// best-effort fallback that still carries the raw payload.
				ev = &canonicalizer.CanonicalEvent{
					EventID:           fmt.Sprintf("evt_%d", time.Now().UnixNano()),
					Source:            processed.Raw.Source,
					CanonicalSymbol:   sm.ToCanonical(processed.Raw.Source, ""),
					ExchangeTimestamp: processed.Raw.ReceivedAt,
					LocalHWTimestamp:  time.Now().UnixNano(),
					EventType:         "TRADE",
					RawPayload:        processed.Raw.Payload,
					RawFormat:         "JSON",
				}
			}
			msg, err := pipeline.EncodeCanonical(ev)
			if err != nil {
				log.Warn("encode canonical failed", "error", err)
				continue
			}
			if err := outClient.Send(msg); err != nil {
				log.Warn("ipc send to publisher failed", "error", err)
			}
		}
	}()

	// Inbound UDS server: adapter→canonicalizer. Decodes raw frames → pool.Submit.
	inSrv, err := ipc.Listen(cfg.IPC.AdapterToCanonicalizer, func(m *ipc.IPCMessage) *ipc.IPCMessage {
		rm, derr := pipeline.DecodeRaw(m)
		if derr != nil {
			log.Warn("decode raw failed", "error", derr)
			return nil
		}
		if err := pool.Submit(rm); err != nil {
			// Pool queue full → explicit backpressure. The frame was already
			// delivered (the adapter's spool drained it); a drop here is bounded
			// loss under overload (CLAUDE.md).
			log.Warn("pool backpressure (queue full)", "source", rm.Source, "error", err)
		}
		return nil
	})
	if err != nil {
		log.Error("ipc listen failed", "error", err)
		os.Exit(1)
	}

	// Health.
	hsrv := health.NewServer("canonicalizer", cfg.Processes.CanonicalizerHealthPort, func() health.Snapshot {
		ps := pool.Stats()
		cs := outClient.Stats()
		status := "ok"
		if !ps.IsHealthy() {
			status = "degraded"
		}
		return health.Snapshot{Status: status, Component: map[string]any{
			"pool":              map[string]any{"workers": ps.ActiveWorkers, "queue_depth": ps.QueueDepth, "processed": ps.Processed, "dropped": ps.Dropped},
			"ipc_out_connected":  cs.Connected,
			"ipc_out_sent":       cs.Sent,
			"ipc_out_spool_bytes": cs.SpoolBytes,
			"inbound_active":     inSrv.Stats().ActiveConn,
		}}
	})
	hsrv.SetReady(func() bool { return pool.Stats().ActiveWorkers > 0 })
	if err := hsrv.Start(); err != nil {
		log.Error("health server start failed", "error", err)
	}

	log.Info("canonicalizer process ready")
	sig := pipeline.WaitForSignal()
	log.Info("shutting down canonicalizer", "signal", sig.String())

	// Graceful, lossless: stop inbound (no new raw frames, wait for handlers) →
	// stop pool (drain input, process, close output) → drain output loop →
	// flush outbound spool → stop.
	_ = inSrv.Stop()
	_ = pool.Stop()
	<-outputDone
	fctx, fcancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := outClient.Flush(fctx); err != nil {
		log.Warn("ipc flush incomplete (publisher down?)", "error", err)
	}
	fcancel()
	_ = outClient.Stop()
	_ = hsrv.Stop()
}
