// cmd/storage — Addım C storage process (health on port 8084).
//
// Binds a UDS server on the publisher→storage socket. For each canonical event
// it writes to the WAL (synchronous, durable — the lossless guarantee) and
// batches into DolphinDB (best-effort; on DB timeout the WAL holds everything and
// is replayed on recovery). This process is the durable sink of the pipeline.
//
// Design (CLAUDE.md): never lose data (WAL always-on before DolphinDB), never
// panic, always observable (/health + /metrics).
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"raw-data-layer/pkg/config"
	"raw-data-layer/pkg/health"
	"raw-data-layer/pkg/ipc"
	"raw-data-layer/pkg/pipeline"
	"raw-data-layer/pkg/storage"
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
	log.Info("storage process starting", "port", cfg.Processes.StorageHealthPort)

	// WAL — always started, even with DolphinDB enabled (WAL is the durable fallback).
	// Addım F: durability mode is config-driven. Default "batched" (deferred fsync,
	// ~4500× faster than sync per Addım E E1). "sync" → per-message fsync, zero
	// in-flight loss on crash but ~20 msg/s. Both satisfy WALWriter.
	walCfg := storage.WALConfig{
		Directory:      cfg.Storage.WAL.Directory,
		MaxFileSize:    cfg.Storage.WAL.RotationSize,
		MaxMessages:    cfg.Storage.WAL.RotationCount,
		RotateInterval: config.ParseDur(cfg.Storage.WAL.SyncInterval, 1*time.Minute),
	}
	batchTimeout := time.Duration(cfg.Storage.WAL.BatchTimeoutMs) * time.Millisecond
	wal, err := storage.NewWALWriter(walCfg, cfg.Storage.WAL.Mode, batchTimeout)
	if err != nil {
		log.Error("WAL init failed", "error", err, "mode", cfg.Storage.WAL.Mode)
		os.Exit(1)
	}
	if err := wal.Start(); err != nil {
		log.Error("WAL start failed", "error", err)
		os.Exit(1)
	}
	log.Info("WAL started", "mode", cfg.Storage.WAL.Mode, "batch_timeout_ms", cfg.Storage.WAL.BatchTimeoutMs)

	// DolphinDB writer (WAL-backed; DB is best-effort).
	dbCfg := storage.DolphinDBConfig{
		Host:         cfg.Storage.DolphinDB.Host,
		Port:         cfg.Storage.DolphinDB.Port,
		Username:     cfg.Storage.DolphinDB.Username,
		Password:     cfg.Storage.DolphinDB.Password,
		Database:     cfg.Storage.DolphinDB.Database,
		BatchSize:    cfg.Storage.DolphinDB.BatchSize,
		BatchTimeout: config.ParseDur(cfg.Storage.DolphinDB.BatchTimeout, 1*time.Second),
	}
	dbWriter := storage.NewDolphinDBWriter(dbCfg, wal)
	dbWriter.Start()
	if cfg.Storage.DolphinDB.Enabled {
		if err := dbWriter.Connect(); err != nil {
			log.Warn("DolphinDB connect failed — WAL-only mode", "error", err)
		} else {
			log.Info("DolphinDB connected", "host", cfg.Storage.DolphinDB.Host, "port", cfg.Storage.DolphinDB.Port)
		}
	}

	// Inbound UDS server: publisher→storage. Decode canonical → dbWriter.Write.
	inSrv, err := ipc.Listen(cfg.IPC.PublisherToStorage, func(m *ipc.IPCMessage) *ipc.IPCMessage {
		ev, derr := pipeline.DecodeCanonical(m)
		if derr != nil {
			log.Warn("decode canonical failed", "error", derr)
			return nil
		}
		if err := dbWriter.Write(ev); err != nil {
			log.Warn("dbWriter.Write failed", "error", err)
		}
		return nil
	})
	if err != nil {
		log.Error("ipc listen failed", "error", err)
		os.Exit(1)
	}

	// Health.
	hsrv := health.NewServer("storage", cfg.Processes.StorageHealthPort, func() health.Snapshot {
		ws := wal.Stats()
		ds := dbWriter.Stats()
		status := "ok"
		if !ws.Running {
			status = "degraded"
		}
		return health.Snapshot{Status: status, Component: map[string]any{
			"wal_running":        ws.Running,
			"wal_total_written":  ws.TotalWritten,
			"wal_rotations":      ws.TotalRotations,
			"db_connected":       ds.Connected,
			"db_total_written":   ds.TotalWritten,
			"db_pending_batch":   ds.PendingBatch,
			"inbound_active":     inSrv.Stats().ActiveConn,
		}}
	})
	hsrv.SetReady(func() bool { return wal.Stats().Running })
	if err := hsrv.Start(); err != nil {
		log.Error("health server start failed", "error", err)
	}

	log.Info("storage process ready")

	sig := pipeline.WaitForSignal()
	log.Info("shutting down storage", "signal", sig.String())

	// Graceful: stop inbound (no new events, wait for handlers) → stop DB writer
	// (flush batch) → stop WAL (flush buffer).
	_ = inSrv.Stop()
	_ = dbWriter.Stop()
	_ = wal.Stop()
	_ = hsrv.Stop()
}
