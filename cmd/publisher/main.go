// cmd/publisher — Addım C publisher process (health on port 8083).
//
// Binds a UDS server on the canonicalizer→publisher socket. For each canonical
// event it (a) broadcasts to strategies via ZeroMQ PUB (best-effort, topic =
// canonical symbol) and (b) forwards the exact received bytes to the storage
// process over a lossless UDS client. Forwarding the original payload bytes —
// rather than decode-then-re-encode — keeps the canonical event byte-for-byte
// intact across this hop and avoids redundant work.
//
// Design (CLAUDE.md): never panic, never lose data (forward spool to storage),
// ZMQ is best-effort (slow consumer never crashes the process), always observable.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"raw-data-layer/pkg/config"
	"raw-data-layer/pkg/health"
	"raw-data-layer/pkg/ipc"
	"raw-data-layer/pkg/pipeline"
	"raw-data-layer/pkg/publisher"
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
	log.Info("publisher process starting", "port", cfg.Processes.PublisherHealthPort)

	// ZeroMQ publisher (best-effort broadcast to strategies).
	var zmqPub *publisher.ZMQPublisher
	if cfg.Publisher.Zeromq.Enabled {
		endpoint := fmt.Sprintf("%s://%s:%d",
			cfg.Publisher.Zeromq.Protocol, cfg.Publisher.Zeromq.BindAddress, cfg.Publisher.Zeromq.Port)
		zmqCfg := publisher.PublisherConfig{
			Endpoint:          endpoint,
			HeartbeatInterval: config.ParseDur(cfg.Publisher.Zeromq.HeartbeatInterval, 5*time.Second),
			SendTimeout:       1 * time.Second,
		}
		pub, err := publisher.NewZMQPublisher(zmqCfg)
		if err != nil {
			log.Warn("zmq publisher init failed (continuing without ZMQ)", "error", err)
		} else if err := pub.Start(); err != nil {
			log.Warn("zmq publisher start failed (continuing without ZMQ)", "error", err)
		} else {
			zmqPub = pub
			log.Info("zmq publisher started", "endpoint", endpoint)
		}
	}

	// Outbound IPC client → storage (lossless, spooled).
	outClient, err := ipc.NewClient(cfg.IPC.PublisherToStorage, ipc.ClientConfig{
		Backoff:   cfg.Adapters.Binance.Reconnect.BackoffSeconds,
		SpoolPath: pipeline.SpoolPath(cfg, "publisher"),
	})
	if err != nil {
		log.Error("ipc client init failed", "error", err)
		os.Exit(1)
	}
	outClient.Start()

	// Inbound UDS server: canonicalizer→publisher. Decodes for ZMQ (best-effort)
	// and forwards the original bytes to storage (lossless, no re-encode).
	inSrv, err := ipc.Listen(cfg.IPC.CanonicalizerToPublisher, func(m *ipc.IPCMessage) *ipc.IPCMessage {
		if zmqPub != nil {
			if ev, derr := pipeline.DecodeCanonical(m); derr == nil && ev != nil {
				if perr := zmqPub.Publish(ev); perr != nil {
					log.Debug("zmq publish failed (best-effort)", "error", perr)
				}
			}
		}
		// Forward the exact canonical bytes onward (byte-for-byte integrity).
		fwd := ipc.NewMessage(ipc.TypeCanonical, m.Payload, ipc.NextSeq())
		if serr := outClient.Send(fwd); serr != nil {
			log.Warn("ipc send to storage failed", "error", serr)
		}
		return nil
	})
	if err != nil {
		log.Error("ipc listen failed", "error", err)
		os.Exit(1)
	}

	// Health.
	hsrv := health.NewServer("publisher", cfg.Processes.PublisherHealthPort, func() health.Snapshot {
		cs := outClient.Stats()
		comp := map[string]any{
			"inbound_active":     inSrv.Stats().ActiveConn,
			"ipc_out_connected":  cs.Connected,
			"ipc_out_sent":       cs.Sent,
			"ipc_out_spool_bytes": cs.SpoolBytes,
		}
		if zmqPub != nil {
			zs := zmqPub.Stats()
			comp["zmq_published"] = zs.Published
			comp["zmq_dropped"] = zs.Dropped
		}
		return health.Snapshot{Status: "ok", Component: comp}
	})
	hsrv.SetReady(func() bool { return inSrv.Stats().ActiveConn >= 0 })
	if err := hsrv.Start(); err != nil {
		log.Error("health server start failed", "error", err)
	}

	log.Info("publisher process ready")
	sig := pipeline.WaitForSignal()
	log.Info("shutting down publisher", "signal", sig.String())

	// Graceful: stop inbound → flush outbound spool to storage → stop client → stop ZMQ.
	_ = inSrv.Stop()
	fctx, fcancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := outClient.Flush(fctx); err != nil {
		log.Warn("ipc flush incomplete (storage down?)", "error", err)
	}
	fcancel()
	_ = outClient.Stop()
	if zmqPub != nil {
		_ = zmqPub.Stop()
	}
	_ = hsrv.Stop()
}
