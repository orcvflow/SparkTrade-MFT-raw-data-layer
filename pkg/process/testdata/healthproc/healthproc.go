// healthproc is a tiny stand-in process for testing pkg/process. It serves
// GET /health on HEALTH_PORT and blocks until SIGINT/SIGTERM. If CRASH_AFTER_MS
// is set, it exits with code 1 after that duration (to simulate a crashing
// process that the manager must restart).
//
// This is a separate `package main` compiled to a binary by the tests.
package main

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {
	port := os.Getenv("HEALTH_PORT")
	if port == "" {
		port = "0"
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, `{"status":"ok"}`)
	})
	srv := &http.Server{Addr: ":" + port, Handler: mux}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// bind failed → exit non-zero so the manager observes a crash.
			os.Exit(2)
		}
	}()

	if raw := os.Getenv("CRASH_AFTER_MS"); raw != "" {
		if ms, err := strconv.Atoi(raw); err == nil && ms > 0 {
			time.Sleep(time.Duration(ms) * time.Millisecond)
			os.Exit(1) // simulate a crash
		}
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	_ = srv.Close()
}
