// Package health exposes Prometheus metrics for the raw-data-layer pipeline
// and an HTTP /metrics endpoint for scraping.
//
// Evidence: Prometheus client_golang — the de-facto Go instrumentation library
// (https://github.com/prometheus/client_golang). Registered metrics are exported
// at /metrics via promhttp.Handler.
package health

import (
	"net/http"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metric identifiers. All names carry the raw_data_ prefix to avoid collisions
// in shared Prometheus deployments.
var (
	// MessagesReceived counts raw messages received per source.
	MessagesReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "raw_data_messages_received_total",
			Help: "Total number of raw messages received, by source.",
		},
		[]string{"source"},
	)

	// AdapterLatency records per-source adapter receive latency in microseconds.
	AdapterLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "raw_data_adapter_latency_microseconds",
			Help:    "Adapter receive latency in microseconds, by source.",
			Buckets: []float64{100, 500, 1000, 5000, 10000},
		},
		[]string{"source"},
	)

	// QueueDepth tracks the current worker pool queue depth.
	QueueDepth = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "raw_data_queue_depth",
			Help: "Current worker pool queue depth.",
		},
	)

	// Backpressure counts backpressure events (queue-full rejections).
	Backpressure = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "raw_data_backpressure_total",
			Help: "Number of backpressure (queue-full) events.",
		},
	)

	// MessagesProcessed counts messages processed by the worker pool.
	MessagesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "raw_data_messages_processed_total",
			Help: "Total number of messages processed, by source.",
		},
		[]string{"source"},
	)

	// WALWrites counts events written to WAL.
	WALWrites = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "raw_data_wal_writes_total",
			Help: "Total number of events written to the WAL.",
		},
	)

	// DolphinDBWrites counts events written to DolphinDB.
	DolphinDBWrites = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "raw_data_dolphindb_writes_total",
			Help: "Total number of events written to DolphinDB.",
		},
	)

	// DolphinDBWriteErrors counts DolphinDB write failures.
	DolphinDBWriteErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "raw_data_dolphindb_write_errors_total",
			Help: "Total number of DolphinDB write failures.",
		},
	)

	registered = false
	registerMu sync.Mutex
)

// Register registers all metrics with the default Prometheus registry.
// Safe to call multiple times; only the first call has effect.
func Register() {
	registerMu.Lock()
	defer registerMu.Unlock()
	if registered {
		return
	}
	prometheus.MustRegister(
		MessagesReceived,
		AdapterLatency,
		QueueDepth,
		Backpressure,
		MessagesProcessed,
		WALWrites,
		DolphinDBWrites,
		DolphinDBWriteErrors,
	)
	registered = true
}

// MetricsHandler returns an http.Handler that exposes registered metrics in
// the Prometheus text exposition format.
func MetricsHandler() http.Handler {
	return promhttp.Handler()
}
