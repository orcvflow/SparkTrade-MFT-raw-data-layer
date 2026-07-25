package workerpool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"raw-data-layer/pkg/adapter"
	"raw-data-layer/pkg/health"
)

// Pool manages a bounded pool of workers for processing messages
// Evidence: DataSea.cn benchmark - unlimited goroutines → OOM at 37s
// Bounded pool (50 workers) → 74.5% less memory, 88.5% less GC pause
type Pool struct {
	// Configuration
	minWorkers int
	maxWorkers int
	queueSize  int
	
	// Channels
	input     chan adapter.RawMessage
	output    chan ProcessedMessage
	
	// State
	activeWorkers atomic.Int32
	queueDepth    atomic.Int32
	processed     atomic.Uint64
	dropped       atomic.Uint64
	errors        atomic.Uint64
	stopped       atomic.Bool // set by Stop() so Submit can reject without racing a channel close
	
	// Synchronization
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	mu         sync.RWMutex
	errorList  []error
	
	// Metrics
	startTime      time.Time
	lastProcessed  atomic.Value // time.Time
	
	// Worker function
	processor ProcessorFunc
	
	// Autoscaling
	autoscaleEnabled bool
	scaleUpThreshold float64
	scaleDownThreshold float64
	lastScaleCheck   time.Time
}

// ProcessedMessage represents a processed message ready for downstream stages.
// Canonical holds the fully-built canonical event produced by the processor
// (typed as `any` to avoid an import cycle between workerpool and canonicalizer).
type ProcessedMessage struct {
	Raw         adapter.RawMessage
	Canonical   any
	Error       error
	ProcessedAt int64
}

// ProcessorFunc is the function signature for message processing
type ProcessorFunc func(ctx context.Context, raw adapter.RawMessage) (ProcessedMessage, error)

// PoolConfig holds worker pool configuration
type PoolConfig struct {
	MinWorkers         int
	MaxWorkers         int
	QueueSize          int
	AutoscaleEnabled   bool
	ScaleUpThreshold   float64  // Queue utilization % to scale up (e.g., 0.8 = 80%)
	ScaleDownThreshold float64  // Queue utilization % to scale down (e.g., 0.5 = 50%)
}

// DefaultPoolConfig returns default configuration
// Based on CLAUDE.md specifications: 50 workers, 10K queue
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		MinWorkers:         50,
		MaxWorkers:         100,
		QueueSize:          10000,
		AutoscaleEnabled:   true,
		ScaleUpThreshold:   0.8,  // Scale up at 80% full
		ScaleDownThreshold: 0.5,  // Scale down at 50% full
	}
}

// NewPool creates a new worker pool
func NewPool(config PoolConfig, processor ProcessorFunc) *Pool {
	ctx, cancel := context.WithCancel(context.Background())
	
	if config.MinWorkers <= 0 {
		config.MinWorkers = 50
	}
	if config.MaxWorkers <= 0 {
		config.MaxWorkers = 100
	}
	if config.QueueSize <= 0 {
		config.QueueSize = 10000
	}
	
	return &Pool{
		minWorkers:         config.MinWorkers,
		maxWorkers:         config.MaxWorkers,
		queueSize:          config.QueueSize,
		input:              make(chan adapter.RawMessage, config.QueueSize),
		output:             make(chan ProcessedMessage, config.QueueSize),
		ctx:                ctx,
		cancel:             cancel,
		errorList:          make([]error, 0),
		startTime:          time.Now(),
		processor:          processor,
		autoscaleEnabled:   config.AutoscaleEnabled,
		scaleUpThreshold:   config.ScaleUpThreshold,
		scaleDownThreshold: config.ScaleDownThreshold,
		lastScaleCheck:     time.Now(),
	}
}

// Start initializes the worker pool with minimum workers
func (p *Pool) Start() error {
	// Start with minimum workers
	for i := 0; i < p.minWorkers; i++ {
		p.spawnWorker()
	}
	
	// Start autoscaler if enabled
	if p.autoscaleEnabled {
		go p.autoscaler()
	}
	
	return nil
}

// Submit submits a message to the worker pool
// Returns error if queue is full (backpressure)
func (p *Pool) Submit(msg adapter.RawMessage) error {
	// Never panic - use defer recover
	defer func() {
		if r := recover(); r != nil {
			p.addError(fmt.Errorf("panic in Submit: %v", r))
		}
	}()

	if p.stopped.Load() {
		p.dropped.Add(1)
		health.Backpressure.Inc()
		return fmt.Errorf("worker pool stopped")
	}

	select {
	case p.input <- msg:
		p.queueDepth.Add(1)
		health.QueueDepth.Set(float64(p.queueDepth.Load()))
		return nil
	default:
		// Queue full - backpressure engaged
		p.dropped.Add(1)
		health.Backpressure.Inc()
		return fmt.Errorf("worker pool queue full (size: %d)", p.queueSize)
	}
}

// Output returns the output channel for processed messages
func (p *Pool) Output() <-chan ProcessedMessage {
	return p.output
}

// spawnWorker spawns a new worker goroutine
func (p *Pool) spawnWorker() {
	current := p.activeWorkers.Add(1)
	if int(current) > p.maxWorkers {
		p.activeWorkers.Add(-1)
		return
	}
	
	p.wg.Add(1)
	go p.worker()
}

// worker is the main worker loop
func (p *Pool) worker() {
	defer func() {
		if r := recover(); r != nil {
			p.addError(fmt.Errorf("panic in worker: %v", r))
		}
		p.activeWorkers.Add(-1)
		p.wg.Done()
	}()
	
	for {
		select {
		case <-p.ctx.Done():
			// Context cancelled (e.g. Stop()). Before exiting, drain any
			// messages already queued so a graceful shutdown does not drop
			// in-flight work. Non-blocking: once the queue is empty, exit.
			for {
				select {
				case msg, ok := <-p.input:
					if !ok {
						return
					}
					p.queueDepth.Add(-1)
					health.QueueDepth.Set(float64(p.queueDepth.Load()))
					p.processMessage(msg)
				default:
					return
				}
			}
		case msg, ok := <-p.input:
			if !ok {
				return
			}

			p.queueDepth.Add(-1)
			health.QueueDepth.Set(float64(p.queueDepth.Load()))
			p.processMessage(msg)
		}
	}
}

// processMessage processes a single message
func (p *Pool) processMessage(msg adapter.RawMessage) {
	defer func() {
		if r := recover(); r != nil {
			p.addError(fmt.Errorf("panic in processMessage: %v", r))
			p.errors.Add(1)
		}
	}()
	
	// Process with timeout
	ctx, cancel := context.WithTimeout(p.ctx, 5*time.Second)
	defer cancel()
	
	processed, err := p.processor(ctx, msg)
	if err != nil {
		p.errors.Add(1)
		p.addError(fmt.Errorf("processor error: %w", err))
		
		// Send error message downstream
		processed = ProcessedMessage{
			Raw:         msg,
			Error:       err,
			ProcessedAt: time.Now().UnixNano(),
		}
	}
	
	// Mark the message as processed BEFORE delivering it. The output channel is
	// buffered, so a consumer that receives from Output() and then immediately
	// reads Stats() must already see the increment — otherwise there is a
	// logical race (send → consumer reads → worker finally increments). On the
	// rare blocked-delivery path we also count a drop (not delivered).
	p.processed.Add(1)
	health.MessagesProcessed.WithLabelValues(msg.Source).Inc()
	p.lastProcessed.Store(time.Now())

	select {
	case p.output <- processed:
	case <-time.After(1 * time.Second):
		p.dropped.Add(1)
		p.addError(fmt.Errorf("output channel blocked"))
	}
}

// autoscaler monitors queue depth and scales workers up/down
func (p *Pool) autoscaler() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-p.ctx.Done():
			return
		case <-ticker.C:
			p.checkScale()
		}
	}
}

// checkScale checks if scaling is needed
func (p *Pool) checkScale() {
	defer func() {
		if r := recover(); r != nil {
			p.addError(fmt.Errorf("panic in checkScale: %v", r))
		}
	}()
	
	queueDepth := int(p.queueDepth.Load())
	utilization := float64(queueDepth) / float64(p.queueSize)
	activeWorkers := int(p.activeWorkers.Load())
	
	// Scale up if queue is filling
	if utilization >= p.scaleUpThreshold && activeWorkers < p.maxWorkers {
		// Add 25% more workers, up to maxWorkers
		toAdd := (p.maxWorkers - activeWorkers) / 4
		if toAdd < 10 {
			toAdd = 10
		}
		
		for i := 0; i < toAdd && activeWorkers < p.maxWorkers; i++ {
			p.spawnWorker()
			activeWorkers++
		}
	}
	
	// Scale down if queue is empty
	if utilization <= p.scaleDownThreshold && activeWorkers > p.minWorkers {
		// This is simplified - in production, we'd need worker cancellation
		// For MVP, workers will naturally exit when pool stops
	}
	
	p.lastScaleCheck = time.Now()
}

// Stop gracefully stops the worker pool.
//
// Race safety: we set `stopped` and cancel the context but do NOT close the
// input channel. Workers exit on ctx.Done() after draining queued messages.
// Closing p.input here would race any in-flight Submit() (a submitter goroutine
// can still be running — e.g. an adapter bridge), producing a
// send-on-closed-channel data race. Leaving the buffered channel open and
// rejecting new submits via the atomic `stopped` flag is race-free; the channel
// is simply GC'd once the pool is unreachable.
func (p *Pool) Stop() error {
	p.stopped.Store(true)
	p.cancel()

	// Wait for all workers to finish
	p.wg.Wait()
	
	// Close output channel
	close(p.output)
	
	return nil
}

// Stats returns current pool statistics
func (p *Pool) Stats() PoolStats {
	p.mu.RLock()
	errors := make([]error, len(p.errorList))
	copy(errors, p.errorList)
	p.mu.RUnlock()
	
	lastProc := time.Time{}
	if v := p.lastProcessed.Load(); v != nil {
		lastProc = v.(time.Time)
	}
	
	return PoolStats{
		ActiveWorkers:  int(p.activeWorkers.Load()),
		QueueDepth:     int(p.queueDepth.Load()),
		QueueSize:      p.queueSize,
		Processed:      p.processed.Load(),
		Dropped:        p.dropped.Load(),
		Errors:         p.errors.Load(),
		LastProcessed:  lastProc,
		Uptime:         time.Since(p.startTime),
		ErrorList:      errors,
	}
}

// PoolStats holds pool statistics
type PoolStats struct {
	ActiveWorkers  int
	QueueDepth     int
	QueueSize      int
	Processed      uint64
	Dropped        uint64
	Errors         uint64
	LastProcessed  time.Time
	Uptime         time.Duration
	ErrorList      []error
}

// Utilization returns queue utilization percentage (0.0 to 1.0)
func (s PoolStats) Utilization() float64 {
	if s.QueueSize == 0 {
		return 0.0
	}
	return float64(s.QueueDepth) / float64(s.QueueSize)
}

// IsHealthy returns true if pool is healthy
func (s PoolStats) IsHealthy() bool {
	// Pool is unhealthy if:
	// - No active workers
	// - Queue is >90% full (backpressure risk)
	// - More than 10% messages dropped
	if s.ActiveWorkers == 0 {
		return false
	}
	
	if s.Utilization() > 0.9 {
		return false
	}
	
	if s.Processed > 0 {
		dropRate := float64(s.Dropped) / float64(s.Processed+s.Dropped)
		if dropRate > 0.1 {
			return false
		}
	}
	
	return true
}

// addError adds an error to the error list (max 10)
func (p *Pool) addError(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	p.errorList = append(p.errorList, err)
	if len(p.errorList) > 10 {
		p.errorList = p.errorList[1:]
	}
}
