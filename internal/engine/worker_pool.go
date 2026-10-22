package engine

import (
	"context"
	"sync"
	"sync/atomic"

	"openboy-engine/internal/models"
	"openboy-engine/internal/store"
	"openboy-engine/pkg/logger"
)

// Job represents an execution task queued for a worker.
type Job struct {
	Monitor models.Monitor
}

// WorkerPool manages concurrent worker goroutines executing health checks.
type WorkerPool struct {
	workers       int
	jobs          chan Job
	store         *store.Store
	prober        *Prober
	activeWorkers int32
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
}

// NewWorkerPool initializes the worker pool with specified concurrency limit.
func NewWorkerPool(workers int, s *store.Store, p *Prober) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerPool{
		workers: workers,
		jobs:    make(chan Job, 1000),
		store:   s,
		prober:  p,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Start launches the worker pool goroutines.
func (wp *WorkerPool) Start() {
	logger.Engine("Starting worker pool with %d concurrent workers", wp.workers)
	for i := 0; i < wp.workers; i++ {
		wp.wg.Add(1)
		go wp.worker(i)
	}
}

func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()

	for {
		select {
		case <-wp.ctx.Done():
			return
		case job, ok := <-wp.jobs:
			if !ok {
				return
			}

			atomic.AddInt32(&wp.activeWorkers, 1)

			// Probe the target endpoint
			log := wp.prober.Probe(wp.ctx, &job.Monitor)

			// Store outcome and update monitor stats
			wp.store.RecordPingResult(log)

			if log.OK {
				logger.Probe("[%s] %s %s -> %d (%dms)", log.MonitorID, log.Method, log.URL, log.Status, log.Latency)
			} else {
				logger.Warn("[%s] %s %s -> %d FAILING: %s", log.MonitorID, log.Method, log.URL, log.Status, log.Error)
			}

			atomic.AddInt32(&wp.activeWorkers, -1)
		}
	}
}

// Enqueue submits a monitor probe job to the worker pool.
func (wp *WorkerPool) Enqueue(m models.Monitor) bool {
	select {
	case wp.jobs <- Job{Monitor: m}:
		return true
	default:
		logger.Warn("Worker queue full! Dropping probe job for %s", m.ID)
		return false
	}
}

// ActiveWorkers returns the count of currently executing workers.
func (wp *WorkerPool) ActiveWorkers() int {
	return int(atomic.LoadInt32(&wp.activeWorkers))
}

// Stop gracefully shuts down all workers.
func (wp *WorkerPool) Stop() {
	logger.Engine("Stopping worker pool...")
	wp.cancel()
	close(wp.jobs)
	wp.wg.Wait()
	logger.Engine("Worker pool stopped cleanly")
}
