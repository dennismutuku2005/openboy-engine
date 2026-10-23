package engine

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"openboy-engine/internal/cronexpr"
	"openboy-engine/internal/models"
	"openboy-engine/internal/store"
	"openboy-engine/pkg/logger"
)

// Scheduler coordinates timed cron invocations and high-frequency tickers.
type Scheduler struct {
	store          *store.Store
	pool           *WorkerPool
	prober         *Prober
	ticker         *time.Ticker
	cronParsers    map[string]*cronexpr.Expression
	cronMu         sync.RWMutex
	lastCronRun    map[string]int64 // monitor/batcher ID -> unix minute
	lastDispatched map[string]time.Time
	dispatchMu     sync.RWMutex
	ctx            context.Context
	cancel         context.CancelFunc
	wg             sync.WaitGroup
}

// NewScheduler constructs the scheduler orchestrator.
func NewScheduler(s *store.Store, pool *WorkerPool, p *Prober) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &Scheduler{
		store:          s,
		pool:           pool,
		prober:         p,
		cronParsers:    make(map[string]*cronexpr.Expression),
		lastCronRun:    make(map[string]int64),
		lastDispatched: make(map[string]time.Time),
		ctx:            ctx,
		cancel:         cancel,
	}
}

// Start launches the precision 1-second scheduler loop.
func (sc *Scheduler) Start() {
	sc.ticker = time.NewTicker(1 * time.Second)
	sc.wg.Add(1)

	logger.Engine("Scheduler started with 1-second master clock")

	go func() {
		defer sc.wg.Done()
		for {
			select {
			case <-sc.ctx.Done():
				return
			case tickTime := <-sc.ticker.C:
				sc.tick(tickTime)
			}
		}
	}()
}

func (sc *Scheduler) tick(now time.Time) {
	monitors := sc.store.GetAllMonitors()

	for _, m := range monitors {
		if m.Status == "paused" {
			continue
		}

		if sc.isMonitorDue(&m, now) {
			if sc.pool.Enqueue(m) {
				sc.dispatchMu.Lock()
				sc.lastDispatched[m.ID] = now
				sc.dispatchMu.Unlock()
			}
		}
	}

	// Evaluate batchers if on a full minute boundary
	if now.Second() == 0 {
		sc.evaluateBatchers(now)
	}
}

func (sc *Scheduler) isMonitorDue(m *models.Monitor, now time.Time) bool {
	// If a health check probe for this monitor is already queued or executing, NEVER enqueue another!
	if sc.pool.IsInFlight(m.ID) {
		return false
	}

	// 1. Cron-based
	if m.CronExpr != "" || m.Interval == "cron" {
		exprStr := m.CronExpr
		if exprStr == "" {
			return false
		}

		sc.cronMu.RLock()
		expr, ok := sc.cronParsers[m.ID]
		sc.cronMu.RUnlock()

		if !ok {
			var err error
			expr, err = cronexpr.Parse(exprStr)
			if err != nil {
				return false
			}
			sc.cronMu.Lock()
			sc.cronParsers[m.ID] = expr
			sc.cronMu.Unlock()
		}

		currentMinute := now.Unix() / 60
		if sc.lastCronRun[m.ID] == currentMinute {
			return false
		}

		if expr.Matches(now) {
			sc.lastCronRun[m.ID] = currentMinute
			logger.Cron("[%s] Cron matched '%s' at %s", m.ID, exprStr, now.Format("15:04:05"))
			return true
		}
		return false
	}

	// 2. High-frequency and interval-based (10s, 30s, 10m, custom seconds)
	intervalSec := m.IntervalSeconds
	if intervalSec <= 0 {
		switch m.Interval {
		case "10s":
			intervalSec = 10
		case "30s":
			intervalSec = 30
		case "1m":
			intervalSec = 60
		case "5m":
			intervalSec = 300
		case "10m":
			intervalSec = 600
		case "30m":
			intervalSec = 1800
		case "1h":
			intervalSec = 3600
		case "24h":
			intervalSec = 86400
		default:
			intervalSec = 30
		}
	}

	// Strict minimum cadence floor: OpenBoy's fastest probe cadence is 10 seconds.
	// This prevents misconfigured monitors or rapid loops from hammering endpoints.
	if intervalSec < 10 {
		intervalSec = 10
	}

	// Cooldown since last dispatch to worker pool
	sc.dispatchMu.RLock()
	lastDisp, hasDisp := sc.lastDispatched[m.ID]
	sc.dispatchMu.RUnlock()
	if hasDisp && now.Sub(lastDisp).Seconds() < float64(intervalSec) {
		return false
	}

	// Cooldown since last completed execution recorded in store
	if m.LastChecked != nil {
		elapsed := now.Sub(*m.LastChecked).Seconds()
		if elapsed < float64(intervalSec) {
			return false
		}
	}

	return true
}

func (sc *Scheduler) evaluateBatchers(now time.Time) {
	batchers := sc.store.GetAllBatchers()
	currentMinute := now.Unix() / 60

	for _, b := range batchers {
		if b.Status != "active" {
			continue
		}

		if sc.lastCronRun[b.ID] == currentMinute {
			continue
		}

		due := false
		sched := strings.ToLower(b.Schedule)

		// Human-friendly schedule evaluations (non-technical)
		if strings.Contains(sched, "10m") || strings.Contains(sched, "10 min") {
			due = (now.Minute() % 10 == 0)
		} else if strings.Contains(sched, "5m") || strings.Contains(sched, "5 min") {
			due = (now.Minute() % 5 == 0)
		} else if strings.Contains(sched, "1m") || strings.Contains(sched, "1 min") {
			due = true
		} else if strings.Contains(sched, "hourly") || strings.Contains(sched, "1 hour") {
			due = (now.Minute() == 0)
		} else if strings.Contains(sched, "03:00") {
			due = (now.Hour() == 3 && now.Minute() == 0)
		} else if strings.Contains(sched, "midnight") || strings.Contains(sched, "daily") {
			due = (now.Hour() == 0 && now.Minute() == 0)
		} else if b.CronExpr != "" {
			// Fallback to cron parser if present
			sc.cronMu.RLock()
			expr, ok := sc.cronParsers[b.ID]
			sc.cronMu.RUnlock()

			if !ok {
				var err error
				expr, err = cronexpr.Parse(b.CronExpr)
				if err == nil {
					sc.cronMu.Lock()
					sc.cronParsers[b.ID] = expr
					sc.cronMu.Unlock()
				}
			}

			if expr != nil && expr.Matches(now) {
				due = true
			}
		}

		if due {
			sc.lastCronRun[b.ID] = currentMinute
			logger.Engine("[BATCHER:%s] Executing scheduled batch job '%s' (%s)", b.ID, b.Name, b.Schedule)
			sc.store.TriggerBatcher(b.ID)
		}
	}
}

// TriggerPingNow manually executes a probe synchronously for immediate UI feedback.
func (sc *Scheduler) TriggerPingNow(monitorID string) (*models.ExecutionLog, error) {
	m, ok := sc.store.GetMonitor(monitorID)
	if !ok {
		return nil, fmt.Errorf("monitor '%s' not found", monitorID)
	}

	if sc.pool.IsInFlight(monitorID) {
		return nil, fmt.Errorf("a health check for monitor '%s' is already in progress", monitorID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	log := sc.prober.Probe(ctx, m)
	sc.store.RecordPingResult(log)

	sc.dispatchMu.Lock()
	sc.lastDispatched[monitorID] = time.Now()
	sc.dispatchMu.Unlock()

	return &log, nil
}

// Stop terminates the scheduler tick loop.
func (sc *Scheduler) Stop() {
	logger.Engine("Stopping scheduler...")
	sc.cancel()
	if sc.ticker != nil {
		sc.ticker.Stop()
	}
	sc.wg.Wait()
	logger.Engine("Scheduler stopped cleanly")
}
