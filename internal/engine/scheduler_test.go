package engine

import (
	"fmt"
	"os"
	"testing"
	"time"

	"openboy-engine/internal/models"
	"openboy-engine/internal/store"
)

func TestWorkerPoolInFlightDeduplication(t *testing.T) {
	tempFile := "test_openboy_pool.json"
	defer os.Remove(tempFile)

	s := store.NewStore(tempFile)
	prober := NewProber()
	pool := NewWorkerPool(2, s, prober)

	mon := models.Monitor{
		ID:              "mon-test-1",
		Name:            "Test Monitor",
		URL:             "https://example.com",
		Method:          "GET",
		Interval:        "10s",
		IntervalSeconds: 10,
		Status:          "active",
	}

	// First enqueue should succeed
	ok := pool.Enqueue(mon)
	if !ok {
		t.Fatalf("Expected first Enqueue to succeed, got false")
	}

	// IsInFlight must be true immediately
	if !pool.IsInFlight(mon.ID) {
		t.Fatalf("Expected IsInFlight to be true after Enqueue")
	}

	// Second enqueue for same monitor must be rejected immediately to avoid stampede
	ok2 := pool.Enqueue(mon)
	if ok2 {
		t.Fatalf("Expected second Enqueue to be rejected while monitor is in flight, got true")
	}
}

func TestSchedulerCadenceAndInFlight(t *testing.T) {
	tempFile := "test_openboy_sched.json"
	defer os.Remove(tempFile)

	s := store.NewStore(tempFile)
	prober := NewProber()
	pool := NewWorkerPool(2, s, prober)
	sched := NewScheduler(s, pool, prober)

	mon := models.Monitor{
		ID:              "mon-test-2",
		Name:            "Test Cadence",
		URL:             "https://example.com",
		Method:          "GET",
		Interval:        "10s",
		IntervalSeconds: 10,
		Status:          "active",
	}

	// Align now to the monitor's deterministic phase offset
	now := time.Now()
	offset := models.ComputeMonitorOffset(mon.ID, 10)
	secInCycle := int(now.Unix() % 10)
	delay := (offset - secInCycle + 10) % 10
	now = now.Add(time.Duration(delay) * time.Second)

	// Brand new monitor at its scheduled offset is due
	if !sched.isMonitorDue(&mon, now) {
		t.Fatalf("Expected brand new monitor to be due at its scheduled offset")
	}

	// Mark in-flight in pool
	pool.inFlight.Store(mon.ID, now)
	if sched.isMonitorDue(&mon, now) {
		t.Fatalf("Expected isMonitorDue to be false while monitor is in-flight")
	}

	// Clear in-flight
	pool.inFlight.Delete(mon.ID)

	// Record last checked 5 seconds ago (less than 10s interval)
	checkedTime := now.Add(-5 * time.Second)
	mon.LastChecked = &checkedTime
	delete(sched.nextRun, mon.ID)

	if sched.isMonitorDue(&mon, now) {
		t.Fatalf("Expected isMonitorDue to be false when elapsed (5s) < interval (10s)")
	}

	// Advance to next cycle 10 seconds later (aligning with offset slot)
	nowNext := now.Add(10 * time.Second)
	checkedTime2 := nowNext.Add(-12 * time.Second)
	mon.LastChecked = &checkedTime2
	delete(sched.nextRun, mon.ID)

	if !sched.isMonitorDue(&mon, nowNext) {
		t.Fatalf("Expected isMonitorDue to be true when elapsed (12s) >= interval (10s)")
	}
}

func TestSchedulerEnforcesMinimumCadenceFloor(t *testing.T) {
	tempFile := "test_openboy_floor.json"
	defer os.Remove(tempFile)

	s := store.NewStore(tempFile)
	prober := NewProber()
	pool := NewWorkerPool(2, s, prober)
	sched := NewScheduler(s, pool, prober)

	mon := models.Monitor{
		ID:              "mon-test-floor",
		Name:            "Test Floor",
		URL:             "https://example.com",
		Method:          "GET",
		Interval:        "custom",
		IntervalSeconds: 1, // Rapid 1s interval attempted
		Status:          "active",
	}

	now := time.Now()
	offset := models.ComputeMonitorOffset(mon.ID, 10)
	secInCycle := int(now.Unix() % 10)
	delay := (offset - secInCycle + 10) % 10
	now = now.Add(time.Duration(delay) * time.Second)

	// Check 5 seconds after last checked
	checkedTime := now.Add(-5 * time.Second)
	mon.LastChecked = &checkedTime
	delete(sched.nextRun, mon.ID)

	// Because of 10s floor, 5 seconds elapsed should NOT be due
	if sched.isMonitorDue(&mon, now) {
		t.Fatalf("Expected minimum 10s floor to prevent 1-second cycling when only 5s elapsed")
	}

	// Advance to next cycle 10 seconds later
	nowNext := now.Add(10 * time.Second)
	checkedTime2 := nowNext.Add(-11 * time.Second)
	mon.LastChecked = &checkedTime2
	delete(sched.nextRun, mon.ID)

	if !sched.isMonitorDue(&mon, nowNext) {
		t.Fatalf("Expected monitor to be due after 11 seconds at cycle")
	}
}

func TestSchedulerStaggeredPhaseOffsets(t *testing.T) {
	tempFile := "test_openboy_stagger.json"
	defer os.Remove(tempFile)

	s := store.NewStore(tempFile)
	prober := NewProber()
	pool := NewWorkerPool(10, s, prober)
	sched := NewScheduler(s, pool, prober)

	// Create 30 monitors with 30s cadence
	var monitors []models.Monitor
	for i := 1; i <= 30; i++ {
		mon := models.Monitor{
			ID:              fmt.Sprintf("mon-batch-%d", i),
			Name:            fmt.Sprintf("Service %d", i),
			URL:             "https://example.com/health",
			Interval:        "30s",
			IntervalSeconds: 30,
			Status:          "active",
		}
		monitors = append(monitors, mon)
	}

	// Base time aligned to a 30-second boundary
	baseTime := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	totalFired := 0

	for sec := 0; sec < 30; sec++ {
		tickTime := baseTime.Add(time.Duration(sec) * time.Second)
		countAtSec := 0
		for _, m := range monitors {
			if sched.isMonitorDue(&m, tickTime) {
				countAtSec++
				totalFired++
				// Simulate dispatch advancement as done by tick()
				sched.dispatchMu.Lock()
				sched.nextRun[m.ID] = tickTime.Add(30 * time.Second)
				sched.lastDispatched[m.ID] = tickTime
				sched.dispatchMu.Unlock()
			}
		}
		// Prove that there is NO thundering herd (e.g. at most 5 monitors fire per second instead of all 30)
		if countAtSec > 5 {
			t.Fatalf("Second %d had %d monitors firing simultaneously! Thundering herd detected.", sec, countAtSec)
		}
	}

	// Over the full 30s cycle, all 30 monitors must have fired exactly once
	if totalFired != 30 {
		t.Fatalf("Expected all 30 monitors to fire across the 30s window, got %d", totalFired)
	}
}
