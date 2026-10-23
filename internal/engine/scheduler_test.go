package engine

import (
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

	now := time.Now()
	mon := models.Monitor{
		ID:              "mon-test-2",
		Name:            "Test Cadence",
		URL:             "https://example.com",
		Method:          "GET",
		Interval:        "10s",
		IntervalSeconds: 10,
		Status:          "active",
	}

	// Brand new monitor with nil LastChecked is due
	if !sched.isMonitorDue(&mon, now) {
		t.Fatalf("Expected brand new monitor to be due")
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

	if sched.isMonitorDue(&mon, now) {
		t.Fatalf("Expected isMonitorDue to be false when elapsed (5s) < interval (10s)")
	}

	// Record last checked 12 seconds ago (greater than 10s interval)
	checkedTime2 := now.Add(-12 * time.Second)
	mon.LastChecked = &checkedTime2

	if !sched.isMonitorDue(&mon, now) {
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

	now := time.Now()
	mon := models.Monitor{
		ID:              "mon-test-floor",
		Name:            "Test Floor",
		URL:             "https://example.com",
		Method:          "GET",
		Interval:        "custom",
		IntervalSeconds: 1, // Rapid 1s interval attempted
		Status:          "active",
	}

	// Check 5 seconds after last checked
	checkedTime := now.Add(-5 * time.Second)
	mon.LastChecked = &checkedTime

	// Because of 10s floor, 5 seconds elapsed should NOT be due
	if sched.isMonitorDue(&mon, now) {
		t.Fatalf("Expected minimum 10s floor to prevent 1-second cycling when only 5s elapsed")
	}

	// 11 seconds elapsed should be due
	checkedTime2 := now.Add(-11 * time.Second)
	mon.LastChecked = &checkedTime2
	if !sched.isMonitorDue(&mon, now) {
		t.Fatalf("Expected monitor to be due after 11 seconds")
	}
}
