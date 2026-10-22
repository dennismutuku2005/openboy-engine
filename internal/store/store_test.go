package store

import (
	"testing"
	"time"

	"openboy-engine/internal/models"
)

func TestStoreOperations(t *testing.T) {
	s := NewStore("") // in-memory only (no file persistence)

	// 1. Verify Default Admin User
	adminUser, ok := s.GetUserByUsernameOrEmail("admin")
	if !ok || adminUser.Username != "admin" {
		t.Fatalf("Expected default admin user to exist")
	}

	// 2. Monitor CRUD
	newMon := s.CreateMonitor(models.Monitor{
		Name:            "Test Health Endpoint",
		URL:             "https://example.com/health",
		Method:          "GET",
		Interval:        "10s",
		IntervalSeconds: 10,
		IntervalLabel:   "10s",
		ExpectedStatus:  200,
	})

	gotMon, ok := s.GetMonitor(newMon.ID)
	if !ok || gotMon.Name != "Test Health Endpoint" {
		t.Fatalf("Failed to retrieve created monitor %s", newMon.ID)
	}

	// Toggle
	toggled, ok := s.ToggleMonitorStatus(newMon.ID)
	if !ok || toggled.Status != "paused" {
		t.Errorf("Expected status 'paused', got '%s'", toggled.Status)
	}

	// Record Ping Result
	s.RecordPingResult(models.ExecutionLog{
		ID:          "test-log-1",
		MonitorID:   newMon.ID,
		MonitorName: newMon.Name,
		URL:         newMon.URL,
		Method:      "GET",
		Status:      200,
		Latency:     45,
		Timestamp:   time.Now(),
		OK:          true,
	})

	updatedMon, _ := s.GetMonitor(newMon.ID)
	if updatedMon.TotalPings != 1 || updatedMon.LastStatus != 200 {
		t.Errorf("Monitor stats not updated properly after ping: %+v", updatedMon)
	}

	// Delete
	if !s.DeleteMonitor(newMon.ID) {
		t.Fatalf("Failed to delete monitor %s", newMon.ID)
	}
	if _, ok := s.GetMonitor(newMon.ID); ok {
		t.Fatalf("Monitor should not exist after deletion")
	}

	// 3. User operations
	user, ok := s.GetUserByUsernameOrEmail("admin")
	if !ok || user.Username != "admin" {
		t.Fatalf("Expected default admin user to exist")
	}

	byKey, ok := s.GetUserByAPIKey(user.APIKey)
	if !ok || byKey.ID != user.ID {
		t.Fatalf("Failed to get user by API key")
	}

	// 4. Batchers
	testBatcher := s.CreateBatcher(models.Batcher{
		Name:     "Test Batch Worker",
		JobKey:   "test_job",
		Schedule: "Every 10m",
		Status:   "active",
	})
	triggered, ok := s.TriggerBatcher(testBatcher.ID)
	if !ok || triggered.LastRunFormatted != "Just now" {
		t.Errorf("Batcher trigger failed: %+v", triggered)
	}

	// 5. Incidents
	testIncident := s.CreateIncident(models.Incident{
		Title:    "Test Outage Incident",
		Service:  "Test Service",
		Severity: "P2 - High",
		Status:   "investigating",
	})
	resolved, ok := s.ResolveIncident(testIncident.ID)
	if !ok || resolved.Status != "resolved" {
		t.Errorf("Incident resolve failed: %+v", resolved)
	}
}
