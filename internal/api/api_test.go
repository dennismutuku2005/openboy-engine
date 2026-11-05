package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"openboy-engine/internal/engine"
	"openboy-engine/internal/models"
	"openboy-engine/internal/store"
	"openboy-engine/pkg/auth"
)

func setupTestRouter(tb testing.TB) (*API, http.Handler, string) {
	s := store.NewStore(tb.TempDir() + "/test_state.json")
	prober := engine.NewProber()
	pool := engine.NewWorkerPool(2, s, prober)

	// In API unit tests, pass nil scheduler so no uncoordinated background network probes are spawned
	apiService := NewAPI(s, nil, pool)
	router := NewRouter(apiService)

	// Create test user and token
	user, err := s.CreateUser(models.User{
		Username:     "testuser",
		Email:        "test@example.com",
		PasswordHash: "dummy",
		Name:         "Test User",
		Role:         "admin",
		APIKey:       "test-api-key-12345",
	})
	if err != nil {
		tb.Fatalf("failed to create test user: %v", err)
	}

	token, err := auth.GenerateToken(auth.Claims{
		UserID:   user.ID,
		Username: user.Username,
		Email:    user.Email,
		Role:     user.Role,
	})
	if err != nil {
		tb.Fatalf("failed to generate test token: %v", err)
	}

	return apiService, router, token
}

func TestPublicRoutes(t *testing.T) {
	_, router, _ := setupTestRouter(t)

	// 1. Root route
	req, _ := http.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 on /, got %d", w.Code)
	}

	var rootResp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &rootResp); err != nil {
		t.Fatalf("failed to unmarshal root response: %v", err)
	}
	if rootResp["framework"] != "Gin" {
		t.Errorf("expected framework to be Gin, got %v", rootResp["framework"])
	}

	// 2. Health check
	req, _ = http.NewRequest(http.MethodGet, "/api/health", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 on /api/health, got %d", w.Code)
	}
}

func TestAuthProtection(t *testing.T) {
	_, router, token := setupTestRouter(t)

	// 1. Unauthenticated request to /api/monitors should return 401
	req, _ := http.NewRequest(http.MethodGet, "/api/monitors", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 without auth token, got %d", w.Code)
	}

	// 2. Authenticated request with Bearer token should return 200
	req, _ = http.NewRequest(http.MethodGet, "/api/monitors", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 with Bearer token, got %d", w.Code)
	}

	// 3. Authenticated request with X-API-Key should return 200
	req, _ = http.NewRequest(http.MethodGet, "/api/monitors", nil)
	req.Header.Set("X-API-Key", "test-api-key-12345")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 with X-API-Key, got %d", w.Code)
	}
}

func TestMonitorsCrudWithGin(t *testing.T) {
	_, router, token := setupTestRouter(t)

	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`))
	}))
	defer mockServer.Close()

	// 1. Create a monitor
	createBody := map[string]interface{}{
		"name":     "Pace Wisp Gateway",
		"url":      mockServer.URL,
		"method":   "GET",
		"interval": "30s",
	}
	bodyBytes, _ := json.Marshal(createBody)
	req, _ := http.NewRequest(http.MethodPost, "/api/monitors", bytes.NewReader(bodyBytes))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("expected 201 on create monitor, got %d (body: %s)", w.Code, w.Body.String())
	}

	var created models.Monitor
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatalf("failed to decode created monitor: %v", err)
	}
	if created.Name != "Pace Wisp Gateway" {
		t.Errorf("expected monitor name 'Pace Wisp Gateway', got '%s'", created.Name)
	}

	// 2. Get the created monitor
	req, _ = http.NewRequest(http.MethodGet, "/api/monitors/"+created.ID, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 on get monitor, got %d", w.Code)
	}

	// 3. Toggle monitor
	req, _ = http.NewRequest(http.MethodPost, "/api/monitors/"+created.ID+"/toggle", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 on toggle monitor, got %d", w.Code)
	}

	var toggled models.Monitor
	_ = json.Unmarshal(w.Body.Bytes(), &toggled)
	if toggled.Status != "paused" {
		t.Errorf("expected status paused after toggle, got %s", toggled.Status)
	}

	// 4. Delete monitor
	req, _ = http.NewRequest(http.MethodDelete, "/api/monitors/"+created.ID, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 on delete monitor, got %d", w.Code)
	}
}

func TestTelemetryAndDataEndpoints(t *testing.T) {
	_, router, token := setupTestRouter(t)

	endpoints := []string{
		"/api/stats",
		"/api/monitors",
		"/api/executions",
		"/api/executions/stats",
		"/api/executions/latency",
		"/api/executions/timeline",
		"/api/batchers",
		"/api/incidents",
		"/api/maintenance/stats",
		"/api/auth/me",
	}

	for _, ep := range endpoints {
		req, _ := http.NewRequest(http.MethodGet, ep, nil)
		req.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("expected 200 on %s, got %d (body: %s)", ep, w.Code, w.Body.String())
		}
	}
}

func TestExecutionLatencyEndpoints(t *testing.T) {
	apiService, router, token := setupTestRouter(t)

	// Create test monitor
	mon := apiService.store.CreateMonitor(models.Monitor{
		Name:            "Latency Test Endpoint",
		URL:             "https://example.com/latency",
		Method:          "GET",
		Interval:        "10s",
		IntervalSeconds: 10,
		Status:          "active",
		LastLatency:     120,
	})

	// Record sample pings
	apiService.store.RecordPingResult(models.ExecutionLog{
		ID:          "log-lat-1",
		MonitorID:   mon.ID,
		MonitorName: mon.Name,
		URL:         mon.URL,
		Status:      200,
		Latency:     100,
		OK:          true,
	})
	apiService.store.RecordPingResult(models.ExecutionLog{
		ID:          "log-lat-2",
		MonitorID:   mon.ID,
		MonitorName: mon.Name,
		URL:         mon.URL,
		Status:      200,
		Latency:     200,
		OK:          true,
	})

	// 1. Query /api/executions/latency?monitor_id=...
	req, _ := http.NewRequest(http.MethodGet, "/api/executions/latency?monitor_id="+mon.ID, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 on executions/latency, got %d", w.Code)
	}

	var stats models.LatencyStats
	if err := json.Unmarshal(w.Body.Bytes(), &stats); err != nil {
		t.Fatalf("failed to unmarshal latency stats: %v", err)
	}
	if stats.AvgLatencyMs != 150 {
		t.Errorf("expected avgLatencyMs 150, got %d", stats.AvgLatencyMs)
	}
	if stats.TotalSamples != 2 {
		t.Errorf("expected totalSamples 2, got %d", stats.TotalSamples)
	}

	// 2. Query /api/monitors/:id/latency
	req, _ = http.NewRequest(http.MethodGet, "/api/monitors/"+mon.ID+"/latency", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 on /monitors/:id/latency, got %d", w.Code)
	}

	var monStats models.LatencyStats
	if err := json.Unmarshal(w.Body.Bytes(), &monStats); err != nil {
		t.Fatalf("failed to unmarshal monitor latency stats: %v", err)
	}
	if monStats.AvgLatencyMs != 150 {
		t.Errorf("expected avgLatencyMs 150, got %d", monStats.AvgLatencyMs)
	}
}

func TestExecutionTimelineEndpoints(t *testing.T) {
	apiService, router, token := setupTestRouter(t)

	// Create test monitor
	mon := apiService.store.CreateMonitor(models.Monitor{
		Name:            "Timeline Test Endpoint",
		URL:             "https://example.com/timeline",
		Method:          "GET",
		Interval:        "10s",
		IntervalSeconds: 10,
		Status:          "active",
	})

	// Record sample pings
	for i := 1; i <= 5; i++ {
		apiService.store.RecordPingResult(models.ExecutionLog{
			ID:          fmt.Sprintf("log-tl-%d", i),
			MonitorID:   mon.ID,
			MonitorName: mon.Name,
			URL:         mon.URL,
			Status:      200,
			Latency:     int64(i * 10),
			OK:          true,
		})
	}

	// 1. Query /api/executions/timeline?monitor_id=...&limit=3
	req, _ := http.NewRequest(http.MethodGet, "/api/executions/timeline?monitor_id="+mon.ID+"&limit=3", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 on executions/timeline, got %d", w.Code)
	}

	var timelineResp models.TimelineResponse
	if err := json.Unmarshal(w.Body.Bytes(), &timelineResp); err != nil {
		t.Fatalf("failed to unmarshal timeline response: %v", err)
	}
	if timelineResp.TotalRecorded != 5 {
		t.Errorf("expected TotalRecorded 5, got %d", timelineResp.TotalRecorded)
	}
	if len(timelineResp.Timeline) != 3 {
		t.Errorf("expected 3 timeline items, got %d", len(timelineResp.Timeline))
	}

	// 2. Query /api/monitors/:id/timeline
	req, _ = http.NewRequest(http.MethodGet, "/api/monitors/"+mon.ID+"/timeline", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 on /monitors/:id/timeline, got %d", w.Code)
	}

	var monTimelineResp models.TimelineResponse
	if err := json.Unmarshal(w.Body.Bytes(), &monTimelineResp); err != nil {
		t.Fatalf("failed to unmarshal monitor timeline response: %v", err)
	}
	if monTimelineResp.TotalRecorded != 5 {
		t.Errorf("expected TotalRecorded 5, got %d", monTimelineResp.TotalRecorded)
	}
	if len(monTimelineResp.Timeline) != 5 {
		t.Errorf("expected 5 timeline items, got %d", len(monTimelineResp.Timeline))
	}
}

func TestDataManagementMaintenance(t *testing.T) {
	apiService, router, token := setupTestRouter(t)

	// Create monitor and logs
	mon := apiService.store.CreateMonitor(models.Monitor{
		Name:            "Maintenance Test Endpoint",
		URL:             "https://example.com/test",
		IntervalSeconds: 30,
		Status:          "active",
		TotalPings:      100,
		FailedPings:     10,
	})

	for i := 1; i <= 3; i++ {
		apiService.store.RecordPingResult(models.ExecutionLog{
			ID:          fmt.Sprintf("maint-log-%d", i),
			MonitorID:   mon.ID,
			MonitorName: mon.Name,
			URL:         mon.URL,
			Status:      200,
			Latency:     50,
			OK:          true,
		})
	}

	// 1. Check Maintenance Stats
	req, _ := http.NewRequest(http.MethodGet, "/api/maintenance/stats", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 on /api/maintenance/stats, got %d", w.Code)
	}

	var mStats models.MaintenanceStats
	if err := json.Unmarshal(w.Body.Bytes(), &mStats); err != nil {
		t.Fatalf("failed to decode maintenance stats: %v", err)
	}
	if mStats.TotalExecutionLogs < 3 {
		t.Errorf("expected at least 3 logs, got %d", mStats.TotalExecutionLogs)
	}

	// 2. Reset Monitor Counters
	req, _ = http.NewRequest(http.MethodPost, "/api/maintenance/reset-monitors?monitor_id="+mon.ID, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 on reset-monitors, got %d", w.Code)
	}

	updatedMon, ok := apiService.store.GetMonitor(mon.ID)
	if !ok || updatedMon.TotalPings != 0 {
		t.Errorf("expected monitor TotalPings to be 0 after reset, got %d", updatedMon.TotalPings)
	}

	// 3. Clear All Logs
	req, _ = http.NewRequest(http.MethodDelete, "/api/executions", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 on DELETE /api/executions, got %d", w.Code)
	}

	postLogs, _ := apiService.store.GetLogs(10, 0, "", "")
	if len(postLogs) != 0 {
		t.Errorf("expected 0 logs after clear, got %d", len(postLogs))
	}

	// 4. Test deleting marked incidents by IDs
	inc1 := apiService.store.CreateIncident(models.Incident{Title: "Outage 1", Status: "investigating"})
	inc2 := apiService.store.CreateIncident(models.Incident{Title: "Outage 2", Status: "investigating"})
	inc3 := apiService.store.CreateIncident(models.Incident{Title: "Outage 3", Status: "resolved"})

	req, _ = http.NewRequest(http.MethodDelete, fmt.Sprintf("/api/incidents?ids=%s,%s", inc1.ID, inc2.ID), nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 on DELETE /api/incidents?ids=..., got %d", w.Code)
	}

	if _, ok := apiService.store.GetIncident(inc1.ID); ok {
		t.Errorf("expected inc1 to be deleted")
	}
	if _, ok := apiService.store.GetIncident(inc2.ID); ok {
		t.Errorf("expected inc2 to be deleted")
	}
	if _, ok := apiService.store.GetIncident(inc3.ID); !ok {
		t.Errorf("expected inc3 to remain")
	}

	// 5. Test batch resolve via POST /api/incidents/batch-resolve
	inc4 := apiService.store.CreateIncident(models.Incident{Title: "Batch Resolve Test", Service: "Auth", Severity: "P1", Status: "investigating"})
	body, _ := json.Marshal(map[string][]string{"ids": {inc4.ID}})
	req, _ = http.NewRequest(http.MethodPost, "/api/incidents/batch-resolve", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 on POST /api/incidents/batch-resolve, got %d", w.Code)
	}
	if fetched, ok := apiService.store.GetIncident(inc4.ID); !ok || fetched.Status != "resolved" {
		t.Errorf("expected inc4 to be resolved, got %v", fetched)
	}

	// 6. Test batch delete via POST /api/incidents/batch-delete
	body, _ = json.Marshal(map[string][]string{"ids": {inc4.ID, inc3.ID}})
	req, _ = http.NewRequest(http.MethodPost, "/api/incidents/batch-delete", bytes.NewBuffer(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 on POST /api/incidents/batch-delete, got %d", w.Code)
	}

	// 7. Test deleting all remaining incidents
	req, _ = http.NewRequest(http.MethodDelete, "/api/incidents", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 on DELETE /api/incidents, got %d", w.Code)
	}
	if len(apiService.store.GetAllIncidents()) != 0 {
		t.Errorf("expected 0 incidents remaining, got %d", len(apiService.store.GetAllIncidents()))
	}
}

func BenchmarkGinMonitorsEndpoint(b *testing.B) {
	_, router, token := setupTestRouter(b)

	req, _ := http.NewRequest(http.MethodGet, "/api/monitors", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	
	for b.Loop() {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
	}
}
