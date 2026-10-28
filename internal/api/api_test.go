package api

import (
	"bytes"
	"encoding/json"
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
	scheduler := engine.NewScheduler(s, pool, prober)

	apiService := NewAPI(s, scheduler, pool)
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

	// 1. Create a monitor
	createBody := map[string]interface{}{
		"name":     "Pace Wisp Gateway",
		"url":      "https://example.com/api",
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
		"/api/batchers",
		"/api/incidents",
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

func BenchmarkGinMonitorsEndpoint(b *testing.B) {
	_, router, token := setupTestRouter(b)

	req, _ := http.NewRequest(http.MethodGet, "/api/monitors", nil)
	req.Header.Set("Authorization", "Bearer "+token)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
	}
}
