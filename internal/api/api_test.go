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
)

func setupTestServer() (*httptest.Server, *store.Store) {
	s := store.NewStore("")
	prober := engine.NewProber()
	pool := engine.NewWorkerPool(2, s, prober)
	scheduler := engine.NewScheduler(s, pool, prober)

	apiService := NewAPI(s, scheduler, pool)
	router := NewRouter(apiService)

	return httptest.NewServer(router), s
}

func TestHealthEndpoint(t *testing.T) {
	server, _ := setupTestServer()
	defer server.Close()

	resp, err := http.Get(server.URL + "/api/health")
	if err != nil {
		t.Fatalf("Failed to GET /api/health: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 OK, got %d", resp.StatusCode)
	}

	var data map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		t.Fatalf("Failed to parse json: %v", err)
	}

	if data["status"] != "healthy" {
		t.Errorf("Expected status 'healthy', got %v", data["status"])
	}
}

func TestAuthAndProtectedRoutes(t *testing.T) {
	server, _ := setupTestServer()
	defer server.Close()

	// 1. Login with seeded admin credentials
	loginBody, _ := json.Marshal(map[string]string{
		"username": "admin",
		"password": "admin",
	})
	resp, err := http.Post(server.URL+"/api/auth/login", "application/json", bytes.NewReader(loginBody))
	if err != nil {
		t.Fatalf("Failed to post login: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Login failed with code %d", resp.StatusCode)
	}

	var loginResp models.LoginResponse
	if err := json.NewDecoder(resp.Body).Decode(&loginResp); err != nil {
		t.Fatalf("Failed to decode login response: %v", err)
	}

	if loginResp.Token == "" {
		t.Fatalf("Expected non-empty JWT token")
	}

	// 2. Access /api/auth/me with Bearer token
	req, _ := http.NewRequest("GET", server.URL+"/api/auth/me", nil)
	req.Header.Set("Authorization", "Bearer "+loginResp.Token)
	client := &http.Client{}
	meResp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Failed GET /api/auth/me: %v", err)
	}
	defer meResp.Body.Close()

	if meResp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 OK for /api/auth/me, got %d", meResp.StatusCode)
	}

	var profile models.UserProfile
	json.NewDecoder(meResp.Body).Decode(&profile)
	if profile.Username != "admin" {
		t.Errorf("Expected profile username 'admin', got '%s'", profile.Username)
	}
}

func TestMonitorsAPI(t *testing.T) {
	server, _ := setupTestServer()
	defer server.Close()

	// List monitors
	resp, err := http.Get(server.URL + "/api/monitors")
	if err != nil {
		t.Fatalf("Failed GET /api/monitors: %v", err)
	}
	defer resp.Body.Close()

	var monitors []models.Monitor
	json.NewDecoder(resp.Body).Decode(&monitors)
	if len(monitors) == 0 {
		t.Fatalf("Expected seeded monitors list to not be empty")
	}

	firstID := monitors[0].ID

	// Ping monitor
	pingResp, err := http.Post(server.URL+"/api/monitors/"+firstID+"/ping", "application/json", nil)
	if err != nil {
		t.Fatalf("Failed POST /api/monitors/%s/ping: %v", firstID, err)
	}
	defer pingResp.Body.Close()

	if pingResp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 OK for ping, got %d", pingResp.StatusCode)
	}

	var pingLog models.ExecutionLog
	json.NewDecoder(pingResp.Body).Decode(&pingLog)
	if pingLog.MonitorID != firstID {
		t.Errorf("Ping log monitor ID mismatch: got %s, expected %s", pingLog.MonitorID, firstID)
	}

	// Toggle monitor
	toggleResp, err := http.Post(server.URL+"/api/monitors/"+firstID+"/toggle", "application/json", nil)
	if err != nil {
		t.Fatalf("Failed toggle: %v", err)
	}
	defer toggleResp.Body.Close()
	if toggleResp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 OK for toggle, got %d", toggleResp.StatusCode)
	}
}
