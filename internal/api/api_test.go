package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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

	// 1. Unauthenticated request should be rejected with 401 Unauthorized
	unauthResp, err := http.Get(server.URL + "/api/monitors")
	if err != nil {
		t.Fatalf("Failed GET /api/monitors: %v", err)
	}
	defer unauthResp.Body.Close()

	if unauthResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("Expected 401 Unauthorized without JWT, got %d", unauthResp.StatusCode)
	}

	// 2. Obtain valid JWT token via login
	loginBody, _ := json.Marshal(map[string]string{
		"username": "admin",
		"password": "admin",
	})
	loginResp, err := http.Post(server.URL+"/api/auth/login", "application/json", bytes.NewReader(loginBody))
	if err != nil {
		t.Fatalf("Failed login: %v", err)
	}
	defer loginResp.Body.Close()

	var authData models.LoginResponse
	json.NewDecoder(loginResp.Body).Decode(&authData)
	token := authData.Token
	client := &http.Client{}

	// 3. Authorized request with Bearer token
	req, _ := http.NewRequest("GET", server.URL+"/api/monitors", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Failed GET /api/monitors with auth: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 OK with JWT, got %d", resp.StatusCode)
	}

	var monitors []models.Monitor
	json.NewDecoder(resp.Body).Decode(&monitors)

	var firstID string
	if len(monitors) == 0 {
		createReq, _ := http.NewRequest("POST", server.URL+"/api/monitors", strings.NewReader(`{"name":"Test API","url":"https://api.github.com","method":"GET","intervalSeconds":30}`))
		createReq.Header.Set("Authorization", "Bearer "+token)
		createReq.Header.Set("Content-Type", "application/json")
		createResp, err := client.Do(createReq)
		if err != nil {
			t.Fatalf("Failed to create monitor: %v", err)
		}
		var created models.Monitor
		json.NewDecoder(createResp.Body).Decode(&created)
		createResp.Body.Close()
		firstID = created.ID
	} else {
		firstID = monitors[0].ID
	}

	// Ping monitor with Bearer token
	pingReq, _ := http.NewRequest("POST", server.URL+"/api/monitors/"+firstID+"/ping", nil)
	pingReq.Header.Set("Authorization", "Bearer "+token)
	pingResp, err := client.Do(pingReq)
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

	// Toggle monitor with Bearer token
	toggleReq, _ := http.NewRequest("POST", server.URL+"/api/monitors/"+firstID+"/toggle", nil)
	toggleReq.Header.Set("Authorization", "Bearer "+token)
	toggleResp, err := client.Do(toggleReq)
	if err != nil {
		t.Fatalf("Failed toggle: %v", err)
	}
	defer toggleResp.Body.Close()
	if toggleResp.StatusCode != http.StatusOK {
		t.Fatalf("Expected 200 OK for toggle, got %d", toggleResp.StatusCode)
	}
}

func TestStaticUIServing(t *testing.T) {
	// Create a temporary static directory
	tmpDir := t.TempDir()
	indexFile := tmpDir + "/index.html"
	_ = os.WriteFile(indexFile, []byte("<h1>OpenBoy Test UI</h1>"), 0644)

	s := store.NewStore("")
	prober := engine.NewProber()
	pool := engine.NewWorkerPool(1, s, prober)
	scheduler := engine.NewScheduler(s, pool, prober)
	apiService := NewAPI(s, scheduler, pool)
	router := NewRouter(apiService, tmpDir)

	server := httptest.NewServer(router)
	defer server.Close()

	// 1. Root / should serve index.html
	resp, err := http.Get(server.URL + "/")
	if err != nil {
		t.Fatalf("Failed to GET /: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200 for static root, got %d", resp.StatusCode)
	}

	// 2. /api/health should still hit API handler
	healthResp, err := http.Get(server.URL + "/api/health")
	if err != nil {
		t.Fatalf("Failed to GET /api/health: %v", err)
	}
	defer healthResp.Body.Close()
	if healthResp.StatusCode != http.StatusOK {
		t.Errorf("Expected 200 for /api/health, got %d", healthResp.StatusCode)
	}
}

