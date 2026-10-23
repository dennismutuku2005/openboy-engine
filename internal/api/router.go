package api

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"openboy-engine/pkg/logger"
)

// Router maps incoming HTTP requests to handlers with CORS and recovery.
type Router struct {
	api       *API
	staticDir string
}

// NewRouter constructs a router instance with optional static UI directory.
func NewRouter(api *API, staticDir ...string) *Router {
	r := &Router{api: api}
	if len(staticDir) > 0 && staticDir[0] != "" {
		if fi, err := os.Stat(staticDir[0]); err == nil && fi.IsDir() {
			r.staticDir = staticDir[0]
		}
	}
	return r
}

// ServeHTTP handles requests, CORS headers, error recovery and routing.
func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Enable CORS for dashboard and external clients
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With, Cache-Control, X-API-Key")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	start := time.Now()

	// Panic recovery wrapper
	defer func() {
		if rec := recover(); rec != nil {
			logger.Error("PANIC in HTTP handler: %v\n%s", rec, debug.Stack())
			writeError(w, http.StatusInternalServerError, "Internal server error")
		}
	}()

	path := strings.TrimSuffix(r.URL.Path, "/")
	if path == "" {
		path = "/"
	}

	// Route dispatch
	switch {
	case path == "" || path == "/":
		if rt.staticDir != "" {
			indexPath := filepath.Join(rt.staticDir, "index.html")
			if _, err := os.Stat(indexPath); err == nil {
				http.ServeFile(w, r, indexPath)
				return
			}
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"service": "OpenBoy Cron & Health Engine",
			"status":  "online",
			"version": "1.0.0",
			"endpoints": []string{
				"POST /api/auth/login",
				"POST /api/auth/register",
				"GET  /api/auth/me",
				"PUT  /api/auth/profile",
				"PUT  /api/auth/password",
				"GET  /api/health",
				"GET  /api/stats",
				"GET  /api/stream",
				"GET  /api/monitors",
				"POST /api/monitors",
				"GET  /api/monitors/{id}",
				"PUT  /api/monitors/{id}",
				"DELETE /api/monitors/{id}",
				"POST /api/monitors/{id}/ping",
				"POST /api/monitors/{id}/toggle",
				"GET  /api/executions",
				"DELETE /api/executions",
				"DELETE /api/executions/{id}",
				"GET  /api/batchers",
				"POST /api/batchers",
				"GET  /api/batchers/{id}",
				"PUT  /api/batchers/{id}",
				"DELETE /api/batchers/{id}",
				"POST /api/batchers/{id}/run",
				"GET  /api/incidents",
				"POST /api/incidents",
				"GET  /api/incidents/{id}",
				"PUT  /api/incidents/{id}",
				"POST /api/incidents/{id}/resolve",
				"DELETE /api/incidents/{id}",
			},
		})

	// Auth Endpoints
	case path == "/api/auth/login" && r.Method == http.MethodPost:
		rt.api.LoginHandler(w, r)

	case path == "/api/auth/register" && r.Method == http.MethodPost:
		rt.api.RegisterHandler(w, r)

	case path == "/api/auth/me" && r.Method == http.MethodGet:
		rt.api.auth.RequireAuth(rt.api.MeHandler)(w, r)

	case path == "/api/auth/profile" && r.Method == http.MethodPut:
		rt.api.auth.RequireAuth(rt.api.UpdateProfileHandler)(w, r)

	case path == "/api/auth/password" && r.Method == http.MethodPut:
		rt.api.auth.RequireAuth(rt.api.UpdatePasswordHandler)(w, r)

	// Health (Public probe)
	case path == "/api/health" && r.Method == http.MethodGet:
		rt.api.HealthHandler(w, r)

	// Protected Telemetry & SSE Stream
	case path == "/api/stats" && r.Method == http.MethodGet:
		rt.api.auth.RequireAuth(rt.api.StatsHandler)(w, r)

	case path == "/api/stream" && r.Method == http.MethodGet:
		rt.api.StreamHandler(w, r)

	// Protected Monitors
	case path == "/api/monitors" && r.Method == http.MethodGet:
		rt.api.auth.RequireAuth(rt.api.ListMonitorsHandler)(w, r)

	case path == "/api/monitors" && r.Method == http.MethodPost:
		rt.api.auth.RequireAuth(rt.api.CreateMonitorHandler)(w, r)

	case strings.HasPrefix(path, "/api/monitors/"):
		rt.api.auth.RequireAuth(func(w http.ResponseWriter, r *http.Request) {
			rt.routeMonitorSubpath(w, r, path[len("/api/monitors/"):])
		})(w, r)

	// Protected Executions
	case path == "/api/executions" && r.Method == http.MethodGet:
		rt.api.auth.RequireAuth(rt.api.ListExecutionsHandler)(w, r)

	case path == "/api/executions" && r.Method == http.MethodDelete:
		rt.api.auth.RequireAuth(rt.api.ClearExecutionsHandler)(w, r)

	case strings.HasPrefix(path, "/api/executions/"):
		rt.api.auth.RequireAuth(func(w http.ResponseWriter, r *http.Request) {
			rt.routeExecutionSubpath(w, r, path[len("/api/executions/"):])
		})(w, r)

	// Protected Batchers
	case path == "/api/batchers" && r.Method == http.MethodGet:
		rt.api.auth.RequireAuth(rt.api.ListBatchersHandler)(w, r)

	case path == "/api/batchers" && r.Method == http.MethodPost:
		rt.api.auth.RequireAuth(rt.api.CreateBatcherHandler)(w, r)

	case strings.HasPrefix(path, "/api/batchers/"):
		rt.api.auth.RequireAuth(func(w http.ResponseWriter, r *http.Request) {
			rt.routeBatcherSubpath(w, r, path[len("/api/batchers/"):])
		})(w, r)

	// Protected Incidents
	case path == "/api/incidents" && r.Method == http.MethodGet:
		rt.api.auth.RequireAuth(rt.api.ListIncidentsHandler)(w, r)

	case path == "/api/incidents" && r.Method == http.MethodPost:
		rt.api.auth.RequireAuth(rt.api.CreateIncidentHandler)(w, r)

	case strings.HasPrefix(path, "/api/incidents/"):
		rt.api.auth.RequireAuth(func(w http.ResponseWriter, r *http.Request) {
			rt.routeIncidentSubpath(w, r, path[len("/api/incidents/"):])
		})(w, r)

	default:
		if rt.staticDir != "" && !strings.HasPrefix(path, "/api") {
			cleanPath := strings.TrimPrefix(r.URL.Path, "/")
			fullPath := filepath.Join(rt.staticDir, filepath.FromSlash(cleanPath))
			if fi, err := os.Stat(fullPath); err == nil {
				if fi.IsDir() {
					dirIndex := filepath.Join(fullPath, "index.html")
					if _, err := os.Stat(dirIndex); err == nil {
						http.ServeFile(w, r, dirIndex)
						return
					}
				} else {
					http.ServeFile(w, r, fullPath)
					return
				}
			}
			if fi, err := os.Stat(fullPath + ".html"); err == nil && !fi.IsDir() {
				http.ServeFile(w, r, fullPath+".html")
				return
			}
			dirIndex := filepath.Join(fullPath, "index.html")
			if fi, err := os.Stat(dirIndex); err == nil && !fi.IsDir() {
				http.ServeFile(w, r, dirIndex)
				return
			}
			spaIndex := filepath.Join(rt.staticDir, "index.html")
			if _, err := os.Stat(spaIndex); err == nil {
				http.ServeFile(w, r, spaIndex)
				return
			}
		}
		writeError(w, http.StatusNotFound, fmt.Sprintf("Route not found: %s %s", r.Method, path))
	}

	duration := time.Since(start)
	if path != "/api/stream" && path != "/api/health" {
		logger.Info("%s %s (%v)", r.Method, path, duration)
	}
}

func (rt *Router) routeMonitorSubpath(w http.ResponseWriter, r *http.Request, subpath string) {
	parts := strings.Split(subpath, "/")
	id := parts[0]

	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			rt.api.GetMonitorHandler(w, r, id)
		case http.MethodPut:
			rt.api.UpdateMonitorHandler(w, r, id)
		case http.MethodDelete:
			rt.api.DeleteMonitorHandler(w, r, id)
		default:
			writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		}
		return
	}

	if len(parts) == 2 {
		action := parts[1]
		switch {
		case action == "toggle" && r.Method == http.MethodPost:
			rt.api.ToggleMonitorHandler(w, r, id)
		case action == "ping" && r.Method == http.MethodPost:
			rt.api.PingMonitorHandler(w, r, id)
		default:
			writeError(w, http.StatusNotFound, "Monitor action not found")
		}
		return
	}

	writeError(w, http.StatusNotFound, "Not found")
}

func (rt *Router) routeExecutionSubpath(w http.ResponseWriter, r *http.Request, subpath string) {
	parts := strings.Split(subpath, "/")
	id := parts[0]

	if len(parts) == 1 && r.Method == http.MethodDelete {
		rt.api.DeleteExecutionHandler(w, r, id)
		return
	}
	writeError(w, http.StatusNotFound, "Execution route not found")
}

func (rt *Router) routeBatcherSubpath(w http.ResponseWriter, r *http.Request, subpath string) {
	parts := strings.Split(subpath, "/")
	id := parts[0]

	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			rt.api.GetBatcherHandler(w, r, id)
		case http.MethodPut:
			rt.api.UpdateBatcherHandler(w, r, id)
		case http.MethodDelete:
			rt.api.DeleteBatcherHandler(w, r, id)
		default:
			writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		}
		return
	}

	if len(parts) == 2 && parts[1] == "run" && r.Method == http.MethodPost {
		rt.api.TriggerBatcherHandler(w, r, id)
		return
	}
	writeError(w, http.StatusNotFound, "Batcher action not found")
}

func (rt *Router) routeIncidentSubpath(w http.ResponseWriter, r *http.Request, subpath string) {
	parts := strings.Split(subpath, "/")
	id := parts[0]

	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			rt.api.GetIncidentHandler(w, r, id)
		case http.MethodPut:
			rt.api.UpdateIncidentHandler(w, r, id)
		case http.MethodDelete:
			rt.api.DeleteIncidentHandler(w, r, id)
		default:
			writeError(w, http.StatusMethodNotAllowed, "Method not allowed")
		}
		return
	}

	if len(parts) == 2 && parts[1] == "resolve" && r.Method == http.MethodPost {
		rt.api.ResolveIncidentHandler(w, r, id)
		return
	}
	writeError(w, http.StatusNotFound, "Incident action not found")
}
