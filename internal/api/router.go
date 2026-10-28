package api

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"openboy-engine/pkg/logger"
)

// NewRouter constructs a high-performance Gin engine router with CORS, recovery, logging, and static asset fallback.
func NewRouter(apiService *API, staticDir ...string) *gin.Engine {
	// Set Gin mode to release to maximize throughput and disable debug noise
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()

	var resolvedStaticDir string
	if len(staticDir) > 0 && staticDir[0] != "" {
		if fi, err := os.Stat(staticDir[0]); err == nil && fi.IsDir() {
			resolvedStaticDir = staticDir[0]
		}
	}

	// 1. Global Panic Recovery Middleware
	r.Use(func(c *gin.Context) {
		defer func() {
			if rec := recover(); rec != nil {
				logger.Error("PANIC in HTTP handler: %v\n%s", rec, debug.Stack())
				c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
					"error": "Internal server error",
				})
			}
		}()
		c.Next()
	})

	// 2. Global CORS & Anti-Caching Middleware
	r.Use(func(c *gin.Context) {
		c.Header("Access-Control-Allow-Origin", "*")
		c.Header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		c.Header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With, Cache-Control, X-API-Key, Pragma")
		c.Header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
		c.Header("Pragma", "no-cache")
		c.Header("Expires", "0")

		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	})

	// 3. Global Access Logger Middleware (excludes continuous telemetry endpoints from flooding logs)
	r.Use(func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path

		c.Next()

		if path != "/api/stream" && path != "/api/health" {
			latency := time.Since(start)
			logger.Info("%s %s (%v) -> %d", c.Request.Method, path, latency, c.Writer.Status())
		}
	})

	// 4. Root Endpoint: Static dashboard index or API service discovery
	r.GET("/", func(c *gin.Context) {
		if resolvedStaticDir != "" {
			indexPath := filepath.Join(resolvedStaticDir, "index.html")
			if _, err := os.Stat(indexPath); err == nil {
				c.File(indexPath)
				return
			}
		}
		c.JSON(http.StatusOK, gin.H{
			"service":   "OpenBoy Cron & Health Engine",
			"status":    "online",
			"framework": "Gin",
			"version":   "1.0.0",
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
				"GET  /api/monitors/:id",
				"PUT  /api/monitors/:id",
				"DELETE /api/monitors/:id",
				"POST /api/monitors/:id/ping",
				"POST /api/monitors/:id/toggle",
				"GET  /api/executions",
				"DELETE /api/executions",
				"DELETE /api/executions/:id",
				"GET  /api/batchers",
				"POST /api/batchers",
				"GET  /api/batchers/:id",
				"PUT  /api/batchers/:id",
				"DELETE /api/batchers/:id",
				"POST /api/batchers/:id/run",
				"GET  /api/incidents",
				"POST /api/incidents",
				"GET  /api/incidents/:id",
				"PUT  /api/incidents/:id",
				"POST /api/incidents/:id/resolve",
				"DELETE /api/incidents/:id",
			},
		})
	})

	// 5. API Route Group
	apiGroup := r.Group("/api")
	{
		// Public Auth
		apiGroup.POST("/auth/login", apiService.LoginHandler)
		apiGroup.POST("/auth/register", apiService.RegisterHandler)

		// Public Health & Streaming
		apiGroup.GET("/health", apiService.HealthHandler)
		apiGroup.GET("/stream", apiService.StreamHandler)

		// Protected Routes
		protected := apiGroup.Group("")
		protected.Use(apiService.auth.RequireAuth())
		{
			// User Profile & Password
			protected.GET("/auth/me", apiService.MeHandler)
			protected.PUT("/auth/profile", apiService.UpdateProfileHandler)
			protected.PUT("/auth/password", apiService.UpdatePasswordHandler)

			// Telemetry & KPI Cards
			protected.GET("/stats", apiService.StatsHandler)

			// Monitors
			protected.GET("/monitors", apiService.ListMonitorsHandler)
			protected.POST("/monitors", apiService.CreateMonitorHandler)
			protected.GET("/monitors/:id", apiService.GetMonitorHandler)
			protected.PUT("/monitors/:id", apiService.UpdateMonitorHandler)
			protected.DELETE("/monitors/:id", apiService.DeleteMonitorHandler)
			protected.POST("/monitors/:id/toggle", apiService.ToggleMonitorHandler)
			protected.POST("/monitors/:id/ping", apiService.PingMonitorHandler)

			// Executions
			protected.GET("/executions", apiService.ListExecutionsHandler)
			protected.DELETE("/executions", apiService.ClearExecutionsHandler)
			protected.DELETE("/executions/:id", apiService.DeleteExecutionHandler)

			// Batchers
			protected.GET("/batchers", apiService.ListBatchersHandler)
			protected.POST("/batchers", apiService.CreateBatcherHandler)
			protected.GET("/batchers/:id", apiService.GetBatcherHandler)
			protected.PUT("/batchers/:id", apiService.UpdateBatcherHandler)
			protected.DELETE("/batchers/:id", apiService.DeleteBatcherHandler)
			protected.POST("/batchers/:id/run", apiService.TriggerBatcherHandler)

			// Incidents
			protected.GET("/incidents", apiService.ListIncidentsHandler)
			protected.POST("/incidents", apiService.CreateIncidentHandler)
			protected.GET("/incidents/:id", apiService.GetIncidentHandler)
			protected.PUT("/incidents/:id", apiService.UpdateIncidentHandler)
			protected.POST("/incidents/:id/resolve", apiService.ResolveIncidentHandler)
			protected.DELETE("/incidents/:id", apiService.DeleteIncidentHandler)
		}
	}

	// 6. NoRoute: Static file serving & SPA fallback
	r.NoRoute(func(c *gin.Context) {
		path := c.Request.URL.Path
		if resolvedStaticDir != "" && !strings.HasPrefix(path, "/api") {
			cleanPath := strings.TrimPrefix(path, "/")
			fullPath := filepath.Join(resolvedStaticDir, filepath.FromSlash(cleanPath))
			if fi, err := os.Stat(fullPath); err == nil {
				if fi.IsDir() {
					dirIndex := filepath.Join(fullPath, "index.html")
					if _, err := os.Stat(dirIndex); err == nil {
						c.File(dirIndex)
						return
					}
				} else {
					c.File(fullPath)
					return
				}
			}
			if fi, err := os.Stat(fullPath + ".html"); err == nil && !fi.IsDir() {
				c.File(fullPath + ".html")
				return
			}
			dirIndex := filepath.Join(fullPath, "index.html")
			if fi, err := os.Stat(dirIndex); err == nil && !fi.IsDir() {
				c.File(dirIndex)
				return
			}
			spaIndex := filepath.Join(resolvedStaticDir, "index.html")
			if _, err := os.Stat(spaIndex); err == nil {
				c.File(spaIndex)
				return
			}
		}

		c.JSON(http.StatusNotFound, gin.H{
			"error": fmt.Sprintf("Route not found: %s %s", c.Request.Method, path),
		})
	})

	return r
}
