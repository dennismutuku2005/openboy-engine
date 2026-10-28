package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"openboy-engine/internal/engine"
	"openboy-engine/internal/models"
	"openboy-engine/internal/store"
	"openboy-engine/pkg/auth"
	"openboy-engine/pkg/logger"
)

// API holds dependencies for HTTP handlers.
type API struct {
	store     *store.Store
	scheduler *engine.Scheduler
	pool      *engine.WorkerPool
	auth      *AuthMiddleware
}

// NewAPI initializes the API service handler.
func NewAPI(s *store.Store, sc *engine.Scheduler, pool *engine.WorkerPool) *API {
	return &API{
		store:     s,
		scheduler: sc,
		pool:      pool,
		auth:      NewAuthMiddleware(s),
	}
}

func writeError(c *gin.Context, status int, message string) {
	c.JSON(status, gin.H{"error": message})
}

// ==========================================
// AUTHENTICATION HANDLERS
// ==========================================

// LoginHandler validates credentials and returns a JWT token.
func (a *API) LoginHandler(c *gin.Context) {
	var req models.LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, http.StatusBadRequest, "Invalid login payload")
		return
	}

	user, ok := a.store.GetUserByUsernameOrEmail(req.Username)
	if !ok {
		writeError(c, http.StatusUnauthorized, "Invalid username or password")
		return
	}

	if !auth.CheckPassword(req.Password, user.PasswordHash, user.PasswordSalt) {
		writeError(c, http.StatusUnauthorized, "Invalid username or password")
		return
	}

	userID := user.ID
	if userID == "" {
		userID = fmt.Sprintf("usr-%d", time.Now().Unix())
		user.ID = userID
	}

	token, err := auth.GenerateToken(auth.Claims{
		UserID:   userID,
		Username: user.Username,
		Email:    user.Email,
		Name:     user.Name,
		Role:     user.Role,
	})
	if err != nil {
		writeError(c, http.StatusInternalServerError, "Failed to generate authentication token")
		return
	}

	logger.Info("User logged in successfully: %s (%s)", user.Username, user.Email)
	c.JSON(http.StatusOK, models.LoginResponse{
		Token: token,
		User:  user.ToProfile(),
	})
}

// RegisterHandler registers a new account and returns a JWT token.
func (a *API) RegisterHandler(c *gin.Context) {
	var req models.RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, http.StatusBadRequest, "Invalid registration payload")
		return
	}

	if req.Username == "" || req.Email == "" || req.Password == "" {
		writeError(c, http.StatusBadRequest, "Username, email, and password are required")
		return
	}

	if len(req.Password) < 6 {
		writeError(c, http.StatusBadRequest, "Password must be at least 6 characters")
		return
	}

	name := req.Name
	if name == "" {
		name = req.Username
	}

	passHash, salt := auth.HashPassword(req.Password)
	apiKey := auth.GenerateAPIKey()

	newUser := models.User{
		Username:     req.Username,
		Email:        req.Email,
		PasswordHash: passHash,
		PasswordSalt: salt,
		Name:         name,
		Role:         "user",
		APIKey:       apiKey,
	}

	created, err := a.store.CreateUser(newUser)
	if err != nil {
		writeError(c, http.StatusConflict, err.Error())
		return
	}

	token, err := auth.GenerateToken(auth.Claims{
		UserID:   created.ID,
		Username: created.Username,
		Email:    created.Email,
		Name:     created.Name,
		Role:     created.Role,
	})
	if err != nil {
		writeError(c, http.StatusInternalServerError, "Account created, but token generation failed")
		return
	}

	logger.Info("New user registered: %s (%s)", created.Username, created.Email)
	c.JSON(http.StatusCreated, models.LoginResponse{
		Token: token,
		User:  created.ToProfile(),
	})
}

// MeHandler returns the authenticated user's profile.
func (a *API) MeHandler(c *gin.Context) {
	user, ok := GetUserFromContext(c)
	if !ok || user == nil {
		writeError(c, http.StatusUnauthorized, "Not authenticated")
		return
	}
	c.JSON(http.StatusOK, user.ToProfile())
}

// UpdateProfileHandler modifies user profile info (name, email).
func (a *API) UpdateProfileHandler(c *gin.Context) {
	user, ok := GetUserFromContext(c)
	if !ok || user == nil {
		writeError(c, http.StatusUnauthorized, "Not authenticated")
		return
	}

	var req models.UpdateProfileRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, http.StatusBadRequest, "Invalid payload")
		return
	}

	user.Name = req.Name
	user.Email = req.Email
	if !a.store.UpdateUser(*user) {
		writeError(c, http.StatusInternalServerError, "Failed to update profile")
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "Profile updated successfully",
		"user":    user.ToProfile(),
	})
}

// UpdatePasswordHandler verifies current password and saves new password.
func (a *API) UpdatePasswordHandler(c *gin.Context) {
	user, ok := GetUserFromContext(c)
	if !ok || user == nil {
		writeError(c, http.StatusUnauthorized, "Not authenticated")
		return
	}

	var req models.UpdatePasswordRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, http.StatusBadRequest, "Invalid payload")
		return
	}

	if !auth.CheckPassword(req.CurrentPassword, user.PasswordHash, user.PasswordSalt) {
		writeError(c, http.StatusBadRequest, "Current password is incorrect")
		return
	}

	if len(req.NewPassword) < 6 {
		writeError(c, http.StatusBadRequest, "New password must be at least 6 characters")
		return
	}

	newHash, newSalt := auth.HashPassword(req.NewPassword)
	if !a.store.UpdatePassword(user.ID, newHash, newSalt) {
		writeError(c, http.StatusInternalServerError, "Failed to update password")
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Password updated successfully"})
}

// ==========================================
// SYSTEM & HEALTH HANDLERS
// ==========================================

// HealthHandler returns engine runtime health.
func (a *API) HealthHandler(c *gin.Context) {
	stats := a.store.GetSystemStats(a.pool.ActiveWorkers())
	c.JSON(http.StatusOK, gin.H{
		"status":     "healthy",
		"service":    "openboy-engine",
		"version":    "1.0.0",
		"uptime":     stats.UptimeSeconds,
		"workers":    stats.ActiveWorkers,
		"goroutines": stats.Goroutines,
		"memoryMb":   fmt.Sprintf("%.2f MB", stats.MemoryAllocMB),
	})
}

// StatsHandler returns system-wide telemetry and KPI cards.
func (a *API) StatsHandler(c *gin.Context) {
	stats := a.store.GetSystemStats(a.pool.ActiveWorkers())
	c.JSON(http.StatusOK, stats)
}

// ==========================================
// MONITOR HANDLERS
// ==========================================

// ListMonitorsHandler returns all monitors, with optional search and category filters.
func (a *API) ListMonitorsHandler(c *gin.Context) {
	monitors := a.store.GetAllMonitors()

	category := strings.ToLower(strings.TrimSpace(c.Query("category")))
	search := strings.ToLower(strings.TrimSpace(c.Query("search")))

	if category != "" || search != "" {
		filtered := make([]models.Monitor, 0, len(monitors))
		for _, m := range monitors {
			if category != "" && strings.ToLower(m.Category) != category {
				continue
			}
			if search != "" {
				nameMatch := strings.Contains(strings.ToLower(m.Name), search)
				urlMatch := strings.Contains(strings.ToLower(m.URL), search)
				if !nameMatch && !urlMatch {
					continue
				}
			}
			filtered = append(filtered, m)
		}
		monitors = filtered
	}

	c.JSON(http.StatusOK, monitors)
}

// CreateMonitorHandler creates a new scheduled monitor.
func (a *API) CreateMonitorHandler(c *gin.Context) {
	var req models.CreateMonitorRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, http.StatusBadRequest, fmt.Sprintf("Invalid request payload: %v", err))
		return
	}

	if req.Name == "" || req.URL == "" {
		writeError(c, http.StatusBadRequest, "Name and URL are required")
		return
	}

	method := strings.ToUpper(req.Method)
	if method == "" {
		method = "GET"
	}

	intervalSeconds := req.IntervalSeconds
	if intervalSeconds <= 0 {
		switch req.Interval {
		case "10s":
			intervalSeconds = 10
		case "30s":
			intervalSeconds = 30
		case "1m":
			intervalSeconds = 60
		case "5m":
			intervalSeconds = 300
		case "10m":
			intervalSeconds = 600
		case "30m":
			intervalSeconds = 1800
		case "1h":
			intervalSeconds = 3600
		case "24h":
			intervalSeconds = 86400
		default:
			intervalSeconds = 30
		}
	}

	intervalLabel := req.IntervalLabel
	if intervalLabel == "" {
		intervalLabel = req.Interval
	}

	expectedStatus := req.ExpectedStatus
	if expectedStatus <= 0 {
		expectedStatus = 200
	}

	timeout := req.Timeout
	if timeout <= 0 {
		timeout = 10
	}

	category := req.Category
	if category == "" {
		category = "uptime"
	}

	notifications := "defaults"
	switch v := req.Notifications.(type) {
	case string:
		if strings.TrimSpace(v) != "" {
			notifications = v
		}
	case map[string]interface{}:
		b, _ := json.Marshal(v)
		notifications = string(b)
	}

	headersMap := make(map[string]string)
	if req.Headers != nil {
		switch v := req.Headers.(type) {
		case map[string]interface{}:
			for k, val := range v {
				headersMap[k] = fmt.Sprint(val)
			}
		case string:
			if strings.TrimSpace(v) != "" {
				lines := strings.Split(v, "\n")
				for _, line := range lines {
					parts := strings.SplitN(line, ":", 2)
					if len(parts) == 2 {
						headersMap[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
					}
				}
			}
		}
	}

	userID := ""
	if user, ok := GetUserFromContext(c); ok && user != nil {
		userID = user.ID
	}

	m := models.Monitor{
		UserID:          userID,
		Name:            req.Name,
		URL:             req.URL,
		Method:          method,
		Interval:        req.Interval,
		IntervalSeconds: intervalSeconds,
		IntervalLabel:   intervalLabel,
		CronExpr:        req.CronExpr,
		Status:          "active",
		Category:        category,
		Notifications:   notifications,
		Headers:         headersMap,
		Body:            req.Body,
		ExpectedStatus:  expectedStatus,
		Timeout:         timeout,
		LastStatus:      0,
		LastLatency:     0,
		Uptime:          "100%",
	}

	created := a.store.CreateMonitor(m)
	logger.Info("Created new monitor [%s] %s (%s)", created.ID, created.Name, created.URL)

	// Immediately probe once to establish baseline
	go func() {
		a.scheduler.TriggerPingNow(created.ID)
	}()

	c.JSON(http.StatusCreated, created)
}

// GetMonitorHandler returns a single monitor by ID.
func (a *API) GetMonitorHandler(c *gin.Context) {
	id := c.Param("id")
	m, ok := a.store.GetMonitor(id)
	if !ok {
		writeError(c, http.StatusNotFound, "Monitor not found")
		return
	}
	c.JSON(http.StatusOK, m)
}

// UpdateMonitorHandler updates a monitor completely.
func (a *API) UpdateMonitorHandler(c *gin.Context) {
	id := c.Param("id")
	var m models.Monitor
	if err := c.ShouldBindJSON(&m); err != nil {
		writeError(c, http.StatusBadRequest, "Invalid payload")
		return
	}
	m.ID = id
	if !a.store.UpdateMonitor(m) {
		writeError(c, http.StatusNotFound, "Monitor not found")
		return
	}
	logger.Info("Updated monitor [%s] %s", id, m.Name)
	c.JSON(http.StatusOK, m)
}

// DeleteMonitorHandler removes a monitor.
func (a *API) DeleteMonitorHandler(c *gin.Context) {
	id := c.Param("id")
	if !a.store.DeleteMonitor(id) {
		writeError(c, http.StatusNotFound, "Monitor not found")
		return
	}
	logger.Info("Deleted monitor [%s]", id)
	c.JSON(http.StatusOK, gin.H{"message": "Monitor deleted successfully"})
}

// ToggleMonitorHandler switches monitor between active and paused.
func (a *API) ToggleMonitorHandler(c *gin.Context) {
	id := c.Param("id")
	updated, ok := a.store.ToggleMonitorStatus(id)
	if !ok {
		writeError(c, http.StatusNotFound, "Monitor not found")
		return
	}
	logger.Info("Toggled monitor [%s] status to %s", id, updated.Status)
	c.JSON(http.StatusOK, updated)
}

// PingMonitorHandler triggers a manual immediate probe.
func (a *API) PingMonitorHandler(c *gin.Context) {
	id := c.Param("id")
	log, err := a.scheduler.TriggerPingNow(id)
	if err != nil {
		writeError(c, http.StatusNotFound, err.Error())
		return
	}
	c.JSON(http.StatusOK, log)
}

// ==========================================
// EXECUTION LOGS HANDLERS
// ==========================================

// ListExecutionsHandler returns execution logs with pagination, filter, search, and optional monitor ID.
func (a *API) ListExecutionsHandler(c *gin.Context) {
	limit := 50
	offset := 0
	filter := strings.ToLower(c.Query("filter"))
	search := c.Query("search")
	monitorID := c.Query("monitor_id")
	if monitorID == "" {
		monitorID = c.Query("monitor")
	}

	if l, err := strconv.Atoi(c.Query("limit")); err == nil && l > 0 {
		limit = l
	} else if monitorID != "" {
		limit = 500
	}
	if o, err := strconv.Atoi(c.Query("offset")); err == nil && o >= 0 {
		offset = o
	}

	logs, total := a.store.GetLogs(limit, offset, filter, search, monitorID)
	c.JSON(http.StatusOK, gin.H{
		"logs":   logs,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

// ClearExecutionsHandler clears all logs.
func (a *API) ClearExecutionsHandler(c *gin.Context) {
	count := a.store.ClearLogs()
	logger.Info("Cleared %d execution logs", count)
	c.JSON(http.StatusOK, gin.H{
		"message": "Execution logs cleared successfully",
		"cleared": count,
	})
}

// DeleteExecutionHandler deletes a single log.
func (a *API) DeleteExecutionHandler(c *gin.Context) {
	id := c.Param("id")
	if !a.store.DeleteLog(id) {
		writeError(c, http.StatusNotFound, "Log not found")
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Log deleted successfully"})
}

// ==========================================
// BATCHER HANDLERS
// ==========================================

// ListBatchersHandler returns all batch jobs.
func (a *API) ListBatchersHandler(c *gin.Context) {
	batchers := a.store.GetAllBatchers()
	c.JSON(http.StatusOK, batchers)
}

// CreateBatcherHandler creates a new batch worker job.
func (a *API) CreateBatcherHandler(c *gin.Context) {
	var req models.CreateBatcherRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, http.StatusBadRequest, "Invalid payload")
		return
	}

	if req.Name == "" || req.JobKey == "" || req.CronExpr == "" {
		writeError(c, http.StatusBadRequest, "Name, JobKey, and CronExpr are required")
		return
	}

	concurrency := req.Concurrency
	if concurrency == "" {
		concurrency = "Mutex Locked"
	}

	schedule := req.Schedule
	if schedule == "" {
		schedule = req.CronExpr
	}

	b := models.Batcher{
		Name:             req.Name,
		JobKey:           req.JobKey,
		Schedule:         schedule,
		CronExpr:         req.CronExpr,
		Status:           "active",
		Concurrency:      concurrency,
		RecordsProcessed: "0 records",
		Duration:         "0s",
		TargetQueue:      req.TargetQueue,
		Description:      req.Description,
	}

	created := a.store.CreateBatcher(b)
	logger.Info("Created batcher [%s] %s (%s)", created.ID, created.Name, created.JobKey)
	c.JSON(http.StatusCreated, created)
}

// GetBatcherHandler gets a single batcher.
func (a *API) GetBatcherHandler(c *gin.Context) {
	id := c.Param("id")
	b, ok := a.store.GetBatcher(id)
	if !ok {
		writeError(c, http.StatusNotFound, "Batcher not found")
		return
	}
	c.JSON(http.StatusOK, b)
}

// UpdateBatcherHandler updates an existing batcher.
func (a *API) UpdateBatcherHandler(c *gin.Context) {
	id := c.Param("id")
	var b models.Batcher
	if err := c.ShouldBindJSON(&b); err != nil {
		writeError(c, http.StatusBadRequest, "Invalid payload")
		return
	}
	b.ID = id
	if !a.store.UpdateBatcher(b) {
		writeError(c, http.StatusNotFound, "Batcher not found")
		return
	}
	c.JSON(http.StatusOK, b)
}

// DeleteBatcherHandler deletes a batcher.
func (a *API) DeleteBatcherHandler(c *gin.Context) {
	id := c.Param("id")
	if !a.store.DeleteBatcher(id) {
		writeError(c, http.StatusNotFound, "Batcher not found")
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Batcher deleted successfully"})
}

// TriggerBatcherHandler runs a batch worker manually.
func (a *API) TriggerBatcherHandler(c *gin.Context) {
	id := c.Param("id")
	b, ok := a.store.TriggerBatcher(id)
	if !ok {
		writeError(c, http.StatusNotFound, "Batcher not found")
		return
	}
	logger.Info("Manually triggered batch worker: [%s] %s", id, b.Name)
	c.JSON(http.StatusOK, b)
}

// ==========================================
// INCIDENT HANDLERS
// ==========================================

// ListIncidentsHandler returns all incidents.
func (a *API) ListIncidentsHandler(c *gin.Context) {
	incidents := a.store.GetAllIncidents()
	c.JSON(http.StatusOK, incidents)
}

// CreateIncidentHandler declares a new incident.
func (a *API) CreateIncidentHandler(c *gin.Context) {
	var req models.CreateIncidentRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		writeError(c, http.StatusBadRequest, "Invalid payload")
		return
	}

	if req.Title == "" || req.Service == "" {
		writeError(c, http.StatusBadRequest, "Title and Service are required")
		return
	}

	sev := req.Severity
	if sev == "" {
		sev = "P2 - High"
	}

	inc := models.Incident{
		MonitorID:    req.MonitorID,
		Title:        req.Title,
		Service:      req.Service,
		URL:          req.URL,
		Severity:     sev,
		Status:       "investigating",
		Cause:        req.Cause,
		DispatchedTo: req.DispatchedTo,
	}

	created := a.store.CreateIncident(inc)
	logger.Warn("Declared incident [%s] %s: %s", created.ID, created.Severity, created.Title)
	c.JSON(http.StatusCreated, created)
}

// GetIncidentHandler gets a single incident.
func (a *API) GetIncidentHandler(c *gin.Context) {
	id := c.Param("id")
	inc, ok := a.store.GetIncident(id)
	if !ok {
		writeError(c, http.StatusNotFound, "Incident not found")
		return
	}
	c.JSON(http.StatusOK, inc)
}

// UpdateIncidentHandler updates an incident.
func (a *API) UpdateIncidentHandler(c *gin.Context) {
	id := c.Param("id")
	var inc models.Incident
	if err := c.ShouldBindJSON(&inc); err != nil {
		writeError(c, http.StatusBadRequest, "Invalid payload")
		return
	}
	inc.ID = id
	if !a.store.UpdateIncident(inc) {
		writeError(c, http.StatusNotFound, "Incident not found")
		return
	}
	c.JSON(http.StatusOK, inc)
}

// ResolveIncidentHandler marks an incident resolved.
func (a *API) ResolveIncidentHandler(c *gin.Context) {
	id := c.Param("id")
	inc, ok := a.store.ResolveIncident(id)
	if !ok {
		writeError(c, http.StatusNotFound, "Incident not found")
		return
	}
	logger.Info("Resolved incident [%s] %s", id, inc.Title)
	c.JSON(http.StatusOK, inc)
}

// DeleteIncidentHandler deletes an incident.
func (a *API) DeleteIncidentHandler(c *gin.Context) {
	id := c.Param("id")
	if !a.store.DeleteIncident(id) {
		writeError(c, http.StatusNotFound, "Incident not found")
		return
	}
	c.JSON(http.StatusOK, gin.H{"message": "Incident deleted successfully"})
}
