package models

import "time"

// User represents an authenticated account in the OpenBoy ecosystem.
type User struct {
	ID           string    `json:"id"`
	Username     string    `json:"username"`
	Email        string    `json:"email"`
	PasswordHash string    `json:"-"`
	PasswordSalt string    `json:"-"`
	Name         string    `json:"name"`
	Role         string    `json:"role"` // "admin", "operator", "viewer"
	APIKey       string    `json:"apiKey"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

// UserProfile represents the public user info returned to API clients.
type UserProfile struct {
	ID        string    `json:"id"`
	Username  string    `json:"username"`
	Email     string    `json:"email"`
	Name      string    `json:"name"`
	Role      string    `json:"role"`
	APIKey    string    `json:"apiKey"`
	CreatedAt time.Time `json:"createdAt"`
}

// ToProfile converts a User to a UserProfile safe for JSON serialization.
func (u *User) ToProfile() UserProfile {
	return UserProfile{
		ID:        u.ID,
		Username:  u.Username,
		Email:     u.Email,
		Name:      u.Name,
		Role:      u.Role,
		APIKey:    u.APIKey,
		CreatedAt: u.CreatedAt,
	}
}

// LoginRequest payload for authenticating.
type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// LoginResponse payload on successful authentication.
type LoginResponse struct {
	Token string      `json:"token"`
	User  UserProfile `json:"user"`
}

// RegisterRequest payload for signing up.
type RegisterRequest struct {
	Username string `json:"username"`
	Email    string `json:"email"`
	Password string `json:"password"`
	Name     string `json:"name"`
}

// UpdateProfileRequest payload for modifying user details.
type UpdateProfileRequest struct {
	Name  string `json:"name,omitempty"`
	Email string `json:"email,omitempty"`
}

// UpdatePasswordRequest payload for changing credentials.
type UpdatePasswordRequest struct {
	CurrentPassword string `json:"currentPassword"`
	NewPassword     string `json:"newPassword"`
}

// HistoryItem represents a historical datapoint for latency sparks.
type HistoryItem struct {
	Status  int    `json:"status"`
	Latency int64  `json:"latency"`
	Time    string `json:"time"`
	OK      bool   `json:"ok"`
}

// Monitor represents an orchestrated cron schedule or health ping endpoint.
type Monitor struct {
	ID               string            `json:"id"`
	UserID           string            `json:"userId,omitempty"`
	Name             string            `json:"name"`
	URL              string            `json:"url"`
	Method           string            `json:"method"`
	Interval         string            `json:"interval"` // "10s", "30s", "1m", "5m", "10m", "30m", "1h", "24h", "custom_seconds", "cron"
	IntervalSeconds  int               `json:"intervalSeconds"`
	IntervalLabel    string            `json:"intervalLabel"`
	CronExpr         string            `json:"cronExpr,omitempty"`
	Status           string            `json:"status"` // "active", "paused", "failing"
	Category         string            `json:"category,omitempty"`
	Notifications    string            `json:"notifications,omitempty"`
	Headers          map[string]string `json:"headers,omitempty"`
	Body             string            `json:"body,omitempty"`
	ExpectedStatus   int               `json:"expectedStatus"`
	Timeout          int               `json:"timeout"` // in seconds
	LastStatus       int               `json:"lastStatus"`
	LastLatency      int64             `json:"lastLatency"` // in ms
	LastChecked      *time.Time        `json:"lastChecked,omitempty"`
	LastCheckedStr   string            `json:"lastCheckedStr,omitempty"`
	Uptime           string            `json:"uptime"`
	TotalPings       int64             `json:"totalPings"`
	FailedPings      int64             `json:"failedPings"`
	ConsecutiveFails int               `json:"consecutiveFails"`
	History          []HistoryItem     `json:"history,omitempty"`
	NextRun          *time.Time        `json:"nextRun,omitempty"`
	CreatedAt        time.Time         `json:"createdAt"`
	UpdatedAt        time.Time         `json:"updatedAt"`
}

// ExecutionLog represents a single recorded ping dispatch and audit trace.
type ExecutionLog struct {
	ID            string    `json:"id"`
	MonitorID     string    `json:"monitorId"`
	MonitorName   string    `json:"monitorName"`
	URL           string    `json:"url"`
	Method        string    `json:"method"`
	Status        int       `json:"status"`
	StatusText    string    `json:"statusText"`
	Latency       int64     `json:"latency"` // in milliseconds
	Timestamp     time.Time `json:"timestamp"`
	FormattedTime string    `json:"formattedTime"`
	OK            bool      `json:"ok"`
	Error         string    `json:"error,omitempty"`
	ResponseBody  string    `json:"responseBody,omitempty"`
}

// Batcher represents a background queue processor or ETL batch worker.
type Batcher struct {
	ID               string    `json:"id"`
	Name             string    `json:"name"`
	JobKey           string    `json:"jobKey"`
	Schedule         string    `json:"schedule"`
	CronExpr         string    `json:"cronExpr"`
	Status           string    `json:"status"` // "active", "paused", "running"
	Concurrency      string    `json:"concurrency"` // "Mutex Locked", "Strict FIFO", "Single Instance"
	LastRun          time.Time `json:"lastRun"`
	LastRunFormatted string    `json:"lastRunFormatted"`
	Duration         string    `json:"duration"`
	RecordsProcessed string    `json:"recordsProcessed"`
	TargetQueue      string    `json:"targetQueue"`
	Description      string    `json:"description"`
}

// Incident represents a downtime alert or SLA disruption.
type Incident struct {
	ID           string     `json:"id"`
	MonitorID    string     `json:"monitorId,omitempty"`
	Title        string     `json:"title"`
	Service      string     `json:"service"`
	URL          string     `json:"url"`
	Severity     string     `json:"severity"` // "P1 - Critical", "P2 - High", "P3 - Moderate"
	Status       string     `json:"status"`   // "investigating", "resolved"
	StartedAt    time.Time  `json:"startedAt"`
	StartedAtStr string     `json:"startedAtStr"`
	Duration     string     `json:"duration"`
	Cause        string     `json:"cause"`
	DispatchedTo []string   `json:"dispatchedTo"`
	ResolvedAt   *time.Time `json:"resolvedAt,omitempty"`
}

// SystemStats represents telemetry and operational KPIs.
type SystemStats struct {
	EngineStatus    string  `json:"engineStatus"`
	UptimeSeconds   int64   `json:"uptimeSeconds"`
	TotalMonitors   int     `json:"totalMonitors"`
	ActiveMonitors  int     `json:"activeMonitors"`
	PausedMonitors  int     `json:"pausedMonitors"`
	FailingMonitors int     `json:"failingMonitors"`
	TotalRuns       int64   `json:"totalRuns"`
	SuccessRate     string  `json:"successRate"`
	AvgLatencyMs    int64   `json:"avgLatencyMs"`
	ActiveWorkers   int     `json:"activeWorkers"`
	Goroutines      int     `json:"goroutines"`
	MemoryAllocMB   float64 `json:"memoryAllocMb"`
}

// CreateMonitorRequest represents the payload to create a new monitor.
type CreateMonitorRequest struct {
	Name            string      `json:"name"`
	URL             string      `json:"url"`
	Method          string      `json:"method"`
	Interval        string      `json:"interval"`
	IntervalSeconds int         `json:"intervalSeconds"`
	IntervalLabel   string      `json:"intervalLabel"`
	CronExpr        string      `json:"cronExpr,omitempty"`
	Category        string      `json:"category,omitempty"`
	Notifications   interface{} `json:"notifications,omitempty"`
	Headers         interface{} `json:"headers,omitempty"`
	Body            string      `json:"body,omitempty"`
	ExpectedStatus  int         `json:"expectedStatus"`
	Timeout         int         `json:"timeout"`
}

// CreateBatcherRequest represents the payload to create a new batcher.
type CreateBatcherRequest struct {
	Name        string `json:"name"`
	JobKey      string `json:"jobKey"`
	Schedule    string `json:"schedule"`
	CronExpr    string `json:"cronExpr"`
	Concurrency string `json:"concurrency"`
	TargetQueue string `json:"targetQueue"`
	Description string `json:"description"`
}

// CreateIncidentRequest represents the payload to declare a new incident.
type CreateIncidentRequest struct {
	MonitorID    string   `json:"monitorId,omitempty"`
	Title        string   `json:"title"`
	Service      string   `json:"service"`
	URL          string   `json:"url"`
	Severity     string   `json:"severity"`
	Cause        string   `json:"cause"`
	DispatchedTo []string `json:"dispatchedTo"`
}
