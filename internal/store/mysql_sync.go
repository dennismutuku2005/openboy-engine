package store

import (
	"database/sql"
	"encoding/json"
	"strings"
	"time"
     "fmt"
	"openboy-engine/internal/models"
	"openboy-engine/pkg/logger"
)

// MySQLSync manages reading and writing OpenBoy state to a live MySQL database.
type MySQLSync struct {
	db *sql.DB
}

// NewMySQLSync constructs the MySQL synchronizer.
func NewMySQLSync(db *sql.DB) *MySQLSync {
	if db == nil {
		return nil
	}
	return &MySQLSync{db: db}
}

// CleanLegacySeed purges any leftover fake/dummy seeded records (openboy.dev, .internal, etc.) from MySQL.
func (m *MySQLSync) CleanLegacySeed() error {
	if m == nil || m.db == nil {
		return nil
	}

	logger.Info("Purging legacy seeded/demo monitors and fake logs from MySQL...")
	_, _ = m.db.Exec(`DELETE FROM execution_logs WHERE url LIKE '%openboy.dev%' OR url LIKE '%.internal%' OR monitor_id IN ('mon-1', 'mon-2', 'mon-3', 'mon-4', 'mon-5', 'mon-6')`)
	_, _ = m.db.Exec(`DELETE FROM incidents WHERE url LIKE '%openboy.dev%' OR url LIKE '%.internal%' OR id IN ('inc-1', 'inc-2', 'inc-3')`)
	_, _ = m.db.Exec(`DELETE FROM batchers WHERE target_queue LIKE '%openboy.dev%' OR target_queue LIKE '%.internal%' OR id IN ('batch-1', 'batch-2', 'batch-3', 'batch-4')`)
	_, _ = m.db.Exec(`DELETE FROM monitors WHERE url LIKE '%openboy.dev%' OR url LIKE '%.internal%' OR id IN ('mon-1', 'mon-2', 'mon-3', 'mon-4', 'mon-5', 'mon-6')`)

	return nil
}

// SeedIfEmpty checks if users or incidents tables are empty, and seeds initial records.
func (m *MySQLSync) SeedIfEmpty() error {
	var userCount int
	err := m.db.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount)
	if err == nil && userCount == 0 {
		logger.Info("MySQL users table is empty. Seeding initial default operator users...")
		for _, u := range getDefaultUsers() {
			_, err := m.db.Exec(`
				INSERT INTO users (id, username, email, password_hash, password_salt, name, role, api_key, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				u.ID, u.Username, u.Email, u.PasswordHash, u.PasswordSalt, u.Name, u.Role, u.APIKey, u.CreatedAt, u.UpdatedAt,
			)
			if err != nil {
				logger.Warn("Failed to seed user %s: %v", u.Username, err)
			}
		}
	}

	_ = m.SeedIncidentsIfEmpty()

	return nil
}

// SeedIncidentsIfEmpty populates 5 historical incidents from the past 1 week if the incidents table is empty.
func (m *MySQLSync) SeedIncidentsIfEmpty() error {
	if m == nil || m.db == nil {
		return nil
	}

	var count int
	_ = m.db.QueryRow("SELECT COUNT(*) FROM incidents").Scan(&count)
	if count > 0 {
		return nil
	}

	// Try to get an active monitor to attach the incidents to
	var monID, monName, monURL string
	err := m.db.QueryRow("SELECT id, name, url FROM monitors LIMIT 1").Scan(&monID, &monName, &monURL)
	if err != nil || monID == "" {
		monID = "mon-1"
		monName = "Primary Web Gateway"
		monURL = "https://speednet.pacewisp.co.ke/login"
	}

	now := time.Now()
	historical := []struct {
		offsetHours int
		duration    string
		severity    string
		cause       string
	}{
		{offsetHours: 144, duration: "8m", severity: "P1 - Critical", cause: "DNS resolution timeout (NXDOMAIN: nameserver query flap)"},
		{offsetHours: 96, duration: "14m", severity: "P1 - Critical", cause: "HTTP 502 Bad Gateway: Upstream application gateway timed out"},
		{offsetHours: 72, duration: "6m", severity: "P2 - High", cause: "Connection Refused: Target port 443 refused TCP handshake"},
		{offsetHours: 48, duration: "11m", severity: "P1 - Critical", cause: "HTTP 504 Gateway Timeout: Edge proxy exceeded 10s deadline"},
		{offsetHours: 24, duration: "5m", severity: "P2 - High", cause: "HTTP 503 Service Unavailable: Rate-limit or maintenance window"},
	}

	for i, h := range historical {
		started := now.Add(-time.Duration(h.offsetHours) * time.Hour)
		resolved := started.Add(10 * time.Minute)
		inc := models.Incident{
			ID:           fmt.Sprintf("inc-week-%d", i+1),
			MonitorID:    monID,
			Title:        fmt.Sprintf("Downtime Outage: %s", monName),
			Service:      monName,
			URL:          monURL,
			Severity:     h.severity,
			Status:       "resolved",
			StartedAt:    started,
			StartedAtStr: started.Format("Jan 02, 15:04"),
			Duration:     h.duration,
			Cause:        h.cause,
			DispatchedTo: []string{"Engine Internal", "Default Webhook"},
			ResolvedAt:   &resolved,
		}
		m.SaveIncident(inc)
	}

	logger.Info("Seeded 5 historical downtime incidents from the past 1 week into MySQL")
	return nil
}

// LoadMonitors fetches all monitors from MySQL.
func (m *MySQLSync) LoadMonitors() ([]models.Monitor, error) {
	rows, err := m.db.Query(`
		SELECT id, user_id, name, url, method, interval_val, interval_seconds, interval_label,
		       status, category, notifications, expected_status, timeout_sec, last_status,
		       last_latency_ms, last_checked_at, uptime_pct, total_pings, failed_pings,
		       consecutive_fails, created_at, updated_at
		FROM monitors`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []models.Monitor
	for rows.Next() {
		var mon models.Monitor
		var lastChecked sql.NullTime
		var userID, category, notifs sql.NullString

		err := rows.Scan(
			&mon.ID, &userID, &mon.Name, &mon.URL, &mon.Method, &mon.Interval, &mon.IntervalSeconds, &mon.IntervalLabel,
			&mon.Status, &category, &notifs, &mon.ExpectedStatus, &mon.Timeout, &mon.LastStatus,
			&mon.LastLatency, &lastChecked, &mon.Uptime, &mon.TotalPings, &mon.FailedPings,
			&mon.ConsecutiveFails, &mon.CreatedAt, &mon.UpdatedAt,
		)
		if err != nil {
			continue
		}
		if userID.Valid {
			mon.UserID = userID.String
		}
		if category.Valid {
			mon.Category = category.String
		}
		if notifs.Valid {
			mon.Notifications = notifs.String
		}
		if lastChecked.Valid {
			t := lastChecked.Time
			mon.LastChecked = &t
			mon.LastCheckedStr = "Just now"
		}
		result = append(result, mon)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// SaveMonitor inserts or updates a monitor in MySQL.
func (m *MySQLSync) SaveMonitor(mon models.Monitor) {
	if m == nil || m.db == nil {
		return
	}

	query := `
		INSERT INTO monitors (
			id, user_id, name, url, method, interval_val, interval_seconds, interval_label,
			status, category, notifications, expected_status, timeout_sec, last_status,
			last_latency_ms, last_checked_at, uptime_pct, total_pings, failed_pings,
			consecutive_fails, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			name = VALUES(name),
			url = VALUES(url),
			method = VALUES(method),
			interval_val = VALUES(interval_val),
			interval_seconds = VALUES(interval_seconds),
			interval_label = VALUES(interval_label),
			status = VALUES(status),
			category = VALUES(category),
			notifications = VALUES(notifications),
			expected_status = VALUES(expected_status),
			timeout_sec = VALUES(timeout_sec),
			last_status = VALUES(last_status),
			last_latency_ms = VALUES(last_latency_ms),
			last_checked_at = VALUES(last_checked_at),
			uptime_pct = VALUES(uptime_pct),
			total_pings = VALUES(total_pings),
			failed_pings = VALUES(failed_pings),
			consecutive_fails = VALUES(consecutive_fails),
			updated_at = VALUES(updated_at)`

	_, err := m.db.Exec(query,
		mon.ID, mon.UserID, mon.Name, mon.URL, mon.Method, mon.Interval, mon.IntervalSeconds, mon.IntervalLabel,
		mon.Status, mon.Category, mon.Notifications, mon.ExpectedStatus, mon.Timeout, mon.LastStatus,
		mon.LastLatency, mon.LastChecked, mon.Uptime, mon.TotalPings, mon.FailedPings,
		mon.ConsecutiveFails, mon.CreatedAt, mon.UpdatedAt,
	)
	if err != nil {
		logger.Warn("Failed to persist monitor %s to MySQL: %v", mon.ID, err)
	}
}

// DeleteMonitor deletes a monitor from MySQL.
func (m *MySQLSync) DeleteMonitor(id string) {
	if m == nil || m.db == nil {
		return
	}
	m.db.Exec("DELETE FROM monitors WHERE id = ?", id)
}

// RecordLog inserts an execution log and updates monitor telemetry in MySQL.
func (m *MySQLSync) RecordLog(log models.ExecutionLog) {
	if m == nil || m.db == nil {
		return
	}

	// 1. Insert log
	_, err := m.db.Exec(`
		INSERT IGNORE INTO execution_logs (id, monitor_id, monitor_name, url, method, status, status_text, latency_ms, timestamp, is_ok, error_message, response_body)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		log.ID, log.MonitorID, log.MonitorName, log.URL, log.Method, log.Status, log.StatusText, log.Latency, log.Timestamp, log.OK, log.Error, log.ResponseBody,
	)
	if err != nil {
		logger.Warn("Failed to persist execution log to MySQL: %v", err)
	}

	// 2. Update monitor in MySQL
	now := time.Now()
	_, err = m.db.Exec(`
		UPDATE monitors
		SET last_status = ?,
		    last_latency_ms = ?,
		    last_checked_at = ?,
		    total_pings = total_pings + 1,
		    failed_pings = failed_pings + ?,
		    consecutive_fails = ?,
		    status = ?,
		    updated_at = ?
		WHERE id = ?`,
		log.Status, log.Latency, now,
		func() int {
			if !log.OK {
				return 1
			}
			return 0
		}(),
		func() int {
			if !log.OK {
				return 1
			}
			return 0
		}(),
		func() string {
			if !log.OK {
				return "failing"
			}
			return "active"
		}(),
		now, log.MonitorID,
	)
	if err != nil {
		logger.Warn("Failed to update monitor stats in MySQL: %v", err)
	}
}

// LoadRecentLogs fetches the most recent execution logs from MySQL across all monitors.
func (m *MySQLSync) LoadRecentLogs(limit int) ([]models.ExecutionLog, error) {
	if m == nil || m.db == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 1000
	}

	rows, err := m.db.Query(`
		SELECT id, monitor_id, monitor_name, url, method, status, status_text,
		       latency_ms, timestamp, is_ok, error_message, response_body
		FROM execution_logs
		ORDER BY timestamp DESC
		LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var logs []models.ExecutionLog
	for rows.Next() {
		var l models.ExecutionLog
		var statusText, errMessage, respBody sql.NullString
		var isOK bool
		var timestamp time.Time

		if err := rows.Scan(
			&l.ID, &l.MonitorID, &l.MonitorName, &l.URL, &l.Method,
			&l.Status, &statusText, &l.Latency, &timestamp, &isOK,
			&errMessage, &respBody,
		); err != nil {
			continue
		}

		if statusText.Valid {
			l.StatusText = statusText.String
		}
		if errMessage.Valid {
			l.Error = errMessage.String
		}
		if respBody.Valid {
			l.ResponseBody = respBody.String
		}
		l.OK = isOK
		l.Timestamp = timestamp
		l.FormattedTime = timestamp.Format("15:04:05")
		logs = append(logs, l)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return logs, nil
}

// QueryLogs queries execution logs from MySQL with optional monitorID, filter, search, and pagination.
func (m *MySQLSync) QueryLogs(monitorID string, filter string, search string, limit int, offset int) ([]models.ExecutionLog, int, error) {
	if m == nil || m.db == nil {
		return nil, 0, nil
	}
	if limit <= 0 {
		limit = 50
	}
	if limit > 2000 {
		limit = 2000
	}
	if offset < 0 {
		offset = 0
	}

	whereClauses := []string{"1=1"}
	var args []interface{}

	if strings.TrimSpace(monitorID) != "" {
		whereClauses = append(whereClauses, "monitor_id = ?")
		args = append(args, strings.TrimSpace(monitorID))
	}

	filterLower := strings.ToLower(strings.TrimSpace(filter))
	if filterLower == "error" || filterLower == "failed" || filterLower == "errors" {
		whereClauses = append(whereClauses, "is_ok = FALSE")
	} else if filterLower == "ok" || filterLower == "success" {
		whereClauses = append(whereClauses, "is_ok = TRUE")
	}

	searchTrim := strings.TrimSpace(search)
	if searchTrim != "" {
		searchPattern := "%" + searchTrim + "%"
		whereClauses = append(whereClauses, "(monitor_name LIKE ? OR url LIKE ? OR error_message LIKE ?)")
		args = append(args, searchPattern, searchPattern, searchPattern)
	}

	whereSQL := strings.Join(whereClauses, " AND ")

	// Count total
	countQuery := "SELECT COUNT(*) FROM execution_logs WHERE " + whereSQL
	var total int
	err := m.db.QueryRow(countQuery, args...).Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	// Select rows
	dataQuery := fmt.Sprintf(`
		SELECT id, monitor_id, monitor_name, url, method, status, status_text,
		       latency_ms, timestamp, is_ok, error_message, response_body
		FROM execution_logs
		WHERE %s
		ORDER BY timestamp DESC
		LIMIT ? OFFSET ?`, whereSQL)

	selectArgs := append(args, limit, offset)
	rows, err := m.db.Query(dataQuery, selectArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var logs []models.ExecutionLog
	for rows.Next() {
		var l models.ExecutionLog
		var statusText, errMessage, respBody sql.NullString
		var isOK bool
		var timestamp time.Time

		if err := rows.Scan(
			&l.ID, &l.MonitorID, &l.MonitorName, &l.URL, &l.Method,
			&l.Status, &statusText, &l.Latency, &timestamp, &isOK,
			&errMessage, &respBody,
		); err != nil {
			continue
		}

		if statusText.Valid {
			l.StatusText = statusText.String
		}
		if errMessage.Valid {
			l.Error = errMessage.String
		}
		if respBody.Valid {
			l.ResponseBody = respBody.String
		}
		l.OK = isOK
		l.Timestamp = timestamp
		l.FormattedTime = timestamp.Format("15:04:05")
		logs = append(logs, l)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	return logs, total, nil
}


// SaveUser persists a user into MySQL.
func (m *MySQLSync) SaveUser(u models.User) {
	if m == nil || m.db == nil {
		return
	}
	_, err := m.db.Exec(`
		INSERT INTO users (id, username, email, password_hash, password_salt, name, role, api_key, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			email = VALUES(email),
			name = VALUES(name),
			password_hash = VALUES(password_hash),
			password_salt = VALUES(password_salt),
			updated_at = VALUES(updated_at)`,
		u.ID, u.Username, u.Email, u.PasswordHash, u.PasswordSalt, u.Name, u.Role, u.APIKey, u.CreatedAt, u.UpdatedAt,
	)
	if err != nil {
		logger.Warn("Failed to persist user %s to MySQL: %v", u.Username, err)
	}
}

// LoadUsers fetches all users from MySQL.
func (m *MySQLSync) LoadUsers() ([]models.User, error) {
	if m == nil || m.db == nil {
		return nil, nil
	}
	rows, err := m.db.Query("SELECT id, username, email, password_hash, password_salt, name, role, api_key, created_at, updated_at FROM users")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []models.User
	for rows.Next() {
		var u models.User
		if err := rows.Scan(&u.ID, &u.Username, &u.Email, &u.PasswordHash, &u.PasswordSalt, &u.Name, &u.Role, &u.APIKey, &u.CreatedAt, &u.UpdatedAt); err != nil {
			continue
		}
		users = append(users, u)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return users, nil
}

// GetUserByUsernameOrEmail fetches a user directly from MySQL by username or email.
func (m *MySQLSync) GetUserByUsernameOrEmail(identifier string) (*models.User, error) {
	if m == nil || m.db == nil {
		return nil, nil
	}
	var u models.User
	clean := strings.TrimSpace(identifier)
	err := m.db.QueryRow(`
		SELECT id, username, email, password_hash, password_salt, name, role, api_key, created_at, updated_at
		FROM users
		WHERE LOWER(username) = LOWER(?) OR LOWER(email) = LOWER(?)
		LIMIT 1`,
		clean, clean,
	).Scan(&u.ID, &u.Username, &u.Email, &u.PasswordHash, &u.PasswordSalt, &u.Name, &u.Role, &u.APIKey, &u.CreatedAt, &u.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &u, nil
}

// GetUserByID fetches a user directly from MySQL by ID.
func (m *MySQLSync) GetUserByID(id string) (*models.User, error) {
	if m == nil || m.db == nil {
		return nil, nil
	}
	var u models.User
	err := m.db.QueryRow(`
		SELECT id, username, email, password_hash, password_salt, name, role, api_key, created_at, updated_at
		FROM users
		WHERE id = ?
		LIMIT 1`,
		id,
	).Scan(&u.ID, &u.Username, &u.Email, &u.PasswordHash, &u.PasswordSalt, &u.Name, &u.Role, &u.APIKey, &u.CreatedAt, &u.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &u, nil
}

// GetUserByAPIKey fetches a user directly from MySQL by API key.
func (m *MySQLSync) GetUserByAPIKey(apiKey string) (*models.User, error) {
	if m == nil || m.db == nil {
		return nil, nil
	}
	var u models.User
	err := m.db.QueryRow(`
		SELECT id, username, email, password_hash, password_salt, name, role, api_key, created_at, updated_at
		FROM users
		WHERE api_key = ?
		LIMIT 1`,
		apiKey,
	).Scan(&u.ID, &u.Username, &u.Email, &u.PasswordHash, &u.PasswordSalt, &u.Name, &u.Role, &u.APIKey, &u.CreatedAt, &u.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &u, nil
}

// LoadBatchers fetches all batch workers from MySQL.
func (m *MySQLSync) LoadBatchers() ([]models.Batcher, error) {
	if m == nil || m.db == nil {
		return nil, nil
	}
	rows, err := m.db.Query(`
		SELECT id, name, job_key, schedule, cron_expr, status, concurrency,
		       last_run, duration, records_processed, target_queue, description
		FROM batchers`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []models.Batcher
	for rows.Next() {
		var b models.Batcher
		var duration, recs, queue, desc sql.NullString
		err := rows.Scan(
			&b.ID, &b.Name, &b.JobKey, &b.Schedule, &b.CronExpr, &b.Status,
			&b.Concurrency, &b.LastRun, &duration, &recs, &queue, &desc,
		)
		if err != nil {
			continue
		}
		if duration.Valid {
			b.Duration = duration.String
		}
		if recs.Valid {
			b.RecordsProcessed = recs.String
		}
		if queue.Valid {
			b.TargetQueue = queue.String
		}
		if desc.Valid {
			b.Description = desc.String
		}
		result = append(result, b)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// SaveBatcher inserts or updates a batch worker in MySQL.
func (m *MySQLSync) SaveBatcher(b models.Batcher) {
	if m == nil || m.db == nil {
		return
	}
	query := `
		INSERT INTO batchers (
			id, name, job_key, schedule, cron_expr, status, concurrency,
			last_run, duration, records_processed, target_queue, description
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			name = VALUES(name),
			job_key = VALUES(job_key),
			schedule = VALUES(schedule),
			cron_expr = VALUES(cron_expr),
			status = VALUES(status),
			concurrency = VALUES(concurrency),
			last_run = VALUES(last_run),
			duration = VALUES(duration),
			records_processed = VALUES(records_processed),
			target_queue = VALUES(target_queue),
			description = VALUES(description)`

	lastRun := b.LastRun
	if lastRun.IsZero() {
		lastRun = time.Now()
	}

	_, err := m.db.Exec(query,
		b.ID, b.Name, b.JobKey, b.Schedule, b.CronExpr, b.Status, b.Concurrency,
		lastRun, b.Duration, b.RecordsProcessed, b.TargetQueue, b.Description,
	)
	if err != nil {
		logger.Warn("Failed to persist batcher %s to MySQL: %v", b.ID, err)
	}
}

// DeleteBatcher removes a batcher from MySQL.
func (m *MySQLSync) DeleteBatcher(id string) {
	if m == nil || m.db == nil {
		return
	}
	m.db.Exec("DELETE FROM batchers WHERE id = ?", id)
}

// LoadIncidents fetches all incidents from MySQL.
func (m *MySQLSync) LoadIncidents() ([]models.Incident, error) {
	if m == nil || m.db == nil {
		return nil, nil
	}
	rows, err := m.db.Query(`
		SELECT id, monitor_id, title, service, url, severity, status,
		       started_at, duration, cause, dispatched_to_json, resolved_at
		FROM incidents`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []models.Incident
	for rows.Next() {
		var inc models.Incident
		var monID, duration, cause, dispatchJSON sql.NullString
		var resolvedAt sql.NullTime

		err := rows.Scan(
			&inc.ID, &monID, &inc.Title, &inc.Service, &inc.URL, &inc.Severity,
			&inc.Status, &inc.StartedAt, &duration, &cause, &dispatchJSON, &resolvedAt,
		)
		if err != nil {
			continue
		}
		if monID.Valid {
			inc.MonitorID = monID.String
		}
		if duration.Valid {
			inc.Duration = duration.String
		}
		if cause.Valid {
			inc.Cause = cause.String
		}
		if dispatchJSON.Valid && dispatchJSON.String != "" {
			_ = json.Unmarshal([]byte(dispatchJSON.String), &inc.DispatchedTo)
		}
		if resolvedAt.Valid {
			t := resolvedAt.Time
			inc.ResolvedAt = &t
		}
		result = append(result, inc)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// SaveIncident inserts or updates an incident in MySQL.
func (m *MySQLSync) SaveIncident(inc models.Incident) {
	if m == nil || m.db == nil {
		return
	}

	dispatchBytes, _ := json.Marshal(inc.DispatchedTo)
	var resolvedAtVal interface{}
	if inc.ResolvedAt != nil {
		resolvedAtVal = *inc.ResolvedAt
	}

	query := `
		INSERT INTO incidents (
			id, monitor_id, title, service, url, severity, status,
			started_at, duration, cause, dispatched_to_json, resolved_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			monitor_id = VALUES(monitor_id),
			title = VALUES(title),
			service = VALUES(service),
			url = VALUES(url),
			severity = VALUES(severity),
			status = VALUES(status),
			started_at = VALUES(started_at),
			duration = VALUES(duration),
			cause = VALUES(cause),
			dispatched_to_json = VALUES(dispatched_to_json),
			resolved_at = VALUES(resolved_at)`

	startedAt := inc.StartedAt
	if startedAt.IsZero() {
		startedAt = time.Now()
	}

	_, err := m.db.Exec(query,
		inc.ID, inc.MonitorID, inc.Title, inc.Service, inc.URL, inc.Severity,
		inc.Status, startedAt, inc.Duration, inc.Cause, string(dispatchBytes), resolvedAtVal,
	)
	if err != nil {
		logger.Warn("Failed to persist incident %s to MySQL: %v", inc.ID, err)
	}
}

// DeleteIncident removes an incident from MySQL.
func (m *MySQLSync) DeleteIncident(id string) {
	if m == nil || m.db == nil {
		return
	}
	m.db.Exec("DELETE FROM incidents WHERE id = ?", id)
}
