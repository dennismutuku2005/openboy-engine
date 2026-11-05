package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"time"

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

	// Ensure all existing and newly loaded monitors have at least a 30-second timeout
	_, _ = m.db.Exec("UPDATE monitors SET timeout_sec = 30 WHERE timeout_sec < 30")

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
		       cron_expr, status, category, notifications, headers_json, body,
		       expected_status, timeout_sec, last_status,
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
		var userID, category, notifs, cronExpr, headersJSON, body sql.NullString

		err := rows.Scan(
			&mon.ID, &userID, &mon.Name, &mon.URL, &mon.Method, &mon.Interval, &mon.IntervalSeconds, &mon.IntervalLabel,
			&cronExpr, &mon.Status, &category, &notifs, &headersJSON, &body,
			&mon.ExpectedStatus, &mon.Timeout, &mon.LastStatus,
			&mon.LastLatency, &lastChecked, &mon.Uptime, &mon.TotalPings, &mon.FailedPings,
			&mon.ConsecutiveFails, &mon.CreatedAt, &mon.UpdatedAt,
		)
		if err != nil {
			continue
		}
		if mon.Timeout < 10 {
			mon.Timeout = 30
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
		if cronExpr.Valid {
			mon.CronExpr = cronExpr.String
		}
		if body.Valid {
			mon.Body = body.String
		}
		if headersJSON.Valid && headersJSON.String != "" {
			_ = json.Unmarshal([]byte(headersJSON.String), &mon.Headers)
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

	headersJSON, _ := json.Marshal(mon.Headers)
	headersStr := string(headersJSON)
	if len(mon.Headers) == 0 {
		headersStr = "{}"
	}

	query := `
		INSERT INTO monitors (
			id, user_id, name, url, method, interval_val, interval_seconds, interval_label,
			cron_expr, status, category, notifications, headers_json, body,
			expected_status, timeout_sec, last_status,
			last_latency_ms, last_checked_at, uptime_pct, total_pings, failed_pings,
			consecutive_fails, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			name = VALUES(name),
			url = VALUES(url),
			method = VALUES(method),
			interval_val = VALUES(interval_val),
			interval_seconds = VALUES(interval_seconds),
			interval_label = VALUES(interval_label),
			cron_expr = VALUES(cron_expr),
			status = VALUES(status),
			category = VALUES(category),
			notifications = VALUES(notifications),
			headers_json = VALUES(headers_json),
			body = VALUES(body),
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
		mon.CronExpr, mon.Status, mon.Category, mon.Notifications, headersStr, mon.Body,
		mon.ExpectedStatus, mon.Timeout, mon.LastStatus,
		mon.LastLatency, mon.LastChecked, mon.Uptime, mon.TotalPings, mon.FailedPings,
		mon.ConsecutiveFails, mon.CreatedAt, mon.UpdatedAt,
	)
	if err != nil {
		logger.Warn("Failed to persist monitor %s to MySQL: %v", mon.ID, err)
	}
}

// DeleteMonitor deletes a monitor and all associated execution logs and incidents from MySQL.
func (m *MySQLSync) DeleteMonitor(id string) {
	if m == nil || m.db == nil {
		return
	}
	_, _ = m.db.Exec("DELETE FROM execution_logs WHERE monitor_id = ?", id)
	_, _ = m.db.Exec("DELETE FROM incidents WHERE monitor_id = ?", id)
	_, _ = m.db.Exec("DELETE FROM monitors WHERE id = ?", id)
}

// GetMonitor fetches a single monitor directly from MySQL by ID.
func (m *MySQLSync) GetMonitor(id string) (*models.Monitor, error) {
	if m == nil || m.db == nil {
		return nil, nil
	}

	var mon models.Monitor
	var lastChecked sql.NullTime
	var userID, category, notifs, cronExpr, headersJSON, body sql.NullString

	err := m.db.QueryRow(`
		SELECT id, user_id, name, url, method, interval_val, interval_seconds, interval_label,
		       cron_expr, status, category, notifications, headers_json, body,
		       expected_status, timeout_sec, last_status, last_latency_ms, last_checked_at,
		       uptime_pct, total_pings, failed_pings, consecutive_fails, created_at, updated_at
		FROM monitors
		WHERE id = ?
		LIMIT 1`, id,
	).Scan(
		&mon.ID, &userID, &mon.Name, &mon.URL, &mon.Method, &mon.Interval, &mon.IntervalSeconds, &mon.IntervalLabel,
		&cronExpr, &mon.Status, &category, &notifs, &headersJSON, &body,
		&mon.ExpectedStatus, &mon.Timeout, &mon.LastStatus, &mon.LastLatency, &lastChecked,
		&mon.Uptime, &mon.TotalPings, &mon.FailedPings, &mon.ConsecutiveFails, &mon.CreatedAt, &mon.UpdatedAt,
	)
	if err != nil {
		return nil, err
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
	if cronExpr.Valid {
		mon.CronExpr = cronExpr.String
	}
	if body.Valid {
		mon.Body = body.String
	}
	if headersJSON.Valid && headersJSON.String != "" {
		_ = json.Unmarshal([]byte(headersJSON.String), &mon.Headers)
	}
	if lastChecked.Valid {
		t := lastChecked.Time
		mon.LastChecked = &t
		mon.LastCheckedStr = "Just now"
	}

	return &mon, nil
}

// ToggleMonitorStatus switches monitor status between active and paused in MySQL.
func (m *MySQLSync) ToggleMonitorStatus(id string) (*models.Monitor, error) {
	if m == nil || m.db == nil {
		return nil, nil
	}

	mon, err := m.GetMonitor(id)
	if err != nil || mon == nil {
		return nil, err
	}

	newStatus := "paused"
	if mon.Status == "paused" {
		newStatus = "active"
	}

	now := time.Now()
	_, err = m.db.Exec("UPDATE monitors SET status = ?, updated_at = ? WHERE id = ?", newStatus, now, id)
	if err != nil {
		return nil, err
	}

	mon.Status = newStatus
	mon.UpdatedAt = now
	return mon, nil
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
	isFailedInt := 0
	if !log.OK {
		isFailedInt = 1
	}
	_, err = m.db.Exec(`
		UPDATE monitors
		SET last_status = ?,
		    last_latency_ms = ?,
		    last_checked_at = ?,
		    total_pings = total_pings + 1,
		    failed_pings = failed_pings + ?,
		    consecutive_fails = CASE WHEN ? = 1 THEN consecutive_fails + 1 ELSE 0 END,
		    status = CASE 
		        WHEN status = 'paused' THEN 'paused'
		        WHEN ? = 1 THEN 'failing'
		        ELSE 'active'
		    END,
		    updated_at = ?
		WHERE id = ?`,
		log.Status, log.Latency, now,
		isFailedInt,
		isFailedInt,
		isFailedInt,
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

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Count total
	countQuery := "SELECT COUNT(*) FROM execution_logs WHERE " + whereSQL
	var total int
	err := m.db.QueryRowContext(ctx, countQuery, args...).Scan(&total)
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
	rows, err := m.db.QueryContext(ctx, dataQuery, selectArgs...)
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

// GetExecutionStats aggregates total runs, successes, errors, and average latency directly from execution_logs.
func (m *MySQLSync) GetExecutionStats(monitorID string) (models.ExecutionStats, error) {
	var stats models.ExecutionStats
	if m == nil || m.db == nil {
		return stats, nil
	}

	whereClause := "1=1"
	var args []interface{}
	if strings.TrimSpace(monitorID) != "" {
		whereClause = "monitor_id = ?"
		args = append(args, strings.TrimSpace(monitorID))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	query := fmt.Sprintf(`
		SELECT
			COUNT(*),
			COALESCE(SUM(CASE WHEN is_ok = 1 THEN 1 ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN is_ok = 0 THEN 1 ELSE 0 END), 0),
			COALESCE(ROUND(AVG(latency_ms)), 0)
		FROM execution_logs
		WHERE %s`, whereClause)

	err := m.db.QueryRowContext(ctx, query, args...).Scan(
		&stats.TotalRuns,
		&stats.SuccessCount,
		&stats.ErrorCount,
		&stats.AvgLatencyMs,
	)
	if err != nil {
		return stats, err
	}

	stats.SuccessRate = "100.0%"
	if stats.TotalRuns > 0 {
		rate := (float64(stats.SuccessCount) / float64(stats.TotalRuns)) * 100.0
		stats.SuccessRate = fmt.Sprintf("%.1f%%", rate)
	}

	return stats, nil
}

// GetLatencyStats calculates dedicated average, min, max, and latest latency directly from execution_logs and monitors.
func (m *MySQLSync) GetLatencyStats(monitorID string) (models.LatencyStats, error) {
	var stats models.LatencyStats
	if m == nil || m.db == nil {
		return stats, nil
	}

	trimmedID := strings.TrimSpace(monitorID)
	stats.MonitorID = trimmedID

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	whereClause := "1=1"
	var args []interface{}
	if trimmedID != "" {
		whereClause = "monitor_id = ?"
		args = append(args, trimmedID)

		// Fetch last_latency_ms from monitors table
		_ = m.db.QueryRowContext(ctx, "SELECT COALESCE(last_latency_ms, 0) FROM monitors WHERE id = ?", trimmedID).Scan(&stats.LastLatencyMs)
	} else {
		// Fetch latest latency from execution_logs across all monitors
		_ = m.db.QueryRowContext(ctx, "SELECT COALESCE(latency_ms, 0) FROM execution_logs ORDER BY timestamp DESC LIMIT 1").Scan(&stats.LastLatencyMs)
	}

	query := fmt.Sprintf(`
		SELECT
			COALESCE(ROUND(AVG(latency_ms)), 0),
			COALESCE(MIN(latency_ms), 0),
			COALESCE(MAX(latency_ms), 0),
			COUNT(*)
		FROM execution_logs
		WHERE %s`, whereClause)

	err := m.db.QueryRowContext(ctx, query, args...).Scan(
		&stats.AvgLatencyMs,
		&stats.MinLatencyMs,
		&stats.MaxLatencyMs,
		&stats.TotalSamples,
	)
	if err != nil {
		return stats, err
	}

	// If no execution logs exist in database yet, but monitor has a recorded lastLatency, use it as baseline
	if stats.TotalSamples == 0 && stats.LastLatencyMs > 0 {
		stats.AvgLatencyMs = stats.LastLatencyMs
		stats.MinLatencyMs = stats.LastLatencyMs
		stats.MaxLatencyMs = stats.LastLatencyMs
	}

	return stats, nil
}

// GetTimeline returns real-time timeline data points directly from execution_logs.
func (m *MySQLSync) GetTimeline(monitorID string, limit int) (models.TimelineResponse, error) {
	var resp models.TimelineResponse
	resp.Timeline = make([]models.TimelineItem, 0)
	if m == nil || m.db == nil {
		resp.SLA = "100.0%"
		return resp, nil
	}

	trimmedID := strings.TrimSpace(monitorID)
	resp.MonitorID = trimmedID
	if limit <= 0 || limit > 100 {
		limit = 30
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 1. Total runs and SLA
	var totalRuns, successCount int64
	if trimmedID != "" {
		_ = m.db.QueryRowContext(ctx, `
			SELECT
				COUNT(*),
				COALESCE(SUM(CASE WHEN is_ok = 1 THEN 1 ELSE 0 END), 0)
			FROM execution_logs
			WHERE monitor_id = ?`, trimmedID).Scan(&totalRuns, &successCount)

		// Fallback to monitors table if execution_logs has been cleared or pruned
		var monTotal, monFailed int64
		var monUptime string
		err := m.db.QueryRowContext(ctx, `SELECT total_pings, failed_pings, uptime_pct FROM monitors WHERE id = ?`, trimmedID).Scan(&monTotal, &monFailed, &monUptime)
		if err == nil && monTotal > totalRuns {
			totalRuns = monTotal
			successCount = monTotal - monFailed
			if monUptime != "" {
				resp.SLA = monUptime
			}
		}
	} else {
		_ = m.db.QueryRowContext(ctx, `
			SELECT
				COUNT(*),
				COALESCE(SUM(CASE WHEN is_ok = 1 THEN 1 ELSE 0 END), 0)
			FROM execution_logs`).Scan(&totalRuns, &successCount)
	}

	resp.TotalRecorded = totalRuns
	if resp.SLA == "" {
		resp.SLA = "100.0%"
		if totalRuns > 0 {
			rate := (float64(successCount) / float64(totalRuns)) * 100.0
			resp.SLA = fmt.Sprintf("%.1f%%", rate)
		}
	}

	// 2. Query recent execution timeline points ordered by timestamp DESC
	whereClause := "1=1"
	var args []interface{}
	if trimmedID != "" {
		whereClause = "monitor_id = ?"
		args = append(args, trimmedID)
	}
	args = append(args, limit)

	query := fmt.Sprintf(`
		SELECT id, monitor_id, status, latency_ms, is_ok, timestamp
		FROM execution_logs
		WHERE %s
		ORDER BY timestamp DESC
		LIMIT ?`, whereClause)

	rows, err := m.db.QueryContext(ctx, query, args...)
	if err != nil {
		return resp, err
	}
	defer rows.Close()

	var descItems []models.TimelineItem
	for rows.Next() {
		var item models.TimelineItem
		var ts time.Time
		if err := rows.Scan(&item.ID, &item.MonitorID, &item.Status, &item.Latency, &item.OK, &ts); err == nil {
			item.Timestamp = ts
			item.FormattedTime = ts.UTC().Format("15:04:05")
			descItems = append(descItems, item)
		}
	}

	// Reverse descItems to chronological order (oldest to newest, left-to-right on timeline)
	resp.Timeline = make([]models.TimelineItem, len(descItems))
	for i, item := range descItems {
		resp.Timeline[len(descItems)-1-i] = item
	}

	return resp, nil
}

// ClearLogs deletes execution logs directly from MySQL with optional filtering (older than N days or by monitor).
func (m *MySQLSync) ClearLogs(days int, monitorID string) int {
	if m == nil || m.db == nil {
		return 0
	}
	query := "DELETE FROM execution_logs WHERE 1=1"
	var args []interface{}
	if days > 0 {
		query += " AND timestamp < (NOW() - INTERVAL ? DAY)"
		args = append(args, days)
	}
	if strings.TrimSpace(monitorID) != "" {
		query += " AND monitor_id = ?"
		args = append(args, strings.TrimSpace(monitorID))
	}
	res, err := m.db.Exec(query, args...)
	if err != nil {
		logger.Warn("Failed to clear execution logs: %v", err)
		return 0
	}
	affected, _ := res.RowsAffected()
	return int(affected)
}

// ResetMonitorCounters resets total_pings, failed_pings, consecutive_fails, and uptime back to 100% baseline.
func (m *MySQLSync) ResetMonitorCounters(monitorID string) int {
	if m == nil || m.db == nil {
		return 0
	}
	query := "UPDATE monitors SET total_pings = 0, failed_pings = 0, consecutive_fails = 0, uptime_pct = '100%', updated_at = NOW() WHERE 1=1"
	var args []interface{}
	if strings.TrimSpace(monitorID) != "" {
		query += " AND id = ?"
		args = append(args, strings.TrimSpace(monitorID))
	}
	res, err := m.db.Exec(query, args...)
	if err != nil {
		logger.Warn("Failed to reset monitor counters: %v", err)
		return 0
	}
	affected, _ := res.RowsAffected()
	return int(affected)
}

// ClearIncidents deletes incidents directly from MySQL (optionally filtered by status, e.g. 'resolved').
func (m *MySQLSync) ClearIncidents(status string) int {
	if m == nil || m.db == nil {
		return 0
	}
	query := "DELETE FROM incidents WHERE 1=1"
	var args []interface{}
	if strings.TrimSpace(status) != "" {
		query += " AND LOWER(status) = ?"
		args = append(args, strings.ToLower(strings.TrimSpace(status)))
	}
	res, err := m.db.Exec(query, args...)
	if err != nil {
		logger.Warn("Failed to clear incidents: %v", err)
		return 0
	}
	affected, _ := res.RowsAffected()
	return int(affected)
}

// DeleteIncidentsByIDs deletes multiple specific incidents by their IDs in chunks.
func (m *MySQLSync) DeleteIncidentsByIDs(ids []string) int {
	if m == nil || m.db == nil || len(ids) == 0 {
		return 0
	}

	chunkSize := 500
	totalDeleted := 0

	for i := 0; i < len(ids); i += chunkSize {
		end := i + chunkSize
		if end > len(ids) {
			end = len(ids)
		}
		chunk := ids[i:end]

		placeholders := make([]string, len(chunk))
		args := make([]interface{}, len(chunk))
		for j, id := range chunk {
			placeholders[j] = "?"
			args[j] = strings.TrimSpace(id)
		}

		query := fmt.Sprintf("DELETE FROM incidents WHERE id IN (%s)", strings.Join(placeholders, ","))
		res, err := m.db.Exec(query, args...)
		if err != nil {
			logger.Warn("Failed to delete marked incidents chunk: %v", err)
			continue
		}
		affected, _ := res.RowsAffected()
		totalDeleted += int(affected)
	}

	return totalDeleted
}

// ResolveIncidentsByIDs resolves multiple incidents by their IDs in chunks.
func (m *MySQLSync) ResolveIncidentsByIDs(ids []string) int {
	if m == nil || m.db == nil || len(ids) == 0 {
		return 0
	}

	now := time.Now()
	chunkSize := 500
	totalResolved := 0

	for i := 0; i < len(ids); i += chunkSize {
		end := i + chunkSize
		if end > len(ids) {
			end = len(ids)
		}
		chunk := ids[i:end]

		placeholders := make([]string, len(chunk))
		args := make([]interface{}, 0, len(chunk)+1)
		args = append(args, now)

		for j, id := range chunk {
			placeholders[j] = "?"
			args = append(args, strings.TrimSpace(id))
		}

		query := fmt.Sprintf(`
			UPDATE incidents 
			SET status = 'resolved', resolved_at = ?, duration = CONCAT(TIMESTAMPDIFF(MINUTE, started_at, NOW()), 'm')
			WHERE id IN (%s) AND status != 'resolved'`,
			strings.Join(placeholders, ","),
		)

		res, err := m.db.Exec(query, args...)
		if err != nil {
			logger.Warn("Failed to batch resolve incidents chunk: %v", err)
			continue
		}
		affected, _ := res.RowsAffected()
		totalResolved += int(affected)
	}

	return totalResolved
}

// GetMaintenanceStats calculates real-time storage metrics and record counts across tables.
func (m *MySQLSync) GetMaintenanceStats() models.MaintenanceStats {
	var stats models.MaintenanceStats
	if m == nil || m.db == nil {
		return stats
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var minTs, maxTs *time.Time
	_ = m.db.QueryRowContext(ctx, "SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM execution_logs").Scan(&stats.TotalExecutionLogs, &minTs, &maxTs)
	if minTs != nil {
		stats.OldestLogTimestamp = minTs.UTC().Format("2006-01-02 15:04:05")
	}
	if maxTs != nil {
		stats.NewestLogTimestamp = maxTs.UTC().Format("2006-01-02 15:04:05")
	}
	_ = m.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM incidents").Scan(&stats.TotalIncidents)
	_ = m.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM monitors").Scan(&stats.TotalMonitors)
	_ = m.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM batchers").Scan(&stats.TotalBatchers)

	// Approximate size: ~512 bytes per execution log row + indexes
	stats.EstimatedStorageMB = float64(stats.TotalExecutionLogs*512) / (1024.0 * 1024.0)
	if stats.EstimatedStorageMB < 0.01 && stats.TotalExecutionLogs > 0 {
		stats.EstimatedStorageMB = 0.01
	}

	// Automatic daily midnight log purge indicator
	stats.DailyAutoCleanupEnabled = true
	now := time.Now()
	nextMidnight := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
	stats.NextDailyCleanup = nextMidnight.Format("2006-01-02 15:04:05")

	return stats
}

// DeleteLog deletes a single execution log directly from MySQL by ID.
func (m *MySQLSync) DeleteLog(id string) bool {
	if m == nil || m.db == nil {
		return false
	}
	res, err := m.db.Exec("DELETE FROM execution_logs WHERE id = ?", id)
	if err != nil {
		return false
	}
	affected, _ := res.RowsAffected()
	return affected > 0
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

// GetBatcher fetches a single batch worker directly from MySQL by ID.
func (m *MySQLSync) GetBatcher(id string) (*models.Batcher, error) {
	if m == nil || m.db == nil {
		return nil, nil
	}

	var b models.Batcher
	var duration, recs, queue, desc sql.NullString
	err := m.db.QueryRow(`
		SELECT id, name, job_key, schedule, cron_expr, status, concurrency,
		       last_run, duration, records_processed, target_queue, description
		FROM batchers
		WHERE id = ?
		LIMIT 1`, id,
	).Scan(
		&b.ID, &b.Name, &b.JobKey, &b.Schedule, &b.CronExpr, &b.Status,
		&b.Concurrency, &b.LastRun, &duration, &recs, &queue, &desc,
	)
	if err != nil {
		return nil, err
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
	b.LastRunFormatted = b.LastRun.Format("15:04:05")
	return &b, nil
}

// TriggerBatcher updates last_run for a batcher directly in MySQL.
func (m *MySQLSync) TriggerBatcher(id string) (*models.Batcher, error) {
	if m == nil || m.db == nil {
		return nil, nil
	}

	now := time.Now()
	_, err := m.db.Exec("UPDATE batchers SET last_run = ? WHERE id = ?", now, id)
	if err != nil {
		return nil, err
	}

	return m.GetBatcher(id)
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

// GetIncident fetches a single incident directly from MySQL by ID.
func (m *MySQLSync) GetIncident(id string) (*models.Incident, error) {
	if m == nil || m.db == nil {
		return nil, nil
	}

	var inc models.Incident
	var monID, duration, cause, dispatchJSON sql.NullString
	var resolvedAt sql.NullTime

	err := m.db.QueryRow(`
		SELECT id, monitor_id, title, service, url, severity, status,
		       started_at, duration, cause, dispatched_to_json, resolved_at
		FROM incidents
		WHERE id = ?
		LIMIT 1`, id,
	).Scan(
		&inc.ID, &monID, &inc.Title, &inc.Service, &inc.URL, &inc.Severity,
		&inc.Status, &inc.StartedAt, &duration, &cause, &dispatchJSON, &resolvedAt,
	)
	if err != nil {
		return nil, err
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
	inc.StartedAtStr = inc.StartedAt.Format("Jan 02, 15:04")
	return &inc, nil
}

// ResolveIncident marks an incident resolved directly in MySQL.
func (m *MySQLSync) ResolveIncident(id string) (*models.Incident, error) {
	if m == nil || m.db == nil {
		return nil, nil
	}

	inc, err := m.GetIncident(id)
	if err != nil || inc == nil {
		return nil, err
	}

	now := time.Now()
	durMinutes := int(now.Sub(inc.StartedAt).Minutes())
	durationStr := fmt.Sprintf("%dm", durMinutes)

	_, err = m.db.Exec(`
		UPDATE incidents
		SET status = 'resolved', resolved_at = ?, duration = ?
		WHERE id = ?`,
		now, durationStr, id,
	)
	if err != nil {
		return nil, err
	}

	inc.Status = "resolved"
	inc.ResolvedAt = &now
	inc.Duration = durationStr
	return inc, nil
}

// GetActiveIncidentForMonitor checks if there is an ongoing (unresolved) incident for this monitor.
func (m *MySQLSync) GetActiveIncidentForMonitor(monitorID string) (*models.Incident, error) {
	if m == nil || m.db == nil || monitorID == "" {
		return nil, nil
	}

	var inc models.Incident
	var monID, duration, cause, dispatchJSON sql.NullString
	var resolvedAt sql.NullTime

	err := m.db.QueryRow(`
		SELECT id, monitor_id, title, service, url, severity, status,
		       started_at, duration, cause, dispatched_to_json, resolved_at
		FROM incidents
		WHERE monitor_id = ? AND status != 'resolved'
		ORDER BY started_at DESC
		LIMIT 1`, monitorID,
	).Scan(
		&inc.ID, &monID, &inc.Title, &inc.Service, &inc.URL, &inc.Severity,
		&inc.Status, &inc.StartedAt, &duration, &cause, &dispatchJSON, &resolvedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
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
	return &inc, nil
}

// ResolveActiveIncidentsForMonitor marks open incidents for this monitor as resolved when probe succeeds.
func (m *MySQLSync) ResolveActiveIncidentsForMonitor(monitorID string) error {
	if m == nil || m.db == nil || monitorID == "" {
		return nil
	}

	now := time.Now()
	rows, err := m.db.Query(`SELECT id, started_at FROM incidents WHERE monitor_id = ? AND status != 'resolved'`, monitorID)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id string
		var startedAt time.Time
		if err := rows.Scan(&id, &startedAt); err == nil {
			durMinutes := int(now.Sub(startedAt).Minutes())
			durationStr := fmt.Sprintf("%dm", durMinutes)
			if durMinutes < 1 {
				durationStr = "< 1m"
			}
			_, _ = m.db.Exec(`UPDATE incidents SET status = 'resolved', resolved_at = ?, duration = ? WHERE id = ?`, now, durationStr, id)
		}
	}
	return nil
}

// UpdateUser modifies user details in MySQL.
func (m *MySQLSync) UpdateUser(u models.User) error {
	if m == nil || m.db == nil {
		return nil
	}
	_, err := m.db.Exec(`
		UPDATE users
		SET name = ?, email = ?, updated_at = ?
		WHERE id = ?`,
		u.Name, u.Email, time.Now(), u.ID,
	)
	return err
}

// UpdatePassword updates password hash and salt for a user in MySQL.
func (m *MySQLSync) UpdatePassword(userID, newHash, newSalt string) error {
	if m == nil || m.db == nil {
		return nil
	}
	_, err := m.db.Exec(`
		UPDATE users
		SET password_hash = ?, password_salt = ?, updated_at = ?
		WHERE id = ?`,
		newHash, newSalt, time.Now(), userID,
	)
	return err
}

// GetSystemStats calculates real-time system stats directly from MySQL.
func (m *MySQLSync) GetSystemStats(activeWorkers int, startTime time.Time) (models.SystemStats, error) {
	var totalMonitors, activeMonitors, pausedMonitors, failingMonitors int
	var totalRuns, totalFailed, totalLatency int64

	if m != nil && m.db != nil {
		_ = m.db.QueryRow("SELECT COUNT(*) FROM monitors").Scan(&totalMonitors)
		_ = m.db.QueryRow("SELECT COUNT(*) FROM monitors WHERE LOWER(status) = 'active'").Scan(&activeMonitors)
		_ = m.db.QueryRow("SELECT COUNT(*) FROM monitors WHERE LOWER(status) = 'paused'").Scan(&pausedMonitors)
		_ = m.db.QueryRow("SELECT COUNT(*) FROM monitors WHERE LOWER(status) = 'failing'").Scan(&failingMonitors)
		_ = m.db.QueryRow("SELECT COALESCE(SUM(total_pings), 0), COALESCE(SUM(failed_pings), 0) FROM monitors").Scan(&totalRuns, &totalFailed)
		_ = m.db.QueryRow("SELECT COALESCE(SUM(total_pings * last_latency_ms), 0) FROM monitors").Scan(&totalLatency)
	}

	uptimeSeconds := int64(time.Since(startTime).Seconds())
	avgLatency := int64(0)
	if totalRuns > 0 {
		avgLatency = totalLatency / totalRuns
	}

	successRate := "100%"
	if totalRuns > 0 {
		sr := float64(totalRuns-totalFailed) / float64(totalRuns) * 100.0
		if sr < 0 {
			sr = 0
		}
		successRate = fmt.Sprintf("%.2f%%", sr)
	}

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	return models.SystemStats{
		EngineStatus:    "running",
		UptimeSeconds:   uptimeSeconds,
		TotalMonitors:   totalMonitors,
		ActiveMonitors:  activeMonitors,
		PausedMonitors:  pausedMonitors,
		FailingMonitors: failingMonitors,
		TotalRuns:       totalRuns,
		SuccessRate:     successRate,
		AvgLatencyMs:    avgLatency,
		ActiveWorkers:   activeWorkers,
		Goroutines:      runtime.NumGoroutine(),
		MemoryAllocMB:   float64(mem.Alloc) / 1024 / 1024,
	}, nil
}
