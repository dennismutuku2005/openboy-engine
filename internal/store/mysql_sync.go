package store

import (
	"database/sql"
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

// SeedIfEmpty checks if tables are empty, and if so seeds default monitors, batchers, users.
func (m *MySQLSync) SeedIfEmpty() error {
	var monitorCount int
	err := m.db.QueryRow("SELECT COUNT(*) FROM monitors").Scan(&monitorCount)
	if err != nil {
		return err
	}

	if monitorCount == 0 {
		logger.Info("MySQL monitors table is empty. Seeding initial starter monitors...")
		for _, mon := range getDefaultMonitors() {
			_, err := m.db.Exec(`
				INSERT INTO monitors (
					id, user_id, name, url, method, interval_val, interval_seconds, interval_label,
					status, category, notifications, expected_status, timeout_sec, last_status,
					last_latency_ms, last_checked_at, uptime_pct, total_pings, failed_pings,
					consecutive_fails, created_at, updated_at
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				mon.ID, mon.UserID, mon.Name, mon.URL, mon.Method, mon.Interval, mon.IntervalSeconds, mon.IntervalLabel,
				mon.Status, mon.Category, mon.Notifications, mon.ExpectedStatus, mon.Timeout, mon.LastStatus,
				mon.LastLatency, mon.LastChecked, mon.Uptime, mon.TotalPings, mon.FailedPings,
				mon.ConsecutiveFails, mon.CreatedAt, mon.UpdatedAt,
			)
			if err != nil {
				logger.Warn("Failed to seed monitor %s: %v", mon.ID, err)
			}
		}
	}

	var userCount int
	err = m.db.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount)
	if err == nil && userCount == 0 {
		logger.Info("MySQL users table is empty. Seeding initial default users...")
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

	var batcherCount int
	err = m.db.QueryRow("SELECT COUNT(*) FROM batchers").Scan(&batcherCount)
	if err == nil && batcherCount == 0 {
		for _, b := range getDefaultBatchers() {
			m.db.Exec(`
				INSERT INTO batchers (id, name, job_key, schedule, cron_expr, status, concurrency, last_run, duration, records_processed, target_queue, description)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				b.ID, b.Name, b.JobKey, b.Schedule, b.CronExpr, b.Status, b.Concurrency, b.LastRun, b.Duration, b.RecordsProcessed, b.TargetQueue, b.Description,
			)
		}
	}

	var incidentCount int
	err = m.db.QueryRow("SELECT COUNT(*) FROM incidents").Scan(&incidentCount)
	if err == nil && incidentCount == 0 {
		for _, inc := range getDefaultIncidents() {
			m.db.Exec(`
				INSERT INTO incidents (id, monitor_id, title, service, url, severity, status, started_at, duration, cause)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				inc.ID, inc.MonitorID, inc.Title, inc.Service, inc.URL, inc.Severity, inc.Status, inc.StartedAt, inc.Duration, inc.Cause,
			)
		}
	}

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
		INSERT INTO execution_logs (id, monitor_id, monitor_name, url, method, status, status_text, latency_ms, timestamp, is_ok, error_message, response_body)
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
