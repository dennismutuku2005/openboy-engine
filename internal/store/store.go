package store

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"openboy-engine/internal/models"
	"openboy-engine/pkg/logger"
)

var incidentSeq int64

type persistedState struct {
	Users     []models.User     `json:"users"`
	Monitors  []models.Monitor  `json:"monitors"`
	Batchers  []models.Batcher  `json:"batchers"`
	Incidents []models.Incident `json:"incidents"`
}

// Store provides direct database-driven access to users, monitors, batchers, incidents, and logs.
type Store struct {
	mu           sync.RWMutex
	users        map[string]*models.User // fallback in-memory store if MySQL is not configured
	monitors     map[string]*models.Monitor
	batchers     map[string]*models.Batcher
	incidents    map[string]*models.Incident
	logs         []models.ExecutionLog
	maxLogs      int
	filePath     string
	startTime    time.Time
	totalRuns    int64
	totalLatency int64
	subscribers           map[chan models.ExecutionLog]struct{}
	subMu                 sync.RWMutex
	mysql                 *MySQLSync
	lastDailyCleanupTime  time.Time
	lastDailyCleanupCount int
}

// NewStore initializes a store, directly delegating to MySQL when connected.
func NewStore(dataFile string, mysqlDB ...*MySQLSync) *Store {
	s := &Store{
		users:       make(map[string]*models.User),
		monitors:    make(map[string]*models.Monitor),
		batchers:    make(map[string]*models.Batcher),
		incidents:   make(map[string]*models.Incident),
		logs:        make([]models.ExecutionLog, 0, 500),
		maxLogs:     500,
		filePath:    dataFile,
		startTime:   time.Now(),
		subscribers: make(map[chan models.ExecutionLog]struct{}),
	}

	if len(mysqlDB) > 0 && mysqlDB[0] != nil {
		s.mysql = mysqlDB[0]

		// Clean up any legacy seeded/demo monitors and fake logs from MySQL
		if err := s.mysql.CleanLegacySeed(); err != nil {
			logger.Warn("Failed to clean legacy seed from MySQL: %v", err)
		}
		// Seed MySQL if empty
		if err := s.mysql.SeedIfEmpty(); err != nil {
			logger.Warn("Failed to seed MySQL: %v", err)
		}
	}

	// Fallback admin user if running without MySQL
	if s.mysql == nil && len(s.users) == 0 {
		s.bootstrapUsers()
	}

	return s
}

func (s *Store) bootstrapSeed() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, u := range getDefaultUsers() {
		copyU := u
		s.users[u.ID] = &copyU
	}
	for _, m := range getDefaultMonitors() {
		copyM := m
		s.monitors[m.ID] = &copyM
		s.totalRuns += m.TotalPings
		s.totalLatency += m.TotalPings * m.LastLatency
	}
	for _, b := range getDefaultBatchers() {
		copyB := b
		s.batchers[b.ID] = &copyB
	}
	for _, inc := range getDefaultIncidents() {
		copyInc := inc
		s.incidents[inc.ID] = &copyInc
	}
	s.logs = append(s.logs, getDefaultLogs()...)
}

func (s *Store) bootstrapUsers() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, u := range getDefaultUsers() {
		copyU := u
		s.users[u.ID] = &copyU
	}
}

func (s *Store) loadFromFile() error {
	return nil
}

// SaveToFile is a no-op as all data persistence is handled directly by MySQL.
func (s *Store) SaveToFile() error {
	return nil
}

// ==========================================
// USER OPERATIONS (DIRECT DATABASE)
// ==========================================

// GetUserByID retrieves a user by ID directly from MySQL.
func (s *Store) GetUserByID(id string) (*models.User, bool) {
	if s.mysql != nil {
		u, err := s.mysql.GetUserByID(id)
		if err == nil && u != nil {
			return u, true
		}
		return nil, false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.users[id]
	if !ok {
		return nil, false
	}
	copyU := *u
	return &copyU, true
}

// GetUserByUsernameOrEmail finds a user by username or email directly from MySQL.
func (s *Store) GetUserByUsernameOrEmail(identifier string) (*models.User, bool) {
	if s.mysql != nil {
		u, err := s.mysql.GetUserByUsernameOrEmail(identifier)
		if err == nil && u != nil {
			return u, true
		}
		return nil, false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	lower := strings.ToLower(strings.TrimSpace(identifier))
	for _, u := range s.users {
		if strings.ToLower(u.Username) == lower || strings.ToLower(u.Email) == lower {
			copyU := *u
			return &copyU, true
		}
	}
	return nil, false
}

// GetUserByAPIKey finds a user by API key directly from MySQL.
func (s *Store) GetUserByAPIKey(key string) (*models.User, bool) {
	if s.mysql != nil {
		u, err := s.mysql.GetUserByAPIKey(key)
		if err == nil && u != nil {
			return u, true
		}
		return nil, false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, u := range s.users {
		if u.APIKey == key {
			copyU := *u
			return &copyU, true
		}
	}
	return nil, false
}

// CreateUser inserts a new user directly into MySQL or fallback store.
func (s *Store) CreateUser(u models.User) (*models.User, error) {
	if u.ID == "" {
		u.ID = fmt.Sprintf("usr-%d", time.Now().UnixNano())
	}
	now := time.Now()
	u.CreatedAt = now
	u.UpdatedAt = now

	if s.mysql != nil {
		s.mysql.SaveUser(u)
		return &u, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	lowerUser := strings.ToLower(strings.TrimSpace(u.Username))
	lowerEmail := strings.ToLower(strings.TrimSpace(u.Email))

	for _, existing := range s.users {
		if strings.ToLower(existing.Username) == lowerUser {
			return nil, fmt.Errorf("username '%s' is already registered", u.Username)
		}
		if strings.ToLower(existing.Email) == lowerEmail {
			return nil, fmt.Errorf("email '%s' is already registered", u.Email)
		}
	}

	s.users[u.ID] = &u
	copyU := u
	return &copyU, nil
}

// UpdateUser modifies user profile fields directly in MySQL or fallback store.
func (s *Store) UpdateUser(u models.User) bool {
	if s.mysql != nil {
		return s.mysql.UpdateUser(u) == nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.users[u.ID]
	if !ok {
		return false
	}
	if u.Name != "" {
		existing.Name = u.Name
	}
	if u.Email != "" {
		existing.Email = u.Email
	}
	existing.UpdatedAt = time.Now()
	return true
}

// UpdatePassword sets a new password hash directly in MySQL or fallback store.
func (s *Store) UpdatePassword(userID, newHash, newSalt string) bool {
	if s.mysql != nil {
		return s.mysql.UpdatePassword(userID, newHash, newSalt) == nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	user, ok := s.users[userID]
	if !ok {
		return false
	}
	user.PasswordHash = newHash
	user.PasswordSalt = newSalt
	user.UpdatedAt = time.Now()
	return true
}

// ==========================================
// MONITOR OPERATIONS (DIRECT DATABASE)
// ==========================================

// GetAllMonitors returns all registered monitors directly from MySQL or fallback store.
func (s *Store) GetAllMonitors() []models.Monitor {
	if s.mysql != nil {
		if mons, err := s.mysql.LoadMonitors(); err == nil {
			return mons
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]models.Monitor, 0, len(s.monitors))
	for _, m := range s.monitors {
		result = append(result, *m)
	}
	return result
}

// GetMonitor retrieves a single monitor directly from MySQL or fallback store.
func (s *Store) GetMonitor(id string) (*models.Monitor, bool) {
	if s.mysql != nil {
		if m, err := s.mysql.GetMonitor(id); err == nil && m != nil {
			return m, true
		}
		return nil, false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.monitors[id]
	if !ok {
		return nil, false
	}
	copyM := *m
	return &copyM, true
}

// CreateMonitor creates a new monitor directly in MySQL or fallback store.
func (s *Store) CreateMonitor(m models.Monitor) models.Monitor {
	if m.ID == "" {
		m.ID = fmt.Sprintf("mon-%d", time.Now().UnixNano())
	}
	if m.Status == "" {
		m.Status = "active"
	}
	if m.Uptime == "" {
		m.Uptime = "100%"
	}
	now := time.Now()
	m.CreatedAt = now
	m.UpdatedAt = now

	if s.mysql != nil {
		s.mysql.SaveMonitor(m)
		return m
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.monitors[m.ID] = &m
	return m
}

// UpdateMonitor updates a monitor directly in MySQL or fallback store.
func (s *Store) UpdateMonitor(m models.Monitor) bool {
	if s.mysql != nil {
		existing, err := s.mysql.GetMonitor(m.ID)
		if err != nil || existing == nil {
			return false
		}
		m.CreatedAt = existing.CreatedAt
		m.TotalPings = existing.TotalPings
		m.FailedPings = existing.FailedPings
		m.ConsecutiveFails = existing.ConsecutiveFails
		m.Uptime = existing.Uptime
		m.History = existing.History
		m.LastStatus = existing.LastStatus
		m.LastLatency = existing.LastLatency
		m.LastChecked = existing.LastChecked
		m.LastCheckedStr = existing.LastCheckedStr
		m.UpdatedAt = time.Now()
		s.mysql.SaveMonitor(m)
		return true
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.monitors[m.ID]
	if !ok {
		return false
	}
	m.CreatedAt = existing.CreatedAt
	m.TotalPings = existing.TotalPings
	m.FailedPings = existing.FailedPings
	m.ConsecutiveFails = existing.ConsecutiveFails
	m.Uptime = existing.Uptime
	m.History = existing.History
	m.LastStatus = existing.LastStatus
	m.LastLatency = existing.LastLatency
	m.LastChecked = existing.LastChecked
	m.LastCheckedStr = existing.LastCheckedStr
	m.UpdatedAt = time.Now()
	s.monitors[m.ID] = &m
	return true
}

// DeleteMonitor deletes a monitor and its associated logs and incidents directly from MySQL or fallback store.
func (s *Store) DeleteMonitor(id string) bool {
	if s.mysql != nil {
		s.mysql.DeleteMonitor(id)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.monitors, id)

	filteredLogs := make([]models.ExecutionLog, 0, len(s.logs))
	for _, l := range s.logs {
		if l.MonitorID != id {
			filteredLogs = append(filteredLogs, l)
		}
	}
	s.logs = filteredLogs

	for incID, inc := range s.incidents {
		if inc.MonitorID == id {
			delete(s.incidents, incID)
		}
	}

	return true
}

// ToggleMonitorStatus switches status directly in MySQL or fallback store.
func (s *Store) ToggleMonitorStatus(id string) (*models.Monitor, bool) {
	if s.mysql != nil {
		mon, err := s.mysql.ToggleMonitorStatus(id)
		if err == nil && mon != nil {
			return mon, true
		}
		return nil, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.monitors[id]
	if !ok {
		return nil, false
	}
	if m.Status == "active" {
		m.Status = "paused"
	} else {
		m.Status = "active"
	}
	m.UpdatedAt = time.Now()
	copyM := *m
	return &copyM, true
}

// RecordPingResult records the execution outcome and updates monitor stats in MySQL.
func (s *Store) RecordPingResult(log models.ExecutionLog) {
	// Sync directly to MySQL
	if s.mysql != nil {
		s.mysql.RecordLog(log)
		if !log.OK {
			// Deduplication: only open a new incident if there is NO active unresolved incident for this monitor!
			activeInc, err := s.mysql.GetActiveIncidentForMonitor(log.MonitorID)
			if err == nil && activeInc == nil {
				newInc := models.Incident{
					ID:           fmt.Sprintf("inc-%d", time.Now().UnixNano()),
					MonitorID:    log.MonitorID,
					Title:        fmt.Sprintf("Outage detected: %s", log.MonitorName),
					Service:      log.MonitorName,
					URL:          log.URL,
					Severity:     "P2 - High",
					Status:       "investigating",
					StartedAt:    time.Now(),
					StartedAtStr: "Just now",
					Duration:     "Ongoing",
					Cause:        log.Error,
					DispatchedTo: []string{"Engine Internal", "Default Webhook"},
				}
				s.mysql.SaveIncident(newInc)
				logger.Warn("Opened new incident [%s] for monitor [%s]: %s", newInc.ID, log.MonitorID, log.Error)
			}
		} else {
			// Probe succeeded: resolve any active ongoing incidents for this monitor
			_ = s.mysql.ResolveActiveIncidentsForMonitor(log.MonitorID)
		}
	}

	// Notify SSE subscribers
	s.broadcastLog(log)

	// In-memory fallback if no MySQL configured
	if s.mysql == nil {
		s.mu.Lock()
		m, ok := s.monitors[log.MonitorID]
		if ok {
			m.TotalPings++
			m.LastStatus = log.Status
			m.LastLatency = log.Latency
			now := time.Now()
			m.LastChecked = &now
			m.LastCheckedStr = "Just now"
			m.UpdatedAt = now

			if !log.OK {
				m.FailedPings++
				m.ConsecutiveFails++
				if m.ConsecutiveFails >= 2 {
					m.Status = "failing"
				}
			} else {
				m.ConsecutiveFails = 0
				if m.Status == "failing" {
					m.Status = "active"
				}
			}
		}

		s.totalRuns++
		s.totalLatency += log.Latency

		if len(s.logs) >= s.maxLogs {
			copy(s.logs[1:], s.logs[:s.maxLogs-1])
			s.logs[0] = log
		} else {
			s.logs = append([]models.ExecutionLog{log}, s.logs...)
		}
		s.mu.Unlock()
	}
}

// ==========================================
// LOG OPERATIONS (DIRECT DATABASE)
// ==========================================

// GetLogs returns paginated and filtered execution logs, reading directly from MySQL.
func (s *Store) GetLogs(limit, offset int, filter, search string, monitorID ...string) ([]models.ExecutionLog, int) {
	targetMonID := ""
	if len(monitorID) > 0 {
		targetMonID = strings.TrimSpace(monitorID[0])
	}

	if s.mysql != nil {
		logs, total, err := s.mysql.QueryLogs(targetMonID, filter, search, limit, offset)
		if err == nil {
			return logs, total
		}
		logger.Warn("Failed to query execution logs from MySQL: %v. Falling back to in-memory logs.", err)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var filtered []models.ExecutionLog
	searchLower := strings.ToLower(strings.TrimSpace(search))

	for _, l := range s.logs {
		if targetMonID != "" && l.MonitorID != targetMonID {
			continue
		}

		if filter == "error" || filter == "failed" || filter == "errors" {
			if l.OK {
				continue
			}
		} else if filter == "ok" || filter == "success" {
			if !l.OK {
				continue
			}
		}

		if searchLower != "" {
			matchName := strings.Contains(strings.ToLower(l.MonitorName), searchLower)
			matchURL := strings.Contains(strings.ToLower(l.URL), searchLower)
			matchID := strings.Contains(strings.ToLower(l.MonitorID), searchLower)
			if !matchName && !matchURL && !matchID {
				continue
			}
		}

		filtered = append(filtered, l)
	}

	total := len(filtered)
	if offset >= total {
		return []models.ExecutionLog{}, total
	}

	end := offset + limit
	if end > total {
		end = total
	}

	slice := filtered[offset:end]
	res := make([]models.ExecutionLog, len(slice))
	copy(res, slice)
	return res, total
}

// GetExecutionStats retrieves aggregated execution metrics directly from MySQL or fallback store.
func (s *Store) GetExecutionStats(monitorID string) models.ExecutionStats {
	if s.mysql != nil {
		if stats, err := s.mysql.GetExecutionStats(monitorID); err == nil {
			return stats
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var totalRuns, successCount, errorCount, totalLatency int64
	for _, l := range s.logs {
		if monitorID != "" && l.MonitorID != monitorID {
			continue
		}
		totalRuns++
		if l.OK {
			successCount++
		} else {
			errorCount++
		}
		totalLatency += l.Latency
	}

	avgLatency := int64(0)
	if totalRuns > 0 {
		avgLatency = totalLatency / totalRuns
	}

	successRate := "100.0%"
	if totalRuns > 0 {
		rate := (float64(successCount) / float64(totalRuns)) * 100.0
		successRate = fmt.Sprintf("%.1f%%", rate)
	}

	return models.ExecutionStats{
		TotalRuns:    totalRuns,
		SuccessCount: successCount,
		ErrorCount:   errorCount,
		SuccessRate:  successRate,
		AvgLatencyMs: avgLatency,
	}
}

// GetLatencyStats calculates average, min, max, and latest latency directly from MySQL or fallback store.
func (s *Store) GetLatencyStats(monitorID string) models.LatencyStats {
	if s.mysql != nil {
		if stats, err := s.mysql.GetLatencyStats(monitorID); err == nil {
			return stats
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	trimmedID := strings.TrimSpace(monitorID)
	var totalLatency, totalSamples int64
	var minLatency, maxLatency, lastLatency int64
	isFirst := true

	for _, l := range s.logs {
		if trimmedID != "" && l.MonitorID != trimmedID {
			continue
		}
		totalSamples++
		totalLatency += l.Latency
		if isFirst {
			minLatency = l.Latency
			maxLatency = l.Latency
			lastLatency = l.Latency
			isFirst = false
		} else {
			if l.Latency < minLatency {
				minLatency = l.Latency
			}
			if l.Latency > maxLatency {
				maxLatency = l.Latency
			}
		}
	}

	if trimmedID != "" {
		for _, m := range s.monitors {
			if m.ID == trimmedID {
				lastLatency = m.LastLatency
				break
			}
		}
	}

	avgLatency := int64(0)
	if totalSamples > 0 {
		avgLatency = totalLatency / totalSamples
	} else if lastLatency > 0 {
		avgLatency = lastLatency
		minLatency = lastLatency
		maxLatency = lastLatency
	}

	return models.LatencyStats{
		MonitorID:     trimmedID,
		AvgLatencyMs:  avgLatency,
		LastLatencyMs: lastLatency,
		MinLatencyMs:  minLatency,
		MaxLatencyMs:  maxLatency,
		TotalSamples:  totalSamples,
	}
}

// GetTimeline returns real-time timeline data points directly from MySQL or fallback store.
func (s *Store) GetTimeline(monitorID string, limit int) models.TimelineResponse {
	if s.mysql != nil {
		if resp, err := s.mysql.GetTimeline(monitorID, limit); err == nil {
			return resp
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	trimmedID := strings.TrimSpace(monitorID)
	if limit <= 0 || limit > 100 {
		limit = 30
	}

	var matchedLogs []models.ExecutionLog
	var totalRecorded, successCount int64

	for _, l := range s.logs {
		if trimmedID != "" && l.MonitorID != trimmedID {
			continue
		}
		totalRecorded++
		if l.OK {
			successCount++
		}
		matchedLogs = append(matchedLogs, l)
	}

	sla := "100.0%"
	if totalRecorded > 0 {
		rate := (float64(successCount) / float64(totalRecorded)) * 100.0
		sla = fmt.Sprintf("%.1f%%", rate)
	} else if trimmedID != "" {
		if m, ok := s.monitors[trimmedID]; ok {
			totalRecorded = m.TotalPings
			if m.Uptime != "" {
				sla = m.Uptime
			}
		}
	}

	start := 0
	if len(matchedLogs) > limit {
		start = len(matchedLogs) - limit
	}
	slice := matchedLogs[start:]

	timeline := make([]models.TimelineItem, 0, len(slice))
	for _, l := range slice {
		timeline = append(timeline, models.TimelineItem{
			ID:            l.ID,
			MonitorID:     l.MonitorID,
			Status:        l.Status,
			Latency:       l.Latency,
			OK:            l.OK,
			Timestamp:     l.Timestamp,
			FormattedTime: l.FormattedTime,
		})
	}

	return models.TimelineResponse{
		MonitorID:     trimmedID,
		TotalRecorded: totalRecorded,
		SLA:           sla,
		Timeline:      timeline,
	}
}

// ClearLogs empties the execution logs directly in MySQL or fallback store with optional filtering.
func (s *Store) ClearLogs(days int, monitorID string) int {
	if s.mysql != nil {
		return s.mysql.ClearLogs(days, monitorID)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	trimmedID := strings.TrimSpace(monitorID)
	cutoff := time.Time{}
	if days > 0 {
		cutoff = time.Now().AddDate(0, 0, -days)
	}

	var remaining []models.ExecutionLog
	cleared := 0
	for _, l := range s.logs {
		matchMonitor := trimmedID == "" || l.MonitorID == trimmedID
		matchTime := days <= 0 || l.Timestamp.Before(cutoff)
		if matchMonitor && matchTime {
			cleared++
		} else {
			remaining = append(remaining, l)
		}
	}
	s.logs = remaining
	return cleared
}

// ResetMonitorCounters resets total_pings, failed_pings, consecutive_fails, and uptime across monitors.
func (s *Store) ResetMonitorCounters(monitorID string) int {
	if s.mysql != nil {
		return s.mysql.ResetMonitorCounters(monitorID)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	trimmedID := strings.TrimSpace(monitorID)
	resetCount := 0
	for id, m := range s.monitors {
		if trimmedID == "" || id == trimmedID {
			m.TotalPings = 0
			m.FailedPings = 0
			m.ConsecutiveFails = 0
			m.Uptime = "100%"
			m.UpdatedAt = time.Now()
			s.monitors[id] = m
			resetCount++
		}
	}
	return resetCount
}

// ClearIncidents removes incidents directly in MySQL or fallback store.
func (s *Store) ClearIncidents(status string) int {
	if s.mysql != nil {
		return s.mysql.ClearIncidents(status)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	trimmedStatus := strings.ToLower(strings.TrimSpace(status))
	remaining := make(map[string]*models.Incident)
	cleared := 0
	for id, inc := range s.incidents {
		if trimmedStatus == "" || strings.ToLower(inc.Status) == trimmedStatus {
			cleared++
		} else {
			remaining[id] = inc
		}
	}
	s.incidents = remaining
	return cleared
}

// DeleteIncidentsByIDs removes multiple specific incidents by their IDs.
func (s *Store) DeleteIncidentsByIDs(ids []string) int {
	if s.mysql != nil {
		return s.mysql.DeleteIncidentsByIDs(ids)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	idMap := make(map[string]bool)
	for _, id := range ids {
		idMap[strings.TrimSpace(id)] = true
	}

	cleared := 0
	for id := range idMap {
		if _, ok := s.incidents[id]; ok {
			delete(s.incidents, id)
			cleared++
		}
	}
	return cleared
}

// RecordDailyCleanup logs the time and count of the daily midnight purge.
func (s *Store) RecordDailyCleanup(t time.Time, count int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastDailyCleanupTime = t
	s.lastDailyCleanupCount = count
}

// GetMaintenanceStats returns real-time storage metrics and table row counts.
func (s *Store) GetMaintenanceStats() models.MaintenanceStats {
	if s.mysql != nil {
		return s.mysql.GetMaintenanceStats()
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var oldest, newest time.Time
	if len(s.logs) > 0 {
		oldest = s.logs[0].Timestamp
		newest = s.logs[len(s.logs)-1].Timestamp
	}

	logsCount := int64(len(s.logs))
	mb := float64(logsCount*512) / (1024.0 * 1024.0)
	if mb < 0.01 && logsCount > 0 {
		mb = 0.01
	}

	oldestStr := ""
	newestStr := ""
	if !oldest.IsZero() {
		oldestStr = oldest.UTC().Format("2006-01-02 15:04:05")
	}
	if !newest.IsZero() {
		newestStr = newest.UTC().Format("2006-01-02 15:04:05")
	}

	now := time.Now()
	nextMidnight := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
	lastCleanupStr := ""
	if !s.lastDailyCleanupTime.IsZero() {
		lastCleanupStr = s.lastDailyCleanupTime.Format("2006-01-02 15:04:05")
	}

	return models.MaintenanceStats{
		TotalExecutionLogs:      logsCount,
		TotalIncidents:          int64(len(s.incidents)),
		TotalMonitors:           len(s.monitors),
		TotalBatchers:           len(s.batchers),
		OldestLogTimestamp:      oldestStr,
		NewestLogTimestamp:      newestStr,
		EstimatedStorageMB:      mb,
		DailyAutoCleanupEnabled: true,
		LastDailyCleanup:        lastCleanupStr,
		NextDailyCleanup:        nextMidnight.Format("2006-01-02 15:04:05"),
	}
}

// DeleteLog removes a single log directly in MySQL or fallback store.
func (s *Store) DeleteLog(id string) bool {
	if s.mysql != nil {
		return s.mysql.DeleteLog(id)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for i, l := range s.logs {
		if l.ID == id {
			s.logs = append(s.logs[:i], s.logs[i+1:]...)
			return true
		}
	}
	return false
}

// ==========================================
// BATCHER OPERATIONS (DIRECT DATABASE)
// ==========================================

// GetAllBatchers returns all batch workers directly from MySQL or fallback store.
func (s *Store) GetAllBatchers() []models.Batcher {
	if s.mysql != nil {
		if batchers, err := s.mysql.LoadBatchers(); err == nil {
			return batchers
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make([]models.Batcher, 0, len(s.batchers))
	for _, b := range s.batchers {
		res = append(res, *b)
	}
	return res
}

// GetBatcher returns a single batcher directly from MySQL or fallback store.
func (s *Store) GetBatcher(id string) (*models.Batcher, bool) {
	if s.mysql != nil {
		if b, err := s.mysql.GetBatcher(id); err == nil && b != nil {
			return b, true
		}
		return nil, false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.batchers[id]
	if !ok {
		return nil, false
	}
	copyB := *b
	return &copyB, true
}

// CreateBatcher creates a new batcher directly in MySQL or fallback store.
func (s *Store) CreateBatcher(b models.Batcher) models.Batcher {
	if b.ID == "" {
		b.ID = fmt.Sprintf("batch-%d", time.Now().UnixNano())
	}
	if b.Status == "" {
		b.Status = "active"
	}
	b.LastRun = time.Now()
	b.LastRunFormatted = "Never"

	if s.mysql != nil {
		s.mysql.SaveBatcher(b)
		return b
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.batchers[b.ID] = &b
	return b
}

// UpdateBatcher updates an existing batcher directly in MySQL or fallback store.
func (s *Store) UpdateBatcher(b models.Batcher) bool {
	if s.mysql != nil {
		existing, err := s.mysql.GetBatcher(b.ID)
		if err != nil || existing == nil {
			return false
		}
		b.LastRun = existing.LastRun
		b.LastRunFormatted = existing.LastRunFormatted
		s.mysql.SaveBatcher(b)
		return true
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.batchers[b.ID]
	if !ok {
		return false
	}
	b.LastRun = existing.LastRun
	b.LastRunFormatted = existing.LastRunFormatted
	s.batchers[b.ID] = &b
	return true
}

// DeleteBatcher removes a batcher directly from MySQL or fallback store.
func (s *Store) DeleteBatcher(id string) bool {
	if s.mysql != nil {
		s.mysql.DeleteBatcher(id)
		return true
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.batchers[id]; !ok {
		return false
	}
	delete(s.batchers, id)
	return true
}

// TriggerBatcher manually triggers a batch worker run directly in MySQL or fallback store.
func (s *Store) TriggerBatcher(id string) (*models.Batcher, bool) {
	if s.mysql != nil {
		b, err := s.mysql.TriggerBatcher(id)
		if err == nil && b != nil {
			return b, true
		}
		return nil, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.batchers[id]
	if !ok {
		return nil, false
	}
	b.LastRun = time.Now()
	b.LastRunFormatted = "Just now"
	copyB := *b
	return &copyB, true
}

// ==========================================
// INCIDENT OPERATIONS (DIRECT DATABASE)
// ==========================================

// GetAllIncidents returns all recorded incidents directly from MySQL or fallback store.
func (s *Store) GetAllIncidents() []models.Incident {
	if s.mysql != nil {
		if incidents, err := s.mysql.LoadIncidents(); err == nil {
			return incidents
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make([]models.Incident, 0, len(s.incidents))
	for _, inc := range s.incidents {
		res = append(res, *inc)
	}
	return res
}

// GetIncident returns a single incident directly from MySQL or fallback store.
func (s *Store) GetIncident(id string) (*models.Incident, bool) {
	if s.mysql != nil {
		if inc, err := s.mysql.GetIncident(id); err == nil && inc != nil {
			return inc, true
		}
		return nil, false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	inc, ok := s.incidents[id]
	if !ok {
		return nil, false
	}
	copyInc := *inc
	return &copyInc, true
}

// CreateIncident adds a new incident directly in MySQL or fallback store.
func (s *Store) CreateIncident(inc models.Incident) models.Incident {
	if inc.ID == "" {
		inc.ID = fmt.Sprintf("inc-%d-%d", time.Now().UnixNano(), atomic.AddInt64(&incidentSeq, 1))
	}
	if inc.Status == "" {
		inc.Status = "investigating"
	}
	now := time.Now()
	inc.StartedAt = now
	inc.StartedAtStr = "Just now"
	inc.Duration = "Ongoing"

	if s.mysql != nil {
		s.mysql.SaveIncident(inc)
		return inc
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.incidents[inc.ID] = &inc
	return inc
}

// UpdateIncident updates an existing incident directly in MySQL or fallback store.
func (s *Store) UpdateIncident(inc models.Incident) bool {
	if s.mysql != nil {
		existing, err := s.mysql.GetIncident(inc.ID)
		if err != nil || existing == nil {
			return false
		}
		inc.StartedAt = existing.StartedAt
		inc.StartedAtStr = existing.StartedAtStr
		s.mysql.SaveIncident(inc)
		return true
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.incidents[inc.ID]
	if !ok {
		return false
	}
	inc.StartedAt = existing.StartedAt
	inc.StartedAtStr = existing.StartedAtStr
	s.incidents[inc.ID] = &inc
	return true
}

// ResolveIncident marks an incident as resolved directly in MySQL or fallback store.
func (s *Store) ResolveIncident(id string) (*models.Incident, bool) {
	if s.mysql != nil {
		inc, err := s.mysql.ResolveIncident(id)
		if err == nil && inc != nil {
			return inc, true
		}
		return nil, false
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	inc, ok := s.incidents[id]
	if !ok {
		return nil, false
	}
	now := time.Now()
	inc.Status = "resolved"
	inc.ResolvedAt = &now
	inc.Duration = fmt.Sprintf("%dm", int(now.Sub(inc.StartedAt).Minutes()))
	copyInc := *inc
	return &copyInc, true
}

// DeleteIncident removes an incident directly from MySQL or fallback store.
func (s *Store) DeleteIncident(id string) bool {
	if s.mysql != nil {
		s.mysql.DeleteIncident(id)
		return true
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.incidents[id]; !ok {
		return false
	}
	delete(s.incidents, id)
	return true
}

// ==========================================
// TELEMETRY & METRICS (DIRECT DATABASE)
// ==========================================

// GetSystemStats calculates real-time telemetry metrics directly from MySQL or fallback store.
func (s *Store) GetSystemStats(activeWorkers int) models.SystemStats {
	if s.mysql != nil {
		if stats, err := s.mysql.GetSystemStats(activeWorkers, s.startTime); err == nil {
			return stats
		}
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	totalMonitors := len(s.monitors)
	activeMonitors := 0
	pausedMonitors := 0
	failingMonitors := 0

	for _, m := range s.monitors {
		switch strings.ToLower(m.Status) {
		case "active":
			activeMonitors++
		case "paused":
			pausedMonitors++
		case "failing":
			failingMonitors++
		}
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	uptimeSeconds := int64(time.Since(s.startTime).Seconds())
	avgLatency := int64(0)
	if s.totalRuns > 0 {
		avgLatency = s.totalLatency / s.totalRuns
	}

	successRate := "100%"
	totalFailed := int64(0)
	for _, mon := range s.monitors {
		totalFailed += mon.FailedPings
	}
	if s.totalRuns > 0 {
		sr := float64(s.totalRuns-totalFailed) / float64(s.totalRuns) * 100.0
		if sr < 0 {
			sr = 0
		}
		successRate = fmt.Sprintf("%.2f%%", sr)
	}

	return models.SystemStats{
		EngineStatus:    "running",
		UptimeSeconds:   uptimeSeconds,
		TotalMonitors:   totalMonitors,
		ActiveMonitors:  activeMonitors,
		PausedMonitors:  pausedMonitors,
		FailingMonitors: failingMonitors,
		TotalRuns:       s.totalRuns,
		SuccessRate:     successRate,
		AvgLatencyMs:    avgLatency,
		ActiveWorkers:   activeWorkers,
		Goroutines:      runtime.NumGoroutine(),
		MemoryAllocMB:   float64(m.Alloc) / 1024 / 1024,
	}
}

// ==========================================
// SSE STREAMING
// ==========================================

// SubscribeLogStream registers a listener channel for realtime SSE broadcast.
func (s *Store) SubscribeLogStream() (chan models.ExecutionLog, func()) {
	ch := make(chan models.ExecutionLog, 64)
	s.subMu.Lock()
	s.subscribers[ch] = struct{}{}
	s.subMu.Unlock()

	unsubscribe := func() {
		s.subMu.Lock()
		delete(s.subscribers, ch)
		close(ch)
		s.subMu.Unlock()
	}
	return ch, unsubscribe
}

func (s *Store) broadcastLog(log models.ExecutionLog) {
	s.subMu.RLock()
	defer s.subMu.RUnlock()

	for ch := range s.subscribers {
		select {
		case ch <- log:
		default:
			// Non-blocking drop if consumer client is lagging
		}
	}
}
