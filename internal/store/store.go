package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"openboy-engine/internal/models"
	"openboy-engine/pkg/logger"
)

type persistedState struct {
	Users     []models.User         `json:"users"`
	Monitors  []models.Monitor      `json:"monitors"`
	Batchers  []models.Batcher      `json:"batchers"`
	Incidents []models.Incident     `json:"incidents"`
	Logs      []models.ExecutionLog `json:"logs"`
}

// Store provides thread-safe access to users, monitors, batchers, incidents, and logs.
type Store struct {
	mu           sync.RWMutex
	users        map[string]*models.User // by ID
	monitors     map[string]*models.Monitor
	batchers     map[string]*models.Batcher
	incidents    map[string]*models.Incident
	logs         []models.ExecutionLog
	maxLogs      int
	filePath     string
	startTime    time.Time
	totalRuns    int64
	totalLatency int64
	subscribers  map[chan models.ExecutionLog]struct{}
	subMu        sync.RWMutex
	mysql        *MySQLSync
}

// NewStore initializes a store, loading from MySQL if connected or data/openboy_state.json if available.
func NewStore(dataFile string, mysqlDB ...*MySQLSync) *Store {
	s := &Store{
		users:       make(map[string]*models.User),
		monitors:    make(map[string]*models.Monitor),
		batchers:    make(map[string]*models.Batcher),
		incidents:   make(map[string]*models.Incident),
		logs:        make([]models.ExecutionLog, 0, 1000),
		maxLogs:     5000,
		filePath:    dataFile,
		startTime:   time.Now(),
		subscribers: make(map[chan models.ExecutionLog]struct{}),
	}

	if len(mysqlDB) > 0 && mysqlDB[0] != nil {
		s.mysql = mysqlDB[0]
		// Seed MySQL if empty and load state directly from MySQL
		if err := s.mysql.SeedIfEmpty(); err != nil {
			logger.Warn("Failed to seed MySQL: %v", err)
		}
		if users, err := s.mysql.LoadUsers(); err == nil && len(users) > 0 {
			logger.Info("Loaded %d users directly from MySQL database", len(users))
			for _, u := range users {
				copyU := u
				s.users[u.ID] = &copyU
			}
		}
		if mons, err := s.mysql.LoadMonitors(); err == nil && len(mons) > 0 {
			logger.Info("Loaded %d monitors directly from MySQL database", len(mons))
			for _, m := range mons {
				copyM := m
				s.monitors[m.ID] = &copyM
				s.totalRuns += m.TotalPings
				s.totalLatency += m.TotalPings * m.LastLatency
			}
		}
	}

	if len(s.monitors) == 0 {
		if err := s.loadFromFile(); err != nil {
			logger.Warn("Could not load previous state from %s (%v), bootstrapping default seed data", dataFile, err)
			s.bootstrapSeed()
		} else {
			logger.Info("Loaded state: %d users, %d monitors, %d batchers, %d incidents, %d logs from %s",
				len(s.users), len(s.monitors), len(s.batchers), len(s.incidents), len(s.logs), dataFile)
		}
	}

	// Always ensure default admin user is seeded if users map is empty
	if len(s.users) == 0 {
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
	if s.filePath == "" {
		return fmt.Errorf("no file path provided")
	}
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		return err
	}

	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, u := range state.Users {
		copyU := u
		s.users[u.ID] = &copyU
	}
	for _, m := range state.Monitors {
		copyM := m
		s.monitors[m.ID] = &copyM
		s.totalRuns += m.TotalPings
		s.totalLatency += m.TotalPings * m.LastLatency
	}
	for _, b := range state.Batchers {
		copyB := b
		s.batchers[b.ID] = &copyB
	}
	for _, inc := range state.Incidents {
		copyInc := inc
		s.incidents[inc.ID] = &copyInc
	}
	s.logs = state.Logs
	return nil
}

// SaveToFile persists current memory state to disk atomically.
func (s *Store) SaveToFile() error {
	if s.filePath == "" {
		return nil
	}

	s.mu.RLock()
	state := persistedState{
		Users:     make([]models.User, 0, len(s.users)),
		Monitors:  make([]models.Monitor, 0, len(s.monitors)),
		Batchers:  make([]models.Batcher, 0, len(s.batchers)),
		Incidents: make([]models.Incident, 0, len(s.incidents)),
		Logs:      s.logs,
	}
	for _, u := range s.users {
		state.Users = append(state.Users, *u)
	}
	for _, m := range s.monitors {
		state.Monitors = append(state.Monitors, *m)
	}
	for _, b := range s.batchers {
		state.Batchers = append(state.Batchers, *b)
	}
	for _, inc := range s.incidents {
		state.Incidents = append(state.Incidents, *inc)
	}
	s.mu.RUnlock()

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}

	dir := filepath.Dir(s.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	tmpFile := s.filePath + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpFile, s.filePath)
}

// ==========================================
// USER OPERATIONS
// ==========================================

// GetUserByID retrieves a user by ID.
func (s *Store) GetUserByID(id string) (*models.User, bool) {
	s.mu.RLock()
	if len(s.users) == 0 {
		s.mu.RUnlock()
		s.bootstrapUsers()
		s.mu.RLock()
	}
	defer s.mu.RUnlock()

	u, ok := s.users[id]
	if !ok {
		return nil, false
	}
	copyU := *u
	return &copyU, true
}

// GetUserByUsernameOrEmail finds a user by username or email (case-insensitive).
func (s *Store) GetUserByUsernameOrEmail(identifier string) (*models.User, bool) {
	s.mu.RLock()
	if len(s.users) == 0 {
		s.mu.RUnlock()
		s.bootstrapUsers()
		s.mu.RLock()
	}
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

// GetUserByAPIKey finds a user by API key.
func (s *Store) GetUserByAPIKey(key string) (*models.User, bool) {
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

// CreateUser inserts a new user.
func (s *Store) CreateUser(u models.User) (*models.User, error) {
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

	if u.ID == "" {
		u.ID = fmt.Sprintf("usr-%d", time.Now().UnixNano())
	}
	now := time.Now()
	u.CreatedAt = now
	u.UpdatedAt = now

	s.users[u.ID] = &u
	copyU := u
	if s.mysql != nil {
		go s.mysql.SaveUser(copyU)
	}
	return &copyU, nil
}

// UpdateUser modifies user profile fields (name, email).
func (s *Store) UpdateUser(u models.User) bool {
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
	copyU := *existing
	if s.mysql != nil {
		go s.mysql.SaveUser(copyU)
	}
	return true
}

// UpdatePassword sets a new password hash and salt for a user.
func (s *Store) UpdatePassword(userID, newHash, newSalt string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	user, ok := s.users[userID]
	if !ok {
		return false
	}
	user.PasswordHash = newHash
	user.PasswordSalt = newSalt
	user.UpdatedAt = time.Now()
	copyU := *user
	if s.mysql != nil {
		go s.mysql.SaveUser(copyU)
	}
	return true
}

// ==========================================
// MONITOR OPERATIONS
// ==========================================

// GetAllMonitors returns a slice copy of all registered monitors.
func (s *Store) GetAllMonitors() []models.Monitor {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]models.Monitor, 0, len(s.monitors))
	for _, m := range s.monitors {
		result = append(result, *m)
	}
	return result
}

// GetMonitor retrieves a single monitor by ID.
func (s *Store) GetMonitor(id string) (*models.Monitor, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	m, ok := s.monitors[id]
	if !ok {
		return nil, false
	}
	copyM := *m
	return &copyM, true
}

// CreateMonitor creates a new monitor.
func (s *Store) CreateMonitor(m models.Monitor) models.Monitor {
	s.mu.Lock()
	defer s.mu.Unlock()

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

	s.monitors[m.ID] = &m
	if s.mysql != nil {
		go s.mysql.SaveMonitor(m)
	}
	return m
}

// UpdateMonitor updates an existing monitor.
func (s *Store) UpdateMonitor(m models.Monitor) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, ok := s.monitors[m.ID]
	if !ok {
		return false
	}

	// Preserve stats
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
	if s.mysql != nil {
		go s.mysql.SaveMonitor(m)
	}
	return true
}

// DeleteMonitor deletes a monitor by ID.
func (s *Store) DeleteMonitor(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.monitors[id]; !ok {
		return false
	}
	delete(s.monitors, id)
	if s.mysql != nil {
		go s.mysql.DeleteMonitor(id)
	}
	return true
}

// ToggleMonitorStatus switches status between active and paused.
func (s *Store) ToggleMonitorStatus(id string) (*models.Monitor, bool) {
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
	if s.mysql != nil {
		go s.mysql.SaveMonitor(copyM)
	}
	return &copyM, true
}

// RecordPingResult records the execution outcome and updates monitor stats.
func (s *Store) RecordPingResult(log models.ExecutionLog) {
	s.mu.Lock()
	m, ok := s.monitors[log.MonitorID]
	if ok {
		m.TotalPings++
		m.LastStatus = log.Status
		m.LastLatency = log.Latency
		now := log.Timestamp
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

		// Calculate uptime %
		if m.TotalPings > 0 {
			success := float64(m.TotalPings-m.FailedPings) / float64(m.TotalPings) * 100.0
			m.Uptime = fmt.Sprintf("%.2f%%", success)
		}

		// Update history sparkline (keep last 5)
		histItem := models.HistoryItem{
			Status:  log.Status,
			Latency: log.Latency,
			Time:    "Just now",
			OK:      log.OK,
		}
		m.History = append([]models.HistoryItem{histItem}, m.History...)
		if len(m.History) > 5 {
			m.History = m.History[:5]
		}
	}

	s.totalRuns++
	s.totalLatency += log.Latency

	// Prepend log to ring buffer
	s.logs = append([]models.ExecutionLog{log}, s.logs...)
	if len(s.logs) > s.maxLogs {
		s.logs = s.logs[:s.maxLogs]
	}
	s.mu.Unlock()

	// Notify SSE subscribers
	s.broadcastLog(log)

	// Sync to MySQL
	if s.mysql != nil {
		go s.mysql.RecordLog(log)
	}
}

// ==========================================
// LOG OPERATIONS
// ==========================================

// GetLogs returns paginated and filtered execution logs.
func (s *Store) GetLogs(limit, offset int, filter, search string) ([]models.ExecutionLog, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var filtered []models.ExecutionLog
	searchLower := strings.ToLower(strings.TrimSpace(search))

	for _, l := range s.logs {
		if filter == "error" || filter == "failed" {
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

// ClearLogs empties the execution logs ring buffer.
func (s *Store) ClearLogs() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := len(s.logs)
	s.logs = make([]models.ExecutionLog, 0, 1000)
	return count
}

// DeleteLog removes a single log by ID.
func (s *Store) DeleteLog(id string) bool {
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
// BATCHER OPERATIONS
// ==========================================

// GetAllBatchers returns all batch workers.
func (s *Store) GetAllBatchers() []models.Batcher {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make([]models.Batcher, 0, len(s.batchers))
	for _, b := range s.batchers {
		res = append(res, *b)
	}
	return res
}

// GetBatcher returns a single batcher by ID.
func (s *Store) GetBatcher(id string) (*models.Batcher, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	b, ok := s.batchers[id]
	if !ok {
		return nil, false
	}
	copyB := *b
	return &copyB, true
}

// CreateBatcher creates a new batcher.
func (s *Store) CreateBatcher(b models.Batcher) models.Batcher {
	s.mu.Lock()
	defer s.mu.Unlock()

	if b.ID == "" {
		b.ID = fmt.Sprintf("batch-%d", time.Now().UnixNano())
	}
	if b.Status == "" {
		b.Status = "active"
	}
	b.LastRun = time.Now()
	b.LastRunFormatted = "Never"

	s.batchers[b.ID] = &b
	return b
}

// UpdateBatcher updates an existing batcher.
func (s *Store) UpdateBatcher(b models.Batcher) bool {
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

// DeleteBatcher removes a batcher.
func (s *Store) DeleteBatcher(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.batchers[id]; !ok {
		return false
	}
	delete(s.batchers, id)
	return true
}

// TriggerBatcher manually triggers a batch worker run.
func (s *Store) TriggerBatcher(id string) (*models.Batcher, bool) {
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
// INCIDENT OPERATIONS
// ==========================================

// GetAllIncidents returns all recorded incidents.
func (s *Store) GetAllIncidents() []models.Incident {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make([]models.Incident, 0, len(s.incidents))
	for _, inc := range s.incidents {
		res = append(res, *inc)
	}
	return res
}

// GetIncident returns a single incident by ID.
func (s *Store) GetIncident(id string) (*models.Incident, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	inc, ok := s.incidents[id]
	if !ok {
		return nil, false
	}
	copyInc := *inc
	return &copyInc, true
}

// CreateIncident adds a new incident to the system.
func (s *Store) CreateIncident(inc models.Incident) models.Incident {
	s.mu.Lock()
	defer s.mu.Unlock()

	if inc.ID == "" {
		inc.ID = fmt.Sprintf("inc-%d", time.Now().UnixNano())
	}
	if inc.Status == "" {
		inc.Status = "investigating"
	}
	now := time.Now()
	inc.StartedAt = now
	inc.StartedAtStr = "Just now"
	inc.Duration = "Ongoing"

	s.incidents[inc.ID] = &inc
	return inc
}

// UpdateIncident updates an existing incident.
func (s *Store) UpdateIncident(inc models.Incident) bool {
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

// ResolveIncident marks an incident as resolved.
func (s *Store) ResolveIncident(id string) (*models.Incident, bool) {
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

// DeleteIncident removes an incident.
func (s *Store) DeleteIncident(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.incidents[id]; !ok {
		return false
	}
	delete(s.incidents, id)
	return true
}

// ==========================================
// TELEMETRY & METRICS
// ==========================================

// GetSystemStats calculates real-time telemetry metrics.
func (s *Store) GetSystemStats(activeWorkers int) models.SystemStats {
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
		MemoryAllocMB:   float64(m.Alloc) / 1024.0 / 1024.0,
	}
}

// SubscribeLogStream adds a channel to receive live execution logs.
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
			// Dropping if subscriber is too slow to prevent blocking
		}
	}
}
