package engine

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"openboy-engine/internal/models"
)

var logCounter uint64

func nextLogID() string {
	seq := atomic.AddUint64(&logCounter, 1)
	return fmt.Sprintf("log-%d-%d", time.Now().UnixNano(), seq)
}

// Prober executes HTTP health checks and measures performance metrics.
type Prober struct {
	client *http.Client
}

// NewProber initializes a Prober with custom transport and connection pooling.
func NewProber() *Prober {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 20,
		IdleConnTimeout:     90 * time.Second,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // Allow self-signed test endpoints
		},
		DisableKeepAlives: false,
	}

	return &Prober{
		client: &http.Client{
			Transport: transport,
			Timeout:   15 * time.Second,
		},
	}
}

// Probe executes a real network health check for the given monitor.
func (p *Prober) Probe(ctx context.Context, m *models.Monitor) models.ExecutionLog {
	start := time.Now()

	timeout := time.Duration(m.Timeout) * time.Second
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Real network probe
	var bodyReader io.Reader
	if m.Body != "" && (m.Method == "POST" || m.Method == "PUT" || m.Method == "PATCH") {
		bodyReader = strings.NewReader(m.Body)
	}

	req, err := http.NewRequestWithContext(ctx, m.Method, m.URL, bodyReader)
	if err != nil {
		latency := time.Since(start).Milliseconds()
		return models.ExecutionLog{
			ID:            fmt.Sprintf("log-%d", time.Now().UnixNano()),
			MonitorID:     m.ID,
			MonitorName:   m.Name,
			URL:           m.URL,
			Method:        m.Method,
			Status:        0,
			StatusText:    "Request Error",
			Latency:       latency,
			Timestamp:     start,
			FormattedTime: start.Format("15:04:05"),
			OK:            false,
			Error:         fmt.Sprintf("Failed to construct request: %v", err),
		}
	}

	// Add custom headers
	for k, v := range m.Headers {
		req.Header.Set(k, v)
	}
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", "OpenBoy-Engine/1.0 (High-Frequency Health Prober)")
	}

	resp, err := p.client.Do(req)
	latency := time.Since(start).Milliseconds()

	if err != nil {
		statusText := "Connection Error"
		if strings.Contains(err.Error(), "no such host") {
			statusText = "DNS NXDOMAIN (Host Not Found)"
		} else if strings.Contains(err.Error(), "connection refused") {
			statusText = "Connection Refused"
		} else if strings.Contains(err.Error(), "context deadline exceeded") || strings.Contains(err.Error(), "timeout") {
			statusText = "Request Timeout"
		}

		return models.ExecutionLog{
			ID:            nextLogID(),
			MonitorID:     m.ID,
			MonitorName:   m.Name,
			URL:           m.URL,
			Method:        m.Method,
			Status:        0,
			StatusText:    statusText,
			Latency:       latency,
			Timestamp:     start,
			FormattedTime: start.Format("15:04:05"),
			OK:            false,
			Error:         err.Error(),
		}
	}
	defer resp.Body.Close()

	// Read small snippet of body (up to 2KB)
	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	bodySnippet := string(bodyBytes)

	expected := m.ExpectedStatus
	if expected <= 0 {
		expected = 200
	}
	isOK := resp.StatusCode == expected

	return models.ExecutionLog{
		ID:            nextLogID(),
		MonitorID:     m.ID,
		MonitorName:   m.Name,
		URL:           m.URL,
		Method:        m.Method,
		Status:        resp.StatusCode,
		StatusText:    http.StatusText(resp.StatusCode),
		Latency:       latency,
		Timestamp:     start,
		FormattedTime: start.Format("15:04:05"),
		OK:            isOK,
		ResponseBody:  bodySnippet,
	}
}
