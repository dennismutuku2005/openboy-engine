package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"openboy-engine/pkg/logger"
)

// StreamHandler streams live execution logs and heartbeats using Server-Sent Events (SSE).
func (a *API) StreamHandler(c *gin.Context) {
	w := c.Writer
	flusher, ok := w.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Streaming unsupported"})
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Flush()

	logChan, unsubscribe := a.store.SubscribeLogStream()
	defer unsubscribe()

	logger.Info("SSE client connected from %s", c.Request.RemoteAddr)

	// Send initial greeting
	stats := a.store.GetSystemStats(a.pool.ActiveWorkers())
	initialData, _ := json.Marshal(map[string]interface{}{
		"type":  "connected",
		"stats": stats,
		"time":  time.Now().Format(time.RFC3339),
	})
	fmt.Fprintf(w, "data: %s\n\n", initialData)
	flusher.Flush()

	// Keep-alive heartbeat ticker every 15s
	keepAlive := time.NewTicker(15 * time.Second)
	defer keepAlive.Stop()

	notify := c.Request.Context().Done()
	for {
		select {
		case <-notify:
			logger.Info("SSE client disconnected: %s", c.Request.RemoteAddr)
			return
		case log, ok := <-logChan:
			if !ok {
				return
			}
			payload, err := json.Marshal(map[string]interface{}{
				"type": "execution",
				"log":  log,
			})
			if err == nil {
				fmt.Fprintf(w, "data: %s\n\n", payload)
				flusher.Flush()
			}
		case <-keepAlive.C:
			// Ping heartbeat
			fmt.Fprintf(w, ": heartbeat\n\n")
			flusher.Flush()
		}
	}
}
