package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"openboy-engine/internal/api"
	"openboy-engine/internal/engine"
	"openboy-engine/internal/store"
	"openboy-engine/pkg/logger"
)

const banner = `
   ___                   ____             ______            _            
  / _ \ _ __   ___ _ __ | __ )  ___  _   | ____|_ __   __ _(_)_ __   ___ 
 | | | | '_ \ / _ \ '_ \|  _ \ / _ \| |  |  _| | '_ \ / _` + "`" + ` | | '_ \ / _ \
 | |_| | |_) |  __/ | | | |_) | (_) |_|  | |___| | | | (_| | | | | |  __/
  \___/| .__/ \___|_| |_|____/ \___/(_)  |_____|_| |_|\__, |_|_| |_|\___|
       |_|                                            |___/              
   High-Frequency Cron & SLA Health Probe Orchestrator (Go Engine v1.0)
`

func main() {
	port := flag.Int("port", 8080, "HTTP REST & SSE API port")
	workers := flag.Int("workers", 15, "Number of concurrent probe worker goroutines")
	dataDir := flag.String("data-dir", "./data", "Directory path for persistent JSON state snapshots")
	flag.Parse()

	fmt.Print(banner)

	dataFile := filepath.Join(*dataDir, "openboy_state.json")
	logger.Info("Bootstrapping OpenBoy Engine...")
	logger.Info("State persistence file: %s", dataFile)
	logger.Info("Configured worker pool concurrency: %d", *workers)

	// 1. Initialize Store
	s := store.NewStore(dataFile)

	// 2. Initialize HTTP Prober
	prober := engine.NewProber()

	// 3. Initialize Worker Pool
	pool := engine.NewWorkerPool(*workers, s, prober)
	pool.Start()

	// 4. Initialize Scheduler
	scheduler := engine.NewScheduler(s, pool, prober)
	scheduler.Start()

	// 5. Initialize API and Router
	apiService := api.NewAPI(s, scheduler, pool)
	router := api.NewRouter(apiService)

	serverAddr := fmt.Sprintf(":%d", *port)
	server := &http.Server{
		Addr:         serverAddr,
		Handler:      router,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// 6. Background periodic state auto-saver (every 30 seconds)
	saverCtx, saverCancel := context.WithCancel(context.Background())
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-saverCtx.Done():
				return
			case <-ticker.C:
				if err := s.SaveToFile(); err != nil {
					logger.Warn("Periodic state save failed: %v", err)
				}
			}
		}
	}()

	// 7. Start HTTP Server
	go func() {
		logger.Info("OpenBoy REST & SSE Engine listening on http://localhost:%d", *port)
		logger.Info("SSE Event Stream accessible at http://localhost:%d/api/stream", *port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server failed: %v", err)
			os.Exit(1)
		}
	}()

	// 8. Graceful Shutdown listener
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	sig := <-sigChan
	logger.Warn("Received shutdown signal: %v. Initiating graceful shutdown...", sig)

	// Stop periodic saver
	saverCancel()

	// Stop scheduler (no new jobs queued)
	scheduler.Stop()

	// Drain worker pool
	pool.Stop()

	// Shutdown HTTP server with 5s deadline
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown error: %v", err)
	}

	// Final state flush to disk
	if err := s.SaveToFile(); err != nil {
		logger.Error("Failed to save final state on shutdown: %v", err)
	} else {
		logger.Info("Successfully persisted final state snapshot to %s", dataFile)
	}

	logger.Info("OpenBoy Engine terminated gracefully. Goodbye!")
}
