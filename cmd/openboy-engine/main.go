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
	"openboy-engine/internal/config"
	"openboy-engine/internal/db"
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
	// 1. Load secrets and defaults from .env
	cfg := config.Load()

	port := flag.Int("port", cfg.ServerPort, "HTTP REST & SSE API port")
	workers := flag.Int("workers", cfg.WorkerCount, "Number of concurrent probe worker goroutines")
	dataDir := flag.String("data-dir", cfg.DataDir, "Directory path for persistent JSON state snapshots")
	mysqlDSN := flag.String("mysql", cfg.MySQLDSN(), "MySQL DSN connection string (e.g. root:password@tcp(127.0.0.1:3306)/openboy?parseTime=true)")
	flag.Parse()

	fmt.Print(banner)

	dataFile := filepath.Join(*dataDir, "openboy_state.json")
	logger.Info("Bootstrapping OpenBoy Engine...")
	logger.Info("Configured worker pool concurrency: %d", *workers)

	// 2. Initialize MySQL or File Store
	var mysqlSync *store.MySQLSync
	if *mysqlDSN != "" {
		logger.Info("Connecting to MySQL: %s", *mysqlDSN)
		sqlDB, err := db.ConnectMySQL(*mysqlDSN)
		if err != nil {
			logger.Warn("Could not connect to MySQL (%v). Falling back to JSON state store: %s", err, dataFile)
			logger.Info("Configure database in .env (DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)")
		} else {
			if err := db.AutoMigrate(sqlDB); err != nil {
				logger.Warn("MySQL auto-migration failed: %v", err)
			}
			mysqlSync = store.NewMySQLSync(sqlDB)
		}
	} else {
		logger.Info("No MySQL configured. Using local state file: %s (Set DB_* in .env or pass -mysql)", dataFile)
	}

	s := store.NewStore(dataFile, mysqlSync)

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
