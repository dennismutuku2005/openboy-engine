package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"openboy-engine/pkg/logger"
)

// ConnectMySQL opens a connection pool to MySQL, auto-creating the database if needed, and verifies connectivity.
func ConnectMySQL(dsn string) (*sql.DB, error) {
	if dsn == "" {
		return nil, fmt.Errorf("empty MySQL DSN provided")
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open MySQL driver: %w", err)
	}

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()

		// If the database doesn't exist, connect to server root and create it!
		if strings.Contains(strings.ToLower(err.Error()), "unknown database") {
			baseDSN, dbName := splitDSNDatabase(dsn)
			if dbName != "" {
				logger.Info("Database '%s' does not exist yet. Creating database automatically...", dbName)
				if createErr := createDatabaseIfNotExists(baseDSN, dbName); createErr == nil {
					// Reconnect to newly created database
					return sql.Open("mysql", dsn)
				}
			}
		}
		return nil, fmt.Errorf("failed to ping MySQL server: %w", err)
	}

	logger.Info("Successfully connected to MySQL database!")
	return db, nil
}

func splitDSNDatabase(dsn string) (string, string) {
	slashIdx := strings.LastIndex(dsn, "/")
	if slashIdx == -1 {
		return dsn, ""
	}
	afterSlash := dsn[slashIdx+1:]
	qIdx := strings.Index(afterSlash, "?")
	var dbName string
	var params string
	if qIdx == -1 {
		dbName = afterSlash
	} else {
		dbName = afterSlash[:qIdx]
		params = afterSlash[qIdx:]
	}
	baseDSN := dsn[:slashIdx+1] + params
	return baseDSN, dbName
}

func createDatabaseIfNotExists(baseDSN, dbName string) error {
	rootDB, err := sql.Open("mysql", baseDSN)
	if err != nil {
		return err
	}
	defer rootDB.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;", dbName)
	_, err = rootDB.ExecContext(ctx, query)
	return err
}

// AutoMigrate creates all necessary tables in MySQL if they do not already exist.
func AutoMigrate(db *sql.DB) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS users (
			id VARCHAR(64) PRIMARY KEY,
			username VARCHAR(64) NOT NULL UNIQUE,
			email VARCHAR(128) NOT NULL UNIQUE,
			password_hash VARCHAR(128) NOT NULL,
			password_salt VARCHAR(64) NOT NULL,
			name VARCHAR(128) NOT NULL,
			role VARCHAR(32) NOT NULL DEFAULT 'user',
			api_key VARCHAR(128) NOT NULL UNIQUE,
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,

		`CREATE TABLE IF NOT EXISTS monitors (
			id VARCHAR(64) PRIMARY KEY,
			user_id VARCHAR(64),
			name VARCHAR(128) NOT NULL,
			url TEXT NOT NULL,
			method VARCHAR(16) NOT NULL DEFAULT 'GET',
			interval_val VARCHAR(32) NOT NULL DEFAULT '30s',
			interval_seconds INT NOT NULL DEFAULT 30,
			interval_label VARCHAR(64),
			cron_expr VARCHAR(64),
			status VARCHAR(32) NOT NULL DEFAULT 'active',
			category VARCHAR(64) DEFAULT 'uptime',
			notifications VARCHAR(64) DEFAULT 'defaults',
			headers_json TEXT,
			body TEXT,
			expected_status INT NOT NULL DEFAULT 200,
			timeout_sec INT NOT NULL DEFAULT 10,
			last_status INT DEFAULT 0,
			last_latency_ms BIGINT DEFAULT 0,
			last_checked_at DATETIME NULL,
			uptime_pct VARCHAR(16) DEFAULT '100%',
			total_pings BIGINT NOT NULL DEFAULT 0,
			failed_pings BIGINT NOT NULL DEFAULT 0,
			consecutive_fails INT NOT NULL DEFAULT 0,
			created_at DATETIME NOT NULL,
			updated_at DATETIME NOT NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,

		`CREATE TABLE IF NOT EXISTS execution_logs (
			id VARCHAR(64) PRIMARY KEY,
			monitor_id VARCHAR(64) NOT NULL,
			monitor_name VARCHAR(128) NOT NULL,
			url TEXT NOT NULL,
			method VARCHAR(16) NOT NULL,
			status INT NOT NULL,
			status_text VARCHAR(64),
			latency_ms BIGINT NOT NULL,
			timestamp DATETIME NOT NULL,
			is_ok BOOLEAN NOT NULL DEFAULT TRUE,
			error_message TEXT,
			response_body TEXT,
			INDEX idx_log_monitor (monitor_id),
			INDEX idx_log_time (timestamp DESC)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,

		`CREATE TABLE IF NOT EXISTS batchers (
			id VARCHAR(64) PRIMARY KEY,
			name VARCHAR(128) NOT NULL,
			job_key VARCHAR(128) NOT NULL UNIQUE,
			schedule VARCHAR(64) NOT NULL,
			cron_expr VARCHAR(64) NOT NULL,
			status VARCHAR(32) NOT NULL DEFAULT 'active',
			concurrency VARCHAR(64) NOT NULL DEFAULT 'Mutex Locked',
			last_run DATETIME NOT NULL,
			duration VARCHAR(32),
			records_processed VARCHAR(64),
			target_queue VARCHAR(128),
			description TEXT
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,

		`CREATE TABLE IF NOT EXISTS incidents (
			id VARCHAR(64) PRIMARY KEY,
			monitor_id VARCHAR(64),
			title VARCHAR(256) NOT NULL,
			service VARCHAR(128) NOT NULL,
			url TEXT NOT NULL,
			severity VARCHAR(32) NOT NULL,
			status VARCHAR(32) NOT NULL DEFAULT 'investigating',
			started_at DATETIME NOT NULL,
			duration VARCHAR(64),
			cause TEXT,
			dispatched_to_json TEXT,
			resolved_at DATETIME NULL
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,
	}

	for _, q := range queries {
		if _, err := db.Exec(q); err != nil {
			return fmt.Errorf("migration query failed: %w", err)
		}
	}

	logger.Info("MySQL tables verified and migrated successfully")
	return nil
}
