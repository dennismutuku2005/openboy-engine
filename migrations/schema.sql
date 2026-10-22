-- ==============================================================================
-- OpenBoy Engine Relational Database Schema
-- Compatible with PostgreSQL, SQLite, and MySQL
-- ==============================================================================

-- 1. Users & Authentication
CREATE TABLE IF NOT EXISTS users (
    id VARCHAR(64) PRIMARY KEY,
    username VARCHAR(64) NOT NULL UNIQUE,
    email VARCHAR(128) NOT NULL UNIQUE,
    password_hash VARCHAR(128) NOT NULL,
    password_salt VARCHAR(64) NOT NULL,
    name VARCHAR(128) NOT NULL,
    role VARCHAR(32) NOT NULL DEFAULT 'user',
    api_key VARCHAR(128) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_api_key ON users(api_key);

-- 2. Monitored Cron & Health Endpoints
CREATE TABLE IF NOT EXISTS monitors (
    id VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(64),
    name VARCHAR(128) NOT NULL,
    url TEXT NOT NULL,
    method VARCHAR(16) NOT NULL DEFAULT 'GET',
    interval VARCHAR(32) NOT NULL DEFAULT '30s',
    interval_seconds INTEGER NOT NULL DEFAULT 30,
    interval_label VARCHAR(64),
    cron_expr VARCHAR(64),
    status VARCHAR(32) NOT NULL DEFAULT 'active', -- 'active', 'paused', 'failing'
    category VARCHAR(64) DEFAULT 'uptime',
    notifications VARCHAR(64) DEFAULT 'defaults',
    headers_json TEXT,
    body TEXT,
    expected_status INTEGER NOT NULL DEFAULT 200,
    timeout_sec INTEGER NOT NULL DEFAULT 10,
    last_status INTEGER DEFAULT 0,
    last_latency_ms BIGINT DEFAULT 0,
    last_checked_at TIMESTAMP,
    uptime_pct VARCHAR(16) DEFAULT '100%',
    total_pings BIGINT NOT NULL DEFAULT 0,
    failed_pings BIGINT NOT NULL DEFAULT 0,
    consecutive_fails INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    CONSTRAINT fk_monitors_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_monitors_status ON monitors(status);
CREATE INDEX IF NOT EXISTS idx_monitors_user_id ON monitors(user_id);
CREATE INDEX IF NOT EXISTS idx_monitors_interval ON monitors(interval_seconds);

-- 3. Execution Logs & Audit Traces
CREATE TABLE IF NOT EXISTS execution_logs (
    id VARCHAR(64) PRIMARY KEY,
    monitor_id VARCHAR(64) NOT NULL,
    monitor_name VARCHAR(128) NOT NULL,
    url TEXT NOT NULL,
    method VARCHAR(16) NOT NULL,
    status INTEGER NOT NULL,
    status_text VARCHAR(64),
    latency_ms BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    is_ok BOOLEAN NOT NULL DEFAULT TRUE,
    error_message TEXT,
    response_body TEXT,
    CONSTRAINT fk_logs_monitor FOREIGN KEY (monitor_id) REFERENCES monitors(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_logs_monitor_id ON execution_logs(monitor_id);
CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON execution_logs(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_logs_is_ok ON execution_logs(is_ok);

-- 4. Background Batchers & Queue Processors
CREATE TABLE IF NOT EXISTS batchers (
    id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    job_key VARCHAR(128) NOT NULL UNIQUE,
    schedule VARCHAR(64) NOT NULL,
    cron_expr VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'active', -- 'active', 'paused', 'running'
    concurrency VARCHAR(64) NOT NULL DEFAULT 'Mutex Locked',
    last_run TIMESTAMP NOT NULL,
    duration VARCHAR(32),
    records_processed VARCHAR(64),
    target_queue VARCHAR(128),
    description TEXT
);

CREATE INDEX IF NOT EXISTS idx_batchers_status ON batchers(status);
CREATE INDEX IF NOT EXISTS idx_batchers_job_key ON batchers(job_key);

-- 5. Incidents & SLA Downtime Alerts
CREATE TABLE IF NOT EXISTS incidents (
    id VARCHAR(64) PRIMARY KEY,
    monitor_id VARCHAR(64),
    title VARCHAR(256) NOT NULL,
    service VARCHAR(128) NOT NULL,
    url TEXT NOT NULL,
    severity VARCHAR(32) NOT NULL, -- 'P1 - Critical', 'P2 - High', 'P3 - Moderate'
    status VARCHAR(32) NOT NULL DEFAULT 'investigating', -- 'investigating', 'resolved'
    started_at TIMESTAMP NOT NULL,
    duration VARCHAR(64),
    cause TEXT,
    dispatched_to_json TEXT,
    resolved_at TIMESTAMP,
    CONSTRAINT fk_incidents_monitor FOREIGN KEY (monitor_id) REFERENCES monitors(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_incidents_status ON incidents(status);
CREATE INDEX IF NOT EXISTS idx_incidents_severity ON incidents(severity);
