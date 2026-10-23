<div align="center">
  <img src="assets/logos/openboy.png" alt="OpenBoy Logo" width="380" />
  <p>High-Performance Health Probe Orchestrator, Queue Batcher & REST/SSE API</p>
</div>

---

## Overview

OpenBoy Engine is the core backend orchestrator for the OpenBoy monitoring platform. Built with Go (`go1.27.0`), it provides sub-second precision tick loops, dispatches multi-worker HTTP health checks, measures round-trip latencies, manages background batch workers and SLA incidents, handles JWT-secured operator authentication, and streams real-time execution telemetry via Server-Sent Events (SSE).

---

## Database & Environment Configuration

All database login credentials, ports, and authentication secrets are configured through `.env` in the project root and managed by the `internal/config` package.

A pre-configured template is provided in `.env.example`:

```env
# Server Network Port
SERVER_PORT=4040

# Concurrency Worker Pool (parallel probe workers)
WORKER_COUNT=15

# Persistent Local Snapshot Directory
DATA_DIR=./data

# MySQL Database Credentials
DB_HOST=127.0.0.1
DB_PORT=3306
DB_USER=root
DB_PASSWORD=
DB_NAME=openboy

# Optional Raw MySQL DSN override
# MYSQL_DSN=root:secret@tcp(127.0.0.1:3306)/openboy?parseTime=true&charset=utf8mb4

# JWT Authentication Secret
JWT_SECRET=openboy-super-secret-jwt-signing-key-2026
```

### Configuration Resolution Order
1. CLI flags (`-port`, `-workers`, `-data-dir`, `-mysql`)
2. Environment variables (`DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`, `MYSQL_DSN`)
3. `.env` file values
4. Safe internal defaults

---

## Architecture

```
openboy-engine/
├── assets/
│   └── logos/
│       ├── logo.png             # Icon logo
│       └── openboy.png          # Full banner logo
├── cmd/
│   └── openboy-engine/
│       └── main.go              # CLI flags, config loading, graceful shutdown
├── internal/
│   ├── config/
│   │   └── config.go            # .env file loader and MySQL DSN builder
│   ├── db/
│   │   └── mysql.go             # MySQL connection pool and automatic schema migrations
│   ├── models/
│   │   └── models.go            # Data models (User, Monitor, Batcher, Incident, Log)
│   ├── store/
│   │   ├── store.go             # Thread-safe in-memory store with periodic JSON snapshots
│   │   ├── mysql_sync.go        # Real-time MySQL synchronization and seeding
│   │   ├── seed.go              # Default starter monitors, batchers, and users
│   │   └── store_test.go        # Unit tests for store operations
│   ├── engine/
│   │   ├── prober.go            # HTTP health checker with TLS latency measurement
│   │   ├── worker_pool.go       # Concurrent worker pool with buffered job queues
│   │   └── scheduler.go         # 1-second master clock executing friendly cadences
│   ├── api/
│   │   ├── router.go            # Request router, CORS middleware, panic recovery
│   │   ├── handlers.go          # REST handlers for auth, monitors, batchers, incidents, logs
│   │   ├── auth_middleware.go   # JWT Bearer and API key verification
│   │   ├── sse.go               # Server-Sent Events live execution feed
│   │   └── api_test.go          # End-to-end HTTP integration tests
│   └── cronexpr/
│       ├── cron.go              # Pure Go 5-field cron parser and evaluator
│       └── cron_test.go         # Cron unit tests
├── pkg/
│   ├── auth/
│   │   ├── jwt.go               # HMAC-SHA256 JWT tokens & salted SHA-256 password hashing
│   │   └── jwt_test.go          # JWT generation & validation unit tests
│   └── logger/
│       └── logger.go            # Structured colored console logger
├── migrations/
│   └── schema.sql               # Relational SQL schema for PostgreSQL, SQLite, and MySQL
├── .env.example                 # Secrets template for MySQL and JWT
├── go.mod                       # Go module definition
└── .gitignore                   # Ignores compiled binaries and runtime state
```

---

## Getting Started

### 1. Build the Binary
```bash
go build -o bin/openboy-engine.exe ./cmd/openboy-engine/main.go
```

### 2. Run the Engine
```bash
# Uses settings from .env (auto-connects to MySQL if running, with JSON fallback):
./bin/openboy-engine.exe

# Override with custom MySQL credentials:
./bin/openboy-engine.exe -mysql "root:secret@tcp(127.0.0.1:3306)/openboy?parseTime=true"
```

### 3. Run All Tests
```bash
go test -v ./...
```

---

## Authentication & Default Credentials

- **Default Username:** `admin`
- **Default Password:** `admin`
- **Default Email:** `admin@openboy.dev`
- **Default Live API Key:** `ob_live_8f99e3a07c1264b91`

---

## API Endpoints

### Authentication
- `POST /api/auth/login` - Authenticate and receive signed JWT token
- `POST /api/auth/register` - Create new operator account
- `GET  /api/auth/me` - Get authenticated user profile (`Bearer <token>`)
- `PUT  /api/auth/profile` - Update profile name and email
- `PUT  /api/auth/password` - Change account password

### System Telemetry & SSE Stream
- `GET /api/health` - Health status, worker count, goroutines, and memory usage
- `GET /api/stats` - Telemetry cards (total runs, uptime, average latency)
- `GET /api/stream` - Live Server-Sent Events (SSE) feed of execution logs

### Monitors (Heartbeat & Health Checks)
- `GET    /api/monitors` - List all monitors (supports `?category=` and `?search=`)
- `POST   /api/monitors` - Create new monitor
- `GET    /api/monitors/:id` - Get monitor details
- `PUT    /api/monitors/:id` - Update monitor
- `DELETE /api/monitors/:id` - Delete monitor
- `POST   /api/monitors/:id/ping` - Trigger manual immediate probe
- `POST   /api/monitors/:id/toggle` - Pause or resume monitor

### Execution Logs
- `GET    /api/executions` - List execution audit traces (`?limit=50&offset=0&filter=ok|failed`)
- `DELETE /api/executions` - Clear execution logs buffer
- `DELETE /api/executions/:id` - Delete single log entry

### Batchers (Queue & ETL Workers)
- `GET    /api/batchers` - List background batchers
- `POST   /api/batchers` - Create new batcher
- `GET    /api/batchers/:id` - Get batcher
- `PUT    /api/batchers/:id` - Update batcher
- `DELETE /api/batchers/:id` - Delete batcher
- `POST   /api/batchers/:id/run` - Trigger batch worker manually

### Incidents (SLA Disruption Alerts)
- `GET    /api/incidents` - List all incidents
- `POST   /api/incidents` - Declare new incident
- `GET    /api/incidents/:id` - Get incident
- `PUT    /api/incidents/:id` - Update incident
- `POST   /api/incidents/:id/resolve` - Mark incident as resolved
- `DELETE /api/incidents/:id` - Delete incident
