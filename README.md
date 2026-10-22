# OpenBoy Engine (Go Backend)

High-performance, concurrent health probe orchestrator, background queue batcher, and REST/SSE API engine built in Go (`go1.27.0`).

---

## ⚡ Overview

OpenBoy Engine is the core orchestration backend for the OpenBoy monitoring platform. It runs autonomous, sub-second precision tick loops, dispatches multi-worker HTTP health checks, measures round-trip latencies with millisecond precision, manages batch workers and incidents, provides JWT-secured authentication, and streams live telemetry via Server-Sent Events (SSE).

---

## 🏗 Architecture & Components

```
openboy-engine/
├── cmd/
│   └── openboy-engine/
│       └── main.go              # CLI flags, engine bootstrap, graceful shutdown
├── internal/
│   ├── models/
│   │   └── models.go            # Domain models (User, Monitor, Batcher, Incident, Log)
│   ├── store/
│   │   ├── store.go             # Thread-safe in-memory store (RWMutex, atomic JSON snapshot)
│   │   ├── seed.go              # Seed data for monitors, batchers, incidents, and default users
│   │   └── store_test.go        # Store CRUD and metrics unit tests
│   ├── engine/
│   │   ├── prober.go            # Precision HTTP prober with TLS latency measurement
│   │   ├── worker_pool.go       # Concurrency worker pool with buffered job queues
│   │   └── scheduler.go         # 1-second master clock executing friendly schedules & intervals
│   ├── api/
│   │   ├── router.go            # Request router, CORS middleware, panic recovery
│   │   ├── handlers.go          # REST handlers for auth, monitors, batchers, incidents, logs
│   │   ├── auth_middleware.go   # JWT & API key verification middleware
│   │   ├── sse.go               # Server-Sent Events real-time log streamer
│   │   └── api_test.go          # End-to-end HTTP integration tests
│   └── cronexpr/
│       ├── cron.go              # Pure Go 5-field cron parser & evaluator
│       └── cron_test.go         # Cron unit tests
├── pkg/
│   ├── auth/
│   │   ├── jwt.go               # HMAC-SHA256 JWT tokens & salted SHA-256 password hashing
│   │   └── jwt_test.go          # JWT generation & validation unit tests
│   └── logger/
│       └── logger.go            # Structured colored console logger
├── migrations/
│   └── schema.sql               # Relational SQL schema for PostgreSQL, SQLite, and MySQL
├── go.mod                       # Go module definition (zero heavy CGO dependencies)
└── .gitignore                   # Ignores bin/, executables, and temporary state files
```

---

## 🚀 Getting Started

### 1. Build the Binary
```bash
go build -o bin/openboy-engine.exe ./cmd/openboy-engine/main.go
```

### 2. Run the Engine
```bash
# Default port 8080 with 15 concurrent workers (In-memory + JSON snapshot):
./bin/openboy-engine.exe

# Connect directly to a live MySQL Database (Auto-migrates tables and queries MySQL):
./bin/openboy-engine.exe -mysql "root:password@tcp(127.0.0.1:3306)/openboy?parseTime=true"

# Or configure via environment variable:
export MYSQL_DSN="root:password@tcp(127.0.0.1:3306)/openboy?parseTime=true"
./bin/openboy-engine.exe

# Custom configuration with workers & data snapshot dir:
./bin/openboy-engine.exe -port 8080 -workers 20 -data-dir ./data -mysql "root:password@tcp(127.0.0.1:3306)/openboy?parseTime=true"
```

### 3. Run Unit & Integration Tests
```bash
go test -v ./...
```

---

## 🔒 Authentication & Default Credentials

OpenBoy Engine features built-in JWT authentication:
- **Default Username:** `admin`
- **Default Password:** `admin`
- **Default Email:** `admin@openboy.dev`
- **Default API Key:** `ob_live_8f99e3a07c1264b91`

---

## 📡 API Specification

### Authentication
- `POST /api/auth/login` — Authenticate and receive signed JWT token
- `POST /api/auth/register` — Create new operator account
- `GET  /api/auth/me` — Get authenticated user profile (`Bearer <token>`)
- `PUT  /api/auth/profile` — Update name or email
- `PUT  /api/auth/password` — Change password

### System Telemetry & SSE Stream
- `GET /api/health` — Engine health, goroutines, uptime, and memory usage
- `GET /api/stats` — Real-time telemetry cards (total runs, uptime, avg latency)
- `GET /api/stream` — Live Server-Sent Events (SSE) feed of execution logs

### Monitors (Heartbeat & Health Checks)
- `GET    /api/monitors` — List all monitors (supports `?category=` and `?search=`)
- `POST   /api/monitors` — Create new monitor
- `GET    /api/monitors/:id` — Get monitor details
- `PUT    /api/monitors/:id` — Update monitor
- `DELETE /api/monitors/:id` — Delete monitor
- `POST   /api/monitors/:id/ping` — Trigger manual immediate probe
- `POST   /api/monitors/:id/toggle` — Pause / Resume monitor

### Execution Logs
- `GET    /api/executions` — List execution audit traces (`?limit=50&offset=0&filter=ok|failed`)
- `DELETE /api/executions` — Clear execution logs buffer
- `DELETE /api/executions/:id` — Delete single log entry

### Batchers (Queue & ETL Workers)
- `GET    /api/batchers` — List background batchers
- `POST   /api/batchers` — Create new batcher
- `GET    /api/batchers/:id` — Get batcher
- `PUT    /api/batchers/:id` — Update batcher
- `DELETE /api/batchers/:id` — Delete batcher
- `POST   /api/batchers/:id/run` — Manually trigger batch worker

### Incidents (SLA Disruption Alerts)
- `GET    /api/incidents` — List all incidents
- `POST   /api/incidents` — Declare new incident
- `GET    /api/incidents/:id` — Get incident
- `PUT    /api/incidents/:id` — Update incident
- `POST   /api/incidents/:id/resolve` — Mark incident as resolved
- `DELETE /api/incidents/:id` — Delete incident

---

## 🗄 Database Schema

The database design is available in [`migrations/schema.sql`](./migrations/schema.sql) with normalized tables for:
- `users`: User identity, password hash + salt, API key, role
- `monitors`: Health endpoints, intervals, timeouts, metrics, history sparklines
- `execution_logs`: Individual audit ping outcomes, latencies, response snippets
- `batchers`: Background queue workers, mutex locks, target topics
- `incidents`: SLA outages, duration, root cause, alert dispatch targets
