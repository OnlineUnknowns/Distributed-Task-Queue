<div align="center">


<br/>

![Go](https://img.shields.io/badge/Go-1.22-00ADD8?style=for-the-badge&logo=go&logoColor=white)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-FF6600?style=for-the-badge&logo=rabbitmq&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?style=for-the-badge&logo=grafana&logoColor=white)
![OpenTelemetry](https://img.shields.io/badge/OpenTelemetry-000000?style=for-the-badge&logo=opentelemetry&logoColor=white)

<br/>

> **Production-grade Distributed Task Queue** built entirely in Go.  
> Covers Concurrency · Distributed Systems · Observability · Clean Architecture.

</div>

---

## 📊 Stats at a glance

| Feature | Value |
|---|---|
| ⚙️ Worker concurrency | Configurable via `WORKER_COUNT` |
| 🔄 Retry attempts | 3 with exponential backoff (10s → 20s → DLQ) |
| 🎯 Priority levels | 1–10 (high priority → dedicated queue) |
| 📦 Job deduplication | SHA-256 · 5-minute window |
| 🧟 Zombie recovery | Every 60s · jobs stuck > 5 min auto-requeued |
| 🏢 Multi-tenancy | Per-tenant rate limiting + full isolation |
| 📜 Event sourcing | Full audit log · dead job replay |
| 🛡️ Backpressure | HTTP 429 when queue depth > `MAX_QUEUE_DEPTH` |
| 🔭 Tracing | OpenTelemetry → Jaeger (trace per job end-to-end) |
| ⚡ Circuit breaker | 5 failures → open 30s → half-open → retry |
| 🧪 Test coverage | 70%+ · unit + integration |

---

## 🏗️ Architecture

```
┌─────────────┐    POST /jobs     ┌──────────────┐    publish    ┌─────────────────────┐
│   Client    │ ────────────────► │  API Server  │ ────────────► │      RabbitMQ       │
└─────────────┘                   │  :8080       │               │  jobs.queue         │
                                  │  Gin + zap   │               │  jobs.priority.queue│
                                  └──────────────┘               │  dead_letter.queue  │
                                         │                        └─────────┬───────────┘
                                         │ save job                         │ consume
                                         ▼                                  ▼
                                  ┌──────────────┐               ┌─────────────────────┐
                                  │  PostgreSQL  │ ◄──────────── │    Worker Pool      │
                                  │  jobs        │   update      │  N goroutines       │
                                  │  job_events  │   status      │  context + WaitGroup│
                                  │  workers     │               │  graceful shutdown  │
                                  │  job_dedup   │               └─────────────────────┘
                                  │  tenant_limits              
                                  └──────────────┘               
                                         │                        
                              ┌──────────┼──────────┐            
                              ▼          ▼          ▼            
                         Prometheus   Grafana    Jaeger          
                          :9090       :3000      :16686          
```

---

## 📁 Project structure

```
gotaskq/
├── cmd/
│   ├── api/main.go            # HTTP API server entrypoint
│   └── worker/main.go         # Worker pool entrypoint
├── config/
│   └── config.go              # Centralized config + validation
├── internal/
│   ├── domain/                # Pure business types — zero dependencies
│   │   ├── job.go             # Job struct + fields
│   │   ├── status.go          # JobStatus constants
│   │   ├── repository.go      # Repository interface
│   │   └── errors.go          # Domain errors
│   ├── application/           # Use cases
│   │   ├── job_service.go     # EnqueueJob, ProcessJob, Circuit Breaker
│   │   ├── middleware.go      # Deduplication
│   │   └── handlers.go        # Job handler registry
│   ├── infrastructure/
│   │   ├── postgres/          # PostgreSQL implementation
│   │   │   ├── db.go
│   │   │   └── job_repository.go  # ClaimJob, pagination, metrics
│   │   └── rabbitmq/          # RabbitMQ implementation
│   │       ├── producer.go
│   │       ├── consumer.go
│   │       └── dead_letter.go
│   └── transport/http/
│       └── handlers.go        # Gin routes + middleware
├── migrations/
│   ├── 001_create_jobs_table.sql
│   └── 002_add_indexes.sql
├── monitoring/
│   ├── metrics.go             # Prometheus metrics
│   └── grafana_dashboard.json
├── Dockerfile.api
├── Dockerfile.worker
├── docker-compose.yml
└── ARCHITECTURE.md
```

---

## 🚀 Quick start

```bash
# 1. Clone
git clone https://github.com/you/gotaskq
cd gotaskq

# 2. Run everything
docker-compose up --build

# 3. Send a job
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: acme" \
  -d '{"type":"send_email","payload":{"to":"user@example.com"},"priority":9}'

# 4. Check status
curl http://localhost:8080/jobs/{id}

# 5. View full history
curl http://localhost:8080/jobs/{id}/history
```

---

## 🌐 API reference

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/jobs` | Enqueue a new job |
| `GET` | `/jobs/:id` | Get job status and result |
| `GET` | `/jobs/:id/history` | Full event sourcing log |
| `POST` | `/jobs/:id/replay` | Re-enqueue a dead job |
| `GET` | `/tenants/:id/stats` | Tenant metrics and queue depth |
| `GET` | `/metrics` | Prometheus metrics endpoint |
| `GET` | `/health` | Health check |

### Example: enqueue with all options

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -H "X-Tenant-ID: acme" \
  -H "Idempotency-Key: my-unique-key-123" \
  -d '{
    "type": "send_email",
    "priority": 9,
    "payload": {
      "to": "user@example.com",
      "subject": "Welcome!",
      "timeout_seconds": 30
    }
  }'
```

### Example: response

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "type": "send_email",
  "status": "pending",
  "priority": 9,
  "attempts": 0,
  "created_at": "2024-01-15T10:30:00Z"
}
```

---

## 🔄 Job lifecycle

```
POST /jobs
    │
    ├─► Deduplication check (SHA-256)
    │       └─► If duplicate within 5min → return existing job ID
    │
    ├─► Save to PostgreSQL (status: pending)
    │
    └─► Publish to RabbitMQ
            ├─► priority >= 7 → jobs.priority.queue
            └─► priority < 7  → jobs.queue

Worker picks up job (SELECT FOR UPDATE SKIP LOCKED)
    │
    ├─► status: running
    ├─► Execute handler with context timeout
    │
    ├─► SUCCESS → status: completed
    │
    └─► FAILURE
            ├─► attempts < 3 → retry with exponential backoff
            │       attempt 1: wait 10s
            │       attempt 2: wait 20s
            │       attempt 3: wait 40s
            └─► attempts >= 3 → status: dead → Dead Letter Queue
```

---

## ⚙️ Environment variables

| Variable | Description | Default | Required |
|---|---|---|---|
| `API_PORT` | HTTP server port | `8080` | No |
| `DB_HOST` | PostgreSQL host | — | Yes |
| `DB_PORT` | PostgreSQL port | `5432` | No |
| `DB_USER` | PostgreSQL user | — | Yes |
| `DB_PASSWORD` | PostgreSQL password | — | Yes |
| `DB_NAME` | PostgreSQL database | — | Yes |
| `RABBITMQ_URL` | RabbitMQ connection URL | — | Yes |
| `WORKER_COUNT` | Number of worker goroutines | `5` | No |
| `MAX_QUEUE_DEPTH` | Backpressure threshold | `10000` | No |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Jaeger OTLP endpoint | — | No |

---

## 📈 Monitoring

### Prometheus metrics

```promql
# Jobs per minute
rate(jobs_processed_total[5m])

# Failure rate
rate(jobs_processed_total{status="failed"}[5m])
  / rate(jobs_processed_total[5m])

# 99th percentile processing time
histogram_quantile(0.99, job_processing_duration_seconds_bucket)

# Current queue depth
jobs_in_queue

# Dead letter queue total
dlq_jobs_total
```

### Dashboards
- **Grafana**: http://localhost:3000 — import `monitoring/grafana_dashboard.json`
- **Jaeger**: http://localhost:16686 — search by `job_id` tag
- **Prometheus**: http://localhost:9090

---

## 🛠️ Tech stack

| Layer | Technology |
|---|---|
| Language | Go 1.22 |
| HTTP Framework | Gin |
| Message Queue | RabbitMQ (amqp091-go) |
| Database | PostgreSQL (pgx/v5) |
| Logging | Uber Zap |
| Metrics | Prometheus + Grafana |
| Tracing | OpenTelemetry + Jaeger |
| Testing | Testify + mocks |
| Containers | Docker Compose |

---

## 🧪 Running tests

```bash
# Unit tests
go test ./internal/application/... -v

# Integration tests (requires running DB)
TEST_DB_URL=postgres://user:pass@localhost:5432/testdb \
  go test ./internal/infrastructure/... -v

# All tests with coverage
go test ./... -coverprofile=coverage.out
go tool cover -html=coverage.out
```

---

## 📖 Architecture decisions

See [ARCHITECTURE.md](./ARCHITECTURE.md) for detailed explanations of:
- Why RabbitMQ over Kafka
- Why Clean Architecture
- Scalability plan for 1M jobs/day
- Failure modes and mitigations
- Every tradeoff made

---

<div align="center">

**Built to understand Go · Concurrency · Databases · Distributed Systems**  
**Networking · Error Handling · Testing · Deployment · Architecture**

</div>

# Distributed Task Queue

A production-ready distributed task queue built in **Go** using **Clean Architecture**, featuring:
- REST API for submitting and querying jobs
- Worker pool that processes jobs concurrently
- RabbitMQ for message passing with Dead Letter Queue support
- PostgreSQL for durable job persistence
- Prometheus metrics + Grafana dashboards
- Exponential backoff retry logic

---

## Architecture

```
┌─────────────────┐       ┌──────────────────┐       ┌─────────────────┐
│   REST Client   │──────▶│    API Server     │──────▶│    PostgreSQL   │
└─────────────────┘       │  (cmd/api)        │       └─────────────────┘
                          │                   │
                          │  POST /jobs       │──────▶┌─────────────────┐
                          │  GET  /jobs/:id   │       │    RabbitMQ     │
                          │  GET  /metrics    │       │  jobs.queue     │
                          └──────────────────┘       │  dead_letter.q  │
                                                      └────────┬────────┘
                                                               │
                                              ┌────────────────▼────────────────┐
                                              │         Worker Pool              │
                                              │        (cmd/worker)              │
                                              │  Worker 1 │ Worker 2 │ Worker N  │
                                              │  ─────────┼──────────┼─────────  │
                                              │        JobService                │
                                              │   ProcessJob + Retry Logic       │
                                              └──────────────────────────────────┘
```

### Layer Overview

| Layer | Package | Responsibility |
|---|---|---|
| **Domain** | `internal/domain` | Core entities, interfaces, errors |
| **Application** | `internal/application` | Business logic, handler registry |
| **Infrastructure** | `internal/infrastructure` | Postgres repo, RabbitMQ producer/consumer |
| **Transport** | `internal/transport/http` | Gin HTTP handlers & middleware |
| **Monitoring** | `monitoring/` | Prometheus metrics, Grafana dashboard |

---

## Quick Start with Docker Compose

### Prerequisites
- [Docker](https://docs.docker.com/get-docker/) & [Docker Compose](https://docs.docker.com/compose/) v2+

### 1. Clone and start all services

```bash
git clone https://github.com/yourname/distributed-task-queue.git
cd distributed-task-queue

docker compose up --build
```

This starts:
| Service | URL |
|---|---|
| **API** | http://localhost:8080 |
| **RabbitMQ Management** | http://localhost:15672 (guest/guest) |
| **Prometheus** | http://localhost:9090 |
| **Grafana** | http://localhost:3000 (admin/admin) |

### 2. Tear down

```bash
docker compose down -v   # -v also removes volumes
```

---

## Environment Variables

### API Server (`cmd/api`)

| Variable | Default | Description |
|---|---|---|
| `API_PORT` | `8080` | HTTP listen port |
| `DB_HOST` | — | PostgreSQL host |
| `DB_PORT` | — | PostgreSQL port |
| `DB_USER` | — | PostgreSQL user |
| `DB_PASSWORD` | — | PostgreSQL password |
| `DB_NAME` | — | PostgreSQL database name |
| `RABBITMQ_URL` | — | RabbitMQ AMQP connection URL |

### Worker (`cmd/worker`)

| Variable | Default | Description |
|---|---|---|
| `WORKER_COUNT` | `5` | Number of concurrent worker goroutines |
| `MAX_ATTEMPTS` | `3` | Max job attempts before dead-lettering |
| `DB_HOST` / `DB_USER` / … | — | Same as API |
| `RABBITMQ_URL` | — | Same as API |

---

## API Reference

### Health Check

```bash
curl http://localhost:8080/health
```

```json
{ "status": "ok" }
```

---

### Enqueue a Job

**`POST /jobs`**

```bash
curl -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "type": "send_email",
    "payload": {
      "to": "user@example.com",
      "subject": "Welcome!",
      "body": "Hello from the task queue."
    }
  }'
```

**Response `201 Created`:**

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "type": "send_email",
  "status": "PENDING",
  "created_at": "2024-01-15T10:30:00Z"
}
```

---

### Get Job Status

**`GET /jobs/:id`**

```bash
curl http://localhost:8080/jobs/550e8400-e29b-41d4-a716-446655440000
```

**Response `200 OK`:**

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "type": "send_email",
  "status": "COMPLETED",
  "attempts": 1,
  "error_message": "",
  "created_at": "2024-01-15T10:30:00Z",
  "updated_at": "2024-01-15T10:30:02Z"
}
```

**Job Status Values:**

| Status | Meaning |
|---|---|
| `PENDING` | Enqueued, awaiting pickup |
| `RUNNING` | Currently being processed by a worker |
| `COMPLETED` | Successfully processed |
| `FAILED` | Failed, scheduled for retry |
| `DEAD` | Exceeded max attempts, moved to dead-letter queue |

---

### Prometheus Metrics

```bash
curl http://localhost:8080/metrics
```

Key metrics:

| Metric | Type | Description |
|---|---|---|
| `jobs_processed_total` | Counter | Total jobs by `job_type` and `status` |
| `job_processing_duration_seconds` | Histogram | Processing time by `job_type` |
| `jobs_in_queue` | Gauge | Estimated jobs waiting in queue |

---

## Running Tests

### Unit Tests (no external dependencies)

```bash
go test ./internal/application/... -v
```

### Integration Tests (requires PostgreSQL)

```bash
# Start only postgres:
docker compose up postgres -d

export TEST_DB_URL="postgres://taskq:taskq_secret@localhost:5432/taskqueue?sslmode=disable"
go test ./internal/infrastructure/postgres/... -v
```

### All tests

```bash
go test ./... -v
```

---

## Retry & Dead Letter Logic

```
Job fails
    │
    ├─ attempts < 3  ──▶  status=FAILED  ──▶  republished to jobs.queue
    │                      with delay = 2^attempts × 5s
    │                      (10s, 20s on 2nd failure)
    │
    └─ attempts >= 3 ──▶  status=DEAD   ──▶  NACK → RabbitMQ routes to
                                              dead_letter.queue via DLX
```

---

## Project Structure

```
.
├── cmd/
│   ├── api/main.go             # API server entry point
│   └── worker/main.go          # Worker pool entry point
├── internal/
│   ├── domain/                 # Job, JobStatus, JobRepository, errors
│   ├── application/            # JobService, JobHandlerRegistry
│   ├── infrastructure/
│   │   ├── postgres/           # pgx repository implementation
│   │   └── rabbitmq/           # producer, consumer, dead-letter setup
│   └── transport/http/         # Gin handlers & middleware
├── migrations/
│   └── 001_create_jobs_table.sql
├── monitoring/
│   ├── metrics.go              # Prometheus metric definitions
│   ├── prometheus.yml          # Prometheus scrape config
│   ├── grafana_dashboard.json  # Importable Grafana dashboard
│   └── grafana_provisioning/   # Auto-provisioned datasource & dashboard
├── Dockerfile.api
├── Dockerfile.worker
├── docker-compose.yml
└── README.md
```
