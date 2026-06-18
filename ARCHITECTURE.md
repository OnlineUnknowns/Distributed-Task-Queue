# Architecture — Distributed Task Queue

## 1. System Overview

The Distributed Task Queue is a horizontally scalable, production-grade job processing system built in Go. It decouples job producers (HTTP API) from job consumers (worker pool) via RabbitMQ, provides durable job persistence in PostgreSQL, and exposes observability through Prometheus, Grafana, and distributed tracing via OpenTelemetry → Jaeger.

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                               Distributed Task Queue                                │
│                                                                                     │
│   HTTP Clients                                                                      │
│       │                                                                             │
│       ▼                                                                             │
│  ┌──────────┐   POST /jobs        ┌──────────────┐    jobs.queue          ┌──────────────┐│
│  │ API      │──────────────────►  │  JobService  │──────────────────────► │  RabbitMQ    ││
│  │ :8080    │   GET  /jobs/:id    │  (EnqueueJob)│    jobs.priority.queue │  AMQP Broker ││
│  │          │◄────────────────── │              │◄─────────────────────── │              ││
│  │ /metrics │                    │  CircuitBreak│    dead_letter.queue    └──────────────┘│
│  └──────────┘                    │  er wrapper  │                                │        │
│       │                          └──────────────┘                                │        │
│       │                                 │                                        │        │
│       ▼                                 ▼                                        ▼        │
│  ┌──────────┐                    ┌──────────────┐                        ┌──────────────┐│
│  │Prometheus│◄────/metrics──────  │  PostgreSQL  │◄──────────────────────│   Workers    ││
│  │ :9090    │                    │  :5432       │   ClaimJob (SKIP LOCK) │  Pool (N)    ││
│  └──────────┘                    │              │                        │              ││
│       │                          │  jobs        │                        │  ProcessJob  ││
│       ▼                          │  workers     │                        │  Heartbeat   ││
│  ┌──────────┐                    │  job_events  │                        │  ZombieRecov ││
│  │ Grafana  │                    │  job_dedup   │                        └──────────────┘│
│  │ :3000    │                    │  id_keys     │                                         │
│  └──────────┘                    │  tenant_lims │                                         │
│                                  └──────────────┘                                         │
│                                                                                           │
│  ┌──────────┐                                                                             │
│  │  Jaeger  │ ◄── OTLP traces (API + Worker)                                             │
│  │  :16686  │                                                                             │
│  └──────────┘                                                                             │
└───────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Component Descriptions

### API Server (`cmd/api`)
- Gin HTTP server exposing REST endpoints for job management.
- Enforces `X-Tenant-ID` header for all write endpoints (multi-tenancy).
- Checks `Idempotency-Key` header to prevent duplicate job submission.
- Polls RabbitMQ Management API every 10s; if queue depth exceeds `MAX_QUEUE_DEPTH`, returns HTTP 429.
- Instruments traces via OpenTelemetry with spans per request.

### Worker Pool (`cmd/worker`)
- Consumes from both `jobs.queue` and `jobs.priority.queue` via a multiplexed channel.
- Self-registers in the `workers` table on startup; heartbeats every 30s.
- Runs zombie recovery every 60s: finds `RUNNING` jobs older than 5 minutes and re-queues them.
- Uses `ClaimJob` (SELECT FOR UPDATE SKIP LOCKED) to prevent double-processing in multi-instance deployments.
- Executes job handlers within per-job `context.WithTimeout` deadlines.

### JobService (`internal/application`)
- Core business logic: `EnqueueJob`, `GetJob`, `ProcessJob`.
- Manual **Circuit Breaker** (Closed → Open → HalfOpen) protects RabbitMQ from cascading failures.
  - Threshold: 5 consecutive failures → Open for 30s → HalfOpen → retry once.
- `DeduplicatingJobService` wrapper adds SHA-256 deduplication for identical (type + payload) jobs within 5 minutes.
- Records `JobEvent` entries at every state transition for a full audit trail.

### PostgreSQL (`internal/infrastructure/postgres`)
- Single source of truth for all job state.
- Key tables: `jobs`, `workers`, `job_events`, `job_dedup`, `idempotency_keys`, `tenant_limits`.
- All repository methods instrument `db_query_duration_seconds` histogram.

### RabbitMQ (`internal/infrastructure/rabbitmq`)
- Two queues: `jobs.queue` (normal priority) and `jobs.priority.queue` (priority ≥ 7).
- Both bound to a Dead Letter Exchange (`dlx`) → `dead_letter.queue` on NACK without requeue.
- Producer routes by `job.Priority`; consumer merges both queues into a single `<-chan ReceivedJob`.

---

## 3. Data Flow

```
Client
  │  POST /jobs  {type, payload, X-Tenant-ID, [Idempotency-Key]}
  ▼
API Handler
  ├─ Validate X-Tenant-ID (400 if missing)
  ├─ Check Idempotency-Key in job_dedup (return cached if hit)
  ├─ Backpressure check → 429 if queue_depth > MAX_QUEUE_DEPTH
  ├─ JobService.EnqueueJob()
  │     ├─ CircuitBreaker.AllowRequest()
  │     ├─ Save job to PostgreSQL (status=PENDING)
  │     ├─ RecordEvent("job_created")
  │     └─ PublishJob → RabbitMQ (routes by priority)
  └─ 201 Created {id, status}

Worker
  ├─ ConsumeWithContext (jobs.queue + jobs.priority.queue → merged channel)
  ├─ ClaimJob() → SELECT FOR UPDATE SKIP LOCKED → status=RUNNING
  ├─ JobService.ProcessJob()
  │     ├─ RecordEvent("job_started")
  │     ├─ context.WithTimeout(timeout_seconds from payload)
  │     ├─ handler(ctx, payload)
  │     └─ RecordEvent("job_completed") or handleFailure()
  │           ├─ attempts < maxAttempts → status=FAILED, re-publish with exponential backoff
  │           └─ attempts >= maxAttempts → status=DEAD, DLQJobsTotal.Inc()
  └─ Ack / Nack RabbitMQ message
```

---

## 4. Resilience Patterns

| Pattern | Implementation |
|---|---|
| **Circuit Breaker** | Manual state machine in `job_service.go`; 5 failures → Open 30s → HalfOpen |
| **Exponential Backoff** | Retry delay = `2^attempts × 5s`, implemented as goroutine sleep |
| **Dead Letter Queue** | Jobs with ≥ 3 failures → `dead_letter.queue` via RabbitMQ DLX |
| **Zombie Recovery** | Worker goroutine every 60s re-queues RUNNING jobs stale > 5 min |
| **ClaimJob Locking** | `SELECT FOR UPDATE SKIP LOCKED` prevents double processing across instances |
| **Deduplication** | SHA-256(type + canonical payload) checked in `job_dedup` table within 5 min window |
| **Idempotency** | `Idempotency-Key` header maps to existing job IDs in `job_dedup` table |
| **Backpressure** | HTTP 429 when `current_queue_depth > MAX_QUEUE_DEPTH` |
| **Per-Job Timeout** | `context.WithTimeout` wraps handler; exceeding → `"job timeout exceeded"` |

---

## 5. Observability

### Metrics (Prometheus → Grafana `:3000`)
| Metric | Type | Labels |
|---|---|---|
| `jobs_processed_total` | Counter | `job_type`, `status` |
| `job_processing_duration_seconds` | Histogram | `job_type` |
| `jobs_in_queue` | Gauge | — |
| `dlq_jobs_total` | Counter | — |
| `current_queue_depth` | Gauge | — |
| `db_query_duration_seconds` | Histogram | `operation` |
| `worker_heartbeat_timestamp` | Gauge | `worker_id` |

### Distributed Tracing (OpenTelemetry → Jaeger `:16686`)
- Every HTTP request creates a root span in the API.
- `JobService` methods continue spans as children.
- Trace context propagated through RabbitMQ message headers.
- Configure via `OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318`.

### Structured Logging (zap)
- All services emit JSON-structured logs at INFO/ERROR/WARN levels.
- Fields: `job_id`, `job_type`, `worker_id`, `duration`, `attempt`, `error`.
