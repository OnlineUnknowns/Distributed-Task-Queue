# Distributed Task Queue Documentation

This document describes the real runtime behavior of the project as implemented in the codebase.

---

## 1. Architecture

### 1.1 System Overview

The system is a Go-based distributed task queue that separates request ingestion, durable state, queueing, and execution.

```text
                    ┌────────────────────────────┐
                    │        HTTP Clients          │
                    └────────────┬─────────────────┘
                                 │
                                 │ POST /jobs
                                 │ GET /jobs/:id
                                 │ GET /jobs/:id/history
                                 │ POST /jobs/:id/replay
                                 ▼
                    ┌────────────────────────────┐
                    │         API Server           │
                    │         cmd/api              │
                    │      Gin HTTP router         │
                    └────────────┬─────────────────┘
                                 │
                                 │ 1. Validate tenant/idempotency
                                 │ 2. Save job to DB
                                 │ 3. Publish to RabbitMQ
                                 ▼
                    ┌────────────────────────────┐
                    │         PostgreSQL           │
                    │      jobs / job_events       │
                    │   job_dedup / workers        │
                    │ idempotency_keys / tenant_limits │
                    └────────────┬─────────────────┘
                                 │
                                 │ SELECT FOR UPDATE SKIP LOCKED
                                 │ job status updates / recovery
                                 ▼
                    ┌────────────────────────────┐
                    │          RabbitMQ            │
                    │   jobs.queue                 │
                    │   jobs.priority.queue        │
                    │   dead_letter.queue          │
                    └────────────┬─────────────────┘
                                 │
                                 │ ack / nack / dead-letter routing
                                 ▼
                    ┌────────────────────────────┐
                    │       Worker Pool            │
                    │        cmd/worker            │
                    │  worker_count goroutines     │
                    └────────────┬─────────────────┘
                                 │
                                 │ ProcessJob(ctx, job)
                                 │ update status to RUNNING / COMPLETED / FAILED
                                 ▼
                    ┌────────────────────────────┐
                    │       Prometheus             │
                    │      /metrics endpoint       │
                    └────────────┬─────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────────┐
                    │          Grafana             │
                    │      dashboard UI            │
                    └────────────┬─────────────────┘
                                 │
                                 ▼
                    ┌────────────────────────────┐
                    │           Jaeger              │
                    │   trace visualization        │
                    └────────────────────────────┘
```

### 1.2 Clean Architecture Layers

#### Domain layer
Location: [internal/domain](internal/domain)

The domain layer contains the business entities and repository interfaces only. It has zero infrastructure dependencies.

Real examples:
- [internal/domain/job.go](internal/domain/job.go) defines `Job`, `JobEvent`, and `TenantStats`.
- [internal/domain/status.go](internal/domain/status.go) defines the allowed statuses: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`, and `DEAD`.
- [internal/domain/repository.go](internal/domain/repository.go) defines the `JobRepository` interface, which the application layer depends on.

Why it is dependency-free:
- It imports only standard library packages (`context` and `time`).
- It does not import Gin, Postgres, RabbitMQ, or Prometheus.

#### Application layer
Location: [internal/application](internal/application)

The application layer contains business rules and orchestration logic.

Real example:
- [internal/application/job_service.go](internal/application/job_service.go) defines `NewJobService(...)`, `EnqueueJob(...)`, `GetJob(...)`, and `ProcessJob(...)`.
- `EnqueueJob` reads the tenant from context, determines priority from payload, saves the job to the repo, records `job_created`, and then publishes to RabbitMQ.

The important dependency rule is:
- `defaultJobService` depends on the repository interface and the `QueueProducer` interface, not concrete implementations.
- This is visible in the struct definition:

```go
type defaultJobService struct {
    repo        domain.JobRepository
    producer    QueueProducer
    registry    JobHandlerRegistry
    maxAttempts int
    cb          *CircuitBreaker
}
```

#### Infrastructure layer
Location: [internal/infrastructure](internal/infrastructure)

This layer implements the interfaces defined in the domain layer.

Real examples:
- [internal/infrastructure/postgres/job_repository.go](internal/infrastructure/postgres/job_repository.go) implements `domain.JobRepository`.
- [internal/infrastructure/rabbitmq/producer.go](internal/infrastructure/rabbitmq/producer.go) implements `QueueProducer`.
- [internal/infrastructure/rabbitmq/consumer.go](internal/infrastructure/rabbitmq/consumer.go) consumes messages from RabbitMQ.

Concrete import chain:
1. [cmd/api/main.go](cmd/api/main.go) creates `repo := postgres.NewJobRepository(dbPool)`.
2. It creates `producer, err := rabbitmq.NewJobProducer()`.
3. It creates `jobService := application.NewJobService(repo, producer, application.DefaultRegistry, 3)`.
4. The HTTP layer then uses the service:
   - [internal/transport/http/handlers.go](internal/transport/http/handlers.go)
   - `handler := transporthttp.NewJobHandler(jobService, repo, logger)`

#### Transport layer
Location: [internal/transport/http](internal/transport/http)

This layer handles HTTP concerns only.

Real example:
- [internal/transport/http/handlers.go](internal/transport/http/handlers.go) defines `JobHandler` and the route registrations.
- `EnqueueJob` validates input, checks `X-Tenant-ID`, checks idempotency keys, and calls `h.service.EnqueueJob(...)`.

Import chain example:
- [cmd/api/main.go](cmd/api/main.go) imports `transporthttp`.
- It builds the router and registers routes with:

```go
handler := transporthttp.NewJobHandler(jobService, repo, logger)
transporthttp.RegisterRoutes(router, handler, logger)
```

### 1.3 Database Schema

The project expects the following six tables (created/modified by the SQL files in [migrations](migrations)).

#### `jobs`
Columns:
- `id UUID PRIMARY KEY`
- `type VARCHAR(100) NOT NULL`
- `payload JSONB NOT NULL`
- `status VARCHAR(50) NOT NULL`
- `attempts INT NOT NULL DEFAULT 0`
- `error_message TEXT`
- `created_at TIMESTAMPTZ NOT NULL`
- `updated_at TIMESTAMPTZ NOT NULL`
- `priority INT NOT NULL DEFAULT 5`
- `tenant_id VARCHAR(100) NOT NULL DEFAULT 'default'`
- `idempotency_key VARCHAR(255)`

Why it exists:
- This is the canonical state store for every job.
- It tracks lifecycle state, retries, and tenant scoping.

#### `workers`
Columns:
- `worker_id UUID PRIMARY KEY`
- `hostname VARCHAR(255) NOT NULL`
- `started_at TIMESTAMPTZ NOT NULL`
- `last_heartbeat TIMESTAMPTZ NOT NULL`

Why it exists:
- Tracks worker liveness and allows the system to detect stale workers and recover jobs.

#### `job_dedup`
Columns:
- `dedup_key VARCHAR(64) PRIMARY KEY`
- `job_id UUID NOT NULL`
- `created_at TIMESTAMPTZ NOT NULL`

Why it exists:
- Prevents duplicate submissions for the same logical job within the 5-minute deduplication window.

#### `idempotency_keys`
Columns:
- `key VARCHAR(255) PRIMARY KEY`
- `response JSONB NOT NULL`
- `created_at TIMESTAMPTZ NOT NULL`

Why it exists:
- Stores a repeatable response for a given request key.
- The HTTP layer checks `Idempotency-Key` for request repetition.

#### `job_events`
Columns:
- `id UUID PRIMARY KEY`
- `job_id UUID NOT NULL`
- `event_type VARCHAR(100) NOT NULL`
- `metadata JSONB NOT NULL`
- `occurred_at TIMESTAMPTZ NOT NULL`

Why it exists:
- Keeps an audit trail of job lifecycle transitions such as `job_created`, `job_started`, `job_failed`, and `job_completed`.

#### `tenant_limits`
Columns:
- `tenant_id VARCHAR(100) PRIMARY KEY`
- `max_jobs_per_minute INT NOT NULL`
- `max_concurrent_jobs INT NOT NULL`

Why it exists:
- Supports per-tenant throttling and concurrency controls.
- The code currently exposes `GET /tenants/:id/stats`, but the actual rate limit enforcement is not yet implemented in runtime logic.

### 1.4 RabbitMQ Topology

The queue topology is declared in [internal/infrastructure/rabbitmq/dead_letter.go](internal/infrastructure/rabbitmq/dead_letter.go).

```text
Default exchange ("")
    │
    ├── routing key = jobs.queue
    │        │
    │        └──> jobs.queue
    │                 │
    │                 └──> workers (consumer)
    │
    └── routing key = jobs.priority.queue
             │
             └──> jobs.priority.queue
                      │
                      └──> workers (consumer)

If a message is rejected without requeue:
    message
      └──> dlx exchange
             │
             └──> dead_letter.queue
```

Message behavior:
- A job with `Priority < 7` is published to `jobs.queue`.
- A job with `Priority >= 7` is published to `jobs.priority.queue`.
- If a worker calls `Nack(false)` (no requeue), RabbitMQ routes the message to the DLX and eventually to `dead_letter.queue`.

### 1.5 Circuit Breaker States

The implementation lives in [internal/application/job_service.go](internal/application/job_service.go).

```text
          +----------------------+
          |        CLOSED         |
          +----------------------+
                    |
                    | 5 consecutive failures
                    v
          +----------------------+
          |         OPEN          |
          |  for 30 seconds       |
          +----------------------+
                    |
                    | time.Since(lastStateChange) >= 30s
                    v
          +----------------------+
          |      HALF_OPEN        |
          +----------------------+
                    |
                    | success => CLOSED
                    | failure => OPEN
```

Exact transition rules:
- `AllowRequest()` returns `true` in `CLOSED`.
- If `OPEN` and 30 seconds passed, the breaker moves to `HALF_OPEN` and allows one request.
- `RecordResult(err)` increments `consecutiveFailures`.
- If `CLOSED` and `consecutiveFailures >= 5`, it becomes `OPEN`.
- If `HALF_OPEN` and a request fails, the breaker goes back to `OPEN`.

---

## 2. Flow

### 2.1 Happy Path — Job Lifecycle

Example job: `send_email`.

Step-by-step flow:
1. A client calls `POST /jobs` with JSON like:
   ```json
   {
     "type": "send_email",
     "payload": {
       "to": "user@example.com",
       "subject": "Welcome"
     }
   }
   ```
2. The HTTP handler in [internal/transport/http/handlers.go](internal/transport/http/handlers.go) reads `X-Tenant-ID` and rejects the request if missing.
3. The handler calls `h.service.EnqueueJob(...)`.
4. `EnqueueJob` in [internal/application/job_service.go](internal/application/job_service.go) creates a `domain.Job` with:
   - status `PENDING`
   - attempts `0`
   - priority default `5` unless the payload contains `{"priority": ...}`
5. The repository saves the row into the `jobs` table.
6. `job_created` is recorded in `job_events`.
7. The producer publishes the job to RabbitMQ.
8. The worker consumes from the queue and calls `repo.ClaimJob()`.
9. `ClaimJob()` uses `SELECT FOR UPDATE SKIP LOCKED` so only one worker claims the row.
10. The worker updates the row to `RUNNING`.
11. `ProcessJob` runs the registered handler (`send_email`) from [internal/application/handlers.go](internal/application/handlers.go).
12. On success, the job is saved again as `COMPLETED` and `job_completed` is logged.

Database changes during this flow:
- Insert into `jobs` (pending)
- Insert into `job_events` (`job_created`)
- Update `jobs` to `RUNNING`
- Update `jobs` to `COMPLETED`
- Insert into `job_events` (`job_started` / `job_completed`)

### 2.2 Retry Flow

When a job fails, the worker does not immediately retry forever. The actual code uses exponential backoff.

The logic in [internal/application/job_service.go](internal/application/job_service.go) is:

```go
delay := time.Duration(1<<job.Attempts) * 5 * time.Second
```

With `MAX_ATTEMPTS=3`, the behavior is:
- Attempt 1 failure: `delay = 10s`
- Attempt 2 failure: `delay = 20s`
- Attempt 3 failure: status becomes `DEAD` and the message is sent to the DLQ

State changes:
- First failure -> `status = FAILED`, `attempts = 1`
- Second failure -> `status = FAILED`, `attempts = 2`
- Third failure -> `status = DEAD`, `attempts = 3`, `dlq_jobs_total` increments

RabbitMQ side:
- For `FAILED` jobs, the code republishes the same job after the delay.
- For `DEAD` jobs, the worker calls `Nack(false)` so RabbitMQ routes the message to `dead_letter.queue`.

### 2.3 Deduplication Flow

The handler checks `Idempotency-Key` and the repository checks `job_dedup`.

Example request:
```bash
POST /jobs
X-Tenant-ID: tenant-a
Idempotency-Key: abc123
{
  "type": "send_email",
  "payload": {
    "to": "user@example.com"
  }
}
```

The deduplication logic is:
1. The handler reads the header `Idempotency-Key`.
2. It calls `repo.GetDeduplicatedJob(...)`.
3. If a row exists for the key within the last 5 minutes, the handler returns the same job record and sets `cached: true`.
4. If the key is new, the job is accepted and the handler later calls `repo.SaveDeduplication(...)`.

The actual key computation used by [internal/application/middleware.go](internal/application/middleware.go) is:

```go
sha256(jobType + canonicalPayload)
```

This means the same request payload submitted twice within 5 minutes will not create a second logical job.

### 2.4 Priority Flow

The producer checks the payload and chooses a queue.

From [internal/infrastructure/rabbitmq/producer.go](internal/infrastructure/rabbitmq/producer.go):

```go
routingKey := JobsQueue
if job.Priority >= 7 {
    routingKey = PriorityQueue
}
```

Example:
- Job A: `priority = 3` -> `jobs.queue`
- Job B: `priority = 9` -> `jobs.priority.queue`

Because the worker consumes from both queues, the worker that gets the message first may process the higher-priority job earlier, depending on broker scheduling.

### 2.5 Zombie Recovery Flow

The worker runs a recovery goroutine every 60 seconds.

Timeline:
1. A worker starts processing a job and updates `jobs.status` to `RUNNING`.
2. The worker crashes or is killed before completing the job.
3. After 5 minutes, the repository query in `RecoverZombies()` finds rows where:
   - `status = 'RUNNING'`
   - `updated_at < NOW() - INTERVAL '5 minutes'`
4. Those rows are reset to `PENDING`.
5. The worker goroutine republishes the recovered jobs back to RabbitMQ.
6. Another worker picks them up with `ClaimJob()`.

### 2.6 Distributed Trace

The API service initializes tracing in [cmd/api/main.go](cmd/api/main.go). The tracer configuration is in [internal/observability/tracer.go](internal/observability/tracer.go).

A complete trace for one job would look like:

```text
HTTP POST /jobs
└── parent span: request
    ├── span: EnqueueJob
    │   ├── span: Save job to DB
    │   ├── span: RecordEvent(job_created)
    │   └── span: PublishJob
    │       └── span: RabbitMQ publish
    └── span: Worker Receive
        ├── span: ClaimJob
        ├── span: ProcessJob
        └── span: UpdateStatus(COMPLETED)
```

Attributes attached by code:
- `service.name` from the tracer config
- `job_id`, `job_type`, `worker_id`, `attempt`, `duration`, `error` in logs
- Prometheus labels for `job_type` and `status`

### 2.7 Multi-tenant Request Flow

The HTTP handler requires `X-Tenant-ID` for write requests.

Flow:
1. `POST /jobs` with header `X-Tenant-ID: tenant-a`.
2. The handler injects `tenant_id` into the request context.
3. `EnqueueJob` reads that context value and stores it in `jobs.tenant_id`.
4. Queries for tenant stats are scoped with `WHERE tenant_id = $1`.
5. If a tenant exceeds configured limits, the system is expected to use `tenant_limits` for throttling, but this is not shown as a runtime enforcement path in the current code.

---

## 3. Deployment

### 3.1 Local Development

From a new machine:

```bash
git clone <repo-url>
cd distributed-task-queue
docker compose up --build
```

Verify services:
- API: `curl http://localhost:8080/health`
- RabbitMQ UI: `http://localhost:15672` (guest/guest)
- Prometheus: `http://localhost:9090`
- Grafana: `http://localhost:3000` (admin/admin)

Run migrations:
- The compose file mounts [migrations](migrations) into PostgreSQL's init directory, so the SQL runs automatically on first boot.
- If you need to re-run manually:

```bash
docker compose exec postgres psql -U taskq -d taskqueue -f /docker-entrypoint-initdb.d/001_create_jobs_table.sql
```

Send a test job:

```bash
curl -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-ID: tenant-a' \
  -d '{
    "type": "send_email",
    "payload": {
      "to": "user@example.com",
      "subject": "Welcome",
      "body": "Hello from the queue"
    }
  }'
```

### 3.2 Environment Variables Reference

The canonical config loader is [config/config.go](config/config.go).

| Variable | Description | Default | Required? |
|---|---|---:|---|
| `DB_HOST` | PostgreSQL host | none | Yes |
| `DB_PORT` | PostgreSQL port | none | Yes |
| `DB_USER` | PostgreSQL user | none | Yes |
| `DB_PASSWORD` | PostgreSQL password | none | Yes |
| `DB_NAME` | PostgreSQL database name | none | Yes |
| `RABBITMQ_URL` | AMQP connection string for RabbitMQ | none | Yes |
| `API_PORT` | HTTP port for the API | `8080` | Optional |
| `WORKER_COUNT` | Number of worker goroutines | `5` | Optional |
| `MAX_ATTEMPTS` | Max retry attempts before DLQ | `3` | Optional |
| `RABBITMQ_MANAGEMENT_URL` | URL used by API to fetch queue depth | `http://guest:guest@rabbitmq:15672` | Optional |
| `MAX_QUEUE_DEPTH` | Backpressure threshold | `10000` | Optional |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTLP endpoint for traces | `http://localhost:4318` | Optional |
| `TEST_DB_URL` | Test DB URL used by integration tests | none | Optional |

### 3.3 Docker Compose Services

The actual compose definition is [docker-compose.yml](docker-compose.yml).

| Service | Port(s) | Health check | Volume(s) | Depends on |
|---|---|---|---|---|
| `postgres` | `5432` | `pg_isready -U taskq -d taskqueue` | `postgres_data`, mounted [migrations](migrations) | none |
| `rabbitmq` | `5672` (AMQP), `15672` (UI) | `rabbitmq-diagnostics ping` | none | none |
| `api` | `8080` | none in compose | built from Dockerfile.api | `postgres`, `rabbitmq` |
| `worker` | none | none in compose | built from Dockerfile.worker | `postgres`, `rabbitmq` |
| `prometheus` | `9090` | none in compose | `prometheus_data`, [monitoring/prometheus.yml](monitoring/prometheus.yml) | none |
| `grafana` | `3000` | none in compose | `grafana_data`, dashboard JSON, provisioning folder | `prometheus` |
| `jaeger` | not currently defined in compose | not currently defined | not currently defined | not currently defined |

> Note: the codebase is configured to emit OTLP traces, but the repository does not currently define a Jaeger service in [docker-compose.yml](docker-compose.yml). The tracer defaults to `http://localhost:4318` if `OTEL_EXPORTER_OTLP_ENDPOINT` is not set.

### 3.4 Real curl Examples

#### POST /jobs with send_email payload
```bash
curl -sS -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-ID: tenant-a' \
  -d '{
    "type": "send_email",
    "payload": {
      "to": "user@example.com",
      "subject": "Welcome"
    }
  }'
```
Expected response:
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "type": "send_email",
  "status": "PENDING",
  "created_at": "2026-06-18T12:00:00Z"
}
```

#### POST /jobs with priority=9
```bash
curl -sS -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-ID: tenant-a' \
  -d '{
    "type": "send_email",
    "payload": {
      "to": "ops@example.com",
      "priority": 9
    }
  }'
```
Expected response:
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440001",
  "type": "send_email",
  "status": "PENDING",
  "created_at": "2026-06-18T12:00:01Z"
}
```

#### POST /jobs with Idempotency-Key header
```bash
curl -sS -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-ID: tenant-a' \
  -H 'Idempotency-Key: demo-key-1' \
  -d '{
    "type": "send_email",
    "payload": {
      "to": "user@example.com"
    }
  }'
```
Expected response:
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440002",
  "type": "send_email",
  "status": "PENDING",
  "created_at": "2026-06-18T12:00:02Z"
}
```

#### POST /jobs with X-Tenant-ID header
```bash
curl -sS -X POST http://localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -H 'X-Tenant-ID: tenant-b' \
  -d '{
    "type": "send_email",
    "payload": {
      "to": "tenant-b@example.com"
    }
  }'
```
Expected response:
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440003",
  "type": "send_email",
  "status": "PENDING",
  "created_at": "2026-06-18T12:00:03Z"
}
```

#### GET /jobs/:id
```bash
curl -sS http://localhost:8080/jobs/550e8400-e29b-41d4-a716-446655440000
```
Expected response:
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "type": "send_email",
  "status": "COMPLETED",
  "attempts": 1,
  "error_message": "",
  "priority": 5,
  "tenant_id": "tenant-a",
  "created_at": "2026-06-18T12:00:00Z",
  "updated_at": "2026-06-18T12:00:04Z"
}
```

#### GET /jobs/:id/history
```bash
curl -sS http://localhost:8080/jobs/550e8400-e29b-41d4-a716-446655440000/history
```
Expected response:
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "events": [
    {
      "id": "11111111-1111-1111-1111-111111111111",
      "job_id": "550e8400-e29b-41d4-a716-446655440000",
      "event_type": "job_created",
      "metadata": {
        "type": "send_email",
        "priority": 5,
        "tenant_id": "tenant-a"
      },
      "occurred_at": "2026-06-18T12:00:00Z"
    }
  ]
}
```

#### POST /jobs/:id/replay
```bash
curl -sS -X POST http://localhost:8080/jobs/550e8400-e29b-41d4-a716-446655440000/replay
```
Expected response:
```json
{
  "original_job_id": "550e8400-e29b-41d4-a716-446655440000",
  "new_job_id": "550e8400-e29b-41d4-a716-446655440010",
  "status": "PENDING"
}
```

#### GET /health
```bash
curl -sS http://localhost:8080/health
```
Expected response:
```json
{
  "status": "ok"
}
```

#### GET /metrics
```bash
curl -sS http://localhost:8080/metrics
```
Expected response:
```text
# HELP jobs_processed_total Total number of jobs processed by the worker pool.
# TYPE jobs_processed_total counter
jobs_processed_total{job_type="send_email",status="completed"} 1
# HELP job_processing_duration_seconds Time spent processing a job in seconds.
# TYPE job_processing_duration_seconds histogram
```

#### GET /tenants/:id/stats
```bash
curl -sS http://localhost:8080/tenants/tenant-a/stats
```
Expected response:
```json
{
  "total_jobs": 12,
  "success_rate": 0.92,
  "average_processing_time_seconds": 0.41,
  "current_queue_depth": 2
}
```

### 3.5 Monitoring Setup

1. Open Grafana at `http://localhost:3000`.
2. Log in with `admin/admin`.
3. The dashboard is auto-provisioned from [monitoring/grafana_dashboard.json](monitoring/grafana_dashboard.json).
4. Open Jaeger at `http://localhost:16686` (once a Jaeger service is added).
5. Search traces by `job_id` or `tenant_id`.
6. Open Prometheus at `http://localhost:9090` and run:

```promql
rate(jobs_processed_total[5m])
histogram_quantile(0.99, job_processing_duration_seconds_bucket)
jobs_in_queue
dlq_jobs_total
```

### 3.6 Production Deployment Checklist

- [ ] Change default passwords in [docker-compose.yml](docker-compose.yml)
- [ ] Set `RABBITMQ_URL` to a clustered RabbitMQ deployment with at least 3 nodes
- [ ] Enable PostgreSQL connection pooling with PgBouncer
- [ ] Set up a DB read replica and route `GET /jobs/:id` to the replica
- [ ] Configure Prometheus alerts for DLQ spikes, queue depth > 5000, and missing worker heartbeats
- [ ] Set CPU and memory limits on worker containers
- [ ] Verify RabbitMQ persistence settings for durable queues and durable exchanges

### 3.7 Scaling Strategy

#### How to add more workers
- Increase `WORKER_COUNT` or run more worker containers.
- This is safe because the repository uses `SELECT FOR UPDATE SKIP LOCKED` in `ClaimJob()`.

#### How to scale the API
- The API is stateless from the perspective of job execution.
- Multiple API instances can run behind a load balancer.

#### When to partition the jobs table
- Partition by range on `created_at` once the table exceeds roughly 50 million rows.
- This allows the database to prune or archive older partitions more efficiently.

#### When to switch from RabbitMQ to Kafka
- Switch when queue throughput and replay requirements exceed what RabbitMQ is comfortable handling for your workload.
- A practical trigger is when the system needs higher throughput and stronger event stream semantics than the current queue model.
- The code changes would involve replacing the RabbitMQ producer/consumer implementations with Kafka producers/consumers and adjusting the DLQ logic.
