package postgres

import (
	"context"
	"errors"
	"time"

	"distributed-task-queue/internal/domain"
	"distributed-task-queue/monitoring"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type JobRepository struct {
	pool *pgxpool.Pool
}

// Ensure JobRepository implements domain.JobRepository interface
var _ domain.JobRepository = (*JobRepository)(nil)

func NewJobRepository(pool *pgxpool.Pool) *JobRepository {
	return &JobRepository{
		pool: pool,
	}
}

// Save inserts or updates a job in the database.
func (r *JobRepository) Save(ctx context.Context, job *domain.Job) error {
	start := time.Now()
	defer func() {
		monitoring.DBQueryDuration.WithLabelValues("Save").Observe(time.Since(start).Seconds())
	}()

	query := `
		INSERT INTO jobs (id, type, payload, status, attempts, error_message, created_at, updated_at, priority, tenant_id, idempotency_key)
		VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (id) DO UPDATE SET
			type = EXCLUDED.type,
			payload = EXCLUDED.payload,
			status = EXCLUDED.status,
			attempts = EXCLUDED.attempts,
			error_message = EXCLUDED.error_message,
			updated_at = EXCLUDED.updated_at,
			priority = EXCLUDED.priority,
			tenant_id = EXCLUDED.tenant_id,
			idempotency_key = EXCLUDED.idempotency_key
	`
	var errMsg *string
	if job.ErrorMessage != "" {
		errMsg = &job.ErrorMessage
	}

	tenantID := job.TenantID
	if tenantID == "" {
		tenantID = "default"
	}

	var idempKey *string
	if job.IdempotencyKey != "" {
		idempKey = &job.IdempotencyKey
	}

	_, err := r.pool.Exec(ctx, query,
		job.ID,
		job.Type,
		job.Payload,
		job.Status,
		job.Attempts,
		errMsg,
		job.CreatedAt,
		job.UpdatedAt,
		job.Priority,
		tenantID,
		idempKey,
	)
	if err != nil {
		return err
	}

	return nil
}

// FindByID retrieves a job by its ID.
func (r *JobRepository) FindByID(ctx context.Context, id string) (*domain.Job, error) {
	start := time.Now()
	defer func() {
		monitoring.DBQueryDuration.WithLabelValues("FindByID").Observe(time.Since(start).Seconds())
	}()

	query := `
		SELECT id, type, payload, status, attempts, error_message, created_at, updated_at, priority, tenant_id, idempotency_key
		FROM jobs
		WHERE id = $1::uuid
	`
	row := r.pool.QueryRow(ctx, query, id)

	var j domain.Job
	var errMsg *string
	var idempKey *string
	err := row.Scan(
		&j.ID,
		&j.Type,
		&j.Payload,
		&j.Status,
		&j.Attempts,
		&errMsg,
		&j.CreatedAt,
		&j.UpdatedAt,
		&j.Priority,
		&j.TenantID,
		&idempKey,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrJobNotFound
		}
		return nil, err
	}

	if errMsg != nil {
		j.ErrorMessage = *errMsg
	}
	if idempKey != nil {
		j.IdempotencyKey = *idempKey
	}

	return &j, nil
}

// UpdateStatus updates the status of a job.
func (r *JobRepository) UpdateStatus(ctx context.Context, id string, status domain.JobStatus) error {
	start := time.Now()
	defer func() {
		monitoring.DBQueryDuration.WithLabelValues("UpdateStatus").Observe(time.Since(start).Seconds())
	}()

	query := `
		UPDATE jobs
		SET status = $1, updated_at = NOW()
		WHERE id = $2::uuid
	`
	res, err := r.pool.Exec(ctx, query, status, id)
	if err != nil {
		return err
	}

	if res.RowsAffected() == 0 {
		return domain.ErrJobNotFound
	}

	return nil
}

// ListFailed lists all jobs with status FAILED with pagination and returns total count.
func (r *JobRepository) ListFailed(ctx context.Context, page, pageSize int) ([]*domain.Job, int, error) {
	start := time.Now()
	defer func() {
		monitoring.DBQueryDuration.WithLabelValues("ListFailed").Observe(time.Since(start).Seconds())
	}()

	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 10
	}
	offset := (page - 1) * pageSize

	var total int
	err := r.pool.QueryRow(ctx, "SELECT COUNT(*) FROM jobs WHERE status = 'FAILED'").Scan(&total)
	if err != nil {
		return nil, 0, err
	}

	query := `
		SELECT id, type, payload, status, attempts, error_message, created_at, updated_at, priority, tenant_id, idempotency_key
		FROM jobs
		WHERE status = $1
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`
	rows, err := r.pool.Query(ctx, query, domain.Failed, pageSize, offset)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	var jobs []*domain.Job
	for rows.Next() {
		var j domain.Job
		var errMsg *string
		var idempKey *string
		err := rows.Scan(
			&j.ID,
			&j.Type,
			&j.Payload,
			&j.Status,
			&j.Attempts,
			&errMsg,
			&j.CreatedAt,
			&j.UpdatedAt,
			&j.Priority,
			&j.TenantID,
			&idempKey,
		)
		if err != nil {
			return nil, 0, err
		}
		if errMsg != nil {
			j.ErrorMessage = *errMsg
		}
		if idempKey != nil {
			j.IdempotencyKey = *idempKey
		}
		jobs = append(jobs, &j)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, err
	}

	return jobs, total, nil
}

// ClaimJob locks and claims a pending job using SELECT FOR UPDATE SKIP LOCKED.
func (r *JobRepository) ClaimJob(ctx context.Context) (*domain.Job, error) {
	start := time.Now()
	defer func() {
		monitoring.DBQueryDuration.WithLabelValues("ClaimJob").Observe(time.Since(start).Seconds())
	}()

	query := `
		UPDATE jobs SET status='RUNNING', updated_at=NOW()
		WHERE id = (
			SELECT id FROM jobs WHERE status='PENDING'
			ORDER BY priority DESC, created_at ASC
			FOR UPDATE SKIP LOCKED LIMIT 1
		)
		RETURNING id, type, payload, status, attempts, error_message, created_at, updated_at, priority, tenant_id, idempotency_key
	`

	row := r.pool.QueryRow(ctx, query)

	var j domain.Job
	var errMsg *string
	var idempKey *string
	err := row.Scan(
		&j.ID,
		&j.Type,
		&j.Payload,
		&j.Status,
		&j.Attempts,
		&errMsg,
		&j.CreatedAt,
		&j.UpdatedAt,
		&j.Priority,
		&j.TenantID,
		&idempKey,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil // no job claimed
		}
		return nil, err
	}

	if errMsg != nil {
		j.ErrorMessage = *errMsg
	}
	if idempKey != nil {
		j.IdempotencyKey = *idempKey
	}

	return &j, nil
}

// RecordEvent records a state transition or action for a job.
func (r *JobRepository) RecordEvent(ctx context.Context, jobID string, eventType string, metadata map[string]any) error {
	start := time.Now()
	defer func() {
		monitoring.DBQueryDuration.WithLabelValues("RecordEvent").Observe(time.Since(start).Seconds())
	}()

	query := `
		INSERT INTO job_events (id, job_id, event_type, metadata, occurred_at)
		VALUES ($1::uuid, $2::uuid, $3, $4, NOW())
	`
	id := uuid.New().String()
	_, err := r.pool.Exec(ctx, query, id, jobID, eventType, metadata)
	return err
}

// GetEventsByJobID returns the event history log for a job.
func (r *JobRepository) GetEventsByJobID(ctx context.Context, jobID string) ([]*domain.JobEvent, error) {
	start := time.Now()
	defer func() {
		monitoring.DBQueryDuration.WithLabelValues("GetEvents").Observe(time.Since(start).Seconds())
	}()

	query := `
		SELECT id, job_id, event_type, metadata, occurred_at
		FROM job_events
		WHERE job_id = $1::uuid
		ORDER BY occurred_at ASC
	`
	rows, err := r.pool.Query(ctx, query, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*domain.JobEvent
	for rows.Next() {
		var ev domain.JobEvent
		err := rows.Scan(
			&ev.ID,
			&ev.JobID,
			&ev.EventType,
			&ev.Metadata,
			&ev.OccurredAt,
		)
		if err != nil {
			return nil, err
		}
		events = append(events, &ev)
	}

	return events, rows.Err()
}

// GetTenantStats aggregates job count, success rate, average process duration, and queue depth for a tenant.
func (r *JobRepository) GetTenantStats(ctx context.Context, tenantID string) (*domain.TenantStats, error) {
	start := time.Now()
	defer func() {
		monitoring.DBQueryDuration.WithLabelValues("GetTenantStats").Observe(time.Since(start).Seconds())
	}()

	var stats domain.TenantStats

	// 1. Total jobs count
	err := r.pool.QueryRow(ctx, "SELECT COUNT(*) FROM jobs WHERE tenant_id = $1", tenantID).Scan(&stats.TotalJobs)
	if err != nil {
		return nil, err
	}

	if stats.TotalJobs == 0 {
		return &stats, nil
	}

	// 2. Success rate
	var completedCount int64
	err = r.pool.QueryRow(ctx, "SELECT COUNT(*) FROM jobs WHERE tenant_id = $1 AND status = 'COMPLETED'", tenantID).Scan(&completedCount)
	if err != nil {
		return nil, err
	}
	stats.SuccessRate = float64(completedCount) / float64(stats.TotalJobs)

	// 3. Average processing duration
	var avgTime *float64
	err = r.pool.QueryRow(ctx, "SELECT AVG(EXTRACT(EPOCH FROM (updated_at - created_at))) FROM jobs WHERE tenant_id = $1 AND status = 'COMPLETED'", tenantID).Scan(&avgTime)
	if err != nil {
		return nil, err
	}
	if avgTime != nil {
		stats.AverageProcessTime = *avgTime
	}

	// 4. Current queue depth
	err = r.pool.QueryRow(ctx, "SELECT COUNT(*) FROM jobs WHERE tenant_id = $1 AND status IN ('PENDING', 'RUNNING')", tenantID).Scan(&stats.CurrentQueueDepth)
	if err != nil {
		return nil, err
	}

	return &stats, nil
}

// GetDeduplicatedJob checks if a job with the same dedupKey was created in the last 5 minutes.
func (r *JobRepository) GetDeduplicatedJob(ctx context.Context, dedupKey string) (string, error) {
	start := time.Now()
	defer func() {
		monitoring.DBQueryDuration.WithLabelValues("GetDeduplicatedJob").Observe(time.Since(start).Seconds())
	}()

	query := `
		SELECT job_id FROM job_dedup
		WHERE dedup_key = $1 AND created_at > NOW() - INTERVAL '5 minutes'
		LIMIT 1
	`
	var jobID string
	err := r.pool.QueryRow(ctx, query, dedupKey).Scan(&jobID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", err
	}
	return jobID, nil
}

// SaveDeduplication records a deduplication key mapping.
func (r *JobRepository) SaveDeduplication(ctx context.Context, dedupKey string, jobID string) error {
	start := time.Now()
	defer func() {
		monitoring.DBQueryDuration.WithLabelValues("SaveDeduplication").Observe(time.Since(start).Seconds())
	}()

	query := `
		INSERT INTO job_dedup (dedup_key, job_id, created_at)
		VALUES ($1, $2::uuid, NOW())
		ON CONFLICT (dedup_key) DO UPDATE SET
			job_id = EXCLUDED.job_id,
			created_at = EXCLUDED.created_at
	`
	_, err := r.pool.Exec(ctx, query, dedupKey, jobID)
	return err
}

// RegisterWorker inserts a new worker record.
func (r *JobRepository) RegisterWorker(ctx context.Context, id, hostname string) error {
	start := time.Now()
	defer func() {
		monitoring.DBQueryDuration.WithLabelValues("RegisterWorker").Observe(time.Since(start).Seconds())
	}()

	query := `
		INSERT INTO workers (worker_id, hostname, started_at, last_heartbeat)
		VALUES ($1::uuid, $2, NOW(), NOW())
		ON CONFLICT (worker_id) DO UPDATE SET
			hostname = EXCLUDED.hostname,
			last_heartbeat = NOW()
	`
	_, err := r.pool.Exec(ctx, query, id, hostname)
	return err
}

// UnregisterWorker removes a worker record.
func (r *JobRepository) UnregisterWorker(ctx context.Context, id string) error {
	start := time.Now()
	defer func() {
		monitoring.DBQueryDuration.WithLabelValues("UnregisterWorker").Observe(time.Since(start).Seconds())
	}()

	query := `
		DELETE FROM workers WHERE worker_id = $1::uuid
	`
	_, err := r.pool.Exec(ctx, query, id)
	return err
}

// UpdateWorkerHeartbeat updates the last_heartbeat timestamp for a worker.
func (r *JobRepository) UpdateWorkerHeartbeat(ctx context.Context, id string) error {
	start := time.Now()
	defer func() {
		monitoring.DBQueryDuration.WithLabelValues("UpdateWorkerHeartbeat").Observe(time.Since(start).Seconds())
	}()

	query := `
		UPDATE workers SET last_heartbeat = NOW() WHERE worker_id = $1::uuid
	`
	_, err := r.pool.Exec(ctx, query, id)
	return err
}

// RecoverZombies finds running jobs updated > 5 mins ago, updates them to PENDING and returns them.
func (r *JobRepository) RecoverZombies(ctx context.Context) ([]*domain.Job, error) {
	start := time.Now()
	defer func() {
		monitoring.DBQueryDuration.WithLabelValues("RecoverZombies").Observe(time.Since(start).Seconds())
	}()

	query := `
		WITH expired_jobs AS (
			SELECT id FROM jobs
			WHERE status = 'RUNNING' AND updated_at < NOW() - INTERVAL '5 minutes'
		), updated_jobs AS (
			UPDATE jobs
			SET status = 'PENDING', updated_at = NOW()
			WHERE id IN (SELECT id FROM expired_jobs)
			RETURNING id, type, payload, status, attempts, error_message, created_at, updated_at, priority, tenant_id, idempotency_key
		)
		SELECT id, type, payload, status, attempts, error_message, created_at, updated_at, priority, tenant_id, idempotency_key
		FROM updated_jobs
	`

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*domain.Job
	for rows.Next() {
		var j domain.Job
		var errMsg *string
		var idempKey *string
		err := rows.Scan(
			&j.ID,
			&j.Type,
			&j.Payload,
			&j.Status,
			&j.Attempts,
			&errMsg,
			&j.CreatedAt,
			&j.UpdatedAt,
			&j.Priority,
			&j.TenantID,
			&idempKey,
		)
		if err != nil {
			return nil, err
		}
		if errMsg != nil {
			j.ErrorMessage = *errMsg
		}
		if idempKey != nil {
			j.IdempotencyKey = *idempKey
		}
		jobs = append(jobs, &j)
	}

	return jobs, rows.Err()
}


