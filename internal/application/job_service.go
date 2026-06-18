package application

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"distributed-task-queue/internal/domain"
	"distributed-task-queue/monitoring"
	"github.com/google/uuid"
)

// QueueProducer defines the interface for publishing jobs to the message queue.
type QueueProducer interface {
	PublishJob(ctx context.Context, job *domain.Job) error
}

// JobService is the interface representing job operations.
type JobService interface {
	EnqueueJob(ctx context.Context, jobType string, payload []byte) (*domain.Job, error)
	GetJob(ctx context.Context, id string) (*domain.Job, error)
	ProcessJob(ctx context.Context, job *domain.Job) error
}

type defaultJobService struct {
	repo        domain.JobRepository
	producer    QueueProducer
	registry    JobHandlerRegistry
	maxAttempts int
	cb          *CircuitBreaker
}

// NewJobService creates a new instance of JobService.
func NewJobService(repo domain.JobRepository, producer QueueProducer, registry JobHandlerRegistry, maxAttempts int) JobService {
	if maxAttempts <= 0 {
		maxAttempts = 3 // default
	}
	return &defaultJobService{
		repo:        repo,
		producer:    producer,
		registry:    registry,
		maxAttempts: maxAttempts,
		cb:          NewCircuitBreaker(30 * time.Second),
	}
}

// EnqueueJob creates a job, saves it to the database, and publishes it to the queue.
func (s *defaultJobService) EnqueueJob(ctx context.Context, jobType string, payload []byte) (*domain.Job, error) {
	// Check Circuit Breaker before interacting with RabbitMQ
	if !s.cb.AllowRequest() {
		return nil, fmt.Errorf("circuit breaker is open: rabbitmq publish blocked")
	}

	// Read tenant ID from context if set, default to "default"
	tenantID, _ := ctx.Value("tenant_id").(string)
	if tenantID == "" {
		tenantID = "default"
	}

	// Read priority from payload if available
	priority := 5
	var priorityPayload struct {
		Priority int `json:"priority"`
	}
	if err := json.Unmarshal(payload, &priorityPayload); err == nil && priorityPayload.Priority >= 1 && priorityPayload.Priority <= 10 {
		priority = priorityPayload.Priority
	}

	job := &domain.Job{
		ID:        uuid.New().String(),
		Type:      jobType,
		Payload:   payload,
		Status:    domain.Pending,
		Attempts:  0,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Priority:  priority,
		TenantID:  tenantID,
	}

	if err := s.repo.Save(ctx, job); err != nil {
		return nil, fmt.Errorf("failed to save job to DB: %w", err)
	}

	// Record event
	_ = s.repo.RecordEvent(ctx, job.ID, "job_created", map[string]any{
		"type":      jobType,
		"priority":  priority,
		"tenant_id": tenantID,
	})

	if err := s.producer.PublishJob(ctx, job); err != nil {
		s.cb.RecordResult(err)
		return job, fmt.Errorf("failed to publish job to queue: %w", err)
	}
	s.cb.RecordResult(nil)

	// Track job entering the queue
	monitoring.JobsInQueue.Inc()

	return job, nil
}

// GetJob retrieves a job from the database.
func (s *defaultJobService) GetJob(ctx context.Context, id string) (*domain.Job, error) {
	job, err := s.repo.FindByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to find job: %w", err)
	}
	return job, nil
}

// ProcessJob updates the status of the job to Running, executes the handler,
// and updates the status to Completed or increments attempts on failure.
func (s *defaultJobService) ProcessJob(ctx context.Context, job *domain.Job) error {
	// 1. Update status to Running in DB
	job.Status = domain.Running
	job.UpdatedAt = time.Now()
	if err := s.repo.Save(ctx, job); err != nil {
		return fmt.Errorf("failed to update job status to Running: %w", err)
	}

	_ = s.repo.RecordEvent(ctx, job.ID, "job_started", map[string]any{
		"attempt": job.Attempts + 1,
	})

	// 2. Find and execute handler
	handler, exists := s.registry[job.Type]
	if !exists {
		err := fmt.Errorf("no handler registered for job type: %s", job.Type)
		s.handleFailure(ctx, job, err.Error())
		return err
	}

	// Read timeout_seconds from payload
	var timeoutVal int
	var payloadData struct {
		TimeoutSeconds int `json:"timeout_seconds"`
	}
	if err := json.Unmarshal(job.Payload, &payloadData); err == nil && payloadData.TimeoutSeconds > 0 {
		timeoutVal = payloadData.TimeoutSeconds
	}

	var handlerErr error
	runCtx := ctx
	if timeoutVal > 0 {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithTimeout(ctx, time.Duration(timeoutVal)*time.Second)
		defer cancel()
	}

	doneChan := make(chan error, 1)
	go func() {
		doneChan <- handler(runCtx, job.Payload)
	}()

	select {
	case handlerErr = <-doneChan:
		// Completed execution normally
	case <-runCtx.Done():
		if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			handlerErr = fmt.Errorf("job timeout exceeded")
		} else {
			handlerErr = runCtx.Err()
		}
	}

	if handlerErr != nil {
		s.handleFailure(ctx, job, handlerErr.Error())
		return fmt.Errorf("job handler execution failed: %w", handlerErr)
	}

	// 3. Mark as Completed on success
	job.Status = domain.Completed
	job.ErrorMessage = ""
	job.UpdatedAt = time.Now()
	if err := s.repo.Save(ctx, job); err != nil {
		return fmt.Errorf("failed to mark job as Completed: %w", err)
	}

	_ = s.repo.RecordEvent(ctx, job.ID, "job_completed", nil)

	return nil
}

func (s *defaultJobService) handleFailure(ctx context.Context, job *domain.Job, errStr string) {
	job.Attempts++
	job.ErrorMessage = errStr
	job.UpdatedAt = time.Now()

	if job.Attempts >= s.maxAttempts {
		job.Status = domain.Dead
		monitoring.DLQJobsTotal.Inc()
	} else {
		job.Status = domain.Failed
	}

	// Persist the failure to DB
	_ = s.repo.Save(ctx, job)

	if job.Status == domain.Dead {
		_ = s.repo.RecordEvent(ctx, job.ID, "job_moved_to_dlq", map[string]any{
			"error":    errStr,
			"attempts": job.Attempts,
		})
	} else {
		_ = s.repo.RecordEvent(ctx, job.ID, "job_failed", map[string]any{
			"error":    errStr,
			"attempts": job.Attempts,
		})
	}

	// If the job is failed but not dead, republish it to the queue for retry with backoff delay
	if job.Status == domain.Failed {
		// delay = 2^attempts * 5 seconds
		delay := time.Duration(1<<job.Attempts) * 5 * time.Second
		go func() {
			// Use context.Background to ensure it runs even if parent context is cancelled
			time.Sleep(delay)
			if s.cb.AllowRequest() {
				_ = s.repo.RecordEvent(context.Background(), job.ID, "job_retried", map[string]any{
					"delay_seconds": delay.Seconds(),
				})
				err := s.producer.PublishJob(context.Background(), job)
				s.cb.RecordResult(err)
			} else {
				s.cb.RecordResult(fmt.Errorf("circuit breaker is open"))
			}
		}()
	}
}

// ─── Manual Circuit Breaker implementation ───────────────────────────────────

type CircuitState string

const (
	StateClosed   CircuitState = "CLOSED"
	StateOpen     CircuitState = "OPEN"
	StateHalfOpen CircuitState = "HALF_OPEN"
)

type CircuitBreaker struct {
	mu                  sync.RWMutex
	state               CircuitState
	consecutiveFailures int
	lastStateChange     time.Time
	openDuration        time.Duration
}

func NewCircuitBreaker(openDuration time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:        StateClosed,
		openDuration: openDuration,
		lastStateChange: time.Now(),
	}
}

func (cb *CircuitBreaker) AllowRequest() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == StateClosed {
		return true
	}

	if cb.state == StateOpen {
		if time.Since(cb.lastStateChange) >= cb.openDuration {
			cb.state = StateHalfOpen
			cb.lastStateChange = time.Now()
			return true
		}
		return false
	}

	// In HalfOpen state, allow the retry request
	return true
}

func (cb *CircuitBreaker) RecordResult(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err == nil {
		if cb.state == StateHalfOpen {
			cb.state = StateClosed
			cb.consecutiveFailures = 0
		} else if cb.state == StateClosed {
			cb.consecutiveFailures = 0
		}
	} else {
		cb.consecutiveFailures++
		if cb.state == StateClosed && cb.consecutiveFailures >= 5 {
			cb.state = StateOpen
			cb.lastStateChange = time.Now()
		} else if cb.state == StateHalfOpen {
			cb.state = StateOpen
			cb.lastStateChange = time.Now()
		}
	}
}

func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}
