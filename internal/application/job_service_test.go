package application_test

import (
    "context"
    "errors"
    "testing"
    "time"

    "distributed-task-queue/internal/application"
    "distributed-task-queue/internal/domain"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/mock"
    "github.com/stretchr/testify/require"
)

// ─── Mocks ────────────────────────────────────────────────────────────────────

type mockJobRepository struct {
    mock.Mock
}

func (m *mockJobRepository) Save(ctx context.Context, job *domain.Job) error {
    args := m.Called(ctx, job)
    return args.Error(0)
}

func (m *mockJobRepository) FindByID(ctx context.Context, id string) (*domain.Job, error) {
    args := m.Called(ctx, id)
    if j, ok := args.Get(0).(*domain.Job); ok {
        return j, args.Error(1)
    }
    return nil, args.Error(1)
}

func (m *mockJobRepository) UpdateStatus(ctx context.Context, id string, status domain.JobStatus) error {
    args := m.Called(ctx, id, status)
    return args.Error(0)
}

func (m *mockJobRepository) ListFailed(ctx context.Context, page, pageSize int) ([]*domain.Job, int, error) {
    args := m.Called(ctx, page, pageSize)
    if jobs, ok := args.Get(0).([]*domain.Job); ok {
        return jobs, args.Int(1), args.Error(2)
    }
    return nil, 0, args.Error(2)
}

func (m *mockJobRepository) ClaimJob(ctx context.Context) (*domain.Job, error) {
    args := m.Called(ctx)
    if j, ok := args.Get(0).(*domain.Job); ok {
        return j, args.Error(1)
    }
    return nil, args.Error(1)
}

func (m *mockJobRepository) RecordEvent(ctx context.Context, jobID string, eventType string, metadata map[string]any) error {
    args := m.Called(ctx, jobID, eventType, metadata)
    return args.Error(0)
}

func (m *mockJobRepository) GetEventsByJobID(ctx context.Context, jobID string) ([]*domain.JobEvent, error) {
    args := m.Called(ctx, jobID)
    if evs, ok := args.Get(0).([]*domain.JobEvent); ok {
        return evs, args.Error(1)
    }
    return nil, args.Error(1)
}

func (m *mockJobRepository) GetTenantStats(ctx context.Context, tenantID string) (*domain.TenantStats, error) {
    args := m.Called(ctx, tenantID)
    if stats, ok := args.Get(0).(*domain.TenantStats); ok {
        return stats, args.Error(1)
    }
    return nil, args.Error(1)
}

func (m *mockJobRepository) GetDeduplicatedJob(ctx context.Context, dedupKey string) (string, error) {
    args := m.Called(ctx, dedupKey)
    return args.String(0), args.Error(1)
}

func (m *mockJobRepository) SaveDeduplication(ctx context.Context, dedupKey string, jobID string) error {
    args := m.Called(ctx, dedupKey, jobID)
    return args.Error(0)
}

func (m *mockJobRepository) RegisterWorker(ctx context.Context, id, hostname string) error {
    args := m.Called(ctx, id, hostname)
    return args.Error(0)
}

func (m *mockJobRepository) UnregisterWorker(ctx context.Context, id string) error {
    args := m.Called(ctx, id)
    return args.Error(0)
}

func (m *mockJobRepository) UpdateWorkerHeartbeat(ctx context.Context, id string) error {
    args := m.Called(ctx, id)
    return args.Error(0)
}

func (m *mockJobRepository) RecoverZombies(ctx context.Context) ([]*domain.Job, error) {
    args := m.Called(ctx)
    if jobs, ok := args.Get(0).([]*domain.Job); ok {
        return jobs, args.Error(1)
    }
    return nil, args.Error(1)
}

type mockQueueProducer struct {
    mock.Mock
}

func (m *mockQueueProducer) PublishJob(ctx context.Context, job *domain.Job) error {
    args := m.Called(ctx, job)
    return args.Error(0)
}

func newTestService(repo *mockJobRepository, producer *mockQueueProducer) application.JobService {
    registry := application.JobHandlerRegistry{
        "test_job": func(ctx context.Context, payload []byte) error {
            return nil
        },
        "failing_job": func(ctx context.Context, payload []byte) error {
            return errors.New("handler error: simulated failure")
        },
        "slow_job": func(ctx context.Context, payload []byte) error {
            select {
            case <-ctx.Done():
                return ctx.Err()
            case <-time.After(2 * time.Second):
                return nil
            }
        },
        "blocking_job": func(ctx context.Context, payload []byte) error {
            <-ctx.Done()
            return ctx.Err()
        },
    }
    return application.NewJobService(repo, producer, registry, 3)
}

// ─── Tests ────────────────────────────────────────────────────────────────────

func TestEnqueueJob_Success(t *testing.T) {
    repo := new(mockJobRepository)
    producer := new(mockQueueProducer)
    svc := newTestService(repo, producer)

    ctx := context.Background()
    payload := []byte(`{"to":"user@example.com"}`)

    repo.On("Save", ctx, mock.AnythingOfType("*domain.Job")).Return(nil).Once()
    repo.On("RecordEvent", ctx, mock.Anything, "job_created", mock.Anything).Return(nil).Once()
    producer.On("PublishJob", ctx, mock.AnythingOfType("*domain.Job")).Return(nil).Once()

    job, err := svc.EnqueueJob(ctx, "test_job", payload)

    require.NoError(t, err)
    require.NotNil(t, job)
    assert.NotEmpty(t, job.ID)
    assert.Equal(t, "test_job", job.Type)
    assert.Equal(t, domain.Pending, job.Status)
    assert.Equal(t, 0, job.Attempts)
    assert.Equal(t, "default", job.TenantID)

    repo.AssertExpectations(t)
    producer.AssertExpectations(t)
}

func TestEnqueueJob_UsesContextTenantAndPriority(t *testing.T) {
    repo := new(mockJobRepository)
    producer := new(mockQueueProducer)
    svc := newTestService(repo, producer)

    ctx := context.WithValue(context.Background(), "tenant_id", "tenant-a")
    payload := []byte(`{"priority":9,"to":"ops@example.com"}`)

    repo.On("Save", ctx, mock.MatchedBy(func(j *domain.Job) bool {
        return j.TenantID == "tenant-a" && j.Priority == 9
    })).Return(nil).Once()
    repo.On("RecordEvent", ctx, mock.Anything, "job_created", mock.Anything).Return(nil).Once()
    producer.On("PublishJob", ctx, mock.AnythingOfType("*domain.Job")).Return(nil).Once()

    job, err := svc.EnqueueJob(ctx, "test_job", payload)

    require.NoError(t, err)
    require.NotNil(t, job)
    assert.Equal(t, "tenant-a", job.TenantID)
    assert.Equal(t, 9, job.Priority)

    repo.AssertExpectations(t)
    producer.AssertExpectations(t)
}

func TestEnqueueJob_SaveFailureReturnsError(t *testing.T) {
    repo := new(mockJobRepository)
    producer := new(mockQueueProducer)
    svc := newTestService(repo, producer)

    ctx := context.Background()
    repo.On("Save", ctx, mock.AnythingOfType("*domain.Job")).Return(errors.New("db connection lost")).Once()

    job, err := svc.EnqueueJob(ctx, "test_job", []byte(`{}`))

    require.Error(t, err)
    assert.Nil(t, job)
    assert.Contains(t, err.Error(), "failed to save job to DB")
    producer.AssertNotCalled(t, "PublishJob")
    repo.AssertExpectations(t)
}

func TestEnqueueJob_PublishFailureMarksCircuitBreaker(t *testing.T) {
    repo := new(mockJobRepository)
    producer := new(mockQueueProducer)
    svc := newTestService(repo, producer)

    ctx := context.Background()

    repo.On("Save", ctx, mock.AnythingOfType("*domain.Job")).Return(nil).Times(5)
    repo.On("RecordEvent", ctx, mock.Anything, "job_created", mock.Anything).Return(nil).Times(5)
    producer.On("PublishJob", ctx, mock.AnythingOfType("*domain.Job")).Return(errors.New("rabbitmq unavailable")).Times(5)

    for i := 0; i < 5; i++ {
        job, err := svc.EnqueueJob(ctx, "test_job", []byte(`{"priority":1}`))
        require.Error(t, err)
        assert.Contains(t, err.Error(), "failed to publish job to queue")
        assert.NotNil(t, job)
    }

    job, err := svc.EnqueueJob(ctx, "test_job", []byte(`{"priority":1}`))
    require.Error(t, err)
    assert.Contains(t, err.Error(), "circuit breaker is open")
    assert.Nil(t, job)

    repo.AssertExpectations(t)
    producer.AssertExpectations(t)
}

func TestGetJob_Success(t *testing.T) {
    repo := new(mockJobRepository)
    producer := new(mockQueueProducer)
    svc := newTestService(repo, producer)

    ctx := context.Background()
    expected := &domain.Job{ID: "job-1", Type: "test_job", Status: domain.Completed}
    repo.On("FindByID", ctx, "job-1").Return(expected, nil).Once()

    job, err := svc.GetJob(ctx, "job-1")

    require.NoError(t, err)
    assert.Equal(t, expected, job)
    repo.AssertExpectations(t)
}

func TestGetJob_Failure(t *testing.T) {
    repo := new(mockJobRepository)
    producer := new(mockQueueProducer)
    svc := newTestService(repo, producer)

    ctx := context.Background()
    repo.On("FindByID", ctx, "missing").Return((*domain.Job)(nil), errors.New("db lookup failed")).Once()

    job, err := svc.GetJob(ctx, "missing")

    require.Error(t, err)
    assert.Nil(t, job)
    assert.Contains(t, err.Error(), "failed to find job")
    repo.AssertExpectations(t)
}

func TestProcessJob_Success(t *testing.T) {
    repo := new(mockJobRepository)
    producer := new(mockQueueProducer)
    svc := newTestService(repo, producer)

    ctx := context.Background()
    job := &domain.Job{
        ID:      "job-789",
        Type:    "test_job",
        Payload: []byte(`{}`),
        Status:  domain.Pending,
    }

    repo.On("Save", ctx, mock.MatchedBy(func(j *domain.Job) bool {
        return j.Status == domain.Running
    })).Return(nil).Once()
    repo.On("RecordEvent", ctx, job.ID, "job_started", mock.Anything).Return(nil).Once()
    repo.On("Save", ctx, mock.MatchedBy(func(j *domain.Job) bool {
        return j.Status == domain.Completed
    })).Return(nil).Once()
    repo.On("RecordEvent", ctx, job.ID, "job_completed", mock.Anything).Return(nil).Once()

    err := svc.ProcessJob(ctx, job)

    require.NoError(t, err)
    assert.Equal(t, domain.Completed, job.Status)
    assert.Empty(t, job.ErrorMessage)
    repo.AssertExpectations(t)
    producer.AssertNotCalled(t, "PublishJob")
}

func TestProcessJob_SaveRunningStatusError(t *testing.T) {
    repo := new(mockJobRepository)
    producer := new(mockQueueProducer)
    svc := newTestService(repo, producer)

    ctx := context.Background()
    job := &domain.Job{ID: "job-run-error", Type: "test_job", Payload: []byte(`{}`)}

    repo.On("Save", ctx, mock.AnythingOfType("*domain.Job")).Return(errors.New("db write failed")).Once()

    err := svc.ProcessJob(ctx, job)

    require.Error(t, err)
    assert.Contains(t, err.Error(), "failed to update job status to Running")
    repo.AssertExpectations(t)
}

func TestProcessJob_UnknownHandlerMarksJobFailed(t *testing.T) {
    repo := new(mockJobRepository)
    producer := new(mockQueueProducer)
    svc := newTestService(repo, producer)

    ctx := context.Background()
    job := &domain.Job{ID: "job-unknown", Type: "missing_job", Payload: []byte(`{}`), Status: domain.Pending}

    repo.On("Save", ctx, mock.MatchedBy(func(j *domain.Job) bool {
        return j.Status == domain.Running
    })).Return(nil).Once()
    repo.On("RecordEvent", ctx, job.ID, "job_started", mock.Anything).Return(nil).Once()
    repo.On("Save", ctx, mock.MatchedBy(func(j *domain.Job) bool {
        return j.Status == domain.Failed && j.Attempts == 1
    })).Return(nil).Once()
    repo.On("RecordEvent", ctx, job.ID, "job_failed", mock.Anything).Return(nil).Once()
    repo.On("RecordEvent", mock.Anything, job.ID, "job_retried", mock.Anything).Return(nil).Maybe()
    producer.On("PublishJob", mock.Anything, mock.Anything).Return(nil).Maybe()

    err := svc.ProcessJob(ctx, job)

    require.Error(t, err)
    assert.Contains(t, err.Error(), "no handler registered")
    assert.Equal(t, domain.Failed, job.Status)
    assert.Equal(t, 1, job.Attempts)
    repo.AssertExpectations(t)
}

func TestProcessJob_TimeoutExceeded(t *testing.T) {
    repo := new(mockJobRepository)
    producer := new(mockQueueProducer)
    svc := newTestService(repo, producer)

    ctx := context.Background()
    job := &domain.Job{
        ID:      "job-timeout",
        Type:    "blocking_job",
        Payload: []byte(`{"timeout_seconds":1}`),
        Status:  domain.Pending,
    }

    repo.On("Save", ctx, mock.MatchedBy(func(j *domain.Job) bool {
        return j.Status == domain.Running
    })).Return(nil).Once()
    repo.On("RecordEvent", ctx, job.ID, "job_started", mock.Anything).Return(nil).Once()
    repo.On("Save", ctx, mock.MatchedBy(func(j *domain.Job) bool {
        return j.Status == domain.Failed && j.Attempts == 1
    })).Return(nil).Once()
    repo.On("RecordEvent", ctx, job.ID, "job_failed", mock.Anything).Return(nil).Once()
    repo.On("RecordEvent", mock.Anything, job.ID, "job_retried", mock.Anything).Return(nil).Maybe()
    producer.On("PublishJob", mock.Anything, mock.Anything).Return(nil).Maybe()

    err := svc.ProcessJob(ctx, job)

    require.Error(t, err)
    assert.Contains(t, err.Error(), "job handler execution failed")
    assert.Equal(t, domain.Failed, job.Status)
    assert.Equal(t, 1, job.Attempts)
}

func TestProcessJob_ExceedsMaxAttemptsMovesToDLQ(t *testing.T) {
    repo := new(mockJobRepository)
    producer := new(mockQueueProducer)
    svc := newTestService(repo, producer)

    ctx := context.Background()
    job := &domain.Job{
        ID:       "job-456",
        Type:     "failing_job",
        Payload:  []byte(`{}`),
        Status:   domain.Pending,
        Attempts: 2,
    }

    repo.On("Save", ctx, mock.MatchedBy(func(j *domain.Job) bool {
        return j.Status == domain.Running
    })).Return(nil).Once()
    repo.On("RecordEvent", ctx, job.ID, "job_started", mock.Anything).Return(nil).Once()
    repo.On("Save", ctx, mock.MatchedBy(func(j *domain.Job) bool {
        return j.Status == domain.Dead && j.Attempts == 3
    })).Return(nil).Once()
    repo.On("RecordEvent", ctx, job.ID, "job_moved_to_dlq", mock.Anything).Return(nil).Once()

    err := svc.ProcessJob(ctx, job)

    require.Error(t, err)
    assert.Equal(t, domain.Dead, job.Status)
    assert.Equal(t, 3, job.Attempts)
    assert.NotEmpty(t, job.ErrorMessage)
    assert.Contains(t, err.Error(), "job handler execution failed")
    repo.AssertExpectations(t)
    producer.AssertNotCalled(t, "PublishJob")
}

func TestProcessJob_CompletedSaveError(t *testing.T) {
    repo := new(mockJobRepository)
    producer := new(mockQueueProducer)
    svc := newTestService(repo, producer)

    ctx := context.Background()
    job := &domain.Job{ID: "job-complete-error", Type: "test_job", Payload: []byte(`{}`), Status: domain.Pending}

    repo.On("Save", ctx, mock.MatchedBy(func(j *domain.Job) bool {
        return j.Status == domain.Running
    })).Return(nil).Once()
    repo.On("RecordEvent", ctx, job.ID, "job_started", mock.Anything).Return(nil).Once()
    repo.On("Save", ctx, mock.MatchedBy(func(j *domain.Job) bool {
        return j.Status == domain.Completed
    })).Return(errors.New("db write failed")).Once()

    err := svc.ProcessJob(ctx, job)

    require.Error(t, err)
    assert.Contains(t, err.Error(), "failed to mark job as Completed")
    repo.AssertExpectations(t)
}
