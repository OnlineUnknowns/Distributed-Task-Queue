package http_test

import (
    "bytes"
    "context"
    "encoding/json"
    "net/http"
    "net/http/httptest"
    "testing"
    "time"

    "distributed-task-queue/internal/domain"
    transporthttp "distributed-task-queue/internal/transport/http"
    "github.com/gin-gonic/gin"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/mock"
    "github.com/stretchr/testify/require"
    "go.uber.org/zap"
)

type mockJobService struct {
    mock.Mock
}

func (m *mockJobService) EnqueueJob(ctx context.Context, jobType string, payload []byte) (*domain.Job, error) {
    args := m.Called(ctx, jobType, payload)
    if job, ok := args.Get(0).(*domain.Job); ok {
        return job, args.Error(1)
    }
    return nil, args.Error(1)
}

func (m *mockJobService) GetJob(ctx context.Context, id string) (*domain.Job, error) {
    args := m.Called(ctx, id)
    if job, ok := args.Get(0).(*domain.Job); ok {
        return job, args.Error(1)
    }
    return nil, args.Error(1)
}

func (m *mockJobService) ProcessJob(ctx context.Context, job *domain.Job) error {
    args := m.Called(ctx, job)
    return args.Error(0)
}

type mockJobRepository struct {
    mock.Mock
}

func (m *mockJobRepository) Save(ctx context.Context, job *domain.Job) error {
    args := m.Called(ctx, job)
    return args.Error(0)
}

func (m *mockJobRepository) FindByID(ctx context.Context, id string) (*domain.Job, error) {
    args := m.Called(ctx, id)
    if job, ok := args.Get(0).(*domain.Job); ok {
        return job, args.Error(1)
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
    if job, ok := args.Get(0).(*domain.Job); ok {
        return job, args.Error(1)
    }
    return nil, args.Error(1)
}

func (m *mockJobRepository) RecordEvent(ctx context.Context, jobID string, eventType string, metadata map[string]any) error {
    args := m.Called(ctx, jobID, eventType, metadata)
    return args.Error(0)
}

func (m *mockJobRepository) GetEventsByJobID(ctx context.Context, jobID string) ([]*domain.JobEvent, error) {
    args := m.Called(ctx, jobID)
    if events, ok := args.Get(0).([]*domain.JobEvent); ok {
        return events, args.Error(1)
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

func newTestRouter(t *testing.T, repo *mockJobRepository, svc *mockJobService) *gin.Engine {
    t.Helper()
    gin.SetMode(gin.TestMode)
    handler := transporthttp.NewJobHandler(svc, repo, zap.NewNop())
    router := gin.New()
    transporthttp.RegisterRoutes(router, handler, zap.NewNop())
    return router
}

func TestEnqueueJob_IdempotencyReturnsCachedResponseWithoutReEnqueue(t *testing.T) {
    repo := new(mockJobRepository)
    svc := new(mockJobService)
    router := newTestRouter(t, repo, svc)

    cachedJob := &domain.Job{
        ID:        "job-cached",
        Type:      "email",
        Status:    domain.Pending,
        CreatedAt: time.Now().UTC(),
    }

    repo.On("GetDeduplicatedJob", mock.Anything, "dedupe-key").Return(cachedJob.ID, nil).Once()
    repo.On("FindByID", mock.Anything, cachedJob.ID).Return(cachedJob, nil).Once()

    reqBody := bytes.NewBufferString(`{"type":"email","payload":{"foo":"bar"}}`)
    req := httptest.NewRequest(http.MethodPost, "/jobs", reqBody)
    req.Header.Set("Content-Type", "application/json")
    req.Header.Set("X-Tenant-ID", "tenant-a")
    req.Header.Set("Idempotency-Key", "dedupe-key")

    rr := httptest.NewRecorder()
    router.ServeHTTP(rr, req)

    require.Equal(t, http.StatusOK, rr.Code)

    var resp map[string]any
    require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
    assert.Equal(t, cachedJob.ID, resp["id"])
    assert.Equal(t, true, resp["cached"])

    svc.AssertNotCalled(t, "EnqueueJob", mock.Anything, mock.Anything, mock.Anything)
    repo.AssertExpectations(t)
}

func TestEnqueueJob_SavesDeduplicationAfterSuccessfulEnqueue(t *testing.T) {
    repo := new(mockJobRepository)
    svc := new(mockJobService)
    router := newTestRouter(t, repo, svc)

    createdJob := &domain.Job{
        ID:        "job-new",
        Type:      "email",
        Status:    domain.Pending,
        CreatedAt: time.Now().UTC(),
    }

    repo.On("GetDeduplicatedJob", mock.Anything, "dedupe-key").Return("", nil).Once()
    svc.On("EnqueueJob", mock.Anything, "email", mock.Anything).Return(createdJob, nil).Once()
    repo.On("SaveDeduplication", mock.Anything, "dedupe-key", createdJob.ID).Return(nil).Once()

    payload := bytes.NewBufferString(`{"type":"email","payload":{"foo":"bar"}}`)
    req := httptest.NewRequest(http.MethodPost, "/jobs", payload)
    req.Header.Set("Content-Type", "application/json")
    req.Header.Set("X-Tenant-ID", "tenant-a")
    req.Header.Set("Idempotency-Key", "dedupe-key")

    rr := httptest.NewRecorder()
    router.ServeHTTP(rr, req)

    require.Equal(t, http.StatusCreated, rr.Code)

    var resp map[string]any
    require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
    assert.Equal(t, createdJob.ID, resp["id"])

    repo.AssertExpectations(t)
    svc.AssertExpectations(t)
}

func TestEnqueueJob_RejectsMissingTenantHeader(t *testing.T) {
    repo := new(mockJobRepository)
    svc := new(mockJobService)
    router := newTestRouter(t, repo, svc)

    payload := bytes.NewBufferString(`{"type":"email","payload":{"foo":"bar"}}`)
    req := httptest.NewRequest(http.MethodPost, "/jobs", payload)
    req.Header.Set("Content-Type", "application/json")

    rr := httptest.NewRecorder()
    router.ServeHTTP(rr, req)

    require.Equal(t, http.StatusBadRequest, rr.Code)

    var resp map[string]any
    require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
    assert.Equal(t, "X-Tenant-ID header is required", resp["error"])

    svc.AssertNotCalled(t, "EnqueueJob", mock.Anything, mock.Anything, mock.Anything)
}