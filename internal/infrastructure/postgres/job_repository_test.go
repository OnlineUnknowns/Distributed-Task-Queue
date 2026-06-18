package postgres_test

import (
    "context"
    "os"
    "sync"
    "testing"
    "time"

    "distributed-task-queue/internal/domain"
    "distributed-task-queue/internal/infrastructure/postgres"

    "github.com/jackc/pgx/v5/pgxpool"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

// connectTestDB connects to the test database using the TEST_DB_URL env var.
// Skips the test if the env var is not set.
func connectTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DB_URL")
	if dsn == "" {
		t.Skip("TEST_DB_URL not set — skipping integration test")
	}

	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err, "failed to connect to test DB")
	require.NoError(t, pool.Ping(context.Background()), "failed to ping test DB")

	t.Cleanup(func() { pool.Close() })
	return pool
}

// truncateJobs clears the jobs table between tests.
func truncateJobs(t *testing.T, pool *pgxpool.Pool) {
	t.Helper()
	_, err := pool.Exec(context.Background(), "TRUNCATE TABLE jobs")
	require.NoError(t, err)
}

// newTestJob creates a domain.Job with unique ID for use in tests.
func newTestJob(id, jobType string) *domain.Job {
	now := time.Now().UTC().Truncate(time.Second)
	return &domain.Job{
		ID:        id,
		Type:      jobType,
		Payload:   []byte(`{"key":"value"}`),
		Status:    domain.Pending,
		Attempts:  0,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// ─── Tests ────────────────────────────────────────────────────────────────────

func TestJobRepository_Save_And_FindByID(t *testing.T) {
	pool := connectTestDB(t)
	repo := postgres.NewJobRepository(pool)
	ctx := context.Background()
	truncateJobs(t, pool)

	job := newTestJob("550e8400-e29b-41d4-a716-446655440001", "send_email")

	err := repo.Save(ctx, job)
	require.NoError(t, err, "Save should not return an error")

	fetched, err := repo.FindByID(ctx, job.ID)
	require.NoError(t, err, "FindByID should not return an error")

	assert.Equal(t, job.ID, fetched.ID)
	assert.Equal(t, job.Type, fetched.Type)
	assert.Equal(t, job.Status, fetched.Status)
	assert.Equal(t, job.Attempts, fetched.Attempts)
}

func TestJobRepository_FindByID_NotFound(t *testing.T) {
	pool := connectTestDB(t)
	repo := postgres.NewJobRepository(pool)
	ctx := context.Background()
	truncateJobs(t, pool)

	_, err := repo.FindByID(ctx, "550e8400-e29b-41d4-a716-000000000000")
	require.ErrorIs(t, err, domain.ErrJobNotFound, "should return ErrJobNotFound for missing ID")
}

func TestJobRepository_UpdateStatus(t *testing.T) {
	pool := connectTestDB(t)
	repo := postgres.NewJobRepository(pool)
	ctx := context.Background()
	truncateJobs(t, pool)

	job := newTestJob("550e8400-e29b-41d4-a716-446655440002", "process_data")
	require.NoError(t, repo.Save(ctx, job))

	err := repo.UpdateStatus(ctx, job.ID, domain.Running)
	require.NoError(t, err)

	fetched, err := repo.FindByID(ctx, job.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.Running, fetched.Status)
}

func TestJobRepository_UpdateStatus_NotFound(t *testing.T) {
	pool := connectTestDB(t)
	repo := postgres.NewJobRepository(pool)
	ctx := context.Background()
	truncateJobs(t, pool)

	err := repo.UpdateStatus(ctx, "550e8400-e29b-41d4-a716-000000000000", domain.Completed)
	require.ErrorIs(t, err, domain.ErrJobNotFound)
}

func TestJobRepository_Save_CancelledContextDoesNotPersistJob(t *testing.T) {
	pool := connectTestDB(t)
	repo := postgres.NewJobRepository(pool)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	truncateJobs(t, pool)

	job := newTestJob("550e8400-e29b-41d4-a716-446655440006", "cancelled_job")
	err := repo.Save(ctx, job)
	require.Error(t, err)

	_, err = repo.FindByID(context.Background(), job.ID)
	require.ErrorIs(t, err, domain.ErrJobNotFound)
}

func TestJobRepository_Save_Upsert(t *testing.T) {
	pool := connectTestDB(t)
	repo := postgres.NewJobRepository(pool)
	ctx := context.Background()
	truncateJobs(t, pool)

	job := newTestJob("550e8400-e29b-41d4-a716-446655440003", "send_email")
	require.NoError(t, repo.Save(ctx, job))

	// Update and re-save (upsert)
	job.Status = domain.Completed
	job.Attempts = 1
	job.ErrorMessage = ""
	require.NoError(t, repo.Save(ctx, job))

	fetched, err := repo.FindByID(ctx, job.ID)
	require.NoError(t, err)
	assert.Equal(t, domain.Completed, fetched.Status)
	assert.Equal(t, 1, fetched.Attempts)
}

func TestJobRepository_ListFailed_Pagination(t *testing.T) {
	pool := connectTestDB(t)
	repo := postgres.NewJobRepository(pool)
	ctx := context.Background()
	truncateJobs(t, pool)

	firstFailed := newTestJob("550e8400-e29b-41d4-a716-446655440004", "send_email")
	firstFailed.Status = domain.Failed
	firstFailed.Attempts = 1
	firstFailed.ErrorMessage = "first timeout"
	firstFailed.CreatedAt = time.Now().UTC().Add(-2 * time.Second)
	firstFailed.UpdatedAt = firstFailed.CreatedAt

	secondFailed := newTestJob("550e8400-e29b-41d4-a716-446655440005", "send_email")
	secondFailed.Status = domain.Failed
	secondFailed.Attempts = 2
	secondFailed.ErrorMessage = "second timeout"
	secondFailed.CreatedAt = time.Now().UTC().Add(-1 * time.Second)
	secondFailed.UpdatedAt = secondFailed.CreatedAt

	thirdFailed := newTestJob("550e8400-e29b-41d4-a716-446655440006", "send_email")
	thirdFailed.Status = domain.Failed
	thirdFailed.Attempts = 3
	thirdFailed.ErrorMessage = "third timeout"
	thirdFailed.CreatedAt = time.Now().UTC()
	thirdFailed.UpdatedAt = thirdFailed.CreatedAt

	completed := newTestJob("550e8400-e29b-41d4-a716-446655440007", "send_email")
	completed.Status = domain.Completed

	require.NoError(t, repo.Save(ctx, firstFailed))
	require.NoError(t, repo.Save(ctx, secondFailed))
	require.NoError(t, repo.Save(ctx, thirdFailed))
	require.NoError(t, repo.Save(ctx, completed))

	results, total, err := repo.ListFailed(ctx, 1, 2)
	require.NoError(t, err)
	require.Equal(t, 3, total)
	require.Len(t, results, 2)
	assert.Equal(t, thirdFailed.ID, results[0].ID)
	assert.Equal(t, secondFailed.ID, results[1].ID)
}

func TestJobRepository_ClaimJob_ConcurrentWorkersDoNotClaimSameJob(t *testing.T) {
	pool := connectTestDB(t)
	repo := postgres.NewJobRepository(pool)
	ctx := context.Background()
	truncateJobs(t, pool)

	jobOne := newTestJob("550e8400-e29b-41d4-a716-446655440008", "concurrent_job")
	jobTwo := newTestJob("550e8400-e29b-41d4-a716-446655440009", "concurrent_job")
	require.NoError(t, repo.Save(ctx, jobOne))
	require.NoError(t, repo.Save(ctx, jobTwo))

	results := make(chan *domain.Job, 2)
	errCh := make(chan error, 2)
	var wg sync.WaitGroup

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			claimed, err := repo.ClaimJob(ctx)
			if err != nil {
				errCh <- err
				return
			}
			if claimed != nil {
				results <- claimed
			}
		}()
	}

	wg.Wait()
	close(results)
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}

	seen := make(map[string]struct{})
	for claimed := range results {
		_, exists := seen[claimed.ID]
		require.False(t, exists, "same job should not be claimed twice")
		seen[claimed.ID] = struct{}{}
	}

	require.Len(t, seen, 2)

	for _, id := range []string{jobOne.ID, jobTwo.ID} {
		job, err := repo.FindByID(ctx, id)
		require.NoError(t, err)
		assert.Equal(t, domain.Running, job.Status)
	}
}

func TestJobRepository_ClaimJob_PrefersHighPriorityJobs(t *testing.T) {
	pool := connectTestDB(t)
	repo := postgres.NewJobRepository(pool)
	ctx := context.Background()
	truncateJobs(t, pool)

	lowPriority := newTestJob("550e8400-e29b-41d4-a716-446655440010", "priority_job")
	lowPriority.Priority = 1
	lowPriority.CreatedAt = time.Now().UTC().Add(-10 * time.Second)
	lowPriority.UpdatedAt = lowPriority.CreatedAt

	highPriority := newTestJob("550e8400-e29b-41d4-a716-446655440011", "priority_job")
	highPriority.Priority = 9
	highPriority.CreatedAt = time.Now().UTC().Add(-5 * time.Second)
	highPriority.UpdatedAt = highPriority.CreatedAt

	require.NoError(t, repo.Save(ctx, lowPriority))
	require.NoError(t, repo.Save(ctx, highPriority))

	claimed, err := repo.ClaimJob(ctx)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	assert.Equal(t, highPriority.ID, claimed.ID)
}

func TestJobRepository_GetTenantStats_IsolatesTenantData(t *testing.T) {
	pool := connectTestDB(t)
	repo := postgres.NewJobRepository(pool)
	ctx := context.Background()
	truncateJobs(t, pool)

	now := time.Now().UTC()
	completed := newTestJob("550e8400-e29b-41d4-a716-446655440012", "tenant_job")
	completed.TenantID = "tenant-a"
	completed.Status = domain.Completed
	completed.CreatedAt = now.Add(-2 * time.Second)
	completed.UpdatedAt = now

	pending := newTestJob("550e8400-e29b-41d4-a716-446655440013", "tenant_job")
	pending.TenantID = "tenant-a"
	pending.Status = domain.Pending
	pending.CreatedAt = now.Add(-5 * time.Second)
	pending.UpdatedAt = now.Add(-4 * time.Second)

	otherTenant := newTestJob("550e8400-e29b-41d4-a716-446655440014", "tenant_job")
	otherTenant.TenantID = "tenant-b"
	otherTenant.Status = domain.Completed
	otherTenant.CreatedAt = now.Add(-1 * time.Second)
	otherTenant.UpdatedAt = now

	require.NoError(t, repo.Save(ctx, completed))
	require.NoError(t, repo.Save(ctx, pending))
	require.NoError(t, repo.Save(ctx, otherTenant))

	stats, err := repo.GetTenantStats(ctx, "tenant-a")
	require.NoError(t, err)
	assert.EqualValues(t, 2, stats.TotalJobs)
	assert.InDelta(t, 0.5, stats.SuccessRate, 0.0001)
	assert.EqualValues(t, 1, stats.CurrentQueueDepth)
}

func TestJobRepository_SaveDeduplication_And_GetDeduplicatedJob(t *testing.T) {
	pool := connectTestDB(t)
	repo := postgres.NewJobRepository(pool)
	ctx := context.Background()
	truncateJobs(t, pool)

	dedupKey := "dedup:test-key"
	jobID := "550e8400-e29b-41d4-a716-446655440015"

	require.NoError(t, repo.SaveDeduplication(ctx, dedupKey, jobID))
	resolvedJobID, err := repo.GetDeduplicatedJob(ctx, dedupKey)
	require.NoError(t, err)
	assert.Equal(t, jobID, resolvedJobID)
}

func TestJobRepository_RecoverZombies_RecoversExpiredRunningJobs(t *testing.T) {
	pool := connectTestDB(t)
	repo := postgres.NewJobRepository(pool)
	ctx := context.Background()
	truncateJobs(t, pool)

	staleJob := newTestJob("550e8400-e29b-41d4-a716-446655440016", "zombie_job")
	staleJob.Status = domain.Running
	staleJob.CreatedAt = time.Now().UTC().Add(-20 * time.Minute)
	staleJob.UpdatedAt = time.Now().UTC().Add(-10 * time.Minute)

	require.NoError(t, repo.Save(ctx, staleJob))

	recovered, err := repo.RecoverZombies(ctx)
	require.NoError(t, err)
	require.Len(t, recovered, 1)
	assert.Equal(t, staleJob.ID, recovered[0].ID)
	assert.Equal(t, domain.Pending, recovered[0].Status)
}
