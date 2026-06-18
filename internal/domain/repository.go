package domain

import "context"

type JobRepository interface {
	Save(ctx context.Context, job *Job) error
	FindByID(ctx context.Context, id string) (*Job, error)
	UpdateStatus(ctx context.Context, id string, status JobStatus) error
	ListFailed(ctx context.Context, page, pageSize int) ([]*Job, int, error)
	ClaimJob(ctx context.Context) (*Job, error)
	RecordEvent(ctx context.Context, jobID string, eventType string, metadata map[string]any) error
	GetEventsByJobID(ctx context.Context, jobID string) ([]*JobEvent, error)
	GetTenantStats(ctx context.Context, tenantID string) (*TenantStats, error)
	GetDeduplicatedJob(ctx context.Context, dedupKey string) (string, error)
	SaveDeduplication(ctx context.Context, dedupKey string, jobID string) error
	RegisterWorker(ctx context.Context, id, hostname string) error
	UnregisterWorker(ctx context.Context, id string) error
	UpdateWorkerHeartbeat(ctx context.Context, id string) error
	RecoverZombies(ctx context.Context) ([]*Job, error)
}
