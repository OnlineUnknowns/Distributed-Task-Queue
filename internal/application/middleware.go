package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"distributed-task-queue/internal/domain"
)

type DeduplicatingJobService struct {
	next JobService
	repo domain.JobRepository
}

func NewDeduplicatingJobService(next JobService, repo domain.JobRepository) *DeduplicatingJobService {
	return &DeduplicatingJobService{
		next: next,
		repo: repo,
	}
}

// Ensure DeduplicatingJobService implements JobService interface
var _ JobService = (*DeduplicatingJobService)(nil)

func (s *DeduplicatingJobService) EnqueueJob(ctx context.Context, jobType string, payload []byte) (*domain.Job, error) {
	dedupKey := computeDedupKey(jobType, payload)

	existingID, err := s.repo.GetDeduplicatedJob(ctx, dedupKey)
	if err == nil && existingID != "" {
		existingJob, err := s.repo.FindByID(ctx, existingID)
		if err == nil {
			return existingJob, nil
		}
	}

	job, err := s.next.EnqueueJob(ctx, jobType, payload)
	if err != nil {
		return nil, err
	}

	_ = s.repo.SaveDeduplication(ctx, dedupKey, job.ID)

	return job, nil
}

func (s *DeduplicatingJobService) GetJob(ctx context.Context, id string) (*domain.Job, error) {
	return s.next.GetJob(ctx, id)
}

func (s *DeduplicatingJobService) ProcessJob(ctx context.Context, job *domain.Job) error {
	return s.next.ProcessJob(ctx, job)
}

func computeDedupKey(jobType string, payload []byte) string {
	var parsed map[string]any
	canonical := payload
	if err := json.Unmarshal(payload, &parsed); err == nil {
		if sortedBytes, err := json.Marshal(parsed); err == nil {
			canonical = sortedBytes
		}
	}
	hash := sha256.Sum256(append([]byte(jobType), canonical...))
	return hex.EncodeToString(hash[:])
}
