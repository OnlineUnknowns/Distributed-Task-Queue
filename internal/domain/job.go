package domain

import "time"

type Job struct {
	ID             string    `json:"id"`
	Type           string    `json:"type"`
	Payload        []byte    `json:"payload"`
	Status         JobStatus `json:"status"`
	Attempts       int       `json:"attempts"`
	ErrorMessage   string    `json:"error_message,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	Priority       int       `json:"priority"`
	TenantID       string    `json:"tenant_id"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
}

type JobEvent struct {
	ID         string         `json:"id"`
	JobID      string         `json:"job_id"`
	EventType  string         `json:"event_type"`
	Metadata   map[string]any `json:"metadata"`
	OccurredAt time.Time      `json:"occurred_at"`
}

type TenantStats struct {
	TotalJobs          int64   `json:"total_jobs"`
	SuccessRate        float64 `json:"success_rate"`
	AverageProcessTime float64 `json:"average_processing_time_seconds"`
	CurrentQueueDepth  int64   `json:"current_queue_depth"`
}

