package monitoring

import (
	"sync/atomic"
	"math"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// currentQueueDepthBits stores the float64 queue depth as uint64 bits atomically.
var currentQueueDepthBits uint64

// GetCurrentQueueDepth returns the last polled queue depth as a float64.
func GetCurrentQueueDepth() float64 {
	bits := atomic.LoadUint64(&currentQueueDepthBits)
	return math.Float64frombits(bits)
}

// SetCurrentQueueDepth updates both the atomic backing variable and the Prometheus gauge.
func SetCurrentQueueDepth(depth float64) {
	atomic.StoreUint64(&currentQueueDepthBits, math.Float64bits(depth))
	CurrentQueueDepth.Set(depth)
}

var (
	// JobsProcessedTotal counts the total number of jobs processed,
	// labelled by job_type and status (completed | failed).
	JobsProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "jobs_processed_total",
			Help: "Total number of jobs processed by the worker pool.",
		},
		[]string{"job_type", "status"},
	)

	// JobProcessingDuration measures the time taken to process a job,
	// labelled by job_type.
	JobProcessingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "job_processing_duration_seconds",
			Help:    "Time spent processing a job in seconds.",
			Buckets: prometheus.DefBuckets, // .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10
		},
		[]string{"job_type"},
	)

	// JobsInQueue is a gauge representing the estimated number of pending jobs.
	// Increment when a job is enqueued; decrement when it starts processing.
	JobsInQueue = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "jobs_in_queue",
			Help: "Current estimated number of jobs waiting in the queue.",
		},
	)

	// WorkerHeartbeatTimestamp tracks worker heartbeat timestamp.
	WorkerHeartbeatTimestamp = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "worker_heartbeat_timestamp",
			Help: "Timestamp of the last registered worker heartbeat.",
		},
		[]string{"worker_id"},
	)

	// DLQJobsTotal counts the total jobs moved to the Dead Letter Queue.
	DLQJobsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "dlq_jobs_total",
			Help: "Total number of jobs moved to the dead letter queue (DLQ).",
		},
	)

	// DBQueryDuration measures database query execution time by operation.
	DBQueryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "db_query_duration_seconds",
			Help:    "Duration of database operations in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation"},
	)

	// CurrentQueueDepth exposes the raw depth of RabbitMQ queues.
	CurrentQueueDepth = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "current_queue_depth",
			Help: "Total number of messages currently in RabbitMQ queues.",
		},
	)
)
