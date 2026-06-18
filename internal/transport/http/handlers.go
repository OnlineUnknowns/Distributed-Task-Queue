package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"distributed-task-queue/internal/application"
	"distributed-task-queue/internal/domain"
	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

// JobHandler holds dependencies for HTTP job endpoints.
type JobHandler struct {
	service application.JobService
	repo    domain.JobRepository
	logger  *zap.Logger
}

func NewJobHandler(service application.JobService, repo domain.JobRepository, logger *zap.Logger) *JobHandler {
	return &JobHandler{
		service: service,
		repo:    repo,
		logger:  logger,
	}
}

// RegisterRoutes wires up all routes and middleware onto the given gin.Engine.
func RegisterRoutes(r *gin.Engine, h *JobHandler, logger *zap.Logger) {
	// Middleware: request tracing
	r.Use(tracingMiddleware())

	// Middleware: structured request logging
	r.Use(zapLoggerMiddleware(logger))

	// Middleware: recovery from panics
	r.Use(gin.RecoveryWithWriter(nil, func(c *gin.Context, err any) {
		logger.Error("panic recovered",
			zap.Any("error", err),
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
		)
		c.AbortWithStatusJSON(http.StatusInternalServerError, gin.H{
			"error": "internal server error",
		})
	}))

	// Routes
	r.GET("/health", h.Health)

	jobs := r.Group("/jobs")
	{
		jobs.POST("", h.EnqueueJob)
		jobs.GET("/:id", h.GetJob)
		jobs.GET("/:id/history", h.GetJobHistory)
		jobs.POST("/:id/replay", h.ReplayJob)
	}

	tenants := r.Group("/tenants")
	{
		tenants.GET("/:id/stats", h.GetTenantStats)
	}
}

// zapLoggerMiddleware logs method, path, status code, and latency using zap.
func tracingMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		spanName := c.Request.Method + " " + c.Request.URL.Path
		ctx, span := otel.Tracer("taskq-api").Start(c.Request.Context(), spanName)
		defer span.End()
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}

func zapLoggerMiddleware(logger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		method := c.Request.Method

		c.Next()

		logger.Info("request",
			zap.String("method", method),
			zap.String("path", path),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("latency", time.Since(start)),
			zap.String("client_ip", c.ClientIP()),
		)
	}
}

// Health handles GET /health
func (h *JobHandler) Health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// enqueueJobRequest is the expected JSON body for POST /jobs.
type enqueueJobRequest struct {
	Type    string          `json:"type"    binding:"required"`
	Payload json.RawMessage `json:"payload" binding:"required"`
}

// EnqueueJob handles POST /jobs
// Requires X-Tenant-ID header. Supports Idempotency-Key header.
func (h *JobHandler) EnqueueJob(c *gin.Context) {
	// PART 11: enforce tenant ID
	tenantID := c.GetHeader("X-Tenant-ID")
	if tenantID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "X-Tenant-ID header is required"})
		return
	}

	// PART 8: check idempotency key
	idempotencyKey := c.GetHeader("Idempotency-Key")
	if idempotencyKey != "" {
		cached, err := h.repo.GetDeduplicatedJob(c.Request.Context(), idempotencyKey)
		if err == nil && cached != "" {
			// Return cached response
			job, err := h.repo.FindByID(c.Request.Context(), cached)
			if err == nil {
				c.JSON(http.StatusOK, gin.H{
					"id":         job.ID,
					"type":       job.Type,
					"status":     job.Status,
					"created_at": job.CreatedAt,
					"cached":     true,
				})
				return
			}
		}
	}

	var req enqueueJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Inject tenant_id into context
	ctx := context.WithValue(c.Request.Context(), "tenant_id", tenantID) //nolint:staticcheck

	job, err := h.service.EnqueueJob(ctx, req.Type, req.Payload)
	if err != nil {
		h.logger.Error("failed to enqueue job", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to enqueue job"})
		return
	}

	// Save idempotency key mapping after successful enqueue
	if idempotencyKey != "" {
		_ = h.repo.SaveDeduplication(c.Request.Context(), idempotencyKey, job.ID)
	}

	c.JSON(http.StatusCreated, gin.H{
		"id":         job.ID,
		"type":       job.Type,
		"status":     job.Status,
		"created_at": job.CreatedAt,
	})
}

// GetJob handles GET /jobs/:id
func (h *JobHandler) GetJob(c *gin.Context) {
	id := c.Param("id")

	job, err := h.service.GetJob(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrJobNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
			return
		}
		h.logger.Error("failed to fetch job", zap.String("job_id", id), zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch job"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"id":             job.ID,
		"type":           job.Type,
		"status":         job.Status,
		"attempts":       job.Attempts,
		"error_message":  job.ErrorMessage,
		"priority":       job.Priority,
		"tenant_id":      job.TenantID,
		"created_at":     job.CreatedAt,
		"updated_at":     job.UpdatedAt,
	})
}

// GetJobHistory handles GET /jobs/:id/history — returns all state-transition events.
func (h *JobHandler) GetJobHistory(c *gin.Context) {
	id := c.Param("id")

	events, err := h.repo.GetEventsByJobID(c.Request.Context(), id)
	if err != nil {
		h.logger.Error("failed to get job history", zap.String("job_id", id), zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get job history"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"job_id": id,
		"events": events,
	})
}

// ReplayJob handles POST /jobs/:id/replay — re-enqueues a Dead job.
func (h *JobHandler) ReplayJob(c *gin.Context) {
	id := c.Param("id")

	job, err := h.service.GetJob(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, domain.ErrJobNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to fetch job"})
		return
	}

	if job.Status != domain.Dead {
		c.JSON(http.StatusBadRequest, gin.H{"error": "only Dead jobs can be replayed"})
		return
	}

	// Re-enqueue by creating a fresh job of the same type/payload
	tenantID := job.TenantID
	if tenantID == "" {
		tenantID = "default"
	}
	ctx := context.WithValue(c.Request.Context(), "tenant_id", tenantID) //nolint:staticcheck

	newJob, err := h.service.EnqueueJob(ctx, job.Type, job.Payload)
	if err != nil {
		h.logger.Error("failed to replay job", zap.String("job_id", id), zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to replay job"})
		return
	}

	_ = h.repo.RecordEvent(c.Request.Context(), id, "job_replayed", map[string]any{
		"new_job_id": newJob.ID,
	})

	c.JSON(http.StatusCreated, gin.H{
		"original_job_id": id,
		"new_job_id":      newJob.ID,
		"status":          newJob.Status,
	})
}

// GetTenantStats handles GET /tenants/:id/stats
func (h *JobHandler) GetTenantStats(c *gin.Context) {
	tenantID := c.Param("id")

	stats, err := h.repo.GetTenantStats(c.Request.Context(), tenantID)
	if err != nil {
		h.logger.Error("failed to get tenant stats", zap.String("tenant_id", tenantID), zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get tenant stats"})
		return
	}

	c.JSON(http.StatusOK, stats)
}
