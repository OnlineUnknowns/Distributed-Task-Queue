package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"distributed-task-queue/internal/application"
	"distributed-task-queue/internal/infrastructure/postgres"
	"distributed-task-queue/internal/infrastructure/rabbitmq"
	"distributed-task-queue/internal/observability"
	transporthttp "distributed-task-queue/internal/transport/http"
	"distributed-task-queue/monitoring"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

func main() {
	// Initialize structured logger
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("failed to initialize logger: %v", err)
	}
	defer logger.Sync()

	logger.Info("Starting API service...")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize OpenTelemetry tracer
	tp, err := observability.InitTracer(ctx, "taskq-api")
	if err != nil {
		logger.Warn("failed to initialize tracer, continuing without tracing", zap.Error(err))
	} else {
		defer func() {
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer shutdownCancel()
			_ = tp.Shutdown(shutdownCtx)
		}()
	}

	// Initialize DB connection pool
	dbPool, err := postgres.NewConnectionPool(ctx)
	if err != nil {
		logger.Fatal("failed to initialize DB connection pool", zap.Error(err))
	}
	defer dbPool.Close()
	logger.Info("Database connection pool established")

	// Initialize repository
	repo := postgres.NewJobRepository(dbPool)

	// Initialize RabbitMQ producer
	producer, err := rabbitmq.NewJobProducer()
	if err != nil {
		logger.Fatal("failed to initialize RabbitMQ producer", zap.Error(err))
	}
	defer producer.Close()
	logger.Info("RabbitMQ producer connected")

	// Initialize job service
	jobService := application.NewJobService(repo, producer, application.DefaultRegistry, 3)

	// Setup Gin engine
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()

	// Register handlers & middleware
	handler := transporthttp.NewJobHandler(jobService, repo, logger)
	transporthttp.RegisterRoutes(router, handler, logger)

	// Expose Prometheus metrics endpoint
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// PART 9: Background poller for RabbitMQ queue depth
	rabbitmqMgmtURL := os.Getenv("RABBITMQ_MANAGEMENT_URL")
	if rabbitmqMgmtURL == "" {
		rabbitmqMgmtURL = "http://guest:guest@rabbitmq:15672"
	}
	maxQueueDepthStr := os.Getenv("MAX_QUEUE_DEPTH")
	maxQueueDepth := 10000
	if maxQueueDepthStr != "" {
		if val, convErr := strconv.Atoi(maxQueueDepthStr); convErr == nil && val > 0 {
			maxQueueDepth = val
		}
	}

	go pollQueueDepth(ctx, logger, rabbitmqMgmtURL)

	// Override EnqueueJob to check backpressure before calling the real handler
	router.Use(func(c *gin.Context) {
		if c.Request.Method == http.MethodPost && c.FullPath() == "/jobs" {
			depth := int(getCurrentQueueDepth())
			if depth > maxQueueDepth {
				c.Header("Retry-After", "5")
				c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
					"error":       "queue is at capacity, please retry later",
					"queue_depth": depth,
					"max_depth":   maxQueueDepth,
				})
				return
			}
		}
		c.Next()
	})

	// Determine port
	port := os.Getenv("API_PORT")
	if port == "" {
		port = "8080"
	}

	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start HTTP server in a background goroutine
	go func() {
		logger.Info("HTTP server listening", zap.String("port", port))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatal("HTTP server failed", zap.Error(err))
		}
	}()

	// Wait for OS shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	sig := <-sigChan
	logger.Info("Shutdown signal received", zap.String("signal", sig.String()))

	// Graceful shutdown: give active requests up to 10s to complete
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server forced to shutdown", zap.Error(err))
	} else {
		logger.Info("HTTP server gracefully stopped")
	}
}

// pollQueueDepth queries the RabbitMQ management API every 10s and updates the Prometheus gauge.
func pollQueueDepth(ctx context.Context, logger *zap.Logger, mgmtURL string) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	client := &http.Client{Timeout: 5 * time.Second}

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			depth, err := fetchQueueMessages(client, mgmtURL)
			if err != nil {
				logger.Warn("failed to fetch queue depth", zap.Error(err))
				continue
			}
			monitoring.SetCurrentQueueDepth(float64(depth))
		}
	}
}

func fetchQueueMessages(client *http.Client, mgmtURL string) (int, error) {
	url := fmt.Sprintf("%s/api/queues/%%2F/jobs.queue", mgmtURL)
	resp, err := client.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	var result struct {
		Messages int `json:"messages"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return 0, err
	}
	return result.Messages, nil
}

// getCurrentQueueDepth reads the current queue depth from the monitoring package.
func getCurrentQueueDepth() float64 {
	return monitoring.GetCurrentQueueDepth()
}
