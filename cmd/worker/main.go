package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"distributed-task-queue/internal/application"
	"distributed-task-queue/internal/domain"
	"distributed-task-queue/internal/infrastructure/postgres"
	"distributed-task-queue/internal/infrastructure/rabbitmq"
	"distributed-task-queue/monitoring"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

func main() {
	// Initialize structured logger
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("failed to initialize zap logger: %v", err)
	}
	defer logger.Sync()

	logger.Info("Starting worker service...")

	// Create root cancelable context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize DB Connection Pool
	dbPool, err := postgres.NewConnectionPool(ctx)
	if err != nil {
		logger.Fatal("failed to initialize database connection pool", zap.Error(err))
	}
	defer dbPool.Close()
	logger.Info("Database connection pool established")

	// Initialize Repository
	repo := postgres.NewJobRepository(dbPool)

	// Initialize RabbitMQ Producer (for retries) and Consumer
	producer, err := rabbitmq.NewJobProducer()
	if err != nil {
		logger.Fatal("failed to initialize RabbitMQ producer", zap.Error(err))
	}
	defer producer.Close()
	logger.Info("RabbitMQ producer connected")

	consumer, err := rabbitmq.NewJobConsumer()
	if err != nil {
		logger.Fatal("failed to initialize RabbitMQ consumer", zap.Error(err))
	}
	defer consumer.Close()
	logger.Info("RabbitMQ consumer connected")

	// Initialize Job Service
	maxAttempts := 3
	if val := os.Getenv("MAX_ATTEMPTS"); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			maxAttempts = n
		}
	}
	jobService := application.NewJobService(repo, producer, application.DefaultRegistry, maxAttempts)

	// Worker Self-Registration (PART 2)
	workerID := uuid.New().String()
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}

	logger.Info("Registering worker", zap.String("worker_id", workerID), zap.String("hostname", hostname))
	if err := repo.RegisterWorker(ctx, workerID, hostname); err != nil {
		logger.Error("failed to register worker", zap.Error(err))
	} else {
		monitoring.WorkerHeartbeatTimestamp.WithLabelValues(workerID).Set(float64(time.Now().Unix()))
	}

	// Defer self-unregistration on exit
	defer func() {
		logger.Info("Unregistering worker", zap.String("worker_id", workerID))
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if err := repo.UnregisterWorker(cleanupCtx, workerID); err != nil {
			logger.Error("failed to unregister worker", zap.Error(err))
		}
	}()

	// 30s Heartbeat updater goroutine (PART 2)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := repo.UpdateWorkerHeartbeat(ctx, workerID); err != nil {
					logger.Error("failed to update heartbeat", zap.Error(err))
				} else {
					monitoring.WorkerHeartbeatTimestamp.WithLabelValues(workerID).Set(float64(time.Now().Unix()))
				}
			}
		}
	}()

	// 60s Zombie recovery goroutine (PART 2)
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				recovered, err := repo.RecoverZombies(ctx)
				if err != nil {
					logger.Error("failed to recover zombie jobs", zap.Error(err))
					continue
				}
				if len(recovered) > 0 {
					logger.Info("Recovered zombie jobs", zap.Int("count", len(recovered)))
					for _, job := range recovered {
						_ = repo.RecordEvent(ctx, job.ID, "job_recovered", nil)
						if err := producer.PublishJob(ctx, job); err != nil {
							logger.Error("failed to republish recovered job", zap.String("job_id", job.ID), zap.Error(err))
						} else {
							monitoring.JobsInQueue.Inc()
						}
					}
				}
			}
		}
	}()

	// Determine worker count
	workerCount := 5
	if val := os.Getenv("WORKER_COUNT"); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			workerCount = n
		}
	}
	logger.Info("Starting worker pool", zap.Int("worker_count", workerCount))

	// Start consuming
	jobChan, err := consumer.Consume(ctx, "worker-pool")
	if err != nil {
		logger.Fatal("failed to start consuming from RabbitMQ", zap.Error(err))
	}

	var wg sync.WaitGroup

	// Spawn workers
	for i := 1; i <= workerCount; i++ {
		wg.Add(1)
		go func(wID int) {
			defer wg.Done()
			wLogger := logger.With(zap.Int("pool_worker_id", wID))
			wLogger.Info("Worker started")

			for {
				select {
				case <-ctx.Done():
					wLogger.Info("Worker stopping (context cancelled)")
					return
				case rjob, ok := <-jobChan:
					if !ok {
						wLogger.Info("Worker stopping (channel closed)")
						return
					}

					// Decrement queue gauge: job has left the queue
					monitoring.JobsInQueue.Dec()

					// Atomically lock and claim a pending job from the database (PART 3)
					claimedJob, err := repo.ClaimJob(ctx)
					if err != nil {
						wLogger.Error("failed to claim job from database", zap.Error(err))
						_ = rjob.Nack(true) // requeue RabbitMQ trigger message
						continue
					}

					if claimedJob == nil {
						// No pending jobs to claim (likely claimed by another instance), ack and continue
						_ = rjob.Ack()
						continue
					}

					jobType := claimedJob.Type
					wLogger.Info("Started job processing",
						zap.String("job_id", claimedJob.ID),
						zap.String("job_type", jobType),
						zap.Int("attempt", claimedJob.Attempts+1),
					)

					startTime := time.Now()
					err = jobService.ProcessJob(ctx, claimedJob)
					duration := time.Since(startTime)

					// Record processing duration
					monitoring.JobProcessingDuration.WithLabelValues(jobType).Observe(duration.Seconds())

					if err == nil {
						monitoring.JobsProcessedTotal.WithLabelValues(jobType, "completed").Inc()
						wLogger.Info("Job completed successfully",
							zap.String("job_id", claimedJob.ID),
							zap.String("job_type", jobType),
							zap.Duration("duration", duration),
						)
						if ackErr := rjob.Ack(); ackErr != nil {
							wLogger.Error("failed to ack job message", zap.String("job_id", claimedJob.ID), zap.Error(ackErr))
						}
					} else {
						monitoring.JobsProcessedTotal.WithLabelValues(jobType, "failed").Inc()
						wLogger.Error("Job processing failed",
							zap.String("job_id", claimedJob.ID),
							zap.String("job_type", jobType),
							zap.Error(err),
							zap.Duration("duration", duration),
						)

						if claimedJob.Status == domain.Dead {
							wLogger.Warn("Job reached max attempts and is dead-lettered",
								zap.String("job_id", claimedJob.ID),
								zap.Int("attempts", claimedJob.Attempts),
							)
							if nackErr := rjob.Nack(false); nackErr != nil {
								wLogger.Error("failed to nack (dead-letter) job message", zap.String("job_id", claimedJob.ID), zap.Error(nackErr))
							}
						} else {
							wLogger.Info("Job rescheduled for retry",
								zap.String("job_id", claimedJob.ID),
								zap.Int("next_attempt", claimedJob.Attempts+1),
							)
							if ackErr := rjob.Ack(); ackErr != nil {
								wLogger.Error("failed to ack old message for retried job", zap.String("job_id", claimedJob.ID), zap.Error(ackErr))
							}
						}
					}
				}
			}
		}(i)
	}

	// Graceful shutdown signal channel
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// Wait for shutdown signal
	sig := <-sigChan
	logger.Info("Shutdown signal received, shutting down gracefully...", zap.String("signal", sig.String()))

	// Cancel context to notify workers to stop
	cancel()

	// Wait for all workers to finish processing current jobs
	workersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(workersDone)
	}()

	select {
	case <-workersDone:
		logger.Info("All workers finished. Exiting.")
	case <-time.After(15 * time.Second):
		logger.Warn("Graceful shutdown timed out, forcing exit.")
	}
}
