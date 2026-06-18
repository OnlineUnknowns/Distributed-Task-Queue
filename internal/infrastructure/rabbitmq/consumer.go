package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"distributed-task-queue/internal/domain"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

type JobConsumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

type ReceivedJob struct {
	Job     *domain.Job
	Ack     func() error
	Nack    func(requeue bool) error
	Context context.Context
}

func NewJobConsumer() (*JobConsumer, error) {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		return nil, fmt.Errorf("RABBITMQ_URL environment variable is not set")
	}

	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open a channel: %w", err)
	}

	// Setup exchanges and queues
	if err := SetupRabbitMQ(ch); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to setup rabbitmq queues: %w", err)
	}

	return &JobConsumer{
		conn:    conn,
		channel: ch,
	}, nil
}

func traceContextFromHeaders(headers amqp.Table) context.Context {
	carrier := propagation.MapCarrier{}
	for key, value := range headers {
		if str, ok := value.(string); ok {
			carrier[key] = str
		}
	}
	return otel.GetTextMapPropagator().Extract(context.Background(), carrier)
}

func (c *JobConsumer) Consume(ctx context.Context, consumerName string) (<-chan ReceivedJob, error) {
	msgsNormal, err := c.channel.ConsumeWithContext(
		ctx,
		JobsQueue,             // queue
		consumerName+"-normal", // consumer name
		false,                 // auto-ack
		false,                 // exclusive
		false,                 // no-local
		false,                 // no-wait
		nil,                   // args
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start normal consuming: %w", err)
	}

	msgsPriority, err := c.channel.ConsumeWithContext(
		ctx,
		PriorityQueue,           // queue
		consumerName+"-priority", // consumer name
		false,                   // auto-ack
		false,                   // exclusive
		false,                   // no-local
		false,                   // no-wait
		nil,                     // args
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start priority consuming: %w", err)
	}

	jobChan := make(chan ReceivedJob)

	forward := func(msgs <-chan amqp.Delivery) {
		for d := range msgs {
			var job domain.Job
			if err := json.Unmarshal(d.Body, &job); err != nil {
				// Reject message without requeueing (sends to Dead Letter directly)
				_ = d.Nack(false, false)
				continue
			}

			ctxWithTrace := traceContextFromHeaders(d.Headers)

			ackFunc := func() error {
				return d.Ack(false)
			}
			nackFunc := func(requeue bool) error {
				return d.Nack(false, requeue)
			}

			select {
			case jobChan <- ReceivedJob{Job: &job, Ack: ackFunc, Nack: nackFunc, Context: ctxWithTrace}:
			case <-ctx.Done():
				return
			}
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		forward(msgsNormal)
	}()
	go func() {
		defer wg.Done()
		forward(msgsPriority)
	}()

	go func() {
		wg.Wait()
		close(jobChan)
	}()

	return jobChan, nil
}

func (c *JobConsumer) Close() error {
	var err error
	if c.channel != nil {
		err = c.channel.Close()
	}
	if c.conn != nil {
		if connErr := c.conn.Close(); connErr != nil {
			err = connErr
		}
	}
	return err
}
