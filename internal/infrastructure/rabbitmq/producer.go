package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"distributed-task-queue/internal/domain"
	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

type JobProducer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func NewJobProducer() (*JobProducer, error) {
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

	return &JobProducer{
		conn:    conn,
		channel: ch,
	}, nil
}

func traceHeadersFromContext(ctx context.Context) amqp.Table {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	headers := amqp.Table{}
	for k, v := range carrier {
		headers[k] = v
	}
	return headers
}

func (p *JobProducer) PublishJob(ctx context.Context, job *domain.Job) error {
	body, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	routingKey := JobsQueue
	if job.Priority >= 7 {
		routingKey = PriorityQueue
	}

	err = p.channel.PublishWithContext(
		ctx,
		"",         // exchange
		routingKey, // routing key / queue name
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
			Headers:     traceHeadersFromContext(ctx),
		},
	)
	if err != nil {
		return fmt.Errorf("failed to publish job to rabbitmq: %w", err)
	}

	return nil
}

func (p *JobProducer) Close() error {
	var err error
	if p.channel != nil {
		err = p.channel.Close()
	}
	if p.conn != nil {
		if connErr := p.conn.Close(); connErr != nil {
			err = connErr
		}
	}
	return err
}
