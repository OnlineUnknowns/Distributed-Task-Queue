package rabbitmq

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	JobsQueue       = "jobs.queue"
	PriorityQueue   = "jobs.priority.queue"
	DeadLetterQueue = "dead_letter.queue"
	DLXExchange     = "dlx"
	DLXRoutingKey   = "dead_letter.queue"
)

// SetupRabbitMQ declares the exchanges, queues, and bindings necessary for jobs and dead-lettering.
func SetupRabbitMQ(ch *amqp.Channel) error {
	// 1. Declare dead-letter exchange (DLX)
	err := ch.ExchangeDeclare(
		DLXExchange, // name
		"direct",    // type
		true,        // durable
		false,       // auto-deleted
		false,       // internal
		false,       // no-wait
		nil,         // arguments
	)
	if err != nil {
		return err
	}

	// 2. Declare dead-letter queue (DLQ)
	_, err = ch.QueueDeclare(
		DeadLetterQueue, // name
		true,            // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		nil,             // arguments
	)
	if err != nil {
		return err
	}

	// 3. Bind DLQ to DLX
	err = ch.QueueBind(
		DeadLetterQueue, // queue name
		DLXRoutingKey,   // routing key
		DLXExchange,     // exchange
		false,           // no-wait
		nil,
	)
	if err != nil {
		return err
	}

	// 4. Declare main queues with DLX configuration
	args := amqp.Table{
		"x-dead-letter-exchange":    DLXExchange,
		"x-dead-letter-routing-key": DLXRoutingKey,
	}
	_, err = ch.QueueDeclare(
		JobsQueue, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		args,      // arguments specifying DLX
	)
	if err != nil {
		return err
	}

	_, err = ch.QueueDeclare(
		PriorityQueue, // name
		true,          // durable
		false,         // delete when unused
		false,         // exclusive
		false,         // no-wait
		args,          // arguments specifying DLX
	)
	if err != nil {
		return err
	}

	return nil
}
