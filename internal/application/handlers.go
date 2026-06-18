package application

import (
	"context"
	"log"
)

// JobHandler defines the signature for job execution handlers.
type JobHandler func(ctx context.Context, payload []byte) error

// JobHandlerRegistry maps job type strings to handler functions.
type JobHandlerRegistry map[string]JobHandler

// DefaultRegistry returns a pre-populated registry with standard handlers.
var DefaultRegistry = JobHandlerRegistry{
	"send_email": func(ctx context.Context, payload []byte) error {
		log.Printf("[Handler] send_email: processing payload: %s\n", string(payload))
		return nil
	},
}
