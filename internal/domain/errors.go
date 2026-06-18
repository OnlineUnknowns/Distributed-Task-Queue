package domain

import "errors"

var (
	ErrJobNotFound        = errors.New("job not found")
	ErrMaxAttemptsReached = errors.New("maximum job attempts reached")
)
