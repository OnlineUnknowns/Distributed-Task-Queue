package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

type DBConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Name     string
}

type RabbitMQConfig struct {
	URL string
}

type WorkerConfig struct {
	Count       int
	MaxAttempts int
}

type APIConfig struct {
	Port string
}

type Config struct {
	DB       DBConfig
	RabbitMQ RabbitMQConfig
	Worker   WorkerConfig
	API      APIConfig
}

// Load reads config from environment variables and validates all required fields.
func Load() (*Config, error) {
	var missing []string

	dbHost := os.Getenv("DB_HOST")
	if dbHost == "" {
		missing = append(missing, "DB_HOST")
	}
	dbPort := os.Getenv("DB_PORT")
	if dbPort == "" {
		missing = append(missing, "DB_PORT")
	}
	dbUser := os.Getenv("DB_USER")
	if dbUser == "" {
		missing = append(missing, "DB_USER")
	}
	dbPassword := os.Getenv("DB_PASSWORD")
	if dbPassword == "" {
		missing = append(missing, "DB_PASSWORD")
	}
	dbName := os.Getenv("DB_NAME")
	if dbName == "" {
		missing = append(missing, "DB_NAME")
	}

	rabbitmqURL := os.Getenv("RABBITMQ_URL")
	if rabbitmqURL == "" {
		missing = append(missing, "RABBITMQ_URL")
	}

	apiPort := os.Getenv("API_PORT")
	if apiPort == "" {
		apiPort = "8080"
	}

	workerCountStr := os.Getenv("WORKER_COUNT")
	workerCount := 5
	if workerCountStr != "" {
		if val, err := strconv.Atoi(workerCountStr); err == nil && val > 0 {
			workerCount = val
		}
	}

	maxAttemptsStr := os.Getenv("MAX_ATTEMPTS")
	maxAttempts := 3
	if maxAttemptsStr != "" {
		if val, err := strconv.Atoi(maxAttemptsStr); err == nil && val > 0 {
			maxAttempts = val
		}
	}

	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}

	return &Config{
		DB: DBConfig{
			Host:     dbHost,
			Port:     dbPort,
			User:     dbUser,
			Password: dbPassword,
			Name:     dbName,
		},
		RabbitMQ: RabbitMQConfig{
			URL: rabbitmqURL,
		},
		API: APIConfig{
			Port: apiPort,
		},
		Worker: WorkerConfig{
			Count:       workerCount,
			MaxAttempts: maxAttempts,
		},
	}, nil
}
