package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Address                string
	DatabaseURL            string
	APIKeyPepper           string
	IMDbDatasetBaseURL     string
	IMDbSyncInterval       time.Duration
	IMDbRunOnStartup       bool
	IMDbForceFullRefresh   bool
	IMDbDeltaBatchSize     int
	IMDbMaintenanceWorkMem string
	BulkJobPollInterval    time.Duration
	HTTPTimeout            time.Duration
}

func Load() (Config, error) {
	cfg := Config{
		Address:                envOrDefault("API_ADDRESS", ":8080"),
		DatabaseURL:            os.Getenv("DATABASE_URL"),
		APIKeyPepper:           os.Getenv("API_KEY_PEPPER"),
		IMDbDatasetBaseURL:     envOrDefault("IMDB_DATASET_BASE_URL", "https://datasets.imdbws.com"),
		IMDbSyncInterval:       envDurationHours("IMDB_SYNC_INTERVAL_HOURS", 12),
		IMDbRunOnStartup:       envBool("IMDB_RUN_ON_STARTUP", true),
		IMDbForceFullRefresh:   envBool("IMDB_FORCE_FULL_REFRESH", false),
		IMDbDeltaBatchSize:     envInt("IMDB_DELTA_BATCH_SIZE", 50000),
		IMDbMaintenanceWorkMem: envOrDefault("IMDB_MAINTENANCE_WORK_MEM", "1GB"),
		BulkJobPollInterval:    envDurationSeconds("BULK_JOB_POLL_INTERVAL_SECONDS", 15),
		HTTPTimeout:            envDurationMinutes("HTTP_TIMEOUT_MINUTES", 30),
	}

	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	if cfg.APIKeyPepper == "" {
		return Config{}, fmt.Errorf("API_KEY_PEPPER is required")
	}

	return cfg, nil
}

func envOrDefault(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	switch raw {
	case "1", "true", "TRUE", "yes", "YES":
		return true
	case "0", "false", "FALSE", "no", "NO":
		return false
	default:
		return fallback
	}
}

func envDurationHours(key string, fallback int) time.Duration {
	return time.Duration(envInt(key, fallback)) * time.Hour
}

func envDurationMinutes(key string, fallback int) time.Duration {
	return time.Duration(envInt(key, fallback)) * time.Minute
}

func envDurationSeconds(key string, fallback int) time.Duration {
	return time.Duration(envInt(key, fallback)) * time.Second
}

func envInt(key string, fallback int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return value
}
