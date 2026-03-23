package config

import (
	"fmt"
	"os"
)

type Config struct {
	Address      string
	DatabaseURL  string
	APIKeyPepper string
}

func Load() (Config, error) {
	cfg := Config{
		Address:      envOrDefault("API_ADDRESS", ":8080"),
		DatabaseURL:  os.Getenv("DATABASE_URL"),
		APIKeyPepper: os.Getenv("API_KEY_PEPPER"),
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
