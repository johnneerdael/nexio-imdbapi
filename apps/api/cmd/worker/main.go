package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"nexio-imdb/apps/api/internal/config"
	"nexio-imdb/apps/api/internal/worker"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	service := worker.New(pool, cfg, log.Default())
	if err := service.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
