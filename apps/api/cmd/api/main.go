package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"
	"time"

	"nexio-imdb/apps/api/internal/app"
	"nexio-imdb/apps/api/internal/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	application, err := app.New(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = application.Shutdown(shutdownCtx)
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = application.Shutdown(shutdownCtx)
	}()

	log.Printf("api listening on %s", cfg.Address)
	if err := application.Run(); err != nil {
		log.Fatal(err)
	}
}
