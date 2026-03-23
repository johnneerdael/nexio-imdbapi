package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"nexio-imdb/apps/api/internal/api"
	"nexio-imdb/apps/api/internal/auth"
	"nexio-imdb/apps/api/internal/config"
	"nexio-imdb/apps/api/internal/imdb"
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

	repo := imdb.NewPostgresRepository(pool)
	service := imdb.NewService(repo)
	authenticator := auth.NewService(auth.NewPostgresKeyStore(pool), cfg.APIKeyPepper)

	server := &http.Server{
		Addr:              cfg.Address,
		Handler:           api.NewRouter(service, authenticator),
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	log.Printf("api listening on %s", cfg.Address)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
