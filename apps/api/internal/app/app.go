package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"nexio-imdb/apps/api/internal/api"
	"nexio-imdb/apps/api/internal/auth"
	"nexio-imdb/apps/api/internal/config"
	"nexio-imdb/apps/api/internal/imdb"
	"nexio-imdb/apps/api/internal/postgres"
)

type App struct {
	server *http.Server
	pool   *pgxpool.Pool
}

func New(ctx context.Context, cfg config.Config) (*App, error) {
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("open postgres pool: %w", err)
	}

	store := postgres.NewStore(pool)
	router := api.NewRouter(imdb.NewService(store), auth.NewService(store, cfg.APIKeyPepper))

	return &App{
		pool: pool,
		server: &http.Server{
			Addr:              cfg.Address,
			Handler:           router,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}, nil
}

func (a *App) Run() error {
	err := a.server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func (a *App) Shutdown(ctx context.Context) error {
	err := a.server.Shutdown(ctx)
	a.pool.Close()
	return err
}
