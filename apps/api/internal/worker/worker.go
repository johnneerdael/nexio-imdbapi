package worker

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"nexio-imdb/apps/api/internal/bulk"
	"nexio-imdb/apps/api/internal/config"
	"nexio-imdb/apps/api/internal/imdb"
	"nexio-imdb/apps/api/internal/ingest"
	"nexio-imdb/apps/api/internal/postgres"
)

type Service struct {
	store    *postgres.Store
	queries  imdb.QueryService
	ingester *ingest.Runner
	cfg      config.Config
	logger   *log.Logger
}

func New(pool *pgxpool.Pool, cfg config.Config, logger *log.Logger) *Service {
	if logger == nil {
		logger = log.Default()
	}
	store := postgres.NewStore(pool)
	return &Service{
		store:   store,
		queries: imdb.NewService(store),
		ingester: ingest.NewRunner(pool, &http.Client{
			Timeout: cfg.HTTPTimeout,
		}, cfg.IMDbDatasetBaseURL, logger, cfg.IMDbForceFullRefresh, cfg.IMDbDeltaBatchSize, cfg.IMDbMaintenanceWorkMem),
		cfg:    cfg,
		logger: logger,
	}
}

func (s *Service) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		s.runSyncLoop(ctx)
	}()

	go func() {
		defer wg.Done()
		s.runJobLoop(ctx)
	}()

	<-ctx.Done()
	wg.Wait()
	return nil
}

func (s *Service) processQueuedJobs(ctx context.Context) error {
	for {
		job, ok, err := s.store.ClaimNextBulkJob(ctx)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}

		result, err := bulk.Execute(ctx, s.queries, job.Operation, job.Payload)
		if err != nil {
			failErr := s.store.FailBulkJob(ctx, job.ID, truncateErr(err))
			if failErr != nil {
				return fmt.Errorf("process bulk job %s: %w (plus fail update error: %v)", job.ID, err, failErr)
			}
			continue
		}

		if err := s.store.CompleteBulkJob(ctx, job.ID, result); err != nil {
			return fmt.Errorf("complete bulk job %s: %w", job.ID, err)
		}
	}
}

func truncateErr(err error) string {
	if err == nil {
		return ""
	}
	value := err.Error()
	if len(value) <= 2000 {
		return value
	}
	return value[:2000]
}

func (s *Service) runSyncLoop(ctx context.Context) {
	if s.cfg.IMDbRunOnStartup {
		s.executeSync(ctx, "initial")
	}

	ticker := time.NewTicker(s.cfg.IMDbSyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.executeSync(ctx, "scheduled")
		}
	}
}

func (s *Service) runJobLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.BulkJobPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.processQueuedJobs(ctx); err != nil {
				s.logger.Printf("bulk job processing failed: %v", err)
			}
		}
	}
}

func (s *Service) executeSync(ctx context.Context, label string) {
	result, err := s.ingester.SyncOnce(ctx)
	if err != nil {
		s.logger.Printf("%s imdb sync failed: %v", label, err)
		return
	}
	if result.Imported {
		s.logger.Printf("%s imdb sync imported snapshot %d (%s)", label, result.SnapshotID, result.DatasetVersion)
		return
	}
	s.logger.Printf("%s imdb sync skipped: upstream unchanged", label)
}
