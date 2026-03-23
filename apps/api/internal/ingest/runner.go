package ingest

import (
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Runner struct {
	pool     *pgxpool.Pool
	client   *http.Client
	baseURL  string
	datasets []datasetSpec
}

type Result struct {
	Imported       bool
	SnapshotID     int64
	DatasetVersion string
}

type datasetSpec struct {
	Name      string
	Filename  string
	TempTable string
	CreateSQL string
	CopySQL   string
}

type remoteDataset struct {
	spec            datasetSpec
	url             string
	etag            string
	lastModified    string
	sourceUpdatedAt *time.Time
}

func NewRunner(pool *pgxpool.Pool, client *http.Client, baseURL string) *Runner {
	baseURL = strings.TrimRight(baseURL, "/")
	return &Runner{
		pool:    pool,
		client:  client,
		baseURL: baseURL,
		datasets: []datasetSpec{
			{
				Name:      "title.basics.tsv.gz",
				Filename:  "title.basics.tsv.gz",
				TempTable: "staging_title_basics_raw",
				CreateSQL: `CREATE TEMP TABLE staging_title_basics_raw (tconst TEXT, title_type TEXT, primary_title TEXT, original_title TEXT, is_adult TEXT, start_year TEXT, end_year TEXT, runtime_minutes TEXT, genres TEXT) ON COMMIT DROP`,
				CopySQL:   `COPY staging_title_basics_raw (tconst, title_type, primary_title, original_title, is_adult, start_year, end_year, runtime_minutes, genres) FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', NULL '\N', HEADER true)`,
			},
			{
				Name:      "title.ratings.tsv.gz",
				Filename:  "title.ratings.tsv.gz",
				TempTable: "staging_title_ratings_raw",
				CreateSQL: `CREATE TEMP TABLE staging_title_ratings_raw (tconst TEXT, average_rating TEXT, num_votes TEXT) ON COMMIT DROP`,
				CopySQL:   `COPY staging_title_ratings_raw (tconst, average_rating, num_votes) FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', NULL '\N', HEADER true)`,
			},
			{
				Name:      "title.episode.tsv.gz",
				Filename:  "title.episode.tsv.gz",
				TempTable: "staging_title_episode_raw",
				CreateSQL: `CREATE TEMP TABLE staging_title_episode_raw (tconst TEXT, parent_tconst TEXT, season_number TEXT, episode_number TEXT) ON COMMIT DROP`,
				CopySQL:   `COPY staging_title_episode_raw (tconst, parent_tconst, season_number, episode_number) FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', NULL '\N', HEADER true)`,
			},
			{
				Name:      "title.crew.tsv.gz",
				Filename:  "title.crew.tsv.gz",
				TempTable: "staging_title_crew_raw",
				CreateSQL: `CREATE TEMP TABLE staging_title_crew_raw (tconst TEXT, directors TEXT, writers TEXT) ON COMMIT DROP`,
				CopySQL:   `COPY staging_title_crew_raw (tconst, directors, writers) FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', NULL '\N', HEADER true)`,
			},
			{
				Name:      "title.principals.tsv.gz",
				Filename:  "title.principals.tsv.gz",
				TempTable: "staging_title_principals_raw",
				CreateSQL: `CREATE TEMP TABLE staging_title_principals_raw (tconst TEXT, ordering TEXT, nconst TEXT, category TEXT, job TEXT, characters TEXT) ON COMMIT DROP`,
				CopySQL:   `COPY staging_title_principals_raw (tconst, ordering, nconst, category, job, characters) FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', NULL '\N', HEADER true)`,
			},
			{
				Name:      "name.basics.tsv.gz",
				Filename:  "name.basics.tsv.gz",
				TempTable: "staging_name_basics_raw",
				CreateSQL: `CREATE TEMP TABLE staging_name_basics_raw (nconst TEXT, primary_name TEXT, birth_year TEXT, death_year TEXT, primary_professions TEXT, known_for_titles TEXT) ON COMMIT DROP`,
				CopySQL:   `COPY staging_name_basics_raw (nconst, primary_name, birth_year, death_year, primary_professions, known_for_titles) FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', NULL '\N', HEADER true)`,
			},
			{
				Name:      "title.akas.tsv.gz",
				Filename:  "title.akas.tsv.gz",
				TempTable: "staging_title_akas_raw",
				CreateSQL: `CREATE TEMP TABLE staging_title_akas_raw (title_id TEXT, ordering TEXT, title TEXT, region TEXT, language TEXT, types TEXT, attributes TEXT, is_original_title TEXT) ON COMMIT DROP`,
				CopySQL:   `COPY staging_title_akas_raw (title_id, ordering, title, region, language, types, attributes, is_original_title) FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', NULL '\N', HEADER true)`,
			},
		},
	}
}

func (r *Runner) SyncOnce(ctx context.Context) (Result, error) {
	remote, err := r.fetchRemoteMetadata(ctx)
	if err != nil {
		return Result{}, err
	}

	changed, err := r.hasChanges(ctx, remote)
	if err != nil {
		return Result{}, err
	}
	if !changed {
		if err := r.upsertSyncState(ctx, remote, nil); err != nil {
			return Result{}, err
		}
		return Result{Imported: false}, nil
	}

	sourceUpdatedAt := latestSourceUpdatedAt(remote)
	datasetVersion := datasetVersion(sourceUpdatedAt)
	snapshotID, err := r.createSnapshot(ctx, sourceUpdatedAt, remote, datasetVersion)
	if err != nil {
		return Result{}, err
	}

	if err := r.importSnapshot(ctx, snapshotID, remote, sourceUpdatedAt, datasetVersion); err != nil {
		_ = r.markSnapshotFailed(ctx, snapshotID, err)
		return Result{}, err
	}

	return Result{
		Imported:       true,
		SnapshotID:     snapshotID,
		DatasetVersion: datasetVersion,
	}, nil
}

func (r *Runner) fetchRemoteMetadata(ctx context.Context) ([]remoteDataset, error) {
	items := make([]remoteDataset, 0, len(r.datasets))
	for _, spec := range r.datasets {
		url := r.baseURL + "/" + spec.Filename
		req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
		if err != nil {
			return nil, fmt.Errorf("build HEAD request for %s: %w", spec.Name, err)
		}
		resp, err := r.client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("fetch metadata for %s: %w", spec.Name, err)
		}
		resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("metadata request for %s returned %s", spec.Name, resp.Status)
		}

		var parsed *time.Time
		if raw := strings.TrimSpace(resp.Header.Get("Last-Modified")); raw != "" {
			if ts, err := http.ParseTime(raw); err == nil {
				value := ts.UTC()
				parsed = &value
			}
		}

		items = append(items, remoteDataset{
			spec:            spec,
			url:             url,
			etag:            strings.TrimSpace(resp.Header.Get("ETag")),
			lastModified:    strings.TrimSpace(resp.Header.Get("Last-Modified")),
			sourceUpdatedAt: parsed,
		})
	}
	return items, nil
}

func (r *Runner) hasChanges(ctx context.Context, remote []remoteDataset) (bool, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT dataset_name, COALESCE(etag, ''), COALESCE(last_modified, '')
		FROM dataset_sync_state
	`)
	if err != nil {
		return false, fmt.Errorf("query dataset sync state: %w", err)
	}
	defer rows.Close()

	existing := map[string][2]string{}
	for rows.Next() {
		var name, etag, lastModified string
		if err := rows.Scan(&name, &etag, &lastModified); err != nil {
			return false, fmt.Errorf("scan dataset sync state: %w", err)
		}
		existing[name] = [2]string{etag, lastModified}
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("iterate dataset sync state: %w", err)
	}

	for _, item := range remote {
		previous, ok := existing[item.spec.Name]
		if !ok {
			return true, nil
		}
		if previous[0] != item.etag || previous[1] != item.lastModified {
			return true, nil
		}
	}
	return false, nil
}

func (r *Runner) createSnapshot(ctx context.Context, sourceUpdatedAt *time.Time, remote []remoteDataset, datasetVersion string) (int64, error) {
	sourceURL := r.baseURL + "/"
	sourceETag := joinRemoteValues(remote, func(item remoteDataset) string { return item.etag })

	var snapshotID int64
	err := r.pool.QueryRow(ctx, `
		INSERT INTO imdb_snapshots (
			dataset_name,
			status,
			dataset_version,
			source_url,
			source_updated_at,
			source_etag,
			notes,
			imported_at,
			is_active
		)
		VALUES ('imdbws', 'importing', $1, $2, $3, $4, '', NOW(), FALSE)
		RETURNING id
	`, datasetVersion, sourceURL, sourceUpdatedAt, sourceETag).Scan(&snapshotID)
	if err != nil {
		return 0, fmt.Errorf("create snapshot row: %w", err)
	}

	return snapshotID, nil
}

func (r *Runner) importSnapshot(ctx context.Context, snapshotID int64, remote []remoteDataset, sourceUpdatedAt *time.Time, datasetVersion string) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin import tx: %w", err)
	}
	defer tx.Rollback(ctx)

	for _, item := range remote {
		if _, err := tx.Exec(ctx, item.spec.CreateSQL); err != nil {
			return fmt.Errorf("create temp table for %s: %w", item.spec.Name, err)
		}
		if err := r.copyDataset(ctx, tx, item); err != nil {
			return err
		}
	}

	if err := r.normalizeSnapshot(ctx, tx, snapshotID, sourceUpdatedAt, datasetVersion, remote); err != nil {
		return err
	}

	if err := upsertSyncStateWithExecutor(ctx, tx, remote, &snapshotID); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit import tx: %w", err)
	}
	return nil
}

func (r *Runner) copyDataset(ctx context.Context, tx pgx.Tx, item remoteDataset) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, item.url, nil)
	if err != nil {
		return fmt.Errorf("build GET request for %s: %w", item.spec.Name, err)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("download %s: %w", item.spec.Name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("download %s returned %s", item.spec.Name, resp.Status)
	}

	reader, err := gzip.NewReader(resp.Body)
	if err != nil {
		return fmt.Errorf("open gzip for %s: %w", item.spec.Name, err)
	}
	defer reader.Close()

	if _, err := tx.Conn().PgConn().CopyFrom(ctx, reader, item.spec.CopySQL); err != nil {
		return fmt.Errorf("copy %s into %s: %w", item.spec.Name, item.spec.TempTable, err)
	}
	return nil
}

func (r *Runner) normalizeSnapshot(ctx context.Context, tx pgx.Tx, snapshotID int64, sourceUpdatedAt *time.Time, datasetVersion string, remote []remoteDataset) error {
	statements := []string{
		`TRUNCATE TABLE title_principals, title_crew_members, title_akas, title_episodes, title_ratings, names, titles RESTART IDENTITY`,
		fmt.Sprintf(`
			INSERT INTO titles (tconst, snapshot_id, title_type, primary_title, original_title, is_adult, start_year, end_year, runtime_minutes, genres, created_at, updated_at)
			SELECT
				tconst,
				%d,
				title_type,
				primary_title,
				COALESCE(original_title, primary_title),
				COALESCE(is_adult = '1', FALSE),
				NULLIF(start_year, '')::INTEGER,
				NULLIF(end_year, '')::INTEGER,
				NULLIF(runtime_minutes, '')::INTEGER,
				CASE
					WHEN genres IS NULL OR genres = '' THEN ARRAY[]::TEXT[]
					ELSE string_to_array(genres, ',')
				END,
				NOW(),
				NOW()
			FROM staging_title_basics_raw
		`, snapshotID),
		fmt.Sprintf(`
			INSERT INTO names (nconst, snapshot_id, primary_name, birth_year, death_year, primary_professions, known_for_titles, created_at, updated_at)
			SELECT
				nconst,
				%d,
				primary_name,
				NULLIF(birth_year, '')::INTEGER,
				NULLIF(death_year, '')::INTEGER,
				CASE
					WHEN primary_professions IS NULL OR primary_professions = '' THEN ARRAY[]::TEXT[]
					ELSE string_to_array(primary_professions, ',')
				END,
				CASE
					WHEN known_for_titles IS NULL OR known_for_titles = '' THEN ARRAY[]::TEXT[]
					ELSE string_to_array(known_for_titles, ',')
				END,
				NOW(),
				NOW()
			FROM staging_name_basics_raw
		`, snapshotID),
		`
			INSERT INTO title_ratings (tconst, average_rating, num_votes, updated_at)
			SELECT tconst, average_rating::NUMERIC(3,1), COALESCE(NULLIF(num_votes, ''), '0')::INTEGER, NOW()
			FROM staging_title_ratings_raw
			WHERE tconst IN (SELECT tconst FROM titles)
		`,
		`
			INSERT INTO title_episodes (tconst, parent_tconst, season_number, episode_number, created_at)
			SELECT
				e.tconst,
				e.parent_tconst,
				NULLIF(e.season_number, '')::INTEGER,
				NULLIF(e.episode_number, '')::INTEGER,
				NOW()
			FROM staging_title_episode_raw e
			JOIN titles child_title ON child_title.tconst = e.tconst
			JOIN titles parent_title ON parent_title.tconst = e.parent_tconst
		`,
		`
			INSERT INTO title_principals (tconst, ordering, nconst, category, job, characters, created_at)
			SELECT
				p.tconst,
				NULLIF(p.ordering, '')::INTEGER,
				p.nconst,
				p.category,
				p.job,
				CASE
					WHEN p.characters IS NULL OR p.characters = '' THEN NULL
					ELSE p.characters::JSONB
				END,
				NOW()
			FROM staging_title_principals_raw p
			JOIN titles t ON t.tconst = p.tconst
			JOIN names n ON n.nconst = p.nconst
		`,
		`
			INSERT INTO title_crew_members (tconst, nconst, role, ordering, created_at)
			SELECT
				c.tconst,
				trim(member.nconst),
				'director',
				member.ord::INTEGER,
				NOW()
			FROM staging_title_crew_raw c
			JOIN titles t ON t.tconst = c.tconst
			CROSS JOIN LATERAL unnest(string_to_array(COALESCE(c.directors, ''), ',')) WITH ORDINALITY AS member(nconst, ord)
			JOIN names n ON n.nconst = trim(member.nconst)
			WHERE trim(member.nconst) <> ''
			ON CONFLICT DO NOTHING
		`,
		`
			INSERT INTO title_crew_members (tconst, nconst, role, ordering, created_at)
			SELECT
				c.tconst,
				trim(member.nconst),
				'writer',
				member.ord::INTEGER,
				NOW()
			FROM staging_title_crew_raw c
			JOIN titles t ON t.tconst = c.tconst
			CROSS JOIN LATERAL unnest(string_to_array(COALESCE(c.writers, ''), ',')) WITH ORDINALITY AS member(nconst, ord)
			JOIN names n ON n.nconst = trim(member.nconst)
			WHERE trim(member.nconst) <> ''
			ON CONFLICT DO NOTHING
		`,
		`
			INSERT INTO title_akas (tconst, ordering, title, region, language, types, attributes, is_original_title, created_at)
			SELECT
				a.title_id,
				NULLIF(a.ordering, '')::INTEGER,
				a.title,
				a.region,
				a.language,
				CASE
					WHEN a.types IS NULL OR a.types = '' THEN ARRAY[]::TEXT[]
					ELSE string_to_array(a.types, ',')
				END,
				CASE
					WHEN a.attributes IS NULL OR a.attributes = '' THEN ARRAY[]::TEXT[]
					ELSE string_to_array(a.attributes, ',')
				END,
				COALESCE(a.is_original_title = '1', FALSE),
				NOW()
			FROM staging_title_akas_raw a
			JOIN titles t ON t.tconst = a.title_id
		`,
		`
			UPDATE imdb_snapshots
			SET is_active = FALSE
			WHERE id <> $1
		`,
		`
			UPDATE imdb_snapshots
			SET
				status = 'ready',
				dataset_version = $2,
				source_updated_at = $3,
				source_etag = $4,
				completed_at = NOW(),
				is_active = TRUE,
				title_count = (SELECT COUNT(*) FROM titles),
				name_count = (SELECT COUNT(*) FROM names),
				rating_count = (SELECT COUNT(*) FROM title_ratings),
				notes = ''
			WHERE id = $1
		`,
	}

	for index, statement := range statements {
		switch index {
		case len(statements) - 2:
			if _, err := tx.Exec(ctx, statement, snapshotID); err != nil {
				return fmt.Errorf("deactivate previous snapshots: %w", err)
			}
		case len(statements) - 1:
			if _, err := tx.Exec(ctx, statement, snapshotID, datasetVersion, sourceUpdatedAt, joinRemoteValues(remote, func(item remoteDataset) string { return item.etag })); err != nil {
				return fmt.Errorf("finalize snapshot: %w", err)
			}
		default:
			if _, err := tx.Exec(ctx, statement); err != nil {
				return fmt.Errorf("normalize dataset statement %d: %w", index+1, err)
			}
		}
	}

	return nil
}

func (r *Runner) markSnapshotFailed(ctx context.Context, snapshotID int64, importErr error) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE imdb_snapshots
		SET status = 'failed', notes = $2, completed_at = NOW(), is_active = FALSE
		WHERE id = $1
	`, snapshotID, truncateText(importErr.Error(), 2000))
	if err != nil {
		return fmt.Errorf("mark snapshot failed: %w", err)
	}
	return nil
}

func (r *Runner) upsertSyncState(ctx context.Context, remote []remoteDataset, snapshotID *int64) error {
	return upsertSyncStateWithExecutor(ctx, r.pool, remote, snapshotID)
}

func upsertSyncStateWithExecutor(ctx context.Context, execer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}, remote []remoteDataset, snapshotID *int64) error {
	var snapshotValue any
	if snapshotID != nil {
		snapshotValue = *snapshotID
	}

	for _, item := range remote {
		_, err := execer.Exec(ctx, `
			INSERT INTO dataset_sync_state (dataset_name, source_url, etag, last_modified, checked_at, imported_at, snapshot_id)
			VALUES ($1, $2, $3, $4, NOW(), CASE WHEN $5::bigint IS NULL THEN NULL ELSE NOW() END, $5)
			ON CONFLICT (dataset_name)
			DO UPDATE SET
				source_url = EXCLUDED.source_url,
				etag = EXCLUDED.etag,
				last_modified = EXCLUDED.last_modified,
				checked_at = NOW(),
				imported_at = CASE WHEN EXCLUDED.snapshot_id IS NULL THEN dataset_sync_state.imported_at ELSE NOW() END,
				snapshot_id = EXCLUDED.snapshot_id
		`, item.spec.Name, item.url, item.etag, item.lastModified, snapshotValue)
		if err != nil {
			return fmt.Errorf("upsert dataset sync state for %s: %w", item.spec.Name, err)
		}
	}
	return nil
}

func latestSourceUpdatedAt(items []remoteDataset) *time.Time {
	var latest *time.Time
	for _, item := range items {
		if item.sourceUpdatedAt == nil {
			continue
		}
		if latest == nil || item.sourceUpdatedAt.After(*latest) {
			value := item.sourceUpdatedAt.UTC()
			latest = &value
		}
	}
	return latest
}

func datasetVersion(sourceUpdatedAt *time.Time) string {
	if sourceUpdatedAt != nil {
		return sourceUpdatedAt.UTC().Format(time.RFC3339)
	}
	return time.Now().UTC().Format(time.RFC3339)
}

func joinRemoteValues(items []remoteDataset, selector func(remoteDataset) string) string {
	parts := make([]string, 0, len(items))
	for _, item := range items {
		value := strings.TrimSpace(selector(item))
		if value == "" {
			continue
		}
		parts = append(parts, item.spec.Name+"="+value)
	}
	return strings.Join(parts, "; ")
}

func truncateText(value string, max int) string {
	if len(value) <= max {
		return value
	}
	return value[:max]
}
