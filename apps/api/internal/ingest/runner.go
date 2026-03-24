package ingest

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Runner struct {
	pool               *pgxpool.Pool
	client             *http.Client
	baseURL            string
	datasets           []datasetSpec
	logger             *log.Logger
	forceFullRefresh   bool
	deltaBatchSize     int
	maintenanceWorkMem string
}

type Result struct {
	Imported       bool
	SnapshotID     int64
	DatasetVersion string
}

type datasetSpec struct {
	Name        string
	Filename    string
	BaseTable   string
	Columns     int
	ColumnDefs  string
	CopyColumns string
}

type remoteDataset struct {
	spec            datasetSpec
	url             string
	etag            string
	lastModified    string
	sourceUpdatedAt *time.Time
}

type tableSet struct {
	Titles           string
	TitleRatings     string
	TitleEpisodes    string
	Names            string
	TitlePrincipals  string
	TitleCrewMembers string
	TitleAkas        string
}

type normalizeCounts struct {
	Titles       int64
	Names        int64
	Ratings      int64
	Episodes     int64
	Principals   int64
	CrewMembers  int64
	AlternateIDs int64
}

type snapshotCounts struct {
	Titles      int64
	Names       int64
	Ratings     int64
	Episodes    int64
	Principals  int64
	CrewMembers int64
	Akas        int64
}

type syncMode string

const (
	syncModeFullRefresh syncMode = "full_refresh"
	syncModeDelta       syncMode = "delta"
)

type indexStatement struct {
	name      string
	statement string
}

type ActiveSnapshotState struct {
	ID     int64
	Exists bool
	Counts snapshotCounts
}

func liveTables() tableSet {
	return tableSet{
		Titles:           "titles",
		TitleRatings:     "title_ratings",
		TitleEpisodes:    "title_episodes",
		Names:            "names",
		TitlePrincipals:  "title_principals",
		TitleCrewMembers: "title_crew_members",
		TitleAkas:        "title_akas",
	}
}

func shadowTables(snapshotID int64) tableSet {
	suffix := fmt.Sprintf("_shadow_%d", snapshotID)
	return tableSet{
		Titles:           "titles" + suffix,
		TitleRatings:     "title_ratings" + suffix,
		TitleEpisodes:    "title_episodes" + suffix,
		Names:            "names" + suffix,
		TitlePrincipals:  "title_principals" + suffix,
		TitleCrewMembers: "title_crew_members" + suffix,
		TitleAkas:        "title_akas" + suffix,
	}
}

func previousTables() tableSet {
	return tableSet{
		Titles:           "titles_previous",
		TitleRatings:     "title_ratings_previous",
		TitleEpisodes:    "title_episodes_previous",
		Names:            "names_previous",
		TitlePrincipals:  "title_principals_previous",
		TitleCrewMembers: "title_crew_members_previous",
		TitleAkas:        "title_akas_previous",
	}
}

func (t tableSet) all() []string {
	return []string{t.Titles, t.TitleRatings, t.TitleEpisodes, t.Names, t.TitlePrincipals, t.TitleCrewMembers, t.TitleAkas}
}

func NewRunner(pool *pgxpool.Pool, client *http.Client, baseURL string, logger *log.Logger, forceFullRefresh bool, deltaBatchSize int, maintenanceWorkMem string) *Runner {
	baseURL = strings.TrimRight(baseURL, "/")
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	if deltaBatchSize <= 0 {
		deltaBatchSize = 50000
	}
	if strings.TrimSpace(maintenanceWorkMem) == "" {
		maintenanceWorkMem = "1GB"
	}
	return &Runner{
		pool:               pool,
		client:             client,
		baseURL:            baseURL,
		logger:             logger,
		forceFullRefresh:   forceFullRefresh,
		deltaBatchSize:     deltaBatchSize,
		maintenanceWorkMem: maintenanceWorkMem,
		datasets: []datasetSpec{
			{
				Name:        "title.basics.tsv.gz",
				Filename:    "title.basics.tsv.gz",
				BaseTable:   "staging_title_basics_raw",
				Columns:     9,
				ColumnDefs:  "(tconst TEXT, title_type TEXT, primary_title TEXT, original_title TEXT, is_adult TEXT, start_year TEXT, end_year TEXT, runtime_minutes TEXT, genres TEXT)",
				CopyColumns: "(tconst, title_type, primary_title, original_title, is_adult, start_year, end_year, runtime_minutes, genres)",
			},
			{
				Name:        "name.basics.tsv.gz",
				Filename:    "name.basics.tsv.gz",
				BaseTable:   "staging_name_basics_raw",
				Columns:     6,
				ColumnDefs:  "(nconst TEXT, primary_name TEXT, birth_year TEXT, death_year TEXT, primary_professions TEXT, known_for_titles TEXT)",
				CopyColumns: "(nconst, primary_name, birth_year, death_year, primary_professions, known_for_titles)",
			},
			{
				Name:        "title.ratings.tsv.gz",
				Filename:    "title.ratings.tsv.gz",
				BaseTable:   "staging_title_ratings_raw",
				Columns:     3,
				ColumnDefs:  "(tconst TEXT, average_rating TEXT, num_votes TEXT)",
				CopyColumns: "(tconst, average_rating, num_votes)",
			},
			{
				Name:        "title.episode.tsv.gz",
				Filename:    "title.episode.tsv.gz",
				BaseTable:   "staging_title_episode_raw",
				Columns:     4,
				ColumnDefs:  "(tconst TEXT, parent_tconst TEXT, season_number TEXT, episode_number TEXT)",
				CopyColumns: "(tconst, parent_tconst, season_number, episode_number)",
			},
			{
				Name:        "title.principals.tsv.gz",
				Filename:    "title.principals.tsv.gz",
				BaseTable:   "staging_title_principals_raw",
				Columns:     6,
				ColumnDefs:  "(tconst TEXT, ordering TEXT, nconst TEXT, category TEXT, job TEXT, characters TEXT)",
				CopyColumns: "(tconst, ordering, nconst, category, job, characters)",
			},
			{
				Name:        "title.crew.tsv.gz",
				Filename:    "title.crew.tsv.gz",
				BaseTable:   "staging_title_crew_raw",
				Columns:     3,
				ColumnDefs:  "(tconst TEXT, directors TEXT, writers TEXT)",
				CopyColumns: "(tconst, directors, writers)",
			},
			{
				Name:        "title.akas.tsv.gz",
				Filename:    "title.akas.tsv.gz",
				BaseTable:   "staging_title_akas_raw",
				Columns:     8,
				ColumnDefs:  "(title_id TEXT, ordering TEXT, title TEXT, region TEXT, language TEXT, types TEXT, attributes TEXT, is_original_title TEXT)",
				CopyColumns: "(title_id, ordering, title, region, language, types, attributes, is_original_title)",
			},
		},
	}
}

func createRawTableStatement(tableName, columnDefs string) string {
	return fmt.Sprintf("CREATE UNLOGGED TABLE %s %s", tableName, columnDefs)
}

func createCopyStatement(tableName, copyColumns string) string {
	return fmt.Sprintf(`COPY %s %s FROM STDIN WITH (FORMAT csv, DELIMITER E'\t', NULL '\N')`, tableName, copyColumns)
}

func rawTableName(snapshotID int64, spec datasetSpec) string {
	return fmt.Sprintf("%s_%d", spec.BaseTable, snapshotID)
}

func selectSyncMode(hasActiveSnapshot, forceFullRefresh bool) syncMode {
	if forceFullRefresh || !hasActiveSnapshot {
		return syncModeFullRefresh
	}
	return syncModeDelta
}

func (r *Runner) logf(format string, args ...any) {
	if r.logger == nil {
		return
	}
	r.logger.Printf(format, args...)
}

func (r *Runner) datasetByName(name string) datasetSpec {
	for _, item := range r.datasets {
		if item.Name == name {
			return item
		}
	}
	return datasetSpec{}
}

func (r *Runner) SyncOnce(ctx context.Context) (Result, error) {
	r.logf("imdb sync checking upstream metadata for %d datasets", len(r.datasets))
	remote, err := r.fetchRemoteMetadata(ctx)
	if err != nil {
		return Result{}, err
	}

	changed, err := r.changedDatasets(ctx, remote)
	if err != nil {
		return Result{}, err
	}
	if len(changed) == 0 {
		if err := r.upsertSyncState(ctx, remote, nil); err != nil {
			return Result{}, err
		}
		r.logf("imdb sync metadata unchanged for all datasets")
		return Result{Imported: false}, nil
	}

	active, err := r.activeSnapshotState(ctx)
	if err != nil {
		return Result{}, err
	}
	mode := selectSyncMode(active.Exists, r.forceFullRefresh)

	sourceUpdatedAt := latestSourceUpdatedAt(remote)
	datasetVersion := datasetVersion(sourceUpdatedAt)
	r.logf("imdb sync changes detected, preparing snapshot for dataset version %s using %s", datasetVersion, mode)
	snapshotID, err := r.createSnapshot(ctx, mode, active.Counts, sourceUpdatedAt, remote, datasetVersion)
	if err != nil {
		return Result{}, err
	}

	if err := r.importSnapshot(ctx, snapshotID, mode, changed, remote, active.Counts, sourceUpdatedAt, datasetVersion); err != nil {
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

func (r *Runner) changedDatasets(ctx context.Context, remote []remoteDataset) ([]remoteDataset, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT dataset_name, COALESCE(etag, ''), COALESCE(last_modified, '')
		FROM dataset_sync_state
	`)
	if err != nil {
		return nil, fmt.Errorf("query dataset sync state: %w", err)
	}
	defer rows.Close()

	existing := map[string][2]string{}
	for rows.Next() {
		var name, etag, lastModified string
		if err := rows.Scan(&name, &etag, &lastModified); err != nil {
			return nil, fmt.Errorf("scan dataset sync state: %w", err)
		}
		existing[name] = [2]string{etag, lastModified}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dataset sync state: %w", err)
	}

	changed := make([]remoteDataset, 0, len(remote))
	for _, item := range remote {
		previous, ok := existing[item.spec.Name]
		if !ok {
			changed = append(changed, item)
			continue
		}
		if previous[0] != item.etag || previous[1] != item.lastModified {
			changed = append(changed, item)
		}
	}
	return changed, nil
}

func (r *Runner) activeSnapshotState(ctx context.Context) (ActiveSnapshotState, error) {
	var state ActiveSnapshotState
	err := r.pool.QueryRow(ctx, `
		SELECT
			id,
			title_count,
			name_count,
			rating_count,
			episode_count,
			principal_count,
			crew_member_count,
			aka_count
		FROM imdb_snapshots
		WHERE is_active = TRUE
		ORDER BY imported_at DESC, id DESC
		LIMIT 1
	`).Scan(
		&state.ID,
		&state.Counts.Titles,
		&state.Counts.Names,
		&state.Counts.Ratings,
		&state.Counts.Episodes,
		&state.Counts.Principals,
		&state.Counts.CrewMembers,
		&state.Counts.Akas,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return ActiveSnapshotState{}, nil
		}
		return ActiveSnapshotState{}, fmt.Errorf("load active snapshot state: %w", err)
	}
	state.Exists = true
	return state, nil
}

func (r *Runner) createSnapshot(ctx context.Context, mode syncMode, baseline snapshotCounts, sourceUpdatedAt *time.Time, remote []remoteDataset, datasetVersion string) (int64, error) {
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
			sync_mode,
			notes,
			imported_at,
			is_active,
			title_count,
			name_count,
			rating_count,
			episode_count,
			principal_count,
			crew_member_count,
			aka_count
		)
		VALUES ('imdbws', 'importing', $1, $2, $3, $4, $5, '', NOW(), FALSE, $6, $7, $8, $9, $10, $11, $12)
		RETURNING id
	`, datasetVersion, sourceURL, sourceUpdatedAt, sourceETag, mode, baseline.Titles, baseline.Names, baseline.Ratings, baseline.Episodes, baseline.Principals, baseline.CrewMembers, baseline.Akas).Scan(&snapshotID)
	if err != nil {
		return 0, fmt.Errorf("create snapshot row: %w", err)
	}

	r.logf("imdb sync created snapshot %d for version %s", snapshotID, datasetVersion)
	return snapshotID, nil
}

func (r *Runner) importSnapshot(ctx context.Context, snapshotID int64, mode syncMode, changed []remoteDataset, remote []remoteDataset, baseline snapshotCounts, sourceUpdatedAt *time.Time, datasetVersion string) error {
	if mode == syncModeDelta {
		return r.importDeltaSnapshot(ctx, snapshotID, changed, remote, baseline, sourceUpdatedAt, datasetVersion)
	}

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin import tx: %w", err)
	}
	defer tx.Rollback(ctx)

	shadow := shadowTables(snapshotID)
	r.logf("imdb sync snapshot %d import started", snapshotID)
	for _, item := range remote {
		stageTable := rawTableName(snapshotID, item.spec)
		r.logf("imdb sync preparing staging table %s for %s", stageTable, item.spec.Name)
		if _, err := tx.Exec(ctx, createRawTableStatement(stageTable, item.spec.ColumnDefs)); err != nil {
			return fmt.Errorf("create raw table for %s: %w", item.spec.Name, err)
		}
		if err := r.copyDataset(ctx, tx, item, stageTable); err != nil {
			return err
		}
	}

	if err := r.setLocalMaintenanceWorkMem(ctx, tx); err != nil {
		return err
	}

	if err := r.createShadowTables(ctx, tx, shadow); err != nil {
		return err
	}

	counts, err := r.normalizeSnapshot(ctx, tx, shadow, snapshotID)
	if err != nil {
		return err
	}
	rawTables := make([]string, 0, len(remote))
	for _, item := range remote {
		rawTables = append(rawTables, rawTableName(snapshotID, item.spec))
	}
	if err := r.dropTables(ctx, tx, rawTables...); err != nil {
		return err
	}

	if err := r.createSecondaryIndexes(ctx, tx, shadow); err != nil {
		return err
	}

	if err := r.promoteShadowTables(ctx, tx, snapshotID, shadow, counts, sourceUpdatedAt, datasetVersion, remote); err != nil {
		return err
	}
	r.logf("imdb sync snapshot %d sync state updated", snapshotID)

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit import tx: %w", err)
	}
	r.logf("imdb sync snapshot %d committed to live tables", snapshotID)
	if err := r.dropPreviousTables(ctx); err != nil {
		r.logf("imdb sync snapshot %d previous table cleanup warning: %v", snapshotID, err)
	}
	if err := r.createDeferredIndexes(ctx); err != nil {
		return err
	}
	if err := r.analyzeTables(ctx, liveTables().all()...); err != nil {
		return err
	}
	return nil
}

func (r *Runner) copyDataset(ctx context.Context, tx pgx.Tx, item remoteDataset, targetTable string) error {
	r.logf("imdb sync download started for %s", item.spec.Name)
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

	progressReader := newDownloadProgressReader(resp.Body, r.logger, item.spec.Name, resp.ContentLength)
	reader, err := gzip.NewReader(progressReader)
	if err != nil {
		return fmt.Errorf("open gzip for %s: %w", item.spec.Name, err)
	}
	defer reader.Close()

	copyReader, err := transformTSVForCopy(reader, item.spec.Columns)
	if err != nil {
		return fmt.Errorf("prepare %s for copy: %w", item.spec.Name, err)
	}

	started := time.Now()
	tag, err := tx.Conn().PgConn().CopyFrom(ctx, copyReader, createCopyStatement(targetTable, item.spec.CopyColumns))
	if err != nil {
		return fmt.Errorf("copy %s into %s: %w", item.spec.Name, targetTable, err)
	}
	r.logf("imdb sync copied %s into %s rows=%d duration=%s", item.spec.Name, targetTable, tag.RowsAffected(), time.Since(started).Round(time.Second))
	return nil
}

func transformTSVForCopy(input io.Reader, expectedColumns int) (io.Reader, error) {
	pipeReader, pipeWriter := io.Pipe()
	go func() {
		defer pipeWriter.Close()
		if err := transformTSVToCopyCSV(input, pipeWriter, expectedColumns); err != nil {
			_ = pipeWriter.CloseWithError(err)
		}
	}()
	return pipeReader, nil
}

func transformTSVToCopyCSV(input io.Reader, output io.Writer, expectedColumns int) error {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)

	writer := csv.NewWriter(output)
	writer.Comma = '\t'

	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		if lineNumber == 1 {
			continue
		}

		record := strings.Split(scanner.Text(), "\t")
		if len(record) != expectedColumns {
			return fmt.Errorf("line %d has %d columns, expected %d", lineNumber, len(record), expectedColumns)
		}
		if err := writer.Write(record); err != nil {
			return fmt.Errorf("write transformed record on line %d: %w", lineNumber, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan tsv input: %w", err)
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return fmt.Errorf("flush transformed csv: %w", err)
	}
	return nil
}

type downloadProgressReader struct {
	reader       io.Reader
	logger       *log.Logger
	datasetName  string
	contentBytes int64
	downloaded   int64
	logEvery     int64
	nextLog      int64
	startedAt    time.Time
	completed    bool
}

func newDownloadProgressReader(reader io.Reader, logger *log.Logger, datasetName string, contentBytes int64) io.Reader {
	logEvery := int64(64 << 20)
	return &downloadProgressReader{
		reader:       reader,
		logger:       logger,
		datasetName:  datasetName,
		contentBytes: contentBytes,
		logEvery:     logEvery,
		nextLog:      logEvery,
		startedAt:    time.Now(),
	}
}

func (r *downloadProgressReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.downloaded += int64(n)
		for r.logEvery > 0 && r.downloaded >= r.nextLog {
			r.logProgress("download progress")
			r.nextLog += r.logEvery
		}
	}
	if err == io.EOF && !r.completed {
		r.completed = true
		r.logProgress("download complete")
	}
	return n, err
}

func (r *downloadProgressReader) logProgress(prefix string) {
	if r.logger == nil {
		return
	}
	duration := time.Since(r.startedAt).Round(time.Second)
	if r.contentBytes > 0 {
		r.logger.Printf("%s %s: %d/%d bytes duration=%s", prefix, r.datasetName, r.downloaded, r.contentBytes, duration)
		return
	}
	r.logger.Printf("%s %s: %d bytes duration=%s", prefix, r.datasetName, r.downloaded, duration)
}

func loadNamesStatement(snapshotID int64, rawTable string) string {
	return fmt.Sprintf(`
			INSERT INTO names (nconst, snapshot_id, primary_name, birth_year, death_year, primary_professions, known_for_titles, row_hash, created_at, updated_at)
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
				md5(concat_ws('|',
					nconst,
					COALESCE(primary_name, ''),
					COALESCE(birth_year, ''),
					COALESCE(death_year, ''),
					COALESCE(primary_professions, ''),
					COALESCE(known_for_titles, '')
				)),
				NOW(),
				NOW()
			FROM %s
			WHERE nconst IS NOT NULL
			  AND primary_name IS NOT NULL
		`, snapshotID, rawTable)
}

func (r *Runner) createShadowTables(ctx context.Context, tx pgx.Tx, tables tableSet) error {
	statements := []struct {
		name      string
		statement string
	}{
		{
			name: "create shadow titles",
			statement: fmt.Sprintf(`
				CREATE UNLOGGED TABLE %s (
					tconst TEXT PRIMARY KEY,
					snapshot_id BIGINT,
					title_type TEXT NOT NULL,
					primary_title TEXT NOT NULL,
					original_title TEXT NOT NULL,
					is_adult BOOLEAN NOT NULL DEFAULT FALSE,
					start_year INTEGER,
					end_year INTEGER,
					runtime_minutes INTEGER,
					genres TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
					row_hash TEXT NOT NULL,
					created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
					updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
				)
			`, tables.Titles),
		},
		{
			name: "create shadow ratings",
			statement: fmt.Sprintf(`
				CREATE UNLOGGED TABLE %s (
					tconst TEXT PRIMARY KEY,
					average_rating NUMERIC(3,1) NOT NULL,
					num_votes INTEGER NOT NULL DEFAULT 0,
					row_hash TEXT NOT NULL,
					updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
				)
			`, tables.TitleRatings),
		},
		{
			name: "create shadow episodes",
			statement: fmt.Sprintf(`
				CREATE UNLOGGED TABLE %s (
					tconst TEXT PRIMARY KEY,
					parent_tconst TEXT NOT NULL,
					season_number INTEGER,
					episode_number INTEGER,
					row_hash TEXT NOT NULL,
					created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
				)
			`, tables.TitleEpisodes),
		},
		{
			name: "create shadow names",
			statement: fmt.Sprintf(`
				CREATE UNLOGGED TABLE %s (
					nconst TEXT PRIMARY KEY,
					snapshot_id BIGINT,
					primary_name TEXT NOT NULL,
					birth_year INTEGER,
					death_year INTEGER,
					primary_professions TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
					known_for_titles TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
					row_hash TEXT NOT NULL,
					created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
					updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
				)
			`, tables.Names),
		},
		{
			name: "create shadow principals",
			statement: fmt.Sprintf(`
				CREATE UNLOGGED TABLE %s (
					id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
					tconst TEXT NOT NULL,
					ordering INTEGER NOT NULL,
					nconst TEXT NOT NULL,
					category TEXT NOT NULL,
					job TEXT,
					characters JSONB,
					row_hash TEXT NOT NULL,
					created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
					UNIQUE (tconst, ordering)
				)
			`, tables.TitlePrincipals),
		},
		{
			name: "create shadow crew",
			statement: fmt.Sprintf(`
				CREATE UNLOGGED TABLE %s (
					id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
					tconst TEXT NOT NULL,
					nconst TEXT NOT NULL,
					role TEXT NOT NULL,
					ordering INTEGER,
					row_hash TEXT NOT NULL,
					created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
					UNIQUE (tconst, nconst, role, ordering)
				)
			`, tables.TitleCrewMembers),
		},
		{
			name: "create shadow akas",
			statement: fmt.Sprintf(`
				CREATE UNLOGGED TABLE %s (
					id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
					tconst TEXT NOT NULL,
					ordering INTEGER,
					title TEXT NOT NULL,
					region TEXT,
					language TEXT,
					types TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
					attributes TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
					is_original_title BOOLEAN NOT NULL DEFAULT FALSE,
					row_hash TEXT NOT NULL,
					created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
				)
			`, tables.TitleAkas),
		},
	}

	for _, statement := range statements {
		if _, err := tx.Exec(ctx, statement.statement); err != nil {
			return fmt.Errorf("%s: %w", statement.name, err)
		}
		r.logf("imdb sync %s ready", statement.name)
	}
	return nil
}

func (r *Runner) createSecondaryIndexes(ctx context.Context, tx pgx.Tx, tables tableSet) error {
	base, _ := buildIndexPlans(tables)
	for _, statement := range base {
		started := time.Now()
		if _, err := tx.Exec(ctx, statement.statement); err != nil {
			return fmt.Errorf("%s: %w", statement.name, err)
		}
		r.logf("imdb sync step complete: %s duration=%s", statement.name, time.Since(started).Round(time.Second))
	}
	return nil
}

func buildIndexPlans(tables tableSet) ([]indexStatement, []indexStatement) {
	base := []indexStatement{
		{name: "index titles snapshot_id", statement: fmt.Sprintf(`CREATE INDEX %s ON %s(snapshot_id)`, tables.Titles+"_snapshot_id_idx", tables.Titles)},
		{name: "index titles title_type", statement: fmt.Sprintf(`CREATE INDEX %s ON %s(title_type)`, tables.Titles+"_title_type_idx", tables.Titles)},
		{name: "index titles start_year", statement: fmt.Sprintf(`CREATE INDEX %s ON %s(start_year)`, tables.Titles+"_start_year_idx", tables.Titles)},
		{name: "index titles primary lower", statement: fmt.Sprintf(`CREATE INDEX %s ON %s (lower(primary_title))`, tables.Titles+"_primary_title_lower_idx", tables.Titles)},
		{name: "index ratings votes", statement: fmt.Sprintf(`CREATE INDEX %s ON %s(num_votes DESC)`, tables.TitleRatings+"_num_votes_idx", tables.TitleRatings)},
		{name: "index episodes parent", statement: fmt.Sprintf(`CREATE INDEX %s ON %s(parent_tconst, season_number, episode_number)`, tables.TitleEpisodes+"_parent_idx", tables.TitleEpisodes)},
		{name: "index principals tconst", statement: fmt.Sprintf(`CREATE INDEX %s ON %s(tconst, ordering)`, tables.TitlePrincipals+"_tconst_idx", tables.TitlePrincipals)},
		{name: "index principals nconst", statement: fmt.Sprintf(`CREATE INDEX %s ON %s(nconst, tconst)`, tables.TitlePrincipals+"_nconst_idx", tables.TitlePrincipals)},
		{name: "index crew tconst", statement: fmt.Sprintf(`CREATE INDEX %s ON %s(tconst, role, ordering)`, tables.TitleCrewMembers+"_tconst_idx", tables.TitleCrewMembers)},
		{name: "index crew nconst", statement: fmt.Sprintf(`CREATE INDEX %s ON %s(nconst, tconst)`, tables.TitleCrewMembers+"_nconst_idx", tables.TitleCrewMembers)},
		{name: "index akas tconst", statement: fmt.Sprintf(`CREATE INDEX %s ON %s(tconst)`, tables.TitleAkas+"_tconst_idx", tables.TitleAkas)},
		{name: "index akas title lower", statement: fmt.Sprintf(`CREATE INDEX %s ON %s (lower(title))`, tables.TitleAkas+"_title_lower_idx", tables.TitleAkas)},
		{name: "index akas region language", statement: fmt.Sprintf(`CREATE INDEX %s ON %s(region, language)`, tables.TitleAkas+"_region_language_idx", tables.TitleAkas)},
		{name: "index names primary lower", statement: fmt.Sprintf(`CREATE INDEX %s ON %s (lower(primary_name))`, tables.Names+"_primary_name_lower_idx", tables.Names)},
	}
	deferred := []indexStatement{
		{name: "index titles primary trgm", statement: `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_titles_search_primary_trgm ON titles USING GIN (lower(primary_title) gin_trgm_ops)`},
		{name: "index akas title trgm", statement: `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_title_akas_search_title_trgm ON title_akas USING GIN (lower(title) gin_trgm_ops)`},
	}
	return base, deferred
}

func recreateDerivedViews(ctx context.Context, execer interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}) error {
	statements := []string{
		`CREATE OR REPLACE VIEW title_search AS
		SELECT
			t.tconst,
			t.title_type,
			t.primary_title,
			t.original_title,
			t.start_year,
			t.end_year,
			t.is_adult,
			COALESCE(r.num_votes, 0) AS num_votes,
			r.average_rating
		FROM titles t
		LEFT JOIN title_ratings r ON r.tconst = t.tconst`,
		`CREATE OR REPLACE VIEW series_episode_ratings AS
		SELECT
			e.parent_tconst,
			e.tconst,
			t.primary_title,
			e.season_number,
			e.episode_number,
			r.average_rating,
			r.num_votes
		FROM title_episodes e
		JOIN titles t ON t.tconst = e.tconst
		LEFT JOIN title_ratings r ON r.tconst = e.tconst`,
	}
	for _, statement := range statements {
		if _, err := execer.Exec(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runner) promoteShadowTables(ctx context.Context, tx pgx.Tx, snapshotID int64, shadow tableSet, counts normalizeCounts, sourceUpdatedAt *time.Time, datasetVersion string, remote []remoteDataset) error {
	live := liveTables()
	previous := previousTables()

	promotionSteps := []struct {
		name      string
		statement string
		args      []any
	}{
		{name: "drop derived views", statement: `DROP VIEW IF EXISTS series_episode_ratings; DROP VIEW IF EXISTS title_search;`},
		{name: "drop previous titles backup", statement: fmt.Sprintf(`DROP TABLE IF EXISTS %s, %s, %s, %s, %s, %s, %s`, previous.TitlePrincipals, previous.TitleCrewMembers, previous.TitleAkas, previous.TitleEpisodes, previous.TitleRatings, previous.Names, previous.Titles)},
		{name: "rename live titles to backup", statement: fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, live.Titles, previous.Titles)},
		{name: "rename live ratings to backup", statement: fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, live.TitleRatings, previous.TitleRatings)},
		{name: "rename live episodes to backup", statement: fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, live.TitleEpisodes, previous.TitleEpisodes)},
		{name: "rename live names to backup", statement: fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, live.Names, previous.Names)},
		{name: "rename live principals to backup", statement: fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, live.TitlePrincipals, previous.TitlePrincipals)},
		{name: "rename live crew to backup", statement: fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, live.TitleCrewMembers, previous.TitleCrewMembers)},
		{name: "rename live akas to backup", statement: fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, live.TitleAkas, previous.TitleAkas)},
		{name: "promote titles", statement: fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, shadow.Titles, live.Titles)},
		{name: "promote ratings", statement: fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, shadow.TitleRatings, live.TitleRatings)},
		{name: "promote episodes", statement: fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, shadow.TitleEpisodes, live.TitleEpisodes)},
		{name: "promote names", statement: fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, shadow.Names, live.Names)},
		{name: "promote principals", statement: fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, shadow.TitlePrincipals, live.TitlePrincipals)},
		{name: "promote crew", statement: fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, shadow.TitleCrewMembers, live.TitleCrewMembers)},
		{name: "promote akas", statement: fmt.Sprintf(`ALTER TABLE %s RENAME TO %s`, shadow.TitleAkas, live.TitleAkas)},
		{name: "deactivate previous snapshots", statement: `UPDATE imdb_snapshots SET is_active = FALSE WHERE id <> $1`, args: []any{snapshotID}},
		{name: "finalize snapshot", statement: `
			UPDATE imdb_snapshots
			SET
				status = 'ready',
				dataset_version = $2,
				source_updated_at = $3,
				source_etag = $4,
				completed_at = NOW(),
				duration_seconds = GREATEST(EXTRACT(EPOCH FROM (NOW() - imported_at))::INTEGER, 0),
				is_active = TRUE,
				title_count = $5,
				name_count = $6,
				rating_count = $7,
				episode_count = $8,
				principal_count = $9,
				crew_member_count = $10,
				aka_count = $11,
				notes = ''
			WHERE id = $1
		`, args: []any{snapshotID, datasetVersion, sourceUpdatedAt, joinRemoteValues(remote, func(item remoteDataset) string { return item.etag }), counts.Titles, counts.Names, counts.Ratings, counts.Episodes, counts.Principals, counts.CrewMembers, counts.AlternateIDs}},
	}

	for _, step := range promotionSteps {
		started := time.Now()
		if _, err := tx.Exec(ctx, step.statement, step.args...); err != nil {
			return fmt.Errorf("%s: %w", step.name, err)
		}
		r.logf("imdb sync snapshot %d promotion step complete: %s duration=%s", snapshotID, step.name, time.Since(started).Round(time.Second))
	}

	if err := recreateDerivedViews(ctx, tx); err != nil {
		return fmt.Errorf("recreate derived views: %w", err)
	}

	if err := upsertSyncStateWithExecutor(ctx, tx, remote, &snapshotID); err != nil {
		return err
	}
	return nil
}

func (r *Runner) dropPreviousTables(ctx context.Context) error {
	previous := previousTables()
	_, err := r.pool.Exec(ctx, fmt.Sprintf(`
		DROP TABLE IF EXISTS %s, %s, %s, %s, %s, %s, %s
	`, previous.TitlePrincipals, previous.TitleCrewMembers, previous.TitleAkas, previous.TitleEpisodes, previous.TitleRatings, previous.Names, previous.Titles))
	if err != nil {
		return fmt.Errorf("drop previous tables: %w", err)
	}
	return nil
}

func (r *Runner) setLocalMaintenanceWorkMem(ctx context.Context, tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, `SELECT set_config('maintenance_work_mem', $1, true)`, r.maintenanceWorkMem); err != nil {
		return fmt.Errorf("set local maintenance_work_mem: %w", err)
	}
	return nil
}

func (r *Runner) createDeferredIndexes(ctx context.Context) error {
	_, deferred := buildIndexPlans(liveTables())
	for _, statement := range deferred {
		started := time.Now()
		if _, err := r.pool.Exec(ctx, statement.statement); err != nil {
			return fmt.Errorf("%s: %w", statement.name, err)
		}
		r.logf("imdb sync step complete: %s duration=%s", statement.name, time.Since(started).Round(time.Second))
	}
	return nil
}

func (r *Runner) analyzeTables(ctx context.Context, tables ...string) error {
	for _, table := range tables {
		started := time.Now()
		if _, err := r.pool.Exec(ctx, fmt.Sprintf("ANALYZE %s", table)); err != nil {
			return fmt.Errorf("analyze %s: %w", table, err)
		}
		r.logf("imdb sync step complete: analyze %s duration=%s", table, time.Since(started).Round(time.Second))
	}
	return nil
}

func (r *Runner) dropTables(ctx context.Context, tx pgx.Tx, tables ...string) error {
	if len(tables) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", strings.Join(tables, ", ")))
	if err != nil {
		return fmt.Errorf("drop tables %s: %w", strings.Join(tables, ", "), err)
	}
	return nil
}

func deltaTableName(snapshotID int64, base string) string {
	return fmt.Sprintf("%s_delta_%d", base, snapshotID)
}

func (r *Runner) importDeltaSnapshot(ctx context.Context, snapshotID int64, changed []remoteDataset, remote []remoteDataset, baseline snapshotCounts, sourceUpdatedAt *time.Time, datasetVersion string) error {
	r.logf("imdb sync snapshot %d delta import started for %d datasets", snapshotID, len(changed))
	counts := baseline

	for _, item := range changed {
		var affectedTables []string
		tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return fmt.Errorf("begin delta tx for %s: %w", item.spec.Name, err)
		}

		stageTable := rawTableName(snapshotID, item.spec)
		r.logf("imdb sync preparing staging table %s for %s", stageTable, item.spec.Name)
		if _, err := tx.Exec(ctx, createRawTableStatement(stageTable, item.spec.ColumnDefs)); err != nil {
			tx.Rollback(ctx)
			return fmt.Errorf("create raw table for %s: %w", item.spec.Name, err)
		}
		if err := r.copyDataset(ctx, tx, item, stageTable); err != nil {
			tx.Rollback(ctx)
			return err
		}

		switch item.spec.Name {
		case "title.basics.tsv.gz":
			counts.Titles, err = r.mergeTitlesDelta(ctx, tx, snapshotID, stageTable)
			affectedTables = []string{"titles", "title_search"}
		case "name.basics.tsv.gz":
			counts.Names, err = r.mergeNamesDelta(ctx, tx, snapshotID, stageTable)
			affectedTables = []string{"names"}
		case "title.ratings.tsv.gz":
			counts.Ratings, err = r.mergeRatingsDelta(ctx, tx, snapshotID, stageTable)
			affectedTables = []string{"title_ratings", "title_search", "series_episode_ratings"}
		case "title.episode.tsv.gz":
			counts.Episodes, err = r.mergeEpisodesDelta(ctx, tx, stageTable)
			affectedTables = []string{"title_episodes", "series_episode_ratings"}
		case "title.principals.tsv.gz":
			counts.Principals, err = r.mergePrincipalsDelta(ctx, tx, stageTable)
			affectedTables = []string{"title_principals"}
		case "title.crew.tsv.gz":
			counts.CrewMembers, err = r.mergeCrewDelta(ctx, tx, stageTable)
			affectedTables = []string{"title_crew_members"}
		case "title.akas.tsv.gz":
			counts.Akas, err = r.mergeAkasDelta(ctx, tx, stageTable)
			affectedTables = []string{"title_akas"}
		default:
			err = fmt.Errorf("unsupported delta dataset %s", item.spec.Name)
		}
		if err != nil {
			tx.Rollback(ctx)
			return err
		}

		if err := r.dropTables(ctx, tx, stageTable); err != nil {
			tx.Rollback(ctx)
			return err
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit delta tx for %s: %w", item.spec.Name, err)
		}
		if err := r.updateSnapshotProgress(ctx, snapshotID, counts); err != nil {
			return err
		}
		if len(affectedTables) > 0 {
			if err := r.analyzeTables(ctx, affectedTables...); err != nil {
				return err
			}
		}
	}

	if err := r.finalizeDeltaSnapshot(ctx, snapshotID, remote, counts, sourceUpdatedAt, datasetVersion); err != nil {
		return err
	}
	return nil
}

func (r *Runner) updateSnapshotProgress(ctx context.Context, snapshotID int64, counts snapshotCounts) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE imdb_snapshots
		SET
			title_count = $2,
			name_count = $3,
			rating_count = $4,
			episode_count = $5,
			principal_count = $6,
			crew_member_count = $7,
			aka_count = $8
		WHERE id = $1
	`, snapshotID, counts.Titles, counts.Names, counts.Ratings, counts.Episodes, counts.Principals, counts.CrewMembers, counts.Akas)
	if err != nil {
		return fmt.Errorf("update snapshot progress: %w", err)
	}
	return nil
}

func (r *Runner) finalizeDeltaSnapshot(ctx context.Context, snapshotID int64, remote []remoteDataset, counts snapshotCounts, sourceUpdatedAt *time.Time, datasetVersion string) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin delta finalize tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `UPDATE imdb_snapshots SET is_active = FALSE WHERE id <> $1`, snapshotID); err != nil {
		return fmt.Errorf("deactivate previous snapshots: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE imdb_snapshots
		SET
			status = 'ready',
			dataset_version = $2,
			source_updated_at = $3,
			source_etag = $4,
			completed_at = NOW(),
			duration_seconds = GREATEST(EXTRACT(EPOCH FROM (NOW() - imported_at))::INTEGER, 0),
			is_active = TRUE,
			title_count = $5,
			name_count = $6,
			rating_count = $7,
			episode_count = $8,
			principal_count = $9,
			crew_member_count = $10,
			aka_count = $11,
			notes = ''
		WHERE id = $1
	`, snapshotID, datasetVersion, sourceUpdatedAt, joinRemoteValues(remote, func(item remoteDataset) string { return item.etag }), counts.Titles, counts.Names, counts.Ratings, counts.Episodes, counts.Principals, counts.CrewMembers, counts.Akas); err != nil {
		return fmt.Errorf("finalize delta snapshot: %w", err)
	}
	if err := upsertSyncStateWithExecutor(ctx, tx, remote, &snapshotID); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit delta finalize tx: %w", err)
	}
	r.logf("imdb sync snapshot %d committed to live tables", snapshotID)
	return nil
}

func (r *Runner) execBatchedStatement(ctx context.Context, tx pgx.Tx, label, statement string, args ...any) (int64, error) {
	var total int64
	for {
		tag, err := tx.Exec(ctx, statement, args...)
		if err != nil {
			return total, fmt.Errorf("%s: %w", label, err)
		}
		if tag.RowsAffected() == 0 {
			return total, nil
		}
		total += tag.RowsAffected()
	}
}

func (r *Runner) mergeTitlesDelta(ctx context.Context, tx pgx.Tx, snapshotID int64, stageTable string) (int64, error) {
	deltaTable := deltaTableName(snapshotID, "titles")
	if _, err := tx.Exec(ctx, fmt.Sprintf(`
		CREATE UNLOGGED TABLE %s (
			tconst TEXT PRIMARY KEY,
			snapshot_id BIGINT,
			title_type TEXT NOT NULL,
			primary_title TEXT NOT NULL,
			original_title TEXT NOT NULL,
			is_adult BOOLEAN NOT NULL DEFAULT FALSE,
			start_year INTEGER,
			end_year INTEGER,
			runtime_minutes INTEGER,
			genres TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
			row_hash TEXT NOT NULL
		)
	`, deltaTable)); err != nil {
		return 0, fmt.Errorf("create titles delta table: %w", err)
	}

	tag, err := tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (tconst, snapshot_id, title_type, primary_title, original_title, is_adult, start_year, end_year, runtime_minutes, genres, row_hash)
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
			CASE WHEN genres IS NULL OR genres = '' THEN ARRAY[]::TEXT[] ELSE string_to_array(genres, ',') END,
			md5(concat_ws('|', tconst, COALESCE(title_type, ''), COALESCE(primary_title, ''), COALESCE(original_title, primary_title, ''), COALESCE(is_adult, ''), COALESCE(start_year, ''), COALESCE(end_year, ''), COALESCE(runtime_minutes, ''), COALESCE(genres, '')))
		FROM %s
	`, deltaTable, snapshotID, stageTable))
	if err != nil {
		return 0, fmt.Errorf("normalize titles delta: %w", err)
	}
	count := tag.RowsAffected()

	if _, err := r.execBatchedStatement(ctx, tx, "insert titles delta batch", fmt.Sprintf(`
		WITH batch AS (
			SELECT d.*
			FROM %s d
			LEFT JOIN titles t ON t.tconst = d.tconst
			WHERE t.tconst IS NULL
			ORDER BY d.tconst
			LIMIT %d
		)
		INSERT INTO titles (tconst, snapshot_id, title_type, primary_title, original_title, is_adult, start_year, end_year, runtime_minutes, genres, row_hash, created_at, updated_at)
		SELECT tconst, snapshot_id, title_type, primary_title, original_title, is_adult, start_year, end_year, runtime_minutes, genres, row_hash, NOW(), NOW()
		FROM batch
	`, deltaTable, r.deltaBatchSize)); err != nil {
		return 0, err
	}
	if _, err := r.execBatchedStatement(ctx, tx, "update titles delta batch", fmt.Sprintf(`
		WITH batch AS (
			SELECT d.*
			FROM %s d
			JOIN titles t ON t.tconst = d.tconst
			WHERE t.row_hash IS DISTINCT FROM d.row_hash
			ORDER BY d.tconst
			LIMIT %d
		)
		UPDATE titles t
		SET
			snapshot_id = b.snapshot_id,
			title_type = b.title_type,
			primary_title = b.primary_title,
			original_title = b.original_title,
			is_adult = b.is_adult,
			start_year = b.start_year,
			end_year = b.end_year,
			runtime_minutes = b.runtime_minutes,
			genres = b.genres,
			row_hash = b.row_hash,
			updated_at = NOW()
		FROM batch b
		WHERE t.tconst = b.tconst
	`, deltaTable, r.deltaBatchSize)); err != nil {
		return 0, err
	}
	if _, err := r.execBatchedStatement(ctx, tx, "delete titles delta batch", fmt.Sprintf(`
		WITH batch AS (
			SELECT t.tconst
			FROM titles t
			LEFT JOIN %s d ON d.tconst = t.tconst
			WHERE d.tconst IS NULL
			ORDER BY t.tconst
			LIMIT %d
		)
		DELETE FROM titles t
		USING batch b
		WHERE t.tconst = b.tconst
	`, deltaTable, r.deltaBatchSize)); err != nil {
		return 0, err
	}
	if err := r.dropTables(ctx, tx, deltaTable); err != nil {
		return 0, err
	}
	return count, nil
}

func (r *Runner) mergeNamesDelta(ctx context.Context, tx pgx.Tx, snapshotID int64, stageTable string) (int64, error) {
	deltaTable := deltaTableName(snapshotID, "names")
	if _, err := tx.Exec(ctx, fmt.Sprintf(`
		CREATE UNLOGGED TABLE %s (
			nconst TEXT PRIMARY KEY,
			snapshot_id BIGINT,
			primary_name TEXT NOT NULL,
			birth_year INTEGER,
			death_year INTEGER,
			primary_professions TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
			known_for_titles TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
			row_hash TEXT NOT NULL
		)
	`, deltaTable)); err != nil {
		return 0, fmt.Errorf("create names delta table: %w", err)
	}
	tag, err := tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (nconst, snapshot_id, primary_name, birth_year, death_year, primary_professions, known_for_titles, row_hash)
		SELECT
			nconst,
			%d,
			primary_name,
			NULLIF(birth_year, '')::INTEGER,
			NULLIF(death_year, '')::INTEGER,
			CASE WHEN primary_professions IS NULL OR primary_professions = '' THEN ARRAY[]::TEXT[] ELSE string_to_array(primary_professions, ',') END,
			CASE WHEN known_for_titles IS NULL OR known_for_titles = '' THEN ARRAY[]::TEXT[] ELSE string_to_array(known_for_titles, ',') END,
			md5(concat_ws('|', nconst, COALESCE(primary_name, ''), COALESCE(birth_year, ''), COALESCE(death_year, ''), COALESCE(primary_professions, ''), COALESCE(known_for_titles, '')))
		FROM %s
		WHERE nconst IS NOT NULL
		  AND primary_name IS NOT NULL
	`, deltaTable, snapshotID, stageTable))
	if err != nil {
		return 0, fmt.Errorf("normalize names delta: %w", err)
	}
	count := tag.RowsAffected()

	if _, err := r.execBatchedStatement(ctx, tx, "insert names delta batch", fmt.Sprintf(`
		WITH batch AS (
			SELECT d.*
			FROM %s d
			LEFT JOIN names n ON n.nconst = d.nconst
			WHERE n.nconst IS NULL
			ORDER BY d.nconst
			LIMIT %d
		)
		INSERT INTO names (nconst, snapshot_id, primary_name, birth_year, death_year, primary_professions, known_for_titles, row_hash, created_at, updated_at)
		SELECT nconst, snapshot_id, primary_name, birth_year, death_year, primary_professions, known_for_titles, row_hash, NOW(), NOW()
		FROM batch
	`, deltaTable, r.deltaBatchSize)); err != nil {
		return 0, err
	}
	if _, err := r.execBatchedStatement(ctx, tx, "update names delta batch", fmt.Sprintf(`
		WITH batch AS (
			SELECT d.*
			FROM %s d
			JOIN names n ON n.nconst = d.nconst
			WHERE n.row_hash IS DISTINCT FROM d.row_hash
			ORDER BY d.nconst
			LIMIT %d
		)
		UPDATE names n
		SET
			snapshot_id = b.snapshot_id,
			primary_name = b.primary_name,
			birth_year = b.birth_year,
			death_year = b.death_year,
			primary_professions = b.primary_professions,
			known_for_titles = b.known_for_titles,
			row_hash = b.row_hash,
			updated_at = NOW()
		FROM batch b
		WHERE n.nconst = b.nconst
	`, deltaTable, r.deltaBatchSize)); err != nil {
		return 0, err
	}
	if _, err := r.execBatchedStatement(ctx, tx, "delete names delta batch", fmt.Sprintf(`
		WITH batch AS (
			SELECT n.nconst
			FROM names n
			LEFT JOIN %s d ON d.nconst = n.nconst
			WHERE d.nconst IS NULL
			ORDER BY n.nconst
			LIMIT %d
		)
		DELETE FROM names n
		USING batch b
		WHERE n.nconst = b.nconst
	`, deltaTable, r.deltaBatchSize)); err != nil {
		return 0, err
	}
	if err := r.dropTables(ctx, tx, deltaTable); err != nil {
		return 0, err
	}
	return count, nil
}

func (r *Runner) mergeRatingsDelta(ctx context.Context, tx pgx.Tx, snapshotID int64, stageTable string) (int64, error) {
	deltaTable := deltaTableName(snapshotID, "title_ratings")
	if _, err := tx.Exec(ctx, fmt.Sprintf(`
		CREATE UNLOGGED TABLE %s (
			tconst TEXT PRIMARY KEY,
			average_rating NUMERIC(3,1) NOT NULL,
			num_votes INTEGER NOT NULL DEFAULT 0,
			row_hash TEXT NOT NULL
		)
	`, deltaTable)); err != nil {
		return 0, fmt.Errorf("create ratings delta table: %w", err)
	}
	tag, err := tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (tconst, average_rating, num_votes, row_hash)
		SELECT
			r.tconst,
			r.average_rating::NUMERIC(3,1),
			COALESCE(NULLIF(r.num_votes, ''), '0')::INTEGER,
			md5(concat_ws('|', r.tconst, COALESCE(r.average_rating, ''), COALESCE(r.num_votes, '0')))
		FROM %s r
		JOIN titles t ON t.tconst = r.tconst
	`, deltaTable, stageTable))
	if err != nil {
		return 0, fmt.Errorf("normalize ratings delta: %w", err)
	}
	count := tag.RowsAffected()
	if _, err := r.execBatchedStatement(ctx, tx, "insert ratings delta batch", fmt.Sprintf(`
		WITH batch AS (
			SELECT d.*
			FROM %s d
			LEFT JOIN title_ratings t ON t.tconst = d.tconst
			WHERE t.tconst IS NULL
			ORDER BY d.tconst
			LIMIT %d
		)
		INSERT INTO title_ratings (tconst, average_rating, num_votes, row_hash, updated_at)
		SELECT tconst, average_rating, num_votes, row_hash, NOW()
		FROM batch
	`, deltaTable, r.deltaBatchSize)); err != nil {
		return 0, err
	}
	if _, err := r.execBatchedStatement(ctx, tx, "update ratings delta batch", fmt.Sprintf(`
		WITH batch AS (
			SELECT d.*
			FROM %s d
			JOIN title_ratings t ON t.tconst = d.tconst
			WHERE t.row_hash IS DISTINCT FROM d.row_hash
			ORDER BY d.tconst
			LIMIT %d
		)
		UPDATE title_ratings t
		SET average_rating = b.average_rating, num_votes = b.num_votes, row_hash = b.row_hash, updated_at = NOW()
		FROM batch b
		WHERE t.tconst = b.tconst
	`, deltaTable, r.deltaBatchSize)); err != nil {
		return 0, err
	}
	if _, err := r.execBatchedStatement(ctx, tx, "delete ratings delta batch", fmt.Sprintf(`
		WITH batch AS (
			SELECT t.tconst
			FROM title_ratings t
			LEFT JOIN %s d ON d.tconst = t.tconst
			WHERE d.tconst IS NULL
			ORDER BY t.tconst
			LIMIT %d
		)
		DELETE FROM title_ratings t
		USING batch b
		WHERE t.tconst = b.tconst
	`, deltaTable, r.deltaBatchSize)); err != nil {
		return 0, err
	}
	if err := r.dropTables(ctx, tx, deltaTable); err != nil {
		return 0, err
	}
	return count, nil
}

func (r *Runner) mergeEpisodesDelta(ctx context.Context, tx pgx.Tx, stageTable string) (int64, error) {
	deltaTable := deltaTableName(time.Now().UnixNano(), "title_episodes")
	if _, err := tx.Exec(ctx, fmt.Sprintf(`
		CREATE UNLOGGED TABLE %s (
			tconst TEXT PRIMARY KEY,
			parent_tconst TEXT NOT NULL,
			season_number INTEGER,
			episode_number INTEGER,
			row_hash TEXT NOT NULL
		)
	`, deltaTable)); err != nil {
		return 0, fmt.Errorf("create episodes delta table: %w", err)
	}
	tag, err := tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (tconst, parent_tconst, season_number, episode_number, row_hash)
		SELECT
			e.tconst,
			e.parent_tconst,
			NULLIF(e.season_number, '')::INTEGER,
			NULLIF(e.episode_number, '')::INTEGER,
			md5(concat_ws('|', e.tconst, e.parent_tconst, COALESCE(e.season_number, ''), COALESCE(e.episode_number, '')))
		FROM %s e
		JOIN titles child_title ON child_title.tconst = e.tconst
		JOIN titles parent_title ON parent_title.tconst = e.parent_tconst
	`, deltaTable, stageTable))
	if err != nil {
		return 0, fmt.Errorf("normalize episodes delta: %w", err)
	}
	count := tag.RowsAffected()
	if _, err := r.execBatchedStatement(ctx, tx, "insert episodes delta batch", fmt.Sprintf(`
		WITH batch AS (
			SELECT d.*
			FROM %s d
			LEFT JOIN title_episodes t ON t.tconst = d.tconst
			WHERE t.tconst IS NULL
			ORDER BY d.tconst
			LIMIT %d
		)
		INSERT INTO title_episodes (tconst, parent_tconst, season_number, episode_number, row_hash, created_at)
		SELECT tconst, parent_tconst, season_number, episode_number, row_hash, NOW()
		FROM batch
	`, deltaTable, r.deltaBatchSize)); err != nil {
		return 0, err
	}
	if _, err := r.execBatchedStatement(ctx, tx, "update episodes delta batch", fmt.Sprintf(`
		WITH batch AS (
			SELECT d.*
			FROM %s d
			JOIN title_episodes t ON t.tconst = d.tconst
			WHERE t.row_hash IS DISTINCT FROM d.row_hash
			ORDER BY d.tconst
			LIMIT %d
		)
		UPDATE title_episodes t
		SET parent_tconst = b.parent_tconst, season_number = b.season_number, episode_number = b.episode_number, row_hash = b.row_hash
		FROM batch b
		WHERE t.tconst = b.tconst
	`, deltaTable, r.deltaBatchSize)); err != nil {
		return 0, err
	}
	if _, err := r.execBatchedStatement(ctx, tx, "delete episodes delta batch", fmt.Sprintf(`
		WITH batch AS (
			SELECT t.tconst
			FROM title_episodes t
			LEFT JOIN %s d ON d.tconst = t.tconst
			WHERE d.tconst IS NULL
			ORDER BY t.tconst
			LIMIT %d
		)
		DELETE FROM title_episodes t
		USING batch b
		WHERE t.tconst = b.tconst
	`, deltaTable, r.deltaBatchSize)); err != nil {
		return 0, err
	}
	if err := r.dropTables(ctx, tx, deltaTable); err != nil {
		return 0, err
	}
	return count, nil
}

func (r *Runner) mergePrincipalsDelta(ctx context.Context, tx pgx.Tx, stageTable string) (int64, error) {
	deltaTable := deltaTableName(time.Now().UnixNano(), "title_principals")
	if _, err := tx.Exec(ctx, fmt.Sprintf(`
		CREATE UNLOGGED TABLE %s (
			tconst TEXT NOT NULL,
			ordering INTEGER NOT NULL,
			nconst TEXT NOT NULL,
			category TEXT NOT NULL,
			job TEXT,
			characters JSONB,
			row_hash TEXT NOT NULL,
			PRIMARY KEY (tconst, ordering)
		)
	`, deltaTable)); err != nil {
		return 0, fmt.Errorf("create principals delta table: %w", err)
	}
	tag, err := tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (tconst, ordering, nconst, category, job, characters, row_hash)
		SELECT
			p.tconst,
			NULLIF(p.ordering, '')::INTEGER,
			p.nconst,
			p.category,
			p.job,
			CASE WHEN p.characters IS NULL OR p.characters = '' THEN NULL ELSE p.characters::JSONB END,
			md5(concat_ws('|', p.tconst, COALESCE(p.ordering, ''), p.nconst, p.category, COALESCE(p.job, ''), COALESCE(p.characters, '')))
		FROM %s p
		JOIN titles t ON t.tconst = p.tconst
		JOIN names n ON n.nconst = p.nconst
	`, deltaTable, stageTable))
	if err != nil {
		return 0, fmt.Errorf("normalize principals delta: %w", err)
	}
	count := tag.RowsAffected()
	if _, err := r.execBatchedStatement(ctx, tx, "insert principals delta batch", fmt.Sprintf(`
		WITH batch AS (
			SELECT d.*
			FROM %s d
			LEFT JOIN title_principals t ON t.tconst = d.tconst AND t.ordering = d.ordering
			WHERE t.tconst IS NULL
			ORDER BY d.tconst, d.ordering
			LIMIT %d
		)
		INSERT INTO title_principals (tconst, ordering, nconst, category, job, characters, row_hash, created_at)
		SELECT tconst, ordering, nconst, category, job, characters, row_hash, NOW()
		FROM batch
	`, deltaTable, r.deltaBatchSize)); err != nil {
		return 0, err
	}
	if _, err := r.execBatchedStatement(ctx, tx, "update principals delta batch", fmt.Sprintf(`
		WITH batch AS (
			SELECT d.*
			FROM %s d
			JOIN title_principals t ON t.tconst = d.tconst AND t.ordering = d.ordering
			WHERE t.row_hash IS DISTINCT FROM d.row_hash
			ORDER BY d.tconst, d.ordering
			LIMIT %d
		)
		UPDATE title_principals t
		SET nconst = b.nconst, category = b.category, job = b.job, characters = b.characters, row_hash = b.row_hash
		FROM batch b
		WHERE t.tconst = b.tconst AND t.ordering = b.ordering
	`, deltaTable, r.deltaBatchSize)); err != nil {
		return 0, err
	}
	if _, err := r.execBatchedStatement(ctx, tx, "delete principals delta batch", fmt.Sprintf(`
		WITH batch AS (
			SELECT t.tconst, t.ordering
			FROM title_principals t
			LEFT JOIN %s d ON d.tconst = t.tconst AND d.ordering = t.ordering
			WHERE d.tconst IS NULL
			ORDER BY t.tconst, t.ordering
			LIMIT %d
		)
		DELETE FROM title_principals t
		USING batch b
		WHERE t.tconst = b.tconst AND t.ordering = b.ordering
	`, deltaTable, r.deltaBatchSize)); err != nil {
		return 0, err
	}
	if err := r.dropTables(ctx, tx, deltaTable); err != nil {
		return 0, err
	}
	return count, nil
}

func (r *Runner) mergeCrewDelta(ctx context.Context, tx pgx.Tx, stageTable string) (int64, error) {
	deltaTable := deltaTableName(time.Now().UnixNano(), "title_crew_members")
	if _, err := tx.Exec(ctx, fmt.Sprintf(`
		CREATE UNLOGGED TABLE %s (
			tconst TEXT NOT NULL,
			nconst TEXT NOT NULL,
			role TEXT NOT NULL,
			ordering INTEGER,
			row_hash TEXT NOT NULL
		)
	`, deltaTable)); err != nil {
		return 0, fmt.Errorf("create crew delta table: %w", err)
	}
	var count int64
	for _, roleColumn := range []struct {
		column string
		role   string
	}{
		{column: "directors", role: "director"},
		{column: "writers", role: "writer"},
	} {
		tag, err := tx.Exec(ctx, fmt.Sprintf(`
			INSERT INTO %s (tconst, nconst, role, ordering, row_hash)
			SELECT
				c.tconst,
				trim(member.nconst),
				'%s',
				member.ord::INTEGER,
				md5(concat_ws('|', c.tconst, trim(member.nconst), '%s', member.ord::TEXT))
			FROM %s c
			JOIN titles t ON t.tconst = c.tconst
			CROSS JOIN LATERAL unnest(string_to_array(COALESCE(c.%s, ''), ',')) WITH ORDINALITY AS member(nconst, ord)
			JOIN names n ON n.nconst = trim(member.nconst)
			WHERE trim(member.nconst) <> ''
		`, deltaTable, roleColumn.role, roleColumn.role, stageTable, roleColumn.column))
		if err != nil {
			return 0, fmt.Errorf("normalize crew delta %s: %w", roleColumn.role, err)
		}
		count += tag.RowsAffected()
	}
	if _, err := r.execBatchedStatement(ctx, tx, "insert crew delta batch", fmt.Sprintf(`
		WITH batch AS (
			SELECT d.*
			FROM %s d
			LEFT JOIN title_crew_members t
			  ON t.tconst = d.tconst
			 AND t.nconst = d.nconst
			 AND t.role = d.role
			 AND t.ordering IS NOT DISTINCT FROM d.ordering
			WHERE t.tconst IS NULL
			ORDER BY d.tconst, d.nconst, d.role, d.ordering
			LIMIT %d
		)
		INSERT INTO title_crew_members (tconst, nconst, role, ordering, row_hash, created_at)
		SELECT tconst, nconst, role, ordering, row_hash, NOW()
		FROM batch
	`, deltaTable, r.deltaBatchSize)); err != nil {
		return 0, err
	}
	if _, err := r.execBatchedStatement(ctx, tx, "delete crew delta batch", fmt.Sprintf(`
		WITH batch AS (
			SELECT t.tconst, t.nconst, t.role, t.ordering
			FROM title_crew_members t
			LEFT JOIN %s d
			  ON d.tconst = t.tconst
			 AND d.nconst = t.nconst
			 AND d.role = t.role
			 AND d.ordering IS NOT DISTINCT FROM t.ordering
			WHERE d.tconst IS NULL
			ORDER BY t.tconst, t.nconst, t.role, t.ordering
			LIMIT %d
		)
		DELETE FROM title_crew_members t
		USING batch b
		WHERE t.tconst = b.tconst AND t.nconst = b.nconst AND t.role = b.role AND t.ordering IS NOT DISTINCT FROM b.ordering
	`, deltaTable, r.deltaBatchSize)); err != nil {
		return 0, err
	}
	if err := r.dropTables(ctx, tx, deltaTable); err != nil {
		return 0, err
	}
	return count, nil
}

func (r *Runner) mergeAkasDelta(ctx context.Context, tx pgx.Tx, stageTable string) (int64, error) {
	deltaTable := deltaTableName(time.Now().UnixNano(), "title_akas")
	if _, err := tx.Exec(ctx, fmt.Sprintf(`
		CREATE UNLOGGED TABLE %s (
			tconst TEXT NOT NULL,
			ordering INTEGER,
			title TEXT NOT NULL,
			region TEXT,
			language TEXT,
			types TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
			attributes TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
			is_original_title BOOLEAN NOT NULL DEFAULT FALSE,
			row_hash TEXT NOT NULL
		)
	`, deltaTable)); err != nil {
		return 0, fmt.Errorf("create akas delta table: %w", err)
	}
	tag, err := tx.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (tconst, ordering, title, region, language, types, attributes, is_original_title, row_hash)
		SELECT
			a.title_id,
			NULLIF(a.ordering, '')::INTEGER,
			a.title,
			a.region,
			a.language,
			CASE WHEN a.types IS NULL OR a.types = '' THEN ARRAY[]::TEXT[] ELSE string_to_array(a.types, ',') END,
			CASE WHEN a.attributes IS NULL OR a.attributes = '' THEN ARRAY[]::TEXT[] ELSE string_to_array(a.attributes, ',') END,
			COALESCE(a.is_original_title = '1', FALSE),
			md5(concat_ws('|', a.title_id, COALESCE(a.ordering, ''), COALESCE(a.title, ''), COALESCE(a.region, ''), COALESCE(a.language, ''), COALESCE(a.types, ''), COALESCE(a.attributes, ''), COALESCE(a.is_original_title, '0')))
		FROM %s a
		JOIN titles t ON t.tconst = a.title_id
	`, deltaTable, stageTable))
	if err != nil {
		return 0, fmt.Errorf("normalize akas delta: %w", err)
	}
	count := tag.RowsAffected()
	if _, err := r.execBatchedStatement(ctx, tx, "insert akas delta batch", fmt.Sprintf(`
		WITH batch AS (
			SELECT d.*
			FROM %s d
			LEFT JOIN title_akas t ON t.tconst = d.tconst AND t.ordering IS NOT DISTINCT FROM d.ordering
			WHERE t.tconst IS NULL
			ORDER BY d.tconst, d.ordering
			LIMIT %d
		)
		INSERT INTO title_akas (tconst, ordering, title, region, language, types, attributes, is_original_title, row_hash, created_at)
		SELECT tconst, ordering, title, region, language, types, attributes, is_original_title, row_hash, NOW()
		FROM batch
	`, deltaTable, r.deltaBatchSize)); err != nil {
		return 0, err
	}
	if _, err := r.execBatchedStatement(ctx, tx, "update akas delta batch", fmt.Sprintf(`
		WITH batch AS (
			SELECT d.*
			FROM %s d
			JOIN title_akas t ON t.tconst = d.tconst AND t.ordering IS NOT DISTINCT FROM d.ordering
			WHERE t.row_hash IS DISTINCT FROM d.row_hash
			ORDER BY d.tconst, d.ordering
			LIMIT %d
		)
		UPDATE title_akas t
		SET title = b.title, region = b.region, language = b.language, types = b.types, attributes = b.attributes, is_original_title = b.is_original_title, row_hash = b.row_hash
		FROM batch b
		WHERE t.tconst = b.tconst AND t.ordering IS NOT DISTINCT FROM b.ordering
	`, deltaTable, r.deltaBatchSize)); err != nil {
		return 0, err
	}
	if _, err := r.execBatchedStatement(ctx, tx, "delete akas delta batch", fmt.Sprintf(`
		WITH batch AS (
			SELECT t.tconst, t.ordering
			FROM title_akas t
			LEFT JOIN %s d ON d.tconst = t.tconst AND d.ordering IS NOT DISTINCT FROM t.ordering
			WHERE d.tconst IS NULL
			ORDER BY t.tconst, t.ordering
			LIMIT %d
		)
		DELETE FROM title_akas t
		USING batch b
		WHERE t.tconst = b.tconst AND t.ordering IS NOT DISTINCT FROM b.ordering
	`, deltaTable, r.deltaBatchSize)); err != nil {
		return 0, err
	}
	if err := r.dropTables(ctx, tx, deltaTable); err != nil {
		return 0, err
	}
	return count, nil
}

func (r *Runner) normalizeSnapshot(ctx context.Context, tx pgx.Tx, tables tableSet, snapshotID int64) (normalizeCounts, error) {
	rawTitleBasics := rawTableName(snapshotID, r.datasetByName("title.basics.tsv.gz"))
	rawNameBasics := rawTableName(snapshotID, r.datasetByName("name.basics.tsv.gz"))
	rawRatings := rawTableName(snapshotID, r.datasetByName("title.ratings.tsv.gz"))
	rawEpisodes := rawTableName(snapshotID, r.datasetByName("title.episode.tsv.gz"))
	rawPrincipals := rawTableName(snapshotID, r.datasetByName("title.principals.tsv.gz"))
	rawCrew := rawTableName(snapshotID, r.datasetByName("title.crew.tsv.gz"))
	rawAkas := rawTableName(snapshotID, r.datasetByName("title.akas.tsv.gz"))

	type normalizeStep struct {
		name      string
		statement string
	}

	var counts normalizeCounts
	steps := []normalizeStep{
		{name: "load titles", statement: fmt.Sprintf(`
			INSERT INTO %s (tconst, snapshot_id, title_type, primary_title, original_title, is_adult, start_year, end_year, runtime_minutes, genres, row_hash, created_at, updated_at)
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
				md5(concat_ws('|',
					tconst,
					COALESCE(title_type, ''),
					COALESCE(primary_title, ''),
					COALESCE(original_title, primary_title, ''),
					COALESCE(is_adult, ''),
					COALESCE(start_year, ''),
					COALESCE(end_year, ''),
					COALESCE(runtime_minutes, ''),
					COALESCE(genres, '')
				)),
				NOW(),
				NOW()
			FROM %s
		`, tables.Titles, snapshotID, rawTitleBasics)},
		{name: "load names", statement: strings.Replace(loadNamesStatement(snapshotID, rawNameBasics), "INSERT INTO names", fmt.Sprintf("INSERT INTO %s", tables.Names), 1)},
		{name: "load ratings", statement: `
			INSERT INTO ` + tables.TitleRatings + ` (tconst, average_rating, num_votes, row_hash, updated_at)
			SELECT
				tconst,
				average_rating::NUMERIC(3,1),
				COALESCE(NULLIF(num_votes, ''), '0')::INTEGER,
				md5(concat_ws('|', tconst, COALESCE(average_rating, ''), COALESCE(num_votes, '0'))),
				NOW()
			FROM ` + rawRatings + `
			WHERE tconst IN (SELECT tconst FROM ` + tables.Titles + `)
		`},
		{name: "load episodes", statement: `
			INSERT INTO ` + tables.TitleEpisodes + ` (tconst, parent_tconst, season_number, episode_number, row_hash, created_at)
			SELECT
				e.tconst,
				e.parent_tconst,
				NULLIF(e.season_number, '')::INTEGER,
				NULLIF(e.episode_number, '')::INTEGER,
				md5(concat_ws('|', e.tconst, e.parent_tconst, COALESCE(e.season_number, ''), COALESCE(e.episode_number, ''))),
				NOW()
			FROM ` + rawEpisodes + ` e
			JOIN ` + tables.Titles + ` child_title ON child_title.tconst = e.tconst
			JOIN ` + tables.Titles + ` parent_title ON parent_title.tconst = e.parent_tconst
		`},
		{name: "load principals", statement: `
			INSERT INTO ` + tables.TitlePrincipals + ` (tconst, ordering, nconst, category, job, characters, row_hash, created_at)
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
				md5(concat_ws('|', p.tconst, COALESCE(p.ordering, ''), p.nconst, p.category, COALESCE(p.job, ''), COALESCE(p.characters, ''))),
				NOW()
			FROM ` + rawPrincipals + ` p
			JOIN ` + tables.Titles + ` t ON t.tconst = p.tconst
			JOIN ` + tables.Names + ` n ON n.nconst = p.nconst
		`},
		{name: "load directors", statement: `
			INSERT INTO ` + tables.TitleCrewMembers + ` (tconst, nconst, role, ordering, row_hash, created_at)
			SELECT
				c.tconst,
				trim(member.nconst),
				'director',
				member.ord::INTEGER,
				md5(concat_ws('|', c.tconst, trim(member.nconst), 'director', member.ord::TEXT)),
				NOW()
			FROM ` + rawCrew + ` c
			JOIN ` + tables.Titles + ` t ON t.tconst = c.tconst
			CROSS JOIN LATERAL unnest(string_to_array(COALESCE(c.directors, ''), ',')) WITH ORDINALITY AS member(nconst, ord)
			JOIN ` + tables.Names + ` n ON n.nconst = trim(member.nconst)
			WHERE trim(member.nconst) <> ''
			ON CONFLICT DO NOTHING
		`},
		{name: "load writers", statement: `
			INSERT INTO ` + tables.TitleCrewMembers + ` (tconst, nconst, role, ordering, row_hash, created_at)
			SELECT
				c.tconst,
				trim(member.nconst),
				'writer',
				member.ord::INTEGER,
				md5(concat_ws('|', c.tconst, trim(member.nconst), 'writer', member.ord::TEXT)),
				NOW()
			FROM ` + rawCrew + ` c
			JOIN ` + tables.Titles + ` t ON t.tconst = c.tconst
			CROSS JOIN LATERAL unnest(string_to_array(COALESCE(c.writers, ''), ',')) WITH ORDINALITY AS member(nconst, ord)
			JOIN ` + tables.Names + ` n ON n.nconst = trim(member.nconst)
			WHERE trim(member.nconst) <> ''
			ON CONFLICT DO NOTHING
		`},
		{name: "load alternate titles", statement: `
			INSERT INTO ` + tables.TitleAkas + ` (tconst, ordering, title, region, language, types, attributes, is_original_title, row_hash, created_at)
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
				md5(concat_ws('|', a.title_id, COALESCE(a.ordering, ''), COALESCE(a.title, ''), COALESCE(a.region, ''), COALESCE(a.language, ''), COALESCE(a.types, ''), COALESCE(a.attributes, ''), COALESCE(a.is_original_title, '0'))),
				NOW()
			FROM ` + rawAkas + ` a
			JOIN ` + tables.Titles + ` t ON t.tconst = a.title_id
		`},
	}

	r.logf("imdb sync snapshot %d normalization started", snapshotID)
	for _, step := range steps {
		stepStarted := time.Now()
		tag, err := tx.Exec(ctx, step.statement)
		if err != nil {
			return counts, fmt.Errorf("%s: %w", step.name, err)
		}
		r.logf("imdb sync snapshot %d step complete: %s rows=%d duration=%s", snapshotID, step.name, tag.RowsAffected(), time.Since(stepStarted).Round(time.Second))
		switch step.name {
		case "load titles":
			counts.Titles = tag.RowsAffected()
		case "load names":
			counts.Names = tag.RowsAffected()
		case "load ratings":
			counts.Ratings = tag.RowsAffected()
		case "load episodes":
			counts.Episodes = tag.RowsAffected()
		case "load principals":
			counts.Principals = tag.RowsAffected()
		case "load directors", "load writers":
			counts.CrewMembers += tag.RowsAffected()
		case "load alternate titles":
			counts.AlternateIDs = tag.RowsAffected()
		}
	}

	return counts, nil
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
