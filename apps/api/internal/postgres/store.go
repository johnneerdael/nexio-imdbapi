package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"nexio-imdb/apps/api/internal/auth"
	"nexio-imdb/apps/api/internal/imdb"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *Store) GetAPIKeyByPrefix(ctx context.Context, prefix string) (auth.KeyRecord, error) {
	var record auth.KeyRecord
	err := s.pool.QueryRow(ctx, `
		SELECT id, user_id, name, key_prefix, key_hash, revoked_at, expires_at
		FROM api_keys
		WHERE key_prefix = $1
		LIMIT 1
	`, prefix).Scan(
		&record.ID,
		&record.UserID,
		&record.Name,
		&record.Prefix,
		&record.Hash,
		&record.RevokedAt,
		&record.ExpiresAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return auth.KeyRecord{}, auth.ErrInvalidAPIKey
		}
		return auth.KeyRecord{}, fmt.Errorf("get api key by prefix: %w", err)
	}
	return record, nil
}

func (s *Store) TouchAPIKeyLastUsed(ctx context.Context, id int64, lastUsedAt time.Time) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE api_keys
		SET last_used_at = $2
		WHERE id = $1
	`, id, lastUsedAt.UTC())
	if err != nil {
		return fmt.Errorf("touch api key last used: %w", err)
	}
	return nil
}

func (s *Store) ListSnapshots(ctx context.Context) ([]imdb.Snapshot, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			id,
			dataset_name,
			status,
			sync_mode,
			dataset_version,
			imported_at,
			source_updated_at,
			source_etag,
			is_active,
			title_count,
			name_count,
			rating_count,
			episode_count,
			principal_count,
			crew_member_count,
			aka_count,
			notes,
			source_url,
			completed_at
		FROM imdb_snapshots
		ORDER BY imported_at DESC, id DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("list snapshots: %w", err)
	}
	defer rows.Close()

	var items []imdb.Snapshot
	for rows.Next() {
		var item imdb.Snapshot
		if err := rows.Scan(
			&item.ID,
			&item.Dataset,
			&item.Status,
			&item.SyncMode,
			&item.DatasetVersion,
			&item.ImportedAt,
			&item.SourceUpdatedAt,
			&item.SourceETag,
			&item.IsActive,
			&item.TitleCount,
			&item.NameCount,
			&item.RatingCount,
			&item.EpisodeCount,
			&item.PrincipalCount,
			&item.CrewMemberCount,
			&item.AKACount,
			&item.Notes,
			&item.SourceURL,
			&item.CompletedAt,
		); err != nil {
			return nil, fmt.Errorf("scan snapshot: %w", err)
		}
		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate snapshots: %w", err)
	}
	return items, nil
}

func (s *Store) GetStats(ctx context.Context) (imdb.Stats, error) {
	var stats imdb.Stats
	err := s.pool.QueryRow(ctx, `
		WITH active_snapshot AS (
			SELECT title_count, name_count, rating_count, episode_count, principal_count, crew_member_count, aka_count
			FROM imdb_snapshots
			WHERE is_active = TRUE
			ORDER BY imported_at DESC, id DESC
			LIMIT 1
		),
		table_estimates AS (
			SELECT
				COALESCE(MAX(CASE WHEN relname = 'title_episodes' THEN n_live_tup::bigint END), 0) AS episodes,
				COALESCE(MAX(CASE WHEN relname = 'title_principals' THEN n_live_tup::bigint END), 0) AS principals,
				COALESCE(MAX(CASE WHEN relname = 'title_crew_members' THEN n_live_tup::bigint END), 0) AS crew_members,
				COALESCE(MAX(CASE WHEN relname = 'title_akas' THEN n_live_tup::bigint END), 0) AS akas
			FROM pg_stat_all_tables
			WHERE schemaname = 'public'
			  AND relname IN ('title_episodes', 'title_principals', 'title_crew_members', 'title_akas')
		)
		SELECT
			COALESCE((SELECT title_count FROM active_snapshot), 0) AS titles,
			COALESCE((SELECT name_count FROM active_snapshot), 0) AS names,
			COALESCE((SELECT rating_count FROM active_snapshot), 0) AS ratings,
			COALESCE(NULLIF((SELECT episode_count FROM active_snapshot), 0), (SELECT episodes FROM table_estimates), 0) AS episodes,
			COALESCE(NULLIF((SELECT principal_count FROM active_snapshot), 0), (SELECT principals FROM table_estimates), 0) AS principals,
			COALESCE(NULLIF((SELECT crew_member_count FROM active_snapshot), 0), (SELECT crew_members FROM table_estimates), 0) AS crew_members,
			COALESCE(NULLIF((SELECT aka_count FROM active_snapshot), 0), (SELECT akas FROM table_estimates), 0) AS akas,
			(SELECT COUNT(*) FROM imdb_snapshots) AS snapshots
	`).Scan(
		&stats.Titles,
		&stats.Names,
		&stats.Ratings,
		&stats.Episodes,
		&stats.Principals,
		&stats.CrewMembers,
		&stats.Akas,
		&stats.Snapshots,
	)
	if err != nil {
		return imdb.Stats{}, fmt.Errorf("get stats: %w", err)
	}
	return stats, nil
}

func (s *Store) GetTitle(ctx context.Context, tconst string) (imdb.TitleDetail, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT
			t.tconst,
			t.title_type,
			t.primary_title,
			t.original_title,
			t.is_adult,
			t.start_year,
			t.end_year,
			t.runtime_minutes,
			t.genres,
			r.average_rating,
			r.num_votes,
			e.parent_tconst,
			e.season_number,
			e.episode_number
		FROM titles t
		LEFT JOIN title_ratings r ON r.tconst = t.tconst
		LEFT JOIN title_episodes e ON e.tconst = t.tconst
		WHERE t.tconst = $1
	`, tconst)

	title, err := scanTitleDetail(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return imdb.TitleDetail{}, imdb.ErrNotFound
		}
		return imdb.TitleDetail{}, fmt.Errorf("get title: %w", err)
	}
	return title, nil
}

func (s *Store) FindResolveCandidates(ctx context.Context, title string) ([]imdb.ResolveCandidate, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			t.tconst,
			t.title_type,
			t.primary_title,
			t.original_title,
			t.is_adult,
			t.start_year,
			t.end_year,
			t.runtime_minutes,
			t.genres,
			COALESCE(r.num_votes, 0) AS num_votes,
			MAX(CASE WHEN lower(a.title) = lower($1) THEN a.title END) AS matched_alias
		FROM titles t
		LEFT JOIN title_akas a ON a.tconst = t.tconst
		LEFT JOIN title_ratings r ON r.tconst = t.tconst
		WHERE lower(t.primary_title) = lower($1)
		   OR lower(a.title) = lower($1)
		GROUP BY
			t.tconst, t.title_type, t.primary_title, t.original_title, t.is_adult,
			t.start_year, t.end_year, t.runtime_minutes, t.genres, r.num_votes
		ORDER BY COALESCE(r.num_votes, 0) DESC, t.start_year DESC NULLS LAST, t.tconst
	`, title)
	if err != nil {
		return nil, fmt.Errorf("find resolve candidates: %w", err)
	}
	defer rows.Close()

	var candidates []imdb.ResolveCandidate
	for rows.Next() {
		summary, numVotes, matchedAlias, err := scanResolveCandidate(rows)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, imdb.ResolveCandidate{
			Title:        summary,
			MatchedAlias: matchedAlias,
			NumVotes:     numVotes,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate resolve candidates: %w", err)
	}
	return candidates, nil
}

func (s *Store) SearchTitles(ctx context.Context, params imdb.SearchTitlesParams) ([]imdb.TitleSearchResult, error) {
	var (
		args = []any{"%" + params.Query + "%", strings.ToLower(params.Query), params.IncludeAkas}
		cond = []string{
			"(lower(t.primary_title) LIKE lower($1) OR lower(t.original_title) LIKE lower($1) OR ($3 AND lower(COALESCE(a.title, '')) LIKE lower($1)))",
		}
	)

	if params.TitleType != "" {
		args = append(args, params.TitleType)
		cond = append(cond, fmt.Sprintf("t.title_type = $%d", len(args)))
	}
	if params.Year != nil {
		args = append(args, *params.Year)
		cond = append(cond, fmt.Sprintf("t.start_year = $%d", len(args)))
	}

	query := fmt.Sprintf(`
		SELECT
			t.tconst,
			t.title_type,
			t.primary_title,
			t.original_title,
			t.is_adult,
			t.start_year,
			t.end_year,
			t.runtime_minutes,
			t.genres,
			r.average_rating,
			r.num_votes,
			MAX(
				CASE
					WHEN $3 AND lower(a.title) LIKE lower($1) THEN a.title
					ELSE NULL
				END
			) AS matched_alias
		FROM titles t
		LEFT JOIN title_akas a ON a.tconst = t.tconst
		LEFT JOIN title_ratings r ON r.tconst = t.tconst
		WHERE %s
		GROUP BY
			t.tconst, t.title_type, t.primary_title, t.original_title, t.is_adult,
			t.start_year, t.end_year, t.runtime_minutes, t.genres, r.average_rating, r.num_votes
		ORDER BY
			MAX(
				CASE
					WHEN lower(t.primary_title) = $2 THEN 500
					WHEN $3 AND lower(a.title) = $2 THEN 450
					WHEN lower(t.primary_title) LIKE lower($1) THEN 350
					WHEN lower(t.original_title) LIKE lower($1) THEN 250
					WHEN $3 AND lower(a.title) LIKE lower($1) THEN 200
					ELSE 0
				END
			) DESC,
			COALESCE(r.num_votes, 0) DESC,
			t.start_year DESC NULLS LAST,
			t.primary_title ASC
		LIMIT 50
	`, strings.Join(cond, " AND "))

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("search titles: %w", err)
	}
	defer rows.Close()

	var items []imdb.TitleSearchResult
	for rows.Next() {
		item, err := scanTitleSearchResult(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate title search: %w", err)
	}
	return items, nil
}

func (s *Store) GetRating(ctx context.Context, tconst string) (imdb.Rating, error) {
	var rating imdb.Rating
	err := s.pool.QueryRow(ctx, `
		SELECT tconst, average_rating, num_votes
		FROM title_ratings
		WHERE tconst = $1
	`, tconst).Scan(&rating.Tconst, &rating.AverageRating, &rating.NumVotes)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return imdb.Rating{}, imdb.ErrNotFound
		}
		return imdb.Rating{}, fmt.Errorf("get rating: %w", err)
	}
	return rating, nil
}

func (s *Store) GetSeriesEpisodeRatings(ctx context.Context, tconst string) ([]imdb.EpisodeRating, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			e.tconst,
			e.parent_tconst,
			t.primary_title,
			e.season_number,
			e.episode_number,
			r.average_rating,
			r.num_votes
		FROM title_episodes e
		JOIN titles t ON t.tconst = e.tconst
		LEFT JOIN title_ratings r ON r.tconst = e.tconst
		WHERE e.parent_tconst = $1
		ORDER BY e.season_number NULLS FIRST, e.episode_number NULLS FIRST, e.tconst
	`, tconst)
	if err != nil {
		return nil, fmt.Errorf("get series episode ratings: %w", err)
	}
	defer rows.Close()

	var items []imdb.EpisodeRating
	for rows.Next() {
		var item imdb.EpisodeRating
		if err := rows.Scan(
			&item.Tconst,
			&item.ParentTconst,
			&item.PrimaryTitle,
			&item.SeasonNumber,
			&item.EpisodeNumber,
			&item.AverageRating,
			&item.NumVotes,
		); err != nil {
			return nil, fmt.Errorf("scan episode rating: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate episode ratings: %w", err)
	}
	return items, nil
}

func (s *Store) GetSeriesEpisodes(ctx context.Context, tconst string) ([]imdb.EpisodeDetail, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			t.tconst,
			t.title_type,
			t.primary_title,
			t.original_title,
			t.is_adult,
			t.start_year,
			t.end_year,
			t.runtime_minutes,
			t.genres,
			e.parent_tconst,
			e.season_number,
			e.episode_number,
			r.average_rating,
			r.num_votes
		FROM title_episodes e
		JOIN titles t ON t.tconst = e.tconst
		LEFT JOIN title_ratings r ON r.tconst = e.tconst
		WHERE e.parent_tconst = $1
		ORDER BY e.season_number NULLS FIRST, e.episode_number NULLS FIRST, e.tconst
	`, tconst)
	if err != nil {
		return nil, fmt.Errorf("get series episodes: %w", err)
	}
	defer rows.Close()

	var items []imdb.EpisodeDetail
	for rows.Next() {
		item, err := scanEpisodeDetail(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate series episodes: %w", err)
	}
	return items, nil
}

func (s *Store) GetEpisode(ctx context.Context, tconst string) (imdb.EpisodeDetail, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT
			t.tconst,
			t.title_type,
			t.primary_title,
			t.original_title,
			t.is_adult,
			t.start_year,
			t.end_year,
			t.runtime_minutes,
			t.genres,
			e.parent_tconst,
			e.season_number,
			e.episode_number,
			r.average_rating,
			r.num_votes
		FROM title_episodes e
		JOIN titles t ON t.tconst = e.tconst
		LEFT JOIN title_ratings r ON r.tconst = e.tconst
		WHERE e.tconst = $1
	`, tconst)

	item, err := scanEpisodeDetail(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return imdb.EpisodeDetail{}, imdb.ErrNotFound
		}
		return imdb.EpisodeDetail{}, fmt.Errorf("get episode: %w", err)
	}
	return item, nil
}

func (s *Store) GetTitlePrincipals(ctx context.Context, tconst string) ([]imdb.Principal, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			p.ordering,
			p.nconst,
			n.primary_name,
			p.category,
			p.job,
			p.characters,
			n.birth_year,
			n.death_year
		FROM title_principals p
		JOIN names n ON n.nconst = p.nconst
		WHERE p.tconst = $1
		ORDER BY p.ordering
	`, tconst)
	if err != nil {
		return nil, fmt.Errorf("get title principals: %w", err)
	}
	defer rows.Close()

	var items []imdb.Principal
	for rows.Next() {
		var (
			item       imdb.Principal
			characters []byte
		)
		if err := rows.Scan(
			&item.Ordering,
			&item.Nconst,
			&item.Name,
			&item.Category,
			&item.Job,
			&characters,
			&item.BirthYear,
			&item.DeathYear,
		); err != nil {
			return nil, fmt.Errorf("scan principal: %w", err)
		}
		if len(characters) > 0 {
			if err := json.Unmarshal(characters, &item.Characters); err != nil {
				return nil, fmt.Errorf("decode principal characters: %w", err)
			}
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate principals: %w", err)
	}
	return items, nil
}

func (s *Store) GetTitleCrew(ctx context.Context, tconst string) ([]imdb.CrewMember, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			c.nconst,
			n.primary_name,
			c.role,
			c.ordering,
			n.birth_year,
			n.death_year
		FROM title_crew_members c
		JOIN names n ON n.nconst = c.nconst
		WHERE c.tconst = $1
		ORDER BY c.role, c.ordering NULLS LAST, c.nconst
	`, tconst)
	if err != nil {
		return nil, fmt.Errorf("get title crew: %w", err)
	}
	defer rows.Close()

	var items []imdb.CrewMember
	for rows.Next() {
		var item imdb.CrewMember
		if err := rows.Scan(
			&item.Nconst,
			&item.Name,
			&item.Role,
			&item.Ordering,
			&item.BirthYear,
			&item.DeathYear,
		); err != nil {
			return nil, fmt.Errorf("scan crew member: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate crew members: %w", err)
	}
	return items, nil
}

func (s *Store) GetTitleCredits(ctx context.Context, tconst string) (imdb.Credits, error) {
	principals, err := s.GetTitlePrincipals(ctx, tconst)
	if err != nil {
		return imdb.Credits{}, err
	}
	crew, err := s.GetTitleCrew(ctx, tconst)
	if err != nil {
		return imdb.Credits{}, err
	}
	return imdb.Credits{
		Principals: principals,
		Crew:       crew,
	}, nil
}

func (s *Store) GetName(ctx context.Context, nconst string) (imdb.NameDetail, error) {
	var item imdb.NameDetail
	err := s.pool.QueryRow(ctx, `
		SELECT nconst, primary_name, birth_year, death_year, primary_professions, known_for_titles
		FROM names
		WHERE nconst = $1
	`, nconst).Scan(
		&item.Nconst,
		&item.PrimaryName,
		&item.BirthYear,
		&item.DeathYear,
		&item.PrimaryProfessions,
		&item.KnownForTitles,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return imdb.NameDetail{}, imdb.ErrNotFound
		}
		return imdb.NameDetail{}, fmt.Errorf("get name: %w", err)
	}
	return item, nil
}

func (s *Store) SearchNames(ctx context.Context, q string) ([]imdb.NameSearchResult, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT nconst, primary_name, birth_year, death_year, primary_professions
		FROM names
		WHERE lower(primary_name) LIKE lower($1)
		ORDER BY
			CASE
				WHEN lower(primary_name) = lower($2) THEN 2
				ELSE 1
			END DESC,
			primary_name ASC
		LIMIT 50
	`, "%"+q+"%", q)
	if err != nil {
		return nil, fmt.Errorf("search names: %w", err)
	}
	defer rows.Close()

	var items []imdb.NameSearchResult
	for rows.Next() {
		var item imdb.NameSearchResult
		if err := rows.Scan(
			&item.Nconst,
			&item.PrimaryName,
			&item.BirthYear,
			&item.DeathYear,
			&item.PrimaryProfessions,
		); err != nil {
			return nil, fmt.Errorf("scan searched name: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate searched names: %w", err)
	}
	return items, nil
}

func (s *Store) GetNameTitles(ctx context.Context, nconst string) ([]imdb.NameTitleCredit, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT
			t.tconst,
			t.title_type,
			t.primary_title,
			t.original_title,
			t.is_adult,
			t.start_year,
			t.end_year,
			t.runtime_minutes,
			t.genres,
			COALESCE(p.categories, ARRAY[]::text[]),
			COALESCE(c.roles, ARRAY[]::text[])
		FROM (
			SELECT DISTINCT tconst FROM title_principals WHERE nconst = $1
			UNION
			SELECT DISTINCT tconst FROM title_crew_members WHERE nconst = $1
		) credits
		JOIN titles t ON t.tconst = credits.tconst
		LEFT JOIN (
			SELECT tconst, array_agg(DISTINCT category ORDER BY category) AS categories
			FROM title_principals
			WHERE nconst = $1
			GROUP BY tconst
		) p ON p.tconst = t.tconst
		LEFT JOIN (
			SELECT tconst, array_agg(DISTINCT role ORDER BY role) AS roles
			FROM title_crew_members
			WHERE nconst = $1
			GROUP BY tconst
		) c ON c.tconst = t.tconst
		ORDER BY t.start_year DESC NULLS LAST, t.primary_title ASC
	`, nconst)
	if err != nil {
		return nil, fmt.Errorf("get name titles: %w", err)
	}
	defer rows.Close()

	var items []imdb.NameTitleCredit
	for rows.Next() {
		var (
			item imdb.NameTitleCredit
		)
		if err := rows.Scan(
			&item.Title.Tconst,
			&item.Title.TitleType,
			&item.Title.PrimaryTitle,
			&item.Title.OriginalTitle,
			&item.Title.IsAdult,
			&item.Title.StartYear,
			&item.Title.EndYear,
			&item.Title.RuntimeMinutes,
			&item.Title.Genres,
			&item.Categories,
			&item.Roles,
		); err != nil {
			return nil, fmt.Errorf("scan name title credit: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate name title credits: %w", err)
	}
	return items, nil
}

func (s *Store) GetTitleAkas(ctx context.Context, tconst string) ([]imdb.AKA, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, tconst, title, region, language, types, attributes, is_original_title
		FROM title_akas
		WHERE tconst = $1
		ORDER BY is_original_title DESC, ordering NULLS LAST, id
	`, tconst)
	if err != nil {
		return nil, fmt.Errorf("get title akas: %w", err)
	}
	defer rows.Close()

	return scanAKAList(rows)
}

func (s *Store) SearchAkas(ctx context.Context, params imdb.SearchAkasParams) ([]imdb.AKA, error) {
	args := []any{"%" + params.Title + "%", params.Title}
	cond := []string{"lower(title) LIKE lower($1)"}

	if params.Region != "" {
		args = append(args, params.Region)
		cond = append(cond, fmt.Sprintf("region = $%d", len(args)))
	}
	if params.Language != "" {
		args = append(args, params.Language)
		cond = append(cond, fmt.Sprintf("language = $%d", len(args)))
	}

	query := fmt.Sprintf(`
		SELECT id, tconst, title, region, language, types, attributes, is_original_title
		FROM title_akas
		WHERE %s
		ORDER BY
			CASE
				WHEN lower(title) = lower($2) THEN 2
				ELSE 1
			END DESC,
			title ASC
		LIMIT 50
	`, strings.Join(cond, " AND "))

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("search akas: %w", err)
	}
	defer rows.Close()

	return scanAKAList(rows)
}

func (s *Store) CreateBulkJob(ctx context.Context, params imdb.CreateBulkJobParams) (imdb.BulkJob, error) {
	var job imdb.BulkJob
	err := s.pool.QueryRow(ctx, `
		INSERT INTO bulk_jobs (
			id,
			job_type,
			status,
			requested_by_user_id,
			payload,
			result,
			error_message,
			created_at,
			updated_at,
			expires_at,
			completed_at
		)
		VALUES ($1::uuid, $2, $3, $4, $5::jsonb, $6::jsonb, $7, NOW(), NOW(), $8, $9)
		RETURNING id::text, job_type, status, created_at, updated_at, expires_at, COALESCE(error_message, '')
	`, params.ID, params.Operation, params.Status, params.RequestedByUserID, string(params.Payload), string(params.Result), params.ErrorMessage, params.ExpiresAt, params.CompletedAt).Scan(
		&job.ID,
		&job.Operation,
		&job.Status,
		&job.CreatedAt,
		&job.UpdatedAt,
		&job.ExpiresAt,
		&job.ErrorMessage,
	)
	if err != nil {
		return imdb.BulkJob{}, fmt.Errorf("create bulk job: %w", err)
	}
	if job.Status == "succeeded" {
		job.ResultURL = fmt.Sprintf("/v1/bulk/jobs/%s/result", job.ID)
	}
	return job, nil
}

func (s *Store) GetBulkJob(ctx context.Context, id string) (imdb.BulkJob, error) {
	var (
		job imdb.BulkJob
	)
	err := s.pool.QueryRow(ctx, `
		SELECT id::text, job_type, status, created_at, updated_at, COALESCE(expires_at, updated_at), COALESCE(error_message, '')
		FROM bulk_jobs
		WHERE id = $1::uuid
	`, id).Scan(&job.ID, &job.Operation, &job.Status, &job.CreatedAt, &job.UpdatedAt, &job.ExpiresAt, &job.ErrorMessage)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return imdb.BulkJob{}, imdb.ErrNotFound
		}
		return imdb.BulkJob{}, fmt.Errorf("get bulk job: %w", err)
	}
	if job.Status == "succeeded" {
		job.ResultURL = fmt.Sprintf("/v1/bulk/jobs/%s/result", job.ID)
	}
	return job, nil
}

func (s *Store) GetBulkJobResult(ctx context.Context, id string) (imdb.BulkJobResult, error) {
	var (
		result imdb.BulkJobResult
		raw    []byte
	)
	err := s.pool.QueryRow(ctx, `
		SELECT id::text, job_type, status, created_at, updated_at, COALESCE(expires_at, updated_at), COALESCE(error_message, ''), result
		FROM bulk_jobs
		WHERE id = $1::uuid
	`, id).Scan(&result.Job.ID, &result.Job.Operation, &result.Job.Status, &result.Job.CreatedAt, &result.Job.UpdatedAt, &result.Job.ExpiresAt, &result.Job.ErrorMessage, &raw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return imdb.BulkJobResult{}, imdb.ErrNotFound
		}
		return imdb.BulkJobResult{}, fmt.Errorf("get bulk job result: %w", err)
	}
	if result.Job.Status == "succeeded" {
		result.Job.ResultURL = fmt.Sprintf("/v1/bulk/jobs/%s/result", result.Job.ID)
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &result.Result); err != nil {
			return imdb.BulkJobResult{}, fmt.Errorf("decode bulk result: %w", err)
		}
	}
	return result, nil
}

type BulkJobRecord struct {
	ID        string
	Operation string
	Payload   []byte
}

func (s *Store) ClaimNextBulkJob(ctx context.Context) (BulkJobRecord, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return BulkJobRecord{}, false, fmt.Errorf("begin claim bulk job tx: %w", err)
	}
	defer tx.Rollback(ctx)

	var record BulkJobRecord
	err = tx.QueryRow(ctx, `
		SELECT id::text, job_type, payload::text
		FROM bulk_jobs
		WHERE status = 'queued'
		  AND (expires_at IS NULL OR expires_at > NOW())
		ORDER BY created_at ASC
		FOR UPDATE SKIP LOCKED
		LIMIT 1
	`).Scan(&record.ID, &record.Operation, &record.Payload)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return BulkJobRecord{}, false, nil
		}
		return BulkJobRecord{}, false, fmt.Errorf("select queued bulk job: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE bulk_jobs
		SET status = 'running', started_at = NOW(), updated_at = NOW(), error_message = NULL
		WHERE id = $1::uuid
	`, record.ID); err != nil {
		return BulkJobRecord{}, false, fmt.Errorf("mark bulk job running: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return BulkJobRecord{}, false, fmt.Errorf("commit claim bulk job tx: %w", err)
	}

	return record, true, nil
}

func (s *Store) CompleteBulkJob(ctx context.Context, id string, result []byte) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE bulk_jobs
		SET status = 'succeeded',
			result = $2::jsonb,
			error_message = NULL,
			updated_at = NOW(),
			completed_at = NOW()
		WHERE id = $1::uuid
	`, id, string(result))
	if err != nil {
		return fmt.Errorf("complete bulk job: %w", err)
	}
	return nil
}

func (s *Store) FailBulkJob(ctx context.Context, id string, message string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE bulk_jobs
		SET status = 'failed',
			error_message = $2,
			updated_at = NOW(),
			completed_at = NOW()
		WHERE id = $1::uuid
	`, id, message)
	if err != nil {
		return fmt.Errorf("fail bulk job: %w", err)
	}
	return nil
}

func scanResolveCandidate(row interface{ Scan(...any) error }) (imdb.TitleSummary, int, string, error) {
	var (
		summary      imdb.TitleSummary
		numVotes     int
		matchedAlias *string
	)
	if err := row.Scan(
		&summary.Tconst,
		&summary.TitleType,
		&summary.PrimaryTitle,
		&summary.OriginalTitle,
		&summary.IsAdult,
		&summary.StartYear,
		&summary.EndYear,
		&summary.RuntimeMinutes,
		&summary.Genres,
		&numVotes,
		&matchedAlias,
	); err != nil {
		return imdb.TitleSummary{}, 0, "", fmt.Errorf("scan resolve candidate: %w", err)
	}
	if matchedAlias != nil {
		return summary, numVotes, *matchedAlias, nil
	}
	return summary, numVotes, "", nil
}

func scanTitleSearchResult(row interface{ Scan(...any) error }) (imdb.TitleSearchResult, error) {
	var (
		result        imdb.TitleSearchResult
		averageRating *float64
		numVotes      *int
		matchedAlias  *string
	)
	if err := row.Scan(
		&result.Title.Tconst,
		&result.Title.TitleType,
		&result.Title.PrimaryTitle,
		&result.Title.OriginalTitle,
		&result.Title.IsAdult,
		&result.Title.StartYear,
		&result.Title.EndYear,
		&result.Title.RuntimeMinutes,
		&result.Title.Genres,
		&averageRating,
		&numVotes,
		&matchedAlias,
	); err != nil {
		return imdb.TitleSearchResult{}, fmt.Errorf("scan title search result: %w", err)
	}
	if averageRating != nil && numVotes != nil {
		result.Rating = &imdb.Rating{
			Tconst:        result.Title.Tconst,
			AverageRating: *averageRating,
			NumVotes:      *numVotes,
		}
	}
	if matchedAlias != nil {
		result.MatchedAlias = *matchedAlias
	}
	return result, nil
}

func scanTitleDetail(row interface{ Scan(...any) error }) (imdb.TitleDetail, error) {
	var (
		item          imdb.TitleDetail
		averageRating *float64
		numVotes      *int
		parentTconst  *string
		seasonNumber  *int
		episodeNumber *int
	)
	if err := row.Scan(
		&item.Title.Tconst,
		&item.Title.TitleType,
		&item.Title.PrimaryTitle,
		&item.Title.OriginalTitle,
		&item.Title.IsAdult,
		&item.Title.StartYear,
		&item.Title.EndYear,
		&item.Title.RuntimeMinutes,
		&item.Title.Genres,
		&averageRating,
		&numVotes,
		&parentTconst,
		&seasonNumber,
		&episodeNumber,
	); err != nil {
		return imdb.TitleDetail{}, err
	}
	if averageRating != nil && numVotes != nil {
		item.Rating = &imdb.Rating{
			Tconst:        item.Title.Tconst,
			AverageRating: *averageRating,
			NumVotes:      *numVotes,
		}
	}
	if parentTconst != nil {
		item.EpisodeInfo = &imdb.EpisodeInfo{
			ParentTconst:  *parentTconst,
			SeasonNumber:  seasonNumber,
			EpisodeNumber: episodeNumber,
		}
	}
	return item, nil
}

func scanEpisodeDetail(row interface{ Scan(...any) error }) (imdb.EpisodeDetail, error) {
	var (
		item          imdb.EpisodeDetail
		averageRating *float64
		numVotes      *int
	)
	if err := row.Scan(
		&item.Title.Tconst,
		&item.Title.TitleType,
		&item.Title.PrimaryTitle,
		&item.Title.OriginalTitle,
		&item.Title.IsAdult,
		&item.Title.StartYear,
		&item.Title.EndYear,
		&item.Title.RuntimeMinutes,
		&item.Title.Genres,
		&item.EpisodeInfo.ParentTconst,
		&item.EpisodeInfo.SeasonNumber,
		&item.EpisodeInfo.EpisodeNumber,
		&averageRating,
		&numVotes,
	); err != nil {
		return imdb.EpisodeDetail{}, fmt.Errorf("scan episode detail: %w", err)
	}
	if averageRating != nil && numVotes != nil {
		item.Rating = &imdb.Rating{
			Tconst:        item.Title.Tconst,
			AverageRating: *averageRating,
			NumVotes:      *numVotes,
		}
	}
	return item, nil
}

func scanAKAList(rows pgx.Rows) ([]imdb.AKA, error) {
	var items []imdb.AKA
	for rows.Next() {
		var item imdb.AKA
		if err := rows.Scan(
			&item.ID,
			&item.Tconst,
			&item.Title,
			&item.Region,
			&item.Language,
			&item.Types,
			&item.Attributes,
			&item.IsOriginalTitle,
		); err != nil {
			return nil, fmt.Errorf("scan aka: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate akas: %w", err)
	}
	return items, nil
}
