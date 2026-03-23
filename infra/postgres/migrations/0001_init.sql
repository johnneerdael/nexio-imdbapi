CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS imdb_snapshots (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dataset_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    dataset_version TEXT,
    source_url TEXT,
    source_updated_at TIMESTAMPTZ,
    source_etag TEXT,
    notes TEXT NOT NULL DEFAULT '',
    imported_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    is_active BOOLEAN NOT NULL DEFAULT FALSE,
    title_count BIGINT NOT NULL DEFAULT 0,
    name_count BIGINT NOT NULL DEFAULT 0,
    rating_count BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dataset_sync_state (
    dataset_name TEXT PRIMARY KEY,
    source_url TEXT NOT NULL,
    etag TEXT,
    last_modified TEXT,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    imported_at TIMESTAMPTZ,
    snapshot_id BIGINT REFERENCES imdb_snapshots(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS titles (
    tconst TEXT PRIMARY KEY,
    snapshot_id BIGINT REFERENCES imdb_snapshots(id) ON DELETE SET NULL,
    title_type TEXT NOT NULL,
    primary_title TEXT NOT NULL,
    original_title TEXT NOT NULL,
    is_adult BOOLEAN NOT NULL DEFAULT FALSE,
    start_year INTEGER,
    end_year INTEGER,
    runtime_minutes INTEGER,
    genres TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS title_ratings (
    tconst TEXT PRIMARY KEY REFERENCES titles(tconst) ON DELETE CASCADE,
    average_rating NUMERIC(3,1) NOT NULL,
    num_votes INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS title_episodes (
    tconst TEXT PRIMARY KEY REFERENCES titles(tconst) ON DELETE CASCADE,
    parent_tconst TEXT NOT NULL REFERENCES titles(tconst) ON DELETE CASCADE,
    season_number INTEGER,
    episode_number INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS names (
    nconst TEXT PRIMARY KEY,
    snapshot_id BIGINT REFERENCES imdb_snapshots(id) ON DELETE SET NULL,
    primary_name TEXT NOT NULL,
    birth_year INTEGER,
    death_year INTEGER,
    primary_professions TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    known_for_titles TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS title_principals (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tconst TEXT NOT NULL REFERENCES titles(tconst) ON DELETE CASCADE,
    ordering INTEGER NOT NULL,
    nconst TEXT NOT NULL REFERENCES names(nconst) ON DELETE CASCADE,
    category TEXT NOT NULL,
    job TEXT,
    characters JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tconst, ordering)
);

CREATE TABLE IF NOT EXISTS title_crew_members (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tconst TEXT NOT NULL REFERENCES titles(tconst) ON DELETE CASCADE,
    nconst TEXT NOT NULL REFERENCES names(nconst) ON DELETE CASCADE,
    role TEXT NOT NULL,
    ordering INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tconst, nconst, role, ordering)
);

CREATE TABLE IF NOT EXISTS title_akas (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tconst TEXT NOT NULL REFERENCES titles(tconst) ON DELETE CASCADE,
    ordering INTEGER,
    title TEXT NOT NULL,
    region TEXT,
    language TEXT,
    types TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    attributes TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    is_original_title BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS users (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    display_name TEXT,
    avatar_url TEXT,
    google_sub TEXT UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMPTZ,
    disabled_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS web_sessions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    session_secret_hash TEXT NOT NULL UNIQUE,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at TIMESTAMPTZ,
    ip_hash TEXT,
    user_agent TEXT
);

CREATE TABLE IF NOT EXISTS api_keys (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
    name TEXT NOT NULL,
    key_prefix TEXT NOT NULL UNIQUE,
    key_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS bulk_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type TEXT NOT NULL,
    status TEXT NOT NULL,
    requested_by_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
    payload JSONB NOT NULL DEFAULT '{}'::JSONB,
    result JSONB NOT NULL DEFAULT '{}'::JSONB,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_titles_snapshot_id ON titles(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_titles_title_type ON titles(title_type);
CREATE INDEX IF NOT EXISTS idx_titles_start_year ON titles(start_year);
CREATE INDEX IF NOT EXISTS idx_titles_primary_title_lower ON titles (lower(primary_title));
CREATE INDEX IF NOT EXISTS idx_titles_search_primary_trgm ON titles USING GIN (lower(primary_title) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_titles_search_original_trgm ON titles USING GIN (lower(original_title) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_title_ratings_num_votes ON title_ratings(num_votes DESC);

CREATE INDEX IF NOT EXISTS idx_title_episodes_parent_tconst ON title_episodes(parent_tconst, season_number, episode_number);

CREATE INDEX IF NOT EXISTS idx_title_principals_tconst ON title_principals(tconst, ordering);
CREATE INDEX IF NOT EXISTS idx_title_principals_nconst ON title_principals(nconst, tconst);

CREATE INDEX IF NOT EXISTS idx_title_crew_members_tconst ON title_crew_members(tconst, role, ordering);
CREATE INDEX IF NOT EXISTS idx_title_crew_members_nconst ON title_crew_members(nconst, tconst);

CREATE INDEX IF NOT EXISTS idx_title_akas_tconst ON title_akas(tconst);
CREATE INDEX IF NOT EXISTS idx_title_akas_title_lower ON title_akas (lower(title));
CREATE INDEX IF NOT EXISTS idx_title_akas_search_title_trgm ON title_akas USING GIN (lower(title) gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_title_akas_region_language ON title_akas(region, language);

CREATE INDEX IF NOT EXISTS idx_names_search_primary_trgm ON names USING GIN (lower(primary_name) gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_web_sessions_user_id ON web_sessions(user_id);
CREATE INDEX IF NOT EXISTS idx_web_sessions_expires_at ON web_sessions(expires_at);

CREATE INDEX IF NOT EXISTS idx_api_keys_user_id ON api_keys(user_id);
CREATE INDEX IF NOT EXISTS idx_bulk_jobs_status ON bulk_jobs(status, created_at DESC);

CREATE OR REPLACE VIEW title_search AS
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
LEFT JOIN title_ratings r ON r.tconst = t.tconst;

CREATE OR REPLACE VIEW series_episode_ratings AS
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
LEFT JOIN title_ratings r ON r.tconst = e.tconst;
