package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"nexio-imdb/apps/api/internal/auth"
	"nexio-imdb/apps/api/internal/imdb"
)

func TestHealthzDoesNotRequireAuth(t *testing.T) {
	t.Parallel()

	router := NewRouter(stubService{}, stubAuthenticator{})
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("expected status ok, got %#v", body)
	}
}

func TestV1RoutesRequireAPIKey(t *testing.T) {
	t.Parallel()

	router := NewRouter(stubService{}, stubAuthenticator{})
	req := httptest.NewRequest(http.MethodGet, "/v1/meta/stats", nil)
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestGetTitleSupportsXAPIKey(t *testing.T) {
	t.Parallel()

	router := NewRouter(stubService{
		getTitle: func(ctx context.Context, tconst string) (imdb.TitleDetail, error) {
			return imdb.TitleDetail{
				Title: imdb.TitleSummary{
					Tconst:       tconst,
					TitleType:    "movie",
					PrimaryTitle: "Inception",
				},
			}, nil
		},
	}, stubAuthenticator{
		authenticate: func(ctx context.Context, key string) (*auth.Principal, error) {
			if key != "valid-key" {
				t.Fatalf("unexpected key %q", key)
			}
			return &auth.Principal{KeyID: 1, Prefix: "test"}, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/titles/tt1375666", nil)
	req.Header.Set("X-API-Key", "valid-key")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"tconst":"tt1375666"`) {
		t.Fatalf("expected title response, got %s", rec.Body.String())
	}
}

func TestResolveTitleReturnsResolutionMetadata(t *testing.T) {
	t.Parallel()

	year := 2025
	router := NewRouter(stubService{
		resolveTitle: func(ctx context.Context, params imdb.ResolveTitleParams) (imdb.ResolveTitleResult, error) {
			if params.Title != "Paradise" {
				t.Fatalf("unexpected title %q", params.Title)
			}
			if params.TitleType != "tvSeries" {
				t.Fatalf("unexpected titleType %q", params.TitleType)
			}
			if params.Year == nil || *params.Year != 2025 {
				t.Fatalf("unexpected year %v", params.Year)
			}
			return imdb.ResolveTitleResult{
				Title: imdb.TitleDetail{
					Title: imdb.TitleSummary{
						Tconst:       "tt27444205",
						PrimaryTitle: "Paradise",
						TitleType:    "tvSeries",
						StartYear:    &year,
					},
				},
				Resolution: imdb.ResolutionMetadata{
					InputTitle:     "Paradise",
					ResolvedTconst: "tt27444205",
					MatchReason:    imdb.MatchReasonExactPrimaryTitleWithFilters,
					AppliedFilters: imdb.ResolutionFilters{
						TitleType: "tvSeries",
						Year:      &year,
					},
				},
			}, nil
		},
	}, stubAuthenticator{
		authenticate: func(ctx context.Context, key string) (*auth.Principal, error) {
			return &auth.Principal{KeyID: 1, Prefix: "test"}, nil
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/titles/resolve?title=Paradise&titleType=tvSeries&year=2025", nil)
	req.Header.Set("Authorization", "Bearer valid-key")
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"matchReason":"exact_primary_title_with_filters"`) {
		t.Fatalf("expected resolution metadata, got %s", rec.Body.String())
	}
}

type stubService struct {
	ready               func(context.Context) error
	listSnapshots       func(context.Context) ([]imdb.Snapshot, error)
	getStats            func(context.Context) (imdb.Stats, error)
	getTitle            func(context.Context, string) (imdb.TitleDetail, error)
	resolveTitle        func(context.Context, imdb.ResolveTitleParams) (imdb.ResolveTitleResult, error)
	searchTitles        func(context.Context, imdb.SearchTitlesParams) ([]imdb.TitleSearchResult, error)
	getRating           func(context.Context, string) (imdb.Rating, error)
	getSeriesRatings    func(context.Context, string) ([]imdb.EpisodeRating, error)
	resolveSeriesRating func(context.Context, imdb.ResolveTitleParams) (imdb.SeriesEpisodeRatingsResult, error)
	getSeriesEpisodes   func(context.Context, string) ([]imdb.EpisodeDetail, error)
	getEpisode          func(context.Context, string) (imdb.EpisodeDetail, error)
	getCredits          func(context.Context, string) (imdb.Credits, error)
	getPrincipals       func(context.Context, string) ([]imdb.Principal, error)
	getCrew             func(context.Context, string) ([]imdb.CrewMember, error)
	getName             func(context.Context, string) (imdb.NameDetail, error)
	searchNames         func(context.Context, string) ([]imdb.NameSearchResult, error)
	getNameTitles       func(context.Context, string) ([]imdb.NameTitleCredit, error)
	getTitleAkas        func(context.Context, string) ([]imdb.AKA, error)
	searchAkas          func(context.Context, imdb.SearchAkasParams) ([]imdb.AKA, error)
}

func (s stubService) Ready(ctx context.Context) error {
	if s.ready != nil {
		return s.ready(ctx)
	}
	return nil
}

func (s stubService) ListSnapshots(ctx context.Context) ([]imdb.Snapshot, error) {
	if s.listSnapshots != nil {
		return s.listSnapshots(ctx)
	}
	return nil, nil
}

func (s stubService) GetStats(ctx context.Context) (imdb.Stats, error) {
	if s.getStats != nil {
		return s.getStats(ctx)
	}
	return imdb.Stats{}, nil
}

func (s stubService) GetTitle(ctx context.Context, tconst string) (imdb.TitleDetail, error) {
	if s.getTitle != nil {
		return s.getTitle(ctx, tconst)
	}
	return imdb.TitleDetail{}, nil
}

func (s stubService) ResolveTitle(ctx context.Context, params imdb.ResolveTitleParams) (imdb.ResolveTitleResult, error) {
	if s.resolveTitle != nil {
		return s.resolveTitle(ctx, params)
	}
	return imdb.ResolveTitleResult{}, nil
}

func (s stubService) SearchTitles(ctx context.Context, params imdb.SearchTitlesParams) ([]imdb.TitleSearchResult, error) {
	if s.searchTitles != nil {
		return s.searchTitles(ctx, params)
	}
	return nil, nil
}

func (s stubService) GetRating(ctx context.Context, tconst string) (imdb.Rating, error) {
	if s.getRating != nil {
		return s.getRating(ctx, tconst)
	}
	return imdb.Rating{}, nil
}

func (s stubService) GetSeriesEpisodeRatings(ctx context.Context, tconst string) ([]imdb.EpisodeRating, error) {
	if s.getSeriesRatings != nil {
		return s.getSeriesRatings(ctx, tconst)
	}
	return nil, nil
}

func (s stubService) ResolveSeriesEpisodeRatings(ctx context.Context, params imdb.ResolveTitleParams) (imdb.SeriesEpisodeRatingsResult, error) {
	if s.resolveSeriesRating != nil {
		return s.resolveSeriesRating(ctx, params)
	}
	return imdb.SeriesEpisodeRatingsResult{}, nil
}

func (s stubService) GetSeriesEpisodes(ctx context.Context, tconst string) ([]imdb.EpisodeDetail, error) {
	if s.getSeriesEpisodes != nil {
		return s.getSeriesEpisodes(ctx, tconst)
	}
	return nil, nil
}

func (s stubService) GetEpisode(ctx context.Context, tconst string) (imdb.EpisodeDetail, error) {
	if s.getEpisode != nil {
		return s.getEpisode(ctx, tconst)
	}
	return imdb.EpisodeDetail{}, nil
}

func (s stubService) GetTitleCredits(ctx context.Context, tconst string) (imdb.Credits, error) {
	if s.getCredits != nil {
		return s.getCredits(ctx, tconst)
	}
	return imdb.Credits{}, nil
}

func (s stubService) GetTitlePrincipals(ctx context.Context, tconst string) ([]imdb.Principal, error) {
	if s.getPrincipals != nil {
		return s.getPrincipals(ctx, tconst)
	}
	return nil, nil
}

func (s stubService) GetTitleCrew(ctx context.Context, tconst string) ([]imdb.CrewMember, error) {
	if s.getCrew != nil {
		return s.getCrew(ctx, tconst)
	}
	return nil, nil
}

func (s stubService) GetName(ctx context.Context, nconst string) (imdb.NameDetail, error) {
	if s.getName != nil {
		return s.getName(ctx, nconst)
	}
	return imdb.NameDetail{}, nil
}

func (s stubService) SearchNames(ctx context.Context, q string) ([]imdb.NameSearchResult, error) {
	if s.searchNames != nil {
		return s.searchNames(ctx, q)
	}
	return nil, nil
}

func (s stubService) GetNameTitles(ctx context.Context, nconst string) ([]imdb.NameTitleCredit, error) {
	if s.getNameTitles != nil {
		return s.getNameTitles(ctx, nconst)
	}
	return nil, nil
}

func (s stubService) GetTitleAkas(ctx context.Context, tconst string) ([]imdb.AKA, error) {
	if s.getTitleAkas != nil {
		return s.getTitleAkas(ctx, tconst)
	}
	return nil, nil
}

func (s stubService) SearchAkas(ctx context.Context, params imdb.SearchAkasParams) ([]imdb.AKA, error) {
	if s.searchAkas != nil {
		return s.searchAkas(ctx, params)
	}
	return nil, nil
}

type stubAuthenticator struct {
	authenticate func(context.Context, string) (*auth.Principal, error)
}

func (s stubAuthenticator) Authenticate(ctx context.Context, key string) (*auth.Principal, error) {
	if s.authenticate != nil {
		return s.authenticate(ctx, key)
	}
	return nil, auth.ErrInvalidAPIKey
}
