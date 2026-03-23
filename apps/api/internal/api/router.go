package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"nexio-imdb/apps/api/internal/auth"
	"nexio-imdb/apps/api/internal/imdb"
)

type Handler struct {
	service imdb.QueryService
	auth    auth.Authenticator
}

type errorEnvelope struct {
	Error apiError `json:"error"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func NewRouter(service imdb.QueryService, authenticator auth.Authenticator) http.Handler {
	handler := Handler{
		service: service,
		auth:    authenticator,
	}

	router := chi.NewRouter()
	router.Use(middleware.RequestID)
	router.Use(middleware.RealIP)
	router.Use(middleware.Recoverer)
	router.Use(middleware.Timeout(30 * time.Second))

	router.Get("/healthz", handler.healthz)
	router.Get("/readyz", handler.readyz)

	router.Route("/v1", func(r chi.Router) {
		r.Use(handler.requireAPIKey)
		r.Get("/meta/snapshots", handler.listSnapshots)
		r.Get("/meta/stats", handler.getStats)
		r.Get("/titles/resolve", handler.resolveTitle)
		r.Get("/titles/search", handler.searchTitles)
		r.Get("/titles/{tconst}", handler.getTitle)
		r.Get("/ratings/{tconst}", handler.getRating)
		r.Get("/series/resolve/episode-ratings", handler.resolveSeriesEpisodeRatings)
		r.Get("/series/{tconst}/episode-ratings", handler.getSeriesEpisodeRatings)
		r.Get("/series/{tconst}/episodes", handler.getSeriesEpisodes)
		r.Get("/episodes/{tconst}", handler.getEpisode)
		r.Get("/titles/{tconst}/credits", handler.getTitleCredits)
		r.Get("/titles/{tconst}/principals", handler.getTitlePrincipals)
		r.Get("/titles/{tconst}/crew", handler.getTitleCrew)
		r.Get("/names/search", handler.searchNames)
		r.Get("/names/{nconst}", handler.getName)
		r.Get("/names/{nconst}/titles", handler.getNameTitles)
		r.Get("/titles/{tconst}/akas", handler.getTitleAkas)
		r.Get("/akas/search", handler.searchAkas)
	})

	return router
}

func (h Handler) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h Handler) readyz(w http.ResponseWriter, r *http.Request) {
	if err := h.service.Ready(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready", "service is not ready")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (h Handler) listSnapshots(w http.ResponseWriter, r *http.Request) {
	items, err := h.service.ListSnapshots(r.Context())
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h Handler) getStats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.service.GetStats(r.Context())
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

func (h Handler) getTitle(w http.ResponseWriter, r *http.Request) {
	title, err := h.service.GetTitle(r.Context(), chi.URLParam(r, "tconst"))
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, title)
}

func (h Handler) resolveTitle(w http.ResponseWriter, r *http.Request) {
	params, err := parseResolveTitleParams(r)
	if err != nil {
		handleServiceError(w, err)
		return
	}

	result, err := h.service.ResolveTitle(r.Context(), params)
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h Handler) searchTitles(w http.ResponseWriter, r *http.Request) {
	params, err := parseSearchTitlesParams(r)
	if err != nil {
		handleServiceError(w, err)
		return
	}

	items, err := h.service.SearchTitles(r.Context(), params)
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h Handler) getRating(w http.ResponseWriter, r *http.Request) {
	rating, err := h.service.GetRating(r.Context(), chi.URLParam(r, "tconst"))
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, rating)
}

func (h Handler) getSeriesEpisodeRatings(w http.ResponseWriter, r *http.Request) {
	items, err := h.service.GetSeriesEpisodeRatings(r.Context(), chi.URLParam(r, "tconst"))
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h Handler) resolveSeriesEpisodeRatings(w http.ResponseWriter, r *http.Request) {
	params, err := parseResolveTitleParams(r)
	if err != nil {
		handleServiceError(w, err)
		return
	}

	result, err := h.service.ResolveSeriesEpisodeRatings(r.Context(), params)
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h Handler) getSeriesEpisodes(w http.ResponseWriter, r *http.Request) {
	items, err := h.service.GetSeriesEpisodes(r.Context(), chi.URLParam(r, "tconst"))
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h Handler) getEpisode(w http.ResponseWriter, r *http.Request) {
	episode, err := h.service.GetEpisode(r.Context(), chi.URLParam(r, "tconst"))
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, episode)
}

func (h Handler) getTitleCredits(w http.ResponseWriter, r *http.Request) {
	credits, err := h.service.GetTitleCredits(r.Context(), chi.URLParam(r, "tconst"))
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, credits)
}

func (h Handler) getTitlePrincipals(w http.ResponseWriter, r *http.Request) {
	items, err := h.service.GetTitlePrincipals(r.Context(), chi.URLParam(r, "tconst"))
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h Handler) getTitleCrew(w http.ResponseWriter, r *http.Request) {
	items, err := h.service.GetTitleCrew(r.Context(), chi.URLParam(r, "tconst"))
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h Handler) getName(w http.ResponseWriter, r *http.Request) {
	name, err := h.service.GetName(r.Context(), chi.URLParam(r, "nconst"))
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, name)
}

func (h Handler) searchNames(w http.ResponseWriter, r *http.Request) {
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query == "" {
		handleServiceError(w, imdb.ErrInvalidRequest)
		return
	}

	items, err := h.service.SearchNames(r.Context(), query)
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h Handler) getNameTitles(w http.ResponseWriter, r *http.Request) {
	items, err := h.service.GetNameTitles(r.Context(), chi.URLParam(r, "nconst"))
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h Handler) getTitleAkas(w http.ResponseWriter, r *http.Request) {
	items, err := h.service.GetTitleAkas(r.Context(), chi.URLParam(r, "tconst"))
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h Handler) searchAkas(w http.ResponseWriter, r *http.Request) {
	title := strings.TrimSpace(r.URL.Query().Get("title"))
	if title == "" {
		handleServiceError(w, imdb.ErrInvalidRequest)
		return
	}

	items, err := h.service.SearchAkas(r.Context(), imdb.SearchAkasParams{
		Title:    title,
		Region:   strings.TrimSpace(r.URL.Query().Get("region")),
		Language: strings.TrimSpace(r.URL.Query().Get("language")),
	})
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (h Handler) requireAPIKey(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		apiKey := strings.TrimSpace(r.Header.Get("X-API-Key"))
		if apiKey == "" {
			authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
			if strings.HasPrefix(strings.ToLower(authHeader), "bearer ") {
				apiKey = strings.TrimSpace(authHeader[7:])
			}
		}
		if apiKey == "" {
			writeError(w, http.StatusUnauthorized, "missing_api_key", "api key required")
			return
		}

		principal, err := h.auth.Authenticate(r.Context(), apiKey)
		if err != nil {
			writeError(w, http.StatusUnauthorized, "invalid_api_key", "api key invalid")
			return
		}

		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalContextKey{}, principal)))
	})
}

type principalContextKey struct{}

func parseResolveTitleParams(r *http.Request) (imdb.ResolveTitleParams, error) {
	title := strings.TrimSpace(r.URL.Query().Get("title"))
	if title == "" {
		return imdb.ResolveTitleParams{}, imdb.ErrInvalidRequest
	}

	year, err := parseOptionalInt(r.URL.Query().Get("year"))
	if err != nil {
		return imdb.ResolveTitleParams{}, imdb.ErrInvalidRequest
	}

	return imdb.ResolveTitleParams{
		Title:     title,
		TitleType: strings.TrimSpace(r.URL.Query().Get("titleType")),
		Year:      year,
	}, nil
}

func parseSearchTitlesParams(r *http.Request) (imdb.SearchTitlesParams, error) {
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	if query == "" {
		return imdb.SearchTitlesParams{}, imdb.ErrInvalidRequest
	}

	year, err := parseOptionalInt(r.URL.Query().Get("year"))
	if err != nil {
		return imdb.SearchTitlesParams{}, imdb.ErrInvalidRequest
	}

	return imdb.SearchTitlesParams{
		Query:       query,
		TitleType:   strings.TrimSpace(r.URL.Query().Get("titleType")),
		Year:        year,
		IncludeAkas: strings.EqualFold(r.URL.Query().Get("includeAkas"), "true"),
	}, nil
}

func parseOptionalInt(raw string) (*int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return nil, err
	}
	return &value, nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorEnvelope{
		Error: apiError{
			Code:    code,
			Message: message,
		},
	})
}

func handleServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, imdb.ErrInvalidRequest):
		writeError(w, http.StatusBadRequest, "invalid_request", "request parameters are invalid")
	case errors.Is(err, imdb.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "resource not found")
	default:
		writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}
