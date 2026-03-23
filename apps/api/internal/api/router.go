package api

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
		r.Post("/titles/bulk/get", handler.bulkGetTitles)
		r.Post("/titles/bulk/resolve", handler.bulkResolveTitles)
		r.Get("/ratings/{tconst}", handler.getRating)
		r.Post("/ratings/bulk", handler.bulkGetRatings)
		r.Get("/series/resolve/episode-ratings", handler.resolveSeriesEpisodeRatings)
		r.Get("/series/{tconst}/episode-ratings", handler.getSeriesEpisodeRatings)
		r.Get("/series/{tconst}/episodes", handler.getSeriesEpisodes)
		r.Post("/series/bulk/episode-ratings", handler.bulkGetSeriesEpisodeRatings)
		r.Get("/episodes/{tconst}", handler.getEpisode)
		r.Post("/episodes/bulk", handler.bulkGetEpisodes)
		r.Get("/titles/{tconst}/credits", handler.getTitleCredits)
		r.Get("/titles/{tconst}/principals", handler.getTitlePrincipals)
		r.Get("/titles/{tconst}/crew", handler.getTitleCrew)
		r.Get("/names/search", handler.searchNames)
		r.Get("/names/{nconst}", handler.getName)
		r.Get("/names/{nconst}/titles", handler.getNameTitles)
		r.Post("/names/bulk", handler.bulkGetNames)
		r.Get("/titles/{tconst}/akas", handler.getTitleAkas)
		r.Get("/akas/search", handler.searchAkas)
		r.Post("/bulk/jobs", handler.createBulkJob)
		r.Get("/bulk/jobs/{jobId}", handler.getBulkJob)
		r.Get("/bulk/jobs/{jobId}/result", handler.getBulkJobResult)
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
	series, err := h.service.GetTitle(r.Context(), chi.URLParam(r, "tconst"))
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"series": series, "episodes": items})
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
	series, err := h.service.GetTitle(r.Context(), chi.URLParam(r, "tconst"))
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"series": series, "episodes": items})
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

func (h Handler) bulkGetTitles(w http.ResponseWriter, r *http.Request) {
	identifiers, err := decodeIdentifierBody(r)
	if err != nil {
		handleServiceError(w, err)
		return
	}

	results := make([]imdb.TitleDetail, 0, len(identifiers))
	missing := make([]string, 0)
	for _, id := range identifiers {
		item, err := h.service.GetTitle(r.Context(), id)
		if err != nil {
			if errors.Is(err, imdb.ErrNotFound) {
				missing = append(missing, id)
				continue
			}
			handleServiceError(w, err)
			return
		}
		results = append(results, item)
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results, "missing": missing})
}

func (h Handler) bulkResolveTitles(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Items []imdb.ResolveTitleParams `json:"items"`
	}
	if err := decodeJSONBody(r, &body); err != nil {
		handleServiceError(w, imdb.ErrInvalidRequest)
		return
	}
	if len(body.Items) == 0 || len(body.Items) > 250 {
		handleServiceError(w, imdb.ErrInvalidRequest)
		return
	}

	results := make([]imdb.ResolveTitleResult, 0, len(body.Items))
	for _, item := range body.Items {
		resolved, err := h.service.ResolveTitle(r.Context(), item)
		if err != nil {
			handleServiceError(w, err)
			return
		}
		results = append(results, resolved)
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

func (h Handler) bulkGetRatings(w http.ResponseWriter, r *http.Request) {
	identifiers, err := decodeIdentifierBody(r)
	if err != nil {
		handleServiceError(w, err)
		return
	}

	results := make([]imdb.Rating, 0, len(identifiers))
	missing := make([]string, 0)
	for _, id := range identifiers {
		item, err := h.service.GetRating(r.Context(), id)
		if err != nil {
			if errors.Is(err, imdb.ErrNotFound) {
				missing = append(missing, id)
				continue
			}
			handleServiceError(w, err)
			return
		}
		results = append(results, item)
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results, "missing": missing})
}

func (h Handler) bulkGetSeriesEpisodeRatings(w http.ResponseWriter, r *http.Request) {
	identifiers, err := decodeIdentifierBody(r)
	if err != nil {
		handleServiceError(w, err)
		return
	}

	type item struct {
		Series   imdb.TitleDetail     `json:"series"`
		Episodes []imdb.EpisodeRating `json:"episodes"`
	}
	results := make([]item, 0, len(identifiers))
	missing := make([]string, 0)
	for _, id := range identifiers {
		series, err := h.service.GetTitle(r.Context(), id)
		if err != nil {
			if errors.Is(err, imdb.ErrNotFound) {
				missing = append(missing, id)
				continue
			}
			handleServiceError(w, err)
			return
		}
		episodes, err := h.service.GetSeriesEpisodeRatings(r.Context(), id)
		if err != nil {
			handleServiceError(w, err)
			return
		}
		results = append(results, item{Series: series, Episodes: episodes})
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results, "missing": missing})
}

func (h Handler) bulkGetEpisodes(w http.ResponseWriter, r *http.Request) {
	identifiers, err := decodeIdentifierBody(r)
	if err != nil {
		handleServiceError(w, err)
		return
	}

	results := make([]imdb.EpisodeDetail, 0, len(identifiers))
	missing := make([]string, 0)
	for _, id := range identifiers {
		item, err := h.service.GetEpisode(r.Context(), id)
		if err != nil {
			if errors.Is(err, imdb.ErrNotFound) {
				missing = append(missing, id)
				continue
			}
			handleServiceError(w, err)
			return
		}
		results = append(results, item)
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results, "missing": missing})
}

func (h Handler) bulkGetNames(w http.ResponseWriter, r *http.Request) {
	identifiers, err := decodeIdentifierBody(r)
	if err != nil {
		handleServiceError(w, err)
		return
	}

	results := make([]imdb.NameDetail, 0, len(identifiers))
	missing := make([]string, 0)
	for _, id := range identifiers {
		item, err := h.service.GetName(r.Context(), id)
		if err != nil {
			if errors.Is(err, imdb.ErrNotFound) {
				missing = append(missing, id)
				continue
			}
			handleServiceError(w, err)
			return
		}
		results = append(results, item)
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results, "missing": missing})
}

func (h Handler) createBulkJob(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Operation string          `json:"operation"`
		Payload   json.RawMessage `json:"payload"`
	}
	if err := decodeJSONBody(r, &body); err != nil || strings.TrimSpace(body.Operation) == "" {
		handleServiceError(w, imdb.ErrInvalidRequest)
		return
	}

	resultPayload, err := h.executeBulkOperation(r.Context(), body.Operation, body.Payload)
	if err != nil {
		handleServiceError(w, err)
		return
	}

	job, err := h.service.CreateBulkJob(r.Context(), imdb.CreateBulkJobParams{
		ID:        newUUID(),
		Operation: body.Operation,
		Status:    "succeeded",
		Payload:   body.Payload,
		Result:    resultPayload,
		ExpiresAt: time.Now().Add(7 * 24 * time.Hour),
	})
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, job)
}

func (h Handler) getBulkJob(w http.ResponseWriter, r *http.Request) {
	job, err := h.service.GetBulkJob(r.Context(), chi.URLParam(r, "jobId"))
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, job)
}

func (h Handler) getBulkJobResult(w http.ResponseWriter, r *http.Request) {
	result, err := h.service.GetBulkJobResult(r.Context(), chi.URLParam(r, "jobId"))
	if err != nil {
		handleServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
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

func decodeJSONBody(r *http.Request, target any) error {
	decoder := json.NewDecoder(io.LimitReader(r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func decodeIdentifierBody(r *http.Request) ([]string, error) {
	var body struct {
		Identifiers []string `json:"identifiers"`
	}
	if err := decodeJSONBody(r, &body); err != nil {
		return nil, err
	}
	if len(body.Identifiers) == 0 || len(body.Identifiers) > 250 {
		return nil, imdb.ErrInvalidRequest
	}

	identifiers := make([]string, 0, len(body.Identifiers))
	for _, item := range body.Identifiers {
		item = strings.TrimSpace(item)
		if item == "" {
			return nil, imdb.ErrInvalidRequest
		}
		identifiers = append(identifiers, item)
	}
	return identifiers, nil
}

func (h Handler) executeBulkOperation(ctx context.Context, operation string, payload json.RawMessage) ([]byte, error) {
	switch operation {
	case "titles.bulk.get":
		var body struct {
			Identifiers []string `json:"identifiers"`
		}
		if err := json.Unmarshal(payload, &body); err != nil {
			return nil, imdb.ErrInvalidRequest
		}
		results := make([]imdb.TitleDetail, 0, len(body.Identifiers))
		for _, id := range body.Identifiers {
			item, err := h.service.GetTitle(ctx, id)
			if err != nil {
				return nil, err
			}
			results = append(results, item)
		}
		return json.Marshal(map[string]any{"results": results})
	case "ratings.bulk":
		var body struct {
			Identifiers []string `json:"identifiers"`
		}
		if err := json.Unmarshal(payload, &body); err != nil {
			return nil, imdb.ErrInvalidRequest
		}
		results := make([]imdb.Rating, 0, len(body.Identifiers))
		for _, id := range body.Identifiers {
			item, err := h.service.GetRating(ctx, id)
			if err != nil {
				return nil, err
			}
			results = append(results, item)
		}
		return json.Marshal(map[string]any{"results": results})
	default:
		return nil, imdb.ErrInvalidRequest
	}
}

func newUUID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf(
		"%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
		b[0], b[1], b[2], b[3],
		b[4], b[5],
		b[6], b[7],
		b[8], b[9],
		b[10], b[11], b[12], b[13], b[14], b[15],
	)
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
