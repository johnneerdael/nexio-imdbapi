package bulk

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"nexio-imdb/apps/api/internal/imdb"
)

const maxAsyncIdentifiers = 10000

func Validate(operation string, payload json.RawMessage, maxIdentifiers int) error {
	switch operation {
	case "titles.bulk.get", "ratings.bulk", "series.bulk.episode-ratings", "episodes.bulk", "names.bulk":
		_, err := decodeIdentifiers(payload, maxIdentifiers)
		return err
	case "titles.bulk.resolve":
		_, err := decodeResolveItems(payload, maxIdentifiers)
		return err
	default:
		return imdb.ErrInvalidRequest
	}
}

func Execute(ctx context.Context, service imdb.QueryService, operation string, payload json.RawMessage) ([]byte, error) {
	switch operation {
	case "titles.bulk.get":
		identifiers, err := decodeIdentifiers(payload, maxAsyncIdentifiers)
		if err != nil {
			return nil, err
		}
		results := make([]imdb.TitleDetail, 0, len(identifiers))
		missing := make([]string, 0)
		for _, id := range identifiers {
			item, err := service.GetTitle(ctx, id)
			if err != nil {
				if errors.Is(err, imdb.ErrNotFound) {
					missing = append(missing, id)
					continue
				}
				return nil, err
			}
			results = append(results, item)
		}
		return json.Marshal(map[string]any{"results": results, "missing": missing})
	case "titles.bulk.resolve":
		items, err := decodeResolveItems(payload, maxAsyncIdentifiers)
		if err != nil {
			return nil, err
		}
		results := make([]imdb.ResolveTitleResult, 0, len(items))
		for _, item := range items {
			resolved, err := service.ResolveTitle(ctx, item)
			if err != nil {
				return nil, err
			}
			results = append(results, resolved)
		}
		return json.Marshal(map[string]any{"results": results})
	case "ratings.bulk":
		identifiers, err := decodeIdentifiers(payload, maxAsyncIdentifiers)
		if err != nil {
			return nil, err
		}
		results := make([]imdb.Rating, 0, len(identifiers))
		missing := make([]string, 0)
		for _, id := range identifiers {
			item, err := service.GetRating(ctx, id)
			if err != nil {
				if errors.Is(err, imdb.ErrNotFound) {
					missing = append(missing, id)
					continue
				}
				return nil, err
			}
			results = append(results, item)
		}
		return json.Marshal(map[string]any{"results": results, "missing": missing})
	case "series.bulk.episode-ratings":
		identifiers, err := decodeIdentifiers(payload, maxAsyncIdentifiers)
		if err != nil {
			return nil, err
		}
		type seriesResult struct {
			Series   imdb.TitleDetail     `json:"series"`
			Episodes []imdb.EpisodeRating `json:"episodes"`
		}
		results := make([]seriesResult, 0, len(identifiers))
		missing := make([]string, 0)
		for _, id := range identifiers {
			series, err := service.GetTitle(ctx, id)
			if err != nil {
				if errors.Is(err, imdb.ErrNotFound) {
					missing = append(missing, id)
					continue
				}
				return nil, err
			}
			episodes, err := service.GetSeriesEpisodeRatings(ctx, id)
			if err != nil {
				return nil, err
			}
			results = append(results, seriesResult{
				Series:   series,
				Episodes: episodes,
			})
		}
		return json.Marshal(map[string]any{"results": results, "missing": missing})
	case "episodes.bulk":
		identifiers, err := decodeIdentifiers(payload, maxAsyncIdentifiers)
		if err != nil {
			return nil, err
		}
		results := make([]imdb.EpisodeDetail, 0, len(identifiers))
		missing := make([]string, 0)
		for _, id := range identifiers {
			item, err := service.GetEpisode(ctx, id)
			if err != nil {
				if errors.Is(err, imdb.ErrNotFound) {
					missing = append(missing, id)
					continue
				}
				return nil, err
			}
			results = append(results, item)
		}
		return json.Marshal(map[string]any{"results": results, "missing": missing})
	case "names.bulk":
		identifiers, err := decodeIdentifiers(payload, maxAsyncIdentifiers)
		if err != nil {
			return nil, err
		}
		results := make([]imdb.NameDetail, 0, len(identifiers))
		missing := make([]string, 0)
		for _, id := range identifiers {
			item, err := service.GetName(ctx, id)
			if err != nil {
				if errors.Is(err, imdb.ErrNotFound) {
					missing = append(missing, id)
					continue
				}
				return nil, err
			}
			results = append(results, item)
		}
		return json.Marshal(map[string]any{"results": results, "missing": missing})
	default:
		return nil, imdb.ErrInvalidRequest
	}
}

func decodeIdentifiers(payload json.RawMessage, maxIdentifiers int) ([]string, error) {
	var body struct {
		Identifiers []string `json:"identifiers"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return nil, imdb.ErrInvalidRequest
	}
	if len(body.Identifiers) == 0 || len(body.Identifiers) > maxIdentifiers {
		return nil, imdb.ErrInvalidRequest
	}

	items := make([]string, 0, len(body.Identifiers))
	for _, id := range body.Identifiers {
		id = strings.TrimSpace(id)
		if id == "" {
			return nil, imdb.ErrInvalidRequest
		}
		items = append(items, id)
	}
	return items, nil
}

func decodeResolveItems(payload json.RawMessage, maxIdentifiers int) ([]imdb.ResolveTitleParams, error) {
	var body struct {
		Items []imdb.ResolveTitleParams `json:"items"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return nil, imdb.ErrInvalidRequest
	}
	if len(body.Items) == 0 || len(body.Items) > maxIdentifiers {
		return nil, imdb.ErrInvalidRequest
	}
	for _, item := range body.Items {
		if strings.TrimSpace(item.Title) == "" {
			return nil, imdb.ErrInvalidRequest
		}
	}
	return body.Items, nil
}
