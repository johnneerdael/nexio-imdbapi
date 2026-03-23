package imdb

import "context"

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) Ready(ctx context.Context) error {
	return s.repo.Ping(ctx)
}

func (s *Service) ListSnapshots(ctx context.Context) ([]Snapshot, error) {
	return s.repo.ListSnapshots(ctx)
}

func (s *Service) GetStats(ctx context.Context) (Stats, error) {
	return s.repo.GetStats(ctx)
}

func (s *Service) GetTitle(ctx context.Context, tconst string) (TitleDetail, error) {
	return s.repo.GetTitle(ctx, tconst)
}

func (s *Service) ResolveTitle(ctx context.Context, params ResolveTitleParams) (ResolveTitleResult, error) {
	candidates, err := s.repo.FindResolveCandidates(ctx, params.Title)
	if err != nil {
		return ResolveTitleResult{}, err
	}

	best, meta, ok := SelectBestTitleMatch(params, candidates)
	if !ok {
		return ResolveTitleResult{}, ErrNotFound
	}

	title, err := s.repo.GetTitle(ctx, best.Title.Tconst)
	if err != nil {
		return ResolveTitleResult{}, err
	}

	return ResolveTitleResult{
		Title:      title,
		Resolution: meta,
	}, nil
}

func (s *Service) SearchTitles(ctx context.Context, params SearchTitlesParams) ([]TitleSearchResult, error) {
	return s.repo.SearchTitles(ctx, params)
}

func (s *Service) GetRating(ctx context.Context, tconst string) (Rating, error) {
	return s.repo.GetRating(ctx, tconst)
}

func (s *Service) GetSeriesEpisodeRatings(ctx context.Context, tconst string) ([]EpisodeRating, error) {
	return s.repo.GetSeriesEpisodeRatings(ctx, tconst)
}

func (s *Service) ResolveSeriesEpisodeRatings(ctx context.Context, params ResolveTitleParams) (SeriesEpisodeRatingsResult, error) {
	resolved, err := s.ResolveTitle(ctx, params)
	if err != nil {
		return SeriesEpisodeRatingsResult{}, err
	}

	items, err := s.repo.GetSeriesEpisodeRatings(ctx, resolved.Title.Title.Tconst)
	if err != nil {
		return SeriesEpisodeRatingsResult{}, err
	}

	return SeriesEpisodeRatingsResult{
		Series:     resolved.Title,
		Resolution: resolved.Resolution,
		Items:      items,
	}, nil
}

func (s *Service) GetSeriesEpisodes(ctx context.Context, tconst string) ([]EpisodeDetail, error) {
	return s.repo.GetSeriesEpisodes(ctx, tconst)
}

func (s *Service) GetEpisode(ctx context.Context, tconst string) (EpisodeDetail, error) {
	return s.repo.GetEpisode(ctx, tconst)
}

func (s *Service) GetTitleCredits(ctx context.Context, tconst string) (Credits, error) {
	return s.repo.GetTitleCredits(ctx, tconst)
}

func (s *Service) GetTitlePrincipals(ctx context.Context, tconst string) ([]Principal, error) {
	return s.repo.GetTitlePrincipals(ctx, tconst)
}

func (s *Service) GetTitleCrew(ctx context.Context, tconst string) ([]CrewMember, error) {
	return s.repo.GetTitleCrew(ctx, tconst)
}

func (s *Service) GetName(ctx context.Context, nconst string) (NameDetail, error) {
	return s.repo.GetName(ctx, nconst)
}

func (s *Service) SearchNames(ctx context.Context, q string) ([]NameSearchResult, error) {
	return s.repo.SearchNames(ctx, q)
}

func (s *Service) GetNameTitles(ctx context.Context, nconst string) ([]NameTitleCredit, error) {
	return s.repo.GetNameTitles(ctx, nconst)
}

func (s *Service) GetTitleAkas(ctx context.Context, tconst string) ([]AKA, error) {
	return s.repo.GetTitleAkas(ctx, tconst)
}

func (s *Service) SearchAkas(ctx context.Context, params SearchAkasParams) ([]AKA, error) {
	return s.repo.SearchAkas(ctx, params)
}
