package imdb

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNotFound       = errors.New("resource not found")
	ErrInvalidRequest = errors.New("invalid request")
)

const (
	MatchReasonExactPrimaryTitleWithFilters = "exact_primary_title_with_filters"
	MatchReasonExactPrimaryTitle            = "exact_primary_title"
	MatchReasonExactAliasWithFilters        = "exact_alias_with_filters"
	MatchReasonExactAlias                   = "exact_alias"
)

type QueryService interface {
	Ready(ctx context.Context) error
	ListSnapshots(ctx context.Context) ([]Snapshot, error)
	GetStats(ctx context.Context) (Stats, error)
	GetTitle(ctx context.Context, tconst string) (TitleDetail, error)
	ResolveTitle(ctx context.Context, params ResolveTitleParams) (ResolveTitleResult, error)
	SearchTitles(ctx context.Context, params SearchTitlesParams) ([]TitleSearchResult, error)
	GetRating(ctx context.Context, tconst string) (Rating, error)
	GetSeriesEpisodeRatings(ctx context.Context, tconst string) ([]EpisodeRating, error)
	ResolveSeriesEpisodeRatings(ctx context.Context, params ResolveTitleParams) (SeriesEpisodeRatingsResult, error)
	GetSeriesEpisodes(ctx context.Context, tconst string) ([]EpisodeDetail, error)
	GetEpisode(ctx context.Context, tconst string) (EpisodeDetail, error)
	GetTitleCredits(ctx context.Context, tconst string) (Credits, error)
	GetTitlePrincipals(ctx context.Context, tconst string) ([]Principal, error)
	GetTitleCrew(ctx context.Context, tconst string) ([]CrewMember, error)
	GetName(ctx context.Context, nconst string) (NameDetail, error)
	SearchNames(ctx context.Context, q string) ([]NameSearchResult, error)
	GetNameTitles(ctx context.Context, nconst string) ([]NameTitleCredit, error)
	GetTitleAkas(ctx context.Context, tconst string) ([]AKA, error)
	SearchAkas(ctx context.Context, params SearchAkasParams) ([]AKA, error)
}

type Repository interface {
	Ping(ctx context.Context) error
	ListSnapshots(ctx context.Context) ([]Snapshot, error)
	GetStats(ctx context.Context) (Stats, error)
	GetTitle(ctx context.Context, tconst string) (TitleDetail, error)
	FindResolveCandidates(ctx context.Context, title string) ([]ResolveCandidate, error)
	SearchTitles(ctx context.Context, params SearchTitlesParams) ([]TitleSearchResult, error)
	GetRating(ctx context.Context, tconst string) (Rating, error)
	GetSeriesEpisodeRatings(ctx context.Context, tconst string) ([]EpisodeRating, error)
	GetSeriesEpisodes(ctx context.Context, tconst string) ([]EpisodeDetail, error)
	GetEpisode(ctx context.Context, tconst string) (EpisodeDetail, error)
	GetTitleCredits(ctx context.Context, tconst string) (Credits, error)
	GetTitlePrincipals(ctx context.Context, tconst string) ([]Principal, error)
	GetTitleCrew(ctx context.Context, tconst string) ([]CrewMember, error)
	GetName(ctx context.Context, nconst string) (NameDetail, error)
	SearchNames(ctx context.Context, q string) ([]NameSearchResult, error)
	GetNameTitles(ctx context.Context, nconst string) ([]NameTitleCredit, error)
	GetTitleAkas(ctx context.Context, tconst string) ([]AKA, error)
	SearchAkas(ctx context.Context, params SearchAkasParams) ([]AKA, error)
}

type Snapshot struct {
	ID          int64      `json:"id"`
	Dataset     string     `json:"dataset"`
	ImportedAt  time.Time  `json:"importedAt"`
	IsActive    bool       `json:"isActive"`
	TitleCount  int64      `json:"titleCount"`
	NameCount   int64      `json:"nameCount"`
	RatingCount int64      `json:"ratingCount"`
	Notes       string     `json:"notes,omitempty"`
	SourceURL   string     `json:"sourceUrl,omitempty"`
	CompletedAt *time.Time `json:"completedAt,omitempty"`
}

type Stats struct {
	Titles      int64 `json:"titles"`
	Names       int64 `json:"names"`
	Ratings     int64 `json:"ratings"`
	Episodes    int64 `json:"episodes"`
	Principals  int64 `json:"principals"`
	CrewMembers int64 `json:"crewMembers"`
	Akas        int64 `json:"akas"`
	Snapshots   int64 `json:"snapshots"`
}

type TitleSummary struct {
	Tconst        string  `json:"tconst"`
	TitleType     string  `json:"titleType"`
	PrimaryTitle  string  `json:"primaryTitle"`
	OriginalTitle string  `json:"originalTitle,omitempty"`
	StartYear     *int    `json:"startYear,omitempty"`
	EndYear       *int    `json:"endYear,omitempty"`
	IsAdult       bool    `json:"isAdult"`
	RuntimeMinute *int    `json:"runtimeMinutes,omitempty"`
	Genres        []string `json:"genres,omitempty"`
}

type TitleDetail struct {
	Title       TitleSummary `json:"title"`
	Rating      *Rating      `json:"rating,omitempty"`
	EpisodeInfo *EpisodeInfo `json:"episodeInfo,omitempty"`
}

type TitleSearchResult struct {
	Title        TitleSummary `json:"title"`
	Rating       *Rating      `json:"rating,omitempty"`
	MatchedAlias string       `json:"matchedAlias,omitempty"`
}

type Rating struct {
	Tconst        string  `json:"tconst"`
	AverageRating float64 `json:"averageRating"`
	NumVotes      int     `json:"numVotes"`
}

type EpisodeInfo struct {
	ParentTconst  string `json:"parentTconst"`
	SeasonNumber  *int   `json:"seasonNumber,omitempty"`
	EpisodeNumber *int   `json:"episodeNumber,omitempty"`
}

type EpisodeRating struct {
	Tconst         string   `json:"tconst"`
	ParentTconst   string   `json:"parentTconst"`
	PrimaryTitle   string   `json:"primaryTitle"`
	SeasonNumber   *int     `json:"seasonNumber,omitempty"`
	EpisodeNumber  *int     `json:"episodeNumber,omitempty"`
	AverageRating  *float64 `json:"averageRating,omitempty"`
	NumVotes       *int     `json:"numVotes,omitempty"`
}

type EpisodeDetail struct {
	Title       TitleSummary `json:"title"`
	EpisodeInfo EpisodeInfo  `json:"episodeInfo"`
	Rating      *Rating      `json:"rating,omitempty"`
}

type Principal struct {
	Ordering   int        `json:"ordering"`
	Nconst     string     `json:"nconst"`
	Name       string     `json:"name"`
	Category   string     `json:"category"`
	Job        string     `json:"job,omitempty"`
	Characters []string   `json:"characters,omitempty"`
	BirthYear  *int       `json:"birthYear,omitempty"`
	DeathYear  *int       `json:"deathYear,omitempty"`
}

type CrewMember struct {
	Nconst    string `json:"nconst"`
	Name      string `json:"name"`
	Role      string `json:"role"`
	Ordering  *int   `json:"ordering,omitempty"`
	BirthYear *int   `json:"birthYear,omitempty"`
	DeathYear *int   `json:"deathYear,omitempty"`
}

type Credits struct {
	Principals []Principal  `json:"principals"`
	Crew       []CrewMember `json:"crew"`
}

type NameDetail struct {
	Nconst             string   `json:"nconst"`
	PrimaryName        string   `json:"primaryName"`
	BirthYear          *int     `json:"birthYear,omitempty"`
	DeathYear          *int     `json:"deathYear,omitempty"`
	PrimaryProfessions []string `json:"primaryProfessions,omitempty"`
	KnownForTitles     []string `json:"knownForTitles,omitempty"`
}

type NameSearchResult struct {
	Nconst             string   `json:"nconst"`
	PrimaryName        string   `json:"primaryName"`
	BirthYear          *int     `json:"birthYear,omitempty"`
	DeathYear          *int     `json:"deathYear,omitempty"`
	PrimaryProfessions []string `json:"primaryProfessions,omitempty"`
}

type NameTitleCredit struct {
	Title      TitleSummary `json:"title"`
	Categories []string     `json:"categories,omitempty"`
	Roles      []string     `json:"roles,omitempty"`
}

type AKA struct {
	ID              int64    `json:"id"`
	Tconst          string   `json:"tconst"`
	Title           string   `json:"title"`
	Region          string   `json:"region,omitempty"`
	Language        string   `json:"language,omitempty"`
	Types           []string `json:"types,omitempty"`
	Attributes      []string `json:"attributes,omitempty"`
	IsOriginalTitle bool     `json:"isOriginalTitle"`
}

type ResolveTitleParams struct {
	Title     string `json:"title"`
	TitleType string `json:"titleType,omitempty"`
	Year      *int   `json:"year,omitempty"`
}

type SearchTitlesParams struct {
	Query       string `json:"q"`
	TitleType   string `json:"titleType,omitempty"`
	Year        *int   `json:"year,omitempty"`
	IncludeAkas bool   `json:"includeAkas,omitempty"`
}

type SearchAkasParams struct {
	Title    string `json:"title"`
	Region   string `json:"region,omitempty"`
	Language string `json:"language,omitempty"`
}

type ResolutionFilters struct {
	TitleType string `json:"titleType,omitempty"`
	Year      *int   `json:"year,omitempty"`
}

type ResolutionMetadata struct {
	InputTitle     string            `json:"inputTitle"`
	ResolvedTconst string            `json:"resolvedTconst"`
	MatchReason    string            `json:"matchReason"`
	MatchedAlias   string            `json:"matchedAlias,omitempty"`
	AppliedFilters ResolutionFilters `json:"appliedFilters,omitempty"`
}

type ResolveTitleResult struct {
	Title      TitleDetail         `json:"title"`
	Resolution ResolutionMetadata  `json:"resolution"`
}

type SeriesEpisodeRatingsResult struct {
	Series     TitleDetail         `json:"series"`
	Resolution ResolutionMetadata  `json:"resolution"`
	Items      []EpisodeRating     `json:"items"`
}

type ResolveCandidate struct {
	Title        TitleSummary
	MatchedAlias string
	NumVotes     int
}
