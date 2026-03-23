package imdb

import "strings"

type rankedCandidate struct {
	candidate      ResolveCandidate
	exactPrimary   bool
	exactAlias     bool
	titleTypeMatch bool
	yearMatch      bool
	startYear      int
}

func SelectBestTitleMatch(params ResolveTitleParams, candidates []ResolveCandidate) (ResolveCandidate, ResolutionMetadata, bool) {
	if len(candidates) == 0 {
		return ResolveCandidate{}, ResolutionMetadata{}, false
	}

	target := strings.TrimSpace(strings.ToLower(params.Title))
	var best rankedCandidate
	found := false

	for _, candidate := range candidates {
		ranked := rankedCandidate{
			candidate:      candidate,
			exactPrimary:   strings.EqualFold(candidate.Title.PrimaryTitle, target),
			exactAlias:     candidate.MatchedAlias != "" && strings.EqualFold(candidate.MatchedAlias, target),
			titleTypeMatch: params.TitleType != "" && strings.EqualFold(candidate.Title.TitleType, params.TitleType),
			yearMatch:      params.Year != nil && candidate.Title.StartYear != nil && *candidate.Title.StartYear == *params.Year,
			startYear:      yearValue(candidate.Title.StartYear),
		}

		if !found || compareRankedCandidates(ranked, best) > 0 {
			best = ranked
			found = true
		}
	}

	if !found {
		return ResolveCandidate{}, ResolutionMetadata{}, false
	}

	return best.candidate, buildResolutionMetadata(params, best), true
}

func compareRankedCandidates(left, right rankedCandidate) int {
	switch {
	case left.exactPrimary != right.exactPrimary:
		return boolCompare(left.exactPrimary, right.exactPrimary)
	case left.exactAlias != right.exactAlias:
		return boolCompare(left.exactAlias, right.exactAlias)
	case left.titleTypeMatch != right.titleTypeMatch:
		return boolCompare(left.titleTypeMatch, right.titleTypeMatch)
	case left.yearMatch != right.yearMatch:
		return boolCompare(left.yearMatch, right.yearMatch)
	case left.candidate.NumVotes != right.candidate.NumVotes:
		return intCompare(left.candidate.NumVotes, right.candidate.NumVotes)
	case left.startYear != right.startYear:
		return intCompare(left.startYear, right.startYear)
	case left.candidate.Title.IsAdult != right.candidate.Title.IsAdult:
		return boolCompare(!left.candidate.Title.IsAdult, !right.candidate.Title.IsAdult)
	default:
		return strings.Compare(left.candidate.Title.Tconst, right.candidate.Title.Tconst) * -1
	}
}

func buildResolutionMetadata(params ResolveTitleParams, candidate rankedCandidate) ResolutionMetadata {
	meta := ResolutionMetadata{
		InputTitle:     params.Title,
		ResolvedTconst: candidate.candidate.Title.Tconst,
		AppliedFilters: ResolutionFilters{
			TitleType: params.TitleType,
			Year:      params.Year,
		},
	}

	switch {
	case candidate.exactPrimary && (candidate.titleTypeMatch || candidate.yearMatch):
		meta.MatchReason = MatchReasonExactPrimaryTitleWithFilters
	case candidate.exactPrimary:
		meta.MatchReason = MatchReasonExactPrimaryTitle
	case candidate.exactAlias && (candidate.titleTypeMatch || candidate.yearMatch):
		meta.MatchReason = MatchReasonExactAliasWithFilters
	default:
		meta.MatchReason = MatchReasonExactAlias
	}
	if candidate.exactAlias {
		meta.MatchedAlias = candidate.candidate.MatchedAlias
	}
	return meta
}

func boolCompare(left, right bool) int {
	switch {
	case left && !right:
		return 1
	case !left && right:
		return -1
	default:
		return 0
	}
}

func intCompare(left, right int) int {
	switch {
	case left > right:
		return 1
	case left < right:
		return -1
	default:
		return 0
	}
}

func yearValue(year *int) int {
	if year == nil {
		return 0
	}
	return *year
}
