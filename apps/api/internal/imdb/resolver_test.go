package imdb

import "testing"

func TestSelectBestTitleMatch_PrefersExactPrimaryBeforeAlias(t *testing.T) {
	t.Parallel()

	year := 2024
	best, meta, ok := SelectBestTitleMatch(ResolveTitleParams{
		Title:     "Paradise",
		TitleType: "tvSeries",
		Year:      &year,
	}, []ResolveCandidate{
		{
			Title: TitleSummary{
				Tconst:       "tt-alias",
				PrimaryTitle: "Different Title",
				TitleType:    "tvSeries",
				StartYear:    intPtr(2024),
			},
			MatchedAlias: "Paradise",
			NumVotes:     5000,
		},
		{
			Title: TitleSummary{
				Tconst:       "tt-primary",
				PrimaryTitle: "Paradise",
				TitleType:    "tvSeries",
				StartYear:    intPtr(2024),
			},
			NumVotes: 100,
		},
	})

	if !ok {
		t.Fatal("expected a match")
	}
	if best.Title.Tconst != "tt-primary" {
		t.Fatalf("expected exact primary match to win, got %s", best.Title.Tconst)
	}
	if meta.MatchReason != MatchReasonExactPrimaryTitleWithFilters {
		t.Fatalf("unexpected match reason %q", meta.MatchReason)
	}
}

func TestSelectBestTitleMatch_PrefersFilterMatchesBeforeVoteCount(t *testing.T) {
	t.Parallel()

	year := 2025
	best, meta, ok := SelectBestTitleMatch(ResolveTitleParams{
		Title:     "Paradise",
		TitleType: "tvSeries",
		Year:      &year,
	}, []ResolveCandidate{
		{
			Title: TitleSummary{
				Tconst:       "tt-high-votes",
				PrimaryTitle: "Paradise",
				TitleType:    "movie",
				StartYear:    intPtr(2020),
			},
			NumVotes: 100000,
		},
		{
			Title: TitleSummary{
				Tconst:       "tt-filter-match",
				PrimaryTitle: "Paradise",
				TitleType:    "tvSeries",
				StartYear:    intPtr(2025),
			},
			NumVotes: 50,
		},
	})

	if !ok {
		t.Fatal("expected a match")
	}
	if best.Title.Tconst != "tt-filter-match" {
		t.Fatalf("expected titleType/year match to win, got %s", best.Title.Tconst)
	}
	if meta.AppliedFilters.TitleType != "tvSeries" {
		t.Fatalf("unexpected applied titleType %#v", meta.AppliedFilters)
	}
	if meta.AppliedFilters.Year == nil || *meta.AppliedFilters.Year != 2025 {
		t.Fatalf("unexpected applied year %#v", meta.AppliedFilters)
	}
}

func TestSelectBestTitleMatch_PrefersNewestNonAdultWhenVotesTie(t *testing.T) {
	t.Parallel()

	best, _, ok := SelectBestTitleMatch(ResolveTitleParams{
		Title: "Paradise",
	}, []ResolveCandidate{
		{
			Title: TitleSummary{
				Tconst:       "tt-adult",
				PrimaryTitle: "Paradise",
				TitleType:    "movie",
				StartYear:    intPtr(2023),
				IsAdult:      true,
			},
			NumVotes: 200,
		},
		{
			Title: TitleSummary{
				Tconst:       "tt-clean",
				PrimaryTitle: "Paradise",
				TitleType:    "movie",
				StartYear:    intPtr(2024),
				IsAdult:      false,
			},
			NumVotes: 200,
		},
	})

	if !ok {
		t.Fatal("expected a match")
	}
	if best.Title.Tconst != "tt-clean" {
		t.Fatalf("expected newest non-adult title to win, got %s", best.Title.Tconst)
	}
}

func intPtr(v int) *int { return &v }
