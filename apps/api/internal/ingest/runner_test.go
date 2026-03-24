package ingest

import (
	"bytes"
	"io"
	"log"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestTransformTSVToCopyCSVSkipsHeaderAndEscapesQuotes(t *testing.T) {
	t.Parallel()

	input := strings.NewReader(strings.Join([]string{
		"tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres",
		`tt0073045	movie	"Giliap"	"Giliap"	0	1975	\N	137	Crime,Drama`,
		"tt0000001\tshort\tCarmencita\tCarmencita\t0\t1894\t\\N\t1\tDocumentary,Short",
	}, "\n") + "\n")

	var out bytes.Buffer
	if err := transformTSVToCopyCSV(input, &out, 9); err != nil {
		t.Fatalf("transformTSVToCopyCSV returned error: %v", err)
	}

	got := out.String()
	if strings.Contains(got, "tconst\ttitleType") {
		t.Fatalf("expected header row to be removed, got %q", got)
	}
	if !strings.Contains(got, "\"\"\"Giliap\"\"\"") {
		t.Fatalf("expected quoted title to be CSV-escaped, got %q", got)
	}
	if !strings.Contains(got, "tt0000001\tshort\tCarmencita") {
		t.Fatalf("expected second record to be preserved, got %q", got)
	}
}

func TestDownloadProgressReaderLogsProgressAndCompletion(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	reader := &downloadProgressReader{
		reader:       strings.NewReader("abcdef"),
		logger:       log.New(&logs, "", 0),
		datasetName:  "title.ratings.tsv.gz",
		contentBytes: 6,
		logEvery:     3,
		startedAt:    time.Unix(0, 0),
	}

	buf := make([]byte, 2)
	for {
		_, err := reader.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("read returned error: %v", err)
		}
	}

	got := logs.String()
	if !strings.Contains(got, "download progress title.ratings.tsv.gz: 4/6 bytes") {
		t.Fatalf("expected progress log, got %q", got)
	}
	if !strings.Contains(got, "download complete title.ratings.tsv.gz: 6/6 bytes") {
		t.Fatalf("expected completion log, got %q", got)
	}
}

func TestLoadNamesStatementSkipsRowsMissingRequiredColumns(t *testing.T) {
	t.Parallel()

	statement := loadNamesStatement(42, "staging_name_basics_raw")
	if !strings.Contains(statement, "FROM staging_name_basics_raw") {
		t.Fatalf("expected names statement to read from staging_name_basics_raw, got %q", statement)
	}
	if !strings.Contains(statement, "WHERE nconst IS NOT NULL") {
		t.Fatalf("expected names statement to guard against null nconst, got %q", statement)
	}
	if !strings.Contains(statement, "AND primary_name IS NOT NULL") {
		t.Fatalf("expected names statement to guard against null primary_name, got %q", statement)
	}
}

func TestCreateRawTableStatementUsesUnloggedTables(t *testing.T) {
	t.Parallel()

	statement := createRawTableStatement("staging_title_basics_raw_7", "(tconst TEXT)")
	if !strings.Contains(statement, "CREATE UNLOGGED TABLE staging_title_basics_raw_7") {
		t.Fatalf("expected unlogged raw table creation, got %q", statement)
	}
}

func TestSelectSyncMode(t *testing.T) {
	t.Parallel()

	if got := selectSyncMode(false, false); got != syncModeFullRefresh {
		t.Fatalf("expected initial sync to use full refresh, got %q", got)
	}
	if got := selectSyncMode(true, false); got != syncModeDelta {
		t.Fatalf("expected recurring sync to use delta, got %q", got)
	}
	if got := selectSyncMode(true, true); got != syncModeFullRefresh {
		t.Fatalf("expected force flag to select full refresh, got %q", got)
	}
}

func TestIndexBuildPlanDefersOnlyRemainingTrigrams(t *testing.T) {
	t.Parallel()

	base, deferred := buildIndexPlans(tableSet{
		Titles:           "titles_shadow_7",
		TitleRatings:     "title_ratings_shadow_7",
		TitleEpisodes:    "title_episodes_shadow_7",
		Names:            "names_shadow_7",
		TitlePrincipals:  "title_principals_shadow_7",
		TitleCrewMembers: "title_crew_members_shadow_7",
		TitleAkas:        "title_akas_shadow_7",
	})

	baseNames := make([]string, 0, len(base))
	for _, item := range base {
		baseNames = append(baseNames, item.name)
	}
	if slices.Contains(baseNames, "index titles original trgm") {
		t.Fatalf("did not expect original title trigram index in base plan: %v", baseNames)
	}
	if slices.Contains(baseNames, "index names primary trgm") {
		t.Fatalf("did not expect names trigram index in base plan: %v", baseNames)
	}

	deferredNames := make([]string, 0, len(deferred))
	for _, item := range deferred {
		deferredNames = append(deferredNames, item.name)
	}
	expected := []string{"index titles primary trgm", "index akas title trgm"}
	if !slices.Equal(deferredNames, expected) {
		t.Fatalf("expected deferred trigram indexes %v, got %v", expected, deferredNames)
	}
}
