package ingest

import (
	"bytes"
	"strings"
	"testing"
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
