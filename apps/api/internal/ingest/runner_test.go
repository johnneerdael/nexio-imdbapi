package ingest

import (
	"bytes"
	"io"
	"log"
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
