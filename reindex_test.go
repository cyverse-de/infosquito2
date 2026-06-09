package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/cyverse-de/esutils/v3"
	"github.com/olivere/elastic/v7"
	"github.com/sirupsen/logrus"
)

// newTestBulkIndexer creates a BulkIndexer backed by a no-op ES client
// suitable for unit tests. The client is configured to never make network
// calls: no health checks, no sniffing, and a large bulk size so Add never
// triggers a Flush.
func newTestBulkIndexer(t *testing.T) *esutils.BulkIndexer {
	t.Helper()
	es, err := elastic.NewSimpleClient(
		elastic.SetURL("http://localhost:19200"),
		elastic.SetHealthcheck(false),
		elastic.SetSniff(false),
	)
	if err != nil {
		t.Fatalf("failed to create test ES client: %v", err)
	}
	return esutils.NewBulkIndexerContext(context.Background(), es, 100000)
}

// --- classify ---

func TestClassify(t *testing.T) {
	doc := ElasticsearchDocument{ID: "abc", Path: "/a", FileSize: 1}
	docChanged := ElasticsearchDocument{ID: "abc", Path: "/a", FileSize: 2}

	cases := []struct {
		name     string
		id       string
		doc      ElasticsearchDocument
		esDocs   map[string]ElasticsearchDocument
		expected DocumentClassification
	}{
		{
			name:     "not-in-es-returns-IndexDocument",
			id:       "abc",
			doc:      doc,
			esDocs:   map[string]ElasticsearchDocument{},
			expected: IndexDocument,
		},
		{
			name:     "in-es-and-equal-returns-NoAction",
			id:       "abc",
			doc:      doc,
			esDocs:   map[string]ElasticsearchDocument{"abc": doc},
			expected: NoAction,
		},
		{
			name:     "in-es-but-different-returns-UpdateDocument",
			id:       "abc",
			doc:      docChanged,
			esDocs:   map[string]ElasticsearchDocument{"abc": doc},
			expected: UpdateDocument,
		},
		{
			name:     "different-id-not-in-es-returns-IndexDocument",
			id:       "xyz",
			doc:      doc,
			esDocs:   map[string]ElasticsearchDocument{"abc": doc},
			expected: IndexDocument,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := classify(c.id, c.doc, c.esDocs)
			if got != c.expected {
				t.Errorf("classify() = %v, want %v", got, c.expected)
			}
		})
	}
}

// --- processDeletions ---

func TestProcessDeletions(t *testing.T) {
	cases := []struct {
		name               string
		esDocs             map[string]ElasticsearchDocument
		esDocTypes         map[string]string
		seenEsDocs         map[string]bool
		wantDataObjRemoved int64
		wantCollsRemoved   int64
		wantLogContains    string // non-empty means we expect this string in log output
	}{
		{
			name:               "all-seen-no-deletions",
			esDocs:             map[string]ElasticsearchDocument{"a": {ID: "a"}},
			esDocTypes:         map[string]string{"a": "file"},
			seenEsDocs:         map[string]bool{"a": true},
			wantDataObjRemoved: 0,
			wantCollsRemoved:   0,
		},
		{
			name:               "unseen-file-deleted",
			esDocs:             map[string]ElasticsearchDocument{"a": {ID: "a"}},
			esDocTypes:         map[string]string{"a": "file"},
			seenEsDocs:         map[string]bool{},
			wantDataObjRemoved: 1,
			wantCollsRemoved:   0,
		},
		{
			name:               "unseen-folder-deleted",
			esDocs:             map[string]ElasticsearchDocument{"b": {ID: "b"}},
			esDocTypes:         map[string]string{"b": "folder"},
			seenEsDocs:         map[string]bool{},
			wantDataObjRemoved: 0,
			wantCollsRemoved:   1,
		},
		{
			name:               "missing-doctype-falls-back-to-file",
			esDocs:             map[string]ElasticsearchDocument{"c": {ID: "c"}},
			esDocTypes:         map[string]string{},
			seenEsDocs:         map[string]bool{},
			wantDataObjRemoved: 1, // counted as file due to fallback
			wantCollsRemoved:   0,
			wantLogContains:    "making rash assumptions",
		},
		{
			name: "mix-of-seen-and-unseen",
			esDocs: map[string]ElasticsearchDocument{
				"file1": {ID: "file1"},
				"dir1":  {ID: "dir1"},
				"file2": {ID: "file2"},
			},
			esDocTypes: map[string]string{
				"file1": "file",
				"dir1":  "folder",
				"file2": "file",
			},
			seenEsDocs:         map[string]bool{"file1": true},
			wantDataObjRemoved: 1, // file2
			wantCollsRemoved:   1, // dir1
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Redirect logrus output so we can assert on log messages.
			var buf bytes.Buffer
			origOut := logrus.StandardLogger().Out
			logrus.SetOutput(&buf)
			defer logrus.SetOutput(origOut)

			var rows rowMetadata
			indexer := newTestBulkIndexer(t)

			err := processDeletions(context.Background(), log, &rows, c.esDocs, c.esDocTypes, c.seenEsDocs, indexer, &ESConnection{index: "test"})
			if err != nil {
				t.Fatalf("processDeletions() returned unexpected error: %v", err)
			}

			if rows.dataobjectsRemoved != c.wantDataObjRemoved {
				t.Errorf("dataobjectsRemoved = %d, want %d", rows.dataobjectsRemoved, c.wantDataObjRemoved)
			}
			if rows.collsRemoved != c.wantCollsRemoved {
				t.Errorf("collsRemoved = %d, want %d", rows.collsRemoved, c.wantCollsRemoved)
			}
			if c.wantLogContains != "" && !bytes.Contains(buf.Bytes(), []byte(c.wantLogContains)) {
				t.Errorf("expected log output to contain %q, got: %s", c.wantLogContains, buf.String())
			}
		})
	}
}
