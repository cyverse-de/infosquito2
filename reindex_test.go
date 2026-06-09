package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
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

// newMockICATTx creates an ICATTx backed by a sqlmock database for testing.
func newMockICATTx(t *testing.T) (*ICATTx, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	mock.ExpectBegin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("failed to begin mock tx: %v", err)
	}
	return &ICATTx{tx: tx}, mock
}

// mustJSON marshals v to a JSON string, failing the test on error.
func mustJSON(t *testing.T, v interface{}) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("failed to marshal JSON: %v", err)
	}
	return string(b)
}

// --- processDataobjects ---

func TestProcessDataobjects(t *testing.T) {
	baseDoc := ElasticsearchDocument{
		DocType:      "file",
		ID:           "obj-1",
		Path:         "/iplant/home/user/file.txt",
		Label:        "file.txt",
		Creator:      "user#iplant",
		FileType:     "generic",
		DateCreated:  1000,
		DateModified: 2000,
		FileSize:     512,
	}

	changedDoc := baseDoc
	changedDoc.FileSize = 1024

	successCases := []struct {
		name                   string
		dbRows                 [][]string // {id, json}
		avus                   map[string]string
		esDocs                 map[string]ElasticsearchDocument
		wantDataobjects        int64
		wantDataobjectsAdded   int64
		wantDataobjectsUpdated int64
		wantSeenIDs            []string
	}{
		{
			name:            "empty-result-set",
			dbRows:          nil,
			avus:            map[string]string{},
			esDocs:          map[string]ElasticsearchDocument{},
			wantDataobjects: 0,
		},
		{
			name:                 "new-document-indexed",
			dbRows:               [][]string{{"obj-1", mustJSON(t, baseDoc)}},
			avus:                 map[string]string{},
			esDocs:               map[string]ElasticsearchDocument{},
			wantDataobjects:      1,
			wantDataobjectsAdded: 1,
			wantSeenIDs:          []string{"obj-1"},
		},
		{
			name:                   "changed-document-updated",
			dbRows:                 [][]string{{"obj-1", mustJSON(t, changedDoc)}},
			avus:                   map[string]string{},
			esDocs:                 map[string]ElasticsearchDocument{"obj-1": baseDoc},
			wantDataobjects:        1,
			wantDataobjectsUpdated: 1,
			wantSeenIDs:            []string{"obj-1"},
		},
		{
			name:            "unchanged-document-no-action",
			dbRows:          [][]string{{"obj-1", mustJSON(t, baseDoc)}},
			avus:            map[string]string{},
			esDocs:          map[string]ElasticsearchDocument{"obj-1": baseDoc},
			wantDataobjects: 1,
			wantSeenIDs:     []string{"obj-1"},
		},
		{
			name:   "cyverse-metadata-merged",
			dbRows: [][]string{{"obj-1", mustJSON(t, baseDoc)}},
			avus: map[string]string{
				"obj-1": `{"cyverse":[{"attribute":"tag","value":"important","unit":""}]}`,
			},
			esDocs:               map[string]ElasticsearchDocument{},
			wantDataobjects:      1,
			wantDataobjectsAdded: 1,
			wantSeenIDs:          []string{"obj-1"},
		},
		{
			name:   "cyverse-metadata-triggers-update",
			dbRows: [][]string{{"obj-1", mustJSON(t, baseDoc)}},
			avus: map[string]string{
				"obj-1": `{"cyverse":[{"attribute":"tag","value":"important","unit":""}]}`,
			},
			esDocs:                 map[string]ElasticsearchDocument{"obj-1": baseDoc},
			wantDataobjects:        1,
			wantDataobjectsUpdated: 1,
			wantSeenIDs:            []string{"obj-1"},
		},
		{
			name: "multiple-documents-mixed-classification",
			dbRows: [][]string{
				{"obj-1", mustJSON(t, baseDoc)},
				{"obj-2", mustJSON(t, changedDoc)},
			},
			avus:                   map[string]string{},
			esDocs:                 map[string]ElasticsearchDocument{"obj-1": baseDoc},
			wantDataobjects:        2,
			wantDataobjectsAdded:   1, // obj-2 is new
			wantDataobjectsUpdated: 0, // obj-1 is unchanged
			wantSeenIDs:            []string{"obj-1", "obj-2"},
		},
	}

	for _, c := range successCases {
		t.Run(c.name, func(t *testing.T) {
			icatTx, mock := newMockICATTx(t)
			mockRows := sqlmock.NewRows([]string{"id", "json"})
			for _, r := range c.dbRows {
				mockRows.AddRow(r[0], r[1])
			}
			mock.ExpectQuery(".*").WillReturnRows(mockRows)

			var rows rowMetadata
			seenEsDocs := make(map[string]bool)
			indexer := newTestBulkIndexer(t)

			err := processDataobjects(context.Background(), log, &rows, c.avus, c.esDocs, seenEsDocs, indexer, &ESConnection{index: "test"}, icatTx, "iplant")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if rows.dataobjects != c.wantDataobjects {
				t.Errorf("rows.dataobjects = %d, want %d", rows.dataobjects, c.wantDataobjects)
			}
			if rows.dataobjectsAdded != c.wantDataobjectsAdded {
				t.Errorf("rows.dataobjectsAdded = %d, want %d", rows.dataobjectsAdded, c.wantDataobjectsAdded)
			}
			if rows.dataobjectsUpdated != c.wantDataobjectsUpdated {
				t.Errorf("rows.dataobjectsUpdated = %d, want %d", rows.dataobjectsUpdated, c.wantDataobjectsUpdated)
			}
			for _, id := range c.wantSeenIDs {
				if !seenEsDocs[id] {
					t.Errorf("seenEsDocs missing %q", id)
				}
			}
		})
	}

	t.Run("query-error", func(t *testing.T) {
		icatTx, mock := newMockICATTx(t)
		mock.ExpectQuery(".*").WillReturnError(fmt.Errorf("connection reset"))

		var rows rowMetadata
		seenEsDocs := make(map[string]bool)
		indexer := newTestBulkIndexer(t)

		err := processDataobjects(context.Background(), log, &rows, map[string]string{}, map[string]ElasticsearchDocument{}, seenEsDocs, indexer, &ESConnection{index: "test"}, icatTx, "iplant")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("scan-error", func(t *testing.T) {
		icatTx, mock := newMockICATTx(t)
		mockRows := sqlmock.NewRows([]string{"id"}).AddRow("obj-1")
		mock.ExpectQuery(".*").WillReturnRows(mockRows)

		var rows rowMetadata
		seenEsDocs := make(map[string]bool)
		indexer := newTestBulkIndexer(t)

		err := processDataobjects(context.Background(), log, &rows, map[string]string{}, map[string]ElasticsearchDocument{}, seenEsDocs, indexer, &ESConnection{index: "test"}, icatTx, "iplant")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("invalid-json", func(t *testing.T) {
		icatTx, mock := newMockICATTx(t)
		mockRows := sqlmock.NewRows([]string{"id", "json"}).AddRow("obj-1", `{not valid json}`)
		mock.ExpectQuery(".*").WillReturnRows(mockRows)

		var rows rowMetadata
		seenEsDocs := make(map[string]bool)
		indexer := newTestBulkIndexer(t)

		err := processDataobjects(context.Background(), log, &rows, map[string]string{}, map[string]ElasticsearchDocument{}, seenEsDocs, indexer, &ESConnection{index: "test"}, icatTx, "iplant")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("invalid-avu-json", func(t *testing.T) {
		icatTx, mock := newMockICATTx(t)
		mockRows := sqlmock.NewRows([]string{"id", "json"}).AddRow("obj-1", mustJSON(t, baseDoc))
		mock.ExpectQuery(".*").WillReturnRows(mockRows)

		var rows rowMetadata
		seenEsDocs := make(map[string]bool)
		indexer := newTestBulkIndexer(t)
		avus := map[string]string{"obj-1": `{broken`}

		err := processDataobjects(context.Background(), log, &rows, avus, map[string]ElasticsearchDocument{}, seenEsDocs, indexer, &ESConnection{index: "test"}, icatTx, "iplant")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("rows-err-after-iteration", func(t *testing.T) {
		icatTx, mock := newMockICATTx(t)
		mockRows := sqlmock.NewRows([]string{"id", "json"}).
			AddRow("obj-1", mustJSON(t, baseDoc)).
			RowError(0, fmt.Errorf("network timeout"))
		mock.ExpectQuery(".*").WillReturnRows(mockRows)

		var rows rowMetadata
		seenEsDocs := make(map[string]bool)
		indexer := newTestBulkIndexer(t)

		err := processDataobjects(context.Background(), log, &rows, map[string]string{}, map[string]ElasticsearchDocument{}, seenEsDocs, indexer, &ESConnection{index: "test"}, icatTx, "iplant")
		if err == nil {
			t.Fatal("expected error from rows.Err(), got nil")
		}
	})
}

// --- processCollections ---

func TestProcessCollections(t *testing.T) {
	baseDoc := ElasticsearchDocument{
		DocType:      "folder",
		ID:           "coll-1",
		Path:         "/iplant/home/user/myfolder",
		Label:        "myfolder",
		Creator:      "user#iplant",
		FileType:     "",
		DateCreated:  1000,
		DateModified: 2000,
		FileSize:     0,
	}

	changedDoc := baseDoc
	changedDoc.DateModified = 3000

	successCases := []struct {
		name             string
		dbRows           [][]string
		avus             map[string]string
		esDocs           map[string]ElasticsearchDocument
		wantColls        int64
		wantCollsAdded   int64
		wantCollsUpdated int64
		wantSeenIDs      []string
	}{
		{
			name:      "empty-result-set",
			dbRows:    nil,
			avus:      map[string]string{},
			esDocs:    map[string]ElasticsearchDocument{},
			wantColls: 0,
		},
		{
			name:           "new-collection-indexed",
			dbRows:         [][]string{{"coll-1", mustJSON(t, baseDoc)}},
			avus:           map[string]string{},
			esDocs:         map[string]ElasticsearchDocument{},
			wantColls:      1,
			wantCollsAdded: 1,
			wantSeenIDs:    []string{"coll-1"},
		},
		{
			name:             "changed-collection-updated",
			dbRows:           [][]string{{"coll-1", mustJSON(t, changedDoc)}},
			avus:             map[string]string{},
			esDocs:           map[string]ElasticsearchDocument{"coll-1": baseDoc},
			wantColls:        1,
			wantCollsUpdated: 1,
			wantSeenIDs:      []string{"coll-1"},
		},
		{
			name:        "unchanged-collection-no-action",
			dbRows:      [][]string{{"coll-1", mustJSON(t, baseDoc)}},
			avus:        map[string]string{},
			esDocs:      map[string]ElasticsearchDocument{"coll-1": baseDoc},
			wantColls:   1,
			wantSeenIDs: []string{"coll-1"},
		},
		{
			name:   "cyverse-metadata-merged",
			dbRows: [][]string{{"coll-1", mustJSON(t, baseDoc)}},
			avus: map[string]string{
				"coll-1": `{"cyverse":[{"attribute":"project","value":"genomics","unit":""}]}`,
			},
			esDocs:         map[string]ElasticsearchDocument{},
			wantColls:      1,
			wantCollsAdded: 1,
			wantSeenIDs:    []string{"coll-1"},
		},
		{
			name:   "cyverse-metadata-triggers-update",
			dbRows: [][]string{{"coll-1", mustJSON(t, baseDoc)}},
			avus: map[string]string{
				"coll-1": `{"cyverse":[{"attribute":"project","value":"genomics","unit":""}]}`,
			},
			esDocs:           map[string]ElasticsearchDocument{"coll-1": baseDoc},
			wantColls:        1,
			wantCollsUpdated: 1,
			wantSeenIDs:      []string{"coll-1"},
		},
		{
			name: "multiple-collections-mixed-classification",
			dbRows: [][]string{
				{"coll-1", mustJSON(t, baseDoc)},
				{"coll-2", mustJSON(t, changedDoc)},
			},
			avus:             map[string]string{},
			esDocs:           map[string]ElasticsearchDocument{"coll-1": baseDoc},
			wantColls:        2,
			wantCollsAdded:   1, // coll-2 is new
			wantCollsUpdated: 0, // coll-1 unchanged
			wantSeenIDs:      []string{"coll-1", "coll-2"},
		},
	}

	for _, c := range successCases {
		t.Run(c.name, func(t *testing.T) {
			icatTx, mock := newMockICATTx(t)
			mockRows := sqlmock.NewRows([]string{"id", "json"})
			for _, r := range c.dbRows {
				mockRows.AddRow(r[0], r[1])
			}
			mock.ExpectQuery(".*").WillReturnRows(mockRows)

			var rows rowMetadata
			seenEsDocs := make(map[string]bool)
			indexer := newTestBulkIndexer(t)

			err := processCollections(context.Background(), log, &rows, c.avus, c.esDocs, seenEsDocs, indexer, &ESConnection{index: "test"}, icatTx, "iplant")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if rows.colls != c.wantColls {
				t.Errorf("rows.colls = %d, want %d", rows.colls, c.wantColls)
			}
			if rows.collsAdded != c.wantCollsAdded {
				t.Errorf("rows.collsAdded = %d, want %d", rows.collsAdded, c.wantCollsAdded)
			}
			if rows.collsUpdated != c.wantCollsUpdated {
				t.Errorf("rows.collsUpdated = %d, want %d", rows.collsUpdated, c.wantCollsUpdated)
			}
			for _, id := range c.wantSeenIDs {
				if !seenEsDocs[id] {
					t.Errorf("seenEsDocs missing %q", id)
				}
			}
		})
	}

	t.Run("query-error", func(t *testing.T) {
		icatTx, mock := newMockICATTx(t)
		mock.ExpectQuery(".*").WillReturnError(fmt.Errorf("connection reset"))

		var rows rowMetadata
		seenEsDocs := make(map[string]bool)
		indexer := newTestBulkIndexer(t)

		err := processCollections(context.Background(), log, &rows, map[string]string{}, map[string]ElasticsearchDocument{}, seenEsDocs, indexer, &ESConnection{index: "test"}, icatTx, "iplant")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("scan-error", func(t *testing.T) {
		icatTx, mock := newMockICATTx(t)
		mockRows := sqlmock.NewRows([]string{"id"}).AddRow("coll-1")
		mock.ExpectQuery(".*").WillReturnRows(mockRows)

		var rows rowMetadata
		seenEsDocs := make(map[string]bool)
		indexer := newTestBulkIndexer(t)

		err := processCollections(context.Background(), log, &rows, map[string]string{}, map[string]ElasticsearchDocument{}, seenEsDocs, indexer, &ESConnection{index: "test"}, icatTx, "iplant")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("invalid-json", func(t *testing.T) {
		icatTx, mock := newMockICATTx(t)
		mockRows := sqlmock.NewRows([]string{"id", "json"}).AddRow("coll-1", `{not valid}`)
		mock.ExpectQuery(".*").WillReturnRows(mockRows)

		var rows rowMetadata
		seenEsDocs := make(map[string]bool)
		indexer := newTestBulkIndexer(t)

		err := processCollections(context.Background(), log, &rows, map[string]string{}, map[string]ElasticsearchDocument{}, seenEsDocs, indexer, &ESConnection{index: "test"}, icatTx, "iplant")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("invalid-avu-json", func(t *testing.T) {
		icatTx, mock := newMockICATTx(t)
		mockRows := sqlmock.NewRows([]string{"id", "json"}).AddRow("coll-1", mustJSON(t, baseDoc))
		mock.ExpectQuery(".*").WillReturnRows(mockRows)

		var rows rowMetadata
		seenEsDocs := make(map[string]bool)
		indexer := newTestBulkIndexer(t)
		avus := map[string]string{"coll-1": `{broken`}

		err := processCollections(context.Background(), log, &rows, avus, map[string]ElasticsearchDocument{}, seenEsDocs, indexer, &ESConnection{index: "test"}, icatTx, "iplant")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("rows-err-after-iteration", func(t *testing.T) {
		icatTx, mock := newMockICATTx(t)
		mockRows := sqlmock.NewRows([]string{"id", "json"}).
			AddRow("coll-1", mustJSON(t, baseDoc)).
			RowError(0, fmt.Errorf("network timeout"))
		mock.ExpectQuery(".*").WillReturnRows(mockRows)

		var rows rowMetadata
		seenEsDocs := make(map[string]bool)
		indexer := newTestBulkIndexer(t)

		err := processCollections(context.Background(), log, &rows, map[string]string{}, map[string]ElasticsearchDocument{}, seenEsDocs, indexer, &ESConnection{index: "test"}, icatTx, "iplant")
		if err == nil {
			t.Fatal("expected error from rows.Err(), got nil")
		}
	})
}
