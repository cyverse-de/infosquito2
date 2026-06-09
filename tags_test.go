package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// newMockDEDBTx creates a DEDBTx backed by a sqlmock database for testing.
func newMockDEDBTx(t *testing.T) (*DEDBTx, sqlmock.Sqlmock) {
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
	return &DEDBTx{tx: tx, schema: "public"}, mock
}

// --- processTags ---

func TestProcessTags(t *testing.T) {
	successCases := []struct {
		name         string
		dbRows       [][]string // each entry is {id, json}
		wantTags     int64
		wantSeenIDs  []string
	}{
		{
			name:        "empty-result-set",
			dbRows:      nil,
			wantTags:    0,
			wantSeenIDs: nil,
		},
		{
			name:        "single-tag",
			dbRows:      [][]string{{"tag-1", `{"id":"tag-1","doc_type":"tag","value":"test"}`}},
			wantTags:    1,
			wantSeenIDs: []string{"tag-1"},
		},
		{
			name: "multiple-tags",
			dbRows: [][]string{
				{"tag-1", `{"id":"tag-1","doc_type":"tag"}`},
				{"tag-2", `{"id":"tag-2","doc_type":"tag"}`},
				{"tag-3", `{"id":"tag-3","doc_type":"tag"}`},
			},
			wantTags:    3,
			wantSeenIDs: []string{"tag-1", "tag-2", "tag-3"},
		},
	}

	for _, c := range successCases {
		t.Run(c.name, func(t *testing.T) {
			deTx, mock := newMockDEDBTx(t)
			mockRows := sqlmock.NewRows([]string{"id", "json"})
			for _, r := range c.dbRows {
				mockRows.AddRow(r[0], r[1])
			}
			mock.ExpectQuery(".*").WillReturnRows(mockRows)

			var rows rowMetadata
			seenDocs := make(map[string]bool)
			indexer := newTestBulkIndexer(t)

			err := processTags(context.Background(), log, &rows, seenDocs, indexer, &ESConnection{index: "test"}, deTx, "iplant")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if rows.tags != c.wantTags {
				t.Errorf("rows.tags = %d, want %d", rows.tags, c.wantTags)
			}
			if rows.processed != c.wantTags {
				t.Errorf("rows.processed = %d, want %d", rows.processed, c.wantTags)
			}
			for _, id := range c.wantSeenIDs {
				if !seenDocs[id] {
					t.Errorf("seenDocs missing %q", id)
				}
			}
			if int64(len(seenDocs)) != c.wantTags {
				t.Errorf("len(seenDocs) = %d, want %d", len(seenDocs), c.wantTags)
			}
		})
	}

	t.Run("query-error", func(t *testing.T) {
		deTx, mock := newMockDEDBTx(t)
		mock.ExpectQuery(".*").WillReturnError(fmt.Errorf("connection refused"))

		var rows rowMetadata
		seenDocs := make(map[string]bool)
		indexer := newTestBulkIndexer(t)

		err := processTags(context.Background(), log, &rows, seenDocs, indexer, &ESConnection{index: "test"}, deTx, "iplant")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if rows.tags != 0 {
			t.Errorf("rows.tags = %d, want 0", rows.tags)
		}
	})

	t.Run("scan-error", func(t *testing.T) {
		deTx, mock := newMockDEDBTx(t)
		// Return a row with the wrong number of columns to trigger a scan error.
		mockRows := sqlmock.NewRows([]string{"id"}).AddRow("tag-1")
		mock.ExpectQuery(".*").WillReturnRows(mockRows)

		var rows rowMetadata
		seenDocs := make(map[string]bool)
		indexer := newTestBulkIndexer(t)

		err := processTags(context.Background(), log, &rows, seenDocs, indexer, &ESConnection{index: "test"}, deTx, "iplant")
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if rows.tags != 0 {
			t.Errorf("rows.tags = %d, want 0", rows.tags)
		}
	})

	t.Run("rows-err-after-iteration", func(t *testing.T) {
		deTx, mock := newMockDEDBTx(t)
		mockRows := sqlmock.NewRows([]string{"id", "json"}).
			AddRow("tag-1", `{"id":"tag-1","doc_type":"tag"}`).
			RowError(0, fmt.Errorf("network timeout"))
		mock.ExpectQuery(".*").WillReturnRows(mockRows)

		var rows rowMetadata
		seenDocs := make(map[string]bool)
		indexer := newTestBulkIndexer(t)

		err := processTags(context.Background(), log, &rows, seenDocs, indexer, &ESConnection{index: "test"}, deTx, "iplant")
		if err == nil {
			t.Fatal("expected error from rows.Err(), got nil")
		}
	})
}

// --- processTagDeletions ---

func TestProcessTagDeletions(t *testing.T) {
	cases := []struct {
		name            string
		esDocs          map[string]ElasticsearchTag
		seenDocs        map[string]bool
		wantTagsRemoved int64
	}{
		{
			name:            "all-seen-no-deletions",
			esDocs:          map[string]ElasticsearchTag{"tag-1": {ID: "tag-1"}},
			seenDocs:        map[string]bool{"tag-1": true},
			wantTagsRemoved: 0,
		},
		{
			name:            "one-unseen-tag-deleted",
			esDocs:          map[string]ElasticsearchTag{"tag-1": {ID: "tag-1"}},
			seenDocs:        map[string]bool{},
			wantTagsRemoved: 1,
		},
		{
			name: "multiple-unseen-tags-deleted",
			esDocs: map[string]ElasticsearchTag{
				"tag-1": {ID: "tag-1"},
				"tag-2": {ID: "tag-2"},
				"tag-3": {ID: "tag-3"},
			},
			seenDocs:        map[string]bool{},
			wantTagsRemoved: 3,
		},
		{
			name: "mix-of-seen-and-unseen",
			esDocs: map[string]ElasticsearchTag{
				"tag-1": {ID: "tag-1"},
				"tag-2": {ID: "tag-2"},
				"tag-3": {ID: "tag-3"},
			},
			seenDocs:        map[string]bool{"tag-2": true},
			wantTagsRemoved: 2,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var rows rowMetadata
			indexer := newTestBulkIndexer(t)

			err := processTagDeletions(context.Background(), log, &rows, c.esDocs, c.seenDocs, indexer, &ESConnection{index: "test"})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if rows.tagsRemoved != c.wantTagsRemoved {
				t.Errorf("tagsRemoved = %d, want %d", rows.tagsRemoved, c.wantTagsRemoved)
			}
		})
	}
}
