package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/olivere/elastic/v7"
	"go.opentelemetry.io/otel"
)

type ElasticsearchTag struct {
	DocType      string `json:"doc_type"`
	ID           string `json:"id"`
	Value        string `json:"value"`
	Description  string `json:"description"`
	Creator      string `json:"creator"`
	FileType     string `json:"fileType"`
	DateCreated  int64  `json:"dateCreated"`
	DateModified int64  `json:"dateModified"`
}

func getIndexedTags(context context.Context, es *ESConnection) (int64, map[string]ElasticsearchTag, error) {
	ctx, span := otel.Tracer(otelName).Start(context, "getIndexedTags")
	defer span.End()

	docs = make(map[string]ElasticsearchTag)

	query := elastic.NewBoolQuery().
		Must(elastic.NewTermQuery("doc_type", "tag"))

	searchService := es.es.Search(es.index).Query(query).Sort("id", true)
	search, err := searchService.Do(ctx)
	if err != nil {
		return 0, nil, err
	}

	for _, hit := range search.Hits.Hits {
		var doc ElasticsearchTag
		b, _ := hit.Source.MarshalJSON()
		err := json.Unmarshal(b, &doc)
		if err != nil {
			// if it broke, just reindex the thing
			continue
		}

		docs[hit.Id] = doc
	}
	return search.TotalHits(), docs, nil
}

// ReindexTags attempts to reindex tags given a DB and ES connection
func ReindexTags(context context.Context, db *DEDBConnection, es *ESConnection, irodsZone string) error {
	ctx, span := otel.Tracer(otelName).Start(context, "ReindexTags")
	defer span.End()

	start := time.Now()

	seenDocs := make(map[string]bool)
	// purge missing tags & index extant tags
	docs, esDocs, err := getIndexedTags(ctx, es)
	if err != nil {
		return err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	rb := func() {
		err := tx.tx.Rollback()
		if err != nil && err.Error() != "sql: transaction has already been committed or rolled back" {
			log.Debugf("Failed rolling back transaction: %s", err.Error())
		}
	}
	defer rb()

	indexer := es.NewBulkIndexer(ctx, 1000)
	defer indexer.Flush()

	tags, err := tx.GetTags(ctx)
	if err != nil {
		return err
	}
	defer tags.Close()
	for tags.Next() {
	}

	return nil
}
