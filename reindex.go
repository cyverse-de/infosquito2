package main

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"gopkg.in/olivere/elastic.v5"
)

type Metadatum struct {
	Attribute string `json:"attribute"`
	Value     string `json:"value"`
	Unit      string `json:"unit"`
}

type UserPermission struct {
	User       string `json:"user"`
	Permission string `json:"permission"`
}

type ElasticsearchDocument struct {
	Id              string           `json:"id"`
	Path            string           `json:"path"`
	Label           string           `json:"label"`
	Creator         string           `json:"creator"`
	FileType        string           `json:"fileType"`
	DateCreated     int64            `json:"dateCreated"`
	DateModified    int64            `json:"dateModified"`
	FileSize        int64            `json:"fileSize"`
	Metadata        []Metadatum      `json:"metadata"`
	UserPermissions []UserPermission `json:"userPermissions"`
}

func logTime(prefixlog *logrus.Entry, start time.Time, rows *struct{ rows int64 }) {
	prefixlog.Infof("Processed %d entries in %s", rows.rows, time.Since(start).String())
}

func ReindexPrefix(db *ICATConnection, es *ESConnection, prefix string) error {
	var rows struct{ rows int64 }
	prefixlog := log.WithFields(logrus.Fields{
		"prefix": prefix,
	})

	start := time.Now()
	defer logTime(prefixlog, start, &rows)

	tx, err := db.BeginTx(context.TODO(), nil)
	if err != nil {
		return err
	}
	defer tx.tx.Rollback()

	r, err := tx.CreateTemporaryTable("object_uuids", "SELECT map.object_id as object_id, meta.meta_attr_value as id FROM r_objt_metamap map JOIN r_meta_main meta ON map.meta_id = meta.meta_id WHERE meta.meta_attr_name = 'ipc_UUID' AND meta.meta_attr_value ILIKE $1 || '%'", prefix)
	rows.rows = r
	if err != nil {
		return err
	}

	prefixlog.Infof("Got %d rows for prefix %s (note that this may include stale unused metadata)", rows.rows, prefix)

	// Resplit if relevant -- maybe throw error?
	if rows.rows == 0 {
		return nil
	}

	prefixQuery := elastic.NewBoolQuery().MinimumNumberShouldMatch(1).Should(elastic.NewPrefixQuery("id", strings.ToUpper(prefix)), elastic.NewPrefixQuery("id", strings.ToLower(prefix)))

	searchService := es.es.Search(es.index).Type("file", "folder").Query(prefixQuery).Sort("id", true).Size(maxInPrefix)
	search, err := searchService.Do(context.TODO())
	if err != nil {
		return err
	}

	prefixlog.Infof("Got %d documents for prefix %s (ES)", search.Hits.TotalHits, prefix)

	for _, hit := range search.Hits.Hits {
		// json.RawMessage's MarshalJSON can't actually throw an error, it's just matching a function signature
		var doc ElasticsearchDocument
		b, _ := hit.Source.MarshalJSON()
		err := json.Unmarshal(b, &doc)
		if err != nil {
			return err
		}

		b2, err := json.Marshal(doc)
		if err != nil {
			return err
		}

		prefixlog.Info(string(b2))
	}

	// Set up other temp tables

	// fetch everything in prefix from ES (files & folders)
	// parallel scroll data objects in prefix:
	// - sort IDs from both queries
	// - each tick, fetch the next ID from each:
	// - - if in ES only, note the ID, get next ES ID & compare to same DB ID
	// - - if in DB only, index, get next DB ID and compare to same ES ID
	// - - if in both, compare values, index if different
	// parallel scroll collections in prefix:
	// - sort IDs, using the noted IDs for the ES side instead of the query directly
	// - each tick, fetch the next ID from each:
	// - - same as above, but delete from ES if it's not in the DB -- since we used the noted IDs this means the ID is neither a file nor a folder

	return nil
}
