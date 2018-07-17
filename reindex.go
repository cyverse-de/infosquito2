package main

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"gopkg.in/olivere/elastic.v5"
)

func logTime(prefixlog *logrus.Entry, start time.Time, rows *struct {
	rows        int64
	processed   int64
	dataobjects int64
	colls       int64
}) {
	prefixlog.Infof("Processed %d entries (of %d rows, %d data objects, %d colls) in %s", rows.processed, rows.rows, rows.dataobjects, rows.colls, time.Since(start).String())
}

func ReindexPrefix(db *ICATConnection, es *ESConnection, prefix string) error {
	var rows struct {
		rows        int64
		processed   int64
		dataobjects int64
		colls       int64
	}
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

	r, err := tx.CreateTemporaryTable("object_uuids", "SELECT map.object_id as object_id, lower(meta.meta_attr_value) as id FROM r_objt_metamap map JOIN r_meta_main meta ON map.meta_id = meta.meta_id WHERE meta.meta_attr_name = 'ipc_UUID' AND meta.meta_attr_value ILIKE $1 || '%'", prefix)
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

		_, err = json.Marshal(doc)
		if err != nil {
			return err
		}

		//prefixlog.Info(string(b2))
	}

	r, err = tx.CreateTemporaryTable("object_perms", `select object_id, json_agg(format('{"user": %s, "permission": %s}', to_json(u.user_name || '#' || u.zone_name), (
                                 CASE a.access_type_id
                                   WHEN 1050 THEN to_json('read'::text)
                                   WHEN 1120 THEN to_json('write'::text)
                                   WHEN 1200 THEN to_json('own'::text)
                                   ELSE 'null'::json
                                 END))::json ORDER BY u.user_name, u.zone_name) AS "userPermissions" from r_objt_access a join r_user_main u on (a.user_id = u.user_id) where a.object_id IN (select object_id from object_uuids) group by object_id`)
	if err != nil {
		return err
	}

	prefixlog.Infof("Got %d rows for perms", r)

	r, err = tx.CreateTemporaryTable("object_metadata", `select object_id, json_agg(format('{"attribute": %s, "value": %s, "unit": %s}',
                        coalesce(to_json(m2.meta_attr_name), 'null'::json),
                        coalesce(to_json(m2.meta_attr_value), 'null'::json),
                        coalesce(to_json(m2.meta_attr_unit), 'null'::json))::json ORDER BY meta_attr_name, meta_attr_value, meta_attr_unit)
                       AS "metadata" from r_objt_metamap map2 left join r_meta_main m2 on map2.meta_id = m2.meta_id where m2.meta_attr_name <> 'ipc_UUID' and object_id IN (select object_id from object_uuids) group by object_id`)
	if err != nil {
		return err
	}

	prefixlog.Infof("Got %d rows for metadata", r)

	dataobjects, err := tx.GetDataObjects("object_uuids", "object_perms", "object_metadata")
	if err != nil {
		return err
	}
	for dataobjects.Next() {
		var id, selectedJson string
		err = dataobjects.Scan(&id, &selectedJson)
		if err != nil {
			dataobjects.Close()
			return err
		}

		//prefixlog.Info(selectedJson)
		rows.processed++
		rows.dataobjects++
	}
	dataobjects.Close()

	colls, err := tx.GetCollections("object_uuids", "object_perms", "object_metadata")
	if err != nil {
		return err
	}
	for colls.Next() {
		var id, selectedJson string
		err = colls.Scan(&id, &selectedJson)
		if err != nil {
			colls.Close()
			return err
		}

		//prefixlog.Info(selectedJson)
		rows.processed++
		rows.colls++
	}
	colls.Close()

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
