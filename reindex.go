package main

import (
	"context"
	"fmt"
)

func ReindexPrefix(db *ICATConnection, es *ESConnection, prefix string) error {
	tx, err := db.BeginTx(context.TODO(), nil)
	if err != nil {
		return err
	}

	rows, err := tx.CreateTemporaryTable("object_uuids", "SELECT map.object_id as object_id, meta.meta_attr_value as id FROM r_objt_metamap map JOIN r_meta_main meta ON map.meta_id = meta.meta_id WHERE meta.meta_attr_name = 'ipc_UUID' AND meta.meta_attr_value ILIKE $1 || '%'", prefix)
	if err != nil {
		return err
	}

	log.Infof("Got %d rows for prefix %s (note that this may include stale unused metadata)", rows, prefix)

	// Resplit if relevant -- maybe throw error?

	escount, err := es.es.Count(es.index).Type("file", "folder").BodyString(fmt.Sprintf(`{"query": {"prefix": {"id": "%s"}}}`, prefix)).Do(context.TODO())
	if err != nil {
		return err
	}

	log.Infof("Got %d documents for prefix %s (ES)", escount, prefix)

	// Set up other temp tables

	// parallel scroll data objects in prefix
	// parallel scroll collections in prefix

	tx.tx.Rollback()

	return nil
}
