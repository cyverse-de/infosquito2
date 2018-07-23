package main

import (
	"context"
	"fmt"

	"database/sql"
	"github.com/cyverse-de/dbutil"

	_ "github.com/lib/pq"
)

type ICATConnection struct {
	db *sql.DB
}

type ICATTx struct {
	tx *sql.Tx
}

func SetupDB(dbURI string) (*ICATConnection, error) {
	connector, err := dbutil.NewDefaultConnector("1m")
	if err != nil {
		return nil, err
	}

	db, err := connector.Connect("postgres", dbURI)
	if err != nil {
		return nil, err
	}

	log.Info("Connected to the database")

	if err = db.Ping(); err != nil {
		return nil, err
	}

	log.Info("Successfully pinged the database")
	return &ICATConnection{db: db}, nil
}

func (d *ICATConnection) BeginTx(ctx context.Context, opts *sql.TxOptions) (*ICATTx, error) {
	tx, err := d.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &ICATTx{tx: tx}, nil
}

func (tx *ICATTx) CreateTemporaryTable(name string, query string, args ...interface{}) (int64, error) {
	res, err := tx.tx.Exec(fmt.Sprintf("CREATE TEMPORARY TABLE %s ON COMMIT DROP AS %s", name, query), args...)
	if err != nil {
		return 0, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	_, err = tx.tx.Exec(fmt.Sprintf("ANALYZE %s", name))
	if err != nil {
		return 0, err
	}

	return rowsAffected, nil
}

func (tx *ICATTx) GetDataObjects(uuidTable string, permsTable string, metaTable string) (*sql.Rows, error) {
	query := fmt.Sprintf(`SELECT id, to_json(q.*) FROM (
SELECT ou.id "id",
       (c.coll_name || '/' || d1.data_name) "path",
       d1.data_name "label",
       (d1.data_owner_name || '#' || d1.data_owner_zone) "creator",
       cast(d1.create_ts AS BIGINT)*1000 AS "dateCreated",
       cast(d1.modify_ts AS BIGINT)*1000 AS "dateModified",
       d1.data_size                      AS "fileSize",
       d1.data_type_name                 AS "fileType",
       op."userPermissions"              AS "userPermissions",
       om.metadata                       AS "metadata"
  FROM r_data_main d1
  JOIN r_coll_main c USING (coll_id)
  JOIN %[1]s ou on d1.data_id = ou.object_id
  LEFT JOIN %[2]s op USING (object_id)
  LEFT JOIN %[3]s om USING (object_id)
 WHERE c.coll_name LIKE '/iplant/%%' AND d1.data_repl_num = (SELECT min(d2.data_repl_num) FROM r_data_main d2 WHERE d2.data_id = d1.data_id)) q ORDER BY id`, uuidTable, permsTable, metaTable)

	return tx.tx.Query(query)
}

func (tx *ICATTx) GetCollections(uuidTable string, permsTable string, metaTable string) (*sql.Rows, error) {
	query := fmt.Sprintf(`SELECT id, to_json(q.*) FROM (
SELECT ou.id "id",
       coll_name "path",
       REPLACE(coll_name, parent_coll_name || '/', '') "label",
       (coll_owner_name || '#' || coll_owner_zone) "creator",
       cast(create_ts AS BIGINT)*1000 AS "dateCreated",
       cast(modify_ts AS BIGINT)*1000 AS "dateModified",
       0                                 AS "fileSize",
       ''                                AS "fileType",
       op."userPermissions"              AS "userPermissions",
       om.metadata                       AS "metadata"
  FROM r_coll_main c
  JOIN %[1]s ou on coll_id = ou.object_id
  LEFT JOIN %[2]s op USING (object_id)
  LEFT JOIN %[3]s om USING (object_id)
 WHERE coll_name LIKE '/iplant/%%' and coll_type = '') q ORDER BY id`, uuidTable, permsTable, metaTable)

	return tx.tx.Query(query)
}
