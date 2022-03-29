package main

import (
	"context"
	"fmt"
	"time"

	"database/sql"

	"github.com/cyverse-de/dbutil"

	_ "github.com/lib/pq"
)

// ICATConnection wraps a sql.DB for the ICAT
type ICATConnection struct {
	db *sql.DB
}

// ICATTx wraps a sql.Tx for the ICAT
type ICATTx struct {
	tx *sql.Tx
}

// SetupDB initializes an ICATConnection for the given dbURI
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

	db.SetMaxOpenConns(10)
	db.SetConnMaxIdleTime(time.Minute)

	return &ICATConnection{db: db}, nil
}

// BeginTx starts an ICATTx for the given ICATConnection
func (d *ICATConnection) BeginTx(ctx context.Context, opts *sql.TxOptions) (*ICATTx, error) {
	tx, err := d.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	stats := d.db.Stats()
	log.Infof("ICAT stats: %d/%d open; %d/%d used/idle", stats.OpenConnections, stats.MaxOpenConnections, stats.InUse, stats.Idle)
	return &ICATTx{tx: tx}, nil
}

// CreateTemporaryTable creates a temporary table set to ON COMMIT DROP for the given name and query on the given ICATTx
func (tx *ICATTx) CreateTemporaryTable(ctx context.Context, name string, query string, args ...interface{}) (int64, error) {
	res, err := tx.tx.ExecContext(ctx, fmt.Sprintf("CREATE TEMPORARY TABLE %s ON COMMIT DROP AS %s", name, query), args...)
	if err != nil {
		return 0, err
	}
	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return 0, err
	}

	_, err = tx.tx.ExecContext(ctx, fmt.Sprintf("ANALYZE %s", name))
	if err != nil {
		return 0, err
	}

	return rowsAffected, nil
}

// GetDataObjects returns a sql.Rows for data objects using the temporary tables which should already be set up
func (tx *ICATTx) GetDataObjects(ctx context.Context, uuidTable string, permsTable string, metaTable string, folderBase string) (*sql.Rows, error) {
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
 WHERE c.coll_name LIKE '/%[4]s/%%' AND d1.data_repl_num = (SELECT min(d2.data_repl_num) FROM r_data_main d2 WHERE d2.data_id = d1.data_id)) q ORDER BY id`, uuidTable, permsTable, metaTable, folderBase)

	return tx.tx.QueryContext(ctx, query)
}

// GetCollections returns a sql.Rows for collections using the temporary tables which should already be set up
func (tx *ICATTx) GetCollections(ctx context.Context, uuidTable string, permsTable string, metaTable string, folderBase string) (*sql.Rows, error) {
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
 WHERE coll_name LIKE '/%[4]s/%%' and coll_type = '') q ORDER BY id`, uuidTable, permsTable, metaTable, folderBase)

	return tx.tx.QueryContext(ctx, query)
}
