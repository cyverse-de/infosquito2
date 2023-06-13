package main

import (
	"context"
	"fmt"
	"time"

	"database/sql"

	"github.com/cyverse-de/dbutil"

	_ "github.com/lib/pq"
)

// DEDBConnection wraps a sql.DB for the DEDB
type DEDBConnection struct {
	db     *sql.DB
	schema string
}

// DEDBTx wraps a sql.Tx for the DEDB
type DEDBTx struct {
	tx     *sql.Tx
	schema string
}

// SetupDEDB initializes an DEDBConnection for the given dbURI
func SetupDEDB(dbURI, schema string) (*DEDBConnection, error) {
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

	return &DEDBConnection{db: db, schema: schema}, nil
}

// BeginTx starts an DEDBTx for the given DEDBConnection
func (d *DEDBConnection) BeginTx(ctx context.Context, opts *sql.TxOptions) (*DEDBTx, error) {
	tx, err := d.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}

	stats := d.db.Stats()
	log.Infof("DEDB stats: %d/%d open; %d/%d used/idle", stats.OpenConnections, stats.MaxOpenConnections, stats.InUse, stats.Idle)
	return &DEDBTx{tx: tx, schema: d.schema}, nil
}

// CreateTemporaryTable creates a temporary table set to ON COMMIT DROP for the given name and query on the given DEDBTx
func (tx *DEDBTx) CreateTemporaryTable(ctx context.Context, name string, query string, args ...interface{}) (int64, error) {
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

// GetTags returns a sql.Rows for all tags
func (tx *DEDBTx) GetTags(ctx context.Context, irodsZone string) (*sql.Rows, error) {
	query = fmt.Sprintf(`WITH attached (tag_id, targets) AS (
SELECT tag_id, 
       json_agg(format('{"id": %s, "type": %s}',
           coalesce(to_json(a_t.target_id::text), 'null'::json),
	   coalesce(to_json(a_t.target_type::text), 'null'::json))::json) "targets"
  FROM attached_tags a_t WHERE a_t.target_type IN ('file', 'folder') GROUP BY a_t.tag_id
)
SELECT id, to_json(q.*) FROM (
SELECT t.id::text,
       t.value,
       t.description,
       t.owner_id || '#%[1]s' "creator",
       t.created_on "dateCreated",
       t.modified_on "dateModified",
       coalesce(a_t.targets, json_build_array()) "targets"
  FROM %[2]s.tags t
  LEFT JOIN attached ON (t.id = attached.tag_id)
  WHERE a_t.target_type IN ('file', 'folder')
        ) q ORDER BY id`, irodsZone, tx.schema)
	return tx.tx.QueryContext(ctx, query)
}
