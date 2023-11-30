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
	query := fmt.Sprintf(`WITH attached (tag_id, targets) AS (
SELECT tag_id, 
       json_agg(format('{"id": %%s, "type": %%s}',
           coalesce(to_json(a_t.target_id::text), 'null'::json),
	   coalesce(to_json(a_t.target_type::text), 'null'::json))::json) "targets"
  FROM attached_tags a_t WHERE a_t.target_type IN ('file', 'folder') GROUP BY a_t.tag_id
)
SELECT id, to_json(q.*) FROM (
SELECT t.id::text,
       'tag' "doc_type",
       t.value,
       t.description,
       t.owner_id || '#%[1]s' "creator",
       t.created_on "dateCreated",
       t.modified_on "dateModified",
       coalesce(attached.targets, json_build_array()) "targets"
  FROM %[2]s.tags t
  LEFT JOIN attached ON (t.id = attached.tag_id)) q ORDER BY id`, irodsZone, tx.schema)
	log.Debugf("Tags query: %s", query)
	return tx.tx.QueryContext(ctx, query)
}

// GetAVUs returns a sql.Rows for CyVerse metadata AVUs with an optional UUID prefix for the ultimate target ID (but still including nested AVUs)
func (tx *DEDBTx) GetAVUs(ctx context.Context, rootTargetIdPrefix string) (*sql.Rows, error) {
	var where string
	if rootTargetIdPrefix != "" {
		where = fmt.Sprintf("WHERE target_id::text LIKE '%s%%'", rootTargetIdPrefix)
	}
	query := fmt.Sprintf(`WITH RECURSIVE all_avus AS (
SELECT cast(id as varchar),
       attribute,
       value,
       unit,
       cast(target_id as varchar),
       cast(target_type as varchar),
       created_by,
       modified_by,
       created_on,
       modified_on
  FROM %s.avus
  %s
UNION ALL
SELECT cast(avus.id as varchar),
       avus.attribute,
       avus.value,
       avus.unit,
       cast(aa.target_id as varchar),
       cast(aa.target_type as varchar),
       avus.created_by,
       avus.modified_by,
       avus.created_on,
       avus.modified_on
  FROM %s.avus
  JOIN all_avus aa ON (avus.target_id = cast(aa.id as uuid) AND avus.target_type = 'avu')
)
SELECT target_id, json_build_object('cyverse', json_agg(format('{"attribute": %%s, "value": %%s, "unit": %%s}',
        coalesce(to_json(attribute), 'null'::json),
        coalesce(to_json(value), 'null'::json),
        coalesce(to_json(unit), 'null'::json))::json ORDER BY attribute, value, unit))
  AS "metadata"
  FROM all_avus
  GROUP BY target_id
  ORDER BY target_id
`, tx.schema, where, tx.schema)
	log.Debugf("AVUs query: %s", query)
	return tx.tx.QueryContext(ctx, query)
}
