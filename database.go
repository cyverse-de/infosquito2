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

func (tx *ICATTx) Count(table string) (int64, error) {
	row := tx.tx.QueryRow(fmt.Sprintf("SELECT count(*) FROM %s", table))
	var count int64
	err := row.Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

//func (tx *ICATTx) GetDataObjects(uuidTable string, permsTable string, metaTable string) (*sql.Rows, error) {
//}
//
//func (tx *ICATTx) GetCollections(uuidTable string, permsTable string, metaTable string) (*sql.Rows, error) {
//}
