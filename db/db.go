package db

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

// TODO: merge tool from sqlite to yugabyte
func OpenDB(dbPath string) (*sql.DB, error) {
	var db *sql.DB
	var err error
	var createDBSQL string

	if strings.HasPrefix(dbPath, "postgres://") {
		db, err = sql.Open("postgres", dbPath)
		if err != nil {
			return nil, err
		}

		createDBSQL = `
		CREATE TABLE IF NOT EXISTS RootBlocks (
			root TEXT NOT NULL PRIMARY KEY,
			size INT NOT NULL,
			block BLOB NOT NULL
		);`
	} else {
		db, err = sql.Open("sqlite3", "file:"+dbPath)
		if err != nil {
			return nil, err
		}
		db.SetMaxOpenConns(1)

		createDBSQL = `
        CREATE TABLE IF NOT EXISTS RootBlocks (
            root TEXT NOT NULL,
            size INTEGER NOT NULL,
            block BYTEA NOT NULL,
            PRIMARY KEY (root)
        );`
	}

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("db ping: %w", err)
	}

	if _, err := db.Exec(createDBSQL); err != nil {
		return nil, fmt.Errorf("failed to create tables in DB: %w", err)
	}

	return db, err
}
