package db

import (
	"database/sql"
	"fmt"
	"strings"

	logging "github.com/ipfs/go-log/v2"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

var log = logging.Logger("db")

type DB struct {
	DB     *sql.DB
	DBType string
}

func OpenDB(dbPath string) (*DB, error) {
	var db *sql.DB
	var err error
	var createDBSQL string
	var dbType string

	if strings.HasPrefix(dbPath, "postgres") || strings.HasPrefix(dbPath, "yugabyte") {
		log.Debugf("open postgres db: %s", dbPath)
		db, err = sql.Open("postgres", dbPath)
		if err != nil {
			return nil, err
		}
		//db.SetMaxOpenConns(10)

		createDBSQL = `
        CREATE TABLE IF NOT EXISTS RootBlocks (
            root TEXT NOT NULL,
            size INTEGER NOT NULL,
            block BYTEA NOT NULL,
            PRIMARY KEY (root)
        );`
		dbType = "postgres"
	} else {
		log.Debugf("open sqlite db: %s", dbPath)
		db, err = sql.Open("sqlite3", "file:"+dbPath)
		if err != nil {
			return nil, err
		}
		db.SetMaxOpenConns(1)

		createDBSQL = `
		CREATE TABLE IF NOT EXISTS RootBlocks (
			root TEXT NOT NULL PRIMARY KEY,
			size INT NOT NULL,
			block BLOB NOT NULL
		);`
		dbType = "sqlite"
	}

	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("db ping: %w", err)
	}

	if _, err := db.Exec(createDBSQL); err != nil {
		return nil, fmt.Errorf("failed to create tables in DB: %w", err)
	}

	return &DB{DB: db, DBType: dbType}, nil
}

// MergeSQLiteToYugabyte 从SQLite合并数据到YugabyteDB
func MergeSQLiteToYugabyte(sqlitePath, yugabyteDSN string) error {
	log.Infof("merge sqlite to yugabyte: %s, %s", sqlitePath, yugabyteDSN)

	// 打开SQLite数据库
	sqliteDB, err := sql.Open("sqlite3", sqlitePath)
	if err != nil {
		return fmt.Errorf("打开SQLite数据库失败: %w", err)
	}
	defer sqliteDB.Close()

	// 连接YugabyteDB
	yugabyteDB, err := sql.Open("postgres", yugabyteDSN)
	if err != nil {
		return fmt.Errorf("连接YugabyteDB失败: %w", err)
	}
	defer yugabyteDB.Close()

	// 从SQLite读取数据
	rows, err := sqliteDB.Query("SELECT root, size, block FROM RootBlocks")
	if err != nil {
		return fmt.Errorf("查询SQLite数据失败: %w", err)
	}
	defer rows.Close()

	// 准备YugabyteDB插入语句
	stmt, err := yugabyteDB.Prepare("INSERT INTO RootBlocks(root, size, block) VALUES($1, $2, $3) ON CONFLICT (root) DO UPDATE SET size = $2, block = $3")
	if err != nil {
		return fmt.Errorf("准备YugabyteDB插入语句失败: %w", err)
	}
	defer stmt.Close()

	// 遍历SQLite数据并插入到YugabyteDB
	for rows.Next() {
		var root string
		var size int
		var block []byte
		if err := rows.Scan(&root, &size, &block); err != nil {
			return fmt.Errorf("扫描SQLite行失败: %w", err)
		}

		_, err = stmt.Exec(root, size, block)
		if err != nil {
			return fmt.Errorf("插入数据到YugabyteDB失败: %w", err)
		}

		log.Debugf("insert data to yugabyte: %s, %d", root, size)
	}

	log.Info("merge sqlite to yugabyte success")

	return nil
}
