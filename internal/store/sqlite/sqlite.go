// Package sqlite implements the single-replica SQLite storage backend.
package sqlite

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"

	_ "modernc.org/sqlite"

	"slackhubspot/internal/store/backend"
	"slackhubspot/internal/store/sqlite/sqlc"
)

//go:embed schema.sql
var schema string

type Database struct{ db *sql.DB }
type transaction struct{ queries *sqlc.Queries }

func Open(ctx context.Context, config map[string]any) (*Database, error) {
	if err := backend.ValidateConfig(config, "path"); err != nil {
		return nil, err
	}
	path, _ := config["path"].(string)
	if path == "" {
		return nil, errors.New("storage.sqlite.path is required")
	}
	if path != ":memory:" {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, err
		}
		path = (&url.URL{Scheme: "file", Path: absolute}).String()
	}
	parameters := url.Values{"_pragma": {"busy_timeout(5000)", "foreign_keys(1)", "journal_mode(WAL)"}, "_txlock": {"immediate"}}
	db, err := sql.Open("sqlite", path+"?"+parameters.Encode())
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect sqlite: %w", err)
	}
	return &Database{db: db}, nil
}

func (d *Database) Check(ctx context.Context) error {
	if err := d.db.PingContext(ctx); err != nil {
		return err
	}
	value, err := sqlc.New(d.db).GetRecord(ctx, backend.SchemaKey)
	if err != nil {
		return fmt.Errorf("SQLite schema is unavailable; run migrations: %w", err)
	}
	return backend.ValidateSchema(value)
}

func (d *Database) Close() error { return d.db.Close() }

func (d *Database) Migrate(ctx context.Context) error {
	if _, err := d.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("initialize SQLite schema: %w", err)
	}
	return d.Update(ctx, func(tx backend.Transaction) error {
		value, err := tx.Get(ctx, backend.SchemaKey)
		if errors.Is(err, backend.ErrNotFound) {
			return tx.Put(ctx, backend.SchemaKey, []byte(backend.SchemaVersion))
		}
		if err != nil {
			return err
		}
		return backend.ValidateSchema(value)
	})
}

func (d *Database) Update(ctx context.Context, apply func(backend.Transaction) error) error {
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := apply(transaction{queries: sqlc.New(tx)}); err != nil {
		return err
	}
	return tx.Commit()
}

func (d *Database) View(ctx context.Context, apply func(backend.Transaction) error) error {
	return d.Update(ctx, apply)
}

func (t transaction) Get(ctx context.Context, key string) ([]byte, error) {
	value, err := t.queries.GetRecord(ctx, key)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, backend.ErrNotFound
	}
	return value, err
}

func (t transaction) Put(ctx context.Context, key string, value []byte) error {
	return t.queries.PutRecord(ctx, sqlc.PutRecordParams{Key: key, Value: value})
}

func (t transaction) Scan(ctx context.Context, prefix string) ([]backend.Entry, error) {
	rows, err := t.queries.ScanRecords(ctx, sqlc.ScanRecordsParams{Key: prefix, Key_2: prefix + "\uffff"})
	if err != nil {
		return nil, err
	}
	entries := make([]backend.Entry, 0, len(rows))
	for _, row := range rows {
		entries = append(entries, backend.Entry{Key: row.Key, Value: row.Value})
	}
	return entries, nil
}
