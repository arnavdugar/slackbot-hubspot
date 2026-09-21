// Package postgres implements serializable PostgreSQL coordination using pgx.
package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"

	"slackhubspot/internal/store/backend"
	"slackhubspot/internal/store/postgres/sqlc"
)

const defaultSchema = "slack_hubspot"

var schemaNamePattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

type Database struct {
	db     *sql.DB
	schema string
}
type transaction struct{ queries *sqlc.Queries }

func Open(ctx context.Context, config map[string]any) (*Database, error) {
	if err := backend.ValidateConfig(config, "dsn", "max_idle_connections", "max_open_connections", "schema"); err != nil {
		return nil, err
	}
	namespace := defaultSchema
	if raw, exists := config["schema"]; exists {
		var ok bool
		namespace, ok = raw.(string)
		if !ok || !schemaNamePattern.MatchString(namespace) || strings.HasPrefix(namespace, "pg_") || namespace == "information_schema" {
			return nil, errors.New("storage.postgres.schema must be a non-system lowercase SQL identifier of at most 63 characters")
		}
	}
	dsn, _ := config["dsn"].(string)
	if dsn == "" {
		return nil, errors.New("storage.postgres.dsn is required")
	}
	pool := map[string]int{"max_idle_connections": 5, "max_open_connections": 20}
	for field := range pool {
		if raw, ok := config[field]; ok {
			value, ok := raw.(int)
			if text, isString := raw.(string); isString {
				var err error
				value, err = strconv.Atoi(text)
				ok = err == nil
			}
			if !ok || value < 1 {
				return nil, fmt.Errorf("storage.postgres.%s must be a positive integer", field)
			}
			pool[field] = value
		}
	}
	connection, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid PostgreSQL connection configuration")
	}
	// Set every pooled connection's namespace without interpolating identifiers
	// into queries. There is no fallback to a shared schema such as public.
	connection.RuntimeParams["search_path"] = pgx.Identifier{namespace}.Sanitize()
	db := stdlib.OpenDB(*connection)
	db.SetConnMaxLifetime(30 * time.Minute)
	db.SetMaxIdleConns(min(pool["max_idle_connections"], pool["max_open_connections"]))
	db.SetMaxOpenConns(pool["max_open_connections"])
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, errors.New("cannot connect to PostgreSQL")
	}
	return &Database{db: db, schema: namespace}, nil
}

func (d *Database) Check(ctx context.Context) error {
	if err := d.db.PingContext(ctx); err != nil {
		return err
	}
	value, err := sqlc.New(d.db).GetRecord(ctx, backend.SchemaKey)
	if err != nil {
		return fmt.Errorf("PostgreSQL schema is unavailable; run migrations: %w", err)
	}
	return backend.ValidateSchema(value)
}

func (d *Database) Close() error { return d.db.Close() }

func (d *Database) Migrate(ctx context.Context) error {
	queries := sqlc.New(d.db)
	exists, err := queries.SchemaExists(ctx, d.schema)
	if err != nil {
		return fmt.Errorf("inspect PostgreSQL namespace: %w", err)
	}
	if !exists {
		if d.schema != defaultSchema {
			return fmt.Errorf("PostgreSQL schema %q does not exist; have a database administrator provision it before running migrations", d.schema)
		}
		if err := queries.CreateDefaultSchema(ctx); err != nil {
			return fmt.Errorf("create PostgreSQL application schema: %w", err)
		}
	}
	if err := queries.CreateRecords(ctx); err != nil {
		return fmt.Errorf("initialize PostgreSQL schema: %w", err)
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
	for attempt := 0; ; attempt++ {
		tx, err := d.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}
		err = apply(transaction{queries: sqlc.New(tx)})
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
		var pgError *pgconn.PgError
		if attempt >= 15 || !errors.As(err, &pgError) || (pgError.Code != "40001" && pgError.Code != "40P01" && pgError.Code != "23505") {
			return err
		}
		delay := time.NewTimer(time.Duration(min(1<<min(attempt, 8), 200)) * time.Millisecond)
		select {
		case <-ctx.Done():
			delay.Stop()
			return ctx.Err()
		case <-delay.C:
		}
	}
}

func (d *Database) View(ctx context.Context, apply func(backend.Transaction) error) error {
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := apply(transaction{queries: sqlc.New(tx)}); err != nil {
		return err
	}
	return tx.Commit()
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
