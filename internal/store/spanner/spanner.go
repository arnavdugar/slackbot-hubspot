// Package spanner implements coordination using native Spanner reads and mutations.
package spanner

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"

	"slackhubspot/internal/store/backend"
)

//go:embed schema.sql
var schema string

// Reserve half of Spanner's 128-character identifier limit for future table
// and index suffixes. An explicitly empty prefix preserves legacy names.
var tablePrefixPattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,63}$`)

type Database struct {
	client      *spanner.Client
	database    string
	options     []option.ClientOption
	tablePrefix string
}

type reader interface {
	Read(context.Context, string, spanner.KeySet, []string) *spanner.RowIterator
	ReadRow(context.Context, string, spanner.Key, []string) (*spanner.Row, error)
}

type transaction struct {
	reader reader
	table  string
	writes *spanner.ReadWriteTransaction
}

func Open(ctx context.Context, config map[string]any) (*Database, error) {
	if err := backend.ValidateConfig(config, "database", "database_id", "emulator_host", "instance_id", "project_id", "table_prefix"); err != nil {
		return nil, err
	}
	prefix := "SlackHubSpot"
	if raw, exists := config["table_prefix"]; exists {
		var ok bool
		prefix, ok = raw.(string)
		if !ok || (prefix != "" && !tablePrefixPattern.MatchString(prefix)) {
			return nil, errors.New("storage.spanner.table_prefix must be empty or begin with a letter and contain at most 64 ASCII letters, digits, or underscores")
		}
	}
	name, _ := config["database"].(string)
	if name == "" {
		databaseID, _ := config["database_id"].(string)
		instanceID, _ := config["instance_id"].(string)
		projectID, _ := config["project_id"].(string)
		if databaseID == "" || instanceID == "" || projectID == "" {
			return nil, errors.New("Spanner project_id, instance_id, and database_id are required")
		}
		name = fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, databaseID)
	}
	parts := strings.Split(name, "/")
	if len(parts) != 6 || parts[0] != "projects" || parts[1] == "" || parts[2] != "instances" || parts[3] == "" || parts[4] != "databases" || parts[5] == "" {
		return nil, errors.New("invalid Spanner database resource name")
	}
	emulator, _ := config["emulator_host"].(string)
	if emulator == "" {
		emulator = os.Getenv("SPANNER_EMULATOR_HOST")
	}
	var options []option.ClientOption
	if emulator != "" {
		options = append(options, option.WithEndpoint(emulator), option.WithoutAuthentication(), option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
	}
	client, err := spanner.NewClient(ctx, name, options...)
	if err != nil {
		return nil, fmt.Errorf("open Spanner: %w", err)
	}
	return &Database{client: client, database: name, options: options, tablePrefix: prefix}, nil
}

// objectName applies the deployment namespace consistently to current and
// future schema objects. Suffixes are driver-owned identifiers of at most 64
// characters, never caller-supplied SQL.
func (d *Database) objectName(suffix string) string { return d.tablePrefix + suffix }

func (d *Database) Check(ctx context.Context) error {
	return d.View(ctx, func(tx backend.Transaction) error {
		value, err := tx.Get(ctx, backend.SchemaKey)
		if err != nil {
			return fmt.Errorf("Spanner schema is unavailable; run migrations: %w", err)
		}
		return backend.ValidateSchema(value)
	})
}

func (d *Database) Close() error { d.client.Close(); return nil }

func (d *Database) Migrate(ctx context.Context) error {
	admin, err := database.NewDatabaseAdminClient(ctx, d.options...)
	if err != nil {
		return err
	}
	defer admin.Close()
	ddl, err := admin.GetDatabaseDdl(ctx, &databasepb.GetDatabaseDdlRequest{Database: d.database})
	if err != nil {
		return err
	}
	exists := false
	table := d.objectName("Records")
	for _, statement := range ddl.Statements {
		// Spanner identifiers are case insensitive, while the DDL response keeps
		// the spelling used when a table was created.
		definition, name := strings.ToLower(statement), strings.ToLower(table)
		if strings.HasPrefix(definition, "create table "+name+" (") || strings.HasPrefix(definition, "create table `"+name+"` (") {
			exists = true
			break
		}
	}
	if !exists {
		// The only substituted DDL token is a validated bare identifier, enclosed
		// in backticks by the embedded template. Data still uses native mutations.
		statement := strings.ReplaceAll(schema, "{{TABLE}}", table)
		operation, err := admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{Database: d.database, Statements: []string{statement}})
		if err != nil {
			return err
		}
		if err := operation.Wait(ctx); err != nil && spanner.ErrCode(err) != codes.AlreadyExists {
			return err
		}
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
	_, err := d.client.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		return apply(transaction{reader: tx, table: d.objectName("Records"), writes: tx})
	})
	return err
}

func (d *Database) View(ctx context.Context, apply func(backend.Transaction) error) error {
	tx := d.client.ReadOnlyTransaction()
	defer tx.Close()
	return apply(transaction{reader: tx, table: d.objectName("Records")})
}

func (t transaction) Get(ctx context.Context, key string) ([]byte, error) {
	row, err := t.reader.ReadRow(ctx, t.table, spanner.Key{key}, []string{"RecordValue"})
	if spanner.ErrCode(err) == codes.NotFound {
		return nil, backend.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var value []byte
	if err := row.Columns(&value); err != nil {
		return nil, err
	}
	return value, nil
}

func (t transaction) Put(ctx context.Context, key string, value []byte) error {
	if t.writes == nil {
		return errors.New("cannot write in a read-only transaction")
	}
	return t.writes.BufferWrite([]*spanner.Mutation{spanner.InsertOrUpdate(t.table, []string{"RecordKey", "RecordValue"}, []any{key, value})})
}

func (t transaction) Scan(ctx context.Context, prefix string) ([]backend.Entry, error) {
	rows := t.reader.Read(ctx, t.table, spanner.KeyRange{End: spanner.Key{prefix + "\uffff"}, Kind: spanner.ClosedOpen, Start: spanner.Key{prefix}}, []string{"RecordKey", "RecordValue"})
	defer rows.Stop()
	var entries []backend.Entry
	for {
		row, err := rows.Next()
		if errors.Is(err, iterator.Done) {
			return entries, nil
		}
		if err != nil {
			return nil, err
		}
		var entry backend.Entry
		if err := row.Columns(&entry.Key, &entry.Value); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
}
