package store_test

import (
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"slackhubspot/internal/domain"
	"slackhubspot/internal/store"
	"slackhubspot/internal/store/backend"
	"slackhubspot/internal/store/conformance"
	"slackhubspot/internal/store/postgres"
	"slackhubspot/internal/store/spanner"
	"slackhubspot/internal/store/sqlite"
)

type namespacedDatabase struct {
	backend.Database
	prefix string
}

type namespacedTransaction struct {
	backend.Transaction
	prefix string
}

func (d namespacedDatabase) Update(ctx context.Context, apply func(backend.Transaction) error) error {
	return d.Database.Update(ctx, func(tx backend.Transaction) error {
		return apply(namespacedTransaction{Transaction: tx, prefix: d.prefix})
	})
}
func (d namespacedDatabase) View(ctx context.Context, apply func(backend.Transaction) error) error {
	return d.Database.View(ctx, func(tx backend.Transaction) error {
		return apply(namespacedTransaction{Transaction: tx, prefix: d.prefix})
	})
}
func (tx namespacedTransaction) Get(ctx context.Context, key string) ([]byte, error) {
	return tx.Transaction.Get(ctx, tx.prefix+key)
}
func (tx namespacedTransaction) Put(ctx context.Context, key string, value []byte) error {
	return tx.Transaction.Put(ctx, tx.prefix+key, value)
}
func (tx namespacedTransaction) Scan(ctx context.Context, prefix string) ([]backend.Entry, error) {
	entries, err := tx.Transaction.Scan(ctx, tx.prefix+prefix)
	for i := range entries {
		entries[i].Key = strings.TrimPrefix(entries[i].Key, tx.prefix)
	}
	return entries, err
}

func TestConformance(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "conformance.sqlite")
	factories := map[string]func() (backend.Database, error){
		"sqlite": func() (backend.Database, error) { return sqlite.Open(ctx, map[string]any{"path": path}) },
	}
	if dsn := os.Getenv("STORE_TEST_POSTGRES_DSN"); dsn != "" {
		factories["postgres"] = func() (backend.Database, error) { return postgres.Open(ctx, map[string]any{"dsn": dsn}) }
	}
	if database := os.Getenv("STORE_TEST_SPANNER_DATABASE"); database != "" {
		factories["spanner"] = func() (backend.Database, error) { return spanner.Open(ctx, map[string]any{"database": database}) }
	}
	for _, driver := range []string{"postgres", "spanner", "sqlite"} {
		open, exists := factories[driver]
		if !exists {
			continue
		}
		t.Run(driver, func(t *testing.T) {
			db, err := open()
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Migrate(ctx); err != nil {
				t.Fatal(err)
			}
			if err := db.Migrate(ctx); err != nil {
				t.Fatalf("migration not idempotent: %v", err)
			}
			if err := db.Check(ctx); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			run := rand.Text()
			conformance.Run(t, func(t *testing.T) domain.Store {
				db, err := open()
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				return store.New(namespacedDatabase{Database: db, prefix: "tests/" + run + "/" + t.Name() + "/"})
			})
		})
	}
}

func TestRegistryAndSchemaValidation(t *testing.T) {
	ctx := context.Background()
	if _, err := store.Open(ctx, store.Config{"driver": "missing"}); err == nil {
		t.Fatal("unknown driver accepted")
	}
	if _, err := store.Open(ctx, store.Config{"driver": "sqlite", "sqlite": map[string]any{"path": ""}}); err == nil {
		t.Fatal("missing path accepted")
	}
	db, err := store.Open(ctx, store.Config{"driver": "sqlite", "postgres": 123, "sqlite": map[string]any{"path": filepath.Join(t.TempDir(), "fresh.sqlite")}})
	if err != nil {
		t.Fatalf("unselected config was validated: %v", err)
	}
	defer db.Close()
	if err := db.Check(ctx); err == nil {
		t.Fatal("unmigrated schema accepted")
	}
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := db.Check(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestNamespaceIdentifierValidation(t *testing.T) {
	ctx := context.Background()
	for _, schema := range []any{"", "name.with.dot", "pg_catalog", "pg_temp", "information_schema", "safe,public", "unsafe;DROP SCHEMA public", "with\"quote", strings.Repeat("a", 64), 42} {
		if db, err := postgres.Open(ctx, map[string]any{"schema": schema}); err == nil {
			db.Close()
			t.Fatalf("invalid PostgreSQL namespace accepted: %v", schema)
		} else if !strings.Contains(err.Error(), "storage.postgres.schema") {
			t.Fatalf("namespace was not validated first: %v", err)
		}
	}
	for _, prefix := range []any{"9starts_with_digit", "schema.table", "unsafe`table", "unsafe;DROP TABLE Records", strings.Repeat("a", 65), 42} {
		if db, err := spanner.Open(ctx, map[string]any{"table_prefix": prefix}); err == nil {
			db.Close()
			t.Fatalf("invalid Spanner table prefix accepted: %v", prefix)
		} else if !strings.Contains(err.Error(), "storage.spanner.table_prefix") {
			t.Fatalf("table prefix was not validated first: %v", err)
		}
	}
	if _, err := spanner.Open(ctx, map[string]any{"table": "CustomRecords"}); err == nil || !strings.Contains(err.Error(), "unknown storage setting") {
		t.Fatalf("obsolete table setting did not fail explicitly: %v", err)
	}
}

func TestNamespaceIsolation(t *testing.T) {
	ctx := context.Background()
	factories := map[string]func(bool) (backend.Database, error){}
	if dsn := os.Getenv("STORE_TEST_POSTGRES_DSN"); dsn != "" {
		otherSchema := os.Getenv("STORE_TEST_POSTGRES_OTHER_SCHEMA")
		if otherSchema == "" {
			otherSchema = "public"
		}
		factories["postgres"] = func(other bool) (backend.Database, error) {
			config := map[string]any{"dsn": dsn}
			if other {
				config["schema"] = otherSchema
			}
			return postgres.Open(ctx, config)
		}
	}
	if database := os.Getenv("STORE_TEST_SPANNER_DATABASE"); database != "" {
		for name, prefix := range map[string]string{
			"spanner":                "SlackHubSpotIsolationTest",
			"spanner_empty_prefix":   "",
			"spanner_maximum_prefix": strings.Repeat("P", 64),
		} {
			factories[name] = func(other bool) (backend.Database, error) {
				config := map[string]any{"database": database}
				if other {
					config["table_prefix"] = prefix
				}
				return spanner.Open(ctx, config)
			}
		}
	}
	drivers := make([]string, 0, len(factories))
	for driver := range factories {
		drivers = append(drivers, driver)
	}
	slices.Sort(drivers)
	for _, driver := range drivers {
		open := factories[driver]
		t.Run(driver, func(t *testing.T) {
			prefix := "isolation/" + rand.Text() + "/"
			var stores []domain.Store
			for _, other := range []bool{false, true} {
				db, err := open(other)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = db.Close() })
				for range 2 {
					if err := db.Migrate(ctx); err != nil {
						t.Fatal(err)
					}
				}
				if err := db.Check(ctx); err != nil {
					t.Fatal(err)
				}
				stores = append(stores, store.New(namespacedDatabase{Database: db, prefix: prefix}))
			}
			conformance.RunIsolation(t, stores[0], stores[1])
		})
	}
}

func TestPostgresMissingNamespaceDoesNotFallBack(t *testing.T) {
	dsn := os.Getenv("STORE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("STORE_TEST_POSTGRES_DSN is not configured")
	}
	ctx := context.Background()
	missing := "missing_" + strings.ToLower(rand.Text())
	db, err := postgres.Open(ctx, map[string]any{"dsn": dsn, "schema": missing})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Migrate(ctx); err == nil || !strings.Contains(err.Error(), "database administrator") {
		t.Fatalf("custom schema was not required to be provisioned: %v", err)
	}
	if err := db.Check(ctx); err == nil {
		t.Fatal("missing namespace fell back to another schema")
	}
}
