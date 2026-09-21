// Package store implements portable durable coordination and driver registration.
package store

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"slackhubspot/internal/domain"
	"slackhubspot/internal/store/backend"
	"slackhubspot/internal/store/postgres"
	"slackhubspot/internal/store/spanner"
	"slackhubspot/internal/store/sqlite"
)

type Config map[string]any
type Factory func(context.Context, Config) (domain.Store, error)

var registry = struct {
	sync.RWMutex
	factories map[string]Factory
}{factories: make(map[string]Factory)}

// Register allows drivers to own their configuration validation and schema.
func Register(name string, factory Factory) error {
	registry.Lock()
	defer registry.Unlock()
	if name == "" || factory == nil {
		return errors.New("driver name and factory are required")
	}
	if _, exists := registry.factories[name]; exists {
		return fmt.Errorf("storage driver %q already registered", name)
	}
	registry.factories[name] = factory
	return nil
}

func init() {
	registry.factories["postgres"] = func(ctx context.Context, cfg Config) (domain.Store, error) {
		db, err := postgres.Open(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return New(db), nil
	}
	registry.factories["spanner"] = func(ctx context.Context, cfg Config) (domain.Store, error) {
		db, err := spanner.Open(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return New(db), nil
	}
	registry.factories["sqlite"] = func(ctx context.Context, cfg Config) (domain.Store, error) {
		db, err := sqlite.Open(ctx, cfg)
		if err != nil {
			return nil, err
		}
		return New(db), nil
	}
}

// Open validates only the chosen driver's configuration. Schema initialization
// is deliberately separate; the migration binary must explicitly call Migrate.
func Open(ctx context.Context, cfg Config) (domain.Store, error) {
	name, _ := cfg["driver"].(string)
	registry.RLock()
	factory, exists := registry.factories[name]
	registry.RUnlock()
	if !exists {
		return nil, fmt.Errorf("unknown storage driver %q", name)
	}
	selected := Config{}
	if raw, ok := cfg[name]; ok {
		switch value := raw.(type) {
		case map[string]any:
			selected = Config(value)
		case Config:
			selected = value
		default:
			return nil, fmt.Errorf("storage.%s must be a mapping", name)
		}
	}
	return factory(ctx, selected)
}

type Store struct{ db backend.Database }

// New applies the common behavioral contract to a serializable backend.
func New(db backend.Database) *Store               { return &Store{db: db} }
func (s *Store) Check(ctx context.Context) error   { return s.db.Check(ctx) }
func (s *Store) Close() error                      { return s.db.Close() }
func (s *Store) Migrate(ctx context.Context) error { return s.db.Migrate(ctx) }

var _ domain.Store = (*Store)(nil)
