// Package backend defines the transactional primitives used by every store driver.
package backend

import (
	"context"
	"errors"
	"fmt"
)

var ErrNotFound = errors.New("record not found")

const SchemaKey = "schema/version"
const SchemaVersion = "1"

func ValidateSchema(value []byte) error {
	if string(value) != SchemaVersion {
		return fmt.Errorf("incompatible storage schema: expected version %s", SchemaVersion)
	}
	return nil
}

func ValidateConfig(config map[string]any, fields ...string) error {
	allowed := make(map[string]bool, len(fields))
	for _, field := range fields {
		allowed[field] = true
	}
	for field := range config {
		if !allowed[field] {
			return fmt.Errorf("unknown storage setting %q", field)
		}
	}
	return nil
}

type Entry struct {
	Key   string
	Value []byte
}

// Transaction provides serializable reads and writes. Callback functions may be
// retried and must not perform external side effects.
type Transaction interface {
	Get(context.Context, string) ([]byte, error)
	Put(context.Context, string, []byte) error
	Scan(context.Context, string) ([]Entry, error)
}

type Database interface {
	Check(context.Context) error
	Close() error
	Migrate(context.Context) error
	Update(context.Context, func(Transaction) error) error
	View(context.Context, func(Transaction) error) error
}
