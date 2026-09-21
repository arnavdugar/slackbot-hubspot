// migrate initializes or advances the selected driver's schema explicitly.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"slackhubspot/internal/config"
	"slackhubspot/internal/store"
)

func main() {
	path := flag.String("config", "/config/config.yaml", "mounted YAML configuration path")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.LoadStorage(*path)
	if err != nil {
		logger.Error("configuration rejected", "error", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	db, err := store.Open(ctx, cfg)
	if err != nil {
		logger.Error("storage unavailable", "driver", cfg["driver"])
		os.Exit(1)
	}
	defer db.Close()
	if err = db.Migrate(ctx); err != nil {
		logger.Error("migration failed; inspect database health and schema version", "driver", cfg["driver"])
		os.Exit(1)
	}
	if err = db.Check(ctx); err != nil {
		logger.Error("schema verification failed", "driver", cfg["driver"])
		os.Exit(1)
	}
	logger.Info("schema ready", "driver", cfg["driver"])
}
