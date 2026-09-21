package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"slackhubspot/internal/config"
	"slackhubspot/internal/httpapi"
	"slackhubspot/internal/hubspot"
	"slackhubspot/internal/render"
	"slackhubspot/internal/slack"
	"slackhubspot/internal/store"
	"slackhubspot/internal/telemetry"
	"slackhubspot/internal/worker"
)

var version = "development"

func main() {
	path := flag.String("config", "/config/config.yaml", "mounted YAML configuration path")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	if err := run(*path, logger); err != nil {
		logger.Error("service stopped", "error", err)
		os.Exit(1)
	}
}

func run(path string, logger *slog.Logger) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	startup, startupCancel := context.WithTimeout(context.Background(), time.Minute)
	defer startupCancel()
	db, err := store.Open(startup, cfg.Storage)
	if err != nil {
		return errors.New("cannot open selected storage driver; verify driver configuration and connectivity")
	}
	defer db.Close()
	if err = db.Check(startup); err != nil {
		return errors.New("storage is unavailable or schema incompatible; run the migration binary before starting")
	}
	summary, err := render.CompileSummary(cfg.SummaryTemplate)
	if err != nil {
		return err
	}
	metrics := telemetry.New(db)
	slackClient, err := slack.New(slack.Config{BotToken: cfg.Slack.BotToken, HistoryToken: cfg.Slack.HistoryToken, RateLimited: func() { metrics.Inc("slack_rate_limits") }})
	if err != nil {
		return err
	}
	if err = slackClient.VerifyIdentity(startup, cfg.Slack.WorkspaceID, cfg.Slack.BotUserID); err != nil {
		return errors.New("Slack credentials do not match configured workspace/bot or Slack is unavailable")
	}
	hubspotClient, err := hubspot.New(hubspot.Config{AccountID: cfg.HubSpot.AccountID, RateLimited: func() { metrics.Inc("hubspot_rate_limits") }, SummaryFields: summary.Fields(), Token: cfg.HubSpot.AccessToken})
	if err != nil {
		return err
	}
	if err = hubspotClient.VerifyAccount(startup); err != nil {
		return errors.New("HubSpot credentials do not match configured account or HubSpot is unavailable")
	}
	var ready atomic.Bool
	ready.Store(true)
	handler := httpapi.New(httpapi.Options{
		BotID:            slackClient.BotID(),
		BotUserID:        cfg.Slack.BotUserID,
		HubSpot:          hubspotClient,
		HubSpotAccountID: cfg.HubSpot.AccountID,
		Logger:           logger,
		MaxBodyBytes:     cfg.Server.MaxBodyBytes,
		Metrics:          metrics,
		Observe: func(outcome string) {
			switch outcome {
			case "duplicate":
				metrics.Inc("duplicate_events")
			case "received":
				metrics.Inc("slack_events_received")
			case "rejected":
				metrics.Inc("slack_events_rejected")
			}
		},
		Ready:          ready.Load,
		RequestTimeout: cfg.Server.RequestTimeout,
		SigningSecret:  cfg.Slack.SigningSecret,
		Slack:          slackClient,
		Store:          db,
		Summary:        summary,
		WorkspaceID:    cfg.Slack.WorkspaceID,
	})
	server := &http.Server{Addr: fmt.Sprintf(":%d", cfg.Server.Port), Handler: handler, IdleTimeout: time.Minute, MaxHeaderBytes: 32 << 10, ReadHeaderTimeout: cfg.Server.ReadHeaderTimeout, ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second}
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		return errors.New("cannot listen on configured server port")
	}
	defer listener.Close()
	service := worker.New(worker.Options{
		AccountID:          cfg.HubSpot.AccountID,
		AllowedMIMETypes:   cfg.Sync.AllowedMIMETypes,
		Attachments:        cfg.Sync.Attachments,
		BackoffBase:        cfg.Worker.BackoffBase,
		BackoffMax:         cfg.Worker.BackoffMax,
		BotUserID:          cfg.Slack.BotUserID,
		CoalesceDelay:      cfg.Worker.CoalesceDelay,
		Concurrency:        cfg.Worker.Concurrency,
		FailureReaction:    cfg.Slack.FailureReaction,
		HubSpot:            hubspotClient,
		LeaseDuration:      cfg.Worker.LeaseDuration,
		Logger:             logger,
		MaxAttachmentBytes: cfg.Sync.MaxAttachmentBytes,
		MaxAttempts:        cfg.Worker.MaxAttempts,
		Metrics:            metrics,
		PollInterval:       cfg.Worker.PollInterval,
		Slack:              slackClient,
		Store:              db,
		SuccessReaction:    cfg.Slack.SuccessReaction,
		TempDir:            cfg.Sync.TempDir,
		WorkerID:           rand.Text(),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := make(chan struct{})
	workersDone := make(chan struct{})
	go func() { defer close(workersDone); service.Run(ctx, stop) }()
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Serve(listener) }()
	interrupt, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	logger.Info("service ready", "port", cfg.Server.Port, "storage_driver", cfg.Storage["driver"], "version", version)
	select {
	case <-interrupt.Done():
	case err = <-serverDone:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Error("HTTP server failed", "error", "listener unavailable")
		}
	}
	ready.Store(false)
	close(stop)
	grace, graceCancel := context.WithTimeout(context.Background(), cfg.Worker.GracePeriod)
	defer graceCancel()
	// Reserve the final part of the grace period for guarded lease release.
	cleanupTime := min(5*time.Second, cfg.Worker.GracePeriod/2)
	cancelTimer := time.AfterFunc(cfg.Worker.GracePeriod-cleanupTime, cancel)
	defer cancelTimer.Stop()
	httpStopped := make(chan struct{})
	go func() { defer close(httpStopped); _ = server.Shutdown(grace) }()
	for workersDone != nil || httpStopped != nil {
		select {
		case <-workersDone:
			workersDone = nil
		case <-httpStopped:
			httpStopped = nil
		case <-grace.Done():
			cancel()
			_ = server.Close()
			// Failed releases remain safe to reclaim after their persisted expiry.
			workersDone, httpStopped = nil, nil
		}
	}
	logger.Info("service shutdown complete")
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return errors.New("HTTP server failed")
	}
	return nil
}
