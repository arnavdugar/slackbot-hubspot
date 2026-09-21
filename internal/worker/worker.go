// Package worker reconciles authoritative Slack threads into integration-owned
// HubSpot notes. It depends only on domain operations, not storage drivers.
package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"mime"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"slackhubspot/internal/domain"
	"slackhubspot/internal/render"
	"slackhubspot/internal/telemetry"
)

type Options struct {
	AccountID          string
	AllowedMIMETypes   []string
	Attachments        bool
	BackoffBase        time.Duration
	BackoffMax         time.Duration
	BotUserID          string
	CoalesceDelay      time.Duration
	Concurrency        int
	FailureReaction    string
	HubSpot            domain.HubSpot
	LeaseDuration      time.Duration
	Logger             *slog.Logger
	MaxAttachmentBytes int64
	MaxAttempts        int
	Metrics            *telemetry.Metrics
	PollInterval       time.Duration
	Slack              domain.Slack
	Store              domain.Store
	SuccessReaction    string
	TempDir            string
	WorkerID           string
}

type Worker struct{ options Options }

func New(options Options) *Worker {
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	return &Worker{options: options}
}

// Run stops claims as soon as stop closes, allowing current remote calls to
// finish until ctx is cancelled by the shutdown grace deadline.
func (w *Worker) Run(ctx context.Context, stop <-chan struct{}) {
	var workers sync.WaitGroup
	for i := 0; i < w.options.Concurrency; i++ {
		workers.Add(1)
		go func(index int) {
			defer workers.Done()
			workerID := fmt.Sprintf("%s/%d", w.options.WorkerID, index)
			timer := time.NewTimer(0)
			defer timer.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-stop:
					return
				case <-timer.C:
				}
				// Recheck the shutdown signal before starting a new claim.
				select {
				case <-stop:
					return
				default:
				}
				if err := w.deliver(ctx, workerID); err != nil && !errors.Is(err, domain.ErrNoWork) && !errors.Is(err, context.Canceled) {
					w.options.Logger.Error("notification delivery failed", "error", safeError(err), "worker_id", workerID)
				}
				select {
				case <-ctx.Done():
					return
				case <-stop:
					return
				default:
				}
				lease, err := w.options.Store.Claim(ctx, workerID, time.Now(), w.options.LeaseDuration)
				if err == nil {
					if lease.Recovered {
						w.options.Metrics.Inc("lease_recovery")
					}
					w.process(ctx, workerID, lease)
				} else if !errors.Is(err, domain.ErrNoWork) && !errors.Is(err, context.Canceled) {
					w.options.Logger.Error("work claim failed", "error", "storage unavailable", "worker_id", workerID)
				}
				timer.Reset(w.options.PollInterval)
			}
		}(i)
	}
	workers.Wait()
}

func (w *Worker) process(ctx context.Context, workerID string, lease domain.Lease) {
	started := time.Now()
	defer func() { w.options.Metrics.ObserveSync(time.Since(started)) }()
	logger := w.options.Logger.With("channel_id", lease.Thread.Key.ChannelID, "event_id", lease.Thread.LastEventID, "lease_id", lease.Token, "message_ts", lease.Thread.LastMessageTS, "thread_ts", lease.Thread.Key.ThreadTS, "ticket_id", lease.Thread.TicketID, "worker_id", workerID, "workspace_id", lease.Thread.Key.WorkspaceID)
	workCtx, cancel := context.WithCancel(ctx)
	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		ticker := time.NewTicker(w.options.LeaseDuration / 3)
		defer ticker.Stop()
		for {
			select {
			case <-workCtx.Done():
				return
			case <-ticker.C:
				if err := w.options.Store.Renew(workCtx, lease, time.Now(), w.options.LeaseDuration); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	completion, err := w.reconcile(workCtx, ctx, lease)
	wasCancelled := workCtx.Err() != nil
	cancel()
	<-heartbeatDone
	// Use a bounded cleanup context so SIGTERM cancellation releases work rather
	// than waiting for expiry, while revision/token checks still fence old work.
	cleanup, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cleanupCancel()
	if err == nil {
		if err = w.options.Store.Complete(cleanup, lease, completion, time.Now()); err == nil {
			logger.Info("thread synchronized", "generation", lease.Generation, "partial_attachments", completion.PartialError != "")
			return
		}
	}
	if errors.Is(err, domain.ErrLeaseLost) {
		logger.Debug("work invalidated")
		return
	}
	var failure domain.Failure
	if wasCancelled || ctx.Err() != nil || errors.Is(err, context.Canceled) {
		failure = domain.Failure{Error: "operation cancelled", NextAttempt: time.Now()}
	} else {
		failure = w.failure(err, lease.Thread.Attempts)
	}
	if failErr := w.options.Store.Fail(cleanup, lease, failure, time.Now()); failErr != nil {
		if !errors.Is(failErr, domain.ErrLeaseLost) {
			logger.Error("could not persist sync failure", "error", "storage unavailable")
		}
		return
	}
	logger.Warn("thread synchronization failed", "attempt", lease.Thread.Attempts, "error", failure.Error, "permanent", failure.Permanent)
	if w.options.FailureReaction != "" && !errors.Is(err, context.Canceled) {
		_ = w.options.Slack.React(cleanup, lease.Thread.Key, lease.Thread.LastMessageTS, w.options.FailureReaction)
	}
}

func (w *Worker) reconcile(ctx, serviceCtx context.Context, lease domain.Lease) (domain.Completion, error) {
	completion := domain.Completion{}
	if w.options.CoalesceDelay > 0 {
		timer := time.NewTimer(w.options.CoalesceDelay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return completion, ctx.Err()
		case <-timer.C:
		}
	}
	if err := w.options.Store.Validate(ctx, lease, time.Now()); err != nil {
		return completion, err
	}
	messages, err := w.options.Slack.Thread(ctx, lease.Thread.Key)
	if err != nil {
		return completion, err
	}
	messages = slices.DeleteFunc(messages, func(message domain.Message) bool {
		return !render.Eligible(message, w.options.BotUserID, w.options.AccountID)
	})
	slices.SortFunc(messages, func(a, b domain.Message) int { return strings.Compare(a.Timestamp, b.Timestamp) })
	attachments := make(map[string]string)
	fileIDs := make([]string, 0)
	partial := 0
	for _, message := range messages {
		for _, file := range message.Files {
			if _, seen := attachments[file.ID]; seen {
				continue
			}
			if !w.options.Attachments {
				attachments[file.ID] = "copying disabled"
				w.options.Metrics.Inc("attachments_skipped")
				continue
			}
			id, fileErr := w.attachment(ctx, serviceCtx, lease, file)
			if fileErr == nil {
				attachments[file.ID] = "synced (HubSpot file " + id + ")"
				fileIDs = append(fileIDs, id)
				continue
			}
			if errors.Is(fileErr, domain.ErrLeaseLost) || errors.Is(fileErr, context.Canceled) {
				return completion, fileErr
			}
			partial++
			attachments[file.ID] = "not synchronized: " + safeError(fileErr)
			// Retry partial attachment failures without withholding message text.
			if lease.Thread.Attempts < w.options.MaxAttempts && retryable(fileErr) {
				retryAt := w.retryAt(fileErr, lease.Thread.Attempts)
				if completion.RetryAt.IsZero() || retryAt.After(completion.RetryAt) {
					completion.RetryAt = retryAt
				}
			}
		}
	}
	if partial != 0 {
		completion.PartialError = fmt.Sprintf("%d attachment(s) not synchronized", partial)
	}
	if !completion.RetryAt.IsZero() {
		w.options.Metrics.Inc("retries")
	}
	slices.Sort(fileIDs)
	fileIDs = slices.Compact(fileIDs)
	body := render.Transcript(lease.Thread.Key, messages, attachments)
	noteID := lease.Thread.NoteID
	if noteID == "" {
		// Recover a create whose remote response or mapping commit was lost.
		noteID, err = w.options.HubSpot.FindNote(ctx, lease.Thread.TicketID, lease.Thread.Key.Marker())
		if err != nil {
			return completion, err
		}
		if err = w.options.Store.Validate(ctx, lease, time.Now()); err != nil {
			return completion, err
		}
		if noteID == "" {
			noteID, err = w.options.HubSpot.CreateNote(ctx, lease.Thread.TicketID, lease.Thread.Key.Marker(), body, fileIDs)
			if err != nil {
				return completion, err
			}
			w.options.Metrics.Inc("transcript_creates")
		}
		if err = w.options.Store.SaveNote(ctx, lease, noteID, time.Now()); err != nil {
			return completion, err
		}
	}
	if err = w.options.Store.Validate(ctx, lease, time.Now()); err != nil {
		return completion, err
	}
	if err = w.options.HubSpot.UpdateNote(ctx, noteID, body, fileIDs); err != nil {
		return completion, err
	}
	w.options.Metrics.Inc("transcript_updates")
	for _, message := range messages {
		if w.options.SuccessReaction == "" {
			break
		}
		if slices.Contains(message.BotReactions, w.options.SuccessReaction) {
			continue
		}
		if err = w.options.Store.Validate(ctx, lease, time.Now()); err != nil {
			return completion, err
		}
		if err = w.options.Slack.React(ctx, lease.Thread.Key, message.Timestamp, w.options.SuccessReaction); err != nil {
			return completion, err
		}
	}
	return completion, nil
}

func (w *Worker) attachment(ctx, serviceCtx context.Context, lease domain.Lease, reference domain.File) (string, error) {
	upload, err := w.options.Store.ClaimUpload(ctx, lease, reference.ID, time.Now(), w.options.LeaseDuration)
	if err != nil {
		if errors.Is(err, domain.ErrNoWork) && upload.State == "failed" {
			return "", &domain.RemoteError{Code: "previous_upload_failed", Service: "attachment"}
		}
		if errors.Is(err, domain.ErrNoWork) && upload.NextAttempt.After(time.Now()) {
			return "", &domain.RemoteError{Code: "upload_retry_pending", RetryAfter: time.Until(upload.NextAttempt), Service: "attachment", Temporary: true}
		}
		return "", err
	}
	if upload.HubSpotID != "" {
		return upload.HubSpotID, nil
	}
	// Each file has its own lease; completing this shared mapping is valid even
	// if a single thread stops while its already-issued upload is in flight.
	uploadCtx, cancel := context.WithCancel(serviceCtx)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(w.options.LeaseDuration / 3)
		defer ticker.Stop()
		for {
			select {
			case <-uploadCtx.Done():
				return
			case <-ticker.C:
				if err := w.options.Store.RenewUpload(uploadCtx, upload, time.Now(), w.options.LeaseDuration); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	defer func() { cancel(); <-done }()
	id, uploadErr := w.copyFile(uploadCtx, ctx, lease, reference)
	cleanup, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cleanupCancel()
	if uploadErr != nil {
		w.options.Metrics.Inc("attachments_failed")
		var failure domain.Failure
		if ctx.Err() != nil || serviceCtx.Err() != nil || errors.Is(uploadErr, domain.ErrLeaseLost) || errors.Is(uploadErr, context.Canceled) {
			// This claim belongs to a global file, not to the stopped thread.
			// Release it for another thread even on the last configured attempt.
			failure = domain.Failure{Error: "upload interrupted", NextAttempt: time.Now()}
		} else {
			failure = w.failure(uploadErr, upload.Attempts)
		}
		if err := w.options.Store.FailUpload(cleanup, upload, failure, time.Now()); err != nil && !errors.Is(err, domain.ErrLeaseLost) {
			return "", err
		}
		return "", uploadErr
	}
	if err = w.options.Store.CompleteUpload(cleanup, upload, id, time.Now()); err != nil {
		return "", err
	}
	w.options.Metrics.Inc("attachments_uploaded")
	return id, nil
}

func (w *Worker) copyFile(uploadCtx, threadCtx context.Context, lease domain.Lease, reference domain.File) (string, error) {
	if err := w.options.Store.Validate(threadCtx, lease, time.Now()); err != nil {
		return "", err
	}
	marker := "slack-file-" + lease.Thread.Key.WorkspaceID + "-" + reference.ID
	id, err := w.options.HubSpot.FindFile(uploadCtx, marker)
	if err != nil || id != "" {
		return id, err
	}
	file, err := w.options.Slack.File(uploadCtx, reference.ID)
	if err != nil {
		return "", err
	}
	if file.Size < 0 || file.Size > w.options.MaxAttachmentBytes {
		return "", &domain.RemoteError{Code: "attachment_size_limit", Service: "policy"}
	}
	mediaType, _, err := mime.ParseMediaType(file.MIME)
	if err != nil || !slices.Contains(w.options.AllowedMIMETypes, mediaType) {
		return "", &domain.RemoteError{Code: "attachment_mime_not_allowed", Service: "policy"}
	}
	if err = w.options.Store.Validate(threadCtx, lease, time.Now()); err != nil {
		return "", err
	}
	reader, err := w.options.Slack.Download(uploadCtx, file)
	if err != nil {
		return "", err
	}
	defer reader.Close()
	// A private temporary file bounds downloads before HubSpot receives bytes.
	// It is never stored in the database, and removal runs on every exit path.
	temp, err := os.CreateTemp(w.options.TempDir, "slackhubspot-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(temp.Name())
	defer temp.Close()
	bytes, err := io.Copy(temp, io.LimitReader(reader, w.options.MaxAttachmentBytes+1))
	if err != nil {
		return "", err
	}
	if bytes > w.options.MaxAttachmentBytes {
		return "", &domain.RemoteError{Code: "attachment_size_limit", Service: "policy"}
	}
	if file.Size > 0 && bytes != file.Size {
		return "", &domain.RemoteError{Code: "incomplete_download", Service: "slack", Temporary: true}
	}
	if _, err = temp.Seek(0, io.SeekStart); err != nil {
		return "", err
	}
	if err = w.options.Store.Validate(threadCtx, lease, time.Now()); err != nil {
		return "", err
	}
	return w.options.HubSpot.Upload(uploadCtx, marker, file, temp)
}

func (w *Worker) deliver(ctx context.Context, workerID string) error {
	notification, err := w.options.Store.ClaimNotification(ctx, workerID, time.Now(), w.options.LeaseDuration)
	if err != nil {
		return err
	}
	deliveryCtx, cancel := context.WithTimeout(ctx, w.options.LeaseDuration/2)
	err = w.options.Slack.Post(deliveryCtx, notification.Key, notification.Text, notification.ID)
	cancel()
	cleanup, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cleanupCancel()
	if err == nil {
		return w.options.Store.CompleteNotification(cleanup, notification, time.Now())
	}
	var failure domain.Failure
	if ctx.Err() != nil || errors.Is(err, context.Canceled) {
		failure = domain.Failure{Error: "delivery interrupted", NextAttempt: time.Now()}
	} else {
		failure = w.failure(err, notification.Attempts)
	}
	if failErr := w.options.Store.FailNotification(cleanup, notification, failure, time.Now()); failErr != nil {
		return failErr
	}
	return err
}

func (w *Worker) failure(err error, attempts int) domain.Failure {
	failure := domain.Failure{Error: safeError(err), Permanent: !retryable(err) || attempts >= w.options.MaxAttempts}
	if failure.Permanent {
		w.options.Metrics.Inc("permanent_failures")
	} else {
		failure.NextAttempt = w.retryAt(err, attempts)
		w.options.Metrics.Inc("retries")
	}
	return failure
}

func (w *Worker) retryAt(err error, attempts int) time.Time {
	backoff := w.options.BackoffBase
	for i := 1; i < attempts && backoff < w.options.BackoffMax; i++ {
		if backoff > w.options.BackoffMax/2 {
			backoff = w.options.BackoffMax
			break
		}
		backoff *= 2
	}
	backoff = min(backoff, w.options.BackoffMax)
	// Equal jitter avoids synchronized retries and never schedules immediately.
	backoff = backoff/2 + time.Duration(rand.Int64N(max(int64(backoff/2), 1)))
	var remote *domain.RemoteError
	if errors.As(err, &remote) {
		backoff = max(backoff, remote.RetryAfter)
	}
	return time.Now().Add(backoff)
}

func retryable(err error) bool {
	var remote *domain.RemoteError
	if errors.As(err, &remote) {
		return remote.Temporary
	}
	return !errors.Is(err, domain.ErrLeaseLost)
}

func safeError(err error) string {
	var remote *domain.RemoteError
	if errors.As(err, &remote) {
		return remote.Error()
	}
	if errors.Is(err, domain.ErrNoWork) {
		return "attachment pending or unavailable"
	}
	if errors.Is(err, domain.ErrLeaseLost) {
		return "work invalidated"
	}
	if errors.Is(err, context.Canceled) {
		return "operation cancelled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "operation timed out"
	}
	return "integration operation failed"
}
