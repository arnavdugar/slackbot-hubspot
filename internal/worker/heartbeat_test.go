package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	"slackhubspot/internal/domain"
)

// Heartbeat tests use virtual time and in-memory doubles: waiting for seven
// seconds exercises two three-second ticks without scheduler-dependent sleeps.
type heartbeatStore struct {
	domain.Store
	renewError                       error
	threadRenewals, uploadRenewals   []time.Time
	threadDurations, uploadDurations []time.Duration
	threadCompleted, uploadCompleted bool
	failure                          *domain.Failure
	cleanupError                     error
}

func (s *heartbeatStore) Validate(context.Context, domain.Lease, time.Time) error { return nil }

func (s *heartbeatStore) Renew(_ context.Context, _ domain.Lease, now time.Time, duration time.Duration) error {
	s.threadRenewals = append(s.threadRenewals, now)
	s.threadDurations = append(s.threadDurations, duration)
	return s.renewError
}

func (s *heartbeatStore) Complete(ctx context.Context, _ domain.Lease, _ domain.Completion, _ time.Time) error {
	s.threadCompleted = true
	s.cleanupError = ctx.Err()
	return nil
}

func (s *heartbeatStore) Fail(ctx context.Context, _ domain.Lease, failure domain.Failure, _ time.Time) error {
	s.failure = &failure
	s.cleanupError = ctx.Err()
	return nil
}

func (s *heartbeatStore) ClaimUpload(_ context.Context, lease domain.Lease, id string, now time.Time, duration time.Duration) (domain.Upload, error) {
	return domain.Upload{WorkspaceID: lease.Thread.Key.WorkspaceID, FileID: id, Attempts: 1, LeaseToken: "upload-token", LeaseExpires: now.Add(duration), State: "running"}, nil
}

func (s *heartbeatStore) RenewUpload(_ context.Context, _ domain.Upload, now time.Time, duration time.Duration) error {
	s.uploadRenewals = append(s.uploadRenewals, now)
	s.uploadDurations = append(s.uploadDurations, duration)
	return s.renewError
}

func (s *heartbeatStore) CompleteUpload(ctx context.Context, _ domain.Upload, id string, _ time.Time) error {
	s.uploadCompleted = id == "existing-file"
	s.cleanupError = ctx.Err()
	return nil
}

func (s *heartbeatStore) FailUpload(ctx context.Context, _ domain.Upload, failure domain.Failure, _ time.Time) error {
	s.failure = &failure
	s.cleanupError = ctx.Err()
	return nil
}

type heartbeatSlack struct {
	domain.Slack
	wait func(context.Context) error
	key  domain.ThreadKey
}

func (s heartbeatSlack) Thread(ctx context.Context, _ domain.ThreadKey) ([]domain.Message, error) {
	if err := s.wait(ctx); err != nil {
		return nil, err
	}
	return []domain.Message{{Timestamp: s.key.ThreadTS, UserID: "U1", Text: "root"}}, nil
}

type heartbeatHubSpot struct {
	domain.HubSpot
	wait func(context.Context) error
}

func (h heartbeatHubSpot) FindFile(ctx context.Context, _ string) (string, error) {
	if err := h.wait(ctx); err != nil {
		return "", err
	}
	return "existing-file", nil
}

func heartbeatWait(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(7 * time.Second):
		return nil
	}
}

func TestThreadHeartbeat(t *testing.T) {
	for _, tc := range []struct {
		name         string
		renewError   error
		cancel       bool
		wantRenewals int
	}{
		{"renews during reconciliation", nil, false, 2},
		{"renewal failure cancels reconciliation", domain.ErrLeaseLost, false, 1},
		{"service cancellation releases work", nil, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				started := time.Now()
				duration := 9 * time.Second
				db := &heartbeatStore{renewError: tc.renewError}
				key := domain.ThreadKey{WorkspaceID: "T1", ChannelID: "C1", ThreadTS: "1.000001"}
				lease := domain.Lease{Token: "thread-token", Thread: domain.Thread{Key: key, NoteID: "existing-note", Attempts: 1}}
				hubspot := &fakeHubSpot{}
				w := New(Options{Store: db, Slack: heartbeatSlack{wait: heartbeatWait, key: key}, HubSpot: hubspot, LeaseDuration: duration, BotUserID: "UBOT", Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if tc.cancel {
					go func() { time.Sleep(time.Second); cancel() }()
				}

				w.process(ctx, "worker", lease)

				if len(db.threadRenewals) != tc.wantRenewals {
					t.Fatalf("renewals = %d, want %d", len(db.threadRenewals), tc.wantRenewals)
				}
				for i, at := range db.threadRenewals {
					if !at.Equal(started.Add(time.Duration(i+1)*3*time.Second)) || db.threadDurations[i] != duration {
						t.Errorf("renewal %d = %v, %v", i, at, db.threadDurations[i])
					}
				}
				if tc.renewError != nil || tc.cancel {
					if db.failure == nil || db.failure.Permanent || db.failure.Error != "operation cancelled" || db.failure.NextAttempt.IsZero() || db.threadCompleted || hubspot.updates != 0 {
						t.Fatalf("canceled work was not released: %+v", db)
					}
				} else if !db.threadCompleted || db.failure != nil || hubspot.updates != 1 {
					t.Fatalf("work did not complete: %+v", db)
				}
				if db.cleanupError != nil {
					t.Errorf("cleanup used canceled context: %v", db.cleanupError)
				}
			})
		})
	}
}

func TestUploadHeartbeat(t *testing.T) {
	for _, tc := range []struct {
		name         string
		renewError   error
		cancel       bool
		wantRenewals int
	}{
		{"renews during file lookup", nil, false, 2},
		{"renewal failure cancels upload", domain.ErrLeaseLost, false, 1},
		{"service cancellation releases upload", nil, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				started := time.Now()
				duration := 9 * time.Second
				db := &heartbeatStore{renewError: tc.renewError}
				lease := domain.Lease{Token: "thread-token", Thread: domain.Thread{Key: domain.ThreadKey{WorkspaceID: "T1", ChannelID: "C1", ThreadTS: "1.000001"}}}
				w := New(Options{Store: db, HubSpot: heartbeatHubSpot{wait: heartbeatWait}, LeaseDuration: duration, MaxAttempts: 1})
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if tc.cancel {
					go func() { time.Sleep(time.Second); cancel() }()
				}

				id, err := w.attachment(context.Background(), ctx, lease, domain.File{ID: "F1"})

				if len(db.uploadRenewals) != tc.wantRenewals {
					t.Fatalf("renewals = %d, want %d", len(db.uploadRenewals), tc.wantRenewals)
				}
				for i, at := range db.uploadRenewals {
					if !at.Equal(started.Add(time.Duration(i+1)*3*time.Second)) || db.uploadDurations[i] != duration {
						t.Errorf("renewal %d = %v, %v", i, at, db.uploadDurations[i])
					}
				}
				if tc.renewError != nil || tc.cancel {
					if !errors.Is(err, context.Canceled) || db.failure == nil || db.failure.Permanent || db.failure.NextAttempt.IsZero() || db.uploadCompleted {
						t.Fatalf("canceled upload was not released: id=%q err=%v state=%+v", id, err, db)
					}
				} else if err != nil || id != "existing-file" || !db.uploadCompleted || db.failure != nil {
					t.Fatalf("upload = %q, %v, state=%+v", id, err, db)
				}
				if db.cleanupError != nil {
					t.Errorf("cleanup used canceled context: %v", db.cleanupError)
				}
			})
		})
	}
}
