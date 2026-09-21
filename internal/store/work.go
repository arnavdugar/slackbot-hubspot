package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"slackhubspot/internal/domain"
	"slackhubspot/internal/store/backend"
)

func (s *Store) Claim(ctx context.Context, worker string, now time.Time, duration time.Duration) (domain.Lease, error) {
	var lease domain.Lease
	if duration <= 0 {
		return lease, errors.New("lease duration must be positive")
	}
	err := s.db.Update(ctx, func(tx backend.Transaction) error {
		lease = domain.Lease{}
		entries, err := tx.Scan(ctx, "threads/")
		if err != nil {
			return err
		}
		for _, entry := range entries {
			var thread domain.Thread
			if err := json.Unmarshal(entry.Value, &thread); err != nil {
				return err
			}
			if !thread.Active || thread.CompletedGeneration >= thread.RequestedGeneration || thread.State == "failed" || thread.NextAttempt.After(now) || thread.LeaseExpires.After(now) {
				continue
			}
			recovered := thread.LeaseToken != ""
			thread.Attempts++
			thread.LeaseExpires = now.Add(duration)
			thread.LeaseToken = token()
			thread.State = "running"
			lease = domain.Lease{Expires: thread.LeaseExpires, Generation: thread.RequestedGeneration, Recovered: recovered, Revision: thread.Revision, Thread: thread, Token: thread.LeaseToken}
			return write(ctx, tx, entry.Key, thread)
		}
		return domain.ErrNoWork
	})
	return lease, err
}

func (s *Store) Validate(ctx context.Context, lease domain.Lease, now time.Time) error {
	return s.db.View(ctx, func(tx backend.Transaction) error {
		thread, err := read[domain.Thread](ctx, tx, threadKey(lease.Thread.Key))
		if errors.Is(err, domain.ErrNotFound) {
			return domain.ErrLeaseLost
		}
		if err != nil {
			return err
		}
		return validate(thread, lease, now)
	})
}

func (s *Store) Renew(ctx context.Context, lease domain.Lease, now time.Time, duration time.Duration) error {
	if duration <= 0 {
		return errors.New("lease duration must be positive")
	}
	return s.db.Update(ctx, func(tx backend.Transaction) error {
		thread, err := read[domain.Thread](ctx, tx, threadKey(lease.Thread.Key))
		if errors.Is(err, domain.ErrNotFound) {
			return domain.ErrLeaseLost
		}
		if err != nil {
			return err
		}
		if err := validate(thread, lease, now); err != nil {
			return err
		}
		thread.LeaseExpires = now.Add(duration)
		return write(ctx, tx, threadKey(thread.Key), thread)
	})
}

func (s *Store) SaveNote(ctx context.Context, lease domain.Lease, noteID string, now time.Time) error {
	if noteID == "" {
		return errors.New("note ID is required")
	}
	return s.db.Update(ctx, func(tx backend.Transaction) error {
		thread, err := read[domain.Thread](ctx, tx, threadKey(lease.Thread.Key))
		if errors.Is(err, domain.ErrNotFound) {
			return domain.ErrLeaseLost
		}
		if err != nil {
			return err
		}
		if err := validate(thread, lease, now); err != nil {
			return err
		}
		if thread.NoteID != "" && thread.NoteID != noteID {
			return errors.New("thread already has a different note mapping")
		}
		thread.NoteID = noteID
		return write(ctx, tx, threadKey(thread.Key), thread)
	})
}

func (s *Store) Complete(ctx context.Context, lease domain.Lease, completion domain.Completion, now time.Time) error {
	return s.db.Update(ctx, func(tx backend.Transaction) error {
		thread, err := read[domain.Thread](ctx, tx, threadKey(lease.Thread.Key))
		if errors.Is(err, domain.ErrNotFound) {
			return domain.ErrLeaseLost
		}
		if err != nil {
			return err
		}
		if err := validate(thread, lease, now); err != nil {
			return err
		}
		thread.CompletedGeneration = max(thread.CompletedGeneration, lease.Generation)
		thread.CurrentError = ""
		thread.LastSuccess = now
		thread.NextAttempt = time.Time{}
		thread.PartialError = boundedError(completion.PartialError)
		thread.State = "up-to-date"
		clearLease(&thread)
		if !completion.RetryAt.IsZero() {
			thread.NextAttempt = completion.RetryAt
			thread.RequestedGeneration = max(thread.RequestedGeneration, lease.Generation+1)
			thread.State = "retrying"
		} else {
			thread.Attempts = 0
			if thread.RequestedGeneration > thread.CompletedGeneration {
				thread.State = "pending"
			}
		}
		return write(ctx, tx, threadKey(thread.Key), thread)
	})
}

func (s *Store) Fail(ctx context.Context, lease domain.Lease, failure domain.Failure, now time.Time) error {
	return s.db.Update(ctx, func(tx backend.Transaction) error {
		thread, err := read[domain.Thread](ctx, tx, threadKey(lease.Thread.Key))
		if errors.Is(err, domain.ErrNotFound) {
			return domain.ErrLeaseLost
		}
		if err != nil {
			return err
		}
		if err := validate(thread, lease, now); err != nil {
			return err
		}
		thread.CurrentError = boundedError(failure.Error)
		thread.HistoricalError = thread.CurrentError
		thread.NextAttempt = failure.NextAttempt
		thread.State = "retrying"
		clearLease(&thread)
		if failure.Permanent {
			thread.NextAttempt = time.Time{}
			thread.State = "failed"
		}
		if thread.RequestedGeneration > lease.Generation {
			thread.Attempts = 0
			thread.NextAttempt = time.Time{}
			thread.State = "pending"
		}
		return write(ctx, tx, threadKey(thread.Key), thread)
	})
}

func (s *Store) Counts(ctx context.Context, now time.Time) (domain.WorkCounts, error) {
	var counts domain.WorkCounts
	err := s.db.View(ctx, func(tx backend.Transaction) error {
		counts = domain.WorkCounts{}
		entries, err := tx.Scan(ctx, "threads/")
		if err != nil {
			return err
		}
		for _, entry := range entries {
			var thread domain.Thread
			if err := json.Unmarshal(entry.Value, &thread); err != nil {
				return err
			}
			if !thread.Active || thread.CompletedGeneration >= thread.RequestedGeneration || thread.State == "failed" {
				continue
			}
			if thread.LeaseExpires.After(now) {
				counts.Leased++
			} else {
				counts.Pending++
			}
		}
		return nil
	})
	return counts, err
}
