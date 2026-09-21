package store

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"slackhubspot/internal/domain"
	"slackhubspot/internal/store/backend"
)

type notificationRecord struct {
	domain.Notification
	Completed   bool
	NextAttempt time.Time
	Permanent   bool
}

func validateNotification(record notificationRecord, claim domain.Notification, now time.Time) error {
	if record.Completed || record.LeaseToken == "" || record.LeaseToken != claim.LeaseToken || !record.LeaseExpires.After(now) {
		return domain.ErrLeaseLost
	}
	return nil
}

func (s *Store) ClaimNotification(ctx context.Context, worker string, now time.Time, duration time.Duration) (domain.Notification, error) {
	var notification domain.Notification
	if duration <= 0 {
		return notification, errors.New("lease duration must be positive")
	}
	err := s.db.Update(ctx, func(tx backend.Transaction) error {
		notification = domain.Notification{}
		entries, err := tx.Scan(ctx, "notifications/")
		if err != nil {
			return err
		}
		for _, entry := range entries {
			var record notificationRecord
			if err := json.Unmarshal(entry.Value, &record); err != nil {
				return err
			}
			if record.Completed || record.Permanent || record.NextAttempt.After(now) || record.LeaseExpires.After(now) {
				continue
			}
			record.Attempts++
			record.LeaseExpires = now.Add(duration)
			record.LeaseToken = token()
			notification = record.Notification
			return write(ctx, tx, entry.Key, record)
		}
		return domain.ErrNoWork
	})
	return notification, err
}

func (s *Store) CompleteNotification(ctx context.Context, claim domain.Notification, now time.Time) error {
	return s.db.Update(ctx, func(tx backend.Transaction) error {
		key := "notifications/" + claim.ID
		record, err := read[notificationRecord](ctx, tx, key)
		if errors.Is(err, domain.ErrNotFound) {
			return domain.ErrLeaseLost
		}
		if err != nil {
			return err
		}
		if err := validateNotification(record, claim, now); err != nil {
			return err
		}
		record.Completed = true
		record.LeaseExpires = time.Time{}
		record.LeaseToken = ""
		return write(ctx, tx, key, record)
	})
}

func (s *Store) FailNotification(ctx context.Context, claim domain.Notification, failure domain.Failure, now time.Time) error {
	return s.db.Update(ctx, func(tx backend.Transaction) error {
		key := "notifications/" + claim.ID
		record, err := read[notificationRecord](ctx, tx, key)
		if errors.Is(err, domain.ErrNotFound) {
			return domain.ErrLeaseLost
		}
		if err != nil {
			return err
		}
		if err := validateNotification(record, claim, now); err != nil {
			return err
		}
		record.LeaseExpires = time.Time{}
		record.LeaseToken = ""
		record.NextAttempt = failure.NextAttempt
		record.Permanent = failure.Permanent
		return write(ctx, tx, key, record)
	})
}
