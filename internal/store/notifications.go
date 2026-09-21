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
	return s.claimNotification(ctx, now, duration, false)
}

func (s *Store) ClaimReactionNotification(ctx context.Context, worker string, now time.Time, duration time.Duration) (domain.Notification, error) {
	return s.claimNotification(ctx, now, duration, true)
}

func (s *Store) claimNotification(ctx context.Context, now time.Time, duration time.Duration, reactions bool) (domain.Notification, error) {
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
		records := make([]notificationRecord, len(entries))
		blocked := make(map[domain.ThreadKey]bool)
		for i, entry := range entries {
			if err := json.Unmarshal(entry.Value, &records[i]); err != nil {
				return err
			}
			record := records[i]
			if !record.Completed && !record.Permanent && (!record.Reactions || record.LeaseExpires.After(now) || record.NextAttempt.After(now)) {
				blocked[record.Key] = true
			}
		}
		for i, entry := range entries {
			record := records[i]
			if record.Reactions != reactions || record.Completed || record.Permanent || record.NextAttempt.After(now) || record.LeaseExpires.After(now) {
				continue
			}
			if reactions {
				if blocked[record.Key] {
					continue
				}
				thread, err := read[domain.Thread](ctx, tx, threadKey(record.Key))
				if err != nil {
					return err
				}
				if thread.Active && (thread.State == "pending" || (thread.State == "running" && thread.LeaseExpires.After(now))) {
					continue
				}
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

// Reaction jobs reuse the outbox's leases and retries. They store no emoji
// history or desired reaction snapshot: the worker reads current state.
func queueReactions(ctx context.Context, tx backend.Transaction, key domain.ThreadKey) error {
	entries, err := tx.Scan(ctx, "notifications/")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		var pending notificationRecord
		if err := json.Unmarshal(entry.Value, &pending); err != nil {
			return err
		}
		if pending.Reactions && pending.Key == key && !pending.Completed && !pending.Permanent && pending.LeaseToken == "" {
			return nil
		}
	}
	record := notificationRecord{Notification: domain.Notification{ID: "reactions/" + token(), Key: key, Reactions: true}}
	return write(ctx, tx, "notifications/"+record.ID, record)
}

func (s *Store) QueueReactions(ctx context.Context, key domain.ThreadKey) error {
	return s.db.Update(ctx, func(tx backend.Transaction) error {
		if _, err := read[domain.Thread](ctx, tx, threadKey(key)); err != nil {
			return err
		}
		return queueReactions(ctx, tx, key)
	})
}

func (s *Store) RenewNotification(ctx context.Context, claim domain.Notification, now time.Time, duration time.Duration) error {
	if duration <= 0 {
		return errors.New("lease duration must be positive")
	}
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
		record.LeaseExpires = now.Add(duration)
		return write(ctx, tx, key, record)
	})
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
		if err := write(ctx, tx, key, record); err != nil {
			return err
		}
		if record.UpdateTracking {
			return queueReactions(ctx, tx, record.Key)
		}
		return nil
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
		if err := write(ctx, tx, key, record); err != nil {
			return err
		}
		if failure.Permanent && record.UpdateTracking {
			return queueReactions(ctx, tx, record.Key)
		}
		return nil
	})
}
