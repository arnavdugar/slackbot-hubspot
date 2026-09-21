package store

import (
	"context"
	"errors"
	"time"

	"slackhubspot/internal/domain"
	"slackhubspot/internal/store/backend"
)

func validateUpload(upload domain.Upload, claim domain.Upload, now time.Time) error {
	if upload.LeaseToken == "" || upload.LeaseToken != claim.LeaseToken || !upload.LeaseExpires.After(now) {
		return domain.ErrLeaseLost
	}
	return nil
}

func (s *Store) ClaimUpload(ctx context.Context, lease domain.Lease, fileID string, now time.Time, duration time.Duration) (domain.Upload, error) {
	var upload domain.Upload
	if fileID == "" || duration <= 0 {
		return upload, errors.New("file ID and positive lease duration are required")
	}
	err := s.db.Update(ctx, func(tx backend.Transaction) error {
		upload = domain.Upload{}
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
		key := uploadKey(thread.Key.WorkspaceID, fileID)
		upload, err = read[domain.Upload](ctx, tx, key)
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return err
		}
		if errors.Is(err, domain.ErrNotFound) {
			upload = domain.Upload{FileID: fileID, WorkspaceID: thread.Key.WorkspaceID}
		}
		if upload.HubSpotID != "" {
			return nil
		}
		if upload.LeaseExpires.After(now) || upload.NextAttempt.After(now) || upload.State == "failed" {
			return domain.ErrNoWork
		}
		upload.Attempts++
		upload.LeaseExpires = now.Add(duration)
		upload.LeaseToken = token()
		upload.State = "running"
		return write(ctx, tx, key, upload)
	})
	return upload, err
}

func (s *Store) CompleteUpload(ctx context.Context, claim domain.Upload, hubspotID string, now time.Time) error {
	if hubspotID == "" {
		return errors.New("HubSpot file ID is required")
	}
	return s.db.Update(ctx, func(tx backend.Transaction) error {
		key := uploadKey(claim.WorkspaceID, claim.FileID)
		upload, err := read[domain.Upload](ctx, tx, key)
		if errors.Is(err, domain.ErrNotFound) {
			return domain.ErrLeaseLost
		}
		if err != nil {
			return err
		}
		if err := validateUpload(upload, claim, now); err != nil {
			return err
		}
		upload.CurrentError = ""
		upload.HubSpotID = hubspotID
		upload.LeaseExpires = time.Time{}
		upload.LeaseToken = ""
		upload.NextAttempt = time.Time{}
		upload.State = "complete"
		return write(ctx, tx, key, upload)
	})
}

func (s *Store) FailUpload(ctx context.Context, claim domain.Upload, failure domain.Failure, now time.Time) error {
	return s.db.Update(ctx, func(tx backend.Transaction) error {
		key := uploadKey(claim.WorkspaceID, claim.FileID)
		upload, err := read[domain.Upload](ctx, tx, key)
		if errors.Is(err, domain.ErrNotFound) {
			return domain.ErrLeaseLost
		}
		if err != nil {
			return err
		}
		if err := validateUpload(upload, claim, now); err != nil {
			return err
		}
		upload.CurrentError = boundedError(failure.Error)
		upload.LeaseExpires = time.Time{}
		upload.LeaseToken = ""
		upload.NextAttempt = failure.NextAttempt
		upload.State = "retrying"
		if failure.Permanent {
			upload.NextAttempt = time.Time{}
			upload.State = "failed"
		}
		return write(ctx, tx, key, upload)
	})
}

func (s *Store) RenewUpload(ctx context.Context, claim domain.Upload, now time.Time, duration time.Duration) error {
	if duration <= 0 {
		return errors.New("lease duration must be positive")
	}
	return s.db.Update(ctx, func(tx backend.Transaction) error {
		key := uploadKey(claim.WorkspaceID, claim.FileID)
		upload, err := read[domain.Upload](ctx, tx, key)
		if errors.Is(err, domain.ErrNotFound) {
			return domain.ErrLeaseLost
		}
		if err != nil {
			return err
		}
		if err := validateUpload(upload, claim, now); err != nil {
			return err
		}
		upload.LeaseExpires = now.Add(duration)
		return write(ctx, tx, key, upload)
	})
}
