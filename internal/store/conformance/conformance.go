// Package conformance contains the common behavioral suite for storage drivers.
// Each factory must return independent connections to the same isolated dataset
// for a given test name, and a different dataset for each distinct test name.
package conformance

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"slackhubspot/internal/domain"
)

type Factory func(*testing.T) domain.Store

func Run(t *testing.T, open Factory) {
	ctx := context.Background()
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	key := domain.ThreadKey{ChannelID: "private-channel", ThreadTS: "1000.000001", WorkspaceID: "workspace"}
	link := domain.Event{Actor: "actor", ID: "root", Key: key, Kind: "link", MessageTS: key.ThreadTS, Now: now, Reply: "Ticket summary", TicketID: "42", TicketURL: "https://example.invalid/ticket/42"}
	record := func(t *testing.T, db domain.Store, event domain.Event) domain.RecordResult {
		t.Helper()
		result, err := db.Record(ctx, event)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	claim := func(t *testing.T, db domain.Store, at time.Time) domain.Lease {
		t.Helper()
		lease, err := db.Claim(ctx, "worker", at, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return lease
	}
	get := func(t *testing.T, db domain.Store) domain.Thread {
		t.Helper()
		thread, err := db.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		return thread
	}
	command := func(name, id, timestamp string) domain.Event {
		return domain.Event{Actor: "command-actor", Command: name, ID: id, Key: key, Kind: "command", MessageTS: timestamp, Now: now}
	}
	noWork := func(t *testing.T, db domain.Store, at time.Time) {
		t.Helper()
		if _, err := db.Claim(ctx, "worker", at, time.Minute); !errors.Is(err, domain.ErrNoWork) {
			t.Fatalf("expected no work, got %v", err)
		}
	}

	t.Run("atomic_event_dedup_and_generations", func(t *testing.T) {
		db := open(t)
		peer := open(t)
		var wg sync.WaitGroup
		results := make(chan domain.RecordResult, 16)
		errorsCh := make(chan error, 16)
		for i := range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				selected := db
				if i%2 == 0 {
					selected = peer
				}
				result, err := selected.Record(ctx, link)
				results <- result
				errorsCh <- err
			}()
		}
		wg.Wait()
		close(results)
		close(errorsCh)
		for err := range errorsCh {
			if err != nil {
				t.Fatal(err)
			}
		}
		duplicates := 0
		for result := range results {
			if result.Duplicate {
				duplicates++
			}
		}
		if duplicates != 15 {
			t.Fatalf("duplicates = %d, want 15", duplicates)
		}
		first := claim(t, db, now)
		if first.Generation != 1 {
			t.Fatalf("generation = %d", first.Generation)
		}
		noWork(t, peer, now)
		message := domain.Event{ID: "message", Key: key, Kind: "message", MessageTS: "1001.000001", Now: now}
		record(t, db, message)
		if !record(t, peer, message).Duplicate {
			t.Fatal("message was not deduplicated")
		}
		if err := db.Complete(ctx, first, domain.Completion{}, now); err != nil {
			t.Fatal(err)
		}
		second := claim(t, peer, now)
		if second.Generation != 2 {
			t.Fatalf("new event lost: generation %d", second.Generation)
		}
		if err := peer.Complete(ctx, second, domain.Completion{}, now); err != nil {
			t.Fatal(err)
		}
		noWork(t, db, now)
		replayed := link
		replayed.ID = "root-replay"
		record(t, db, replayed)
		noWork(t, db, now)
		if get(t, db).RequestedGeneration != 2 {
			t.Fatal("root replay queued work")
		}
		conflict := link
		conflict.ID, conflict.TicketID = "conflicting-link", "999"
		if record(t, db, conflict).Outcome != "conflict" {
			t.Fatal("different ticket was not rejected")
		}
		if get(t, db).TicketID != "42" {
			t.Fatal("ticket mapping changed")
		}
	})

	t.Run("expired_lease_and_stale_mutations", func(t *testing.T) {
		db := open(t)
		record(t, db, link)
		first := claim(t, db, now)
		later := now.Add(time.Minute)
		for name, mutate := range map[string]func() error{
			"complete": func() error { return db.Complete(ctx, first, domain.Completion{}, later) },
			"fail":     func() error { return db.Fail(ctx, first, domain.Failure{Error: "timeout"}, later) },
			"note":     func() error { return db.SaveNote(ctx, first, "note", later) },
			"renew":    func() error { return db.Renew(ctx, first, later, time.Minute) },
			"upload":   func() error { _, err := db.ClaimUpload(ctx, first, "file", later, time.Minute); return err },
			"validate": func() error { return db.Validate(ctx, first, later) },
		} {
			if err := mutate(); !errors.Is(err, domain.ErrLeaseLost) {
				t.Fatalf("%s accepted expired lease: %v", name, err)
			}
		}
		second := claim(t, db, later)
		if first.Token == second.Token || !second.Recovered {
			t.Fatal("expired lease not replaced")
		}
		if err := db.Complete(ctx, first, domain.Completion{}, later); !errors.Is(err, domain.ErrLeaseLost) {
			t.Fatalf("old completion accepted: %v", err)
		}
		if err := db.Renew(ctx, second, later.Add(30*time.Second), time.Minute); err != nil {
			t.Fatal(err)
		}
		noWork(t, db, later.Add(time.Minute))
		if err := db.Complete(ctx, second, domain.Completion{}, later.Add(time.Minute)); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("simultaneous_thread_and_upload_claims", func(t *testing.T) {
		db := open(t)
		peer := open(t)
		record(t, db, link)
		var wg sync.WaitGroup
		leases := make(chan domain.Lease, 12)
		errorsCh := make(chan error, 12)
		for i := range 12 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				selected := db
				if i%2 == 0 {
					selected = peer
				}
				lease, err := selected.Claim(ctx, fmt.Sprint(i), now, time.Minute)
				if err == nil {
					leases <- lease
				}
				errorsCh <- err
			}()
		}
		wg.Wait()
		close(leases)
		close(errorsCh)
		for err := range errorsCh {
			if err != nil && !errors.Is(err, domain.ErrNoWork) {
				t.Fatal(err)
			}
		}
		if len(leases) != 1 {
			t.Fatalf("simultaneous active thread leases = %d", len(leases))
		}
		first := <-leases
		otherLink := link
		otherLink.ID, otherLink.Key.ThreadTS = "other-root", "1000.000002"
		record(t, db, otherLink)
		second := claim(t, peer, now)
		uploads := make(chan domain.Upload, 12)
		errorsCh = make(chan error, 12)
		for i := range 12 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				selected, lease := db, first
				if i%2 == 0 {
					selected, lease = peer, second
				}
				upload, err := selected.ClaimUpload(ctx, lease, "concurrent-file", now, time.Minute)
				if err == nil {
					uploads <- upload
				}
				errorsCh <- err
			}()
		}
		wg.Wait()
		close(uploads)
		close(errorsCh)
		for err := range errorsCh {
			if err != nil && !errors.Is(err, domain.ErrNoWork) {
				t.Fatal(err)
			}
		}
		if len(uploads) != 1 {
			t.Fatalf("simultaneous global upload leases = %d", len(uploads))
		}
	})

	t.Run("concurrent_commands_converge_to_newest_timestamp", func(t *testing.T) {
		db := open(t)
		peer := open(t)
		record(t, db, link)
		var wg sync.WaitGroup
		errorsCh := make(chan error, 16)
		for i := range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				selected, name := db, "resume"
				if i%2 == 1 {
					selected, name = peer, "untrack"
				}
				_, err := selected.Record(ctx, command(name, fmt.Sprintf("concurrent-%d", i), fmt.Sprintf("2000.%06d", i)))
				errorsCh <- err
			}()
		}
		wg.Wait()
		close(errorsCh)
		for err := range errorsCh {
			if err != nil {
				t.Fatal(err)
			}
		}
		thread := get(t, db)
		if thread.Active || thread.LastCommandTS != "2000.000015" {
			t.Fatalf("commands did not converge: %+v", thread)
		}
		equal := command("resume", "equal-timestamp", "2000.000015")
		if record(t, db, equal).Outcome != "stale" || get(t, db).Active {
			t.Fatal("equal timestamp was not rejected")
		}
	})

	t.Run("stop_resume_fences_and_preserves_mappings", func(t *testing.T) {
		db := open(t)
		record(t, db, link)
		lease := claim(t, db, now)
		if err := db.SaveNote(ctx, lease, "existing-note", now); err != nil {
			t.Fatal(err)
		}
		upload, err := db.ClaimUpload(ctx, lease, "shared-file", now, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		record(t, db, command("untrack", "stop", "1003.0"))
		stopped := get(t, db)
		if stopped.Active || stopped.State != "stopped" || stopped.Revision != lease.Revision+1 {
			t.Fatalf("bad stop: %+v", stopped)
		}
		noWork(t, db, now.Add(time.Hour))
		for name, mutate := range map[string]func() error{
			"complete": func() error { return db.Complete(ctx, lease, domain.Completion{}, now) },
			"fail":     func() error { return db.Fail(ctx, lease, domain.Failure{Error: "timeout"}, now) },
			"note":     func() error { return db.SaveNote(ctx, lease, "other-note", now) },
			"renew":    func() error { return db.Renew(ctx, lease, now, time.Minute) },
			"upload":   func() error { _, err := db.ClaimUpload(ctx, lease, "another-file", now, time.Minute); return err },
		} {
			if err := mutate(); !errors.Is(err, domain.ErrLeaseLost) {
				t.Fatalf("%s accepted stopped lease: %v", name, err)
			}
		}
		// A shared upload already in flight may still complete for other threads.
		if err := db.CompleteUpload(ctx, upload, "hubspot-file", now); err != nil {
			t.Fatal(err)
		}
		record(t, db, command("resume", "old-resume", "1002.9"))
		record(t, db, command("sync", "stopped-sync", "1004.0"))
		record(t, db, domain.Event{ID: "delayed-message", Key: key, Kind: "message", Now: now})
		replay := link
		replay.ID = "replay"
		record(t, db, replay)
		if after := get(t, db); after.Active || after.RequestedGeneration != stopped.RequestedGeneration {
			t.Fatal("stopped tracking was reactivated")
		}
		record(t, db, command("untrack", "repeat-stop", "1005.0"))
		record(t, db, command("resume", "older-than-noop", "1004.5"))
		if get(t, db).Active {
			t.Fatal("no-op stop failed to fence an older resume")
		}
		record(t, db, command("resume", "resume", "1006.0"))
		resumed := get(t, db)
		if !resumed.Active || resumed.NoteID != "existing-note" || resumed.RequestedGeneration != stopped.RequestedGeneration+1 || resumed.Revision != stopped.Revision+1 {
			t.Fatalf("bad resume: %+v", resumed)
		}
		record(t, db, command("resume", "repeat-resume", "1007.0"))
		record(t, db, command("untrack", "older-than-active-noop", "1006.5"))
		if after := get(t, db); !after.Active || after.RequestedGeneration != resumed.RequestedGeneration || after.ChangedAt != resumed.ChangedAt || after.Revision != resumed.Revision {
			t.Fatal("no-op resume changed state or failed to fence older stop")
		}
		fresh := claim(t, db, now)
		if fresh.Token == lease.Token {
			t.Fatal("resume reused token")
		}
		cached, err := db.ClaimUpload(ctx, fresh, "shared-file", now, time.Minute)
		if err != nil || cached.HubSpotID != "hubspot-file" {
			t.Fatalf("file mapping not reused: %+v, %v", cached, err)
		}
		if err := db.Complete(ctx, lease, domain.Completion{}, now); !errors.Is(err, domain.ErrLeaseLost) {
			t.Fatal("pre-stop completion accepted after resume")
		}
	})

	t.Run("status_help_and_unknown_are_read_only", func(t *testing.T) {
		db := open(t)
		if _, err := db.Get(ctx, key); !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Get missing: %v", err)
		}
		for i, name := range []string{"help", "resume", "status", "sync", "untrack", "unknown"} {
			record(t, db, command(name, fmt.Sprintf("unlinked-%d", i), fmt.Sprint(2000+i)))
		}
		if _, err := db.Get(ctx, key); !errors.Is(err, domain.ErrNotFound) {
			t.Fatal("command linked an unlinked thread")
		}
		noWork(t, db, now)
		record(t, db, link)
		before := get(t, db)
		for i, name := range []string{"status", "help", "unknown"} {
			record(t, db, command(name, fmt.Sprintf("readonly-%d", i), fmt.Sprint(3000+i)))
		}
		if !reflect.DeepEqual(before, get(t, db)) {
			t.Fatal("read-only command changed persisted thread")
		}
		var messages []string
		for {
			notification, err := db.ClaimNotification(ctx, "notifier", now, time.Minute)
			if errors.Is(err, domain.ErrNoWork) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			messages = append(messages, notification.Text)
			if err := db.CompleteNotification(ctx, notification, now); err != nil {
				t.Fatal(err)
			}
		}
		text := strings.Join(messages, "\n")
		for _, expected := range []string{"not tracked", "resume", "untrack", "Ticket: 42", "Last successful sync: never"} {
			if !strings.Contains(text, expected) {
				t.Fatalf("responses missing %q", expected)
			}
		}
	})

	t.Run("retry_permanent_failure_and_partial_success", func(t *testing.T) {
		db := open(t)
		record(t, db, link)
		first := claim(t, db, now)
		next := now.Add(10 * time.Second)
		if err := db.Fail(ctx, first, domain.Failure{Error: "transient_error", NextAttempt: next}, now); err != nil {
			t.Fatal(err)
		}
		noWork(t, db, now)
		second := claim(t, db, next)
		if second.Thread.Attempts != 2 {
			t.Fatalf("attempts = %d", second.Thread.Attempts)
		}
		if err := db.Fail(ctx, second, domain.Failure{Error: "permanent_error", Permanent: true}, next); err != nil {
			t.Fatal(err)
		}
		noWork(t, db, now.Add(time.Hour))
		record(t, db, command("sync", "manual-repair", "2001"))
		third := claim(t, db, next)
		if third.Thread.Attempts != 1 {
			t.Fatalf("manual repair attempts = %d", third.Thread.Attempts)
		}
		partialRetry := next.Add(10 * time.Second)
		if err := db.Complete(ctx, third, domain.Completion{PartialError: "one attachment unavailable", RetryAt: partialRetry}, next); err != nil {
			t.Fatal(err)
		}
		partial := get(t, db)
		if partial.CurrentError != "" || partial.HistoricalError != "permanent_error" || partial.PartialError == "" || partial.LastSuccess.IsZero() || partial.State != "retrying" {
			t.Fatalf("partial status wrong: %+v", partial)
		}
		noWork(t, db, next)
		fourth := claim(t, db, partialRetry)
		if fourth.Thread.Attempts != 2 {
			t.Fatalf("partial retry reset attempts: %d", fourth.Thread.Attempts)
		}
		if err := db.Complete(ctx, fourth, domain.Completion{}, partialRetry); err != nil {
			t.Fatal(err)
		}
		complete := get(t, db)
		if complete.State != "up-to-date" || complete.PartialError != "" || complete.CurrentError != "" || complete.HistoricalError != "permanent_error" {
			t.Fatalf("recovery status wrong: %+v", complete)
		}
	})

	t.Run("shared_uploads_and_expiration", func(t *testing.T) {
		db := open(t)
		peer := open(t)
		record(t, db, link)
		first := claim(t, db, now)
		otherLink := link
		otherLink.ID = "other-root"
		otherLink.Key.ThreadTS = "1000.000002"
		record(t, db, otherLink)
		second := claim(t, peer, now)
		upload, err := db.ClaimUpload(ctx, first, "same-file", now, 10*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := peer.ClaimUpload(ctx, second, "same-file", now, time.Minute); !errors.Is(err, domain.ErrNoWork) {
			t.Fatalf("duplicate upload claim: %v", err)
		}
		later := now.Add(10 * time.Second)
		if err := db.CompleteUpload(ctx, upload, "remote-file", later); !errors.Is(err, domain.ErrLeaseLost) {
			t.Fatal("expired upload accepted")
		}
		reclaimed, err := peer.ClaimUpload(ctx, second, "same-file", later, time.Minute)
		if err != nil || reclaimed.LeaseToken == upload.LeaseToken {
			t.Fatalf("upload not reclaimed: %v", err)
		}
		if err := db.RenewUpload(ctx, upload, later, time.Minute); !errors.Is(err, domain.ErrLeaseLost) {
			t.Fatal("stale upload renew accepted")
		}
		if err := peer.FailUpload(ctx, reclaimed, domain.Failure{Error: "rate_limited", NextAttempt: later.Add(5 * time.Second)}, later); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ClaimUpload(ctx, first, "same-file", later, time.Minute); !errors.Is(err, domain.ErrNoWork) {
			t.Fatal("upload retried before schedule")
		}
		retry, err := db.ClaimUpload(ctx, first, "same-file", later.Add(5*time.Second), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.CompleteUpload(ctx, retry, "remote-file", later.Add(5*time.Second)); err != nil {
			t.Fatal(err)
		}
		cached, err := peer.ClaimUpload(ctx, second, "same-file", later.Add(5*time.Second), time.Minute)
		if err != nil || cached.HubSpotID != "remote-file" {
			t.Fatalf("mapping was not shared: %+v %v", cached, err)
		}
	})

	t.Run("outbox_dedup_and_lease_recovery", func(t *testing.T) {
		db := open(t)
		peer := open(t)
		record(t, db, link)
		record(t, peer, link)
		first, err := db.ClaimNotification(ctx, "first", now, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if first.Text != link.Reply {
			t.Fatal("link summary not persisted")
		}
		if _, err := peer.ClaimNotification(ctx, "second", now, time.Minute); !errors.Is(err, domain.ErrNoWork) {
			t.Fatal("concurrent notification lease")
		}
		later := now.Add(time.Second)
		second, err := peer.ClaimNotification(ctx, "second", later, time.Minute)
		if err != nil || first.LeaseToken == second.LeaseToken {
			t.Fatalf("outbox lease not recovered: %v", err)
		}
		if err := db.CompleteNotification(ctx, first, later); !errors.Is(err, domain.ErrLeaseLost) {
			t.Fatal("stale outbox completion accepted")
		}
		if err := peer.FailNotification(ctx, second, domain.Failure{NextAttempt: later.Add(time.Second)}, later); err != nil {
			t.Fatal(err)
		}
		if _, err := db.ClaimNotification(ctx, "first", later, time.Minute); !errors.Is(err, domain.ErrNoWork) {
			t.Fatal("outbox ignored retry schedule")
		}
		third, err := db.ClaimNotification(ctx, "first", later.Add(time.Second), time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if err := db.CompleteNotification(ctx, third, later.Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := peer.ClaimNotification(ctx, "second", later.Add(time.Hour), time.Minute); !errors.Is(err, domain.ErrNoWork) {
			t.Fatal("completed response redelivered")
		}
	})

	t.Run("durable_restart_and_work_counts", func(t *testing.T) {
		db := open(t)
		record(t, db, link)
		counts, err := db.Counts(ctx, now)
		if err != nil || counts.Pending != 1 || counts.Leased != 0 {
			t.Fatalf("pending counts: %+v %v", counts, err)
		}
		lease := claim(t, db, now)
		if err := db.SaveNote(ctx, lease, "durable-note", now); err != nil {
			t.Fatal(err)
		}
		counts, err = db.Counts(ctx, now)
		if err != nil || counts.Pending != 0 || counts.Leased != 1 {
			t.Fatalf("leased counts: %+v %v", counts, err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		reopened := open(t)
		if get(t, reopened).NoteID != "durable-note" {
			t.Fatal("note mapping lost on restart")
		}
		noWork(t, reopened, now)
		recovered := claim(t, reopened, now.Add(time.Minute))
		if recovered.Token == lease.Token {
			t.Fatal("restart did not reclaim expired lease")
		}
		record(t, reopened, command("untrack", "durable-stop", "2000"))
		if err := reopened.Close(); err != nil {
			t.Fatal(err)
		}
		restarted := open(t)
		if get(t, restarted).Active {
			t.Fatal("stopped state lost on restart")
		}
		noWork(t, restarted, now.Add(time.Hour))
	})
}
