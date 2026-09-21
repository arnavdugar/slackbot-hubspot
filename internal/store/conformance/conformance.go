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

	claimReply := func(t *testing.T, db domain.Store, at time.Time) domain.Notification {
		t.Helper()
		n, err := db.ClaimNotification(ctx, "notifier", at, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	completeReply := func(t *testing.T, db domain.Store, at time.Time) {
		t.Helper()
		if err := db.CompleteNotification(ctx, claimReply(t, db, at), at); err != nil {
			t.Fatal(err)
		}
	}
	claimReaction := func(t *testing.T, db domain.Store, at time.Time, duration time.Duration) domain.Notification {
		t.Helper()
		n, err := db.ClaimReactionNotification(ctx, "notifier", at, duration)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	noReactions := func(t *testing.T, db domain.Store, at time.Time) {
		t.Helper()
		if _, err := db.ClaimReactionNotification(ctx, "notifier", at, time.Minute); !errors.Is(err, domain.ErrNoWork) {
			t.Fatalf("reaction claim = %v, want no work", err)
		}
	}
	readyReactions := func(t *testing.T) domain.Store {
		t.Helper()
		db := open(t)
		record(t, db, link)
		completeReply(t, db, now)
		if err := db.Complete(ctx, claim(t, db, now), domain.Completion{LastMessageTS: key.ThreadTS}, now); err != nil {
			t.Fatal(err)
		}
		return db
	}

	t.Run("concurrent_command_deliveries_queue_one_reply", func(t *testing.T) {
		connections := []domain.Store{open(t), open(t)}
		type outcome struct {
			result domain.RecordResult
			err    error
		}
		outcomes := make(chan outcome, 16)
		var wg sync.WaitGroup

		for i := range 16 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				result, err := connections[i%2].Record(ctx, command("help", fmt.Sprintf("delivery-%d", i), "2000.000001"))
				outcomes <- outcome{result: result, err: err}
			}()
		}
		wg.Wait()
		close(outcomes)

		duplicates := 0
		for outcome := range outcomes {
			if outcome.err != nil {
				t.Fatal(outcome.err)
			}
			if outcome.result.Duplicate {
				duplicates++
			}
		}
		if duplicates != 15 {
			t.Errorf("duplicates = %d, want 15", duplicates)
		}
		completeReply(t, connections[0], now)
		if _, err := connections[1].ClaimNotification(ctx, "notifier", now, time.Minute); !errors.Is(err, domain.ErrNoWork) {
			t.Errorf("second reply claim = %v, want no work", err)
		}
	})

	for _, tc := range []struct{ name, workspace, channel, timestamp string }{
		{name: "timestamp", workspace: "workspace", channel: "private-channel", timestamp: "2000.000002"},
		{name: "channel", workspace: "workspace", channel: "other-channel", timestamp: "2000.000001"},
		{name: "workspace", workspace: "other-workspace", channel: "private-channel", timestamp: "2000.000001"},
	} {
		t.Run("distinct_command_"+tc.name, func(t *testing.T) {
			db := open(t)
			record(t, db, command("help", "first", "2000.000001"))
			event := command("help", "second", tc.timestamp)
			event.Key.WorkspaceID, event.Key.ChannelID = tc.workspace, tc.channel

			result := record(t, db, event)

			if result.Duplicate {
				t.Fatal("distinct command was deduplicated")
			}
		})
	}

	t.Run("concurrent_link_deliveries_commit_one_generation", func(t *testing.T) {

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

		if thread := get(t, db); thread.RequestedGeneration != 1 || thread.TicketID != "42" {
			t.Fatalf("atomic link: %+v", thread)
		}
	})

	t.Run("duplicate_message_does_not_advance_generation", func(t *testing.T) {
		db, peer := open(t), open(t)
		record(t, db, link)
		message := domain.Event{ID: "message", Key: key, Kind: "message", MessageTS: "1001.000001", Now: now}
		record(t, db, message)

		result := record(t, peer, message)

		if !result.Duplicate || get(t, db).RequestedGeneration != 2 {
			t.Fatalf("duplicate advanced generation: %+v", get(t, db))
		}
	})
	t.Run("completion_preserves_events_during_lease", func(t *testing.T) {
		db := open(t)
		record(t, db, link)
		first := claim(t, db, now)
		record(t, db, domain.Event{ID: "message", Key: key, Kind: "message", MessageTS: "1001.000001", Now: now})

		err := db.Complete(ctx, first, domain.Completion{}, now)

		if err != nil {
			t.Fatal(err)
		}
		thread := get(t, db)
		if thread.CompletedGeneration != 1 || thread.RequestedGeneration != 2 || thread.State != "pending" {
			t.Fatalf("new event lost: %+v", thread)
		}
		if next := claim(t, db, now); next.Generation != 2 {
			t.Fatalf("next generation = %d", next.Generation)
		}
	})
	t.Run("root_replay_does_not_queue_work", func(t *testing.T) {
		db := open(t)
		record(t, db, link)
		if err := db.Complete(ctx, claim(t, db, now), domain.Completion{}, now); err != nil {
			t.Fatal(err)
		}
		replay := link
		replay.ID = "root-replay"

		record(t, db, replay)

		noWork(t, db, now)
		if get(t, db).RequestedGeneration != 1 {
			t.Fatal("root replay queued work")
		}
	})
	t.Run("conflicting_ticket_preserves_mapping", func(t *testing.T) {
		db := open(t)
		record(t, db, link)
		conflict := link
		conflict.ID, conflict.TicketID = "conflicting-link", "999"

		result := record(t, db, conflict)

		if result.Outcome != "conflict" || get(t, db).TicketID != "42" {
			t.Fatalf("conflicting ticket changed mapping: %+v", result)
		}
	})

	for _, tc := range []struct {
		name   string
		mutate func(domain.Store, domain.Lease, time.Time) error
	}{
		{"complete", func(db domain.Store, lease domain.Lease, at time.Time) error {
			return db.Complete(ctx, lease, domain.Completion{}, at)
		}},
		{"fail", func(db domain.Store, lease domain.Lease, at time.Time) error {
			return db.Fail(ctx, lease, domain.Failure{Error: "timeout"}, at)
		}},
		{"note", func(db domain.Store, lease domain.Lease, at time.Time) error {
			return db.SaveNote(ctx, lease, "other-note", at)
		}},
		{"renew", func(db domain.Store, lease domain.Lease, at time.Time) error {
			return db.Renew(ctx, lease, at, time.Minute)
		}},
		{"upload", func(db domain.Store, lease domain.Lease, at time.Time) error {
			_, err := db.ClaimUpload(ctx, lease, "file", at, time.Minute)
			return err
		}},
		{"validate", func(db domain.Store, lease domain.Lease, at time.Time) error { return db.Validate(ctx, lease, at) }},
	} {
		t.Run("expired_thread_lease_rejects/"+tc.name, func(t *testing.T) {
			db := open(t)
			record(t, db, link)
			lease := claim(t, db, now)

			err := tc.mutate(db, lease, now.Add(time.Minute))

			if !errors.Is(err, domain.ErrLeaseLost) {
				t.Fatalf("expired lease accepted: %v", err)
			}
		})
	}
	t.Run("expired_thread_lease_recovery_rotates_token", func(t *testing.T) {
		db := open(t)
		record(t, db, link)
		first := claim(t, db, now)

		second := claim(t, db, now.Add(time.Minute))

		if first.Token == second.Token || !second.Recovered {
			t.Fatal("expired lease not replaced")
		}
		if err := db.Complete(ctx, first, domain.Completion{}, now.Add(time.Minute)); !errors.Is(err, domain.ErrLeaseLost) {
			t.Fatalf("old completion accepted: %v", err)
		}
	})
	t.Run("thread_renewal_extends_lease", func(t *testing.T) {
		db := open(t)
		record(t, db, link)
		lease := claim(t, db, now)

		err := db.Renew(ctx, lease, now.Add(30*time.Second), time.Minute)

		if err != nil {
			t.Fatal(err)
		}
		noWork(t, db, now.Add(time.Minute))
		if err := db.Complete(ctx, lease, domain.Completion{}, now.Add(time.Minute)); err != nil {
			t.Fatalf("renewed lease expired at original deadline: %v", err)
		}
	})

	t.Run("simultaneous_thread_claims_grant_one_lease", func(t *testing.T) {

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

	})
	t.Run("simultaneous_upload_claims_grant_one_global_lease", func(t *testing.T) {
		db, peer := open(t), open(t)
		record(t, db, link)
		first := claim(t, db, now)
		var wg sync.WaitGroup
		otherLink := link
		otherLink.ID, otherLink.Key.ThreadTS = "other-root", "1000.000002"
		record(t, db, otherLink)
		second := claim(t, peer, now)
		uploads := make(chan domain.Upload, 12)
		errorsCh := make(chan error, 12)
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

	})

	t.Run("equal_timestamp_command_is_deduplicated", func(t *testing.T) {
		db := open(t)
		record(t, db, link)
		record(t, db, command("untrack", "stop", "2000.000015"))

		result := record(t, db, command("resume", "equal-timestamp", "2000.000015"))

		if !result.Duplicate || get(t, db).Active {
			t.Fatal("duplicate command reactivated thread")
		}
	})

	t.Run("untrack_stops_and_invalidates_work", func(t *testing.T) {
		db := open(t)
		record(t, db, link)
		lease := claim(t, db, now)

		record(t, db, command("untrack", "stop", "1003.0"))

		stopped := get(t, db)
		if stopped.Active || stopped.State != "stopped" || stopped.Revision != lease.Revision+1 || stopped.LeaseToken != "" {
			t.Fatalf("bad stop: %+v", stopped)
		}
		noWork(t, db, now.Add(time.Hour))
	})
	for _, tc := range []struct {
		name   string
		mutate func(domain.Store, domain.Lease, time.Time) error
	}{
		{"complete", func(db domain.Store, lease domain.Lease, at time.Time) error {
			return db.Complete(ctx, lease, domain.Completion{}, at)
		}},
		{"fail", func(db domain.Store, lease domain.Lease, at time.Time) error {
			return db.Fail(ctx, lease, domain.Failure{Error: "timeout"}, at)
		}},
		{"note", func(db domain.Store, lease domain.Lease, at time.Time) error {
			return db.SaveNote(ctx, lease, "other-note", at)
		}},
		{"renew", func(db domain.Store, lease domain.Lease, at time.Time) error {
			return db.Renew(ctx, lease, at, time.Minute)
		}},
		{"upload", func(db domain.Store, lease domain.Lease, at time.Time) error {
			_, err := db.ClaimUpload(ctx, lease, "file", at, time.Minute)
			return err
		}},
		{"validate", func(db domain.Store, lease domain.Lease, at time.Time) error { return db.Validate(ctx, lease, at) }},
	} {
		t.Run("stopped_thread_lease_rejects/"+tc.name, func(t *testing.T) {
			db := open(t)
			record(t, db, link)
			lease := claim(t, db, now)
			record(t, db, command("untrack", "stop", "1003.0"))

			err := tc.mutate(db, lease, now)

			if !errors.Is(err, domain.ErrLeaseLost) {
				t.Fatalf("stopped lease accepted: %v", err)
			}
		})
	}
	t.Run("shared_upload_in_flight_can_complete_after_untrack", func(t *testing.T) {
		db := open(t)
		record(t, db, link)
		lease := claim(t, db, now)
		upload, err := db.ClaimUpload(ctx, lease, "shared-file", now, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		record(t, db, command("untrack", "stop", "1003.0"))

		err = db.CompleteUpload(ctx, upload, "hubspot-file", now)

		if err != nil {
			t.Fatalf("shared upload invalidated by thread stop: %v", err)
		}
	})
	for _, tc := range []struct {
		name  string
		event domain.Event
	}{
		{"old resume", command("resume", "old-resume", "1002.9")},
		{"sync", command("sync", "stopped-sync", "1004.0")},
		{"message", domain.Event{ID: "delayed-message", Key: key, Kind: "message", Now: now}},
		{"root replay", func() domain.Event { e := link; e.ID = "replay"; return e }()},
	} {
		t.Run("stopped_thread_ignores/"+tc.name, func(t *testing.T) {
			db := open(t)
			record(t, db, link)
			record(t, db, command("untrack", "stop", "1003.0"))
			before := get(t, db)

			record(t, db, tc.event)

			after := get(t, db)
			if after.Active || after.RequestedGeneration != before.RequestedGeneration {
				t.Fatal("stopped thread reactivated")
			}
			noWork(t, db, now)
		})
	}
	for _, tc := range []struct {
		name, noop, opposite string
		stopped              bool
	}{
		{"stop fences resume", "untrack", "resume", true},
		{"resume fences stop", "resume", "untrack", false},
	} {
		t.Run("noop_command_fences_older_opposite/"+tc.name, func(t *testing.T) {
			db := open(t)
			record(t, db, link)
			if tc.stopped {
				record(t, db, command("untrack", "stop", "1003.0"))
			}
			before := get(t, db)
			record(t, db, command(tc.noop, "noop", "1005.0"))

			record(t, db, command(tc.opposite, "older-than-noop", "1004.5"))

			after := get(t, db)
			if after.Active != before.Active || after.RequestedGeneration != before.RequestedGeneration || after.ChangedAt != before.ChangedAt || after.Revision != before.Revision || after.LastCommandTS != "1005.0" {
				t.Fatalf("no-op failed to fence command or changed state: %+v", after)
			}
		})
	}
	t.Run("resume_preserves_mappings_and_rotates_lease", func(t *testing.T) {
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
		if err := db.CompleteUpload(ctx, upload, "hubspot-file", now); err != nil {
			t.Fatal(err)
		}
		record(t, db, command("untrack", "stop", "1003.0"))
		stopped := get(t, db)

		record(t, db, command("resume", "resume", "1006.0"))

		resumed := get(t, db)
		if !resumed.Active || resumed.NoteID != "existing-note" || resumed.RequestedGeneration != stopped.RequestedGeneration+1 || resumed.Revision != stopped.Revision+1 {
			t.Fatalf("bad resume: %+v", resumed)
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

	for _, name := range []string{"help", "resume", "status", "sync", "untrack", "unknown"} {
		t.Run("unlinked_command_does_not_create_mapping/"+name, func(t *testing.T) {
			db := open(t)

			record(t, db, command(name, "unlinked", "2000"))

			if _, err := db.Get(ctx, key); !errors.Is(err, domain.ErrNotFound) {
				t.Fatalf("command linked unlinked thread: %v", err)
			}
			noWork(t, db, now)
			reply := claimReply(t, db, now)
			if name != "help" && name != "unknown" && !strings.Contains(reply.Text, "not tracked") {
				t.Fatalf("unexpected unlinked reply: %s", reply.Text)
			}
			if (name == "help" || name == "unknown") && (!strings.Contains(reply.Text, "resume") || !strings.Contains(reply.Text, "untrack")) {
				t.Fatalf("missing help: %s", reply.Text)
			}
		})
	}
	for _, name := range []string{"status", "help", "unknown"} {
		t.Run("linked_command_is_read_only/"+name, func(t *testing.T) {
			db := open(t)
			record(t, db, link)
			completeReply(t, db, now)
			before := get(t, db)

			record(t, db, command(name, "readonly", "3000"))

			if !reflect.DeepEqual(before, get(t, db)) {
				t.Fatal("read-only command changed persisted thread")
			}
			text := claimReply(t, db, now).Text
			if name == "status" && (!strings.Contains(text, "Ticket: 42") || !strings.Contains(text, "Last successful sync: never")) {
				t.Fatalf("incomplete status: %s", text)
			}
			if name != "status" && (!strings.Contains(text, "resume") || !strings.Contains(text, "untrack")) {
				t.Fatalf("incomplete help: %s", text)
			}
		})
	}

	t.Run("transient_failure_obeys_retry_schedule", func(t *testing.T) {
		db := open(t)
		record(t, db, link)
		lease := claim(t, db, now)
		next := now.Add(10 * time.Second)

		err := db.Fail(ctx, lease, domain.Failure{Error: "transient_error", NextAttempt: next}, now)

		if err != nil {
			t.Fatal(err)
		}
		noWork(t, db, now)
		second := claim(t, db, next)
		if second.Thread.Attempts != 2 {
			t.Fatalf("attempts = %d", second.Thread.Attempts)
		}
	})
	t.Run("permanent_failure_stops_automatic_retry", func(t *testing.T) {
		db := open(t)
		record(t, db, link)
		lease := claim(t, db, now)

		err := db.Fail(ctx, lease, domain.Failure{Error: "permanent_error", Permanent: true}, now)

		if err != nil {
			t.Fatal(err)
		}
		noWork(t, db, now.Add(time.Hour))
		thread := get(t, db)
		if thread.State != "failed" || thread.CurrentError != "permanent_error" {
			t.Fatalf("failure state: %+v", thread)
		}
	})
	failedThread := func(t *testing.T) domain.Store {
		t.Helper()
		db := open(t)
		record(t, db, link)
		if err := db.Fail(ctx, claim(t, db, now), domain.Failure{Error: "permanent_error", Permanent: true}, now); err != nil {
			t.Fatal(err)
		}
		return db
	}
	t.Run("manual_sync_resets_failed_attempts", func(t *testing.T) {
		db := failedThread(t)

		record(t, db, command("sync", "manual-repair", "2001"))

		if lease := claim(t, db, now); lease.Thread.Attempts != 1 {
			t.Fatalf("manual repair attempts = %d", lease.Thread.Attempts)
		}
	})
	partialThread := func(t *testing.T) domain.Store {
		t.Helper()
		db := failedThread(t)
		record(t, db, command("sync", "manual-repair", "2001"))
		if err := db.Complete(ctx, claim(t, db, now), domain.Completion{PartialError: "one attachment unavailable", RetryAt: now.Add(10 * time.Second)}, now); err != nil {
			t.Fatal(err)
		}
		return db
	}
	t.Run("partial_success_records_status_and_retains_history", func(t *testing.T) {
		db := failedThread(t)
		record(t, db, command("sync", "manual-repair", "2001"))
		lease := claim(t, db, now)

		err := db.Complete(ctx, lease, domain.Completion{PartialError: "one attachment unavailable", RetryAt: now.Add(10 * time.Second)}, now)

		if err != nil {
			t.Fatal(err)
		}
		partial := get(t, db)
		if partial.CurrentError != "" || partial.HistoricalError != "permanent_error" || partial.PartialError != "one attachment unavailable" || partial.LastSuccess.IsZero() || partial.State != "retrying" {
			t.Fatalf("partial status wrong: %+v", partial)
		}
	})
	t.Run("partial_retry_keeps_attempt_count_and_schedule", func(t *testing.T) {
		db := partialThread(t)
		noWork(t, db, now)

		lease := claim(t, db, now.Add(10*time.Second))

		if lease.Thread.Attempts != 2 {
			t.Fatalf("partial retry reset attempts: %d", lease.Thread.Attempts)
		}
	})
	t.Run("successful_retry_clears_partial_error_and_retains_history", func(t *testing.T) {
		db := partialThread(t)
		later := now.Add(10 * time.Second)
		lease := claim(t, db, later)

		err := db.Complete(ctx, lease, domain.Completion{}, later)

		if err != nil {
			t.Fatal(err)
		}
		thread := get(t, db)
		if thread.State != "up-to-date" || thread.PartialError != "" || thread.CurrentError != "" || thread.HistoricalError != "permanent_error" {
			t.Fatalf("recovery status wrong: %+v", thread)
		}
	})

	uploadFixture := func(t *testing.T) (domain.Store, domain.Store, domain.Lease, domain.Lease, domain.Upload) {
		t.Helper()
		db, peer := open(t), open(t)
		record(t, db, link)
		first := claim(t, db, now)
		other := link
		other.ID, other.Key.ThreadTS = "other-root", "1000.000002"
		record(t, db, other)
		second := claim(t, peer, now)
		upload, err := db.ClaimUpload(ctx, first, "same-file", now, 10*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		return db, peer, first, second, upload
	}
	t.Run("live_upload_excludes_other_thread", func(t *testing.T) {
		_, peer, _, second, _ := uploadFixture(t)

		_, err := peer.ClaimUpload(ctx, second, "same-file", now, time.Minute)

		if !errors.Is(err, domain.ErrNoWork) {
			t.Fatalf("duplicate upload claim: %v", err)
		}
	})
	for _, tc := range []struct {
		name   string
		mutate func(domain.Store, domain.Upload, time.Time) error
	}{
		{"complete", func(db domain.Store, upload domain.Upload, at time.Time) error {
			return db.CompleteUpload(ctx, upload, "remote-file", at)
		}},
		{"renew", func(db domain.Store, upload domain.Upload, at time.Time) error {
			return db.RenewUpload(ctx, upload, at, time.Minute)
		}},
		{"fail", func(db domain.Store, upload domain.Upload, at time.Time) error {
			return db.FailUpload(ctx, upload, domain.Failure{Error: "timeout"}, at)
		}},
	} {
		t.Run("expired_upload_lease_rejects/"+tc.name, func(t *testing.T) {
			db, _, _, _, upload := uploadFixture(t)

			err := tc.mutate(db, upload, now.Add(10*time.Second))

			if !errors.Is(err, domain.ErrLeaseLost) {
				t.Fatalf("expired upload accepted: %v", err)
			}
		})
	}
	t.Run("expired_upload_recovery_rotates_token", func(t *testing.T) {
		_, peer, _, second, upload := uploadFixture(t)

		reclaimed, err := peer.ClaimUpload(ctx, second, "same-file", now.Add(10*time.Second), time.Minute)

		if err != nil || reclaimed.LeaseToken == upload.LeaseToken {
			t.Fatalf("upload not reclaimed: %+v, %v", reclaimed, err)
		}
	})
	t.Run("upload_renewal_extends_lease", func(t *testing.T) {
		db, _, _, _, upload := uploadFixture(t)

		err := db.RenewUpload(ctx, upload, now.Add(5*time.Second), 10*time.Second)

		if err != nil {
			t.Fatal(err)
		}
		if err := db.CompleteUpload(ctx, upload, "remote-file", now.Add(10*time.Second)); err != nil {
			t.Fatalf("renewed upload expired at original deadline: %v", err)
		}
	})
	t.Run("upload_retry_obeys_schedule", func(t *testing.T) {
		db, _, first, _, upload := uploadFixture(t)
		later := now.Add(5 * time.Second)
		if err := db.FailUpload(ctx, upload, domain.Failure{Error: "rate_limited", NextAttempt: later}, now); err != nil {
			t.Fatal(err)
		}

		_, err := db.ClaimUpload(ctx, first, "same-file", now, time.Minute)

		if !errors.Is(err, domain.ErrNoWork) {
			t.Fatalf("upload retried before schedule: %v", err)
		}
		if _, err := db.ClaimUpload(ctx, first, "same-file", later, time.Minute); err != nil {
			t.Fatalf("scheduled retry unavailable: %v", err)
		}
	})
	t.Run("completed_upload_mapping_is_shared_between_threads", func(t *testing.T) {
		db, peer, _, second, upload := uploadFixture(t)
		if err := db.CompleteUpload(ctx, upload, "remote-file", now); err != nil {
			t.Fatal(err)
		}

		cached, err := peer.ClaimUpload(ctx, second, "same-file", now, time.Minute)

		if err != nil || cached.HubSpotID != "remote-file" {
			t.Fatalf("mapping was not shared: %+v, %v", cached, err)
		}
	})
	for _, name := range []string{"sync", "resume"} {
		t.Run("permanent_upload_manual_retry/"+name, func(t *testing.T) {
			db, peer, first, second, upload := uploadFixture(t)
			if err := db.FailUpload(ctx, upload, domain.Failure{Error: "invalid_file", Permanent: true}, now); err != nil {
				t.Fatal(err)
			}
			if err := db.Complete(ctx, first, domain.Completion{}, now); err != nil {
				t.Fatal(err)
			}
			if name == "resume" {
				record(t, db, command("untrack", "stop", "2000"))
			}
			record(t, db, command(name, "retry-command", "2001"))
			lease := claim(t, db, now)

			if _, err := peer.ClaimUpload(ctx, second, "same-file", now, time.Minute); !errors.Is(err, domain.ErrNoWork) {
				t.Fatalf("manual sync reset an unrelated thread's upload: %v", err)
			}
			failed, err := db.ClaimUpload(ctx, lease, "same-file", now, time.Minute)

			if name == "sync" {
				if err != nil || failed.State != "running" || failed.Attempts != 1 || failed.CurrentError != "" {
					t.Fatalf("manual sync did not retry: %+v, %v", failed, err)
				}
				if err := db.FailUpload(ctx, failed, domain.Failure{Error: "still_invalid", Permanent: true}, now); err != nil {
					t.Fatal(err)
				}
				if _, err := db.ClaimUpload(ctx, lease, "same-file", now, time.Minute); !errors.Is(err, domain.ErrNoWork) {
					t.Fatalf("same command retried permanent failure twice: %v", err)
				}
				return
			}
			if !errors.Is(err, domain.ErrNoWork) || failed.State != "failed" || failed.CurrentError != "invalid_file" {
				t.Fatalf("permanent file failure reset: %+v, %v", failed, err)
			}
		})
	}

	withoutReply := link
	withoutReply.Reply = ""
	for _, tc := range []struct {
		name         string
		before       []domain.Event
		event        domain.Event
		wantTracking bool
	}{
		{name: "link without text", event: withoutReply, wantTracking: true},
		{name: "untrack", before: []domain.Event{link}, event: command("untrack", "stop", "1001.0"), wantTracking: true},
		{name: "already stopped", before: []domain.Event{link, command("untrack", "stop", "1001.0")}, event: command("untrack", "repeat-stop", "1002.0"), wantTracking: true},
		{name: "sync while stopped", before: []domain.Event{link, command("untrack", "stop", "1001.0")}, event: command("sync", "stopped-sync", "1002.0"), wantTracking: true},
		{name: "resume", before: []domain.Event{link, command("untrack", "stop", "1001.0")}, event: command("resume", "resume", "1002.0"), wantTracking: true},
		{name: "already active", before: []domain.Event{link}, event: command("resume", "resume", "1001.0"), wantTracking: true},
		{name: "stale command", before: []domain.Event{link, command("sync", "sync", "1002.0")}, event: command("untrack", "stale-stop", "1001.0"), wantTracking: false},
		{name: "status", before: []domain.Event{link}, event: command("status", "status", "1001.0"), wantTracking: false},
	} {
		t.Run("confirmation_updates_tracking/"+tc.name, func(t *testing.T) {
			db := open(t)
			for _, event := range tc.before {
				record(t, db, event)
				completeReply(t, db, now)
			}

			record(t, db, tc.event)
			notification := claimReply(t, db, now)

			if notification.UpdateTracking != tc.wantTracking {
				t.Errorf("UpdateTracking = %v, want %v", notification.UpdateTracking, tc.wantTracking)
			}
		})
	}

	for _, tc := range []struct {
		name                                           string
		finishReply, finishSync, startSync, retryReply bool
	}{
		{name: "pending confirmation", finishSync: true},
		{name: "retrying confirmation", finishSync: true, retryReply: true},
		{name: "pending transcript", finishReply: true},
		{name: "running transcript", finishReply: true, startSync: true},
	} {
		t.Run("reactions_wait_for_core_work/"+tc.name, func(t *testing.T) {
			db := open(t)
			record(t, db, link)
			if err := db.QueueReactions(ctx, key); err != nil {
				t.Fatal(err)
			}
			if tc.finishReply {
				completeReply(t, db, now)
			}
			if tc.retryReply {
				if err := db.FailNotification(ctx, claimReply(t, db, now), domain.Failure{NextAttempt: now.Add(time.Minute)}, now); err != nil {
					t.Fatal(err)
				}
			}
			if tc.startSync {
				claim(t, db, now)
			}
			if tc.finishSync {
				if err := db.Complete(ctx, claim(t, db, now), domain.Completion{}, now); err != nil {
					t.Fatal(err)
				}
			}

			noReactions(t, db, now)
		})
	}

	t.Run("reaction_requests_coalesce", func(t *testing.T) {
		db := readyReactions(t)

		if err := db.QueueReactions(ctx, key); err != nil {
			t.Fatal(err)
		}
		first := claimReaction(t, db, now, time.Minute)
		if err := db.CompleteNotification(ctx, first, now); err != nil {
			t.Fatal(err)
		}

		noReactions(t, db, now)
	})

	t.Run("sync_persists_reaction_checkpoint", func(t *testing.T) {
		db := open(t)
		record(t, db, link)
		lease := claim(t, db, now)

		if err := db.Complete(ctx, lease, domain.Completion{LastMessageTS: "1001.000001"}, now); err != nil {
			t.Fatal(err)
		}

		if got := get(t, db).LastSyncedMessageTS; got != "1001.000001" {
			t.Errorf("checkpoint = %q", got)
		}
	})

	t.Run("reaction_claim_has_no_reply_payload", func(t *testing.T) {
		db := readyReactions(t)

		notification := claimReaction(t, db, now, time.Minute)

		if !notification.Reactions {
			t.Error("claim is not a reaction job")
		}
		if notification.Text != "" {
			t.Errorf("unexpected reply text: %q", notification.Text)
		}
		if notification.UpdateTracking {
			t.Error("reaction completion would queue another reaction")
		}
	})

	t.Run("expired_reaction_lease_is_recovered", func(t *testing.T) {
		db, peer := readyReactions(t), open(t)
		first := claimReaction(t, db, now, time.Second)
		later := now.Add(time.Second)

		second := claimReaction(t, peer, later, time.Minute)

		if second.ID != first.ID {
			t.Errorf("recovered job = %q, want %q", second.ID, first.ID)
		}
		if second.LeaseToken == first.LeaseToken {
			t.Error("recovered lease reused expired token")
		}
		if err := db.CompleteNotification(ctx, first, later); !errors.Is(err, domain.ErrLeaseLost) {
			t.Errorf("stale completion = %v, want lease lost", err)
		}
		if err := db.RenewNotification(ctx, first, later, time.Minute); !errors.Is(err, domain.ErrLeaseLost) {
			t.Errorf("stale renewal = %v, want lease lost", err)
		}
		if err := peer.CompleteNotification(ctx, second, later); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("reaction_lease_survives_untrack", func(t *testing.T) {
		db := readyReactions(t)
		notification := claimReaction(t, db, now, time.Minute)

		record(t, db, command("untrack", "stop", "1002.0"))

		if err := db.RenewNotification(ctx, notification, now, time.Minute); err != nil {
			t.Errorf("renewal after untrack: %v", err)
		}
		if err := db.CompleteNotification(ctx, notification, now); err != nil {
			t.Errorf("completion after untrack: %v", err)
		}
	})

	for _, tc := range []struct {
		name  string
		retry bool
	}{
		{name: "live lease"},
		{name: "retry delay", retry: true},
	} {
		t.Run("new_reaction_waits_for_existing_job/"+tc.name, func(t *testing.T) {
			db, peer := readyReactions(t), open(t)
			first := claimReaction(t, db, now, time.Minute)
			if err := db.QueueReactions(ctx, key); err != nil {
				t.Fatal(err)
			}
			if tc.retry {
				if err := db.FailNotification(ctx, first, domain.Failure{NextAttempt: now.Add(time.Minute)}, now); err != nil {
					t.Fatal(err)
				}
			}

			noReactions(t, peer, now)
		})
	}

	t.Run("stopped_thread_retains_reaction_retries", func(t *testing.T) {
		db := readyReactions(t)
		first := claimReaction(t, db, now, time.Minute)
		record(t, db, command("untrack", "stop", "1002.0"))
		completeReply(t, db, now)
		later := now.Add(time.Minute)
		if err := db.FailNotification(ctx, first, domain.Failure{NextAttempt: later}, now); err != nil {
			t.Fatal(err)
		}

		for range 2 {
			n := claimReaction(t, db, later, time.Minute)
			if err := db.CompleteNotification(ctx, n, later); err != nil {
				t.Fatal(err)
			}
		}

		noReactions(t, db, later)
	})

	t.Run("duplicate_link_queues_one_reply", func(t *testing.T) {
		db, peer := open(t), open(t)
		record(t, db, link)

		record(t, peer, link)

		reply := claimReply(t, db, now)
		if reply.Text != link.Reply {
			t.Fatal("link summary not persisted")
		}
		if err := db.CompleteNotification(ctx, reply, now); err != nil {
			t.Fatal(err)
		}
		if _, err := peer.ClaimNotification(ctx, "other", now, time.Minute); !errors.Is(err, domain.ErrNoWork) {
			t.Fatal("duplicate queued another reply")
		}
	})
	t.Run("live_reply_lease_excludes_another_notifier", func(t *testing.T) {
		db, peer := open(t), open(t)
		record(t, db, link)
		claimReply(t, db, now)

		_, err := peer.ClaimNotification(ctx, "other", now, time.Minute)

		if !errors.Is(err, domain.ErrNoWork) {
			t.Fatalf("concurrent reply lease: %v", err)
		}
	})
	t.Run("expired_reply_lease_recovery_rotates_token", func(t *testing.T) {
		db, peer := open(t), open(t)
		record(t, db, link)
		first, err := db.ClaimNotification(ctx, "first", now, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		later := now.Add(time.Second)

		second, err := peer.ClaimNotification(ctx, "second", later, time.Minute)

		if err != nil || first.LeaseToken == second.LeaseToken || first.ID != second.ID {
			t.Fatalf("reply lease not recovered: %+v, %v", second, err)
		}
		if err := db.CompleteNotification(ctx, first, later); !errors.Is(err, domain.ErrLeaseLost) {
			t.Fatal("stale reply completion accepted")
		}
	})
	t.Run("reply_retry_obeys_schedule", func(t *testing.T) {
		db := open(t)
		record(t, db, link)
		later := now.Add(time.Second)
		if err := db.FailNotification(ctx, claimReply(t, db, now), domain.Failure{NextAttempt: later}, now); err != nil {
			t.Fatal(err)
		}

		_, err := db.ClaimNotification(ctx, "notifier", now, time.Minute)

		if !errors.Is(err, domain.ErrNoWork) {
			t.Fatal("outbox ignored retry schedule")
		}
		claimReply(t, db, later)
	})
	t.Run("completed_reply_is_not_redelivered", func(t *testing.T) {
		db := open(t)
		record(t, db, link)
		notification := claimReply(t, db, now)

		err := db.CompleteNotification(ctx, notification, now)

		if err != nil {
			t.Fatal(err)
		}
		if _, err := db.ClaimNotification(ctx, "notifier", now.Add(time.Hour), time.Minute); !errors.Is(err, domain.ErrNoWork) {
			t.Fatal("completed response redelivered")
		}
	})

	for _, tc := range []struct {
		name    string
		claimed bool
		want    domain.WorkCounts
	}{
		{"pending", false, domain.WorkCounts{Pending: 1}},
		{"leased", true, domain.WorkCounts{Leased: 1}},
	} {
		t.Run("work_counts/"+tc.name, func(t *testing.T) {
			db := open(t)
			record(t, db, link)
			if tc.claimed {
				claim(t, db, now)
			}

			counts, err := db.Counts(ctx, now)

			if err != nil || counts != tc.want {
				t.Fatalf("counts: %+v, %v; want %+v", counts, err, tc.want)
			}
		})
	}
	t.Run("note_mapping_survives_restart", func(t *testing.T) {
		db := open(t)
		record(t, db, link)
		lease := claim(t, db, now)
		if err := db.SaveNote(ctx, lease, "durable-note", now); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}

		reopened := open(t)

		if get(t, reopened).NoteID != "durable-note" {
			t.Fatal("note mapping lost on restart")
		}
	})
	t.Run("restart_preserves_live_lease", func(t *testing.T) {
		db := open(t)
		record(t, db, link)
		claim(t, db, now)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}

		reopened := open(t)

		noWork(t, reopened, now)
	})
	t.Run("restart_recovers_expired_lease", func(t *testing.T) {
		db := open(t)
		record(t, db, link)
		lease := claim(t, db, now)
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		reopened := open(t)

		recovered := claim(t, reopened, now.Add(time.Minute))

		if recovered.Token == lease.Token || !recovered.Recovered {
			t.Fatal("restart did not reclaim expired lease")
		}
	})
	t.Run("stopped_state_survives_restart", func(t *testing.T) {
		db := open(t)
		record(t, db, link)
		record(t, db, command("untrack", "durable-stop", "2000"))
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}

		reopened := open(t)

		if get(t, reopened).Active {
			t.Fatal("stopped state lost on restart")
		}
		noWork(t, reopened, now.Add(time.Hour))
	})
}
