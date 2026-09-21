package conformance

import (
	"context"
	"errors"
	"testing"
	"time"

	"slackhubspot/internal/domain"
)

// RunIsolation verifies that independent application namespaces can reuse every
// external identity in one physical database without sharing durable state.
func RunIsolation(t *testing.T, first, second domain.Store) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 9, 21, 12, 0, 0, 0, time.UTC)
	key := domain.ThreadKey{ChannelID: "same-channel", ThreadTS: "1000.000001", WorkspaceID: "same-workspace"}
	event := domain.Event{ID: "same-event", Key: key, Kind: "link", MessageTS: key.ThreadTS, Now: now, Reply: "first namespace", TicketID: "first-ticket"}
	if _, err := first.Record(ctx, event); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Get(ctx, key); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("thread leaked across namespaces: %v", err)
	}
	event.Reply, event.TicketID = "second namespace", "second-ticket"
	result, err := second.Record(ctx, event)
	if err != nil || result.Duplicate {
		t.Fatalf("event deduplication leaked across namespaces: %+v %v", result, err)
	}
	firstLease, err := first.Claim(ctx, "same-worker", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	secondLease, err := second.Claim(ctx, "same-worker", now, time.Minute)
	if err != nil {
		t.Fatalf("thread work claim leaked across namespaces: %v", err)
	}
	if err := first.SaveNote(ctx, firstLease, "first-note", now); err != nil {
		t.Fatal(err)
	}
	if err := second.SaveNote(ctx, secondLease, "second-note", now); err != nil {
		t.Fatal(err)
	}
	firstUpload, err := first.ClaimUpload(ctx, firstLease, "same-file", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	secondUpload, err := second.ClaimUpload(ctx, secondLease, "same-file", now, time.Minute)
	if err != nil {
		t.Fatalf("upload claim leaked across namespaces: %v", err)
	}
	if err := first.CompleteUpload(ctx, firstUpload, "first-remote-file", now); err != nil {
		t.Fatal(err)
	}
	if err := second.CompleteUpload(ctx, secondUpload, "second-remote-file", now); err != nil {
		t.Fatal(err)
	}
	firstCached, err := first.ClaimUpload(ctx, firstLease, "same-file", now, time.Minute)
	if err != nil || firstCached.HubSpotID != "first-remote-file" {
		t.Fatalf("first file mapping changed: %+v %v", firstCached, err)
	}
	secondCached, err := second.ClaimUpload(ctx, secondLease, "same-file", now, time.Minute)
	if err != nil || secondCached.HubSpotID != "second-remote-file" {
		t.Fatalf("second file mapping changed: %+v %v", secondCached, err)
	}
	firstResponse, err := first.ClaimNotification(ctx, "same-notifier", now, time.Minute)
	if err != nil || firstResponse.Text != "first namespace" {
		t.Fatalf("first outbox not isolated: %+v %v", firstResponse, err)
	}
	secondResponse, err := second.ClaimNotification(ctx, "same-notifier", now, time.Minute)
	if err != nil || secondResponse.Text != "second namespace" {
		t.Fatalf("second outbox not isolated: %+v %v", secondResponse, err)
	}
	stop := domain.Event{Command: "untrack", ID: "same-command", Key: key, Kind: "command", MessageTS: "1001.000001", Now: now}
	if _, err := first.Record(ctx, stop); err != nil {
		t.Fatal(err)
	}
	if err := second.Validate(ctx, secondLease, now); err != nil {
		t.Fatalf("tracking command invalidated another namespace: %v", err)
	}
	firstThread, err := first.Get(ctx, key)
	if err != nil || firstThread.Active || firstThread.NoteID != "first-note" || firstThread.TicketID != "first-ticket" {
		t.Fatalf("first thread mapping changed: %+v %v", firstThread, err)
	}
	secondThread, err := second.Get(ctx, key)
	if err != nil || !secondThread.Active || secondThread.NoteID != "second-note" || secondThread.TicketID != "second-ticket" {
		t.Fatalf("second thread mapping changed: %+v %v", secondThread, err)
	}
	if result, err := second.Record(ctx, stop); err != nil || result.Duplicate {
		t.Fatalf("tracking command deduplication leaked: %+v %v", result, err)
	}
}
