package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"slackhubspot/internal/domain"
	"slackhubspot/internal/store"
)

// Checking through another connection when the response headers are written
// catches an acknowledgement sent before its state and reply outbox commit.
type committedResponseWriter struct {
	http.ResponseWriter
	check func()
}

func (w committedResponseWriter) WriteHeader(status int) {
	if status == http.StatusOK && w.check != nil {
		w.check()
	}
	w.ResponseWriter.WriteHeader(status)
}

func TestSignedEventsCommitTrackingAndRepliesBeforeAcknowledgement(t *testing.T) {
	ctx := context.Background()
	config := store.Config{"driver": "sqlite", "sqlite": map[string]any{"path": filepath.Join(t.TempDir(), "events.sqlite")}}
	durable, err := store.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = durable.Close() })
	if err := durable.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	observer, err := store.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = observer.Close() })
	options, _, _, hubspot := setup(t)
	options.Store = durable
	handler := New(options)
	now := options.Now()
	key := domain.ThreadKey{ChannelID: "CPRIVATE", ThreadTS: "1700000000.000001", WorkspaceID: "TTEAM"}
	read := func() domain.Thread {
		t.Helper()
		thread, err := observer.Get(ctx, key)
		if err != nil {
			t.Fatal(err)
		}
		return thread
	}
	reply := func(expected string) {
		t.Helper()
		notification, err := observer.ClaimNotification(ctx, "outbox-observer", now, time.Minute)
		if err != nil {
			t.Fatalf("committed reply unavailable: %v", err)
		}
		if notification.Key != key || !strings.Contains(notification.Text, expected) {
			t.Fatalf("reply = %+v, want %q in linked thread", notification, expected)
		}
		if err := observer.CompleteNotification(ctx, notification, now); err != nil {
			t.Fatal(err)
		}
	}
	noReply := func() {
		t.Helper()
		if _, err := observer.ClaimNotification(ctx, "outbox-observer", now, time.Minute); !errors.Is(err, domain.ErrNoWork) {
			t.Fatalf("unexpected reply or outbox error: %v", err)
		}
	}
	post := func(id, timestamp, text string, atAcknowledgement func()) {
		t.Helper()
		event := map[string]any{"channel": key.ChannelID, "text": text, "ts": timestamp, "type": "message", "user": "UUSER"}
		if timestamp != key.ThreadTS {
			event["thread_ts"] = key.ThreadTS
		}
		body, err := json.Marshal(map[string]any{"event": event, "event_id": id, "team_id": key.WorkspaceID, "type": "event_callback"})
		if err != nil {
			t.Fatal(err)
		}
		response := signed(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			handler.ServeHTTP(committedResponseWriter{ResponseWriter: w, check: atAcknowledgement}, r)
		}), string(body), now.Unix(), options.SigningSecret)
		if response.Code != http.StatusOK {
			t.Fatalf("event %s returned %d", id, response.Code)
		}
	}

	post("EVLINK", key.ThreadTS, "<@UBOT> 456", func() {
		thread := read()
		if !thread.Active || thread.RequestedGeneration != 1 || thread.TicketID != "456" {
			t.Fatalf("link not committed before acknowledgement: %+v", thread)
		}
		reply("Ticket summary 456")
	})
	linked := read()
	post("EVLINK", key.ThreadTS, "<@UBOT> 456", func() {
		if thread := read(); !reflect.DeepEqual(thread, linked) {
			t.Fatal("duplicate event changed durable state")
		}
		noReply()
	})
	if hubspot.calls != 1 {
		t.Fatalf("duplicate link repeated ticket lookup: %d calls", hubspot.calls)
	}
	lease, err := durable.Claim(ctx, "old-worker", now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := durable.SaveNote(ctx, lease, "existing-note", now); err != nil {
		t.Fatal(err)
	}

	post("EVSTOP", "1700000000.000002", "<@UBOT> untrack", func() {
		thread := read()
		if thread.Active || thread.Revision != linked.Revision+1 || thread.NoteID != "existing-note" || thread.RequestedGeneration != linked.RequestedGeneration || thread.LeaseToken != "" {
			t.Fatalf("stop not committed before acknowledgement: %+v", thread)
		}
		reply("Tracking stopped")
	})
	if err := durable.Validate(ctx, lease, now); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("untrack left old work valid: %v", err)
	}
	if err := durable.Complete(ctx, lease, domain.Completion{}, now); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("stale worker completed after untrack: %v", err)
	}

	// Restart the application's connection: tracking state and event IDs must
	// survive independently of the handler and in-memory client instances.
	if err := durable.Close(); err != nil {
		t.Fatal(err)
	}
	durable, err = store.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	options.Store = durable
	handler = New(options)
	stopped := read()
	for _, event := range []struct{ id, timestamp, text string }{
		{"EVSTOP", "1700000000.000002", "<@UBOT> untrack"},
		{"EVLINK", key.ThreadTS, "<@UBOT> 456"},
		{"EVROOTREPLAY", key.ThreadTS, "<@UBOT> 456"},
		{"EVORDINARY", "1700000000.000003", "Message posted while stopped"},
	} {
		post(event.id, event.timestamp, event.text, func() {
			if thread := read(); !reflect.DeepEqual(thread, stopped) {
				t.Fatalf("stopped thread changed for %s: %+v", event.id, thread)
			}
			noReply()
		})
	}
	if _, err := durable.Claim(ctx, "new-worker", now, time.Minute); !errors.Is(err, domain.ErrNoWork) {
		t.Fatalf("stopped thread had eligible work: %v", err)
	}
	post("EVSYNCOFF", "1700000000.000004", "<@UBOT> sync", func() {
		thread := read()
		if thread.Active || thread.RequestedGeneration != stopped.RequestedGeneration {
			t.Fatal("sync reactivated or scheduled a stopped thread")
		}
		reply("Use @bot resume")
	})
	stopped = read()
	for _, command := range []struct{ id, timestamp, text, reply string }{
		{"EVSTATUS", "1700000000.000005", "<@UBOT> status", "Tracking: stopped"},
		{"EVHELP", "1700000000.000006", "<@UBOT> help", "preserving ticket, note, files, and reactions"},
		{"EVMALFORMED", "1700000000.000007", "<@UBOT> resume 456", "I couldn't understand that request"},
	} {
		post(command.id, command.timestamp, command.text, func() {
			if thread := read(); !reflect.DeepEqual(thread, stopped) {
				t.Fatalf("read-only command %s changed thread", command.id)
			}
			reply(command.reply)
		})
	}
	post("EVRESUME", "1700000000.000008", "<@UBOT> resume", func() {
		thread := read()
		if !thread.Active || thread.Revision != stopped.Revision+1 || thread.RequestedGeneration != stopped.RequestedGeneration+1 || thread.NoteID != "existing-note" || thread.TicketID != stopped.TicketID {
			t.Fatalf("resume not committed before acknowledgement: %+v", thread)
		}
		reply("Tracking resumed; sync queued")
	})
	resumed := read()
	post("EVRESUME", "1700000000.000008", "<@UBOT> resume", func() {
		if thread := read(); !reflect.DeepEqual(thread, resumed) {
			t.Fatal("duplicate resume queued additional work")
		}
		noReply()
	})
	fresh, err := durable.Claim(ctx, "new-worker", now, time.Minute)
	if err != nil || fresh.Token == lease.Token || fresh.Generation != resumed.RequestedGeneration || fresh.Revision != resumed.Revision {
		t.Fatalf("resume did not yield fresh work: lease=%+v error=%v", fresh, err)
	}
	key.ThreadTS = "1700000000.000010"
	post("EVUNLINKEDHELP", key.ThreadTS, "<@UBOT> help", func() {
		if _, err := observer.Get(ctx, key); !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("help created a mapping on an unlinked thread: %v", err)
		}
		reply("Start tracking in a new root message")
	})
	if hubspot.calls != 1 {
		t.Fatalf("control messages or replay looked up ticket properties: %d calls", hubspot.calls)
	}
}
