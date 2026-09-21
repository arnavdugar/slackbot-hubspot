package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"slices"
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
	check       func()
	wroteHeader bool
}

func (w *committedResponseWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	if status == http.StatusOK && w.check != nil {
		w.check()
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *committedResponseWriter) Write(body []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

func TestSignedEventCommitsBeforeAcknowledgement(t *testing.T) {
	for _, tc := range []struct {
		name               string
		before             []string
		text, reply        string
		unlinked, readOnly bool
	}{
		{"link", nil, "<@UBOT> 456", "Ticket summary 456", false, false},
		{"untrack", []string{"link"}, "<@UBOT> untrack", "Tracking stopped", false, false},
		{"resume", []string{"link", "untrack"}, "<@UBOT> resume", "Tracking resumed", false, false},
		{"sync", []string{"link"}, "<@UBOT> sync", "Sync queued", false, false},
		{"sync while stopped", []string{"link", "untrack"}, "<@UBOT> sync", "Use @bot resume", false, true},
		{"status", []string{"link", "untrack"}, "<@UBOT> status", "Tracking: stopped", false, true},
		{"help", []string{"link", "untrack"}, "<@UBOT> help", "*Start tracking*", false, true},
		{"malformed command", []string{"link", "untrack"}, "<@UBOT> resume 456", "I couldn't understand", false, true},
		{"unlinked help", nil, "<@UBOT> help", "*Start tracking*", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options, handler, observer, _, key := durableEventHandler(t)
			prepareEventThread(t, options, key, tc.before)
			before, beforeError := observer.Get(context.Background(), key)
			if beforeError != nil && !errors.Is(beforeError, domain.ErrNotFound) {
				t.Fatal(beforeError)
			}
			acknowledgements := 0
			wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handler.ServeHTTP(&committedResponseWriter{ResponseWriter: w, check: func() {
					acknowledgements++
					thread, err := observer.Get(context.Background(), key)
					if tc.unlinked {
						if !errors.Is(err, domain.ErrNotFound) {
							t.Fatalf("read-only command created mapping: %+v, %v", thread, err)
						}
					} else {
						if err != nil {
							t.Fatal(err)
						}
						if thread.TicketID != "456" {
							t.Errorf("ticket = %q", thread.TicketID)
						}
						comparison := thread
						if tc.name == "sync while stopped" {
							if thread.LastCommandTS != "1700000001.000001" {
								t.Errorf("stopped sync did not fence older commands: %+v", thread)
							}
							comparison.LastCommandTS = before.LastCommandTS
						}
						if tc.readOnly && !reflect.DeepEqual(comparison, before) {
							t.Errorf("read-only command changed state: %+v", thread)
						}
						switch tc.name {
						case "link":
							if !thread.Active || thread.RequestedGeneration != 1 {
								t.Errorf("link not committed: %+v", thread)
							}
						case "untrack":
							if thread.Active || thread.Revision != before.Revision+1 || thread.RequestedGeneration != before.RequestedGeneration || thread.LeaseToken != "" {
								t.Errorf("stop not committed: %+v", thread)
							}
						case "resume":
							if !thread.Active || thread.Revision != before.Revision+1 || thread.RequestedGeneration != before.RequestedGeneration+1 {
								t.Errorf("resume not committed: %+v", thread)
							}
						case "sync":
							if !thread.Active || thread.RequestedGeneration != before.RequestedGeneration+1 {
								t.Errorf("sync not committed: %+v", thread)
							}
						}
						if len(tc.before) > 0 && thread.NoteID != "existing-note" {
							t.Errorf("note mapping lost: %+v", thread)
						}
					}
					notification, err := observer.ClaimNotification(context.Background(), "observer", options.Now(), time.Minute)
					if err != nil || notification.Key != key || !strings.Contains(notification.Text, tc.reply) {
						t.Errorf("reply not committed: %+v, %v", notification, err)
					}
				}}, r)
			})

			timestamp := "1700000001.000001"
			if tc.name == "link" {
				timestamp = key.ThreadTS
			}
			postTestEvent(t, options, wrapped, key, "event", "message", tc.text, timestamp)

			if acknowledgements != 1 {
				t.Fatalf("commit checks = %d, want 1", acknowledgements)
			}
		})
	}
}

func TestSignedDuplicateEventDoesNotChangeStateOrQueueReply(t *testing.T) {
	options, handler, observer, hubspot, key := durableEventHandler(t)
	postTestEvent(t, options, handler, key, "link", "message", "<@UBOT> 456", key.ThreadTS)
	notification := claimTestReply(t, options)
	if err := options.Store.CompleteNotification(context.Background(), notification, options.Now()); err != nil {
		t.Fatal(err)
	}
	before, err := observer.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}

	postTestEvent(t, options, handler, key, "link", "message", "<@UBOT> 456", key.ThreadTS)

	after, err := observer.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Errorf("duplicate changed state: %+v", after)
	}
	if _, err := observer.ClaimNotification(context.Background(), "observer", options.Now(), time.Minute); !errors.Is(err, domain.ErrNoWork) {
		t.Errorf("duplicate queued reply: %v", err)
	}
	if hubspot.calls != 1 {
		t.Errorf("ticket lookups = %d, want 1", hubspot.calls)
	}
}

func TestStoppedThreadIgnoresReplayedAndOrdinaryEvents(t *testing.T) {
	for _, tc := range []struct{ name, id, text string }{
		{"duplicate untrack", "setup-1", "<@UBOT> untrack"},
		{"replayed root", "root-replay", "<@UBOT> 456"},
		{"ordinary reply", "reply", "posted while stopped"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options, handler, observer, hubspot, key := durableEventHandler(t)
			prepareEventThread(t, options, key, []string{"link", "untrack"})
			before, err := observer.Get(context.Background(), key)
			if err != nil {
				t.Fatal(err)
			}

			timestamp := "1700000001.000001"
			if tc.name == "replayed root" {
				timestamp = key.ThreadTS
			}
			postTestEvent(t, options, handler, key, tc.id, "message", tc.text, timestamp)

			after, err := observer.Get(context.Background(), key)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Errorf("stopped state changed: %+v", after)
			}
			if _, err := observer.ClaimNotification(context.Background(), "observer", options.Now(), time.Minute); !errors.Is(err, domain.ErrNoWork) {
				t.Errorf("reply = %v, want no work", err)
			}
			if _, err := observer.Claim(context.Background(), "worker", options.Now(), time.Minute); !errors.Is(err, domain.ErrNoWork) {
				t.Errorf("work = %v, want none", err)
			}
			if hubspot.calls != 0 {
				t.Errorf("unexpected ticket lookup count = %d", hubspot.calls)
			}
		})
	}
}

func TestSignedThreadLifecycleSurvivesRestart(t *testing.T) {
	options, handler, observer, hubspot, key, config := durableEventHandlerWithConfig(t)
	postTestEvent(t, options, handler, key, "link", "message", "<@UBOT> 456", key.ThreadTS)
	notification := claimTestReply(t, options)
	if err := options.Store.CompleteNotification(context.Background(), notification, options.Now()); err != nil {
		t.Fatal(err)
	}
	lease, err := options.Store.Claim(context.Background(), "old-worker", options.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := options.Store.SaveNote(context.Background(), lease, "existing-note", options.Now()); err != nil {
		t.Fatal(err)
	}

	postTestEvent(t, options, handler, key, "stop", "message", "<@UBOT> untrack")
	if err := options.Store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := observer.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := store.Open(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	options.Store, observer = reopened, reopened
	handler = New(options)
	body := `{"type":"event_callback","event_id":"resume","team_id":"TTEAM","event":{"type":"message","channel":"CPRIVATE","user":"UUSER","text":"<@UBOT> resume","ts":"1700000002.000001","thread_ts":"1700000000.000001"}}`
	response := signed(t, handler, body, options.Now().Unix(), options.SigningSecret)

	if response.Code != http.StatusOK {
		t.Fatalf("resume status = %d", response.Code)
	}
	thread, err := observer.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if !thread.Active || thread.NoteID != "existing-note" || thread.TicketID != "456" || thread.RequestedGeneration != 2 {
		t.Fatalf("restart/resume lost state: %+v", thread)
	}
	fresh, err := observer.Claim(context.Background(), "new-worker", options.Now(), time.Minute)
	if err != nil || fresh.Token == lease.Token {
		t.Fatalf("fresh work = %+v, %v", fresh, err)
	}
	if err := observer.Complete(context.Background(), lease, domain.Completion{}, options.Now()); !errors.Is(err, domain.ErrLeaseLost) {
		t.Errorf("old worker still valid: %v", err)
	}
	if hubspot.calls != 1 {
		t.Errorf("ticket lookups = %d, want 1", hubspot.calls)
	}
}

func TestCommitObserverChecksImplicitSuccessHeader(t *testing.T) {
	response := httptest.NewRecorder()
	checks := 0
	writer := &committedResponseWriter{ResponseWriter: response, check: func() { checks++ }}

	_, err := writer.Write([]byte("ok"))

	if err != nil || response.Code != http.StatusOK || checks != 1 {
		t.Fatalf("implicit acknowledgement check: code=%d checks=%d err=%v", response.Code, checks, err)
	}
}

func durableEventHandlerWithConfig(t *testing.T) (Options, http.Handler, domain.Store, *testHubSpot, domain.ThreadKey, store.Config) {
	t.Helper()
	ctx := context.Background()
	config := store.Config{"driver": "sqlite", "sqlite": map[string]any{"path": filepath.Join(t.TempDir(), "events.sqlite")}}
	db, err := store.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	observer, err := store.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = observer.Close() })
	options, _, _, hubspot := setup(t)
	options.Store = db
	key := domain.ThreadKey{ChannelID: "CPRIVATE", ThreadTS: "1700000000.000001", WorkspaceID: "TTEAM"}
	return options, New(options), observer, hubspot, key, config
}

func durableEventHandler(t *testing.T) (Options, http.Handler, domain.Store, *testHubSpot, domain.ThreadKey) {
	t.Helper()
	options, handler, observer, hubspot, key, _ := durableEventHandlerWithConfig(t)
	return options, handler, observer, hubspot, key
}

func prepareEventThread(t *testing.T, options Options, key domain.ThreadKey, commands []string) {
	t.Helper()
	ctx := context.Background()
	for i, command := range commands {
		event := domain.Event{ID: fmt.Sprintf("setup-%d", i), Key: key, Kind: "command", Command: command, MessageTS: fmt.Sprintf("1700000000.%06d", i+1), Now: options.Now()}
		if command == "link" {
			event.Kind = "link"
			event.Command = ""
			event.TicketID = "456"
			event.Reply = "Ticket summary"
		}
		if _, err := options.Store.Record(ctx, event); err != nil {
			t.Fatal(err)
		}
		notification := claimTestReply(t, options)
		if err := options.Store.CompleteNotification(ctx, notification, options.Now()); err != nil {
			t.Fatal(err)
		}
		if command == "link" {
			lease, err := options.Store.Claim(ctx, "setup", options.Now(), time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if err := options.Store.SaveNote(ctx, lease, "existing-note", options.Now()); err != nil {
				t.Fatal(err)
			}
			if err := options.Store.Complete(ctx, lease, domain.Completion{}, options.Now()); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestCommandMessageAndMentionQueueOneReply(t *testing.T) {
	for _, command := range []string{"help", "status", "sync", "untrack", "resume", "bogus"} {
		for _, order := range []struct {
			name  string
			types [2]string
		}{
			{name: "message first", types: [2]string{"message", "app_mention"}},
			{name: "mention first", types: [2]string{"app_mention", "message"}},
		} {
			t.Run(command+"/"+order.name, func(t *testing.T) {
				options, handler, key := linkedEventHandler(t)

				for i, eventType := range order.types {
					postTestEvent(t, options, handler, key, fmt.Sprintf("event-%d", i), eventType, "<@UBOT> "+command)
				}

				notification := claimTestReply(t, options)
				if notification.Text == "" {
					t.Fatal("command reply is empty")
				}
				if _, err := options.Store.ClaimNotification(context.Background(), "observer", options.Now(), time.Minute); !errors.Is(err, domain.ErrNoWork) {
					t.Fatalf("second reply claim = %v, want no work", err)
				}
			})
		}
	}
}

func TestRepliesPersistBlockLayouts(t *testing.T) {
	for _, tc := range []struct {
		name, text, threadTS, wantText string
		wantTypes                      []string
	}{
		{name: "ticket summary", text: "<@UBOT> 456", threadTS: "1700000001.000001", wantText: "Ticket summary 456"},
		{name: "status", text: "<@UBOT> status", threadTS: "1700000000.000001", wantText: "Tracking: active", wantTypes: []string{"section", "section", "context"}},
		{name: "help", text: "<@UBOT> help", threadTS: "1700000000.000001", wantText: "*Start tracking*", wantTypes: []string{"header", "section", "divider", "section", "section", "section", "section", "section", "section", "context"}},
		{name: "invalid command", text: "<@UBOT> bogus", threadTS: "1700000000.000001", wantText: "I couldn't understand", wantTypes: []string{"section", "header", "section", "divider", "section", "section", "section", "section", "section", "section", "context"}},
		{name: "confirmation stays text", text: "<@UBOT> sync", threadTS: "1700000000.000001", wantText: "Sync queued."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options, handler, key := linkedEventHandler(t)
			key.ThreadTS = tc.threadTS

			postTestEvent(t, options, handler, key, "event", "message", tc.text)

			notification := claimTestReply(t, options)
			if !strings.Contains(notification.Text, tc.wantText) {
				t.Errorf("reply = %q, want %q", notification.Text, tc.wantText)
			}
			var types []string
			for _, block := range notification.Blocks {
				types = append(types, block.Type)
			}
			if !slices.Equal(types, tc.wantTypes) {
				t.Errorf("block types = %v, want %v", types, tc.wantTypes)
			}
		})
	}
}

func linkedEventHandler(t *testing.T) (Options, http.Handler, domain.ThreadKey) {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, store.Config{"driver": "sqlite", "sqlite": map[string]any{"path": filepath.Join(t.TempDir(), "events.sqlite")}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	options, _, _, _ := setup(t)
	options.Store = db
	key := domain.ThreadKey{WorkspaceID: "TTEAM", ChannelID: "CPRIVATE", ThreadTS: "1700000000.000001"}
	if _, err := db.Record(ctx, domain.Event{ID: "link", Kind: "link", Key: key, MessageTS: key.ThreadTS, TicketID: "456", Now: options.Now()}); err != nil {
		t.Fatal(err)
	}
	notification := claimTestReply(t, options)
	if err := db.CompleteNotification(ctx, notification, options.Now()); err != nil {
		t.Fatal(err)
	}
	return options, New(options), key
}

func claimTestReply(t *testing.T, options Options) domain.Notification {
	t.Helper()
	notification, err := options.Store.ClaimNotification(context.Background(), "observer", options.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return notification
}

func postTestEvent(t *testing.T, options Options, handler http.Handler, key domain.ThreadKey, id, eventType, text string, timestamps ...string) {
	t.Helper()
	messageTS := "1700000001.000001"
	if len(timestamps) > 0 {
		messageTS = timestamps[0]
	}
	body, err := json.Marshal(map[string]any{
		"type": "event_callback", "event_id": id, "team_id": key.WorkspaceID,
		"event": map[string]any{"type": eventType, "channel": key.ChannelID, "user": "UUSER", "text": text, "ts": messageTS, "thread_ts": key.ThreadTS},
	})
	if err != nil {
		t.Fatal(err)
	}
	response := signed(t, handler, string(body), options.Now().Unix(), options.SigningSecret)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.Code)
	}
}
