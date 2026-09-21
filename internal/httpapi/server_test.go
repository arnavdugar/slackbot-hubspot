package httpapi

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"slackhubspot/internal/domain"
	"slackhubspot/internal/render"
)

type testStore struct {
	domain.Store
	checkErr  error
	events    []domain.Event
	recordErr error
	thread    *domain.Thread
}

func (s *testStore) Check(context.Context) error { return s.checkErr }

func (s *testStore) Get(context.Context, domain.ThreadKey) (domain.Thread, error) {
	if s.thread == nil {
		return domain.Thread{}, domain.ErrNotFound
	}
	return *s.thread, nil
}

func (s *testStore) Record(_ context.Context, event domain.Event) (domain.RecordResult, error) {
	s.events = append(s.events, event)
	return domain.RecordResult{}, s.recordErr
}

type testSlack struct {
	domain.Slack
}

type testHubSpot struct {
	domain.HubSpot
	calls int
	err   error
}

func (h *testHubSpot) Ticket(_ context.Context, id string) (domain.Ticket, error) {
	h.calls++
	return domain.Ticket{ID: id, Subject: "Ticket summary", URL: "https://app.hubspot.com/contacts/123/ticket/456"}, h.err
}

func setup(t *testing.T) (Options, *testStore, *testSlack, *testHubSpot) {
	t.Helper()
	store := &testStore{}
	slack := &testSlack{}
	hubspot := &testHubSpot{}
	summary, err := render.CompileSummary("{{.Subject}} {{.ID}}")
	if err != nil {
		t.Fatal(err)
	}
	return Options{BotID: "BBOT", BotUserID: "UBOT", HubSpot: hubspot, HubSpotAccountID: "123", Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), MaxBodyBytes: 1024, Now: func() time.Time { return time.Unix(1700000000, 0) }, SigningSecret: "test-signing-secret", Slack: slack, Store: store, Summary: summary, WorkspaceID: "TTEAM"}, store, slack, hubspot
}

func signed(t *testing.T, handler http.Handler, body string, timestamp int64, secret string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/slack/events", strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	ts := strconv.FormatInt(timestamp, 10)
	request.Header.Set("X-Slack-Request-Timestamp", ts)
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte("v0:" + ts + ":" + body))
	request.Header.Set("X-Slack-Signature", "v0="+hex.EncodeToString(mac.Sum(nil)))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func TestSignatureAndEnvelopeValidation(t *testing.T) {
	for _, tc := range []struct {
		name, body, secret string
		offset             int64
		status             int
	}{
		{"challenge", `{"type":"url_verification","challenge":"challenge"}`, "test-signing-secret", 0, 200},
		{"invalid signature", `{"type":"url_verification","challenge":"challenge"}`, "wrong", 0, 401},
		{"stale", `{"type":"url_verification","challenge":"challenge"}`, "test-signing-secret", -301, 401},
		{"future", `{"type":"url_verification","challenge":"challenge"}`, "test-signing-secret", 301, 401},
		{"malformed", `{"type":`, "test-signing-secret", 0, 400},
		{"authenticate before JSON parsing", `{"type":`, "wrong", 0, 401},
		{"trailing JSON", `{"type":"url_verification","challenge":"challenge"}{}`, "test-signing-secret", 0, 400},
		{"missing challenge", `{"type":"url_verification"}`, "test-signing-secret", 0, 400},
		{"missing envelope fields", `{"type":"event_callback"}`, "test-signing-secret", 0, 400},
		{"wrong workspace", `{"event":{"type":"message"},"event_id":"EV1","team_id":"TOTHER","type":"event_callback"}`, "test-signing-secret", 0, 401},
		{"oversized", strings.Repeat("x", 1025), "test-signing-secret", 0, 413},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options, store, _, _ := setup(t)
			response := signed(t, New(options), tc.body, options.Now().Unix()+tc.offset, tc.secret)
			if response.Code != tc.status {
				t.Fatalf("status = %d, want %d", response.Code, tc.status)
			}
			if len(store.events) != 0 {
				t.Fatal("invalid envelope or challenge reached integrations")
			}
		})
	}
}

func TestEventRouting(t *testing.T) {
	for _, tc := range []struct {
		name, event, kind, command, thread string
		status, records                    int
	}{
		{"command", `{"channel":"CPRIVATE","text":"<@UBOT> untrack","thread_ts":"1700000000.000001","ts":"1700000001.000001","type":"message","user":"UUSER"}`, "command", "untrack", "1700000000.000001", 200, 1},
		{"malformed command", `{"channel":"CPRIVATE","text":"<@UBOT> untrack 456","ts":"1700000000.000001","type":"message","user":"UUSER"}`, "reply", "", "1700000000.000001", 200, 1},
		{"wrong account", `{"channel":"CPRIVATE","text":"<@UBOT> https://app.hubspot.com/contacts/999/ticket/456","ts":"1700000000.000001","type":"message","user":"UUSER"}`, "reply", "", "1700000000.000001", 200, 1},
		{"ordinary", `{"channel":"CPRIVATE","text":"reply","thread_ts":"1700000000.000001","ts":"1700000001.000001","type":"message","user":"UUSER"}`, "message", "", "1700000000.000001", 200, 1},
		{"edit controls only reconcile", `{"channel":"CPRIVATE","message":{"text":"<@UBOT> untrack","thread_ts":"1700000000.000001","ts":"1700000001.000001","user":"UUSER"},"subtype":"message_changed","type":"message"}`, "message", "", "1700000000.000001", 200, 1},
		{"deleted reply", `{"channel":"CPRIVATE","deleted_ts":"1700000001.000001","previous_message":{"text":"deleted","thread_ts":"1700000000.000001","ts":"1700000001.000001","user":"UUSER"},"subtype":"message_deleted","type":"message"}`, "message", "", "1700000000.000001", 200, 1},
		{"own bot", `{"channel":"CPRIVATE","text":"reply","ts":"1700000000.000001","type":"message","user":"UBOT"}`, "", "", "", 200, 0},
		{"own bot ID", `{"bot_id":"BBOT","channel":"CPRIVATE","text":"reply","ts":"1700000000.000001","type":"message"}`, "", "", "", 200, 0},
		{"system subtype", `{"channel":"CPRIVATE","subtype":"channel_join","ts":"1700000000.000001","type":"message","user":"UUSER"}`, "", "", "", 200, 0},
		{"public channel", `{"channel":"CPUBLIC","channel_type":"channel","text":"<@UBOT> 456","ts":"1700000000.000001","type":"message","user":"UUSER"}`, "link", "", "1700000000.000001", 200, 1},
		{"missing message timestamp", `{"channel":"CPRIVATE","type":"message","user":"UUSER"}`, "", "", "", 400, 0},
		{"invalid thread timestamp", `{"channel":"CPRIVATE","thread_ts":"bad","ts":"1700000000.000001","type":"message","user":"UUSER"}`, "", "", "", 400, 0},
		{"malformed deletion", `{"channel":"CPRIVATE","deleted_ts":"1700000000.000001","subtype":"message_deleted","type":"message"}`, "", "", "", 400, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options, store, _, _ := setup(t)
			body := `{"event":` + tc.event + `,"event_id":"EV1","team_id":"TTEAM","type":"event_callback"}`
			response := signed(t, New(options), body, options.Now().Unix(), options.SigningSecret)
			if response.Code != tc.status || len(store.events) != tc.records {
				t.Fatalf("status=%d records=%d", response.Code, len(store.events))
			}
			if tc.records > 0 {
				e := store.events[0]
				if e.Kind != tc.kind || e.Command != tc.command || e.Key.ThreadTS != tc.thread {
					t.Fatalf("event = %+v", e)
				}
			}
		})
	}
}

func TestLinkAcceptance(t *testing.T) {
	for _, tc := range []struct {
		name                               string
		lookupError, recordError           error
		existing                           bool
		wantStatus, wantCalls, wantRecords int
	}{
		{"lookup failure", errors.New("network"), nil, false, 503, 1, 0},
		{"new link", nil, nil, false, 200, 1, 1},
		{"existing mapping", errors.New("network"), nil, true, 200, 0, 1},
		{"commit failure", nil, errors.New("disk unavailable"), true, 503, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options, store, _, hubspot := setup(t)
			hubspot.err = tc.lookupError
			store.recordErr = tc.recordError
			if tc.existing {
				store.thread = &domain.Thread{Active: false, TicketID: "456", TicketURL: "https://example.test/ticket"}
			}
			handler := New(options)
			body := `{"event":{"channel":"CPRIVATE","text":"<@UBOT> 456","ts":"1700000000.000001","type":"message","user":"UUSER"},"event_id":"EV1","team_id":"TTEAM","type":"event_callback"}`

			response := signed(t, handler, body, options.Now().Unix(), options.SigningSecret)

			if response.Code != tc.wantStatus || hubspot.calls != tc.wantCalls || len(store.events) != tc.wantRecords {
				t.Fatalf("status=%d calls=%d records=%d", response.Code, hubspot.calls, len(store.events))
			}
			if tc.name == "new link" {
				event := store.events[0]
				if event.Kind != "link" || event.TicketID != "456" || event.Reply != "Ticket summary 456" {
					t.Errorf("link = %+v", event)
				}
			}
		})
	}
}

func TestReadinessFollowsStorageAndShutdown(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ready  bool
		err    error
		status int
	}{
		{"ready", true, nil, 200}, {"storage unavailable", true, errors.New("schema incompatible"), 503}, {"shutting down", false, nil, 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options, store, _, _ := setup(t)
			options.Ready = func() bool { return tc.ready }
			store.checkErr = tc.err
			handler := New(options)
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))

			if response.Code != tc.status {
				t.Fatalf("readiness = %d, want %d", response.Code, tc.status)
			}
		})
	}
}

// Exercise generated schema validation and HTTP error mapping at the public boundary.
func TestOgenWebhookValidation(t *testing.T) {
	validHubspot := `[{"portalId":123,"objectId":456,"occurredAt":1700000000000,"subscriptionType":"ticket.creation","futureField":true}]`
	for _, tc := range []struct {
		name, body, contentType string
		status                  int
	}{
		{"valid batch with unknown fields", validHubspot, "application/json; charset=utf-8", 501},
		{"empty batch", `[]`, "application/json", 501},
		{"null batch", `null`, "application/json", 400},
		{"null item", `[null]`, "application/json", 400},
		{"missing required properties", `[{}]`, "application/json", 400},
		{"wrong property type", `[{"portalId":"123","objectId":456,"occurredAt":0,"subscriptionType":"ticket.creation"}]`, "application/json", 400},
		{"trailing JSON", validHubspot + `[]`, "application/json", 400},
		{"missing media type", validHubspot, "", 415},
		{"malformed media type", validHubspot, "application/json; broken", 415},
		{"unsupported media type", validHubspot, "text/plain", 415},
		{"oversized", strings.Repeat(" ", 1025) + validHubspot, "application/json", 413},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options, _, _, _ := setup(t)
			request := httptest.NewRequest(http.MethodPost, "/hubspot/events", strings.NewReader(tc.body))
			request.Header.Set("Content-Type", tc.contentType)
			response := httptest.NewRecorder()
			New(options).ServeHTTP(response, request)
			if response.Code != tc.status {
				t.Fatalf("status = %d, want %d", response.Code, tc.status)
			}
		})
	}
}

func TestSlackGeneratedValidation(t *testing.T) {
	for _, tc := range []struct {
		name, body, contentType string
		status                  int
	}{
		{"JSON parameters", `{"type":"url_verification","challenge":"ok"}`, "application/json; charset=utf-8", 200},
		{"null envelope", `null`, "application/json", 400},
		{"missing required type", `{"challenge":"ok"}`, "application/json", 400},
		{"missing media type", `{"type":"url_verification","challenge":"ok"}`, "", 415},
		{"malformed media type", `{"type":"url_verification","challenge":"ok"}`, "application/json; broken", 415},
		{"unsupported media type", `{"type":"url_verification","challenge":"ok"}`, "text/plain", 415},
	} {
		t.Run(tc.name, func(t *testing.T) {
			options, _, _, _ := setup(t)
			handler := New(options)
			wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				r.Header.Set("Content-Type", tc.contentType)
				handler.ServeHTTP(w, r)
			})
			response := signed(t, wrapped, tc.body, options.Now().Unix(), options.SigningSecret)
			if response.Code != tc.status {
				t.Fatalf("status = %d, want %d", response.Code, tc.status)
			}
		})
	}
}
