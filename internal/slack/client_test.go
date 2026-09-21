package slack

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"slackhubspot/internal/domain"
)

func TestThreadPaginatesAndResolvesIdentity(t *testing.T) {
	pages, users := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("unexpected method %s", r.Method)
		}
		if r.URL.Path == "/conversations.replies" {
			if r.Header.Get("Authorization") != "Bearer history-secret" {
				t.Error("history token not used")
			}
			pages++
			if r.URL.Query().Get("cursor") == "" {
				io.WriteString(w, `{"ok":true,"has_more":true,"messages":[{"ts":"2.000001","user":"U2","text":"Thanks <@U1>"}],"response_metadata":{"next_cursor":"next"}}`)
			} else {
				io.WriteString(w, `{"ok":true,"messages":[{"ts":"1.000001","user":"U1","text":"Root","files":[{"id":"F1","name":"x.png","url_private":"https://files.slack.com/x"}]}]}`)
			}
			return
		}
		if r.Header.Get("Authorization") != "Bearer bot-secret" {
			t.Error("bot token not used")
		}
		switch r.URL.Path {
		case "/users.info":
			users++
			json.NewEncoder(w).Encode(map[string]any{"ok": true, "user": map[string]any{"profile": map[string]string{"display_name": "Name " + r.URL.Query().Get("user")}}})
		case "/chat.getPermalink":
			io.WriteString(w, `{"ok":true,"permalink":"https://team.slack.com/archives/C/p1"}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client, err := New(Config{BaseURL: server.URL, BotToken: "bot-secret", HTTPClient: server.Client(), HistoryToken: "history-secret"})
	if err != nil {
		t.Fatal(err)
	}
	messages, err := client.Thread(context.Background(), domain.ThreadKey{ChannelID: "C", ThreadTS: "1.000001"})
	if err != nil {
		t.Fatal(err)
	}
	if pages != 2 || users != 2 || len(messages) != 2 || messages[0].Timestamp != "1.000001" || messages[1].Text != "Thanks <@U1|Name U1>" || messages[0].Files[0].URL != "https://files.slack.com/x" {
		t.Fatalf("unexpected thread: %#v (pages %d users %d)", messages, pages, users)
	}
}

func TestRateLimitError(t *testing.T) {
	client := newTestSlackClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "17")
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, `{"error":"secret personal data"}`)
	})

	_, err := client.File(context.Background(), "F1")

	var remote *domain.RemoteError
	if !errors.As(err, &remote) || !remote.Temporary || remote.RetryAfter != 17*time.Second || strings.Contains(err.Error(), "personal") {
		t.Fatalf("bad error: %v", err)
	}
}

func TestDownloadRejectsUnsafeURL(t *testing.T) {
	for _, target := range []string{"http://files.slack.com/private", "https://slack.com.evil.test/private", "https://user:pass@files.slack.com/private"} {
		t.Run(target, func(t *testing.T) {
			client := newTestSlackClient(t, func(w http.ResponseWriter, r *http.Request) {
				t.Error("unsafe download reached HTTP")
				w.WriteHeader(500)
			})

			body, err := client.Download(context.Background(), domain.File{URL: target})

			if body != nil {
				body.Close()
			}
			if err == nil {
				t.Fatal("unsafe URL accepted")
			}
		})
	}
}

func TestNewRejectsPlaintextRemoteAPI(t *testing.T) {
	_, err := New(Config{BaseURL: "http://slack.com/api", BotToken: "secret"})

	if err == nil {
		t.Fatal("plaintext remote API accepted")
	}
}

func TestVerifyIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, workspace, user string
		rejected              bool
	}{
		{"matching", "T1", "U1", false}, {"wrong workspace", "T2", "U1", true}, {"wrong user", "T1", "U2", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := make(chan string, 2)
			client := newTestSlackClient(t, func(w http.ResponseWriter, r *http.Request) {
				requests <- r.URL.Path
				io.WriteString(w, `{"ok":true,"bot_id":"B1","team_id":"T1","user_id":"U1"}`)
			})

			err := client.VerifyIdentity(context.Background(), tc.workspace, tc.user)

			if (err != nil) != tc.rejected {
				t.Fatalf("verification = %v, rejected=%v", err, tc.rejected)
			}
			wantRequests := 2
			if tc.rejected {
				wantRequests = 1
			}
			if len(requests) != wantRequests {
				t.Fatalf("requests = %d, want %d", len(requests), wantRequests)
			}
			for len(requests) > 0 {
				if path := <-requests; path != "/auth.test" {
					t.Errorf("path = %q", path)
				}
			}
			if !tc.rejected && client.BotID() != "B1" {
				t.Errorf("bot ID = %q, want B1", client.BotID())
			}
		})
	}
}

func TestReactionAddRequest(t *testing.T) {
	requests := make(chan map[string]string, 2)
	client := newTestSlackClient(t, func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		payload["method"], payload["path"], payload["authorization"] = r.Method, r.URL.Path, r.Header.Get("Authorization")
		requests <- payload
		io.WriteString(w, `{"ok":true}`)
	})
	want := map[string]string{"method": "POST", "path": "/reactions.add", "authorization": "Bearer bot-secret", "channel": "C1", "timestamp": "1.000001", "name": "white_check_mark"}

	err := client.React(context.Background(), domain.ThreadKey{ChannelID: "C1"}, "1.000001", "white_check_mark")

	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(requests))
	}
	if got := <-requests; !reflect.DeepEqual(got, want) {
		t.Errorf("request = %v, want %v", got, want)
	}
}

func TestReactionAlreadyPresentIsSuccessful(t *testing.T) {
	requests := make(chan string, 2)
	client := newTestSlackClient(t, func(w http.ResponseWriter, r *http.Request) {
		requests <- r.URL.Path
		io.WriteString(w, `{"ok":false,"error":"already_reacted"}`)
	})

	err := client.React(context.Background(), domain.ThreadKey{ChannelID: "C1"}, "1.000001", "white_check_mark")

	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(requests))
	}
	if path := <-requests; path != "/reactions.add" {
		t.Errorf("path = %q", path)
	}
}

func TestRemoveReactionRequest(t *testing.T) {
	requests := make(chan map[string]string, 1)
	client := newTestSlackClient(t, func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]string
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
			return
		}
		payload["method"], payload["path"], payload["authorization"] = r.Method, r.URL.Path, r.Header.Get("Authorization")
		requests <- payload
		io.WriteString(w, `{"ok":true}`)
	})
	want := map[string]string{"method": "POST", "path": "/reactions.remove", "authorization": "Bearer bot-secret", "channel": "C1", "timestamp": "1.000001", "name": "white_check_mark"}

	err := client.RemoveReaction(context.Background(), domain.ThreadKey{ChannelID: "C1"}, "1.000001", "white_check_mark")

	if err != nil {
		t.Fatal(err)
	}
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(requests))
	}
	if got := <-requests; !reflect.DeepEqual(got, want) {
		t.Errorf("request = %v, want %v", got, want)
	}
}

func TestRemoveReactionResponses(t *testing.T) {
	for _, tc := range []struct {
		name, response, retryAfter string
		want                       *domain.RemoteError
	}{
		{name: "removed", response: `{"ok":true}`},
		{name: "already absent", response: `{"ok":false,"error":"no_reaction"}`},
		{name: "message deleted", response: `{"ok":false,"error":"message_not_found"}`},
		{name: "missing permission", response: `{"ok":false,"error":"missing_scope"}`, want: &domain.RemoteError{Service: "slack", Code: "missing_scope"}},
		{name: "rate limited", retryAfter: "17", response: `{"ok":false,"error":"ratelimited"}`, want: &domain.RemoteError{Service: "slack", Code: "ratelimited", Temporary: true, RetryAfter: 17 * time.Second}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newTestSlackClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Retry-After", tc.retryAfter)
				io.WriteString(w, tc.response)
			})

			err := client.RemoveReaction(context.Background(), domain.ThreadKey{ChannelID: "C1"}, "1.000001", "white_check_mark")

			if tc.want == nil {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var got *domain.RemoteError
			if !errors.As(err, &got) {
				t.Fatalf("error = %v, want RemoteError", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("error = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func newTestSlackClient(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := New(Config{BaseURL: server.URL, BotToken: "bot-secret", HistoryToken: "history-secret", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestPostUsesStableUUID(t *testing.T) {
	requests := make(chan map[string]any, 3)
	client := newTestSlackClient(t, func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		requests <- payload
		io.WriteString(w, `{"ok":true}`)
	})
	key := domain.ThreadKey{ChannelID: "C1", ThreadTS: "1.0"}

	for range 2 {
		if err := client.Post(context.Background(), key, "message", "durable-event-key"); err != nil {
			t.Fatal(err)
		}
	}

	if len(requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(requests))
	}
	first, second := <-requests, <-requests
	id, _ := first["client_msg_id"].(string)
	if len(id) != 36 || second["client_msg_id"] != id || first["thread_ts"] != "1.0" || first["unfurl_links"] != false {
		t.Errorf("post payloads = %v, %v", first, second)
	}
}

func TestDownloadRejectsUntrustedRedirect(t *testing.T) {
	requests := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.URL.Path
		http.Redirect(w, r, "https://attacker.invalid/private", http.StatusFound)
	}))
	t.Cleanup(server.Close)
	client, err := New(Config{BaseURL: server.URL, BotToken: "secret", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}

	body, err := client.Download(context.Background(), domain.File{URL: server.URL + "/file"})

	if body != nil {
		body.Close()
	}
	if err == nil {
		t.Fatal("untrusted redirect accepted")
	}
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(requests))
	}
}

func TestDownloadPreservesAuthorizationAcrossTrustedRedirect(t *testing.T) {
	requests := make(chan string, 3)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.URL.Path + " " + r.Header.Get("Authorization")
		if r.URL.Path == "/allowed" {
			http.Redirect(w, r, "/done", http.StatusFound)
			return
		}
		io.WriteString(w, "payload")
	}))
	t.Cleanup(server.Close)
	client, err := New(Config{BaseURL: server.URL, BotToken: "secret", HTTPClient: server.Client()})
	if err != nil {
		t.Fatal(err)
	}

	body, err := client.Download(context.Background(), domain.File{URL: server.URL + "/allowed"})

	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	payload, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != "payload" {
		t.Errorf("payload = %q", payload)
	}
	if len(requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(requests))
	}
	if first, second := <-requests, <-requests; first != "/allowed Bearer secret" || second != "/done Bearer secret" {
		t.Errorf("requests = %q, %q", first, second)
	}
}

func TestRateLimitedReplyPageRetainsCursor(t *testing.T) {
	var cursors []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/conversations.replies":
			cursors = append(cursors, r.URL.Query().Get("cursor"))
			if len(cursors) == 1 {
				io.WriteString(w, `{"ok":true,"messages":[{"ts":"1.000001","user":"U1","text":"Root","reactions":[{"name":"white_check_mark","users":["UBOT"]},{"name":"eyes","users":["UOTHER"]},{"name":"heart","users":["UBOT","UOTHER"]}]}],"response_metadata":{"next_cursor":"page-two"}}`)
				return
			}
			if len(cursors) == 2 {
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			io.WriteString(w, `{"ok":true,"messages":[{"ts":"2.000001","user":"U1","text":"Reply"}]}`)
		case "/users.info":
			io.WriteString(w, `{"ok":true,"user":{"profile":{"display_name":"Ada"}}}`)
		case "/chat.getPermalink":
			io.WriteString(w, `{"ok":true,"permalink":"https://team.slack.com/archives/C/p1"}`)
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client, _ := New(Config{BaseURL: server.URL, BotToken: "secret", HTTPClient: server.Client()})
	client.botUserID = "UBOT"
	started := time.Now()
	messages, err := client.Thread(context.Background(), domain.ThreadKey{ChannelID: "C", ThreadTS: "1.000001"})
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(started) < time.Second {
		t.Fatal("Retry-After was not honored")
	}
	if !slices.Equal(cursors, []string{"", "page-two", "page-two"}) {
		t.Fatalf("pagination restarted: %#v", cursors)
	}
	if len(messages) != 2 {
		t.Fatalf("messages = %d, want 2", len(messages))
	}
}

func TestReplyRateLimitWaitCancelsPromptly(t *testing.T) {
	limited := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "60")
		w.WriteHeader(http.StatusTooManyRequests)
		close(limited)
	}))
	defer server.Close()
	client, _ := New(Config{BaseURL: server.URL, BotToken: "secret", HTTPClient: server.Client()})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := client.Thread(ctx, domain.ThreadKey{ChannelID: "C", ThreadTS: "1.000001"})
		done <- err
	}()
	<-limited
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected cancellation error %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("rate-limit wait ignored cancellation")
	}
}

func TestPostIncludesBlocksAndFallback(t *testing.T) {
	for _, tc := range []struct {
		name   string
		blocks []domain.SlackBlock
	}{
		{name: "text only"},
		{name: "structured message", blocks: []domain.SlackBlock{{Type: "section", Text: &domain.SlackText{Type: "mrkdwn", Text: "*Help*"}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			type message struct {
				Text   string              `json:"text"`
				Blocks []domain.SlackBlock `json:"blocks"`
			}
			requests := make(chan message, 1)
			client := newTestSlackClient(t, func(w http.ResponseWriter, r *http.Request) {
				var payload message
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Error(err)
					return
				}
				requests <- payload
				io.WriteString(w, `{"ok":true}`)
			})

			err := client.Post(context.Background(), domain.ThreadKey{ChannelID: "C1", ThreadTS: "1.0"}, "Accessible help", "help-event", tc.blocks...)

			if err != nil {
				t.Fatal(err)
			}
			if len(requests) != 1 {
				t.Fatalf("requests = %d, want 1", len(requests))
			}
			got := <-requests
			if got.Text != "Accessible help" {
				t.Errorf("fallback = %q", got.Text)
			}
			if !reflect.DeepEqual(got.Blocks, tc.blocks) {
				t.Errorf("blocks = %+v, want %+v", got.Blocks, tc.blocks)
			}
		})
	}
}

func TestThreadIdentifiesOnlyOwnReactions(t *testing.T) {
	client := newTestSlackClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/conversations.replies":
			io.WriteString(w, `{"ok":true,"messages":[{"ts":"1.000001","user":"U1","text":"Root","reactions":[{"name":"white_check_mark","users":["UBOT"]},{"name":"eyes","users":["UOTHER"]},{"name":"heart","users":["UBOT","UOTHER"]}]},{"ts":"2.000001","user":"U1","text":"Reply"}]}`)
		case "/users.info":
			io.WriteString(w, `{"ok":true,"user":{"profile":{"display_name":"Ada"}}}`)
		case "/chat.getPermalink":
			io.WriteString(w, `{"ok":true,"permalink":"https://team.slack.com/archives/C/p1"}`)
		default:
			http.NotFound(w, r)
		}
	})
	client.botUserID = "UBOT"

	messages, err := client.Thread(context.Background(), domain.ThreadKey{ChannelID: "C", ThreadTS: "1.000001"})

	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 2 {
		t.Fatalf("messages = %d, want 2", len(messages))
	}
	if !slices.Equal(messages[0].BotReactions, []string{"heart", "white_check_mark"}) || len(messages[1].BotReactions) != 0 {
		t.Fatalf("incorrect own reactions: %+v", messages)
	}
}
