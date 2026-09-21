package slack

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
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

func TestRateLimitAndCredentialBoundaries(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "17")
		w.WriteHeader(429)
		io.WriteString(w, `{"error":"secret personal data"}`)
	}))
	defer server.Close()
	client, _ := New(Config{BaseURL: server.URL, BotToken: "secret", HTTPClient: server.Client()})
	_, err := client.File(context.Background(), "F1")
	var remote *domain.RemoteError
	if !errors.As(err, &remote) || !remote.Temporary || remote.RetryAfter != 17*time.Second || strings.Contains(err.Error(), "personal") {
		t.Fatalf("bad error: %v", err)
	}
	for _, target := range []string{"http://files.slack.com/private", "https://slack.com.evil.test/private", "https://user:pass@files.slack.com/private"} {
		if body, err := client.Download(context.Background(), domain.File{URL: target}); err == nil {
			body.Close()
			t.Errorf("accepted unsafe URL %s", target)
		}
	}
	if _, err := New(Config{BaseURL: "http://slack.com/api", BotToken: "secret"}); err == nil {
		t.Fatal("accepted plaintext remote API")
	}
}

func TestIdentityAndIdempotentReaction(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/auth.test" {
			io.WriteString(w, `{"ok":true,"bot_id":"B1","team_id":"T1","user_id":"U1"}`)
		} else {
			io.WriteString(w, `{"ok":false,"error":"already_reacted"}`)
		}
	}))
	defer server.Close()
	client, _ := New(Config{BaseURL: server.URL, BotToken: "secret", HTTPClient: server.Client()})
	if err := client.VerifyIdentity(context.Background(), "T2", "U1"); err == nil {
		t.Fatal("accepted wrong workspace")
	}
	if err := client.VerifyIdentity(context.Background(), "T1", "U1"); err != nil {
		t.Fatal(err)
	}
	if client.BotID() != "B1" {
		t.Fatal("bot ID not recorded")
	}
	if err := client.React(context.Background(), domain.ThreadKey{ChannelID: "C"}, "1.0", "white_check_mark"); err != nil {
		t.Fatal(err)
	}
}

func TestPostUsesStableUUIDAndDownloadRejectsUntrustedRedirects(t *testing.T) {
	var ids []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/chat.postMessage" {
			var payload map[string]any
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Error(err)
			}
			id, _ := payload["client_msg_id"].(string)
			ids = append(ids, id)
			if len(id) != 36 || payload["thread_ts"] != "1.0" || payload["unfurl_links"] != false {
				t.Errorf("unexpected post %#v", payload)
			}
			io.WriteString(w, `{"ok":true}`)
			return
		}
		if r.URL.Path == "/allowed" {
			http.Redirect(w, r, "/done", http.StatusFound)
			return
		}
		if r.URL.Path == "/done" {
			if r.Header.Get("Authorization") != "Bearer secret" {
				t.Error("lost file authorization")
			}
			io.WriteString(w, "payload")
			return
		}
		http.Redirect(w, r, "https://attacker.invalid/private", http.StatusFound)
	}))
	defer server.Close()
	client, _ := New(Config{BaseURL: server.URL, BotToken: "secret", HTTPClient: server.Client()})
	for range 2 {
		if err := client.Post(context.Background(), domain.ThreadKey{ChannelID: "C1", ThreadTS: "1.0"}, "message", "durable-event-key"); err != nil {
			t.Fatal(err)
		}
	}
	if ids[0] != ids[1] {
		t.Fatal("idempotency key changed")
	}
	if body, err := client.Download(context.Background(), domain.File{URL: server.URL + "/file"}); err == nil {
		body.Close()
		t.Fatal("unexpected successful redirected download")
	}
	body, err := client.Download(context.Background(), domain.File{URL: server.URL + "/allowed"})
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	payload, _ := io.ReadAll(body)
	if string(payload) != "payload" {
		t.Fatal("trusted redirect did not download")
	}
}

func TestPrivateMemberCacheDoesNotCacheDenials(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		json.NewEncoder(w).Encode(map[string]any{"ok": true, "channel": map[string]bool{"is_member": calls > 1, "is_private": true}})
	}))
	defer server.Close()
	client, _ := New(Config{BaseURL: server.URL, BotToken: "secret", HTTPClient: server.Client()})
	for _, want := range []bool{false, true, true} {
		if got, err := client.IsPrivateMember(context.Background(), "C1"); err != nil || got != want {
			t.Fatalf("membership %v %v, want %v", got, err, want)
		}
	}
	if calls != 2 {
		t.Fatalf("expected denial recheck and positive cache; calls=%d", calls)
	}
}

func TestRateLimitedReplyPageRetainsCursorAndOwnReactions(t *testing.T) {
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
	if !slices.Equal(messages[0].BotReactions, []string{"heart", "white_check_mark"}) || len(messages[1].BotReactions) != 0 {
		t.Fatalf("incorrect own reaction detection %#v", messages)
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
