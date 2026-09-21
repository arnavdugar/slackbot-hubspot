package hubspot

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"slackhubspot/internal/domain"
)

func TestNotesUseTicketAssociationsAndReplaceManagedBody(t *testing.T) {
	marker := "slack-thread:T:C:1.0"
	pages := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Error("missing authentication")
		}
		switch r.URL.Path {
		case "/crm/v3/objects/tickets/123/associations/notes":
			pages++
			if r.URL.Query().Get("after") == "" {
				io.WriteString(w, `{"results":[{"id":"10"}],"paging":{"next":{"after":"next"}}}`)
			} else {
				io.WriteString(w, `{"results":[{"id":"11"}]}`)
			}
		case "/crm/v3/objects/notes/10":
			io.WriteString(w, `{"id":"10","properties":{"hs_note_body":"unrelated manual note"}}`)
		case "/crm/v3/objects/notes/11":
			if r.Method == http.MethodPatch {
				var payload struct {
					Properties map[string]string `json:"properties"`
				}
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Error(err)
				}
				if payload.Properties["hs_attachment_ids"] != "F1;F2" || payload.Properties["hs_note_body"] != "replacement" {
					t.Errorf("unexpected update %#v", payload)
				}
				w.WriteHeader(200)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"id": "11", "properties": map[string]string{"hs_note_body": "<p>Source: " + marker + "</p>old content"}})
		case "/crm/v3/objects/notes":
			var payload struct {
				Associations []struct {
					To struct {
						ID string `json:"id"`
					} `json:"to"`
					Types []struct {
						AssociationTypeID int `json:"associationTypeId"`
					} `json:"types"`
				} `json:"associations"`
				Properties map[string]string `json:"properties"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Error(err)
			}
			if len(payload.Associations) != 1 || payload.Associations[0].To.ID != "123" || payload.Associations[0].Types[0].AssociationTypeID != 228 || !strings.Contains(payload.Properties["hs_note_body"], "Source: "+marker) || payload.Properties["hs_timestamp"] == "" {
				t.Errorf("bad note payload %#v", payload)
			}
			io.WriteString(w, `{"id":"12"}`)
		default:
			t.Errorf("unexpected endpoint %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	client, _ := New(Config{AccountID: "42", BaseURL: server.URL, HTTPClient: server.Client(), Token: "secret"})
	id, err := client.FindNote(context.Background(), "123", marker)
	if err != nil || id != "11" || pages != 2 {
		t.Fatalf("find note %q %v pages=%d", id, err, pages)
	}
	if err := client.UpdateNote(context.Background(), id, "replacement", []string{"F1", "F2"}); err != nil {
		t.Fatal(err)
	}
	if id, err := client.CreateNote(context.Background(), "123", marker, "body", nil); err != nil || id != "12" {
		t.Fatalf("create note %q %v", id, err)
	}
}

func TestPrivateFileUploadAndStableRecovery(t *testing.T) {
	marker := "slack-file:T:F1"
	name := fileName(marker)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Error("missing authentication")
		}
		switch r.URL.Path {
		case "/files/v3/files":
			if err := r.ParseMultipartForm(1 << 20); err != nil {
				t.Error(err)
				return
			}
			defer r.MultipartForm.RemoveAll()
			var options map[string]any
			if err := json.Unmarshal([]byte(r.FormValue("options")), &options); err != nil {
				t.Error(err)
			}
			if options["access"] != "PRIVATE" || options["duplicateValidationStrategy"] != "RETURN_EXISTING" || options["duplicateValidationScope"] != "EXACT_FOLDER" || r.FormValue("fileName") != name+".pdf" || r.FormValue("folderPath") != "/slack-hubspot" {
				t.Errorf("bad upload options %#v", options)
			}
			file, header, err := r.FormFile("file")
			if err != nil {
				t.Error(err)
				return
			}
			defer file.Close()
			body, _ := io.ReadAll(file)
			if string(body) != "file payload" || header.Header.Get("Content-Type") != "application/pdf" {
				t.Error("file stream did not match")
			}
			io.WriteString(w, `{"id":"888"}`)
		case "/files/v3/files/search":
			if r.URL.Query().Get("name") != name {
				t.Error("unstable source name")
			}
			json.NewEncoder(w).Encode(map[string]any{"results": []any{map[string]string{"id": "777", "name": name, "path": "/unrelated/" + name + ".pdf"}, map[string]string{"id": "888", "name": name, "path": "/slack-hubspot/" + name + ".pdf"}}})
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()
	client, _ := New(Config{AccountID: "42", BaseURL: server.URL, HTTPClient: server.Client(), Token: "secret"})
	id, err := client.Upload(context.Background(), marker, domain.File{ID: "F1", MIME: "application/pdf", Name: "private customer.pdf"}, strings.NewReader("file payload"))
	if err != nil || id != "888" {
		t.Fatalf("upload %q %v", id, err)
	}
	id, err = client.FindFile(context.Background(), marker)
	if err != nil || id != "888" {
		t.Fatalf("recovery %q %v", id, err)
	}
}

func TestAccountAndRateLimitErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/account-info/v3/details" {
			io.WriteString(w, `{"portalId":42}`)
			return
		}
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(429)
		io.WriteString(w, `{"message":"secret payload"}`)
	}))
	defer server.Close()
	client, _ := New(Config{AccountID: "99", BaseURL: server.URL, HTTPClient: server.Client(), Token: "secret"})
	if err := client.VerifyAccount(context.Background()); err == nil {
		t.Fatal("accepted wrong account")
	}
	client.accountID = "42"
	if err := client.VerifyAccount(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := client.Ticket(context.Background(), "123")
	var remote *domain.RemoteError
	if !errors.As(err, &remote) || !remote.Temporary || remote.RetryAfter != 7*time.Second || strings.Contains(err.Error(), "payload") {
		t.Fatalf("bad rate limit error %v", err)
	}
}
