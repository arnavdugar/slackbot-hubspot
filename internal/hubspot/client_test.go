package hubspot

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"slackhubspot/internal/domain"
)

const fileMarker = "slack-file-T-F1"
const stableFileName = "slack-c8df7c49f87c8a67d818ca52bea4b82f4a4716f725bdf3be5c8b9dc640669cee"

type recordedRequest struct {
	Method, Path, Authorization string
	Query                       url.Values
	Body                        []byte
}

func testClient(t *testing.T, handler http.HandlerFunc, fields ...string) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	client, err := New(Config{AccountID: "42", BaseURL: server.URL, HTTPClient: server.Client(), Token: "secret", SummaryFields: fields})
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func captureRequest(t *testing.T, r *http.Request) recordedRequest {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.Error(err)
	}
	return recordedRequest{r.Method, r.URL.Path, r.Header.Get("Authorization"), r.URL.Query(), body}
}

func onlyRequest(t *testing.T, requests chan recordedRequest) recordedRequest {
	t.Helper()
	if len(requests) != 1 {
		t.Fatalf("requests = %d, want 1", len(requests))
	}
	return <-requests
}

func assertRemoteError(t *testing.T, err error, code string, temporary bool) {
	t.Helper()
	var remote *domain.RemoteError
	if !errors.As(err, &remote) || remote.Code != code || remote.Service != "hubspot" || remote.Temporary != temporary {
		t.Fatalf("error = %v, want hubspot %s (temporary=%v)", err, code, temporary)
	}
}

func TestTicketDecodesProperties(t *testing.T) {
	for _, tc := range []struct{ name, timestamps, created, updated string }{
		{"object timestamps", `"createdAt":"created","updatedAt":"updated",`, "created", "updated"},
		{"property timestamp fallback", "", "property-created", "property-updated"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := make(chan recordedRequest, 2)
			client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				requests <- captureRequest(t, r)
				io.WriteString(w, `{`+tc.timestamps+`"id":"123","properties":{"subject":"Customer request","hs_pipeline":"pipeline","hs_pipeline_stage":"stage","hs_ticket_priority":"HIGH","hubspot_owner_id":"owner","createdate":"property-created","hs_lastmodifieddate":"property-updated"}}`)
			})
			want := domain.Ticket{ID: "123", Subject: "Customer request", Pipeline: "pipeline", Status: "stage", Priority: "HIGH", Owner: "owner", CreatedAt: tc.created, UpdatedAt: tc.updated, URL: "https://app.hubspot.com/contacts/42/record/0-5/123"}

			got, err := client.Ticket(context.Background(), "123")

			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("ticket = %+v, want %+v", got, want)
			}
			request := onlyRequest(t, requests)
			if request.Method != "GET" || request.Path != "/crm/v3/objects/tickets/123" || request.Authorization != "Bearer secret" || request.Query.Get("associations") != "" {
				t.Errorf("request = %+v", request)
			}
			if request.Query.Get("properties") != "createdate,hs_lastmodifieddate,hs_pipeline,hs_pipeline_stage,hs_ticket_priority,hubspot_owner_id,subject" {
				t.Errorf("requested properties = %q", request.Query.Get("properties"))
			}
		})
	}
}

func TestTicketEnrichesSelectedField(t *testing.T) {
	for _, tc := range []struct{ field, path, response, association, properties, want string }{
		{"Pipeline", "/crm/v3/pipelines/tickets/pipeline", `{"label":"Support","stages":[{"id":"stage","label":"Open"}]}`, "", "", "Support"},
		{"Status", "/crm/v3/pipelines/tickets/pipeline", `{"label":"Support","stages":[{"id":"stage","label":"Open"}]}`, "", "", "Open"},
		{"Owner", "/crm/v3/owners/owner", `{"firstName":"Ada","lastName":"Lovelace"}`, "", "", "Ada Lovelace"},
		{"Company", "/crm/v3/objects/companies/10", `{"properties":{"name":"Example Ltd"}}`, "companies", "name", "Example Ltd"},
		{"Contact", "/crm/v3/objects/contacts/10", `{"properties":{"firstname":"Grace","lastname":"Hopper"}}`, "contacts", "firstname,lastname", "Grace Hopper"},
	} {
		t.Run(tc.field, func(t *testing.T) {
			requests := make(chan recordedRequest, 4)
			client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				requests <- captureRequest(t, r)
				switch r.URL.Path {
				case "/crm/v3/objects/tickets/123":
					io.WriteString(w, `{"id":"123","properties":{"hs_pipeline":"pipeline","hs_pipeline_stage":"stage","hubspot_owner_id":"owner"},"associations":{"companies":{"results":[{"id":"20"},{"id":"10"}]},"contacts":{"results":[{"id":"20"},{"id":"10"}]}}}`)
				case tc.path:
					io.WriteString(w, tc.response)
				default:
					http.NotFound(w, r)
				}
			}, tc.field)

			got, err := client.Ticket(context.Background(), "123")

			if err != nil {
				t.Fatal(err)
			}
			values := map[string]string{"Pipeline": got.Pipeline, "Status": got.Status, "Owner": got.Owner, "Company": got.Company, "Contact": got.Contact}
			if values[tc.field] != tc.want {
				t.Errorf("%s = %q, want %q", tc.field, values[tc.field], tc.want)
			}
			if len(requests) != 2 {
				t.Fatalf("requests = %d, want 2", len(requests))
			}
			first, second := <-requests, <-requests
			if first.Query.Get("associations") != tc.association || second.Path != tc.path || second.Query.Get("properties") != tc.properties {
				t.Errorf("enrichment requests = %+v, %+v", first, second)
			}
		})
	}
}

func TestTicketRejectsInvalidIdentity(t *testing.T) {
	for _, response := range []string{`{"properties":{}}`, `{"id":"999"}`} {
		t.Run(response, func(t *testing.T) {
			client := testClient(t, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, response) })

			_, err := client.Ticket(context.Background(), "123")

			assertRemoteError(t, err, "invalid_response", false)
		})
	}
}

func TestFindNotePaginatesTicketAssociations(t *testing.T) {
	requests := make(chan recordedRequest, 8)
	client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		requests <- captureRequest(t, r)
		switch r.URL.Path {
		case "/crm/v3/objects/tickets/123/associations/notes":
			if r.URL.Query().Get("after") == "" {
				io.WriteString(w, `{"results":[{"id":"10"}],"paging":{"next":{"after":"next"}}}`)
			} else {
				io.WriteString(w, `{"results":[{"id":"11"}]}`)
			}
		case "/crm/v3/objects/notes/10":
			io.WriteString(w, `{"id":"10","properties":{"hs_note_body":"manual note"}}`)
		case "/crm/v3/objects/notes/11":
			io.WriteString(w, `{"id":"11","properties":{"hs_note_body":"<p>Source: slack-thread:T:C:1.0</p>body"}}`)
		default:
			http.NotFound(w, r)
		}
	})

	id, err := client.FindNote(context.Background(), "123", "slack-thread:T:C:1.0")

	if err != nil || id != "11" {
		t.Fatalf("note = %q, %v", id, err)
	}
	if len(requests) != 4 {
		t.Fatalf("requests = %d, want 4", len(requests))
	}
	var cursors []string
	for len(requests) > 0 {
		request := <-requests
		if request.Authorization != "Bearer secret" || request.Method != "GET" {
			t.Errorf("request = %+v", request)
		}
		if strings.HasSuffix(request.Path, "/associations/notes") {
			cursors = append(cursors, request.Query.Get("after"))
		}
	}
	if !slices.Equal(cursors, []string{"", "next"}) {
		t.Errorf("cursors = %v", cursors)
	}
}

func TestFindNoteReturnsMissing(t *testing.T) {
	requests := make(chan recordedRequest, 2)
	client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		requests <- captureRequest(t, r)
		io.WriteString(w, `{"results":[]}`)
	})

	id, err := client.FindNote(context.Background(), "123", "slack-thread:T:C:1.0")

	if err != nil || id != "" {
		t.Fatalf("missing note = %q, %v", id, err)
	}
	request := onlyRequest(t, requests)
	if request.Path != "/crm/v3/objects/tickets/123/associations/notes" {
		t.Errorf("request = %+v", request)
	}
}

func TestFindNoteRejectsRepeatedCursor(t *testing.T) {
	requests := make(chan recordedRequest, 4)
	client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		requests <- captureRequest(t, r)
		if len(requests) > 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		io.WriteString(w, `{"results":[],"paging":{"next":{"after":"same"}}}`)
	})

	_, err := client.FindNote(context.Background(), "123", "slack-thread:T:C:1.0")

	assertRemoteError(t, err, "repeated_cursor", true)
	if len(requests) != 2 {
		t.Errorf("requests = %d, want 2", len(requests))
	}
}

type notePayload struct {
	Associations []struct {
		To struct {
			ID string `json:"id"`
		} `json:"to"`
		Types []struct {
			Category string `json:"associationCategory"`
			ID       int    `json:"associationTypeId"`
		} `json:"types"`
	} `json:"associations"`
	Properties map[string]string `json:"properties"`
}

func TestCreateNoteAssociatesMarkedBodyAndAttachments(t *testing.T) {
	requests := make(chan recordedRequest, 2)
	client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		requests <- captureRequest(t, r)
		io.WriteString(w, `{"id":"12"}`)
	})

	id, err := client.CreateNote(context.Background(), "123", "slack-thread:T:C:1.0", "body", []string{"F1", "F2"})

	if err != nil || id != "12" {
		t.Fatalf("note = %q, %v", id, err)
	}
	request := onlyRequest(t, requests)
	if request.Method != "POST" || request.Path != "/crm/v3/objects/notes" || request.Authorization != "Bearer secret" {
		t.Errorf("request = %+v", request)
	}
	var payload notePayload
	if err := json.Unmarshal(request.Body, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Associations) != 1 {
		t.Fatalf("associations = %+v", payload.Associations)
	}
	association := payload.Associations[0]
	if association.To.ID != "123" || len(association.Types) != 1 {
		t.Fatalf("association = %+v", association)
	}
	if association.Types[0].ID != 228 || association.Types[0].Category != "HUBSPOT_DEFINED" {
		t.Errorf("association type = %+v", association.Types[0])
	}
	if payload.Properties["hs_note_body"] != "<p>Source: slack-thread:T:C:1.0</p>body" || payload.Properties["hs_attachment_ids"] != "F1;F2" {
		t.Errorf("properties = %+v", payload.Properties)
	}
	if _, err := time.Parse(time.RFC3339Nano, payload.Properties["hs_timestamp"]); err != nil {
		t.Errorf("timestamp: %v", err)
	}
}

func TestUpdateNoteReplacesBodyAndAttachments(t *testing.T) {
	requests := make(chan recordedRequest, 2)
	client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		requests <- captureRequest(t, r)
		w.WriteHeader(http.StatusOK)
	})
	want := map[string]string{"hs_note_body": "replacement", "hs_attachment_ids": "F1;F2"}

	err := client.UpdateNote(context.Background(), "11", "replacement", []string{"F1", "F2"})

	if err != nil {
		t.Fatal(err)
	}
	request := onlyRequest(t, requests)
	if request.Method != "PATCH" || request.Path != "/crm/v3/objects/notes/11" || request.Authorization != "Bearer secret" {
		t.Errorf("request = %+v", request)
	}
	var payload notePayload
	if err := json.Unmarshal(request.Body, &payload); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(payload.Properties, want) {
		t.Errorf("properties = %+v, want %+v", payload.Properties, want)
	}
}

func TestCreateNoteSizeLimit(t *testing.T) {
	const marker = "slack-thread:T:C:1.0"
	const prefix = "<p>Source: slack-thread:T:C:1.0</p>"
	for _, tc := range []struct {
		name, body string
		rejected   bool
	}{
		{"ASCII boundary with marker", prefix + strings.Repeat("a", 65536-len(prefix)), false},
		{"Unicode boundary with marker", prefix + strings.Repeat("界", 65536-len(prefix)), false},
		{"over limit with marker", prefix + strings.Repeat("a", 65537-len(prefix)), true},
		{"boundary after marker insertion", strings.Repeat("界", 65536-len(prefix)), false},
		{"over limit after marker insertion", strings.Repeat("界", 65537-len(prefix)), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := make(chan recordedRequest, 2)
			client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				requests <- captureRequest(t, r)
				io.WriteString(w, `{"id":"12"}`)
			})

			id, err := client.CreateNote(context.Background(), "123", marker, tc.body, nil)

			if tc.rejected {
				assertRemoteError(t, err, "note_too_large", false)
				if len(requests) != 0 {
					t.Error("oversized note reached HubSpot")
				}
				return
			}
			if err != nil || id != "12" {
				t.Fatalf("boundary note = %q, %v", id, err)
			}
			request := onlyRequest(t, requests)
			var payload notePayload
			if err := json.Unmarshal(request.Body, &payload); err != nil {
				t.Fatal(err)
			}
			if utf8.RuneCountInString(payload.Properties["hs_note_body"]) != 65536 || strings.Count(payload.Properties["hs_note_body"], prefix) != 1 {
				t.Errorf("body length = %d, marker count = %d", utf8.RuneCountInString(payload.Properties["hs_note_body"]), strings.Count(payload.Properties["hs_note_body"], prefix))
			}
		})
	}
}

func TestUpdateNoteSizeLimit(t *testing.T) {
	for _, tc := range []struct {
		name, character string
		size            int
		rejected        bool
	}{
		{"ASCII boundary", "a", 65536, false}, {"Unicode boundary", "界", 65536, false},
		{"ASCII over limit", "a", 65537, true}, {"Unicode over limit", "界", 65537, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := make(chan recordedRequest, 2)
			client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				requests <- captureRequest(t, r)
				w.WriteHeader(http.StatusOK)
			})
			body := strings.Repeat(tc.character, tc.size)

			err := client.UpdateNote(context.Background(), "11", body, nil)

			if tc.rejected {
				assertRemoteError(t, err, "note_too_large", false)
				if len(requests) != 0 {
					t.Error("oversized note reached HubSpot")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			request := onlyRequest(t, requests)
			var payload notePayload
			if err := json.Unmarshal(request.Body, &payload); err != nil {
				t.Fatal(err)
			}
			if payload.Properties["hs_note_body"] != body {
				t.Error("boundary note was truncated or changed")
			}
		})
	}
}

func TestFileNameUsesStableSourceIdentity(t *testing.T) {
	for _, tc := range []struct {
		name, marker string
		wantSame     bool
	}{
		{"same source", fileMarker, true},
		{"different workspace", "slack-file-OTHER-F1", false},
		{"different file", "slack-file-T-F2", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := fileName(tc.marker)

			if (got == stableFileName) != tc.wantSame {
				t.Errorf("filename = %q, same source=%v", got, tc.wantSame)
			}
		})
	}
}

func TestFindFileSearchUsesHashCharacters(t *testing.T) {
	requests := make(chan recordedRequest, 2)
	client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		requests <- captureRequest(t, r)
		io.WriteString(w, `{"results":[]}`)
	})

	id, err := client.FindFile(context.Background(), fileMarker)

	if err != nil || id != "" {
		t.Fatalf("missing file = %q, %v", id, err)
	}
	request := onlyRequest(t, requests)
	if request.Method != "GET" || request.Path != "/files/v3/files/search" || request.Query.Get("name") != "c8df7c49f87c8a67d81" || request.Query.Get("limit") != "100" {
		t.Errorf("search request = %+v", request)
	}
}

func TestFindFileMatchesFullIdentity(t *testing.T) {
	other := stableFileName[:len(stableFileName)-1] + "0"
	for _, tc := range []struct {
		name    string
		results []map[string]string
		want    string
	}{
		{"no results", nil, ""},
		{"hash prefix collision", []map[string]string{{"id": "666", "name": other, "path": "/slack-hubspot/" + other + ".pdf"}}, ""},
		{"different folder", []map[string]string{{"id": "777", "name": stableFileName, "path": "/unrelated/" + stableFileName + ".pdf"}}, ""},
		{"different path basename", []map[string]string{{"id": "777", "name": stableFileName, "path": "/slack-hubspot/other.pdf"}}, ""},
		{"filename with extension", []map[string]string{{"id": "888", "name": stableFileName + ".pdf", "path": "/slack-hubspot/" + stableFileName + ".pdf"}}, "888"},
		{"filename without extension", []map[string]string{{"id": "888", "name": stableFileName, "path": "/slack-hubspot/" + stableFileName}}, "888"},
		{"distinct candidates", []map[string]string{
			{"id": "666", "name": other, "path": "/slack-hubspot/" + other + ".pdf"},
			{"id": "777", "name": stableFileName, "path": "/unrelated/" + stableFileName + ".pdf"},
			{"id": "888", "name": stableFileName, "path": "/slack-hubspot/" + stableFileName + ".pdf"},
		}, "888"},
		// Duplicate exact names may exist after interrupted remote uploads. Recovery
		// reuses the first matching ID instead of creating another file.
		{"multiple exact matches", []map[string]string{
			{"id": "888", "name": stableFileName, "path": "/slack-hubspot/" + stableFileName + ".pdf"},
			{"id": "999", "name": stableFileName, "path": "/slack-hubspot/" + stableFileName + ".pdf"},
		}, "888"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := make(chan recordedRequest, 2)
			client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				requests <- captureRequest(t, r)
				json.NewEncoder(w).Encode(map[string]any{"results": tc.results})
			})

			id, err := client.FindFile(context.Background(), fileMarker)

			if err != nil || id != tc.want {
				t.Fatalf("file = %q, %v; want %q", id, err, tc.want)
			}
			request := onlyRequest(t, requests)
			if request.Path != "/files/v3/files/search" {
				t.Errorf("request = %+v", request)
			}
		})
	}
}

func TestFindFilePaginates(t *testing.T) {
	requests := make(chan recordedRequest, 4)
	client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		requests <- captureRequest(t, r)
		if r.URL.Query().Get("after") == "" {
			io.WriteString(w, `{"results":[],"paging":{"next":{"after":"next"}}}`)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"results": []map[string]string{{"id": "888", "name": stableFileName, "path": "/slack-hubspot/" + stableFileName + ".pdf"}}})
	})

	id, err := client.FindFile(context.Background(), fileMarker)

	if err != nil || id != "888" {
		t.Fatalf("file = %q, %v", id, err)
	}
	if len(requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(requests))
	}
	first, second := <-requests, <-requests
	if first.Query.Get("after") != "" || second.Query.Get("after") != "next" || first.Query.Get("name") != second.Query.Get("name") {
		t.Errorf("page queries = %v, %v", first.Query, second.Query)
	}
}

func TestFindFileRejectsRepeatedCursor(t *testing.T) {
	requests := make(chan recordedRequest, 4)
	client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		requests <- captureRequest(t, r)
		if len(requests) > 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		io.WriteString(w, `{"results":[],"paging":{"next":{"after":"same"}}}`)
	})

	_, err := client.FindFile(context.Background(), fileMarker)

	assertRemoteError(t, err, "repeated_cursor", true)
	if len(requests) != 2 {
		t.Errorf("requests = %d, want 2", len(requests))
	}
}

func TestUploadUsesPrivateStableFile(t *testing.T) {
	type upload struct {
		method, path, authorization, name, folder, mime, body string
		options                                               map[string]any
	}
	requests := make(chan upload, 2)
	client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Error(err)
			http.Error(w, "bad form", 400)
			return
		}
		defer r.MultipartForm.RemoveAll()
		file, header, err := r.FormFile("file")
		if err != nil {
			t.Error(err)
			http.Error(w, "missing file", 400)
			return
		}
		defer file.Close()
		body, err := io.ReadAll(file)
		if err != nil {
			t.Error(err)
		}
		var options map[string]any
		if err := json.Unmarshal([]byte(r.FormValue("options")), &options); err != nil {
			t.Error(err)
		}
		requests <- upload{r.Method, r.URL.Path, r.Header.Get("Authorization"), r.FormValue("fileName"), r.FormValue("folderPath"), header.Header.Get("Content-Type"), string(body), options}
		io.WriteString(w, `{"id":"888"}`)
	})
	wantOptions := map[string]any{"access": "PRIVATE", "duplicateValidationScope": "EXACT_FOLDER", "duplicateValidationStrategy": "RETURN_EXISTING", "overwrite": false}

	id, err := client.Upload(context.Background(), fileMarker, domain.File{ID: "F1", MIME: "application/pdf", Name: "private customer.PDF"}, strings.NewReader("file payload"))

	if err != nil || id != "888" {
		t.Fatalf("upload = %q, %v", id, err)
	}
	if len(requests) != 1 {
		t.Fatalf("uploads = %d, want 1", len(requests))
	}
	got := <-requests
	if got.method != "POST" || got.path != "/files/v3/files" || got.authorization != "Bearer secret" || got.name != stableFileName+".pdf" || got.folder != "/slack-hubspot" || got.mime != "application/pdf" || got.body != "file payload" || !reflect.DeepEqual(got.options, wantOptions) {
		t.Errorf("upload = %+v", got)
	}
}

func TestVerifyAccount(t *testing.T) {
	for _, tc := range []struct {
		name, id string
		rejected bool
	}{{"matching", "42", false}, {"different", "99", true}} {
		t.Run(tc.name, func(t *testing.T) {
			requests := make(chan recordedRequest, 2)
			client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
				requests <- captureRequest(t, r)
				io.WriteString(w, `{"portalId":`+tc.id+`}`)
			})

			err := client.VerifyAccount(context.Background())

			if (err != nil) != tc.rejected {
				t.Fatalf("verification = %v, rejected=%v", err, tc.rejected)
			}
			request := onlyRequest(t, requests)
			if request.Path != "/account-info/v3/details" {
				t.Errorf("path = %q", request.Path)
			}
		})
	}
}

func TestRateLimitError(t *testing.T) {
	client := testClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
		io.WriteString(w, `{"message":"secret payload"}`)
	})

	_, err := client.Ticket(context.Background(), "123")

	assertRemoteError(t, err, "ratelimited", true)
	var remote *domain.RemoteError
	errors.As(err, &remote)
	if remote.RetryAfter != 7*time.Second || strings.Contains(err.Error(), "payload") {
		t.Fatalf("unsafe or incomplete rate limit error: %v", err)
	}
}
