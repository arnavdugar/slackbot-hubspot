// Package hubspot provides the authenticated HubSpot CRM and private Files APIs.
package hubspot

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"html"
	"io"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"slackhubspot/internal/domain"
)

type Config struct {
	AccountID     string
	BaseURL       string
	HTTPClient    *http.Client
	RateLimited   func()
	SummaryFields []string
	Token         string
}

// VerifyAccount ensures the configured URL account is the token's real account.
func (c *Client) VerifyAccount(ctx context.Context) error {
	var account struct {
		PortalID json.Number `json:"portalId"`
	}
	if err := c.json(ctx, http.MethodGet, "/account-info/v3/details", nil, &account); err != nil {
		return err
	}
	if account.PortalID.String() != c.accountID {
		return errors.New("HubSpot token does not match configured account")
	}
	return nil
}

type Client struct {
	accountID   string
	baseURL     string
	fields      map[string]bool
	http        *http.Client
	rateLimited func()
	token       string
}

func New(config Config) (*Client, error) {
	if config.AccountID == "" || config.Token == "" {
		return nil, errors.New("HubSpot account ID and access token are required")
	}
	if config.BaseURL == "" {
		config.BaseURL = "https://api.hubapi.com"
	}
	base, err := url.Parse(config.BaseURL)
	if err != nil || base.Host == "" || base.User != nil || (base.Scheme != "https" && !(config.HTTPClient != nil && base.Scheme == "http" && (base.Hostname() == "127.0.0.1" || base.Hostname() == "localhost" || base.Hostname() == "::1"))) {
		return nil, errors.New("HubSpot API requires HTTPS")
	}
	client := http.Client{Timeout: 60 * time.Second}
	if config.HTTPClient != nil {
		client = *config.HTTPClient
		if client.Timeout == 0 {
			client.Timeout = 60 * time.Second
		}
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	fields := map[string]bool{}
	for _, field := range config.SummaryFields {
		fields[field] = true
	}
	return &Client{accountID: config.AccountID, baseURL: strings.TrimRight(config.BaseURL, "/"), fields: fields, http: &client, rateLimited: config.RateLimited, token: config.Token}, nil
}

func (c *Client) request(ctx context.Context, method, path, contentType string, body io.Reader, result any) error {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return &domain.RemoteError{Code: "invalid_request", Service: "hubspot"}
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &domain.RemoteError{Code: "transport_error", Service: "hubspot", Temporary: true}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == http.StatusTooManyRequests && c.rateLimited != nil {
			c.rateLimited()
		}
		e := &domain.RemoteError{Code: "http_error", Service: "hubspot", Temporary: resp.StatusCode == 408 || resp.StatusCode == 423 || resp.StatusCode == 429 || resp.StatusCode >= 500}
		switch resp.StatusCode {
		case 400:
			e.Code = "invalid_request"
		case 401:
			e.Code = "invalid_auth"
		case 403:
			e.Code = "missing_scope"
		case 404:
			e.Code = "not_found"
		case 413:
			e.Code = "too_large"
		case 423:
			e.Code = "locked"
		case 429:
			e.Code = "ratelimited"
		}
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
			e.RetryAfter = time.Duration(seconds) * time.Second
		} else if at, err := http.ParseTime(resp.Header.Get("Retry-After")); err == nil && time.Until(at) > 0 {
			e.RetryAfter = time.Until(at)
		}
		return e
	}
	if result != nil {
		if err := json.NewDecoder(io.LimitReader(resp.Body, 16<<20)).Decode(result); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return &domain.RemoteError{Code: "invalid_response", Service: "hubspot", Temporary: true}
		}
	}
	return nil
}

func (c *Client) json(ctx context.Context, method, path string, body, result any) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return &domain.RemoteError{Code: "invalid_request", Service: "hubspot"}
		}
		reader = bytes.NewReader(data)
	}
	return c.request(ctx, method, path, "application/json", reader, result)
}

type object struct {
	Associations map[string]struct {
		Results []struct {
			ID string `json:"id"`
		} `json:"results"`
	} `json:"associations"`
	CreatedAt  string            `json:"createdAt"`
	ID         string            `json:"id"`
	Properties map[string]string `json:"properties"`
	UpdatedAt  string            `json:"updatedAt"`
}

func (c *Client) Ticket(ctx context.Context, id string) (domain.Ticket, error) {
	query := url.Values{"properties": {"createdate,hs_lastmodifieddate,hs_pipeline,hs_pipeline_stage,hs_ticket_priority,hubspot_owner_id,subject"}}
	var associations []string
	if c.fields["Company"] {
		associations = append(associations, "companies")
	}
	if c.fields["Contact"] {
		associations = append(associations, "contacts")
	}
	if len(associations) > 0 {
		query.Set("associations", strings.Join(associations, ","))
	}
	var ticket object
	if err := c.json(ctx, http.MethodGet, "/crm/v3/objects/tickets/"+url.PathEscape(id)+"?"+query.Encode(), nil, &ticket); err != nil {
		return domain.Ticket{}, err
	}
	if ticket.ID == "" || ticket.ID != id {
		return domain.Ticket{}, &domain.RemoteError{Code: "invalid_response", Service: "hubspot"}
	}
	result := domain.Ticket{CreatedAt: ticket.CreatedAt, ID: ticket.ID, Owner: ticket.Properties["hubspot_owner_id"], Pipeline: ticket.Properties["hs_pipeline"], Priority: ticket.Properties["hs_ticket_priority"], Status: ticket.Properties["hs_pipeline_stage"], Subject: ticket.Properties["subject"], UpdatedAt: ticket.UpdatedAt, URL: "https://app.hubspot.com/contacts/" + c.accountID + "/record/0-5/" + ticket.ID}
	if result.CreatedAt == "" {
		result.CreatedAt = ticket.Properties["createdate"]
	}
	if result.UpdatedAt == "" {
		result.UpdatedAt = ticket.Properties["hs_lastmodifieddate"]
	}
	if (c.fields["Pipeline"] || c.fields["Status"]) && result.Pipeline != "" {
		var pipeline struct {
			Label  string `json:"label"`
			Stages []struct {
				ID    string `json:"id"`
				Label string `json:"label"`
			} `json:"stages"`
		}
		if err := c.json(ctx, http.MethodGet, "/crm/v3/pipelines/tickets/"+url.PathEscape(result.Pipeline), nil, &pipeline); err != nil {
			return domain.Ticket{}, err
		}
		if pipeline.Label != "" {
			result.Pipeline = pipeline.Label
		}
		for _, stage := range pipeline.Stages {
			if stage.ID == result.Status {
				result.Status = stage.Label
				break
			}
		}
	}
	if c.fields["Owner"] && result.Owner != "" {
		var owner struct {
			FirstName string `json:"firstName"`
			LastName  string `json:"lastName"`
		}
		if err := c.json(ctx, http.MethodGet, "/crm/v3/owners/"+url.PathEscape(result.Owner), nil, &owner); err != nil {
			return domain.Ticket{}, err
		}
		if name := strings.TrimSpace(owner.FirstName + " " + owner.LastName); name != "" {
			result.Owner = name
		}
	}
	for _, kind := range associations {
		items := ticket.Associations[kind].Results
		if len(items) == 0 {
			continue
		}
		sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
		properties := "name"
		if kind == "contacts" {
			properties = "firstname,lastname"
		}
		var associated object
		if err := c.json(ctx, http.MethodGet, "/crm/v3/objects/"+kind+"/"+url.PathEscape(items[0].ID)+"?properties="+properties, nil, &associated); err != nil {
			return domain.Ticket{}, err
		}
		if kind == "companies" {
			result.Company = associated.Properties["name"]
		} else {
			result.Contact = strings.TrimSpace(associated.Properties["firstname"] + " " + associated.Properties["lastname"])
		}
	}
	return result, nil
}

// FindNote traverses ticket associations, because notes are not supported by
// HubSpot CRM search. The source marker permits reconciliation after a crash.
func (c *Client) FindNote(ctx context.Context, ticketID, marker string) (string, error) {
	seen := map[string]bool{}
	for after := ""; ; {
		var page struct {
			Paging struct {
				Next struct {
					After string `json:"after"`
				} `json:"next"`
			} `json:"paging"`
			Results []struct {
				ID string `json:"id"`
			} `json:"results"`
		}
		query := url.Values{"limit": {"100"}}
		if after != "" {
			query.Set("after", after)
		}
		if err := c.json(ctx, http.MethodGet, "/crm/v3/objects/tickets/"+url.PathEscape(ticketID)+"/associations/notes?"+query.Encode(), nil, &page); err != nil {
			return "", err
		}
		for _, entry := range page.Results {
			var note object
			if err := c.json(ctx, http.MethodGet, "/crm/v3/objects/notes/"+url.PathEscape(entry.ID)+"?properties=hs_note_body", nil, &note); err != nil {
				return "", err
			}
			if note.ID == "" {
				return "", &domain.RemoteError{Code: "invalid_response", Service: "hubspot", Temporary: true}
			}
			if strings.Contains(note.Properties["hs_note_body"], "Source: "+html.EscapeString(marker)+"</p>") {
				return note.ID, nil
			}
		}
		after = page.Paging.Next.After
		if after == "" {
			break
		}
		if seen[after] {
			return "", &domain.RemoteError{Code: "repeated_cursor", Service: "hubspot", Temporary: true}
		}
		seen[after] = true
	}
	return "", nil
}

func (c *Client) CreateNote(ctx context.Context, ticketID, marker, body string, fileIDs []string) (string, error) {
	if !strings.Contains(body, "Source: "+html.EscapeString(marker)+"</p>") {
		body = "<p>Source: " + html.EscapeString(marker) + "</p>" + body
	}
	if utf8.RuneCountInString(body) > 65536 {
		return "", &domain.RemoteError{Code: "note_too_large", Service: "hubspot"}
	}
	var result object
	err := c.json(ctx, http.MethodPost, "/crm/v3/objects/notes", map[string]any{
		"associations": []any{map[string]any{"to": map[string]string{"id": ticketID}, "types": []any{map[string]any{"associationCategory": "HUBSPOT_DEFINED", "associationTypeId": 228}}}},
		"properties":   map[string]string{"hs_attachment_ids": strings.Join(fileIDs, ";"), "hs_note_body": body, "hs_timestamp": time.Now().UTC().Format(time.RFC3339Nano)},
	}, &result)
	if err == nil && result.ID == "" {
		err = &domain.RemoteError{Code: "invalid_response", Service: "hubspot", Temporary: true}
	}
	return result.ID, err
}

func (c *Client) UpdateNote(ctx context.Context, id, body string, fileIDs []string) error {
	if utf8.RuneCountInString(body) > 65536 {
		return &domain.RemoteError{Code: "note_too_large", Service: "hubspot"}
	}
	return c.json(ctx, http.MethodPatch, "/crm/v3/objects/notes/"+url.PathEscape(id), map[string]any{"properties": map[string]string{"hs_attachment_ids": strings.Join(fileIDs, ";"), "hs_note_body": body}}, nil)
}

// The stable name deliberately excludes user-provided filenames and personal
// data. Source IDs remain in the durable local mapping and transcript.
func fileName(marker string) string {
	hash := sha256.Sum256([]byte(marker))
	return "slack-" + hex.EncodeToString(hash[:])
}

func (c *Client) FindFile(ctx context.Context, marker string) (string, error) {
	name := fileName(marker)
	seen := map[string]bool{}
	for after := ""; ; {
		query := url.Values{"limit": {"100"}, "name": {name}}
		if after != "" {
			query.Set("after", after)
		}
		var page struct {
			Paging struct {
				Next struct {
					After string `json:"after"`
				} `json:"next"`
			} `json:"paging"`
			Results []struct {
				ID   string `json:"id"`
				Name string `json:"name"`
				Path string `json:"path"`
			} `json:"results"`
		}
		if err := c.json(ctx, http.MethodGet, "/files/v3/files/search?"+query.Encode(), nil, &page); err != nil {
			return "", err
		}
		for _, file := range page.Results {
			base := strings.TrimSuffix(path.Base(file.Path), path.Ext(file.Path))
			if (file.Name == name || strings.TrimSuffix(file.Name, path.Ext(file.Name)) == name) && path.Dir(file.Path) == "/slack-hubspot" && (base == name || path.Base(file.Path) == name) {
				return file.ID, nil
			}
		}
		after = page.Paging.Next.After
		if after == "" {
			return "", nil
		}
		if seen[after] {
			return "", &domain.RemoteError{Code: "repeated_cursor", Service: "hubspot", Temporary: true}
		}
		seen[after] = true
	}
}

func (c *Client) Upload(ctx context.Context, marker string, file domain.File, source io.Reader) (string, error) {
	name := fileName(marker)
	extension := strings.ToLower(path.Ext(file.Name))
	if len(extension) > 1 && len(extension) <= 12 && strings.IndexFunc(extension[1:], func(r rune) bool { return !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9') }) < 0 {
		name += extension
	}
	reader, writer := io.Pipe()
	form := multipart.NewWriter(writer)
	done := make(chan error, 1)
	go func() {
		var err error
		for _, field := range []struct{ name, value string }{{"fileName", name}, {"folderPath", "/slack-hubspot"}, {"options", `{"access":"PRIVATE","duplicateValidationScope":"EXACT_FOLDER","duplicateValidationStrategy":"RETURN_EXISTING","overwrite":false}`}} {
			if err = form.WriteField(field.name, field.value); err != nil {
				break
			}
		}
		if err == nil {
			var part io.Writer
			header := make(textproto.MIMEHeader)
			header.Set("Content-Disposition", `form-data; name="file"; filename="`+name+`"`)
			mime := file.MIME
			if mime == "" || strings.ContainsAny(mime, "\r\n") {
				mime = "application/octet-stream"
			}
			header.Set("Content-Type", mime)
			part, err = form.CreatePart(header)
			if err == nil {
				_, err = io.Copy(part, source)
			}
		}
		if err == nil {
			err = form.Close()
		}
		writer.CloseWithError(err)
		done <- err
	}()
	var result struct {
		ID string `json:"id"`
	}
	err := c.request(ctx, http.MethodPost, "/files/v3/files", form.FormDataContentType(), reader, &result)
	reader.CloseWithError(errors.New("upload finished"))
	streamErr := <-done
	if err != nil {
		return "", err
	}
	if streamErr != nil {
		return "", &domain.RemoteError{Code: "upload_stream_error", Service: "hubspot", Temporary: true}
	}
	if result.ID == "" {
		return "", &domain.RemoteError{Code: "invalid_response", Service: "hubspot", Temporary: true}
	}
	return result.ID, nil
}

var _ domain.HubSpot = (*Client)(nil)
