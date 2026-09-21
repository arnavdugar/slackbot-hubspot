// Package slack provides the authenticated Slack Web API adapter.
package slack

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"slackhubspot/internal/domain"
)

type Config struct {
	BaseURL      string
	BotToken     string
	HTTPClient   *http.Client
	HistoryToken string
	RateLimited  func()
}

type Client struct {
	baseURL      string
	botID        string
	botToken     string
	botUserID    string
	http         *http.Client
	historyToken string
	members      map[string]time.Time
	membersMu    sync.Mutex
	rateLimited  func()
}

// VerifyIdentity must run before serving requests. It guards against a token
// from another workspace or a misconfigured bot user ID.
func (c *Client) VerifyIdentity(ctx context.Context, workspaceID, botUserID string) error {
	for _, token := range []string{c.botToken, c.historyToken} {
		var identity struct {
			BotID  string `json:"bot_id"`
			TeamID string `json:"team_id"`
			UserID string `json:"user_id"`
		}
		if err := c.call(ctx, "auth.test", token, nil, &identity); err != nil {
			return err
		}
		if identity.TeamID != workspaceID || (token == c.botToken && identity.UserID != botUserID) {
			return errors.New("Slack token identity does not match configured workspace or bot")
		}
		if token == c.botToken {
			c.botID = identity.BotID
			c.botUserID = identity.UserID
		}
		if c.historyToken == c.botToken {
			break
		}
	}
	return nil
}

func (c *Client) BotID() string { return c.botID }

func New(config Config) (*Client, error) {
	if config.BotToken == "" {
		return nil, errors.New("Slack bot token is required")
	}
	if config.BaseURL == "" {
		config.BaseURL = "https://slack.com/api"
	}
	base, err := url.Parse(config.BaseURL)
	if err != nil || base.Host == "" || base.User != nil || (base.Scheme != "https" && !(config.HTTPClient != nil && base.Scheme == "http" && (base.Hostname() == "127.0.0.1" || base.Hostname() == "localhost" || base.Hostname() == "::1"))) {
		return nil, errors.New("Slack API requires HTTPS")
	}
	client := http.Client{Timeout: 30 * time.Second}
	if config.HTTPClient != nil {
		client = *config.HTTPClient
		if client.Timeout == 0 {
			client.Timeout = 30 * time.Second
		}
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if config.HistoryToken == "" {
		config.HistoryToken = config.BotToken
	}
	return &Client{baseURL: strings.TrimRight(config.BaseURL, "/"), botToken: config.BotToken, http: &client, historyToken: config.HistoryToken, members: make(map[string]time.Time), rateLimited: config.RateLimited}, nil
}

func (c *Client) call(ctx context.Context, method, token string, args map[string]any, output any) error {
	body, err := json.Marshal(args)
	if err != nil {
		return &domain.RemoteError{Code: "invalid_request", Service: "slack"}
	}
	httpMethod, target, requestBody := http.MethodPost, c.baseURL+"/"+method, io.Reader(bytes.NewReader(body))
	if method != "chat.postMessage" && method != "reactions.add" {
		httpMethod = http.MethodGet
		requestBody = nil
		query := url.Values{}
		for key, value := range args {
			query.Set(key, fmt.Sprint(value))
		}
		target += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, httpMethod, target, requestBody)
	if err != nil {
		return &domain.RemoteError{Code: "invalid_request", Service: "slack"}
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &domain.RemoteError{Code: "transport_error", Service: "slack", Temporary: true}
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == http.StatusTooManyRequests && c.rateLimited != nil {
			c.rateLimited()
		}
		return responseError(resp)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20))
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &domain.RemoteError{Code: "read_error", Service: "slack", Temporary: true}
	}
	var envelope struct {
		Error string `json:"error"`
		OK    bool   `json:"ok"`
	}
	if json.Unmarshal(data, &envelope) != nil {
		return &domain.RemoteError{Code: "invalid_response", Service: "slack", Temporary: true}
	}
	if !envelope.OK {
		if envelope.Error == "ratelimited" && c.rateLimited != nil {
			c.rateLimited()
		}
		code := "api_error"
		switch envelope.Error {
		case "already_reacted", "channel_not_found", "file_not_found", "invalid_auth", "missing_scope", "not_authed", "not_in_channel", "ratelimited", "request_timeout", "service_unavailable", "thread_not_found", "token_revoked", "user_not_found", "user_not_visible":
			code = envelope.Error
		}
		e := &domain.RemoteError{Code: code, Service: "slack", Temporary: code == "ratelimited" || code == "request_timeout" || code == "service_unavailable"}
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
			e.RetryAfter = time.Duration(seconds) * time.Second
		}
		return e
	}
	if output != nil && json.Unmarshal(data, output) != nil {
		return &domain.RemoteError{Code: "invalid_response", Service: "slack", Temporary: true}
	}
	return nil
}

func responseError(resp *http.Response) error {
	e := &domain.RemoteError{Code: "http_error", Service: "slack", Temporary: resp.StatusCode == 429 || resp.StatusCode >= 500}
	switch resp.StatusCode {
	case 401:
		e.Code = "invalid_auth"
	case 403:
		e.Code = "missing_scope"
	case 404:
		e.Code = "not_found"
	case 429:
		e.Code = "ratelimited"
	}
	if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
		e.RetryAfter = time.Duration(seconds) * time.Second
	}
	return e
}

type apiFile struct {
	ID         string `json:"id"`
	MIME       string `json:"mimetype"`
	Name       string `json:"name"`
	Size       int64  `json:"size"`
	URL        string `json:"url_private_download"`
	URLPrivate string `json:"url_private"`
}

func (f apiFile) domain() domain.File {
	if f.URL == "" {
		f.URL = f.URLPrivate
	}
	return domain.File{ID: f.ID, MIME: f.MIME, Name: f.Name, Size: f.Size, URL: f.URL}
}

func (c *Client) File(ctx context.Context, id string) (domain.File, error) {
	var result struct {
		File apiFile `json:"file"`
	}
	err := c.call(ctx, "files.info", c.botToken, map[string]any{"file": id}, &result)
	if err == nil && (result.File.ID == "" || result.File.ID != id) {
		err = &domain.RemoteError{Code: "invalid_response", Service: "slack", Temporary: true}
	}
	return result.File.domain(), err
}

func (c *Client) Download(ctx context.Context, file domain.File) (io.ReadCloser, error) {
	target, err := url.Parse(file.URL)
	if err != nil || !c.fileURLAllowed(target) {
		return nil, &domain.RemoteError{Code: "unsafe_download_url", Service: "slack"}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, &domain.RemoteError{Code: "invalid_request", Service: "slack"}
	}
	req.Header.Set("Authorization", "Bearer "+c.botToken)
	downloadClient := *c.http
	downloadClient.CheckRedirect = func(next *http.Request, via []*http.Request) error {
		if len(via) >= 5 || !c.fileURLAllowed(next.URL) {
			return errors.New("unsafe file redirect")
		}
		next.Header.Set("Authorization", "Bearer "+c.botToken)
		return nil
	}
	resp, err := downloadClient.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, &domain.RemoteError{Code: "transport_error", Service: "slack", Temporary: true}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusTooManyRequests && c.rateLimited != nil {
			c.rateLimited()
		}
		return nil, responseError(resp)
	}
	return resp.Body, nil
}

func (c *Client) fileURLAllowed(target *url.URL) bool {
	if target.User != nil {
		return false
	}
	if target.Scheme == "https" && (target.Port() == "" || target.Port() == "443") && (target.Hostname() == "slack.com" || strings.HasSuffix(target.Hostname(), ".slack.com")) {
		return true
	}
	base, _ := url.Parse(c.baseURL)
	return base.Host == target.Host && base.Scheme == target.Scheme && (base.Hostname() == "127.0.0.1" || base.Hostname() == "localhost" || base.Hostname() == "::1")
}

func (c *Client) IsPrivateMember(ctx context.Context, channel string) (bool, error) {
	c.membersMu.Lock()
	until := c.members[channel]
	c.membersMu.Unlock()
	if time.Now().Before(until) {
		return true, nil
	}
	var result struct {
		Channel struct {
			IsArchived bool `json:"is_archived"`
			IsMember   bool `json:"is_member"`
			IsPrivate  bool `json:"is_private"`
		} `json:"channel"`
	}
	if err := c.call(ctx, "conversations.info", c.botToken, map[string]any{"channel": channel}, &result); err != nil {
		return false, err
	}
	member := result.Channel.IsMember && result.Channel.IsPrivate && !result.Channel.IsArchived
	if member {
		c.membersMu.Lock()
		if len(c.members) >= 1024 {
			for id, expires := range c.members {
				if !time.Now().Before(expires) {
					delete(c.members, id)
				}
			}
		}
		if len(c.members) < 1024 {
			c.members[channel] = time.Now().Add(30 * time.Second)
		}
		c.membersMu.Unlock()
	}
	return member, nil
}

func (c *Client) Post(ctx context.Context, key domain.ThreadKey, text, idempotencyKey string) error {
	hash := sha256.Sum256([]byte(idempotencyKey))
	hash[6] = (hash[6] & 0x0f) | 0x50
	hash[8] = (hash[8] & 0x3f) | 0x80
	idempotencyKey = fmt.Sprintf("%x-%x-%x-%x-%x", hash[0:4], hash[4:6], hash[6:8], hash[8:10], hash[10:16])
	return c.call(ctx, "chat.postMessage", c.botToken, map[string]any{"channel": key.ChannelID, "client_msg_id": idempotencyKey, "text": text, "thread_ts": key.ThreadTS, "unfurl_links": false, "unfurl_media": false}, nil)
}

func (c *Client) React(ctx context.Context, key domain.ThreadKey, timestamp, name string) error {
	err := c.call(ctx, "reactions.add", c.botToken, map[string]any{"channel": key.ChannelID, "name": name, "timestamp": timestamp}, nil)
	var remote *domain.RemoteError
	if errors.As(err, &remote) && remote.Code == "already_reacted" {
		return nil
	}
	return err
}

func (c *Client) Thread(ctx context.Context, key domain.ThreadKey) ([]domain.Message, error) {
	var messages []domain.Message
	seenCursors := map[string]bool{}
	seenMessages := map[string]bool{}
	for cursor := ""; ; {
		var page struct {
			HasMore  bool `json:"has_more"`
			Messages []struct {
				BotID     string    `json:"bot_id"`
				Files     []apiFile `json:"files"`
				Reactions []struct {
					Name  string   `json:"name"`
					Users []string `json:"users"`
				} `json:"reactions"`
				Subtype   string `json:"subtype"`
				Text      string `json:"text"`
				ThreadTS  string `json:"thread_ts"`
				Timestamp string `json:"ts"`
				UserID    string `json:"user"`
				Username  string `json:"username"`
			} `json:"messages"`
			Metadata struct {
				Cursor string `json:"next_cursor"`
			} `json:"response_metadata"`
		}
		// Keep the cursor while honoring rate limits. Retrying the entire job
		// would repeatedly fetch page one on installations limited to 1/minute.
		// Three retries bound each page; the worker persists any remaining failure.
		for attempt := 0; ; attempt++ {
			err := c.call(ctx, "conversations.replies", c.historyToken, map[string]any{"channel": key.ChannelID, "cursor": cursor, "limit": 100, "ts": key.ThreadTS}, &page)
			if err == nil {
				break
			}
			var remote *domain.RemoteError
			if attempt >= 3 || !errors.As(err, &remote) || remote.Code != "ratelimited" {
				return nil, err
			}
			delay := remote.RetryAfter
			if delay <= 0 {
				delay = time.Second * time.Duration(1<<attempt)
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
			}
		}
		for _, m := range page.Messages {
			if m.Timestamp == "" || seenMessages[m.Timestamp] {
				continue
			}
			seenMessages[m.Timestamp] = true
			message := domain.Message{BotID: m.BotID, SenderName: m.Username, Subtype: m.Subtype, Text: m.Text, ThreadTS: m.ThreadTS, Timestamp: m.Timestamp, UserID: m.UserID}
			if message.UserID == "" && message.BotID != "" && message.BotID == c.botID {
				message.UserID = c.botUserID
			}
			for _, reaction := range m.Reactions {
				if c.botUserID != "" && slices.Contains(reaction.Users, c.botUserID) {
					message.BotReactions = append(message.BotReactions, reaction.Name)
				}
			}
			slices.Sort(message.BotReactions)
			message.BotReactions = slices.Compact(message.BotReactions)
			for _, file := range m.Files {
				message.Files = append(message.Files, file.domain())
			}
			messages = append(messages, message)
		}
		cursor = strings.TrimSpace(page.Metadata.Cursor)
		if cursor == "" {
			if page.HasMore {
				return nil, &domain.RemoteError{Code: "incomplete_pagination", Service: "slack", Temporary: true}
			}
			break
		}
		if seenCursors[cursor] {
			return nil, &domain.RemoteError{Code: "repeated_cursor", Service: "slack", Temporary: true}
		}
		seenCursors[cursor] = true
	}
	if !seenMessages[key.ThreadTS] {
		return nil, &domain.RemoteError{Code: "incomplete_thread", Service: "slack", Temporary: true}
	}
	names := map[string]string{}
	mentions := regexp.MustCompile(`<@([A-Z0-9]+)(?:\|[^>]*)?>`)
	for i := range messages {
		m := &messages[i]
		userIDs := []string{m.UserID}
		for _, match := range mentions.FindAllStringSubmatch(m.Text, -1) {
			userIDs = append(userIDs, match[1])
		}
		for _, userID := range userIDs {
			if userID == "" {
				continue
			}
			if _, ok := names[userID]; !ok {
				var result struct {
					User struct {
						Name    string `json:"name"`
						Profile struct {
							DisplayName string `json:"display_name"`
							RealName    string `json:"real_name"`
						} `json:"profile"`
					} `json:"user"`
				}
				err := c.call(ctx, "users.info", c.botToken, map[string]any{"user": userID}, &result)
				if err != nil {
					var remote *domain.RemoteError
					if !errors.As(err, &remote) || (remote.Code != "user_not_found" && remote.Code != "user_not_visible") {
						return nil, err
					}
				}
				name := result.User.Profile.DisplayName
				if name == "" {
					name = result.User.Profile.RealName
				}
				if name == "" {
					name = result.User.Name
				}
				if name == "" {
					name = userID
				}
				names[userID] = name
			}
		}
		if m.UserID != "" {
			m.SenderName = names[m.UserID]
		}
		m.Text = mentions.ReplaceAllStringFunc(m.Text, func(token string) string {
			match := mentions.FindStringSubmatch(token)
			name := names[match[1]]
			name = strings.NewReplacer("<", "", ">", "", "|", "").Replace(name)
			return "<@" + match[1] + "|" + name + ">"
		})
		if m.SenderName == "" {
			m.SenderName = m.BotID
		}
		var result struct {
			Permalink string `json:"permalink"`
		}
		if err := c.call(ctx, "chat.getPermalink", c.botToken, map[string]any{"channel": key.ChannelID, "message_ts": m.Timestamp}, &result); err != nil {
			return nil, err
		}
		m.Permalink = result.Permalink
	}
	sort.Slice(messages, func(i, j int) bool { return messages[i].Timestamp < messages[j].Timestamp })
	return messages, nil
}

var _ domain.Slack = (*Client)(nil)
