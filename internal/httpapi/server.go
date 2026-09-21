package httpapi

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"slackhubspot/internal/domain"
	"slackhubspot/internal/render"
)

type Options struct {
	BotID            string
	BotUserID        string
	HubSpot          domain.HubSpot
	HubSpotAccountID string
	Logger           *slog.Logger
	MaxBodyBytes     int64
	Metrics          http.Handler
	Now              func() time.Time
	Observe          func(string)
	Ready            func() bool
	RequestTimeout   time.Duration
	SigningSecret    string
	Slack            domain.Slack
	Store            domain.Store
	Summary          *render.Summary
	WorkspaceID      string
}

type server struct{ options Options }

var slackTimestamp = regexp.MustCompile(`^[0-9]{1,16}\.[0-9]{6}$`)

// New exposes the generated strict OpenAPI server behind raw-body authentication.
func New(options Options) http.Handler {
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.MaxBodyBytes == 0 {
		options.MaxBodyBytes = 1 << 20
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Observe == nil {
		options.Observe = func(string) {}
	}
	if options.Ready == nil {
		options.Ready = func() bool { return true }
	}
	if options.RequestTimeout == 0 {
		options.RequestTimeout = 2500 * time.Millisecond
	}
	s := &server{options: options}
	strict := NewStrictHandlerWithOptions(s, []StrictMiddlewareFunc{func(next StrictHandlerFunc, operation string) StrictHandlerFunc {
		return func(ctx context.Context, w http.ResponseWriter, r *http.Request, request any) (any, error) {
			// Let the metrics exporter negotiate and encode its wire format while
			// keeping routing and the endpoint contract in the strict OpenAPI server.
			if operation == "Metrics" && options.Metrics != nil {
				options.Metrics.ServeHTTP(w, r)
				return nil, nil
			}
			return next(ctx, w, r, request)
		}
	}}, StrictHTTPServerOptions{
		RequestErrorHandlerFunc: func(w http.ResponseWriter, _ *http.Request, _ error) {
			options.Observe("rejected")
			w.WriteHeader(http.StatusBadRequest)
		},
		ResponseErrorHandlerFunc: func(w http.ResponseWriter, _ *http.Request, _ error) { w.WriteHeader(http.StatusServiceUnavailable) },
	})
	handler := Handler(strict)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), options.RequestTimeout)
		defer cancel()
		r = r.WithContext(ctx)
		if r.Method == http.MethodPost && r.URL.Path == "/slack/events" {
			options.Observe("received")
			body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, options.MaxBodyBytes))
			if err != nil {
				options.Observe("rejected")
				var tooLarge *http.MaxBytesError
				if errors.As(err, &tooLarge) {
					w.WriteHeader(http.StatusRequestEntityTooLarge)
				} else {
					w.WriteHeader(http.StatusBadRequest)
				}
				return
			}
			timestamp := r.Header.Get("X-Slack-Request-Timestamp")
			unix, err := strconv.ParseInt(timestamp, 10, 64)
			age := options.Now().Sub(time.Unix(unix, 0))
			mac := hmac.New(sha256.New, []byte(options.SigningSecret))
			_, _ = mac.Write([]byte("v0:" + timestamp + ":"))
			_, _ = mac.Write(body)
			expected := "v0=" + hex.EncodeToString(mac.Sum(nil))
			if options.SigningSecret == "" || err != nil || age < -5*time.Minute || age > 5*time.Minute || !hmac.Equal([]byte(expected), []byte(r.Header.Get("X-Slack-Signature"))) {
				options.Observe("rejected")
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			if !json.Valid(body) {
				options.Observe("rejected")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			contentType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if err != nil || contentType != "application/json" {
				options.Observe("rejected")
				w.WriteHeader(http.StatusUnsupportedMediaType)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
		}
		handler.ServeHTTP(w, r)
	})
}

func (s *server) Live(context.Context, LiveRequestObject) (LiveResponseObject, error) {
	return Live200Response{}, nil
}
func (s *server) Metrics(context.Context, MetricsRequestObject) (MetricsResponseObject, error) {
	return Metrics503TextResponse("metrics unavailable"), nil
}
func (s *server) Ready(ctx context.Context, _ ReadyRequestObject) (ReadyResponseObject, error) {
	if !s.options.Ready() || s.options.Store.Check(ctx) != nil {
		return Ready503Response{}, nil
	}
	return Ready200Response{}, nil
}

func (s *server) SlackEvent(ctx context.Context, request SlackEventRequestObject) (SlackEventResponseObject, error) {
	reject := func() (SlackEventResponseObject, error) {
		s.options.Observe("rejected")
		return SlackEvent400Response{}, nil
	}
	ignore := func() (SlackEventResponseObject, error) {
		s.options.Observe("ignored")
		return SlackEvent200JSONResponse{}, nil
	}
	if request.Body == nil {
		return reject()
	}
	body := request.Body
	if body.Type == "url_verification" {
		if value(body.Challenge) == "" {
			return reject()
		}
		if body.TeamId != nil && *body.TeamId != s.options.WorkspaceID {
			s.options.Observe("rejected")
			return SlackEvent401Response{}, nil
		}
		return SlackEvent200JSONResponse{Challenge: body.Challenge}, nil
	}
	if body.Type != "event_callback" || value(body.EventId) == "" || value(body.TeamId) == "" || body.Event == nil {
		return reject()
	}
	if *body.TeamId != s.options.WorkspaceID {
		s.options.Observe("rejected")
		return SlackEvent401Response{}, nil
	}
	e := body.Event
	if e.Type == "" {
		return reject()
	}
	if e.Type != "message" && e.Type != "app_mention" {
		return ignore()
	}
	if value(e.Channel) == "" {
		return reject()
	}
	if value(e.ChannelType) != "" && value(e.ChannelType) != "group" {
		return ignore()
	}
	subtype := value(e.Subtype)
	message := SlackMessage{BotId: e.BotId, Subtype: e.Subtype, Text: e.Text, ThreadTs: e.ThreadTs, Ts: e.Ts, User: e.User}
	switch subtype {
	case "", "bot_message", "file_share", "thread_broadcast":
	case "message_changed":
		if e.Message == nil {
			return reject()
		}
		message = *e.Message
	case "message_deleted":
		if e.PreviousMessage == nil || value(e.DeletedTs) == "" {
			return reject()
		}
		message = *e.PreviousMessage
		message.Ts = e.DeletedTs
	default:
		return ignore()
	}
	if value(message.User) == s.options.BotUserID || (s.options.BotID != "" && value(message.BotId) == s.options.BotID) {
		return ignore()
	}
	if inner := value(message.Subtype); inner != "" && inner != "bot_message" && inner != "file_share" && inner != "thread_broadcast" && inner != "message_changed" && inner != "message_deleted" {
		return ignore()
	}
	actor := value(message.User)
	if actor == "" {
		actor = value(message.BotId)
	}
	if actor == "" || !slackTimestamp.MatchString(value(message.Ts)) {
		return reject()
	}
	threadTS := value(message.ThreadTs)
	if threadTS == "" {
		threadTS = *message.Ts
	}
	if !slackTimestamp.MatchString(threadTS) {
		return reject()
	}
	key := domain.ThreadKey{ChannelID: *e.Channel, ThreadTS: threadTS, WorkspaceID: *body.TeamId}
	logger := s.options.Logger.With("channel_id", key.ChannelID, "event_id", *body.EventId, "message_ts", *message.Ts, "thread_ts", key.ThreadTS, "workspace_id", key.WorkspaceID)
	member, err := s.options.Slack.IsPrivateMember(ctx, key.ChannelID)
	if err != nil {
		logger.Warn("Slack channel validation unavailable", "error", safeError(err))
		return SlackEvent503Response{}, nil
	}
	if !member {
		return ignore()
	}
	parsed := render.Parse(value(message.Text), s.options.BotUserID, s.options.HubSpotAccountID)
	event := domain.Event{Actor: actor, ID: *body.EventId, Key: key, Kind: "message", MessageTS: *message.Ts, Now: s.options.Now()}
	changed := subtype == "message_changed" || subtype == "message_deleted"
	if !changed && parsed.Invalid {
		event.Kind = "reply"
		event.Reply = "I couldn't understand that request. " + helpText(s.options.BotUserID)
	} else if !changed && parsed.Command != "" {
		event.Command = parsed.Command
		event.Kind = "command"
		if parsed.Command == "help" {
			event.Reply = helpText(s.options.BotUserID)
		}
	} else if !changed && parsed.TicketID != "" {
		if key.ThreadTS != event.MessageTS {
			event.Kind = "reply"
			event.Reply = "Start tracking by mentioning me with a ticket ID or URL in a new root message. A thread can track only one ticket."
		} else {
			event.Kind = "link"
			event.TicketID = parsed.TicketID
			existing, err := s.options.Store.Get(ctx, key)
			if err != nil && !errors.Is(err, domain.ErrNotFound) {
				logger.Error("Read thread mapping failed", "error", safeError(err))
				return SlackEvent503Response{}, nil
			}
			if errors.Is(err, domain.ErrNotFound) {
				ticket, err := s.options.HubSpot.Ticket(ctx, parsed.TicketID)
				if err != nil {
					var remote *domain.RemoteError
					if errors.As(err, &remote) && !remote.Temporary {
						event.Kind = "reply"
						event.Reply = "I couldn't validate that ticket. Check the ticket ID and my HubSpot access, then send a new root message."
					} else {
						logger.Warn("HubSpot ticket validation unavailable", "error", safeError(err), "ticket_id", parsed.TicketID)
						return SlackEvent503Response{}, nil
					}
				} else {
					event.Reply = s.options.Summary.Render(ticket)
					event.TicketURL = ticket.URL
				}
			} else {
				event.TicketURL = existing.TicketURL
			}
		}
	}
	result, err := s.options.Store.Record(ctx, event)
	if err != nil {
		logger.Error("Durable event recording failed", "error", safeError(err))
		return SlackEvent503Response{}, nil
	}
	if result.Duplicate {
		s.options.Observe("duplicate")
	} else {
		s.options.Observe("accepted")
	}
	logger.Debug("Slack event recorded", "duplicate", result.Duplicate, "outcome", result.Outcome)
	return SlackEvent200JSONResponse{}, nil
}

func value(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func safeError(err error) string {
	if errors.Is(err, context.Canceled) {
		return "request_cancelled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "request_deadline_exceeded"
	}
	var remote *domain.RemoteError
	if errors.As(err, &remote) {
		return remote.Error()
	}
	return "operation_failed"
}

func helpText(bot string) string {
	mention := "<@" + bot + ">"
	return "Start tracking in a new root message: `" + mention + " 12345` or `" + mention + " https://app.hubspot.com/contacts/ACCOUNT/ticket/12345`. In its thread, use `" + mention + " sync` to rebuild the transcript, `" + mention + " untrack` to stop tracking while preserving ticket, note, files, and reactions, `" + mention + " resume` to resume and backfill messages/files posted while stopped, `" + mention + " status` to check tracking and synchronization, or `" + mention + " help` for these examples."
}
