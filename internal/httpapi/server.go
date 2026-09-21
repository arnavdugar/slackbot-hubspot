package httpapi

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ogen-go/ogen/ogenerrors"
	"github.com/ogen-go/ogen/validate"

	"slackhubspot/internal/domain"
	"slackhubspot/internal/httpapi/api"
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

type server struct {
	options Options
}

var slackTimestamp = regexp.MustCompile(`^[0-9]{1,16}\.[0-9]{6}$`)

// New exposes the generated ogen OpenAPI server behind raw-body authentication.
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
	handler, err := api.NewServer(s, s, api.WithErrorHandler(
		func(_ context.Context, w http.ResponseWriter, r *http.Request, err error) {
			code := http.StatusServiceUnavailable
			var requestError *ogenerrors.DecodeRequestError
			var securityError *ogenerrors.SecurityError
			var tooLarge *http.MaxBytesError
			var contentType *validate.InvalidContentTypeError
			switch {
			case errors.As(err, &tooLarge):
				code = http.StatusRequestEntityTooLarge
			case errors.As(err, &contentType):
				code = http.StatusUnsupportedMediaType
			case errors.As(err, &securityError):
				code = http.StatusUnauthorized
			case errors.As(err, &requestError):
				code = http.StatusBadRequest
				if ct, _, mediaErr := mime.ParseMediaType(r.Header.Get("Content-Type")); mediaErr != nil || ct != "application/json" {
					code = http.StatusUnsupportedMediaType
				}
			}
			if r.URL.Path == "/slack/events" {
				options.Observe("rejected")
			}
			w.WriteHeader(code)
		},
	))
	if err != nil {
		panic(err)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), options.RequestTimeout)
		defer cancel()
		r = r.WithContext(ctx)
		route, matched := handler.FindRoute(r.Method, r.URL.Path)
		if matched && route.OperationID() == "metrics" && options.Metrics != nil {
			options.Metrics.ServeHTTP(w, r)
			return
		}
		if matched && route.OperationID() == "hubspotEvent" {
			r.Body = http.MaxBytesReader(w, r.Body, options.MaxBodyBytes)
		}
		if matched && route.OperationID() == "slackEvent" {
			var authenticated bool
			r, authenticated = s.authenticateSlack(w, r)
			if !authenticated {
				return
			}
		}
		handler.ServeHTTP(w, r)
	})
}

// authenticateSlack verifies the exact bytes before ogen decodes the request.
func (s *server) authenticateSlack(
	w http.ResponseWriter, r *http.Request,
) (*http.Request, bool) {
	s.options.Observe("received")
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.options.MaxBodyBytes))
	if err != nil {
		s.options.Observe("rejected")
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			w.WriteHeader(http.StatusRequestEntityTooLarge)
		} else {
			w.WriteHeader(http.StatusBadRequest)
		}
		return nil, false
	}
	timestamp := r.Header.Get("X-Slack-Request-Timestamp")
	unix, err := strconv.ParseInt(timestamp, 10, 64)
	age := s.options.Now().Sub(time.Unix(unix, 0))
	mac := hmac.New(sha256.New, []byte(s.options.SigningSecret))
	_, _ = mac.Write([]byte("v0:" + timestamp + ":"))
	_, _ = mac.Write(body)
	expected := "v0=" + hex.EncodeToString(mac.Sum(nil))
	if s.options.SigningSecret == "" || err != nil || age < -5*time.Minute || age > 5*time.Minute || !hmac.Equal([]byte(expected), []byte(r.Header.Get("X-Slack-Signature"))) {
		s.options.Observe("rejected")
		w.WriteHeader(http.StatusUnauthorized)
		return nil, false
	}
	r = r.WithContext(context.WithValue(r.Context(), slackAuthenticatedKey{}, true))
	r.Body = io.NopCloser(bytes.NewReader(body))
	return r, true
}

type slackAuthenticatedKey struct{}

// HandleSlackSignature requires the raw-body authentication performed by New.
func (s *server) HandleSlackSignature(
	ctx context.Context, _ api.OperationName, _ api.SlackSignature,
) (context.Context, error) {
	if authenticated, _ := ctx.Value(slackAuthenticatedKey{}).(bool); !authenticated {
		return ctx, errors.New("Slack request was not authenticated")
	}
	return ctx, nil
}

func (s *server) Live(context.Context) error {
	return nil
}

func (s *server) Metrics(context.Context) (api.MetricsRes, error) {
	return &api.MetricsServiceUnavailable{Data: strings.NewReader("metrics unavailable")}, nil
}

func (s *server) Ready(ctx context.Context) (api.ReadyRes, error) {
	if !s.options.Ready() || s.options.Store.Check(ctx) != nil {
		return &api.ReadyServiceUnavailable{}, nil
	}
	return &api.ReadyOK{}, nil
}

func (s *server) SlackEvent(
	ctx context.Context, body *api.Envelope, _ api.SlackEventParams,
) (api.SlackEventRes, error) {
	reject := func() (api.SlackEventRes, error) {
		s.options.Observe("rejected")
		return &api.SlackEventBadRequest{}, nil
	}
	ignore := func() (api.SlackEventRes, error) {
		s.options.Observe("ignored")
		return &api.Acknowledgement{}, nil
	}
	if body == nil {
		return reject()
	}
	if body.Type == "url_verification" {
		if body.Challenge.Value == "" {
			return reject()
		}
		if body.TeamID.Set && body.TeamID.Value != s.options.WorkspaceID {
			s.options.Observe("rejected")
			return &api.SlackEventUnauthorized{}, nil
		}
		return &api.Acknowledgement{Challenge: body.Challenge}, nil
	}
	if body.Type != "event_callback" || body.EventID.Value == "" || body.TeamID.Value == "" || !body.Event.Set {
		return reject()
	}
	if body.TeamID.Value != s.options.WorkspaceID {
		s.options.Observe("rejected")
		return &api.SlackEventUnauthorized{}, nil
	}
	e := body.Event.Value
	if e.Type == "" {
		return reject()
	}
	if e.Type != "message" && e.Type != "app_mention" {
		return ignore()
	}
	if e.Channel.Value == "" {
		return reject()
	}
	subtype := e.Subtype.Value
	message := api.SlackMessage{
		BotID:    e.BotID,
		Subtype:  e.Subtype,
		Text:     e.Text,
		ThreadTs: e.ThreadTs,
		Ts:       e.Ts,
		User:     e.User,
	}
	switch subtype {
	case "", "bot_message", "file_share", "thread_broadcast":
	case "message_changed":
		if !e.Message.Set {
			return reject()
		}
		message = e.Message.Value
	case "message_deleted":
		if !e.PreviousMessage.Set || e.DeletedTs.Value == "" {
			return reject()
		}
		message = e.PreviousMessage.Value
		message.Ts = e.DeletedTs
	default:
		return ignore()
	}
	if message.User.Value == s.options.BotUserID || (s.options.BotID != "" && message.BotID.Value == s.options.BotID) {
		return ignore()
	}
	if inner := message.Subtype.Value; inner != "" && inner != "bot_message" && inner != "file_share" && inner != "thread_broadcast" && inner != "message_changed" && inner != "message_deleted" {
		return ignore()
	}
	actor := message.User.Value
	if actor == "" {
		actor = message.BotID.Value
	}
	if actor == "" || !slackTimestamp.MatchString(message.Ts.Value) {
		return reject()
	}
	threadTS := message.ThreadTs.Value
	if threadTS == "" {
		threadTS = message.Ts.Value
	}
	if !slackTimestamp.MatchString(threadTS) {
		return reject()
	}
	key := domain.ThreadKey{
		ChannelID:   e.Channel.Value,
		ThreadTS:    threadTS,
		WorkspaceID: body.TeamID.Value,
	}
	logger := s.options.Logger.With(
		"channel_id", key.ChannelID,
		"event_id", body.EventID.Value,
		"message_ts", message.Ts.Value,
		"thread_ts", key.ThreadTS,
		"workspace_id", key.WorkspaceID,
	)
	parsed := render.Parse(message.Text.Value, s.options.BotUserID, s.options.HubSpotAccountID)
	event := domain.Event{
		Actor:     actor,
		ID:        body.EventID.Value,
		Key:       key,
		Kind:      "message",
		MessageTS: message.Ts.Value,
		Now:       s.options.Now(),
	}
	changed := subtype == "message_changed" || subtype == "message_deleted"
	if !changed && parsed.Invalid {
		event.Kind = "reply"
		event.Reply, event.ReplyBlocks = render.Help(s.options.BotUserID, true)
	} else if !changed && parsed.Command != "" {
		event.Command = parsed.Command
		event.Kind = "command"
		if parsed.Command == "help" {
			event.Reply, event.ReplyBlocks = render.Help(s.options.BotUserID, false)
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
				return &api.SlackEventServiceUnavailable{}, nil
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
						return &api.SlackEventServiceUnavailable{}, nil
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
		return &api.SlackEventServiceUnavailable{}, nil
	}
	if result.Duplicate {
		s.options.Observe("duplicate")
	} else {
		s.options.Observe("accepted")
	}
	logger.Debug("Slack event recorded", "duplicate", result.Duplicate, "outcome", result.Outcome)
	return &api.Acknowledgement{}, nil
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

// HubSpotEvent deliberately does not acknowledge delivery until authenticated,
// durable preview refresh processing exists.
func (s *server) HubspotEvent(
	context.Context, []api.HubSpotWebhookEvent,
) (api.HubspotEventRes, error) {
	return &api.HubspotEventNotImplemented{}, nil
}
