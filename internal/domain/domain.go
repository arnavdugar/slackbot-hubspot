// Package domain defines the portable storage and integration contracts.
package domain

import (
	"context"
	"errors"
	"io"
	"time"
)

var (
	ErrLeaseLost = errors.New("lease lost or tracking stopped")
	ErrNotFound  = errors.New("not found")
	ErrNoWork    = errors.New("no eligible work")
)

type ThreadKey struct {
	ChannelID   string
	ThreadTS    string
	WorkspaceID string
}

func (k ThreadKey) String() string { return k.WorkspaceID + ":" + k.ChannelID + ":" + k.ThreadTS }
func (k ThreadKey) Marker() string { return "slack-thread:" + k.String() }

type Thread struct {
	Active              bool
	Attempts            int
	ChangedAt           time.Time
	ChangedBy           string
	CompletedGeneration int64
	CurrentError        string
	HistoricalError     string
	Key                 ThreadKey
	LastCommandTS       string
	LastEventID         string
	LastMessageTS       string
	LastSuccess         time.Time
	LeaseExpires        time.Time
	LeaseToken          string
	NextAttempt         time.Time
	NoteID              string
	PartialError        string
	RequestedGeneration int64
	Revision            int64
	State               string
	TicketID            string
	TicketURL           string
}

// Event is already authenticated, filtered and parsed. Link events contain a
// verified ticket and rendered summary. No conversation archive is persisted.
type Event struct {
	Actor     string
	Command   string
	ID        string
	Key       ThreadKey
	Kind      string // command, link, message, reply
	MessageTS string
	Now       time.Time
	Reply     string
	TicketID  string
	TicketURL string
}

type RecordResult struct {
	Duplicate bool
	Outcome   string
	Thread    *Thread
}

type Lease struct {
	Expires    time.Time
	Generation int64
	Recovered  bool
	Revision   int64
	Thread     Thread
	Token      string
}

type Completion struct {
	PartialError string
	RetryAt      time.Time
}

type Failure struct {
	Error       string // bounded, sanitized operational summary, never remote payloads
	NextAttempt time.Time
	Permanent   bool
}

type Upload struct {
	Attempts     int
	CurrentError string
	FileID       string
	HubSpotID    string
	LeaseExpires time.Time
	LeaseToken   string
	NextAttempt  time.Time
	State        string
	WorkspaceID  string
}

type Notification struct {
	Attempts     int
	ID           string
	Key          ThreadKey
	LeaseExpires time.Time
	LeaseToken   string
	Text         string
}

type WorkCounts struct {
	Leased  int64
	Pending int64
}

// Store operations are atomic. All lease mutations must reject expired tokens,
// stopped threads and changed tracking revisions with ErrLeaseLost. Uploads are
// workspace-wide and may outlive one thread's lease if other threads need them.
type Store interface {
	Check(context.Context) error
	Claim(context.Context, string, time.Time, time.Duration) (Lease, error)
	ClaimNotification(context.Context, string, time.Time, time.Duration) (Notification, error)
	ClaimUpload(context.Context, Lease, string, time.Time, time.Duration) (Upload, error)
	Close() error
	Complete(context.Context, Lease, Completion, time.Time) error
	CompleteNotification(context.Context, Notification, time.Time) error
	CompleteUpload(context.Context, Upload, string, time.Time) error
	Counts(context.Context, time.Time) (WorkCounts, error)
	Fail(context.Context, Lease, Failure, time.Time) error
	FailNotification(context.Context, Notification, Failure, time.Time) error
	FailUpload(context.Context, Upload, Failure, time.Time) error
	Get(context.Context, ThreadKey) (Thread, error)
	Migrate(context.Context) error
	Record(context.Context, Event) (RecordResult, error)
	Renew(context.Context, Lease, time.Time, time.Duration) error
	RenewUpload(context.Context, Upload, time.Time, time.Duration) error
	SaveNote(context.Context, Lease, string, time.Time) error
	Validate(context.Context, Lease, time.Time) error
}

type File struct {
	ID   string
	MIME string
	Name string
	Size int64
	URL  string
}

type Message struct {
	BotID        string
	BotReactions []string
	Files        []File
	Permalink    string
	SenderName   string
	Subtype      string
	Text         string
	ThreadTS     string
	Timestamp    string
	UserID       string
}

type Ticket struct {
	Company   string
	Contact   string
	CreatedAt string
	ID        string
	Owner     string
	Pipeline  string
	Priority  string
	Status    string
	Subject   string
	UpdatedAt string
	URL       string
}

type Slack interface {
	File(context.Context, string) (File, error)
	Download(context.Context, File) (io.ReadCloser, error)
	IsPrivateMember(context.Context, string) (bool, error)
	Post(context.Context, ThreadKey, string, string) error
	React(context.Context, ThreadKey, string, string) error
	Thread(context.Context, ThreadKey) ([]Message, error)
}

type HubSpot interface {
	CreateNote(context.Context, string, string, string, []string) (string, error)
	FindNote(context.Context, string, string) (string, error)
	FindFile(context.Context, string) (string, error)
	Ticket(context.Context, string) (Ticket, error)
	UpdateNote(context.Context, string, string, []string) error
	Upload(context.Context, string, File, io.Reader) (string, error)
}

// RemoteError carries only a safe, fixed code, not remote response bodies/URLs.
type RemoteError struct {
	Code       string
	RetryAfter time.Duration
	Service    string
	Temporary  bool
}

func (e *RemoteError) Error() string { return e.Service + ": " + e.Code }
