package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"slackhubspot/internal/domain"
	"slackhubspot/internal/store"
)

type fakeSlack struct {
	domain.Slack
	files     map[string]domain.File
	messages  []domain.Message
	onThread  func()
	onPost    func()
	posts     int
	reactions []string
}

func (s *fakeSlack) File(_ context.Context, id string) (domain.File, error) { return s.files[id], nil }
func (s *fakeSlack) Download(context.Context, domain.File) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("file")), nil
}
func (s *fakeSlack) Post(context.Context, domain.ThreadKey, string, string) error {
	s.posts++
	if s.onPost != nil {
		s.onPost()
	}
	return nil
}
func (s *fakeSlack) React(_ context.Context, _ domain.ThreadKey, timestamp, _ string) error {
	s.reactions = append(s.reactions, timestamp)
	return nil
}
func (s *fakeSlack) Thread(context.Context, domain.ThreadKey) ([]domain.Message, error) {
	if s.onThread != nil {
		s.onThread()
	}
	return append([]domain.Message(nil), s.messages...), nil
}

type fakeHubSpot struct {
	domain.HubSpot
	body       string
	creates    int
	fileIDs    []string
	noteID     string
	onFindFile func()
	onUpdate   func()
	updates    int
	uploadErr  error
	uploads    int
}

func (h *fakeHubSpot) CreateNote(_ context.Context, _, _, body string, files []string) (string, error) {
	h.creates++
	h.body = body
	h.fileIDs = files
	h.noteID = "note-1"
	return h.noteID, nil
}
func (h *fakeHubSpot) FindNote(context.Context, string, string) (string, error) { return h.noteID, nil }
func (h *fakeHubSpot) FindFile(context.Context, string) (string, error) {
	if h.onFindFile != nil {
		h.onFindFile()
	}
	return "", nil
}
func (h *fakeHubSpot) UpdateNote(_ context.Context, _ string, body string, files []string) error {
	h.updates++
	h.body = body
	h.fileIDs = files
	if h.onUpdate != nil {
		h.onUpdate()
	}
	return nil
}
func (h *fakeHubSpot) Upload(_ context.Context, _ string, _ domain.File, data io.Reader) (string, error) {
	h.uploads++
	if _, err := io.ReadAll(data); err != nil {
		return "", err
	}
	if h.uploadErr != nil {
		return "", h.uploadErr
	}
	return "hub-file-1", nil
}

func setup(t *testing.T) (*Worker, domain.Store, *fakeSlack, *fakeHubSpot, domain.ThreadKey) {
	t.Helper()
	ctx := context.Background()
	db, err := store.Open(ctx, store.Config{"driver": "sqlite", "sqlite": map[string]any{"path": filepath.Join(t.TempDir(), "test.db")}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err = db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	key := domain.ThreadKey{ChannelID: "G1", ThreadTS: "1700000000.000001", WorkspaceID: "T1"}
	if _, err = db.Record(ctx, domain.Event{Actor: "U1", ID: "link", Key: key, Kind: "link", MessageTS: key.ThreadTS, Now: time.Now(), Reply: "Ticket summary", TicketID: "123", TicketURL: "https://app.hubspot.com/contacts/1/record/0-5/123"}); err != nil {
		t.Fatal(err)
	}
	slack := &fakeSlack{files: map[string]domain.File{"F1": {ID: "F1", MIME: "text/plain", Name: "file.txt", Size: 4}, "F2": {ID: "F2", MIME: "text/plain", Name: "large.txt", Size: 100}}}
	hubspot := &fakeHubSpot{}
	w := New(Options{AccountID: "1", AllowedMIMETypes: []string{"text/plain"}, Attachments: true, BackoffBase: time.Second, BackoffMax: time.Minute, BotUserID: "UBOT", Concurrency: 1, HubSpot: hubspot, LeaseDuration: time.Minute, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), MaxAttachmentBytes: 8, MaxAttempts: 3, PollInterval: time.Millisecond, Slack: slack, Store: db, SuccessReaction: "white_check_mark", TempDir: t.TempDir()})
	return w, db, slack, hubspot, key
}

func TestFullReconciliationPreservesTextOnAttachmentFailureAndReusesMappings(t *testing.T) {
	w, db, slack, hubspot, key := setup(t)
	slack.messages = []domain.Message{
		{Files: []domain.File{slack.files["F1"], slack.files["F2"]}, Text: "latest <script>text</script>", Timestamp: "1700000002.000000", UserID: "U2"},
		{Text: "<@UBOT> status", Timestamp: "1700000003.000000", UserID: "U2"},
		{Text: "summary must not copy", Timestamp: "1700000004.000000", UserID: "UBOT"},
		{Text: "<@UBOT> 123", Timestamp: key.ThreadTS, UserID: "U1"},
	}
	ctx := context.Background()
	lease, err := db.Claim(ctx, "worker", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	w.process(ctx, "worker", lease)
	thread, err := db.Get(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if thread.NoteID != "note-1" || thread.LastSuccess.IsZero() || thread.PartialError == "" || thread.State != "up-to-date" {
		t.Fatalf("unexpected state: %+v", thread)
	}
	if hubspot.creates != 1 || hubspot.uploads != 1 || len(hubspot.fileIDs) != 1 {
		t.Fatalf("remote operations: creates=%d uploads=%d files=%v", hubspot.creates, hubspot.uploads, hubspot.fileIDs)
	}
	if !strings.Contains(hubspot.body, key.Marker()) || !strings.Contains(hubspot.body, "attachment_size_limit") || strings.Contains(hubspot.body, "<script>") || strings.Contains(hubspot.body, "summary must not copy") || strings.Contains(hubspot.body, "status") {
		t.Fatalf("unsafe or incomplete transcript: %s", hubspot.body)
	}
	if strings.Index(hubspot.body, "U1") > strings.Index(hubspot.body, "U2") {
		t.Fatal("transcript is not chronological")
	}
	if len(slack.reactions) != 2 {
		t.Fatalf("reactions: %v", slack.reactions)
	}
	entries, err := os.ReadDir(w.options.TempDir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("temporary files were retained: %v %v", entries, err)
	}
	// Rebuilding an unchanged thread updates the same note and uses the upload map.
	slack.messages[0].Files = []domain.File{slack.files["F1"]}
	if _, err = db.Record(ctx, domain.Event{Command: "sync", ID: "sync", Key: key, Kind: "command", MessageTS: "1700000005.000000", Now: time.Now()}); err != nil {
		t.Fatal(err)
	}
	lease, err = db.Claim(ctx, "worker", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	w.process(ctx, "worker", lease)
	if hubspot.creates != 1 || hubspot.uploads != 1 || hubspot.updates != 2 {
		t.Fatalf("mappings not reused: %+v", hubspot)
	}
}

func TestUntrackInvalidatesInFlightWorkAndResumeBackfillsExistingNote(t *testing.T) {
	w, db, slack, hubspot, key := setup(t)
	ctx := context.Background()
	slack.messages = []domain.Message{{Text: "root", Timestamp: key.ThreadTS, UserID: "U1"}}
	lease, err := db.Claim(ctx, "worker", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err = db.SaveNote(ctx, lease, "existing-note", time.Now()); err != nil {
		t.Fatal(err)
	}
	slack.onThread = func() {
		_, err := db.Record(ctx, domain.Event{Actor: "U2", Command: "untrack", ID: "stop", Key: key, Kind: "command", MessageTS: "1700000001.000000", Now: time.Now()})
		if err != nil {
			t.Fatal(err)
		}
	}
	w.process(ctx, "worker", lease)
	thread, _ := db.Get(ctx, key)
	if thread.Active || thread.NoteID != "existing-note" || hubspot.creates != 0 || hubspot.updates != 0 {
		t.Fatal("invalidated worker wrote remote content or lost mapping")
	}
	if err = db.Complete(ctx, lease, domain.Completion{}, time.Now()); !errors.Is(err, domain.ErrLeaseLost) {
		t.Fatalf("old completion: %v", err)
	}
	slack.onThread = nil
	slack.messages = append(slack.messages, domain.Message{Text: "posted while stopped", Timestamp: "1700000002.000000", UserID: "U2"})
	if _, err = db.Record(ctx, domain.Event{Actor: "U2", Command: "resume", ID: "resume", Key: key, Kind: "command", MessageTS: "1700000003.000000", Now: time.Now()}); err != nil {
		t.Fatal(err)
	}
	lease, err = db.Claim(ctx, "worker", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	w.process(ctx, "worker", lease)
	if hubspot.creates != 0 || hubspot.updates != 1 || !strings.Contains(hubspot.body, "posted while stopped") {
		t.Fatal("resume did not backfill existing note")
	}
}

func TestEventsDuringWriteRemainPendingAndRemoteNoteIsRecovered(t *testing.T) {
	w, db, slack, hubspot, key := setup(t)
	ctx := context.Background()
	hubspot.noteID = "recovered-note"
	slack.messages = []domain.Message{{Text: "root", Timestamp: key.ThreadTS, UserID: "U1"}}
	hubspot.onUpdate = func() {
		_, err := db.Record(ctx, domain.Event{ID: "during-write", Key: key, Kind: "message", MessageTS: "1700000001.000000", Now: time.Now()})
		if err != nil {
			t.Fatal(err)
		}
	}
	lease, err := db.Claim(ctx, "worker", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	w.process(ctx, "worker", lease)
	thread, _ := db.Get(ctx, key)
	if thread.State != "pending" || thread.CompletedGeneration != 1 || thread.RequestedGeneration != 2 || thread.NoteID != "recovered-note" || hubspot.creates != 0 {
		t.Fatalf("lost event or created duplicate note: %+v", thread)
	}
}

func TestTransientAttachmentFailureRetriesAfterTextWasSynced(t *testing.T) {
	w, db, slack, hubspot, key := setup(t)
	hubspot.uploadErr = &domain.RemoteError{Code: "ratelimited", RetryAfter: time.Minute, Service: "hubspot", Temporary: true}
	slack.messages = []domain.Message{{Files: []domain.File{slack.files["F1"]}, Text: "important text", Timestamp: key.ThreadTS, UserID: "U1"}}
	ctx := context.Background()
	lease, err := db.Claim(ctx, "worker", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	w.process(ctx, "worker", lease)
	thread, _ := db.Get(ctx, key)
	if thread.State != "retrying" || thread.PartialError == "" || thread.CurrentError != "" || thread.LastSuccess.IsZero() || thread.NextAttempt.Before(time.Now().Add(50*time.Second)) || !strings.Contains(hubspot.body, "important text") {
		t.Fatalf("partial failure did not preserve text or retry: %+v", thread)
	}
}

func TestStoppingOneThreadReleasesSharedFileForAnother(t *testing.T) {
	w, db, slack, hubspot, key := setup(t)
	w.options.MaxAttempts = 1 // Cancellation must not exhaust the shared file.
	ctx := context.Background()
	slack.messages = []domain.Message{{Files: []domain.File{slack.files["F1"]}, Text: "root", Timestamp: key.ThreadTS, UserID: "U1"}}
	hubspot.onFindFile = func() {
		_, err := db.Record(ctx, domain.Event{Command: "untrack", ID: "stop-upload", Key: key, Kind: "command", MessageTS: "1700000001.000000", Now: time.Now()})
		if err != nil {
			t.Fatal(err)
		}
	}
	lease, err := db.Claim(ctx, "worker", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	w.process(ctx, "worker", lease)
	other := domain.ThreadKey{ChannelID: "G2", ThreadTS: key.ThreadTS, WorkspaceID: key.WorkspaceID}
	if _, err = db.Record(ctx, domain.Event{ID: "other-link", Key: other, Kind: "link", MessageTS: other.ThreadTS, Now: time.Now(), TicketID: "456"}); err != nil {
		t.Fatal(err)
	}
	otherLease, err := db.Claim(ctx, "other-worker", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	upload, err := db.ClaimUpload(ctx, otherLease, "F1", time.Now(), time.Minute)
	if err != nil || upload.LeaseToken == "" {
		t.Fatalf("stopped thread poisoned shared file: %+v %v", upload, err)
	}
	if hubspot.uploads != 0 || hubspot.creates != 0 {
		t.Fatal("worker wrote remote content after untrack")
	}
}

func TestShutdownDuringNotificationDoesNotClaimAnotherThread(t *testing.T) {
	w, db, slack, hubspot, key := setup(t)
	stop := make(chan struct{})
	slack.onPost = func() { close(stop) }
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	w.Run(ctx, stop)
	thread, err := db.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if thread.LeaseToken != "" || thread.State != "pending" || hubspot.creates != 0 {
		t.Fatalf("new work was claimed after shutdown: %+v", thread)
	}
}

func TestReconciliationSkipsSuccessfulReactionsFromEarlierAttempts(t *testing.T) {
	w, db, slack, _, key := setup(t)
	slack.messages = []domain.Message{
		{BotReactions: []string{"white_check_mark"}, Text: "already synchronized", Timestamp: key.ThreadTS, UserID: "U1"},
		{BotReactions: []string{"eyes"}, Text: "new source", Timestamp: "1700000001.000000", UserID: "U2"},
	}
	lease, err := db.Claim(context.Background(), "worker", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	w.process(context.Background(), "worker", lease)
	if len(slack.reactions) != 1 || slack.reactions[0] != "1700000001.000000" {
		t.Fatalf("replayed completed reactions: %v", slack.reactions)
	}
}
