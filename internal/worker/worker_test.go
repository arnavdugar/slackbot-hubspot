package worker

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"slackhubspot/internal/domain"
	"slackhubspot/internal/store"
)

type fakeSlack struct {
	domain.Slack
	files        map[string]domain.File
	messages     []domain.Message
	onReact      func(reactionCall) error
	onThread     func()
	onPost       func()
	posts        int
	postedBlocks []domain.SlackBlock
	reactions    []reactionCall
}

type reactionCall struct {
	operation string
	timestamp string
	name      string
}

func (s *fakeSlack) File(_ context.Context, id string) (domain.File, error) { return s.files[id], nil }

func (s *fakeSlack) Download(context.Context, domain.File) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("file")), nil
}

func (s *fakeSlack) Post(_ context.Context, _ domain.ThreadKey, _, _ string, blocks ...domain.SlackBlock) error {
	s.postedBlocks = blocks
	s.posts++
	if s.onPost != nil {
		s.onPost()
	}
	return nil
}

func (s *fakeSlack) React(_ context.Context, _ domain.ThreadKey, timestamp, name string) error {
	return s.react("add", timestamp, name)
}

func (s *fakeSlack) RemoveReaction(_ context.Context, _ domain.ThreadKey, timestamp, name string) error {
	return s.react("remove", timestamp, name)
}

func (s *fakeSlack) react(operation, timestamp, name string) error {
	call := reactionCall{operation, timestamp, name}
	s.reactions = append(s.reactions, call)
	if s.onReact != nil {
		if err := s.onReact(call); err != nil {
			return err
		}
	}
	for i := range s.messages {
		message := &s.messages[i]
		if message.Timestamp != timestamp {
			continue
		}
		reactions := slices.Clone(message.BotReactions)
		if operation == "add" {
			if !slices.Contains(reactions, name) {
				reactions = append(reactions, name)
			}
		} else {
			reactions = slices.DeleteFunc(reactions, func(reaction string) bool { return reaction == name })
		}
		message.BotReactions = reactions
	}
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
	updateErr  error
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
	return h.updateErr
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

func TestReconciliationPreservesTextAndOtherAttachmentsOnPermanentFailure(t *testing.T) {
	w, db, slack, hubspot, key := setup(t)
	slack.messages = []domain.Message{{Files: []domain.File{slack.files["F1"], slack.files["F2"]}, Text: "important text", Timestamp: key.ThreadTS, UserID: "U1"}}
	lease := claimWorker(t, db)

	w.process(context.Background(), "worker", lease)

	thread := workerThread(t, db, key)
	if thread.LastSuccess.IsZero() || thread.PartialError == "" || thread.State != "up-to-date" || thread.CurrentError != "" {
		t.Fatalf("partial completion = %+v", thread)
	}
	if hubspot.creates != 1 || hubspot.uploads != 1 || !slices.Equal(hubspot.fileIDs, []string{"hub-file-1"}) {
		t.Fatalf("remote operations = %+v", hubspot)
	}
	if !strings.Contains(hubspot.body, "important text") || !strings.Contains(hubspot.body, "attachment_size_limit") {
		t.Fatalf("text or failure missing: %s", hubspot.body)
	}
}

func TestReconciliationFiltersIneligibleMessages(t *testing.T) {
	w, db, slack, hubspot, key := setup(t)
	slack.messages = []domain.Message{
		{Text: "ordinary content", Timestamp: key.ThreadTS, UserID: "U1"},
		{Text: "<@UBOT> status", Timestamp: "1700000001.000000", UserID: "U2"},
		{Text: "summary must not copy", Timestamp: "1700000002.000000", UserID: "UBOT"},
	}
	lease := claimWorker(t, db)

	w.process(context.Background(), "worker", lease)

	if !strings.Contains(hubspot.body, "ordinary content") || strings.Contains(hubspot.body, "summary must not copy") || strings.Contains(hubspot.body, "status") {
		t.Fatalf("incorrect transcript filtering: %s", hubspot.body)
	}
}

func TestReconciliationOrdersEligibleMessages(t *testing.T) {
	w, db, slack, hubspot, key := setup(t)
	slack.messages = []domain.Message{
		{Text: "later message", Timestamp: "1700000001.000000", UserID: "U2"},
		{Text: "earlier message", Timestamp: key.ThreadTS, UserID: "U1"},
	}
	lease := claimWorker(t, db)

	w.process(context.Background(), "worker", lease)

	first, second := strings.Index(hubspot.body, "earlier message"), strings.Index(hubspot.body, "later message")
	if first < 0 || second < 0 || first >= second {
		t.Fatalf("missing or unsorted transcript: %s", hubspot.body)
	}
}

func TestReconciliationUsesEscapedMarkedTranscript(t *testing.T) {
	w, db, slack, hubspot, key := setup(t)
	slack.messages = []domain.Message{{Text: "<script>text</script>", Timestamp: key.ThreadTS, UserID: "U1"}}
	lease := claimWorker(t, db)

	w.process(context.Background(), "worker", lease)

	if !strings.Contains(hubspot.body, key.Marker()) || !strings.Contains(hubspot.body, "&lt;script&gt;") || strings.Contains(hubspot.body, "<script>") {
		t.Fatalf("unsafe or unmarked transcript: %s", hubspot.body)
	}
}

func TestResyncReusesNoteAndUploadMappings(t *testing.T) {
	w, db, slack, hubspot, key := setup(t)
	slack.messages = []domain.Message{{Files: []domain.File{slack.files["F1"]}, Text: "root", Timestamp: key.ThreadTS, UserID: "U1"}}
	completeCoreWork(t, w)
	recordWorkerEvent(t, db, key, domain.Event{Command: "sync", ID: "sync", Kind: "command", MessageTS: "1700000001.000000"})
	lease := claimWorker(t, db)

	w.process(context.Background(), "worker", lease)

	if hubspot.creates != 1 || hubspot.uploads != 1 || hubspot.updates != 2 {
		t.Fatalf("mappings not reused: %+v", hubspot)
	}
}

func TestAttachmentStagingFileIsRemoved(t *testing.T) {
	for _, tc := range []struct {
		name        string
		uploadError error
	}{
		{"successful upload", nil},
		{"failed upload", &domain.RemoteError{Code: "missing_scope", Service: "hubspot"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, db, slack, hubspot, key := setup(t)
			hubspot.uploadErr = tc.uploadError
			slack.messages = []domain.Message{{Files: []domain.File{slack.files["F1"]}, Text: "root", Timestamp: key.ThreadTS, UserID: "U1"}}
			lease := claimWorker(t, db)

			w.process(context.Background(), "worker", lease)

			if hubspot.uploads != 1 {
				t.Fatalf("uploads = %d, want 1", hubspot.uploads)
			}
			entries, err := os.ReadDir(w.options.TempDir)
			if err != nil || len(entries) != 0 {
				t.Fatalf("staging files retained: %v, %v", entries, err)
			}
		})
	}
}

func claimWorker(t *testing.T, db domain.Store) domain.Lease {
	t.Helper()
	lease, err := db.Claim(context.Background(), "worker", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return lease
}

func TestUntrackPreventsInFlightRemoteWrite(t *testing.T) {
	w, db, slack, hubspot, key := setup(t)
	slack.messages = []domain.Message{{Text: "root", Timestamp: key.ThreadTS, UserID: "U1"}}
	lease := claimWorker(t, db)
	if err := db.SaveNote(context.Background(), lease, "existing-note", time.Now()); err != nil {
		t.Fatal(err)
	}
	slack.onThread = func() {
		recordWorkerEvent(t, db, key, domain.Event{Actor: "U2", Command: "untrack", ID: "stop", Kind: "command", MessageTS: "1700000001.000000"})
	}

	w.process(context.Background(), "worker", lease)

	thread := workerThread(t, db, key)
	if thread.Active || thread.NoteID != "existing-note" || hubspot.creates != 0 || hubspot.updates != 0 {
		t.Fatalf("invalidated worker wrote content or lost mapping: %+v", thread)
	}
}

func TestResumeBackfillsExistingNote(t *testing.T) {
	w, db, slack, hubspot, key := setup(t)
	lease := claimWorker(t, db)
	if err := db.SaveNote(context.Background(), lease, "existing-note", time.Now()); err != nil {
		t.Fatal(err)
	}
	recordWorkerEvent(t, db, key, domain.Event{Actor: "U2", Command: "untrack", ID: "stop", Kind: "command", MessageTS: "1700000001.000000"})
	slack.messages = []domain.Message{
		{Text: "root", Timestamp: key.ThreadTS, UserID: "U1"},
		{Text: "posted while stopped", Timestamp: "1700000002.000000", UserID: "U2"},
	}
	recordWorkerEvent(t, db, key, domain.Event{Actor: "U2", Command: "resume", ID: "resume", Kind: "command", MessageTS: "1700000003.000000"})
	fresh := claimWorker(t, db)

	w.process(context.Background(), "worker", fresh)

	if hubspot.creates != 0 || hubspot.updates != 1 || !strings.Contains(hubspot.body, "posted while stopped") {
		t.Fatal("resume did not backfill existing note")
	}
}

func TestEventsDuringWriteRemainPending(t *testing.T) {
	w, db, slack, hubspot, key := setup(t)
	slack.messages = []domain.Message{{Text: "root", Timestamp: key.ThreadTS, UserID: "U1"}}
	lease := claimWorker(t, db)
	if err := db.SaveNote(context.Background(), lease, "existing-note", time.Now()); err != nil {
		t.Fatal(err)
	}
	lease.Thread.NoteID = "existing-note"
	hubspot.onUpdate = func() {
		recordWorkerEvent(t, db, key, domain.Event{ID: "during-write", Kind: "message", MessageTS: "1700000001.000000"})
	}

	w.process(context.Background(), "worker", lease)

	thread := workerThread(t, db, key)
	if thread.State != "pending" || thread.CompletedGeneration != 1 || thread.RequestedGeneration != 2 {
		t.Fatalf("event during write lost: %+v", thread)
	}
}

func TestReconciliationRecoversRemoteNoteMapping(t *testing.T) {
	w, db, slack, hubspot, key := setup(t)
	hubspot.noteID = "recovered-note"
	slack.messages = []domain.Message{{Text: "root", Timestamp: key.ThreadTS, UserID: "U1"}}
	lease := claimWorker(t, db)

	w.process(context.Background(), "worker", lease)

	thread := workerThread(t, db, key)
	if thread.NoteID != "recovered-note" || hubspot.creates != 0 || hubspot.updates != 1 {
		t.Fatalf("recovery created a duplicate note or lost mapping: %+v", thread)
	}
}

func TestRunProcessesPendingThread(t *testing.T) {
	w, db, slack, hubspot, key := setup(t)
	slack.messages = []domain.Message{{Text: "root", Timestamp: key.ThreadTS, UserID: "U1"}}
	stop := make(chan struct{})
	hubspot.onUpdate = func() { close(stop) }
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	w.Run(ctx, stop)

	thread := workerThread(t, db, key)
	if ctx.Err() != nil || thread.State != "up-to-date" || thread.NoteID != "note-1" || hubspot.updates != 1 {
		t.Fatalf("run loop did not process work: %+v, context=%v", thread, ctx.Err())
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
	if slack.posts != 1 || ctx.Err() != nil {
		t.Fatalf("notification did not trigger shutdown: posts=%d context=%v", slack.posts, ctx.Err())
	}
	select {
	case <-stop:
	default:
		t.Fatal("notification did not close the stop signal")
	}
	thread, err := db.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if thread.LeaseToken != "" || thread.State != "pending" || hubspot.creates != 0 {
		t.Fatalf("new work was claimed after shutdown: %+v", thread)
	}
}

func TestReconciliationKeepsOnlyLatestSuccessReaction(t *testing.T) {
	const root, middle, latest = "1700000000.000001", "1700000001.000000", "1700000002.000000"
	for _, tc := range []struct {
		name     string
		reaction string
		messages []domain.Message
		want     []reactionCall
	}{
		{
			name: "root only", reaction: "white_check_mark",
			messages: []domain.Message{{Text: "root", Timestamp: root, UserID: "U1"}},
			want:     []reactionCall{{operation: "add", timestamp: root, name: "white_check_mark"}},
		},
		{
			name: "latest by timestamp", reaction: "white_check_mark",
			messages: []domain.Message{
				{Text: "latest", Timestamp: latest, UserID: "U2"},
				{Text: "root", Timestamp: root, UserID: "U1"},
				{Text: "middle", Timestamp: middle, UserID: "U2"},
			},
			want: []reactionCall{{operation: "add", timestamp: latest, name: "white_check_mark"}},
		},
		{
			name: "remove all older markers before adding", reaction: "white_check_mark",
			messages: []domain.Message{
				{BotReactions: []string{"white_check_mark", "eyes"}, Text: "root", Timestamp: root, UserID: "U1"},
				{BotReactions: []string{"white_check_mark"}, Text: "middle", Timestamp: middle, UserID: "U2"},
				{Text: "latest", Timestamp: latest, UserID: "U2"},
			},
			want: []reactionCall{{operation: "remove", timestamp: root, name: "white_check_mark"}, {operation: "remove", timestamp: root, name: "eyes"}, {operation: "remove", timestamp: middle, name: "white_check_mark"}, {operation: "add", timestamp: latest, name: "white_check_mark"}},
		},
		{
			name: "latest already marked", reaction: "white_check_mark",
			messages: []domain.Message{
				{BotReactions: []string{"white_check_mark"}, Text: "root", Timestamp: root, UserID: "U1"},
				{BotReactions: []string{"white_check_mark"}, Text: "latest", Timestamp: latest, UserID: "U2"},
			},
			want: []reactionCall{{operation: "remove", timestamp: root, name: "white_check_mark"}},
		},
		{
			name: "excluded messages cannot keep or receive marker", reaction: "white_check_mark",
			messages: []domain.Message{
				{Text: "root", Timestamp: root, UserID: "U1"},
				{BotReactions: []string{"white_check_mark"}, Text: "<@UBOT> sync", Timestamp: middle, UserID: "U2"},
				{Text: "sync queued", Timestamp: latest, UserID: "UBOT"},
			},
			want: []reactionCall{{operation: "remove", timestamp: middle, name: "white_check_mark"}, {operation: "add", timestamp: root, name: "white_check_mark"}},
		},
		{
			name: "configured success reaction", reaction: "heavy_check_mark",
			messages: []domain.Message{
				{BotReactions: []string{"heavy_check_mark", "eyes"}, Text: "root", Timestamp: root, UserID: "U1"},
				{BotReactions: []string{"warning"}, Text: "latest", Timestamp: latest, UserID: "U2"},
			},
			want: []reactionCall{{operation: "remove", timestamp: root, name: "heavy_check_mark"}, {operation: "remove", timestamp: root, name: "eyes"}, {operation: "remove", timestamp: latest, name: "warning"}, {operation: "add", timestamp: latest, name: "heavy_check_mark"}},
		},
		{
			name: "reactions disabled",
			messages: []domain.Message{
				{BotReactions: []string{"white_check_mark"}, Text: "root", Timestamp: root, UserID: "U1"},
				{Text: "latest", Timestamp: latest, UserID: "U2"},
			},
			want: []reactionCall{{operation: "remove", timestamp: root, name: "white_check_mark"}},
		},
		{name: "empty thread", reaction: "white_check_mark"},
		{
			name: "no eligible messages", reaction: "white_check_mark",
			messages: []domain.Message{{Text: "<@UBOT> sync", Timestamp: latest, UserID: "U2"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, _, slack, _, _ := setup(t)
			w.options.SuccessReaction = tc.reaction
			slack.messages = tc.messages

			completeCoreWork(t, w)
			deliverAllReactions(t, w)

			if !slices.Equal(slack.reactions, tc.want) {
				t.Fatalf("reaction calls: got %v, want %v", slack.reactions, tc.want)
			}
		})
	}
}

func TestNewWorkMovesSuccessReaction(t *testing.T) {
	const latest = "1700000001.000000"
	for _, tc := range []struct {
		name  string
		event domain.Event
	}{
		{name: "new reply", event: domain.Event{ID: "reply", Kind: "message", MessageTS: latest}},
		{name: "explicit sync", event: domain.Event{ID: "sync", Kind: "command", Command: "sync", MessageTS: "1700000002.000000"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, db, slack, _, key := syncedReactionThread(t)
			slack.messages = append(slack.messages, domain.Message{Text: "latest reply", Timestamp: latest, UserID: "U2"})
			slack.reactions = nil
			want := []reactionCall{
				{operation: "remove", timestamp: key.ThreadTS, name: "white_check_mark"},
				{operation: "add", timestamp: latest, name: "white_check_mark"},
			}

			recordWorkerEvent(t, db, key, tc.event)
			completeCoreWork(t, w)
			deliverAllReactions(t, w)

			if !slices.Equal(slack.reactions, want) {
				t.Errorf("calls = %v, want %v", slack.reactions, want)
			}
		})
	}
}

func TestNotificationDeliversPersistedBlocks(t *testing.T) {
	w, db, slack, _, key := setup(t)
	deliverReplies(t, w)
	blocks := []domain.SlackBlock{{Type: "section", Text: &domain.SlackText{Type: "mrkdwn", Text: "*Help*"}}}
	_, err := db.Record(context.Background(), domain.Event{
		ID: "block-help", Key: key, Kind: "command", Command: "help",
		MessageTS: "1700000003.000000", Now: time.Now(), Reply: "Help", ReplyBlocks: blocks,
	})
	if err != nil {
		t.Fatal(err)
	}

	deliverReplies(t, w)

	if !reflect.DeepEqual(slack.postedBlocks, blocks) {
		t.Fatalf("worker lost persisted blocks: %+v", slack.postedBlocks)
	}
}
