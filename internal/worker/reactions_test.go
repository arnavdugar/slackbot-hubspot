package worker

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"slackhubspot/internal/domain"
)

func TestUntrackCleansReactionsAfterConfirmationFailsPermanently(t *testing.T) {
	w, db, slack, _, key := syncedReactionThread(t)
	recordWorkerEvent(t, db, key, domain.Event{ID: "stop", Kind: "command", Command: "untrack", MessageTS: "1700000002.000000"})
	notification, err := db.ClaimNotification(context.Background(), "notifier", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	if err := db.FailNotification(context.Background(), notification, domain.Failure{Permanent: true}, time.Now()); err != nil {
		t.Fatal(err)
	}
	deliverAllReactions(t, w)

	if got := slack.messages[0].BotReactions; !slices.Equal(got, []string{"white_check_mark"}) {
		t.Errorf("reactions = %v, want only the success marker", got)
	}
}

func deliverReplies(t *testing.T, w *Worker) {
	t.Helper()
	for range 100 {
		err := w.deliver(context.Background(), "notifier")
		if errors.Is(err, domain.ErrNoWork) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("reply outbox did not drain")
}

func deliverAllReactions(t *testing.T, w *Worker) {
	t.Helper()
	for range 100 {
		err := w.deliverReactions(context.Background(), "notifier")
		if errors.Is(err, domain.ErrNoWork) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	t.Fatal("reaction outbox did not drain")
}

func completeCoreWork(t *testing.T, w *Worker) {
	t.Helper()
	deliverReplies(t, w)
	ctx := context.Background()
	lease, err := w.options.Store.Claim(ctx, "worker", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	w.process(ctx, "worker", lease)
}

func TestReactionFailurePreservesCompletedSync(t *testing.T) {
	for _, tc := range []struct{ name, emoji string }{
		{name: "tracking marker fails", emoji: "eyes"},
		{name: "success marker fails", emoji: "white_check_mark"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, db, slack, _, key := setup(t)
			w.options.TrackingReaction = "eyes"
			slack.messages = []domain.Message{{Text: "root", Timestamp: key.ThreadTS, UserID: "U1"}}
			completeCoreWork(t, w)
			before := workerThread(t, db, key)
			failure := &domain.RemoteError{Code: "api_error", Service: "slack"}
			slack.onReact = func(call reactionCall) error {
				if call.name == tc.emoji {
					return failure
				}
				return nil
			}

			err := w.deliverReactions(context.Background(), "notifier")

			if !errors.Is(err, failure) {
				t.Fatalf("error = %v, want %v", err, failure)
			}
			if after := workerThread(t, db, key); !reflect.DeepEqual(after, before) {
				t.Errorf("reaction failure changed sync state: before=%+v, after=%+v", before, after)
			}
		})
	}
}

func TestReactionCleanupRetriesWithoutRepeatingCoreWork(t *testing.T) {
	w, _, slack, hubspot, key := setup(t)
	w.options.TrackingReaction = "eyes"
	w.options.BackoffBase, w.options.BackoffMax = time.Nanosecond, time.Nanosecond
	const middle, latest = "1700000001.000000", "1700000002.000000"
	slack.messages = []domain.Message{
		{BotReactions: []string{"white_check_mark", "eyes"}, Text: "root", Timestamp: key.ThreadTS, UserID: "U1"},
		{BotReactions: []string{"white_check_mark"}, Text: "middle", Timestamp: middle, UserID: "U2"},
		{Text: "latest", Timestamp: latest, UserID: "U2"},
	}
	completeCoreWork(t, w)
	failure := &domain.RemoteError{Code: "service_unavailable", Service: "slack", Temporary: true}
	slack.onReact = func(call reactionCall) error {
		if call.operation == "remove" && call.timestamp == middle {
			return failure
		}
		return nil
	}
	want := []reactionCall{
		{operation: "remove", timestamp: key.ThreadTS, name: "white_check_mark"},
		{operation: "remove", timestamp: middle, name: "white_check_mark"},
		{operation: "remove", timestamp: middle, name: "white_check_mark"},
		{operation: "add", timestamp: latest, name: "white_check_mark"},
	}

	err := w.deliverReactions(context.Background(), "notifier")
	slack.onReact = nil
	deliverAllReactions(t, w)

	if !errors.Is(err, failure) {
		t.Errorf("initial error = %v, want %v", err, failure)
	}
	if !slices.Equal(slack.reactions, want) {
		t.Errorf("calls = %v, want %v", slack.reactions, want)
	}
	if hubspot.updates != 1 {
		t.Errorf("HubSpot writes = %d, want 1", hubspot.updates)
	}
	if slack.posts != 1 {
		t.Errorf("Slack replies = %d, want 1", slack.posts)
	}
}

func TestTrackingReplacementRemovesUnwantedReactionsFirst(t *testing.T) {
	const root, botReply, latest = "1700000000.000001", "1700000001.000000", "1700000002.000000"
	for _, tc := range []struct {
		name, replacement string
		want              []reactionCall
	}{
		{name: "replace tracking emoji", replacement: "pushpin", want: []reactionCall{
			{operation: "remove", timestamp: root, name: "eyes"},
			{operation: "remove", timestamp: root, name: "bookmark"},
			{operation: "remove", timestamp: botReply, name: "heart"},
			{operation: "add", timestamp: latest, name: "white_check_mark"},
			{operation: "add", timestamp: root, name: "pushpin"},
		}},
		{name: "disable tracking emoji", want: []reactionCall{
			{operation: "remove", timestamp: root, name: "eyes"},
			{operation: "remove", timestamp: root, name: "bookmark"},
			{operation: "remove", timestamp: botReply, name: "heart"},
			{operation: "add", timestamp: latest, name: "white_check_mark"},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, _, slack, _, _ := setup(t)
			w.options.TrackingReaction = tc.replacement
			slack.messages = []domain.Message{
				{BotReactions: []string{"eyes", "bookmark"}, Text: "root", Timestamp: root, UserID: "U1"},
				{BotReactions: []string{"heart"}, Text: "bot reply", Timestamp: botReply, UserID: "UBOT"},
				{Text: "latest", Timestamp: latest, UserID: "U2"},
			}
			completeCoreWork(t, w)

			deliverAllReactions(t, w)

			if !slices.Equal(slack.reactions, tc.want) {
				t.Errorf("calls = %v, want %v", slack.reactions, tc.want)
			}
		})
	}
}

func TestUntrackCleansPreviousTrackingEmojiAfterConfigurationChange(t *testing.T) {
	for _, tc := range []struct{ name, replacement string }{
		{name: "changed emoji", replacement: "pushpin"},
		{name: "disabled emoji"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, db, slack, _, key := syncedReactionThread(t)
			w.options.TrackingReaction = tc.replacement

			recordWorkerEvent(t, db, key, domain.Event{ID: "stop", Kind: "command", Command: "untrack", MessageTS: "1700000001.000000"})
			deliverReplies(t, w)
			deliverAllReactions(t, w)

			if got := slack.messages[0].BotReactions; !slices.Equal(got, []string{"white_check_mark"}) {
				t.Errorf("reactions = %v, want only success", got)
			}
		})
	}
}

func TestResumeRestoresTrackingReaction(t *testing.T) {
	w, db, slack, _, key := syncedReactionThread(t)
	recordWorkerEvent(t, db, key, domain.Event{ID: "stop", Kind: "command", Command: "untrack", MessageTS: "1700000001.000000"})
	deliverReplies(t, w)
	deliverAllReactions(t, w)

	recordWorkerEvent(t, db, key, domain.Event{ID: "resume", Kind: "command", Command: "resume", MessageTS: "1700000002.000000"})
	completeCoreWork(t, w)
	deliverAllReactions(t, w)

	if got := slack.messages[0].BotReactions; !slices.Equal(got, []string{"white_check_mark", "eyes"}) {
		t.Errorf("reactions = %v, want success and tracking", got)
	}
}

func TestCanceledReactionIsRetriedAfterUntrack(t *testing.T) {
	w, db, slack, hubspot, key := setup(t)
	w.options.TrackingReaction = "eyes"
	slack.messages = []domain.Message{{Text: "root", Timestamp: key.ThreadTS, UserID: "U1"}}
	completeCoreWork(t, w)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slack.onReact = func(call reactionCall) error {
		if call.operation != "add" || call.name != "eyes" {
			return nil
		}
		slack.onReact = nil
		_, err := db.Record(context.Background(), domain.Event{Command: "untrack", ID: "stop-during-add", Key: key, Kind: "command", MessageTS: "1700000001.000000", Now: time.Now()})
		if err != nil {
			return err
		}
		deliverReplies(t, w)
		cancel() // The already-issued add still completes in Slack.
		return nil
	}

	err := w.deliverReactions(ctx, "notifier")
	deliverAllReactions(t, w)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("unexpected cancellation result: %v", err)
	}
	if thread := workerThread(t, db, key); thread.Active {
		t.Error("thread is still active")
	}
	if got := slack.messages[0].BotReactions; !slices.Equal(got, []string{"white_check_mark"}) {
		t.Errorf("reactions = %v, want only success", got)
	}
	if hubspot.updates != 1 {
		t.Errorf("HubSpot writes = %d, want 1", hubspot.updates)
	}
}

func failedReactionThread(t *testing.T) (*Worker, domain.Store, *fakeSlack, *fakeHubSpot, domain.ThreadKey) {
	t.Helper()
	w, db, slack, hubspot, key := setup(t)
	w.options.TrackingReaction, w.options.FailureReaction = "eyes", "warning"
	slack.messages = []domain.Message{{Text: "root", Timestamp: key.ThreadTS, UserID: "U1"}}
	hubspot.updateErr = &domain.RemoteError{Code: "note_too_large", Service: "hubspot"}
	completeCoreWork(t, w)
	return w, db, slack, hubspot, key
}

func TestCoreFailureGetsFailureReaction(t *testing.T) {
	w, _, slack, _, _ := failedReactionThread(t)

	deliverAllReactions(t, w)

	if !slices.Contains(slack.messages[0].BotReactions, "warning") {
		t.Fatal("core failure did not get a failure reaction")
	}
}

func TestSuccessfulSyncCleansFailureReaction(t *testing.T) {
	w, db, slack, hubspot, key := failedReactionThread(t)
	deliverAllReactions(t, w)
	if !slices.Contains(slack.messages[0].BotReactions, "warning") {
		t.Fatal("fixture has no failure marker")
	}
	hubspot.updateErr = nil
	recordWorkerEvent(t, db, key, domain.Event{Command: "sync", ID: "retry", Kind: "command", MessageTS: "1700000001.000000"})

	completeCoreWork(t, w)
	deliverAllReactions(t, w)

	if !slices.Equal(slack.messages[0].BotReactions, []string{"eyes", "white_check_mark"}) {
		t.Fatalf("incorrect reactions after recovery: %v", slack.messages[0].BotReactions)
	}
}

func syncedReactionThread(t *testing.T) (*Worker, domain.Store, *fakeSlack, *fakeHubSpot, domain.ThreadKey) {
	t.Helper()
	w, db, slack, hubspot, key := setup(t)
	w.options.TrackingReaction = "eyes"
	slack.messages = []domain.Message{{Text: "root", Timestamp: key.ThreadTS, UserID: "U1"}}
	completeCoreWork(t, w)
	deliverAllReactions(t, w)
	if got := slack.messages[0].BotReactions; !slices.Equal(got, []string{"white_check_mark", "eyes"}) {
		t.Fatalf("setup reactions = %v, want success and tracking", got)
	}
	return w, db, slack, hubspot, key
}

func recordWorkerEvent(t *testing.T, db domain.Store, key domain.ThreadKey, event domain.Event) {
	t.Helper()
	event.Key, event.Now = key, time.Now()
	if _, err := db.Record(context.Background(), event); err != nil {
		t.Fatal(err)
	}
}

func workerThread(t *testing.T, db domain.Store, key domain.ThreadKey) domain.Thread {
	t.Helper()
	thread, err := db.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	return thread
}

func TestCoreWorkDefersReactions(t *testing.T) {
	w, _, slack, _, key := setup(t)
	w.options.TrackingReaction = "eyes"
	slack.messages = []domain.Message{{Text: "root", Timestamp: key.ThreadTS, UserID: "U1"}}

	completeCoreWork(t, w)

	if len(slack.reactions) != 0 {
		t.Errorf("core work made reaction calls: %v", slack.reactions)
	}
}
