package render

import (
	"slackhubspot/internal/domain"
	"strings"
	"testing"
	"time"
)

func TestStatusDisplaysLeaseState(t *testing.T) {
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name   string
		active bool
		state  string
		expiry time.Time
		want   string
	}{
		{"expired lease", true, "running", now.Add(-time.Minute), "pending"},
		{"live lease", true, "running", now.Add(time.Minute), "running"},
		{"stopped", false, "stopped", time.Time{}, "stopped"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			text, blocks := Status(domain.Thread{TicketID: "123", Active: tc.active, State: tc.state, LeaseExpires: tc.expiry}, now)
			if len(blocks) != 3 || !strings.Contains(blocks[1].Text.Text, tc.want) || !strings.Contains(text, "Sync: "+tc.want) {
				t.Fatalf("status = %s, %+v", text, blocks)
			}
			if !strings.Contains(blocks[2].Elements[0].Text, "Last synced never") {
				t.Fatal("missing last sync")
			}
		})
	}
}

func TestStatusIncludesErrorDetails(t *testing.T) {
	now := time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC)
	text, blocks := Status(domain.Thread{Active: true, LastSuccess: now.Add(-time.Hour), NextAttempt: now.Add(time.Minute), CurrentError: "rate limited", PartialError: "attachment <failed>", HistoricalError: "old failure"}, now)
	if len(blocks) != 8 {
		t.Fatalf("blocks = %d", len(blocks))
	}
	if !strings.Contains(blocks[1].Text.Text, "Sync needs attention") {
		t.Fatal("missing warning")
	}
	if got := blocks[2].Elements[0].Text; !strings.Contains(got, "<!date^1790593200^{date_long_pretty} at {time_secs}|Sep 28, 2026 at 11:00:00 UTC>") {
		t.Fatalf("timestamp = %s", got)
	}
	if !strings.Contains(blocks[6].Text.Text, "attachment &lt;failed&gt;") {
		t.Fatal("attachment error not escaped")
	}
	for _, detail := range []string{"Current error: rate limited", "Next retry: 2026-09-28T12:01:00Z", "Attachments needing attention: attachment <failed>", "Historical error: old failure"} {
		if !strings.Contains(text, detail) {
			t.Errorf("missing %s", detail)
		}
	}
}

func TestStatusPartialAttachments(t *testing.T) {
	for _, tc := range []struct{ partial, want string }{
		{"", ":white_check_mark: Messages and attachments synced"},
		{"1 attachment(s) not synchronized", ":warning: Messages synced · 1 attachment failed"},
		{"2 attachment(s) not synchronized", ":warning: Messages synced · 2 attachments failed"},
		{"download failed", ":warning: Messages synced · attachment upload incomplete"},
	} {
		_, blocks := Status(domain.Thread{State: "up-to-date", PartialError: tc.partial, TicketID: "42", TicketURL: "https://example.com/?a=1&b=2"}, time.Now())
		if blocks[1].Text.Text != "*"+tc.want+"*" {
			t.Fatalf("summary = %s", blocks[1].Text.Text)
		}
		if blocks[0].Text.Text != "*<https://example.com/?a=1&amp;b=2|Ticket 42>*" {
			t.Fatal("ticket link")
		}
	}
}

func TestStoppedStatusOmitsRetry(t *testing.T) {
	text, blocks := Status(domain.Thread{State: "stopped", NextAttempt: time.Now()}, time.Now())
	if len(blocks) != 3 || strings.Contains(text, "Next retry") {
		t.Fatal("stopped status advertises retry")
	}
}
