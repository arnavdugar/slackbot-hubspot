package render

import (
	"strings"
	"testing"

	"slackhubspot/internal/domain"
)

func TestParseControlPrecedenceAndAccountBoundary(t *testing.T) {
	for _, test := range []struct {
		text string
		want Result
	}{
		{"<@UBOT> sync", Result{Command: "sync", Mentioned: true}},
		{"<@UBOT> sync https://app.hubspot.com/contacts/42/record/0-5/123", Result{Command: "sync", Invalid: true, Mentioned: true}},
		{"<@UBOT|Bot> 123", Result{Mentioned: true, TicketID: "123"}},
		{"Please <@UBOT> <https://app-eu1.hubspot.com/contacts/42/record/0-5/123|Ticket>", Result{Mentioned: true, TicketID: "123"}},
		{"<@UBOT> https://app.hubspot.com/contacts/99/ticket/123", Result{Invalid: true, Mentioned: true}},
		{"<@UBOT> https://app.hubspot.com.evil.test/contacts/42/ticket/123", Result{Invalid: true, Mentioned: true}},
		{"<@UBOT> https://app.hubspot.com/contacts/42/ticket/123 https://app.hubspot.com/contacts/42/ticket/456", Result{Invalid: true, Mentioned: true}},
		{"<@UBOT> frobnicate", Result{Invalid: true, Mentioned: true}},
		{"<@UBOT> frobnicate https://app.hubspot.com/contacts/42/ticket/123", Result{Invalid: true, Mentioned: true}},
		{"Thanks <@UBOT> for the update", Result{Mentioned: true}},
		{"ordinary message 123", Result{}},
	} {
		t.Run(test.text, func(t *testing.T) {
			if got := Parse(test.text, "UBOT", "42"); got != test.want {
				t.Fatalf("got %#v; want %#v", got, test.want)
			}
		})
	}
}

func TestEligibleFiltersControlAndSystemMessages(t *testing.T) {
	for _, tc := range []struct {
		name    string
		message domain.Message
		want    bool
	}{
		{"own bot", domain.Message{UserID: "UBOT", Text: "summary"}, false},
		{"control command", domain.Message{UserID: "U1", Text: "<@UBOT> untrack"}, false},
		{"system subtype", domain.Message{UserID: "U1", Subtype: "channel_join"}, false},
		{"another bot", domain.Message{BotID: "BOTHER", Subtype: "bot_message", Text: "Build complete"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := Eligible(tc.message, "UBOT", "42")

			if got != tc.want {
				t.Errorf("eligible = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestCompileSummaryRejectsExecutableOrMalformedTemplates(t *testing.T) {
	for _, source := range []string{"{{.Secret}}", "{{call .Subject}}", "{{if .ID}}yes{{end}}", "{{.ID | printf}}", "{{.ID", "text }}", "text }} {{.ID}}"} {
		t.Run(source, func(t *testing.T) {
			_, err := CompileSummary(source)

			if err == nil {
				t.Fatal("invalid template accepted")
			}
		})
	}
}

func TestSummaryEscapesTicketValues(t *testing.T) {
	summary, err := CompileSummary("*{{.Subject}}* {{.ID}} <{{.URL}}|Ticket>")
	if err != nil {
		t.Fatal(err)
	}
	ticket := domain.Ticket{ID: "123", Subject: "<@UALL> & <script>", URL: "https://app.hubspot.com/ticket/123"}

	got := summary.Render(ticket)

	if strings.Contains(got, "<@UALL>") || !strings.Contains(got, "&lt;@UALL&gt;") || !strings.Contains(got, "<https://app.hubspot.com/ticket/123|Ticket>") {
		t.Fatal(got)
	}
}

func TestTranscriptEscapesUntrustedContent(t *testing.T) {
	messages := []domain.Message{{Files: []domain.File{{ID: "F1", Name: "<img>.png"}}, Permalink: "javascript:alert(1)", SenderName: "<svg>", Text: "<script>alert(1)</script> <javascript:alert(1)|click> &lt;b&gt;", Timestamp: "1710000002.000002", UserID: "U2"}}

	got := Transcript(domain.ThreadKey{ChannelID: "C1", ThreadTS: "1.0", WorkspaceID: "T1"}, messages, map[string]string{"F1": "failed: <size>"})

	for _, unsafe := range []string{"<script>", "<svg>", "<img>", `href="javascript:`} {
		if strings.Contains(got, unsafe) {
			t.Errorf("unsafe HTML %q in %s", unsafe, got)
		}
	}
	for _, want := range []string{"&lt;script&gt;", "&lt;svg&gt;", "Attachment: &lt;img&gt;.png — failed: &lt;size&gt;"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in %s", want, got)
		}
	}
}

func TestTranscriptOrdersCompleteEntries(t *testing.T) {
	messages := []domain.Message{
		{SenderName: "Grace", Text: "Later entry", Timestamp: "1710000002.000002", UserID: "U2"},
		{SenderName: "Ada", Text: "Earlier entry", Timestamp: "1710000001.000001", UserID: "U1"},
	}

	got := Transcript(domain.ThreadKey{}, messages, nil)

	first, second := strings.Index(got, "Earlier entry"), strings.Index(got, "Later entry")
	if first < 0 || second < 0 || first >= second {
		t.Fatalf("entries missing or not chronological: %s", got)
	}
}

func TestTranscriptRendersSlackFormatting(t *testing.T) {
	messages := []domain.Message{{Permalink: "https://workspace.slack.com/archives/C1/p1", SenderName: "Ada", Text: "Hello <@U2|Grace> *world* <https://example.com/?a=1&amp;b=2|docs>", Timestamp: "1710000001.000001", UserID: "U1"}}

	got := Transcript(domain.ThreadKey{}, messages, nil)

	for _, want := range []string{"@Grace (U2)", "<strong>world</strong>", "Slack message", `href="https://example.com/?a=1&amp;b=2"`} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in %s", want, got)
		}
	}
}

func TestTranscriptIncludesStableSourceMarker(t *testing.T) {
	key := domain.ThreadKey{ChannelID: "C1", ThreadTS: "1.0", WorkspaceID: "T1"}

	got := Transcript(key, nil, nil)

	if !strings.Contains(got, "Source: slack-thread:T1:C1:1.0") {
		t.Fatal(got)
	}
}

func TestTranscriptRendersMentionWithoutLabel(t *testing.T) {
	messages := []domain.Message{{Text: "Hello <@U2>", Timestamp: "1710000001.000001", UserID: "U1"}}

	got := Transcript(domain.ThreadKey{}, messages, nil)

	if !strings.Contains(got, "Hello @U2") || strings.Contains(got, "<@U2>") {
		t.Fatalf("unlabeled mention rendered incorrectly: %s", got)
	}
}
