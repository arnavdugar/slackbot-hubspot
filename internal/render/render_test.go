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
	for _, message := range []domain.Message{{UserID: "UBOT", Text: "summary"}, {UserID: "U1", Text: "<@UBOT> untrack"}, {UserID: "U1", Subtype: "channel_join"}} {
		if Eligible(message, "UBOT", "42") {
			t.Fatalf("control/system message eligible: %#v", message)
		}
	}
	if !Eligible(domain.Message{BotID: "BOTHER", Subtype: "bot_message", Text: "Build complete"}, "UBOT", "42") {
		t.Fatal("other bots must be included")
	}
}

func TestSummaryRejectsCodeAndEscapesTicketValues(t *testing.T) {
	for _, source := range []string{"{{.Secret}}", "{{call .Subject}}", "{{if .ID}}yes{{end}}", "{{.ID | printf}}", "{{.ID", "text }}", "text }} {{.ID}}"} {
		if _, err := CompileSummary(source); err == nil {
			t.Errorf("accepted invalid template %q", source)
		}
	}
	summary, err := CompileSummary("*{{.Subject}}* {{.ID}} <{{.URL}}|Ticket>")
	if err != nil {
		t.Fatal(err)
	}
	got := summary.Render(domain.Ticket{ID: "123", Subject: "<@UALL> & <script>", URL: "https://app.hubspot.com/ticket/123"})
	if strings.Contains(got, "<@UALL>") || !strings.Contains(got, "&lt;@UALL&gt;") || !strings.Contains(got, "<https://app.hubspot.com/ticket/123|Ticket>") {
		t.Fatal(got)
	}
}

func TestTranscriptEscapesInputsAndOrdersCompleteEntries(t *testing.T) {
	messages := []domain.Message{
		{Files: []domain.File{{ID: "F1", Name: "<img>.png"}}, Permalink: "javascript:alert(1)", SenderName: "<svg>", Text: "<script>alert(1)</script> <javascript:alert(1)|click> &lt;b&gt;", Timestamp: "1710000002.000002", UserID: "U2"},
		{Permalink: "https://workspace.slack.com/archives/C1/p1", SenderName: "Ada", Text: "Hello <@U2|Grace> *world* <https://example.com/?a=1&amp;b=2|docs>", Timestamp: "1710000001.000001", UserID: "U1"},
	}
	got := Transcript(domain.ThreadKey{ChannelID: "C1", ThreadTS: "1.0", WorkspaceID: "T1"}, messages, map[string]string{"F1": "failed: <size>"})
	for _, unsafe := range []string{"<script>", "<svg>", "<img>", `href="javascript:`} {
		if strings.Contains(got, unsafe) {
			t.Errorf("unsafe HTML %q in %s", unsafe, got)
		}
	}
	for _, want := range []string{"Source: slack-thread:T1:C1:1.0", "@Grace (U2)", "<strong>world</strong>", "&lt;script&gt;", "Attachment: &lt;img&gt;.png — failed: &lt;size&gt;", "Slack message"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in %s", want, got)
		}
	}
	if strings.Index(got, "Ada") > strings.Index(got, "&lt;svg&gt;") {
		t.Fatal("messages are not chronological")
	}
}
