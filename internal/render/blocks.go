package render

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"slackhubspot/internal/domain"
)

func blockText(kind, text string) domain.SlackBlock {
	return domain.SlackBlock{Type: "section", Text: &domain.SlackText{Type: kind, Text: text}}
}

func blockHeader(text string) domain.SlackBlock {
	return domain.SlackBlock{Type: "header", Text: &domain.SlackText{Type: "plain_text", Text: text}}
}

func blockField(label, value string) domain.SlackText {
	if value == "" {
		value = "—"
	}
	return domain.SlackText{Type: "plain_text", Text: label + "\n" + value}
}

// Fall back to the full message instead of truncating custom content or sending
// a block that Slack will reject. Section text and fields have separate limits.
func boundedBlocks(blocks []domain.SlackBlock) []domain.SlackBlock {
	for _, block := range blocks {
		if block.Text != nil && (block.Text.Text == "" || utf8.RuneCountInString(block.Text.Text) > 3000) {
			return nil
		}
		for _, field := range block.Fields {
			if utf8.RuneCountInString(field.Text) > 2000 {
				return nil
			}
		}
	}
	return blocks
}

func slackEscape(text string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(text)
}

func Status(thread domain.Thread, now time.Time) (string, []domain.SlackBlock) {
	tracking := "active"
	if !thread.Active {
		tracking = "stopped"
	}
	state := thread.State
	if thread.Active && state == "running" && !thread.LeaseExpires.After(now) {
		state = "pending"
	}
	lastSuccess := "never"
	if !thread.LastSuccess.IsZero() {
		lastSuccess = thread.LastSuccess.UTC().Format(time.RFC3339)
	}
	fallback := fmt.Sprintf("Ticket: %s\nLink: %s\nTracking: %s\nSync: %s\nLast successful sync: %s", thread.TicketID, thread.TicketURL, tracking, state, lastSuccess)
	title := "*Ticket " + slackEscape(thread.TicketID) + "*"
	if thread.TicketURL != "" {
		title = "*<" + slackEscape(thread.TicketURL) + "|Ticket " + slackEscape(thread.TicketID) + ">*"
	}
	summary := "Sync: " + slackEscape(state)
	if state == "up-to-date" {
		summary = ":white_check_mark: Messages and attachments synced"
		if thread.PartialError != "" {
			summary = ":warning: Messages synced · attachment upload incomplete"
			if count, ok := strings.CutSuffix(thread.PartialError, " attachment(s) not synchronized"); ok {
				if n, err := strconv.Atoi(count); err == nil && n > 0 {
					label := "attachments"
					if n == 1 {
						label = "attachment"
					}
					summary = fmt.Sprintf(":warning: Messages synced · %d %s failed", n, label)
				}
			}
		}
	}
	if thread.CurrentError != "" {
		summary = ":warning: Sync needs attention · " + slackEscape(state)
	}
	lastSuccessDisplay := "never"
	if !thread.LastSuccess.IsZero() {
		lastSuccessDisplay = slackTimestamp(thread.LastSuccess)
	}
	blocks := []domain.SlackBlock{
		blockText("mrkdwn", title),
		blockText("mrkdwn", "*"+summary+"*"),
		{Type: "context", Elements: []domain.SlackText{{Type: "mrkdwn", Text: "Tracking " + tracking + " · Last synced " + lastSuccessDisplay}}},
	}
	var details []domain.SlackBlock
	addDetail := func(label, value string) {
		if value != "" {
			fallback += "\n" + label + ": " + value
			details = append(details, blockText("mrkdwn", "*"+label+"*\n"+slackEscape(value)))
		}
	}
	addDetail("Current error", thread.CurrentError)
	if !thread.NextAttempt.IsZero() && thread.Active {
		addDetail("Next retry", thread.NextAttempt.UTC().Format(time.RFC3339))
	}
	addDetail("Attachments needing attention", thread.PartialError)
	addDetail("Historical error", thread.HistoricalError)
	if len(details) != 0 {
		blocks = append(blocks, domain.SlackBlock{Type: "divider"})
		blocks = append(blocks, details...)
	}
	return fallback, boundedBlocks(blocks)
}

// Slack renders the full date and time in the viewer's timezone.
func slackTimestamp(t time.Time) string {
	return fmt.Sprintf("<!date^%d^{date_long_pretty} at {time_secs}|%s>", t.Unix(), t.UTC().Format("Jan 2, 2006 at 15:04:05 MST"))
}

// Help renders command guidance, optionally prefixed with a parse error.
func Help(bot string, invalid bool) (string, []domain.SlackBlock) {
	mention := "@bot"
	if bot != "" {
		mention = "<@" + bot + ">"
	}
	blocks := []domain.SlackBlock{}
	if invalid {
		blocks = append(blocks, blockText("mrkdwn", "I couldn't understand that request."))
	}
	blocks = append(blocks,
		blockHeader("Start tracking"),
		blockText("mrkdwn", "Post a new channel message with a ticket ID or HubSpot ticket URL:\n"+mention+" `12345`"),
		domain.SlackBlock{Type: "divider"},
		blockText("mrkdwn", "*In the thread*"),
	)
	for _, command := range []struct{ name, description string }{
		{"status", "Check tracking and sync status"},
		{"sync", "Update the HubSpot transcript"},
		{"untrack", "Stop tracking"},
		{"resume", "Resume and sync missed messages and files"},
		{"help", "Show these commands"},
	} {
		blocks = append(blocks, blockText("mrkdwn", mention+" `"+command.name+"` — "+command.description))
	}
	blocks = append(blocks, domain.SlackBlock{Type: "context", Elements: []domain.SlackText{{
		Type: "mrkdwn", Text: "Stopping removes the tracking reaction. Your ticket, note, files, and sync reactions stay.",
	}}})
	// Keep the full help available to notifications and screen readers.
	var fallback []string
	for _, block := range blocks {
		if block.Text != nil {
			text := block.Text.Text
			if block.Type == "header" {
				text = "*" + text + "*"
			}
			fallback = append(fallback, text)
		}
		for _, element := range block.Elements {
			fallback = append(fallback, element.Text)
		}
	}
	return strings.Join(fallback, "\n\n"), blocks
}
