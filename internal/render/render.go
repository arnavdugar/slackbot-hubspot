// Package render parses bot invocations and renders safe Slack summaries and HTML transcripts.
package render

import (
	"errors"
	"html"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"slackhubspot/internal/domain"
)

var (
	angleToken       = regexp.MustCompile(`<([^<>]+)>`)
	digits           = regexp.MustCompile(`^[0-9]+$`)
	linkToken        = regexp.MustCompile(`https?://[^\s<>|]+`)
	mentionToken     = regexp.MustCompile(`<@([A-Z0-9]+)(?:\|([^>]*))?>`)
	templateVariable = regexp.MustCompile(`^\s*\.([A-Za-z]+)\s*$`)
)

type Result struct {
	Command   string
	Invalid   bool
	Mentioned bool
	TicketID  string
}

// Parse gives explicit commands precedence over links. A mention embedded in
// ordinary prose is not an unknown command; a leading invocation is.
func Parse(text, botUserID, accountID string) Result {
	var result Result
	indices := mentionToken.FindAllStringSubmatchIndex(text, -1)
	start, end := -1, -1
	for _, at := range indices {
		if text[at[2]:at[3]] == botUserID {
			start, end = at[0], at[1]
			break
		}
	}
	if start < 0 {
		return result
	}
	result.Mentioned = true
	rest := strings.TrimSpace(text[end:])
	words := strings.Fields(rest)
	if len(words) > 0 {
		switch strings.ToLower(words[0]) {
		case "help", "resume", "status", "sync", "untrack":
			result.Command = strings.ToLower(words[0])
			result.Invalid = len(words) != 1
			return result
		}
		if digits.MatchString(words[0]) {
			if len(words) == 1 {
				result.TicketID = words[0]
			} else {
				result.Invalid = true
			}
			return result
		}
		if strings.TrimSpace(text[:start]) == "" && !digits.MatchString(words[0]) && !strings.HasPrefix(strings.TrimPrefix(words[0], "<"), "https://") && !strings.HasPrefix(strings.TrimPrefix(words[0], "<"), "http://") {
			result.Invalid = true
			return result
		}
	}
	for _, raw := range linkToken.FindAllString(rest, -1) {
		u, err := url.Parse(strings.TrimRight(raw, ".,)"))
		if err != nil {
			continue
		}
		host := strings.ToLower(u.Hostname())
		if host != "app.hubspot.com" && host != "app-eu1.hubspot.com" && host != "app-na2.hubspot.com" {
			continue
		}
		parts := strings.Split(strings.Trim(u.Path, "/"), "/")
		id, account := "", ""
		if len(parts) == 5 && parts[0] == "contacts" && parts[2] == "record" && parts[3] == "0-5" {
			account, id = parts[1], parts[4]
		}
		if len(parts) == 4 && (parts[0] == "contacts" || parts[0] == "tickets") && parts[2] == "ticket" {
			account, id = parts[1], parts[3]
		}
		if u.Scheme != "https" || u.User != nil || u.Port() != "" || account != accountID || !digits.MatchString(id) || result.TicketID != "" {
			result.Invalid = true
			result.TicketID = ""
			return result
		}
		result.TicketID = id
	}
	if result.TicketID == "" && strings.TrimSpace(text[:start]) == "" {
		result.Invalid = true
	}
	return result
}

// Eligible is shared by event processing and full-thread reconciliation.
func Eligible(message domain.Message, botUserID, accountID string) bool {
	if message.UserID == botUserID || (message.Subtype != "" && message.Subtype != "file_share" && message.Subtype != "bot_message" && message.Subtype != "thread_broadcast") {
		return false
	}
	result := Parse(message.Text, botUserID, accountID)
	return result.Command == "" && !result.Invalid
}

const DefaultSummary = "*{{.Subject}}*\nTicket {{.ID}} · {{.Status}} · {{.Priority}}\n<{{.URL}}|Open ticket>"

type summaryPart struct {
	field   string
	literal string
}
type Summary struct{ parts []summaryPart }

func (s *Summary) Fields() []string {
	seen := map[string]bool{}
	for _, part := range s.parts {
		if part.field != "" {
			seen[part.field] = true
		}
	}
	fields := make([]string, 0, len(seen))
	for field := range seen {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	return fields
}

// CompileSummary accepts only literal text and {{.Field}} substitutions. It
// intentionally does not interpret Go templates, functions, loops, or methods.
func CompileSummary(source string) (*Summary, error) {
	if strings.TrimSpace(source) == "" {
		return nil, errors.New("ticket summary template is empty")
	}
	summary := &Summary{}
	for source != "" {
		at := strings.Index(source, "{{")
		if at < 0 {
			if strings.Contains(source, "}}") {
				return nil, errors.New("unmatched template delimiter")
			}
			summary.parts = append(summary.parts, summaryPart{literal: source})
			break
		}
		if at > 0 {
			if strings.Contains(source[:at], "}}") {
				return nil, errors.New("unmatched template delimiter")
			}
			summary.parts = append(summary.parts, summaryPart{literal: source[:at]})
		}
		source = source[at+2:]
		end := strings.Index(source, "}}")
		if end < 0 {
			return nil, errors.New("unmatched template delimiter")
		}
		match := templateVariable.FindStringSubmatch(source[:end])
		if match == nil {
			return nil, errors.New("summary supports only {{.Field}} substitutions")
		}
		switch match[1] {
		case "Company", "Contact", "CreatedAt", "ID", "Owner", "Pipeline", "Priority", "Status", "Subject", "URL", "UpdatedAt":
		default:
			return nil, errors.New("unsupported ticket summary variable")
		}
		summary.parts = append(summary.parts, summaryPart{field: match[1]})
		source = source[end+2:]
	}
	return summary, nil
}

func (s *Summary) Render(ticket domain.Ticket) string {
	fields := map[string]string{"Company": ticket.Company, "Contact": ticket.Contact, "CreatedAt": ticket.CreatedAt, "ID": ticket.ID, "Owner": ticket.Owner, "Pipeline": ticket.Pipeline, "Priority": ticket.Priority, "Status": ticket.Status, "Subject": ticket.Subject, "URL": ticket.URL, "UpdatedAt": ticket.UpdatedAt}
	var out strings.Builder
	for _, part := range s.parts {
		if part.field == "" {
			out.WriteString(part.literal)
		} else {
			out.WriteString(strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(fields[part.field]))
		}
	}
	return out.String()
}

// Transcript rebuilds the entire integration-owned note. Attachments maps Slack
// file IDs to human-readable synchronization status, including partial failures.
func Transcript(key domain.ThreadKey, messages []domain.Message, attachments map[string]string) string {
	messages = append([]domain.Message(nil), messages...)
	sort.SliceStable(messages, func(i, j int) bool {
		left, lf, _ := strings.Cut(messages[i].Timestamp, ".")
		right, rf, _ := strings.Cut(messages[j].Timestamp, ".")
		left = strings.TrimLeft(left, "0")
		right = strings.TrimLeft(right, "0")
		if len(left) != len(right) {
			return len(left) < len(right)
		}
		if left != right {
			return left < right
		}
		return strings.TrimRight(lf, "0") < strings.TrimRight(rf, "0")
	})
	var out strings.Builder
	out.WriteString("<p><strong>Slack thread transcript</strong><br>Source: " + html.EscapeString(key.Marker()) + "</p>")
	for _, message := range messages {
		name := message.SenderName
		if name == "" {
			name = message.UserID
		}
		id := message.UserID
		if id == "" {
			id = message.BotID
		}
		timestamp := message.Timestamp
		if seconds, fraction, ok := strings.Cut(timestamp, "."); ok {
			if n, err := strconv.ParseInt(seconds, 10, 64); err == nil {
				timestamp = time.Unix(n, 0).UTC().Format("2006-01-02 15:04:05") + "." + fraction + " UTC"
			}
		}
		out.WriteString("<p><strong>" + html.EscapeString(name) + "</strong> (" + html.EscapeString(id) + ") — " + html.EscapeString(timestamp))
		if safeURL(message.Permalink) {
			out.WriteString(` · <a href="` + html.EscapeString(message.Permalink) + `">Slack message</a>`)
		}
		out.WriteString("<br>" + slackHTML(message.Text))
		files := append([]domain.File(nil), message.Files...)
		sort.SliceStable(files, func(i, j int) bool { return files[i].Name < files[j].Name })
		for _, file := range files {
			status := attachments[file.ID]
			if status == "" {
				status = "pending upload"
			}
			out.WriteString("<br>Attachment: " + html.EscapeString(file.Name) + " — " + html.EscapeString(status))
		}
		out.WriteString("</p>")
	}
	return out.String()
}

func safeURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "https" || u.Scheme == "http") && u.Host != "" && u.User == nil
}

func slackHTML(text string) string {
	// Tokenize before escaping. Arbitrary tags are always rendered as text.
	var out strings.Builder
	last := 0
	for _, index := range angleToken.FindAllStringSubmatchIndex(text, -1) {
		out.WriteString(mrkdwn(text[last:index[0]]))
		token := text[index[2]:index[3]]
		target, label, hasLabel := strings.Cut(token, "|")
		switch {
		case strings.HasPrefix(target, "@"):
			if !hasLabel {
				label = target[1:]
			}
			out.WriteString(html.EscapeString("@" + label))
			if hasLabel && label != target[1:] {
				out.WriteString(" (" + html.EscapeString(target[1:]) + ")")
			}
		case strings.HasPrefix(target, "#"):
			if !hasLabel {
				label = target[1:]
			}
			out.WriteString(html.EscapeString("#" + label))
		case target == "!here" || target == "!channel" || target == "!everyone":
			out.WriteString(html.EscapeString("@" + target[1:]))
		case strings.HasPrefix(target, "!subteam^"):
			if !hasLabel {
				label = strings.TrimPrefix(target, "!subteam^")
			}
			out.WriteString(html.EscapeString(label))
		case safeURL(html.UnescapeString(target)):
			if !hasLabel {
				label = target
			}
			out.WriteString(`<a href="` + html.EscapeString(html.UnescapeString(target)) + `">` + html.EscapeString(html.UnescapeString(label)) + `</a>`)
		default:
			out.WriteString(html.EscapeString(text[index[0]:index[1]]))
		}
		last = index[1]
	}
	out.WriteString(mrkdwn(text[last:]))
	return out.String()
}

func mrkdwn(text string) string {
	// Formatting is rendered from bounded delimiter pairs, never as raw HTML.
	var out strings.Builder
	for len(text) > 0 {
		delimiter, open, close := "", "", ""
		switch {
		case strings.HasPrefix(text, "```"):
			delimiter, open, close = "```", "<pre>", "</pre>"
		case text[0] == '`':
			delimiter, open, close = "`", "<code>", "</code>"
		case text[0] == '*':
			delimiter, open, close = "*", "<strong>", "</strong>"
		case text[0] == '_':
			delimiter, open, close = "_", "<em>", "</em>"
		case text[0] == '~':
			delimiter, open, close = "~", "<del>", "</del>"
		}
		if delimiter != "" {
			if end := strings.Index(text[len(delimiter):], delimiter); end > 0 {
				out.WriteString(open + html.EscapeString(html.UnescapeString(text[len(delimiter):len(delimiter)+end])) + close)
				text = text[len(delimiter)*2+end:]
				continue
			}
		}
		if text[0] == '\n' {
			out.WriteString("<br>")
			text = text[1:]
			continue
		}
		// Escape a whole run to preserve UTF-8 and decode Slack's three entities.
		end := 1
		for end < len(text) && !strings.ContainsRune("\n`*_~", rune(text[end])) {
			end++
		}
		out.WriteString(html.EscapeString(html.UnescapeString(text[:end])))
		text = text[end:]
	}
	return out.String()
}
