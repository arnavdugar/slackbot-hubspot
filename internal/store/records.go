package store

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"slackhubspot/internal/domain"
	"slackhubspot/internal/store/backend"
)

func component(value string) string { return base64.RawURLEncoding.EncodeToString([]byte(value)) }
func threadKey(key domain.ThreadKey) string {
	return "threads/" + component(key.WorkspaceID) + "/" + component(key.ChannelID) + "/" + component(key.ThreadTS)
}
func uploadKey(workspace, file string) string {
	return "uploads/" + component(workspace) + "/" + component(file)
}
func token() string { return rand.Text() }

func read[T any](ctx context.Context, tx backend.Transaction, key string) (T, error) {
	var result T
	data, err := tx.Get(ctx, key)
	if errors.Is(err, backend.ErrNotFound) {
		return result, domain.ErrNotFound
	}
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return result, fmt.Errorf("decode stored record: %w", err)
	}
	return result, nil
}

func write(ctx context.Context, tx backend.Transaction, key string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return tx.Put(ctx, key, data)
}

func newer(left, right string) bool {
	if right == "" {
		return left != ""
	}
	l, lok := new(big.Rat).SetString(left)
	r, rok := new(big.Rat).SetString(right)
	if lok && rok {
		return l.Cmp(r) > 0
	}
	return left > right
}

func clearLease(thread *domain.Thread) {
	thread.LeaseExpires = time.Time{}
	thread.LeaseToken = ""
}

func validate(thread domain.Thread, lease domain.Lease, now time.Time) error {
	if !thread.Active || thread.Revision != lease.Revision || thread.LeaseToken == "" || thread.LeaseToken != lease.Token || !thread.LeaseExpires.After(now) {
		return domain.ErrLeaseLost
	}
	return nil
}

func (s *Store) Get(ctx context.Context, key domain.ThreadKey) (domain.Thread, error) {
	var thread domain.Thread
	err := s.db.View(ctx, func(tx backend.Transaction) error {
		var err error
		thread, err = read[domain.Thread](ctx, tx, threadKey(key))
		return err
	})
	return thread, err
}

const help = "Start tracking by mentioning the bot and a HubSpot ticket ID or URL in a new root message. Commands: @bot sync rebuilds the transcript; @bot untrack stops tracking and preserves the ticket, note, and files; @bot resume restarts tracking and backfills messages and files; @bot status shows synchronization status; @bot help shows this help."

// Record commits event deduplication, state transitions, and response outbox
// entries together. A response can never confirm an uncommitted transition.
func (s *Store) Record(ctx context.Context, event domain.Event) (domain.RecordResult, error) {
	var result domain.RecordResult
	if event.ID == "" || event.Key.WorkspaceID == "" || event.Key.ChannelID == "" || event.Key.ThreadTS == "" {
		return result, errors.New("event ID and complete thread key are required")
	}
	eventKey := "events/" + component(event.Key.WorkspaceID) + "/" + component(event.ID)
	err := s.db.Update(ctx, func(tx backend.Transaction) error {
		result = domain.RecordResult{}
		if _, err := tx.Get(ctx, eventKey); err == nil {
			result.Duplicate = true
			return nil
		} else if !errors.Is(err, backend.ErrNotFound) {
			return err
		}
		thread, err := read[domain.Thread](ctx, tx, threadKey(event.Key))
		exists := err == nil
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return err
		}
		changed := false
		reply := ""
		switch event.Kind {
		case "link":
			switch {
			case !exists:
				if event.TicketID == "" {
					return errors.New("linked ticket ID is required")
				}
				thread = domain.Thread{Active: true, ChangedAt: event.Now, ChangedBy: event.Actor, Key: event.Key, LastEventID: event.ID, LastMessageTS: event.MessageTS, RequestedGeneration: 1, Revision: 1, State: "pending", TicketID: event.TicketID, TicketURL: event.TicketURL}
				exists, changed, reply, result.Outcome = true, true, event.Reply, "linked"
			case thread.TicketID != event.TicketID:
				reply, result.Outcome = "This thread is already linked to ticket "+thread.TicketID+"; its ticket cannot be changed.", "conflict"
			default:
				result.Outcome = "already linked"
			}
		case "message":
			if exists && thread.Active {
				thread.CurrentError = ""
				thread.RequestedGeneration++
				thread.LastEventID = event.ID
				if newer(event.MessageTS, thread.LastMessageTS) {
					thread.LastMessageTS = event.MessageTS
				}
				if !thread.LeaseExpires.After(event.Now) {
					thread.State = "pending"
					clearLease(&thread)
				}
				thread.Attempts = 0
				thread.NextAttempt = time.Time{}
				changed, result.Outcome = true, "queued"
			}
		case "command":
			command := event.Command
			if command == "help" {
				reply = event.Reply
				if reply == "" {
					reply = help
				}
			} else if command != "sync" && command != "untrack" && command != "resume" && command != "status" {
				reply = event.Reply
				if reply == "" {
					reply = help
				}
			} else if !exists {
				reply = "This thread is not tracked."
				if command == "resume" {
					reply += " Start tracking by mentioning the bot and a HubSpot ticket ID or URL in a new root message."
				}
			} else if command == "status" {
				tracking := "active"
				if !thread.Active {
					tracking = "stopped"
				}
				state := thread.State
				if thread.Active && state == "running" && !thread.LeaseExpires.After(event.Now) {
					state = "pending"
				}
				lastSuccess := "never"
				if !thread.LastSuccess.IsZero() {
					lastSuccess = thread.LastSuccess.UTC().Format(time.RFC3339)
				}
				reply = fmt.Sprintf("Ticket: %s\nLink: %s\nTracking: %s\nSync: %s\nLast successful sync: %s", thread.TicketID, thread.TicketURL, tracking, state, lastSuccess)
				if thread.CurrentError != "" {
					reply += "\nCurrent error: " + thread.CurrentError
				}
				if !thread.NextAttempt.IsZero() && thread.Active {
					reply += "\nNext retry: " + thread.NextAttempt.UTC().Format(time.RFC3339)
				}
				if thread.PartialError != "" {
					reply += "\nLast sync partial attachment failure: " + thread.PartialError
				}
				if thread.HistoricalError != "" {
					reply += "\nHistorical error: " + thread.HistoricalError
				}
			} else if !newer(event.MessageTS, thread.LastCommandTS) {
				reply, result.Outcome = "Ignored an older or already applied tracking command.", "stale"
			} else {
				thread.LastCommandTS = event.MessageTS
				changed = true
				switch command {
				case "sync":
					if !thread.Active {
						reply = "Tracking is stopped. Use @bot resume to resume tracking."
						break
					}
					thread.RequestedGeneration++
					thread.CurrentError = ""
					thread.LastEventID = event.ID
					thread.Attempts = 0
					thread.NextAttempt = time.Time{}
					if !thread.LeaseExpires.After(event.Now) {
						thread.State = "pending"
						clearLease(&thread)
					}
					reply, result.Outcome = "Sync queued.", "queued"
				case "untrack":
					if !thread.Active {
						reply = "Tracking is already stopped; ticket, note, and file mappings are preserved."
						break
					}
					thread.Active = false
					thread.ChangedAt, thread.ChangedBy = event.Now, event.Actor
					thread.CurrentError = ""
					thread.Revision++
					thread.NextAttempt = time.Time{}
					thread.State = "stopped"
					clearLease(&thread)
					reply, result.Outcome = "Tracking stopped; ticket, note, and file mappings are preserved.", "stopped"
				case "resume":
					if thread.Active {
						reply = "Tracking is already active."
						break
					}
					thread.Active = true
					thread.Attempts = 0
					thread.ChangedAt, thread.ChangedBy = event.Now, event.Actor
					thread.CurrentError = ""
					thread.LastEventID = event.ID
					thread.NextAttempt = time.Time{}
					thread.RequestedGeneration++
					thread.Revision++
					thread.State = "pending"
					clearLease(&thread)
					reply, result.Outcome = "Tracking resumed; sync queued.", "resumed"
				}
			}
		case "reply":
			reply = event.Reply
		default:
			return errors.New("unsupported event kind")
		}
		if changed {
			if err := write(ctx, tx, threadKey(event.Key), thread); err != nil {
				return err
			}
		}
		if exists {
			result.Thread = &thread
		}
		if reply != "" {
			notification := notificationRecord{Notification: domain.Notification{ID: component(event.Key.WorkspaceID) + "/" + component(event.ID), Key: event.Key, Text: reply}}
			if err := write(ctx, tx, "notifications/"+notification.ID, notification); err != nil {
				return err
			}
		}
		return tx.Put(ctx, eventKey, []byte("1"))
	})
	return result, err
}

// boundedError defends the storage boundary against unexpectedly large summaries.
func boundedError(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 1024 {
		value = value[:1024]
	}
	return value
}
