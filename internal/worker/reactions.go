package worker

import (
	"context"
	"errors"
	"slices"
	"time"

	"slackhubspot/internal/domain"
	"slackhubspot/internal/render"
)

func (w *Worker) deliverReactions(ctx context.Context, workerID string) error {
	notification, err := w.options.Store.ClaimReactionNotification(ctx, workerID, time.Now(), w.options.LeaseDuration)
	if err != nil {
		return err
	}
	// This outbox lease survives stop/resume. A canceled sync lease must never
	// prevent the final state check or discard a needed reaction retry.
	reactionCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(w.options.LeaseDuration / 3)
		defer ticker.Stop()
		for {
			select {
			case <-reactionCtx.Done():
				return
			case <-ticker.C:
				if err := w.options.Store.RenewNotification(reactionCtx, notification, time.Now(), w.options.LeaseDuration); err != nil {
					cancel()
					return
				}
			}
		}
	}()
	err = w.reconcileReactions(reactionCtx, notification.Key)
	cancel()
	<-done
	err = w.finishNotification(ctx, notification, err)
	if errors.Is(err, domain.ErrLeaseLost) {
		// A remote request may have finished after another worker recovered the
		// lease. Schedule a fresh check even if that worker already completed.
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		return w.options.Store.QueueReactions(cleanup, notification.Key)
	}
	return err
}

type reactionTarget struct {
	timestamp string
	name      string
}

func (w *Worker) reconcileReactions(ctx context.Context, key domain.ThreadKey) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		thread, err := w.options.Store.Get(ctx, key)
		if err != nil {
			return err
		}
		// Core work arriving after the claim will enqueue its own final update.
		if thread.Active && (thread.State == "pending" || thread.State == "running") {
			return nil
		}
		messages, err := w.options.Slack.Thread(ctx, key)
		if err != nil {
			return err
		}
		desired := make(map[reactionTarget]bool)
		syncedTS := thread.LastSyncedMessageTS
		if syncedTS == "" && !thread.LastSuccess.IsZero() {
			// Older records have no sync checkpoint. Preserve the newest existing
			// success marker until a successful sync records its source timestamp.
			for _, message := range messages {
				if message.Timestamp > syncedTS && slices.Contains(message.BotReactions, w.options.SuccessReaction) && render.Eligible(message, w.options.BotUserID, w.options.AccountID) {
					syncedTS = message.Timestamp
				}
			}
		}
		success := reactionTarget{syncedTS, w.options.SuccessReaction}
		tracking := reactionTarget{key.ThreadTS, w.options.TrackingReaction}
		failure := reactionTarget{thread.LastMessageTS, w.options.FailureReaction}
		keep := func(target reactionTarget) {
			if target.name == "" || target.timestamp == "" {
				return
			}
			if slices.ContainsFunc(messages, func(message domain.Message) bool { return message.Timestamp == target.timestamp }) {
				desired[target] = true
			}
		}
		keep(success)
		if thread.Active {
			keep(tracking)
		}
		if thread.CurrentError != "" && thread.CurrentError != "operation cancelled" {
			keep(failure)
		}
		present := make(map[reactionTarget]bool)
		for _, message := range messages {
			for _, name := range message.BotReactions {
				present[reactionTarget{message.Timestamp, name}] = true
			}
		}
		// Slack is the source of emoji history. Clear all bot-owned reactions
		// outside the desired set before adding any new markers, including when
		// a configured emoji was changed or disabled.
		for _, message := range messages {
			for _, name := range message.BotReactions {
				target := reactionTarget{message.Timestamp, name}
				if !desired[target] {
					if err := w.options.Slack.RemoveReaction(ctx, key, target.timestamp, target.name); err != nil {
						return err
					}
				}
			}
		}
		for _, target := range []reactionTarget{success, tracking, failure} {
			if desired[target] && !present[target] {
				if err := w.options.Slack.React(ctx, key, target.timestamp, target.name); err != nil {
					return err
				}
				present[target] = true
			}
		}
		current, err := w.options.Store.Get(ctx, key)
		if err != nil {
			return err
		}
		if current.Active == thread.Active && current.LastSyncedMessageTS == thread.LastSyncedMessageTS && current.LastMessageTS == thread.LastMessageTS && current.CurrentError == thread.CurrentError {
			return nil
		}
	}
}
