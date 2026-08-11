package expiration

import (
	"context"
	"errors"
	"time"

	"github.com/cyverse-de/app-exposer/db"
	"github.com/jmoiron/sqlx"
)

// warnExpiring warns the owners of the analyses expiring between now+from and
// now+to. The two windows the sweep uses are adjacent rather than nested, so an
// analysis whose whole time limit is shorter than a day gets one warning rather
// than the day and hour warnings — whose text is identical — at the same moment.
func (w *Worker) warnExpiring(ctx context.Context, from, to time.Duration, kind db.WarningKind) {
	analyses, err := w.db.ListAnalysesExpiringWithin(ctx, kind, from, to)
	if err != nil {
		log.Errorf("listing analyses expiring between %s and %s from now: %v", from, to, err)
		return
	}

	for i := range analyses {
		if w.notificationBudgetSpent(ctx, string(kind)+" warnings") {
			return
		}
		analysis := &analyses[i]
		if !w.trackNotifications(ctx, analysis) {
			continue
		}
		w.deliver(ctx, analysis, kind)
	}
}

// notifyTerminated tells the owners of the analyses terminated this sweep that
// their analysis hit its time limit. It is a separate pass from the termination
// itself so that one user's slow notification cannot delay another user's
// termination.
func (w *Worker) notifyTerminated(ctx context.Context, analyses []*db.Analysis) {
	for _, analysis := range analyses {
		if w.notificationBudgetSpent(ctx, "termination notices") {
			return
		}
		if !w.trackNotifications(ctx, analysis) {
			continue
		}
		w.deliver(ctx, analysis, db.KillWarning)
	}
}

// notificationBudgetSpent reports whether this sweep's notification budget is
// gone, logging the pass that was cut short. The remaining analyses keep their
// unsent state, so the next sweep picks them up where this one stopped.
func (w *Worker) notificationBudgetSpent(ctx context.Context, pass string) bool {
	if ctx.Err() == nil {
		return false
	}
	log.Warnf(
		"this sweep's notification budget of %s is spent, so the remaining %s wait for the next sweep. "+
			"This usually means notification-agent is responding slowly",
		w.notificationBudget, pass,
	)
	return true
}

// remindStillRunning sends the periodic "your analysis is still running"
// reminder to the owners of analyses whose reminder period has elapsed.
func (w *Worker) remindStillRunning(ctx context.Context) {
	analyses, err := w.db.ListAnalysesDueForPeriodicReminder(ctx)
	if err != nil {
		log.Errorf("listing analyses due for a periodic reminder: %v", err)
		return
	}

	for i := range analyses {
		if w.notificationBudgetSpent(ctx, "periodic reminders") {
			return
		}
		analysis := &analyses[i]
		if !w.trackNotifications(ctx, analysis) {
			continue
		}

		claimErr := w.db.ClaimNotifStatuses(ctx, analysis.ID, func(tx *sqlx.Tx, statuses *db.NotifStatuses) error {
			if !reminderDue(analysis, statuses, time.Now()) {
				return nil
			}
			if err := w.notifier.NotifyStillRunning(ctx, analysis); err != nil {
				// Roll back rather than record a send that didn't happen;
				// the next sweep retries. Unlike the expiry warnings there
				// is no attempt ceiling here, because a missed reminder is
				// harmless and the reminder period paces the retries.
				return err
			}
			return w.db.SetLastPeriodicWarning(ctx, tx, analysis.ID)
		})
		w.logClaimResult(analysis, "periodic reminder", claimErr)
	}
}

// reminderDue reports whether an analysis's periodic reminder is due. The
// reminder is paced from the later of the analysis's start and its last
// reminder, so a freshly started analysis waits a full period before its first
// one rather than being reminded immediately.
func reminderDue(analysis *db.Analysis, statuses *db.NotifStatuses, now time.Time) bool {
	if analysis.StartDate == nil {
		return false
	}

	period := DefaultPeriodicReminderPeriod
	if statuses.PeriodicWarningSeconds > 0 {
		period = time.Duration(statuses.PeriodicWarningSeconds) * time.Second
	}

	since := *analysis.StartDate
	if statuses.LastPeriodicWarning != nil && statuses.LastPeriodicWarning.After(since) {
		since = *statuses.LastPeriodicWarning
	}

	return since.Add(period).Before(now)
}

// deliver sends one of the expiry notifications for an analysis, exactly once
// across all replicas, and records the outcome.
//
// Delivery failures are counted rather than retried immediately: the next
// attempt waits out a backoff that widens with the failure count, and after
// MaxNotificationAttempts spaced attempts the notification is marked sent so the
// DE stops trying. The counter is committed even on failure, which is why fn
// returns nil on the failure path.
func (w *Worker) deliver(ctx context.Context, analysis *db.Analysis, kind db.WarningKind) {
	key := retryKey{analysisID: analysis.ID, kind: kind}
	if !w.retries.due(key, time.Now()) {
		return
	}

	if analysis.StartDate == nil {
		// Every notification renders the analysis's elapsed runtime, so there
		// is nothing to send. Retrying would only spend the attempt ceiling on
		// a condition no retry fixes.
		log.Warnf(
			"analysis %s has no start date, so its %s notification cannot be built; skipping it. "+
				"This usually means the jobs row was never fully populated",
			analysis.ID, kind,
		)
		return
	}

	claimErr := w.db.ClaimNotifStatuses(ctx, analysis.ID, func(tx *sqlx.Tx, statuses *db.NotifStatuses) error {
		if statuses.Sent(kind) {
			return nil
		}

		sendErr := w.notify(ctx, analysis, kind)
		if sendErr == nil {
			w.retries.clear(key)
			return w.db.SetWarningSent(ctx, tx, kind, analysis.ID, true)
		}

		if ctx.Err() != nil {
			// The sweep's own notification budget cut the attempt short, so
			// nothing was learned about the recipient. Roll back rather than
			// spend an attempt on it.
			return sendErr
		}

		failures := statuses.FailureCount(kind) + 1
		log.Errorf(
			"delivering %s notification for analysis %s failed (attempt %d of %d, next retry in %s); "+
				"this usually means notification-agent or iplant-groups is unreachable: %v",
			kind, analysis.ID, failures, MaxNotificationAttempts, retryBackoff(failures), sendErr,
		)

		if err := w.db.SetWarningFailureCount(ctx, tx, kind, analysis.ID, failures); err != nil {
			return err
		}

		if failures >= MaxNotificationAttempts {
			log.Warnf(
				"giving up on the %s notification for analysis %s after %d failed attempts",
				kind, analysis.ID, failures,
			)
			w.retries.clear(key)
			return w.db.SetWarningSent(ctx, tx, kind, analysis.ID, true)
		}

		w.retries.schedule(key, failures, time.Now())
		return nil
	})
	w.logClaimResult(analysis, string(kind)+" notification", claimErr)
}

// notify dispatches to the notification matching the warning kind.
func (w *Worker) notify(ctx context.Context, analysis *db.Analysis, kind db.WarningKind) error {
	switch kind {
	case db.KillWarning:
		return w.notifier.NotifyTerminated(ctx, analysis)
	case db.DayWarning, db.HourWarning:
		return w.notifier.NotifyExpiringSoon(ctx, analysis)
	}
	return errors.New("unknown warning kind " + string(kind))
}

// trackNotifications makes sure the analysis has a notification-tracking row,
// reporting whether it is safe to go on and notify. Analyses with no external
// ID yet are skipped: they have no job_steps row, so nothing can be tracked or
// terminated for them.
func (w *Worker) trackNotifications(ctx context.Context, analysis *db.Analysis) bool {
	if analysis.ExternalID == "" {
		log.Warnf(
			"analysis %s has no external ID; skipping it this pass. "+
				"This usually means its job_steps row hasn't been written yet",
			analysis.ID,
		)
		return false
	}

	if err := w.db.EnsureNotifStatuses(ctx, analysis.ID, analysis.ExternalID, analysis.PeriodicPeriod); err != nil {
		log.Errorf("creating the notification-tracking row for analysis %s: %v", analysis.ID, err)
		return false
	}

	return true
}

// logClaimResult reports the outcome of a claimed notification attempt. Losing
// the claim is the expected outcome on every replica but one, so it is logged
// at debug level.
func (w *Worker) logClaimResult(analysis *db.Analysis, what string, err error) {
	switch {
	case err == nil:
	case errors.Is(err, db.ErrNotClaimed):
		log.Debugf("another replica is handling the %s for analysis %s", what, analysis.ID)
	default:
		log.Errorf("handling the %s for analysis %s: %v", what, analysis.ID, err)
	}
}
