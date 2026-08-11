// Package expiration enforces the time limits on VICE analyses. A background
// worker periodically warns users whose analyses are about to expire, sends the
// periodic "still running" reminder, and terminates analyses that have run past
// their planned end date. It also backfills the runtime fields an analysis
// needs (subdomain, planned end date) if they were not set at launch.
//
// This work moved here from the standalone `timelord` service. Running it
// inside app-exposer removes the HTTP hop it used to make back into app-exposer
// to terminate an analysis, and lets it share the operator scheduler.
package expiration

import (
	"context"
	"errors"
	"time"

	"github.com/cyverse-de/app-exposer/common"
	"github.com/cyverse-de/app-exposer/db"
	"github.com/cyverse-de/app-exposer/incluster"
	"github.com/cyverse-de/app-exposer/notifications"
	"github.com/cyverse-de/app-exposer/operatorclient"
	"github.com/jmoiron/sqlx"
	"github.com/sirupsen/logrus"
)

var log = common.Log.WithFields(logrus.Fields{"package": "expiration"})

const (
	// DefaultSweepInterval is how often the worker re-examines running
	// analyses. Warnings and terminations are both driven off planned end
	// dates, so the cadence bounds how late either can be.
	DefaultSweepInterval = 10 * time.Second

	// DefaultExpiryWarning is how far ahead of expiry the short-notice
	// warning goes out.
	DefaultExpiryWarning = time.Hour

	// DayExpiryWarning is how far ahead of expiry the day-notice warning
	// goes out.
	DayExpiryWarning = 24 * time.Hour

	// DefaultPeriodicReminderPeriod is how often a long-running analysis's
	// "still running" reminder repeats when the analysis did not request an
	// interval of its own.
	DefaultPeriodicReminderPeriod = 4 * time.Hour

	// MaxNotificationAttempts bounds how many times delivery of a single
	// notification is retried before the DE gives up and stops trying. Without
	// it a persistently unreachable recipient would be retried on every sweep
	// for the life of the analysis.
	MaxNotificationAttempts = 3
)

// completedMessage is recorded against an analysis that the DE marks Completed
// because it is no longer running anywhere. It lands in the job status history,
// so it names the reason rather than just the new state.
const completedMessage = "analysis is past its planned end date and is not running in any cluster; marking it Completed"

// Init configures a Worker.
type Init struct {
	// SweepInterval is how often to examine running analyses. Zero selects
	// DefaultSweepInterval.
	SweepInterval time.Duration

	// ExpiryWarning is how far ahead of expiry to send the short-notice
	// warning. Zero selects DefaultExpiryWarning.
	ExpiryWarning time.Duration
}

// Worker runs the periodic expiration sweep.
type Worker struct {
	db            db.ExpirationDB
	notifier      notifications.AnalysisNotifier
	scheduler     *operatorclient.Scheduler
	status        incluster.AnalysisStatusPublisher
	sweepInterval time.Duration
	expiryWarning time.Duration
}

// New creates a Worker. The scheduler is used both to find which cluster is
// running an analysis and to terminate it there; status publishes to
// job-status-listener for analyses that have already vanished from every
// cluster.
func New(
	database db.ExpirationDB,
	notifier notifications.AnalysisNotifier,
	scheduler *operatorclient.Scheduler,
	status incluster.AnalysisStatusPublisher,
	init Init,
) *Worker {
	w := &Worker{
		db:            database,
		notifier:      notifier,
		scheduler:     scheduler,
		status:        status,
		sweepInterval: init.SweepInterval,
		expiryWarning: init.ExpiryWarning,
	}
	if w.sweepInterval <= 0 {
		w.sweepInterval = DefaultSweepInterval
	}
	if w.expiryWarning <= 0 {
		w.expiryWarning = DefaultExpiryWarning
	}
	return w
}

// Run sweeps until the context is canceled. The first sweep happens
// immediately so a restart doesn't delay an overdue termination.
func (w *Worker) Run(ctx context.Context) {
	log.Infof(
		"starting analysis expiration worker (sweep every %s, expiry warning %s ahead)",
		w.sweepInterval, w.expiryWarning,
	)

	ticker := time.NewTicker(w.sweepInterval)
	defer ticker.Stop()

	w.sweep(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Info("stopping analysis expiration worker")
			return
		case <-ticker.C:
			w.sweep(ctx)
		}
	}
}

// sweep runs one pass of all four responsibilities. Each pass is guarded
// against panics: this worker is secondary to app-exposer's API, and a panic
// here would otherwise take the VICE launch path down with it.
func (w *Worker) sweep(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("panic in analysis expiration sweep, skipping this pass: %v", r)
		}
	}()

	w.repairRuntime(ctx)
	w.warnExpiring(ctx, w.expiryWarning, db.HourWarning)
	w.warnExpiring(ctx, DayExpiryWarning, db.DayWarning)
	w.remindStillRunning(ctx)
	w.terminateExpired(ctx)
}

// repairRuntime fills in the subdomain and planned end date of any running VICE
// analysis that is missing them. It runs first so an analysis repaired this
// sweep is a candidate for the passes that follow.
//
// The AMQP consumer in this package does the same repair as job status updates
// arrive, but it only exists when amqp.uri is configured. This pass is the one
// that is always present: without it, an analysis whose launch-time write
// failed would be unroutable and would never expire, with nothing to notice.
func (w *Worker) repairRuntime(ctx context.Context) {
	analyses, err := w.db.ListAnalysesMissingRuntime(ctx)
	if err != nil {
		log.Errorf("listing analyses missing a subdomain or planned end date: %v", err)
		return
	}

	for i := range analyses {
		analysis := &analyses[i]
		if analysis.ExternalID == "" {
			// No job_steps row yet, so there is no external ID to derive the
			// subdomain from. The next sweep picks it up.
			continue
		}
		initializeRuntime(ctx, w.db, analysis, analysis.ExternalID, log.WithFields(logrus.Fields{
			"analysisID": analysis.ID,
		}))
	}
}

// warnExpiring warns the owners of analyses expiring inside the given window.
func (w *Worker) warnExpiring(ctx context.Context, window time.Duration, kind db.WarningKind) {
	analyses, err := w.db.ListAnalysesExpiringWithin(ctx, window)
	if err != nil {
		log.Errorf("listing analyses expiring within %s: %v", window, err)
		return
	}

	for i := range analyses {
		analysis := &analyses[i]
		if !w.trackNotifications(ctx, analysis) {
			continue
		}
		w.deliver(ctx, analysis, kind)
	}
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
// Delivery failures are counted rather than retried immediately; after
// MaxNotificationAttempts the notification is marked sent so the DE stops
// trying. The counter is committed even on failure, which is why fn returns nil
// on the failure path.
func (w *Worker) deliver(ctx context.Context, analysis *db.Analysis, kind db.WarningKind) {
	claimErr := w.db.ClaimNotifStatuses(ctx, analysis.ID, func(tx *sqlx.Tx, statuses *db.NotifStatuses) error {
		if statuses.Sent(kind) {
			return nil
		}

		sendErr := w.notify(ctx, analysis, kind)
		if sendErr == nil {
			return w.db.SetWarningSent(ctx, tx, kind, analysis.ID, true)
		}

		failures := statuses.FailureCount(kind) + 1
		log.Errorf(
			"delivering %s notification for analysis %s failed (attempt %d of %d); "+
				"this usually means notification-agent or iplant-groups is unreachable: %v",
			kind, analysis.ID, failures, MaxNotificationAttempts, sendErr,
		)

		if err := w.db.SetWarningFailureCount(ctx, tx, kind, analysis.ID, failures); err != nil {
			return err
		}

		if failures >= MaxNotificationAttempts {
			log.Warnf(
				"giving up on the %s notification for analysis %s after %d failed attempts",
				kind, analysis.ID, failures,
			)
			return w.db.SetWarningSent(ctx, tx, kind, analysis.ID, true)
		}

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
