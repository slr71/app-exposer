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
	"time"

	"github.com/cyverse-de/app-exposer/common"
	"github.com/cyverse-de/app-exposer/db"
	"github.com/cyverse-de/app-exposer/incluster"
	"github.com/cyverse-de/app-exposer/notifications"
	"github.com/cyverse-de/app-exposer/operatorclient"
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
	// it a persistently unreachable recipient would be retried for the life of
	// the analysis.
	//
	// Attempts are spaced by an exponential backoff (see retry.go), so the
	// ceiling is reached after a couple of hours of failures rather than after a
	// couple of sweeps. Both numbers have to be read together: raising the
	// ceiling without the pacing is what let a brief restart of
	// notification-agent abandon every notification in flight.
	MaxNotificationAttempts = 10

	// DefaultNotificationBudget bounds how long one sweep may spend sending
	// notifications. Terminations run first and notifications second, so the
	// budget is what keeps a notification-agent that hangs rather than refuses
	// from pushing the next sweep — and with it the next round of terminations —
	// arbitrarily far out.
	DefaultNotificationBudget = 2 * time.Minute
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

	// NotificationBudget is how long one sweep may spend on notifications
	// before deferring the rest to the next sweep. Zero selects
	// DefaultNotificationBudget.
	NotificationBudget time.Duration
}

// Worker runs the periodic expiration sweep.
type Worker struct {
	db                 db.ExpirationDB
	notifier           notifications.AnalysisNotifier
	scheduler          *operatorclient.Scheduler
	status             incluster.AnalysisStatusPublisher
	sweepInterval      time.Duration
	expiryWarning      time.Duration
	notificationBudget time.Duration
	retries            *retryTracker
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
		db:                 database,
		notifier:           notifier,
		scheduler:          scheduler,
		status:             status,
		sweepInterval:      init.SweepInterval,
		expiryWarning:      init.ExpiryWarning,
		notificationBudget: init.NotificationBudget,
		retries:            newRetryTracker(),
	}
	if w.sweepInterval <= 0 {
		w.sweepInterval = DefaultSweepInterval
	}
	if w.expiryWarning <= 0 {
		w.expiryWarning = DefaultExpiryWarning
	}
	if w.notificationBudget <= 0 {
		w.notificationBudget = DefaultNotificationBudget
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
	logLocalZone()

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

// logLocalZone reports the zone the DE's naive analysis timestamps are read and
// written in. It is resolved from the process environment rather than from
// configuration, so this is the only place the deployment gets to see what it
// actually resolved to before an analysis is terminated on the strength of it.
func logLocalZone() {
	name, offset, configured := db.LocalZone()
	log.Infof("analysis timestamps are interpreted as wall-clock time in %s (UTC%+.0fh)", name, offset.Hours())
	if !configured {
		log.Warnf(
			"TZ is not set, so analysis timestamps are being interpreted in %s. If the DE writes them in "+
				"another zone, every analysis expires early by the difference; set TZ to the deployment's zone",
			name,
		)
	}
}

// sweep runs one pass of all four responsibilities. Each pass is guarded
// against panics: this worker is secondary to app-exposer's API, and a panic
// here would otherwise take the VICE launch path down with it.
//
// Terminations run before notifications, under their own unbounded context,
// because they are the pass with a deadline the DE cannot make up later: an
// analysis left running past its time limit holds a node and the user's quota.
// The notification passes share one budget, so a notification-agent that hangs
// delays warnings rather than terminations.
func (w *Worker) sweep(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("panic in analysis expiration sweep, skipping this pass: %v", r)
		}
	}()

	w.repairRuntime(ctx)
	terminated := w.terminateExpired(ctx)

	notifyCtx, cancel := context.WithTimeout(ctx, w.notificationBudget)
	defer cancel()

	w.notifyTerminated(notifyCtx, terminated)
	w.warnExpiring(notifyCtx, 0, w.expiryWarning, db.HourWarning)
	w.warnExpiring(notifyCtx, w.expiryWarning, DayExpiryWarning, db.DayWarning)
	w.remindStillRunning(notifyCtx)

	w.retries.prune(time.Now())
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
