package notifications

import (
	"context"

	"github.com/cyverse-de/app-exposer/db"
)

// Disabled is the notifier used when the deployment's notification settings
// could not be turned into a working client. It reports success without sending
// anything, so the caller records the notification as handled instead of
// retrying a client that cannot be built.
//
// It exists so that a misconfigured notification URL costs the DE its analysis
// notifications and nothing else. The alternative — refusing to start the
// expiration worker — stops VICE analyses being terminated at their time limit,
// which fills the cluster with analyses that run forever.
type Disabled struct{}

// Compile-time check that Disabled is a drop-in for the real notifier.
var _ AnalysisNotifier = Disabled{}

// NotifyTerminated drops the termination notice.
func (Disabled) NotifyTerminated(_ context.Context, analysis *db.Analysis) error {
	logDropped("terminated", analysis)
	return nil
}

// NotifyExpiringSoon drops the expiry warning.
func (Disabled) NotifyExpiringSoon(_ context.Context, analysis *db.Analysis) error {
	logDropped("expiring soon", analysis)
	return nil
}

// NotifyStillRunning drops the periodic reminder.
func (Disabled) NotifyStillRunning(_ context.Context, analysis *db.Analysis) error {
	logDropped("still running", analysis)
	return nil
}

func logDropped(what string, analysis *db.Analysis) {
	log.Warnf(
		"dropping the %q notification for analysis %s: analysis notifications are disabled. "+
			"This usually means notification_agent.base or iplant_groups.base is malformed",
		what, analysis.ID,
	)
}
