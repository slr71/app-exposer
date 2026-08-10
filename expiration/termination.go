package expiration

import (
	"context"

	"github.com/cyverse-de/app-exposer/db"
	"github.com/cyverse-de/app-exposer/operatorclient"
)

// terminateExpired terminates the analyses that have run past their planned end
// date, and reconciles the ones that are already gone.
func (w *Worker) terminateExpired(ctx context.Context) {
	analyses, err := w.db.ListExpiredAnalyses(ctx)
	if err != nil {
		log.Errorf("listing analyses past their planned end date: %v", err)
		return
	}

	for i := range analyses {
		analysis := &analyses[i]

		if analysis.ExternalID == "" {
			log.Warnf(
				"analysis %s is past its planned end date but has no external ID; "+
					"skipping it. This usually means its job_steps row hasn't been written yet",
				analysis.ID,
			)
			continue
		}

		// Ask the operators directly rather than trusting the operator id
		// recorded in the database: the question here is whether the analysis
		// still exists in a cluster, and acting on a stale answer either kills
		// nothing or marks a live analysis Completed.
		client, err := w.scheduler.FindAnalysis(ctx, analysis.ID)
		if err != nil {
			// An operator was unreachable, so "not found" can't be trusted.
			// Leave the analysis alone: it will be reconsidered next sweep.
			// Marking it Completed here would end a possibly-live analysis on
			// the strength of an outage.
			log.Errorf(
				"cannot determine whether expired analysis %s is still running, leaving it as-is: %v",
				analysis.ID, err,
			)
			continue
		}

		if client == nil {
			w.markCompleted(ctx, analysis)
			continue
		}

		w.terminate(ctx, analysis, client)
	}
}

// markCompleted records an analysis as Completed because it is past its planned
// end date and no longer present in any cluster — the DE's record of it is
// simply out of date. No notification is sent: from the user's point of view
// the analysis already finished.
func (w *Worker) markCompleted(ctx context.Context, analysis *db.Analysis) {
	log.Infof(
		"expired analysis %s (external %s) is not running in any cluster; marking it Completed",
		analysis.ID, analysis.ExternalID,
	)

	if err := w.status.Success(ctx, string(analysis.ExternalID), completedMessage); err != nil {
		log.Errorf("marking analysis %s Completed: %v", analysis.ID, err)
	}
}

// terminate asks the owning operator to save the analysis's outputs and exit,
// then tells the user it was terminated.
//
// The termination request is re-sent on every sweep until the analysis leaves
// the cluster; the operator's save-and-exit is expected to tolerate that. The
// notification, by contrast, goes out exactly once — it is claim-guarded.
func (w *Worker) terminate(ctx context.Context, analysis *db.Analysis, client *operatorclient.Client) {
	log.Infof(
		"terminating analysis %s (external %s) on operator %s: it is past its planned end date",
		analysis.ID, analysis.ExternalID, client.Name(),
	)

	if err := client.SaveAndExit(ctx, analysis.ID); err != nil {
		// Don't stop here: the user should still learn their analysis hit its
		// time limit, and the next sweep retries the termination.
		log.Errorf(
			"requesting save-and-exit for analysis %s on operator %s failed; will retry next sweep: %v",
			analysis.ID, client.Name(), err,
		)
	}

	if !w.trackNotifications(ctx, analysis) {
		return
	}

	w.deliver(ctx, analysis, db.KillWarning)
}
