package expiration

import (
	"context"
	"errors"
	"time"

	"github.com/cyverse-de/app-exposer/db"
	"github.com/cyverse-de/app-exposer/operatorclient"
)

// terminationGracePeriod bounds how long an expired analysis stays in limbo
// while the worker cannot tell whether it is still running. Waiting is the safe
// default, but waiting forever is its own failure: one permanently unreachable
// operator would otherwise pin every expired analysis in Running, holding the
// users' quota and leaving their listings wrong with no recovery short of an
// admin deleting the operator row.
const terminationGracePeriod = 24 * time.Hour

// terminateExpired terminates the analyses that have run past their planned end
// date, and reconciles the ones that are already gone. It returns the analyses
// whose owners are owed a termination notice, which the sweep sends afterwards:
// the notification path is the slow one, and nothing about it should stand
// between the next expired analysis and its termination.
func (w *Worker) terminateExpired(ctx context.Context) []*db.Analysis {
	analyses, err := w.db.ListExpiredAnalyses(ctx)
	if err != nil {
		log.Errorf("listing analyses past their planned end date: %v", err)
		return nil
	}

	terminated := make([]*db.Analysis, 0, len(analyses))

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
			w.handleIndeterminate(ctx, analysis, err)
			continue
		}

		if client == nil {
			w.markCompleted(ctx, analysis)
			continue
		}

		w.terminate(ctx, analysis, client)
		terminated = append(terminated, analysis)
	}

	return terminated
}

// handleIndeterminate decides what to do with an expired analysis whose
// whereabouts could not be established. The default is to wait for the next
// sweep: marking it Completed on the strength of an outage would end an
// analysis that may still be live, without saving its outputs. Past
// terminationGracePeriod the DE reconciles it anyway rather than leaving the
// row stuck in Running forever.
func (w *Worker) handleIndeterminate(ctx context.Context, analysis *db.Analysis, err error) {
	// No operator was asked at all, so nothing was learned and there is no
	// unreachable cluster to give up on. Never escalate this one: a scheduler
	// that has not synced yet would otherwise complete every expired analysis
	// in the DE once the grace period elapsed.
	if errors.Is(err, operatorclient.ErrNoOperators) {
		log.Warnf(
			"cannot determine whether expired analysis %s is still running: no operators are registered. "+
				"This usually means the reconciler has not synced the operators table yet",
			analysis.ID,
		)
		return
	}

	if analysis.PlannedEndDate != nil && time.Since(*analysis.PlannedEndDate) > terminationGracePeriod {
		log.Warnf(
			"expired analysis %s has been undeterminable for more than %s, so the DE is reconciling it anyway. "+
				"This usually means an operator in the operators table is permanently unreachable "+
				"(a decommissioned cluster, an expired certificate) and should be removed: %v",
			analysis.ID, terminationGracePeriod, err,
		)
		w.markCompleted(ctx, analysis)
		return
	}

	log.Errorf(
		"cannot determine whether expired analysis %s is still running, leaving it as-is: %v",
		analysis.ID, err,
	)
}

// markCompleted records an analysis as Completed because it is past its planned
// end date and no longer present in any cluster — the DE's record of it is
// simply out of date. No notification is sent: from the user's point of view
// the analysis already finished.
//
// Publishing the status once is meant to be enough: the DE transitions the
// analysis and it stops matching ListExpiredAnalyses. When that transition does
// not happen the analysis stays Running and expired indefinitely, so the publish
// is guarded — without that, every sweep on every replica publishes another
// status update, without bound, for a condition no amount of publishing fixes.
func (w *Worker) markCompleted(ctx context.Context, analysis *db.Analysis) {
	published, err := w.db.HasCompletedStatus(ctx, analysis.ExternalID)
	if err != nil {
		// Publishing regardless would risk the unbounded loop this check exists
		// to prevent, so leave the analysis for the next sweep.
		log.Errorf(
			"checking whether analysis %s was already reported Completed, skipping it this pass: %v",
			analysis.ID, err,
		)
		return
	}

	if published {
		log.Debugf(
			"expired analysis %s is gone from every cluster and has already been reported Completed, but "+
				"the DE still has it Running. This usually means the job-status pipeline is stalled or the "+
				"analysis's jobs row is held by a long-running transaction; re-publishing would fix neither",
			analysis.ID,
		)
		return
	}

	log.Infof(
		"expired analysis %s (external %s) is not running in any cluster; marking it Completed",
		analysis.ID, analysis.ExternalID,
	)

	if err := w.status.Success(ctx, string(analysis.ExternalID), completedMessage); err != nil {
		log.Errorf("marking analysis %s Completed: %v", analysis.ID, err)
	}
}

// terminate asks the owning operator to save the analysis's outputs and exit.
// The user is told separately, by the sweep, once every termination has been
// requested.
//
// The termination request is re-sent on every sweep until the analysis leaves
// the cluster, and every replica sweeps, so duplicates are routine: the
// operator drops a save-and-exit for an analysis whose upload is already
// running. The notification, by contrast, goes out exactly once — it is
// claim-guarded.
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
}
