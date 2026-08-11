package expiration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/cyverse-de/app-exposer/constants"
	"github.com/cyverse-de/app-exposer/db"
	"github.com/cyverse-de/messaging/v12"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

// QueueName is the AMQP queue the runtime backfill consumes job status updates
// from. It is deliberately unchanged from the standalone timelord service so
// the existing queue and its bindings carry over on deploy rather than a new
// one being declared alongside it.
const QueueName = "timelord"

// RuntimeBackfill is the safety net for the runtime fields an analysis needs:
// its routing subdomain and its planned end date.
//
// The VICE launch handler is the canonical writer of both. This consumer
// watches job status updates and fills them in for any interactive analysis
// that reaches Running without them — an analysis launched by an app-exposer
// that predates the launch-time write, or one whose write failed. Every
// backfill is logged at warn level, because in steady state this should never
// fire.
type RuntimeBackfill struct {
	db db.ExpirationDB
}

// NewRuntimeBackfill returns a RuntimeBackfill backed by the given database.
func NewRuntimeBackfill(database db.ExpirationDB) *RuntimeBackfill {
	return &RuntimeBackfill{db: database}
}

// MessageHandler returns the AMQP handler for job status update messages.
func (r *RuntimeBackfill) MessageHandler() messaging.MessageHandler {
	return func(ctx context.Context, delivery amqp.Delivery) {
		// Acknowledge up front: this is a backfill for something the launch
		// handler already does, so dropping a message on a crash costs nothing
		// that the next Running update won't also cover.
		if err := delivery.Ack(false); err != nil {
			log.Errorf("acknowledging job status update: %v", err)
		}

		update := &messaging.UpdateMessage{}
		if err := json.Unmarshal(delivery.Body, update); err != nil {
			log.Errorf("unmarshaling job status update: %v", err)
			return
		}

		// Only a Running analysis has the start date the planned end date is
		// derived from. Checked before any query so the vast majority of
		// status updates cost nothing.
		if update.State != messaging.RunningState {
			return
		}

		if update.Job == nil || update.Job.InvocationID == "" {
			log.Error("job status update carries no invocation ID; ignoring it")
			return
		}

		r.backfill(ctx, constants.ExternalID(update.Job.InvocationID))
	}
}

// backfill fills in the runtime fields of the interactive analysis owning the
// given external ID.
func (r *RuntimeBackfill) backfill(ctx context.Context, externalID constants.ExternalID) {
	msgLog := log.WithFields(logrus.Fields{"externalID": externalID})

	analysis, err := r.db.GetAnalysisByExternalID(ctx, externalID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// A status update for an analysis the DE has no record of. Not
			// actionable here; job-status-listener owns that complaint.
			msgLog.Debug("no analysis found for external ID; ignoring the update")
			return
		}
		msgLog.Errorf("looking up the analysis for external ID: %v", err)
		return
	}

	msgLog = msgLog.WithFields(logrus.Fields{"analysisID": analysis.ID})

	interactive, err := r.db.IsInteractive(ctx, analysis.ID)
	if err != nil {
		msgLog.Errorf("determining whether the analysis is interactive: %v", err)
		return
	}
	if !interactive {
		// Only VICE analyses get a subdomain and a planned end date.
		return
	}

	initializeRuntime(ctx, r.db, analysis, externalID, msgLog)
}

// initializeRuntime fills in an interactive analysis's subdomain and planned end
// date if they are missing, logging at warn level when it had to: in steady
// state the launch handler has already written both, so anything repaired here
// means an analysis was launched by an older app-exposer or its launch-time
// write failed.
func initializeRuntime(ctx context.Context, database db.ExpirationDB, analysis *db.Analysis, externalID constants.ExternalID, entry *logrus.Entry) {
	changed, err := database.InitializeRuntime(ctx, analysis.ID, analysis.UserID, string(externalID))
	if err != nil {
		entry.Errorf("backfilling the subdomain and planned end date: %v", err)
		return
	}

	if changed {
		entry.Warnf(
			"backfilled the subdomain and/or planned end date for analysis %s; "+
				"the launch handler should have set these, so this analysis was "+
				"either launched by an older app-exposer or its launch-time write failed",
			analysis.ID,
		)
	}
}
