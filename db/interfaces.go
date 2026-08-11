package db

import (
	"context"
	"time"

	"github.com/cyverse-de/app-exposer/constants"
	"github.com/cyverse-de/messaging/v12"
	"github.com/jmoiron/sqlx"
)

// ExpirationDB is the narrow subset of *Database operations used by the
// background analysis-expiration worker. As with ReconcilerDB it exists so the
// worker can be unit-tested against a fake; the production *Database satisfies
// it structurally.
type ExpirationDB interface {
	// ListExpiredAnalyses returns running analyses past their planned end date.
	ListExpiredAnalyses(ctx context.Context) ([]Analysis, error)

	// ListAnalysesExpiringWithin returns the running analyses due the given
	// expiry warning: those expiring between now+from and now+to that have not
	// been warned yet.
	ListAnalysesExpiringWithin(ctx context.Context, kind WarningKind, from, to time.Duration) ([]Analysis, error)

	// ListAnalysesDueForPeriodicReminder returns running analyses whose last
	// periodic reminder is older than their configured reminder period.
	ListAnalysesDueForPeriodicReminder(ctx context.Context) ([]Analysis, error)

	// ListAnalysesMissingRuntime returns the running VICE analyses that have no
	// subdomain or no planned end date.
	ListAnalysesMissingRuntime(ctx context.Context) ([]Analysis, error)

	// GetAnalysisByExternalID returns the analysis owning an external ID, or
	// sql.ErrNoRows when there is none.
	GetAnalysisByExternalID(ctx context.Context, externalID constants.ExternalID) (*Analysis, error)

	// IsInteractive reports whether the analysis has a VICE step.
	IsInteractive(ctx context.Context, analysisID constants.AnalysisID) (bool, error)

	// HasCompletedStatus reports whether a Completed status has already been
	// recorded for the given external ID.
	HasCompletedStatus(ctx context.Context, externalID constants.ExternalID) (bool, error)

	// InitializeRuntime fills in the analysis's subdomain and planned end date
	// when they are not already set, reporting whether it changed anything.
	InitializeRuntime(ctx context.Context, analysisID constants.AnalysisID, userID, externalID string) (bool, error)

	// EnsureNotifStatuses creates the analysis's notification-tracking row if
	// it is missing.
	EnsureNotifStatuses(ctx context.Context, analysisID constants.AnalysisID, externalID constants.ExternalID, periodicPeriodSeconds int) error

	// ClaimNotifStatuses locks the analysis's notification-tracking row and
	// runs fn against it, returning ErrNotClaimed if another replica holds it.
	ClaimNotifStatuses(ctx context.Context, analysisID constants.AnalysisID, fn func(tx *sqlx.Tx, statuses *NotifStatuses) error) error

	// SetWarningSent records delivery (or abandonment) of a notification.
	SetWarningSent(ctx context.Context, tx *sqlx.Tx, kind WarningKind, analysisID constants.AnalysisID, sent bool) error

	// SetWarningFailureCount records the failed-attempt count for a notification.
	SetWarningFailureCount(ctx context.Context, tx *sqlx.Tx, kind WarningKind, analysisID constants.AnalysisID, count int) error

	// SetLastPeriodicWarning records that a periodic reminder has just been sent.
	SetLastPeriodicWarning(ctx context.Context, tx *sqlx.Tx, analysisID constants.AnalysisID) error
}

// ReconcilerDB is the narrow subset of *Database operations used by the
// background reconciliation worker. It exists so the reconciler can be
// unit-tested against a fake without pulling in a real Postgres. The
// production *Database satisfies this interface structurally.
type ReconcilerDB interface {
	// ListOperators returns every configured operator ordered by priority.
	ListOperators(ctx context.Context) ([]Operator, error)

	// ClaimAndReconcile selects one operator whose last-reconciled timestamp
	// is older than reconciliationTTL, locks its row, and invokes fn within
	// the same transaction. Callbacks receive the claimed operator and the
	// active transaction; callers must thread tx through any DB methods
	// used inside fn.
	ClaimAndReconcile(ctx context.Context, hostname string, reconciliationTTL time.Duration, fn func(tx *sqlx.Tx, op *Operator) error) error

	// GetLatestStatusByExternalID returns the most recent status recorded in
	// job_status_updates for the given external ID. Returns sql.ErrNoRows
	// when no status has been recorded yet.
	GetLatestStatusByExternalID(ctx context.Context, tx *sqlx.Tx, externalID constants.ExternalID) (messaging.JobState, error)

	// InsertJobStatusUpdate appends a new row to job_status_updates.
	InsertJobStatusUpdate(ctx context.Context, tx *sqlx.Tx, update *JobStatusUpdate) error

	// URI returns the connection string used to open the database. The
	// reconciler uses it to open a dedicated LISTEN connection; returning
	// "" disables NOTIFY-driven syncs and falls back to periodic polling.
	URI() string
}
