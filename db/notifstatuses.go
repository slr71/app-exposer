package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/cyverse-de/app-exposer/constants"
	"github.com/jmoiron/sqlx"
)

// ErrNotClaimed is returned by ClaimNotifStatuses when another replica already
// holds the analysis's notif_statuses row. It is an expected outcome, not a
// fault: the holder is sending the notification, so this replica skips the
// analysis for this pass.
var ErrNotClaimed = errors.New("notif_statuses row claimed by another process")

// WarningKind identifies one of the three expiry-related notifications whose
// delivery is tracked per analysis.
type WarningKind string

const (
	// DayWarning is the notification sent a day before an analysis expires.
	DayWarning WarningKind = "day"
	// HourWarning is the notification sent shortly before an analysis expires.
	HourWarning WarningKind = "hour"
	// KillWarning is the notification sent when an analysis has been
	// terminated for exceeding its time limit.
	KillWarning WarningKind = "kill"
)

// NotifStatuses records which notifications have already been delivered for an
// analysis, and how many delivery attempts have failed. LastPeriodicWarning is
// nil when no periodic reminder has been sent yet, and is read through
// AT TIME ZONE current_setting('TimeZone') for the same reason as the timestamps
// on Analysis: the column is naive and holds local wall-clock time, so reading
// it raw would put every comparison off by the local UTC offset.
type NotifStatuses struct {
	AnalysisID              constants.AnalysisID `db:"analysis_id"`
	ExternalID              constants.ExternalID `db:"external_id"`
	HourWarningSent         bool                 `db:"hour_warning_sent"`
	HourWarningFailureCount int                  `db:"hour_warning_failure_count"`
	DayWarningSent          bool                 `db:"day_warning_sent"`
	DayWarningFailureCount  int                  `db:"day_warning_failure_count"`
	KillWarningSent         bool                 `db:"kill_warning_sent"`
	KillWarningFailureCount int                  `db:"kill_warning_failure_count"`
	LastPeriodicWarning     *time.Time           `db:"last_periodic_warning"`
	PeriodicWarningSeconds  int64                `db:"periodic_warning_seconds"`
}

// Sent reports whether the given notification has already been delivered (or
// abandoned after too many failures).
func (n *NotifStatuses) Sent(kind WarningKind) bool {
	switch kind {
	case DayWarning:
		return n.DayWarningSent
	case HourWarning:
		return n.HourWarningSent
	case KillWarning:
		return n.KillWarningSent
	}
	return false
}

// FailureCount returns how many delivery attempts for the given notification
// have failed so far.
func (n *NotifStatuses) FailureCount(kind WarningKind) int {
	switch kind {
	case DayWarning:
		return n.DayWarningFailureCount
	case HourWarning:
		return n.HourWarningFailureCount
	case KillWarning:
		return n.KillWarningFailureCount
	}
	return 0
}

// warningStatements pairs each notification kind with the statements that
// update its columns. The statements are constants selected by a closed set of
// WarningKind values, so no column name is ever interpolated from input.
var warningStatements = map[WarningKind]struct{ setSent, setFailureCount string }{
	DayWarning: {
		setSent:         `UPDATE notif_statuses SET day_warning_sent = $2 WHERE analysis_id = $1`,
		setFailureCount: `UPDATE notif_statuses SET day_warning_failure_count = $2 WHERE analysis_id = $1`,
	},
	HourWarning: {
		setSent:         `UPDATE notif_statuses SET hour_warning_sent = $2 WHERE analysis_id = $1`,
		setFailureCount: `UPDATE notif_statuses SET hour_warning_failure_count = $2 WHERE analysis_id = $1`,
	},
	KillWarning: {
		setSent:         `UPDATE notif_statuses SET kill_warning_sent = $2 WHERE analysis_id = $1`,
		setFailureCount: `UPDATE notif_statuses SET kill_warning_failure_count = $2 WHERE analysis_id = $1`,
	},
}

// EnsureNotifStatuses creates the analysis's notif_statuses row if it does not
// exist. periodicPeriodSeconds is the reminder interval requested at launch; a
// non-positive value stores the DE's four-hour default.
//
// Idempotent by way of the unique constraint on analysis_id, so concurrent
// replicas racing to create the same row both succeed.
func (d *Database) EnsureNotifStatuses(ctx context.Context, analysisID constants.AnalysisID, externalID constants.ExternalID, periodicPeriodSeconds int) error {
	period := fmt.Sprintf("%d seconds", periodicPeriodSeconds)
	if periodicPeriodSeconds <= 0 {
		period = "4 hours"
	}

	const stmt = `
		INSERT INTO notif_statuses (analysis_id, external_id, periodic_warning_period)
		VALUES ($1, $2, CAST($3 AS interval))
		ON CONFLICT (analysis_id) DO NOTHING
	`
	_, err := d.db.ExecContext(ctx, stmt, analysisID, externalID, period)
	return err
}

// ClaimNotifStatuses locks the analysis's notif_statuses row and runs fn with
// the loaded statuses and the owning transaction. The transaction commits when
// fn returns nil and rolls back otherwise, so a notification whose delivery
// fails leaves no partial bookkeeping behind.
//
// The lock is taken FOR UPDATE SKIP LOCKED, which is what keeps concurrent
// app-exposer replicas from both sending the same notification: the replica
// that loses the race gets ErrNotClaimed and moves on. Callers must have
// created the row (see EnsureNotifStatuses) first; sql.ErrNoRows means the row
// genuinely does not exist rather than that it was locked elsewhere.
//
// fn runs while the row lock is held and is expected to make a notification
// call out to the network, matching ClaimAndReconcile's existing tradeoff:
// the lock is held for one analysis's notification, not for a whole sweep.
func (d *Database) ClaimNotifStatuses(ctx context.Context, analysisID constants.AnalysisID, fn func(tx *sqlx.Tx, statuses *NotifStatuses) error) error {
	tx, err := d.db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // rollback after commit is a no-op

	const claimQuery = `
		SELECT analysis_id,
		       external_id,
		       hour_warning_sent,
		       hour_warning_failure_count,
		       day_warning_sent,
		       day_warning_failure_count,
		       kill_warning_sent,
		       kill_warning_failure_count,
		       last_periodic_warning AT TIME ZONE current_setting('TimeZone') AS last_periodic_warning,
		       COALESCE(EXTRACT(EPOCH FROM periodic_warning_period), 0)::bigint AS periodic_warning_seconds
		  FROM notif_statuses
		 WHERE analysis_id = $1
		   FOR UPDATE SKIP LOCKED
	`
	var statuses NotifStatuses
	if err := tx.GetContext(ctx, &statuses, claimQuery, analysisID); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		// SKIP LOCKED returns no rows both when the row is locked and when
		// it is absent. Distinguish the two so a missing row surfaces as a
		// real error instead of being silently skipped forever.
		claimed, existsErr := d.notifStatusesExists(ctx, analysisID)
		if existsErr != nil {
			return existsErr
		}
		if claimed {
			return ErrNotClaimed
		}
		return sql.ErrNoRows
	}

	if err := fn(tx, &statuses); err != nil {
		return err
	}

	return tx.Commit()
}

// notifStatusesExists reports whether the analysis has a notif_statuses row.
// Deliberately runs outside the claiming transaction: inside it, the row would
// be invisible to the same SKIP LOCKED semantics that just missed it.
func (d *Database) notifStatusesExists(ctx context.Context, analysisID constants.AnalysisID) (bool, error) {
	const query = `SELECT EXISTS (SELECT 1 FROM notif_statuses WHERE analysis_id = $1)`
	var exists bool
	err := d.db.QueryRowContext(ctx, query, analysisID).Scan(&exists)
	return exists, err
}

// SetWarningSent records that the given notification has been delivered, or has
// failed often enough that the DE stops retrying it.
func (d *Database) SetWarningSent(ctx context.Context, tx *sqlx.Tx, kind WarningKind, analysisID constants.AnalysisID, sent bool) error {
	stmts, ok := warningStatements[kind]
	if !ok {
		return fmt.Errorf("unknown warning kind %q", kind)
	}
	_, err := tx.ExecContext(ctx, stmts.setSent, analysisID, sent)
	return err
}

// SetWarningFailureCount records how many delivery attempts for the given
// notification have failed.
func (d *Database) SetWarningFailureCount(ctx context.Context, tx *sqlx.Tx, kind WarningKind, analysisID constants.AnalysisID, count int) error {
	stmts, ok := warningStatements[kind]
	if !ok {
		return fmt.Errorf("unknown warning kind %q", kind)
	}
	_, err := tx.ExecContext(ctx, stmts.setFailureCount, analysisID, count)
	return err
}

// SetLastPeriodicWarning records that a periodic reminder has just gone out,
// which is what paces the next one.
//
// The time comes from the database rather than from Go: last_periodic_warning
// is a naive `timestamp` holding local wall-clock time, so a Go instant written
// into it lands shifted by the difference between the process's zone and the
// database's, throwing off every comparison that paces the reminders.
func (d *Database) SetLastPeriodicWarning(ctx context.Context, tx *sqlx.Tx, analysisID constants.AnalysisID) error {
	const stmt = `UPDATE notif_statuses SET last_periodic_warning = now()::timestamp WHERE analysis_id = $1`
	_, err := tx.ExecContext(ctx, stmt, analysisID)
	return err
}
