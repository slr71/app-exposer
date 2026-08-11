package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/cyverse-de/app-exposer/common"
	"github.com/cyverse-de/app-exposer/constants"
	"github.com/cyverse-de/messaging/v12"
)

// Analysis carries the jobs-table fields needed to decide whether an analysis
// has reached its time limit and to describe it in a user notification.
//
// StartDate and PlannedEndDate are nullable: an analysis that has not started,
// or whose planned end date has not been set yet, leaves them nil. Both come
// from naive `timestamp` columns and are only correct instants once
// localizeTimestamps has run — see db/timestamps.go.
type Analysis struct {
	ID             constants.AnalysisID `db:"id"`
	AppID          string               `db:"app_id"`
	UserID         string               `db:"user_id"`
	Status         string               `db:"status"`
	Description    string               `db:"job_description"`
	Name           string               `db:"job_name"`
	ResultFolder   string               `db:"result_folder_path"`
	StartDate      *time.Time           `db:"start_date"`
	PlannedEndDate *time.Time           `db:"planned_end_date"`
	Subdomain      string               `db:"subdomain"`
	Kind           string               `db:"system_id"`
	Username       string               `db:"username"`
	ExternalID     constants.ExternalID `db:"external_id"`
	NotifyPeriodic bool                 `db:"notify_periodic"`
	PeriodicPeriod int                  `db:"periodic_period"`
}

// analysisColumns is the shared projection behind every *Analysis query.
//
// external_id comes from a LIMIT 1 subquery rather than a join so multi-step
// analyses yield exactly one row. It is COALESCEd to the empty string — an
// analysis with no job_steps row yet is skipped by callers rather than failing
// the whole sweep.
const analysisColumns = `
	SELECT jobs.id,
	       jobs.app_id,
	       jobs.user_id,
	       jobs.status,
	       jobs.job_description,
	       jobs.job_name,
	       jobs.result_folder_path,
	       jobs.start_date,
	       jobs.planned_end_date,
	       COALESCE(jobs.subdomain, '') AS subdomain,
	       job_types.system_id,
	       users.username,
	       COALESCE((SELECT js.external_id
	                   FROM job_steps js
	                  WHERE js.job_id = jobs.id
	               ORDER BY js.step_number
	                  LIMIT 1), '') AS external_id,
	       COALESCE((jobs.submission->>'notify_periodic')::bool, TRUE) AS notify_periodic,
	       COALESCE((jobs.submission->>'periodic_period')::int, 0) AS periodic_period
	  FROM jobs
	  JOIN job_types ON jobs.job_type_id = job_types.id
	  JOIN users ON jobs.user_id = users.id
`

// runningStatus is the jobs.status value for an analysis that is still up. Only
// running analyses are candidates for expiry warnings or termination.
const runningStatus = "Running"

// localizeTimestamps relabels the analysis's naive timestamps into the zone the
// DE wrote them in, which is what makes every duration derived from them — the
// termination grace period, the reminder pacing, the times in the notification
// emails — measure the interval the user actually sees.
func (a *Analysis) localizeTimestamps() {
	a.StartDate = InLocalZone(a.StartDate)
	a.PlannedEndDate = InLocalZone(a.PlannedEndDate)
}

// selectAnalyses runs one of the analysisColumns queries. Every multi-row
// Analysis query goes through it so that none can return timestamps that have
// not been relabeled.
func (d *Database) selectAnalyses(ctx context.Context, query string, args ...any) ([]Analysis, error) {
	analyses := []Analysis{}
	if err := d.db.SelectContext(ctx, &analyses, query, args...); err != nil {
		return nil, err
	}

	for i := range analyses {
		analyses[i].localizeTimestamps()
	}

	return analyses, nil
}

// The statements that compare or write one of the naive timestamp columns are
// package-level so the contract in db/timestamps.go can be asserted against them
// directly — the bug they guard against is invisible at the Go type level and
// only shows up as analyses terminating hours early.
const (
	expiredAnalysesQuery = analysisColumns + `
		 WHERE jobs.status = $1
		   AND jobs.planned_end_date <= $2::timestamp
	`

	expiringWithinQuery = analysisColumns + `
		 WHERE jobs.status = $1
		   AND jobs.planned_end_date > $2::timestamp
		   AND jobs.planned_end_date <= $3::timestamp
	`

	periodicReminderQuery = analysisColumns + `
		  LEFT JOIN notif_statuses ON jobs.id = notif_statuses.analysis_id
		 WHERE jobs.status = $1
		   AND jobs.start_date IS NOT NULL
		   AND jobs.planned_end_date > $2::timestamp
		   AND GREATEST(jobs.start_date, notif_statuses.last_periodic_warning) <
		       $2::timestamp - COALESCE(notif_statuses.periodic_warning_period, '4 hours'::interval)
	`

	initialRuntimeStmt = `
		UPDATE ONLY jobs
		   SET planned_end_date = COALESCE(
		           planned_end_date,
		           COALESCE(start_date, $4::timestamp) + make_interval(secs => $3)
		       ),
		       subdomain = COALESCE(NULLIF(subdomain, ''), $2)
		 WHERE id = $1
		   AND (planned_end_date IS NULL OR subdomain IS NULL OR subdomain = '')
	`
)

// ListExpiredAnalyses returns the running analyses whose planned end date has
// passed and which are therefore due for termination.
func (d *Database) ListExpiredAnalyses(ctx context.Context) ([]Analysis, error) {
	return d.selectAnalyses(ctx, expiredAnalysesQuery, runningStatus, time.Now())
}

// ListAnalysesExpiringWithin returns the running analyses whose planned end
// date falls between now and the given window — the candidates for an advance
// "your analysis will terminate" warning.
func (d *Database) ListAnalysesExpiringWithin(ctx context.Context, window time.Duration) ([]Analysis, error) {
	now := time.Now()
	return d.selectAnalyses(ctx, expiringWithinQuery, runningStatus, now, now.Add(window))
}

// ListAnalysesDueForPeriodicReminder returns the running analyses that have not
// yet expired and whose reminder period has elapsed.
//
// The period is measured from the analysis's last reminder, or from its start
// date when it has had none, which is what keeps a freshly launched analysis
// from being reminded immediately. That pacing has to match the worker's
// reminderDue check: an analysis returned here but not yet due costs a
// tracking-row insert and a row lock on every sweep, on every replica, and
// records nothing to stop it happening again ten seconds later. GREATEST
// ignores NULLs in Postgres, so it doubles as the has-no-reminder-yet case.
func (d *Database) ListAnalysesDueForPeriodicReminder(ctx context.Context) ([]Analysis, error) {
	return d.selectAnalyses(ctx, periodicReminderQuery, runningStatus, time.Now())
}

// HasCompletedStatus reports whether a Completed status has already been
// recorded for the given external ID.
//
// The expiration worker uses this to avoid re-publishing a Completed status it
// has already sent. It reads job_status_updates rather than jobs.status on
// purpose: the jobs row is updated by a separate pipeline that can lag or stall,
// and a stall is exactly the case where the worker would otherwise re-publish on
// every sweep forever.
func (d *Database) HasCompletedStatus(ctx context.Context, externalID constants.ExternalID) (bool, error) {
	tx, err := d.db.BeginTxx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback() //nolint:errcheck // rollback after commit is a no-op

	status, err := d.GetLatestStatusByExternalID(ctx, tx, externalID)
	if err != nil {
		// No status recorded yet means nothing has been published.
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}

	return status == messaging.SucceededState, nil
}

// interactiveAnalysis is the predicate identifying an analysis with a VICE
// step, written against jobs.id so the queries that must exclude batch analyses
// and IsInteractive share one definition of "interactive".
const interactiveAnalysis = `
	EXISTS (SELECT 1
	          FROM job_steps js
	          JOIN job_types jt ON js.job_type_id = jt.id
	         WHERE js.job_id = jobs.id
	           AND jt.name = 'Interactive')
`

// ListAnalysesMissingRuntime returns the running VICE analyses that reached
// Running without a subdomain or a planned end date.
//
// These are the analyses the launch-time write missed. Left alone, one is
// unroutable — the DE cannot build its access URL without a subdomain — and
// invisible to ListExpiredAnalyses, since a NULL planned end date never
// compares true against now(), so it would run until an admin killed it.
//
// Batch analyses are excluded: they legitimately have neither field, and
// without the filter every running HPC job would come back on every sweep.
func (d *Database) ListAnalysesMissingRuntime(ctx context.Context) ([]Analysis, error) {
	const query = analysisColumns + `
		 WHERE jobs.status = $1
		   AND (jobs.planned_end_date IS NULL OR COALESCE(jobs.subdomain, '') = '')
		   AND ` + interactiveAnalysis
	return d.selectAnalyses(ctx, query, runningStatus)
}

// GetAnalysisByExternalID returns the analysis owning the given external ID, or
// sql.ErrNoRows when no analysis has that external ID.
func (d *Database) GetAnalysisByExternalID(ctx context.Context, externalID constants.ExternalID) (*Analysis, error) {
	const query = analysisColumns + `
		 WHERE EXISTS (SELECT 1
		                 FROM job_steps js
		                WHERE js.job_id = jobs.id
		                  AND js.external_id = $1)
	`
	var analysis Analysis
	if err := d.db.GetContext(ctx, &analysis, query, externalID); err != nil {
		return nil, err
	}
	analysis.localizeTimestamps()
	return &analysis, nil
}

// IsInteractive reports whether any of the analysis's steps is a VICE
// (Interactive) step. An analysis the DE has no record of is not interactive.
func (d *Database) IsInteractive(ctx context.Context, analysisID constants.AnalysisID) (bool, error) {
	const query = `
		SELECT COALESCE((SELECT ` + interactiveAnalysis + `
		                   FROM jobs
		                  WHERE jobs.id = $1), FALSE)
	`
	var interactive bool
	err := d.db.QueryRowContext(ctx, query, analysisID).Scan(&interactive)
	return interactive, err
}

// DefaultToolTimeLimitSeconds is the runtime allowed for a tool that declares
// no time limit of its own — 72 hours.
const DefaultToolTimeLimitSeconds = 259200

// GetTimeLimitSeconds returns how long an analysis is allowed to run. A time
// limit requested at launch (initial_time_limit_seconds) wins; otherwise the
// limit is the sum of the tools' limits, with DefaultToolTimeLimitSeconds
// standing in for any tool that declares none.
func (d *Database) GetTimeLimitSeconds(ctx context.Context, analysisID constants.AnalysisID) (int64, error) {
	const query = `
		SELECT COALESCE(
		    jobs.initial_time_limit_seconds,
		    sum(CASE WHEN tools.time_limit_seconds > 0
		             THEN tools.time_limit_seconds
		             ELSE $2 END)
		)
		  FROM tools
		  JOIN tasks ON tools.id = tasks.tool_id
		  JOIN app_steps ON tasks.id = app_steps.task_id
		  JOIN jobs ON jobs.app_version_id = app_steps.app_version_id
		 WHERE jobs.id = $1
		 GROUP BY jobs.id
	`
	var seconds int64
	err := d.db.QueryRowContext(ctx, query, analysisID, DefaultToolTimeLimitSeconds).Scan(&seconds)
	return seconds, err
}

// SetInitialRuntime fills in the runtime fields the DE derives at launch:
// the analysis's subdomain and its planned end date, the latter computed as
// start_date (or now, if the analysis has no start date yet) plus
// timeLimitSeconds.
//
// Both columns are only filled when currently unset, which makes this safe to
// call more than once: it never clobbers a subdomain already in use for
// routing, and never rolls back a time-limit extension the user has been
// granted. Returns whether the statement changed anything, so callers can log
// when the launch-time write had to be backfilled later.
func (d *Database) SetInitialRuntime(ctx context.Context, analysisID constants.AnalysisID, subdomain string, timeLimitSeconds int64) (bool, error) {
	result, err := d.db.ExecContext(ctx, initialRuntimeStmt, analysisID, subdomain, timeLimitSeconds, time.Now())
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

// InitializeRuntime derives and stores the runtime fields the DE computes once
// per analysis: its routing subdomain and its planned end date. Safe to call
// repeatedly — neither column is overwritten once set.
//
// This is the single path for that write. The VICE launch handler calls it as
// the canonical writer; the job-status AMQP handler calls it again as a safety
// net for analyses that reached Running without it (an analysis launched by an
// older app-exposer, or one whose launch-time write failed). Returns whether
// anything changed, which is what lets the safety net log that it had to
// backfill.
func (d *Database) InitializeRuntime(ctx context.Context, analysisID constants.AnalysisID, userID, externalID string) (bool, error) {
	timeLimitSeconds, err := d.GetTimeLimitSeconds(ctx, analysisID)
	if err != nil {
		return false, fmt.Errorf("determining time limit for analysis %s: %w", analysisID, err)
	}
	return d.SetInitialRuntime(ctx, analysisID, common.Subdomain(userID, externalID), timeLimitSeconds)
}

// Compile-time check that *Database satisfies the expiration worker's needs.
var _ ExpirationDB = (*Database)(nil)
