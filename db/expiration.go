package db

import (
	"context"
	"fmt"
	"time"

	"github.com/cyverse-de/app-exposer/common"
	"github.com/cyverse-de/app-exposer/constants"
)

// Analysis carries the jobs-table fields needed to decide whether an analysis
// has reached its time limit and to describe it in a user notification.
//
// StartDate and PlannedEndDate are nullable: an analysis that has not started,
// or whose planned end date has not been set yet, leaves them nil. Both are
// selected through AT TIME ZONE current_setting('TimeZone') so the naive
// `timestamp` columns arrive as the absolute instants they were written as —
// the columns hold local wall-clock time, so reading them without the
// conversion would silently shift every duration by the local UTC offset.
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
	       jobs.start_date AT TIME ZONE current_setting('TimeZone') AS start_date,
	       jobs.planned_end_date AT TIME ZONE current_setting('TimeZone') AS planned_end_date,
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

// ListExpiredAnalyses returns the running analyses whose planned end date has
// passed and which are therefore due for termination.
func (d *Database) ListExpiredAnalyses(ctx context.Context) ([]Analysis, error) {
	const query = analysisColumns + `
		 WHERE jobs.status = $1
		   AND jobs.planned_end_date <= now()
	`
	analyses := []Analysis{}
	err := d.db.SelectContext(ctx, &analyses, query, runningStatus)
	return analyses, err
}

// ListAnalysesExpiringWithin returns the running analyses whose planned end
// date falls between now and the given window — the candidates for an advance
// "your analysis will terminate" warning.
func (d *Database) ListAnalysesExpiringWithin(ctx context.Context, window time.Duration) ([]Analysis, error) {
	const query = analysisColumns + `
		 WHERE jobs.status = $1
		   AND jobs.planned_end_date > now()
		   AND jobs.planned_end_date <= now() + make_interval(secs => $2)
	`
	analyses := []Analysis{}
	err := d.db.SelectContext(ctx, &analyses, query, runningStatus, window.Seconds())
	return analyses, err
}

// ListAnalysesDueForPeriodicReminder returns the running analyses that have not
// yet expired and whose last periodic reminder is older than their configured
// reminder period. Analyses with no notif_statuses row yet are included: their
// first reminder is due immediately.
func (d *Database) ListAnalysesDueForPeriodicReminder(ctx context.Context) ([]Analysis, error) {
	const query = analysisColumns + `
		  LEFT JOIN notif_statuses ON jobs.id = notif_statuses.analysis_id
		 WHERE jobs.status = $1
		   AND jobs.planned_end_date > now()
		   AND (notif_statuses.last_periodic_warning IS NULL
		    OR notif_statuses.last_periodic_warning <
		       now() - COALESCE(notif_statuses.periodic_warning_period, '4 hours'::interval))
	`
	analyses := []Analysis{}
	err := d.db.SelectContext(ctx, &analyses, query, runningStatus)
	return analyses, err
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
	return &analysis, nil
}

// IsInteractive reports whether any of the analysis's steps is a VICE
// (Interactive) step.
func (d *Database) IsInteractive(ctx context.Context, analysisID constants.AnalysisID) (bool, error) {
	const query = `
		SELECT EXISTS (SELECT 1
		                 FROM job_steps js
		                 JOIN job_types jt ON js.job_type_id = jt.id
		                WHERE js.job_id = $1
		                  AND jt.name = 'Interactive')
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
	const stmt = `
		UPDATE ONLY jobs
		   SET planned_end_date = COALESCE(
		           planned_end_date,
		           COALESCE(start_date, now()::timestamp) + make_interval(secs => $3)
		       ),
		       subdomain = COALESCE(NULLIF(subdomain, ''), $2)
		 WHERE id = $1
		   AND (planned_end_date IS NULL OR subdomain IS NULL OR subdomain = '')
	`
	result, err := d.db.ExecContext(ctx, stmt, analysisID, subdomain, timeLimitSeconds)
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
