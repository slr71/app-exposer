package db

import (
	"context"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/cyverse-de/app-exposer/common"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const timeLimitSQL = `
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

const initialRuntimeSQL = `
		UPDATE ONLY jobs
		   SET planned_end_date = COALESCE(
		           planned_end_date,
		           COALESCE(start_date, $4::timestamp) + make_interval(secs => $3)
		       ),
		       subdomain = COALESCE(NULLIF(subdomain, ''), $2)
		 WHERE id = $1
		   AND (planned_end_date IS NULL OR subdomain IS NULL OR subdomain = '')
	`

func TestGetTimeLimitSeconds(t *testing.T) {
	database, mock := newTestDB(t)

	mock.ExpectQuery(timeLimitSQL).
		WithArgs(testAnalysisID, DefaultToolTimeLimitSeconds).
		WillReturnRows(sqlmock.NewRows([]string{"coalesce"}).AddRow(int64(3600)))

	seconds, err := database.GetTimeLimitSeconds(context.Background(), testAnalysisID)

	require.NoError(t, err)
	assert.Equal(t, int64(3600), seconds)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSetInitialRuntime(t *testing.T) {
	tests := []struct {
		name        string
		rowsChanged int64
		execErr     error
		wantChanged bool
		wantErr     bool
	}{
		{
			name:        "reports a change when the columns were unset",
			rowsChanged: 1,
			wantChanged: true,
		},
		{
			// Both columns were already populated, so the guarded UPDATE
			// matched nothing. This is the steady-state result of the AMQP
			// safety net running after the launch handler already wrote them.
			name:        "reports no change when both columns were already set",
			rowsChanged: 0,
			wantChanged: false,
		},
		{
			name:    "surfaces a database error",
			execErr: errors.New("connection reset"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			database, mock := newTestDB(t)

			expectation := mock.ExpectExec(initialRuntimeSQL).
				WithArgs(testAnalysisID, "a1b2c3d4e", int64(3600), sqlmock.AnyArg())
			if tt.execErr != nil {
				expectation.WillReturnError(tt.execErr)
			} else {
				expectation.WillReturnResult(sqlmock.NewResult(0, tt.rowsChanged))
			}

			changed, err := database.SetInitialRuntime(context.Background(), testAnalysisID, "a1b2c3d4e", 3600)

			if tt.wantErr {
				require.Error(t, err)
				assert.False(t, changed)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantChanged, changed)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

// TestInitializeRuntimeUsesTheSharedSubdomain verifies that the runtime write
// derives the subdomain with common.Subdomain — the same helper the launch path
// uses for pod labels and gateway hostnames. If these two ever diverge, an
// analysis becomes unroutable, so the coupling is asserted explicitly.
func TestInitializeRuntimeUsesTheSharedSubdomain(t *testing.T) {
	database, mock := newTestDB(t)

	const userID = "user-1"
	const externalID = "external-1"
	wantSubdomain := common.Subdomain(userID, externalID)

	mock.ExpectQuery(timeLimitSQL).
		WithArgs(testAnalysisID, DefaultToolTimeLimitSeconds).
		WillReturnRows(sqlmock.NewRows([]string{"coalesce"}).AddRow(int64(259200)))
	mock.ExpectExec(initialRuntimeSQL).
		WithArgs(testAnalysisID, wantSubdomain, int64(259200), sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	changed, err := database.InitializeRuntime(context.Background(), testAnalysisID, userID, externalID)

	require.NoError(t, err)
	assert.True(t, changed)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInitializeRuntimeSurfacesATimeLimitFailure(t *testing.T) {
	database, mock := newTestDB(t)

	mock.ExpectQuery(timeLimitSQL).
		WithArgs(testAnalysisID, DefaultToolTimeLimitSeconds).
		WillReturnError(errors.New("no such analysis"))

	changed, err := database.InitializeRuntime(context.Background(), testAnalysisID, "user-1", "external-1")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "determining time limit")
	assert.False(t, changed)
}

// newRegexpTestDB matches expectations as regular expressions so a test can pin
// the one predicate it cares about instead of restating the whole shared
// analysis projection.
func newRegexpTestDB(t *testing.T) (*Database, sqlmock.Sqlmock) {
	t.Helper()
	rawDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	require.NoError(t, err)
	t.Cleanup(func() { _ = rawDB.Close() })
	return New(sqlx.NewDb(rawDB, "postgres"), ""), mock
}

// TestListAnalysesDueForPeriodicReminderPacesFromTheLastReminder pins the
// pacing to match the worker's reminderDue check. An analysis returned here but
// not actually due costs a tracking-row insert and a row lock on every sweep, on
// every replica, and records nothing that would stop it happening again ten
// seconds later.
func TestListAnalysesDueForPeriodicReminderPacesFromTheLastReminder(t *testing.T) {
	database, mock := newRegexpTestDB(t)

	mock.ExpectQuery(`GREATEST\(jobs\.start_date, notif_statuses\.last_periodic_warning\)\s+<\s+\$2::timestamp - COALESCE\(notif_statuses\.periodic_warning_period`).
		WithArgs(runningStatus, sqlmock.AnyArg()).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	analyses, err := database.ListAnalysesDueForPeriodicReminder(context.Background())

	require.NoError(t, err)
	assert.Empty(t, analyses)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestListAnalysesMissingRuntimeExcludesBatchAnalyses pins the interactive
// filter. Batch analyses legitimately have neither a subdomain nor a planned end
// date, so without it every running HPC job would come back on every sweep.
func TestListAnalysesMissingRuntimeExcludesBatchAnalyses(t *testing.T) {
	database, mock := newRegexpTestDB(t)

	mock.ExpectQuery(`planned_end_date IS NULL OR COALESCE\(jobs\.subdomain, ''\) = ''[\s\S]*jt\.name = 'Interactive'`).
		WithArgs(runningStatus).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))

	analyses, err := database.ListAnalysesMissingRuntime(context.Background())

	require.NoError(t, err)
	assert.Empty(t, analyses)
	assert.NoError(t, mock.ExpectationsWereMet())
}
