package db

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/cyverse-de/app-exposer/constants"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testAnalysisID = constants.AnalysisID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

// claimSQL and existsSQL mirror the statements in ClaimNotifStatuses. They are
// restated here so an unintended change to either statement fails the test
// rather than silently altering the locking behavior.
const claimSQL = `
		SELECT analysis_id,
		       external_id,
		       hour_warning_sent,
		       hour_warning_failure_count,
		       day_warning_sent,
		       day_warning_failure_count,
		       kill_warning_sent,
		       kill_warning_failure_count,
		       last_periodic_warning,
		       COALESCE(EXTRACT(EPOCH FROM periodic_warning_period), 0)::bigint AS periodic_warning_seconds
		  FROM notif_statuses
		 WHERE analysis_id = $1
		   FOR UPDATE SKIP LOCKED
	`

const existsSQL = `SELECT EXISTS (SELECT 1 FROM notif_statuses WHERE analysis_id = $1)`

func claimColumns() []string {
	return []string{
		"analysis_id", "external_id",
		"hour_warning_sent", "hour_warning_failure_count",
		"day_warning_sent", "day_warning_failure_count",
		"kill_warning_sent", "kill_warning_failure_count",
		"last_periodic_warning", "periodic_warning_seconds",
	}
}

// TestClaimNotifStatuses covers the concurrency contract that keeps two
// app-exposer replicas from sending the same notification twice.
func TestClaimNotifStatuses(t *testing.T) {
	t.Run("runs the callback and commits when the row is claimed", func(t *testing.T) {
		database, mock := newTestDB(t)

		mock.ExpectBegin()
		mock.ExpectQuery(claimSQL).
			WithArgs(testAnalysisID).
			WillReturnRows(sqlmock.NewRows(claimColumns()).
				AddRow(string(testAnalysisID), "external-1", true, 2, false, 0, false, 0, nil, int64(7200)))
		mock.ExpectCommit()

		var seen *NotifStatuses
		err := database.ClaimNotifStatuses(context.Background(), testAnalysisID, func(_ *sqlx.Tx, statuses *NotifStatuses) error {
			seen = statuses
			return nil
		})

		require.NoError(t, err)
		require.NotNil(t, seen)
		assert.True(t, seen.HourWarningSent)
		assert.Equal(t, 2, seen.HourWarningFailureCount)
		assert.Equal(t, int64(7200), seen.PeriodicWarningSeconds)
		assert.Nil(t, seen.LastPeriodicWarning)
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns ErrNotClaimed when another replica holds the row", func(t *testing.T) {
		database, mock := newTestDB(t)

		mock.ExpectBegin()
		mock.ExpectQuery(claimSQL).
			WithArgs(testAnalysisID).
			WillReturnRows(sqlmock.NewRows(claimColumns()))
		// The row exists, so the empty result means it was locked elsewhere.
		mock.ExpectQuery(existsSQL).
			WithArgs(testAnalysisID).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
		mock.ExpectRollback()

		called := false
		err := database.ClaimNotifStatuses(context.Background(), testAnalysisID, func(*sqlx.Tx, *NotifStatuses) error {
			called = true
			return nil
		})

		require.ErrorIs(t, err, ErrNotClaimed)
		assert.False(t, called, "the callback must not run when the claim is lost")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("returns ErrNoRows when the row genuinely does not exist", func(t *testing.T) {
		database, mock := newTestDB(t)

		mock.ExpectBegin()
		mock.ExpectQuery(claimSQL).
			WithArgs(testAnalysisID).
			WillReturnRows(sqlmock.NewRows(claimColumns()))
		mock.ExpectQuery(existsSQL).
			WithArgs(testAnalysisID).
			WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
		mock.ExpectRollback()

		err := database.ClaimNotifStatuses(context.Background(), testAnalysisID, func(*sqlx.Tx, *NotifStatuses) error {
			return nil
		})

		require.ErrorIs(t, err, sql.ErrNoRows,
			"a missing row must surface as an error rather than being skipped forever")
		assert.NoError(t, mock.ExpectationsWereMet())
	})

	t.Run("rolls back when the callback fails", func(t *testing.T) {
		database, mock := newTestDB(t)
		sentinel := errors.New("notification-agent is down")

		mock.ExpectBegin()
		mock.ExpectQuery(claimSQL).
			WithArgs(testAnalysisID).
			WillReturnRows(sqlmock.NewRows(claimColumns()).
				AddRow(string(testAnalysisID), "external-1", false, 0, false, 0, false, 0, nil, int64(0)))
		mock.ExpectRollback()

		err := database.ClaimNotifStatuses(context.Background(), testAnalysisID, func(*sqlx.Tx, *NotifStatuses) error {
			return sentinel
		})

		require.ErrorIs(t, err, sentinel)
		assert.NoError(t, mock.ExpectationsWereMet())
	})
}

func TestEnsureNotifStatuses(t *testing.T) {
	const expectedSQL = `
		INSERT INTO notif_statuses (analysis_id, external_id, periodic_warning_period)
		VALUES ($1, $2, CAST($3 AS interval))
		ON CONFLICT (analysis_id) DO NOTHING
	`

	tests := []struct {
		name        string
		periodSecs  int
		wantsPeriod string
	}{
		{name: "a requested period is stored as seconds", periodSecs: 900, wantsPeriod: "900 seconds"},
		{name: "zero selects the four-hour default", periodSecs: 0, wantsPeriod: "4 hours"},
		{name: "a negative period selects the default", periodSecs: -5, wantsPeriod: "4 hours"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			database, mock := newTestDB(t)

			mock.ExpectExec(expectedSQL).
				WithArgs(testAnalysisID, constants.ExternalID("external-1"), tt.wantsPeriod).
				WillReturnResult(sqlmock.NewResult(0, 1))

			err := database.EnsureNotifStatuses(context.Background(), testAnalysisID, "external-1", tt.periodSecs)

			require.NoError(t, err)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSetWarningSentAndFailureCount(t *testing.T) {
	tests := []struct {
		name         string
		kind         WarningKind
		wantSentSQL  string
		wantCountSQL string
	}{
		{
			name:         "day",
			kind:         DayWarning,
			wantSentSQL:  `UPDATE notif_statuses SET day_warning_sent = $2 WHERE analysis_id = $1`,
			wantCountSQL: `UPDATE notif_statuses SET day_warning_failure_count = $2 WHERE analysis_id = $1`,
		},
		{
			name:         "hour",
			kind:         HourWarning,
			wantSentSQL:  `UPDATE notif_statuses SET hour_warning_sent = $2 WHERE analysis_id = $1`,
			wantCountSQL: `UPDATE notif_statuses SET hour_warning_failure_count = $2 WHERE analysis_id = $1`,
		},
		{
			name:         "kill",
			kind:         KillWarning,
			wantSentSQL:  `UPDATE notif_statuses SET kill_warning_sent = $2 WHERE analysis_id = $1`,
			wantCountSQL: `UPDATE notif_statuses SET kill_warning_failure_count = $2 WHERE analysis_id = $1`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			database, mock := newTestDB(t)

			mock.ExpectBegin()
			mock.ExpectExec(tt.wantSentSQL).
				WithArgs(testAnalysisID, true).
				WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectExec(tt.wantCountSQL).
				WithArgs(testAnalysisID, 3).
				WillReturnResult(sqlmock.NewResult(0, 1))
			mock.ExpectCommit()

			tx, err := database.db.BeginTxx(context.Background(), nil)
			require.NoError(t, err)

			require.NoError(t, database.SetWarningSent(context.Background(), tx, tt.kind, testAnalysisID, true))
			require.NoError(t, database.SetWarningFailureCount(context.Background(), tx, tt.kind, testAnalysisID, 3))
			require.NoError(t, tx.Commit())

			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestWarningStatementsRejectAnUnknownKind(t *testing.T) {
	database, mock := newTestDB(t)

	mock.ExpectBegin()
	mock.ExpectRollback()

	tx, err := database.db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	err = database.SetWarningSent(context.Background(), tx, WarningKind("bogus"), testAnalysisID, true)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown warning kind")

	err = database.SetWarningFailureCount(context.Background(), tx, WarningKind("bogus"), testAnalysisID, 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown warning kind")
}

func TestSetLastPeriodicWarning(t *testing.T) {
	database, mock := newTestDB(t)

	// The timestamp comes from Go and is cast to a naive timestamp, which stores
	// the deployment's wall clock rather than the database session zone's.
	mock.ExpectBegin()
	mock.ExpectExec(`UPDATE notif_statuses SET last_periodic_warning = $2::timestamp WHERE analysis_id = $1`).
		WithArgs(testAnalysisID, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	tx, err := database.db.BeginTxx(context.Background(), nil)
	require.NoError(t, err)

	require.NoError(t, database.SetLastPeriodicWarning(context.Background(), tx, testAnalysisID))
	require.NoError(t, tx.Commit())

	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestNotifStatusesAccessors(t *testing.T) {
	statuses := &NotifStatuses{
		HourWarningSent:         true,
		HourWarningFailureCount: 1,
		DayWarningSent:          false,
		DayWarningFailureCount:  2,
		KillWarningSent:         true,
		KillWarningFailureCount: 3,
	}

	tests := []struct {
		name         string
		kind         WarningKind
		wantSent     bool
		wantFailures int
	}{
		{name: "hour", kind: HourWarning, wantSent: true, wantFailures: 1},
		{name: "day", kind: DayWarning, wantSent: false, wantFailures: 2},
		{name: "kill", kind: KillWarning, wantSent: true, wantFailures: 3},
		{name: "unknown kinds read as unsent", kind: WarningKind("bogus"), wantSent: false, wantFailures: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantSent, statuses.Sent(tt.kind))
			assert.Equal(t, tt.wantFailures, statuses.FailureCount(tt.kind))
		})
	}
}
