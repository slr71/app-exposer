package httphandlers

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/cyverse-de/app-exposer/apps"
	"github.com/cyverse-de/app-exposer/operatorclient"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestOperatorClientForAnalysisWithNoOperators covers the answer the handlers
// give while the scheduler is empty, which it is on every restart until the
// reconciler has synced the operators table.
//
// An empty scheduler means the analysis is not running anywhere, and the
// handlers turn a non-nil error into a 503. Reporting one here tells the apps
// service that a save-and-exit is worth retrying, and turns
// /vice/admin/is-deployed — an "exists?" question with a perfectly good answer —
// into an outage.
func TestOperatorClientForAnalysisWithNoOperators(t *testing.T) {
	rawDB, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rawDB.Close() })

	// No recorded operator id, which is what sends the lookup down the search
	// path in the first place.
	mock.ExpectQuery("SELECT operator_id").
		WillReturnRows(sqlmock.NewRows([]string{"operator_id"}).AddRow(nil))

	h := &HTTPHandlers{
		apps:      &apps.Apps{DB: sqlx.NewDb(rawDB, "postgres")},
		scheduler: operatorclient.NewScheduler(nil),
	}

	client, err := h.operatorClientForAnalysis(context.Background(), "analysis-1")

	require.NoError(t, err, "an empty scheduler is an answer, not a failed lookup")
	assert.Nil(t, client)
	assert.NoError(t, mock.ExpectationsWereMet())
}
