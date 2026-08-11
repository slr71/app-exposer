package db

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func timePtr(t time.Time) *time.Time { return &t }

// TestInZonePreservesTheWallClock covers the relabeling that makes the DE's
// naive timestamp columns usable as instants. lib/pq hands these values back
// labeled UTC whatever zone they were written in, so the wall clock has to be
// kept and the label replaced — not converted, which would move the wall clock
// and leave the instant just as wrong.
func TestInZonePreservesTheWallClock(t *testing.T) {
	phoenix, err := time.LoadLocation("America/Phoenix")
	require.NoError(t, err)

	tests := []struct {
		name string
		in   *time.Time
		loc  *time.Location
		// wantOffsetHours is the difference between the relabeled instant and
		// the same wall clock read as UTC.
		wantOffsetHours int
	}{
		{
			name:            "a naive value relabeled into a western zone moves the instant later",
			in:              timePtr(time.Date(2026, 8, 11, 15, 31, 44, 0, time.UTC)),
			loc:             phoenix,
			wantOffsetHours: 7,
		},
		{
			name:            "relabeling into UTC is a no-op",
			in:              timePtr(time.Date(2026, 8, 11, 15, 31, 44, 0, time.UTC)),
			loc:             time.UTC,
			wantOffsetHours: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := inZone(tt.in, tt.loc)
			require.NotNil(t, got)

			assert.Equal(t, tt.in.Format("2006-01-02T15:04:05"), got.Format("2006-01-02T15:04:05"),
				"the wall clock must be preserved exactly")
			assert.Equal(t, tt.loc, got.Location())
			assert.Equal(t, time.Duration(tt.wantOffsetHours)*time.Hour, got.Sub(*tt.in),
				"the instant must move by the zone's offset")
		})
	}
}

func TestInZonePassesNilThrough(t *testing.T) {
	assert.Nil(t, inZone(nil, time.UTC))
	assert.Nil(t, InLocalZone(nil))
}

// TestNaiveTimestampsAreNeverComparedAgainstNow is a regression guard on the bug
// that terminated live analyses: comparing one of these naive columns against
// SQL now() makes Postgres resolve it in the database session's zone rather than
// the deployment's, so on a UTC session every analysis looked hours older than
// it was. The cutoffs must come from Go and be cast with ::timestamp.
func TestNaiveTimestampsAreNeverComparedAgainstNow(t *testing.T) {
	tests := []struct {
		name  string
		query string
	}{
		{name: "expired", query: expiredAnalysesQuery},
		{name: "expiring within a window", query: expiringWithinQuery},
		{name: "due for a periodic reminder", query: periodicReminderQuery},
		{name: "initial runtime write", query: initialRuntimeStmt},
		{name: "last periodic warning write", query: lastPeriodicWarningStmt},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotContains(t, tt.query, "now()",
				"the database clock resolves naive columns in the session zone")
			assert.NotContains(t, tt.query, "current_setting('TimeZone')",
				"the session zone is not the zone these columns were written in")
			assert.Contains(t, tt.query, "::timestamp",
				"the Go-supplied cutoff must be cast to a naive timestamp")
		})
	}
}
