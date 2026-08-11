package incluster

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPlannedEndEpoch pins the conversion behind the VICE time-limit API. The
// jobs.planned_end_date column is naive and holds wall-clock time in the
// deployment's zone; converting it with the database session's zone instead
// returned an epoch off by the difference between the two, so the UI showed a
// time limit hours away from the one actually enforced.
func TestPlannedEndEpoch(t *testing.T) {
	phoenix, err := time.LoadLocation("America/Phoenix")
	require.NoError(t, err)

	// 16:00:31 wall clock, as the column stores it.
	stored := time.Date(2026, 8, 11, 16, 0, 31, 0, time.UTC)

	tests := []struct {
		name       string
		plannedEnd sql.NullTime
		wantOK     bool
		// wantEpochIn is the zone the wall clock is expected to be read in; the
		// conversion is relative to whatever zone the process runs in.
		wantEpochIn *time.Location
		// wantRoundedUp pins the fractional-second handling inherited from the
		// EXTRACT(EPOCH ...)::bigint this replaced.
		wantRoundedUp bool
	}{
		{
			name:        "a stored wall clock converts using the deployment's zone",
			plannedEnd:  sql.NullTime{Time: stored, Valid: true},
			wantOK:      true,
			wantEpochIn: time.Local,
		},
		{
			name:          "fractional seconds round rather than truncate",
			plannedEnd:    sql.NullTime{Time: stored.Add(658301 * time.Microsecond), Valid: true},
			wantOK:        true,
			wantEpochIn:   time.Local,
			wantRoundedUp: true,
		},
		{
			name:       "a NULL planned end date reports no epoch",
			plannedEnd: sql.NullTime{Valid: false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			epoch, ok := plannedEndEpoch(tt.plannedEnd)

			assert.Equal(t, tt.wantOK, ok)
			if !tt.wantOK {
				assert.Zero(t, epoch)
				return
			}

			want := time.Date(
				stored.Year(), stored.Month(), stored.Day(),
				stored.Hour(), stored.Minute(), stored.Second(), 0,
				tt.wantEpochIn,
			).Unix()
			if tt.wantRoundedUp {
				want++
			}
			assert.Equal(t, want, epoch)
		})
	}

	// The bug concretely: reading the same stored value as UTC rather than as a
	// US zone yields an epoch that is hours early.
	t.Run("reading the wall clock as UTC loses the zone offset", func(t *testing.T) {
		asPhoenix := time.Date(2026, 8, 11, 16, 0, 31, 0, phoenix).Unix()
		assert.Equal(t, int64(7*3600), asPhoenix-stored.Unix(),
			"the offset is exactly what the old current_setting('TimeZone') form dropped")
	})
}
