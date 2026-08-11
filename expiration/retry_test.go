package expiration

import (
	"testing"
	"time"

	"github.com/cyverse-de/app-exposer/db"
	"github.com/stretchr/testify/assert"
)

// TestRetryBackoffWidensWithFailures covers the pacing that makes
// MaxNotificationAttempts a measure of elapsed time rather than of sweep count.
// The sweep runs every ten seconds, so without this an unreachable
// notification-agent spends the whole attempt ceiling in half a minute.
func TestRetryBackoffWidensWithFailures(t *testing.T) {
	tests := []struct {
		name     string
		failures int
		want     time.Duration
	}{
		{name: "a count below the first attempt still waits the base delay", failures: 0, want: retryBackoffBase},
		{name: "the first failure waits the base delay", failures: 1, want: retryBackoffBase},
		{name: "the second failure waits twice as long", failures: 2, want: 2 * retryBackoffBase},
		{name: "the fourth failure waits eight times as long", failures: 4, want: 8 * retryBackoffBase},
		{name: "the delay is capped", failures: 12, want: retryBackoffMax},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, retryBackoff(tt.failures))
		})
	}
}

// TestRetryBackoffReachesTheCeilingSlowly pins the property the ceiling depends
// on: spending every attempt must take hours, not the seconds it takes to run
// MaxNotificationAttempts sweeps.
func TestRetryBackoffReachesTheCeilingSlowly(t *testing.T) {
	var total time.Duration
	for failures := 1; failures < MaxNotificationAttempts; failures++ {
		total += retryBackoff(failures)
	}

	assert.Greater(t, total, time.Hour,
		"a brief notification-agent restart must not be able to spend the attempt ceiling")
}

func TestRetryTracker(t *testing.T) {
	key := retryKey{analysisID: "analysis-1", kind: db.HourWarning}
	now := time.Now()

	t.Run("a notification with no recorded failure is due", func(t *testing.T) {
		assert.True(t, newRetryTracker().due(key, now))
	})

	t.Run("a scheduled retry is held back until its backoff elapses", func(t *testing.T) {
		r := newRetryTracker()
		r.schedule(key, 1, now)

		assert.False(t, r.due(key, now.Add(retryBackoffBase/2)))
		assert.True(t, r.due(key, now.Add(retryBackoffBase)))
	})

	t.Run("the two warning kinds are paced independently", func(t *testing.T) {
		r := newRetryTracker()
		r.schedule(key, 1, now)

		dayKey := retryKey{analysisID: key.analysisID, kind: db.DayWarning}
		assert.True(t, r.due(dayKey, now))
	})

	t.Run("clearing a retry makes the notification due again", func(t *testing.T) {
		r := newRetryTracker()
		r.schedule(key, 1, now)
		r.clear(key)

		assert.True(t, r.due(key, now))
	})

	t.Run("pruning drops entries long past their retry time", func(t *testing.T) {
		r := newRetryTracker()
		r.schedule(key, 1, now)

		r.prune(now.Add(retryEntryTTL / 2))
		assert.Len(t, r.nextTime, 1, "a pending backoff must survive pruning")

		r.prune(now.Add(retryEntryTTL * 2))
		assert.Empty(t, r.nextTime, "an entry nothing came back for must not be kept forever")
	})
}
