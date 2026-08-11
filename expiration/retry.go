package expiration

import (
	"sync"
	"time"

	"github.com/cyverse-de/app-exposer/constants"
	"github.com/cyverse-de/app-exposer/db"
)

// Notification retries are paced so that the attempt ceiling measures elapsed
// time rather than sweep count. The sweep runs every ten seconds, so an
// unpaced ceiling of three attempts is spent by a notification-agent restart —
// half a minute of unavailability would permanently abandon every warning and
// termination notice in flight at the time.
//
// The delay doubles with each failure, up to retryBackoffMax, which puts the
// ceiling roughly two and a half hours out. Pacing is per replica: the attempt
// count is shared through notif_statuses, but when each attempt happens is not,
// so N replicas can spend the ceiling up to N times faster. That is a bounded
// loss of patience, not the cliff the sweep cadence created.
const (
	retryBackoffBase = time.Minute
	retryBackoffMax  = 30 * time.Minute

	// retryEntryTTL bounds how long a pending retry is remembered for an
	// analysis nothing asks about again — one whose analysis was deleted, say.
	// Well past retryBackoffMax, so it never discards a live backoff.
	retryEntryTTL = 2 * time.Hour
)

// retryKey identifies one notification: the same analysis can have a day
// warning in backoff while its hour warning is still on its first attempt.
type retryKey struct {
	analysisID constants.AnalysisID
	kind       db.WarningKind
}

// retryTracker holds the earliest time each failed notification may be tried
// again. It is in-memory on purpose: the durable state that matters — how many
// attempts have failed and whether the notification was ever delivered — lives
// in notif_statuses, and losing the pacing on a restart costs one early retry.
type retryTracker struct {
	mu       sync.Mutex
	nextTime map[retryKey]time.Time
}

func newRetryTracker() *retryTracker {
	return &retryTracker{nextTime: map[retryKey]time.Time{}}
}

// due reports whether the notification may be attempted now.
func (r *retryTracker) due(key retryKey, now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	next, pending := r.nextTime[key]
	if !pending {
		return true
	}
	if now.Before(next) {
		return false
	}
	delete(r.nextTime, key)
	return true
}

// schedule holds the notification back until the backoff for the given number
// of consecutive failures has elapsed.
func (r *retryTracker) schedule(key retryKey, failures int, now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nextTime[key] = now.Add(retryBackoff(failures))
}

// clear forgets any pending backoff, which is what a delivered — or abandoned —
// notification leaves behind.
func (r *retryTracker) clear(key retryKey) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.nextTime, key)
}

// prune drops entries whose analyses stopped coming back from the sweep, so the
// tracker can't grow without bound over the process's lifetime.
func (r *retryTracker) prune(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, next := range r.nextTime {
		if now.Sub(next) > retryEntryTTL {
			delete(r.nextTime, key)
		}
	}
}

// retryBackoff returns how long to wait after the given number of consecutive
// delivery failures.
func retryBackoff(failures int) time.Duration {
	if failures < 1 {
		return retryBackoffBase
	}

	backoff := retryBackoffBase
	for range failures - 1 {
		backoff *= 2
		if backoff >= retryBackoffMax {
			return retryBackoffMax
		}
	}
	return backoff
}
