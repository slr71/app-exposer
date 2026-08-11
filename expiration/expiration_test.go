package expiration

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/cyverse-de/app-exposer/constants"
	"github.com/cyverse-de/app-exposer/db"
	"github.com/cyverse-de/app-exposer/operatorclient"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// expiringWindow records one ListAnalysesExpiringWithin call, so tests can
// assert on the windows the sweep asks for as well as on what it does with the
// answers.
type expiringWindow struct {
	kind db.WarningKind
	from time.Duration
	to   time.Duration
}

// fakeExpirationDB implements db.ExpirationDB for unit tests. It records the
// bookkeeping the worker performs so assertions can be made against it, and
// per-method errors can be injected to exercise the failure paths.
type fakeExpirationDB struct {
	expiring       []db.Analysis
	expired        []db.Analysis
	periodic       []db.Analysis
	missingRuntime []db.Analysis

	byExtID   map[constants.ExternalID]*db.Analysis
	statuses  map[constants.AnalysisID]*db.NotifStatuses
	claimedBy map[constants.AnalysisID]bool // true => another replica holds the row

	interactive bool

	// completedStatusFor records the external IDs already reported Completed.
	completedStatusFor map[constants.ExternalID]bool

	// Injected errors (nil by default).
	expiringErr        error
	expiredErr         error
	periodicErr        error
	missingRuntimeErr  error
	ensureErr          error
	initErr            error
	completedStatusErr error

	// Recorded side effects.
	calls           []string
	expiringWindows []expiringWindow
	ensured         []constants.AnalysisID
	sentFlags       map[string]bool
	failureCounts   map[string]int
	lastPeriodic    map[constants.AnalysisID]time.Time
	initRuntimeFor  []constants.AnalysisID
	initChanged     bool
	commits         int
}

func newFakeDB() *fakeExpirationDB {
	return &fakeExpirationDB{
		byExtID:       map[constants.ExternalID]*db.Analysis{},
		statuses:      map[constants.AnalysisID]*db.NotifStatuses{},
		claimedBy:     map[constants.AnalysisID]bool{},
		sentFlags:     map[string]bool{},
		failureCounts: map[string]int{},
		lastPeriodic:  map[constants.AnalysisID]time.Time{},

		completedStatusFor: map[constants.ExternalID]bool{},
	}
}

func flagKey(kind db.WarningKind, id constants.AnalysisID) string {
	return string(kind) + ":" + string(id)
}

func (f *fakeExpirationDB) ListExpiredAnalyses(context.Context) ([]db.Analysis, error) {
	f.calls = append(f.calls, "expired")
	return f.expired, f.expiredErr
}

func (f *fakeExpirationDB) ListAnalysesExpiringWithin(_ context.Context, kind db.WarningKind, from, to time.Duration) ([]db.Analysis, error) {
	f.calls = append(f.calls, "expiring:"+string(kind))
	f.expiringWindows = append(f.expiringWindows, expiringWindow{kind: kind, from: from, to: to})
	return f.expiring, f.expiringErr
}

func (f *fakeExpirationDB) ListAnalysesDueForPeriodicReminder(context.Context) ([]db.Analysis, error) {
	f.calls = append(f.calls, "periodic")
	return f.periodic, f.periodicErr
}

func (f *fakeExpirationDB) ListAnalysesMissingRuntime(context.Context) ([]db.Analysis, error) {
	return f.missingRuntime, f.missingRuntimeErr
}

func (f *fakeExpirationDB) GetAnalysisByExternalID(_ context.Context, externalID constants.ExternalID) (*db.Analysis, error) {
	analysis, ok := f.byExtID[externalID]
	if !ok {
		return nil, sql.ErrNoRows
	}
	return analysis, nil
}

func (f *fakeExpirationDB) IsInteractive(context.Context, constants.AnalysisID) (bool, error) {
	return f.interactive, nil
}

func (f *fakeExpirationDB) HasCompletedStatus(_ context.Context, externalID constants.ExternalID) (bool, error) {
	if f.completedStatusErr != nil {
		return false, f.completedStatusErr
	}
	return f.completedStatusFor[externalID], nil
}

func (f *fakeExpirationDB) InitializeRuntime(_ context.Context, analysisID constants.AnalysisID, _, _ string) (bool, error) {
	f.initRuntimeFor = append(f.initRuntimeFor, analysisID)
	return f.initChanged, f.initErr
}

func (f *fakeExpirationDB) EnsureNotifStatuses(_ context.Context, analysisID constants.AnalysisID, externalID constants.ExternalID, _ int) error {
	if f.ensureErr != nil {
		return f.ensureErr
	}
	f.ensured = append(f.ensured, analysisID)
	if _, ok := f.statuses[analysisID]; !ok {
		f.statuses[analysisID] = &db.NotifStatuses{AnalysisID: analysisID, ExternalID: externalID}
	}
	return nil
}

// ClaimNotifStatuses mimics the FOR UPDATE SKIP LOCKED claim: a row marked as
// held by another replica yields ErrNotClaimed, a missing row yields
// sql.ErrNoRows, and the callback otherwise runs against the stored statuses.
// The fake passes a nil transaction, which the worker only ever forwards back
// into this same fake.
func (f *fakeExpirationDB) ClaimNotifStatuses(_ context.Context, analysisID constants.AnalysisID, fn func(*sqlx.Tx, *db.NotifStatuses) error) error {
	if f.claimedBy[analysisID] {
		return db.ErrNotClaimed
	}
	statuses, ok := f.statuses[analysisID]
	if !ok {
		return sql.ErrNoRows
	}
	if err := fn(nil, statuses); err != nil {
		return err
	}
	f.commits++
	return nil
}

func (f *fakeExpirationDB) SetWarningSent(_ context.Context, _ *sqlx.Tx, kind db.WarningKind, analysisID constants.AnalysisID, sent bool) error {
	f.sentFlags[flagKey(kind, analysisID)] = sent
	return nil
}

func (f *fakeExpirationDB) SetWarningFailureCount(_ context.Context, _ *sqlx.Tx, kind db.WarningKind, analysisID constants.AnalysisID, count int) error {
	f.failureCounts[flagKey(kind, analysisID)] = count
	return nil
}

func (f *fakeExpirationDB) SetLastPeriodicWarning(_ context.Context, _ *sqlx.Tx, analysisID constants.AnalysisID) error {
	f.lastPeriodic[analysisID] = time.Now()
	return nil
}

// fakeNotifier records which notifications were requested and can be made to
// fail.
type fakeNotifier struct {
	err error

	terminated   []constants.AnalysisID
	expiringSoon []constants.AnalysisID
	stillRunning []constants.AnalysisID
}

func (f *fakeNotifier) NotifyTerminated(_ context.Context, a *db.Analysis) error {
	f.terminated = append(f.terminated, a.ID)
	return f.err
}

func (f *fakeNotifier) NotifyExpiringSoon(_ context.Context, a *db.Analysis) error {
	f.expiringSoon = append(f.expiringSoon, a.ID)
	return f.err
}

func (f *fakeNotifier) NotifyStillRunning(_ context.Context, a *db.Analysis) error {
	f.stillRunning = append(f.stillRunning, a.ID)
	return f.err
}

// fakeStatusPublisher records the analysis status updates the worker publishes
// for analyses it reconciles without terminating.
type fakeStatusPublisher struct {
	succeeded []string
}

func (f *fakeStatusPublisher) Fail(context.Context, string, string) error { return nil }

func (f *fakeStatusPublisher) Success(_ context.Context, jobID, _ string) error {
	f.succeeded = append(f.succeeded, jobID)
	return nil
}

func (f *fakeStatusPublisher) Running(context.Context, string, string) error { return nil }

func timePtr(t time.Time) *time.Time { return &t }

// testAnalysis returns a running analysis that expires in an hour.
func testAnalysis(id constants.AnalysisID) db.Analysis {
	return db.Analysis{
		ID:             id,
		ExternalID:     constants.ExternalID("ext-" + string(id)),
		Name:           "test analysis",
		Status:         "Running",
		UserID:         "user-1",
		Username:       "someone@example.org",
		ResultFolder:   "/iplant/home/someone/analyses/test",
		StartDate:      timePtr(time.Now().Add(-2 * time.Hour)),
		PlannedEndDate: timePtr(time.Now().Add(time.Hour)),
		NotifyPeriodic: true,
	}
}

func newTestWorker(database db.ExpirationDB, notifier *fakeNotifier) *Worker {
	return New(database, notifier, nil, nil, Init{})
}

func TestNewAppliesDefaults(t *testing.T) {
	tests := []struct {
		name              string
		init              Init
		wantSweep         time.Duration
		wantExpiryWarning time.Duration
	}{
		{
			name:              "zero values select the defaults",
			init:              Init{},
			wantSweep:         DefaultSweepInterval,
			wantExpiryWarning: DefaultExpiryWarning,
		},
		{
			name:              "negative values select the defaults",
			init:              Init{SweepInterval: -time.Second, ExpiryWarning: -time.Hour},
			wantSweep:         DefaultSweepInterval,
			wantExpiryWarning: DefaultExpiryWarning,
		},
		{
			name:              "explicit values are kept",
			init:              Init{SweepInterval: 30 * time.Second, ExpiryWarning: 2 * time.Hour},
			wantSweep:         30 * time.Second,
			wantExpiryWarning: 2 * time.Hour,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := New(newFakeDB(), &fakeNotifier{}, nil, nil, tt.init)
			assert.Equal(t, tt.wantSweep, w.sweepInterval)
			assert.Equal(t, tt.wantExpiryWarning, w.expiryWarning)
		})
	}
}

func TestWarnExpiring(t *testing.T) {
	const id = constants.AnalysisID("analysis-1")

	tests := []struct {
		name string
		// setup mutates the fake DB before the sweep runs.
		setup func(*fakeExpirationDB)
		// notifyErr is injected into the notifier.
		notifyErr error

		wantNotified     bool
		wantSentFlag     bool
		wantSentFlagSet  bool
		wantFailureCount int
	}{
		{
			name:            "sends the warning and records it",
			wantNotified:    true,
			wantSentFlag:    true,
			wantSentFlagSet: true,
		},
		{
			name: "does not resend a warning already sent",
			setup: func(f *fakeExpirationDB) {
				f.statuses[id] = &db.NotifStatuses{AnalysisID: id, HourWarningSent: true}
			},
			wantNotified:    false,
			wantSentFlagSet: false,
		},
		{
			name: "another replica holding the row is skipped",
			setup: func(f *fakeExpirationDB) {
				f.claimedBy[id] = true
			},
			wantNotified:    false,
			wantSentFlagSet: false,
		},
		{
			name:             "a delivery failure is counted and retried later",
			notifyErr:        errors.New("notification-agent is down"),
			wantNotified:     true,
			wantSentFlagSet:  false,
			wantFailureCount: 1,
		},
		{
			name:      "delivery is abandoned once the attempt ceiling is reached",
			notifyErr: errors.New("notification-agent is down"),
			setup: func(f *fakeExpirationDB) {
				f.statuses[id] = &db.NotifStatuses{
					AnalysisID:              id,
					HourWarningFailureCount: MaxNotificationAttempts - 1,
				}
			},
			wantNotified:     true,
			wantSentFlag:     true,
			wantSentFlagSet:  true,
			wantFailureCount: MaxNotificationAttempts,
		},
		{
			name: "an analysis with no external ID is skipped entirely",
			setup: func(f *fakeExpirationDB) {
				f.expiring[0].ExternalID = ""
			},
			wantNotified:    false,
			wantSentFlagSet: false,
		},
		{
			name: "a listing failure is not fatal",
			setup: func(f *fakeExpirationDB) {
				f.expiringErr = errors.New("database is down")
			},
			wantNotified:    false,
			wantSentFlagSet: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			database := newFakeDB()
			database.expiring = []db.Analysis{testAnalysis(id)}
			if tt.setup != nil {
				tt.setup(database)
			}

			notifier := &fakeNotifier{err: tt.notifyErr}
			w := newTestWorker(database, notifier)

			w.warnExpiring(context.Background(), 0, time.Hour, db.HourWarning)

			if tt.wantNotified {
				assert.Equal(t, []constants.AnalysisID{id}, notifier.expiringSoon)
			} else {
				assert.Empty(t, notifier.expiringSoon)
			}

			sent, ok := database.sentFlags[flagKey(db.HourWarning, id)]
			assert.Equal(t, tt.wantSentFlagSet, ok, "sent flag written")
			if tt.wantSentFlagSet {
				assert.Equal(t, tt.wantSentFlag, sent)
			}

			assert.Equal(t, tt.wantFailureCount, database.failureCounts[flagKey(db.HourWarning, id)])
		})
	}
}

// TestWarnExpiringUsesTheRequestedKind verifies that the day and hour warnings
// are tracked independently, so sending one doesn't suppress the other.
func TestWarnExpiringUsesTheRequestedKind(t *testing.T) {
	const id = constants.AnalysisID("analysis-1")

	database := newFakeDB()
	database.expiring = []db.Analysis{testAnalysis(id)}
	database.statuses[id] = &db.NotifStatuses{AnalysisID: id, HourWarningSent: true}

	notifier := &fakeNotifier{}
	w := newTestWorker(database, notifier)

	w.warnExpiring(context.Background(), DefaultExpiryWarning, DayExpiryWarning, db.DayWarning)

	assert.Equal(t, []constants.AnalysisID{id}, notifier.expiringSoon,
		"the day warning should still go out when only the hour warning was sent")
	assert.True(t, database.sentFlags[flagKey(db.DayWarning, id)])
	_, hourWritten := database.sentFlags[flagKey(db.HourWarning, id)]
	assert.False(t, hourWritten, "the hour warning's flag should be left alone")
}

// TestDeliverPacesRetries covers the pacing between delivery attempts. The
// attempt ceiling is the DE's only protection against retrying an undeliverable
// notification forever, and the sweep runs every ten seconds — so without a
// backoff between attempts, a notification-agent that is down for the length of
// a rolling restart abandons every notification in flight.
func TestDeliverPacesRetries(t *testing.T) {
	const id = constants.AnalysisID("analysis-1")

	database := newFakeDB()
	analysis := testAnalysis(id)
	database.statuses[id] = &db.NotifStatuses{AnalysisID: id}

	notifier := &fakeNotifier{err: errors.New("notification-agent is down")}
	w := newTestWorker(database, notifier)

	// Stands in for consecutive sweeps: the interval between them is far
	// shorter than the backoff the first failure schedules.
	for range 5 {
		w.deliver(context.Background(), &analysis, db.HourWarning)
	}

	assert.Len(t, notifier.expiringSoon, 1,
		"only the first attempt should have been made; the rest fall inside the backoff")
	assert.Equal(t, 1, database.failureCounts[flagKey(db.HourWarning, id)])
	assert.NotContains(t, database.sentFlags, flagKey(db.HourWarning, id),
		"a notification must not be abandoned while its retries are still pending")
}

// TestDeliverSkipsAnalysesWithNoStartDate covers a row the notification cannot
// be rendered from at all. Counting those as delivery failures spends the
// attempt ceiling on a condition no retry fixes, and logs a cause —
// notification-agent being unreachable — that is not the real one.
func TestDeliverSkipsAnalysesWithNoStartDate(t *testing.T) {
	const id = constants.AnalysisID("analysis-1")

	database := newFakeDB()
	analysis := testAnalysis(id)
	analysis.StartDate = nil
	database.statuses[id] = &db.NotifStatuses{AnalysisID: id}

	notifier := &fakeNotifier{}
	newTestWorker(database, notifier).deliver(context.Background(), &analysis, db.KillWarning)

	assert.Empty(t, notifier.terminated)
	assert.Empty(t, database.failureCounts)
	assert.Empty(t, database.sentFlags)
}

// TestNotificationPassesStopWhenTheBudgetIsSpent covers the bound on how long a
// sweep spends notifying. Delivery holds a row lock and blocks on an HTTP POST,
// so an unbounded notification pass delays the next sweep — and with it the next
// round of terminations — for as long as notification-agent stays slow.
func TestNotificationPassesStopWhenTheBudgetIsSpent(t *testing.T) {
	const id = constants.AnalysisID("analysis-1")

	spent, cancel := context.WithCancel(context.Background())
	cancel()

	t.Run("expiry warnings", func(t *testing.T) {
		database := newFakeDB()
		database.expiring = []db.Analysis{testAnalysis(id)}
		notifier := &fakeNotifier{}

		newTestWorker(database, notifier).warnExpiring(spent, 0, time.Hour, db.HourWarning)

		assert.Empty(t, notifier.expiringSoon)
		assert.Empty(t, database.ensured, "no tracking row should be written for a pass that was cut short")
	})

	t.Run("termination notices", func(t *testing.T) {
		database := newFakeDB()
		analysis := testAnalysis(id)
		notifier := &fakeNotifier{}

		newTestWorker(database, notifier).notifyTerminated(spent, []*db.Analysis{&analysis})

		assert.Empty(t, notifier.terminated)
	})

	t.Run("periodic reminders", func(t *testing.T) {
		database := newFakeDB()
		database.periodic = []db.Analysis{testAnalysis(id)}
		notifier := &fakeNotifier{}

		newTestWorker(database, notifier).remindStillRunning(spent)

		assert.Empty(t, notifier.stillRunning)
	})
}

// TestSweepTerminatesBeforeNotifying pins the order of the passes. Terminations
// are the one pass with a deadline the DE cannot make up later — an analysis
// past its time limit holds a node and the user's quota — so they must not queue
// behind notification delivery.
func TestSweepTerminatesBeforeNotifying(t *testing.T) {
	database := newFakeDB()
	w := newTestWorker(database, &fakeNotifier{})

	w.sweep(context.Background())

	assert.Equal(t,
		[]string{"expired", "expiring:hour", "expiring:day", "periodic"},
		database.calls,
	)
}

// TestSweepWarnsOverDisjointWindows covers the windows the two expiry warnings
// cover. A day window that contains the hour window sends both notifications —
// whose text is identical — in the same pass to any analysis whose whole time
// limit is shorter than a day.
func TestSweepWarnsOverDisjointWindows(t *testing.T) {
	database := newFakeDB()
	w := New(database, &fakeNotifier{}, nil, nil, Init{})

	w.sweep(context.Background())

	assert.Equal(t,
		[]expiringWindow{
			{kind: db.HourWarning, from: 0, to: DefaultExpiryWarning},
			{kind: db.DayWarning, from: DefaultExpiryWarning, to: DayExpiryWarning},
		},
		database.expiringWindows,
	)
}

func TestReminderDue(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name     string
		analysis db.Analysis
		statuses db.NotifStatuses
		want     bool
	}{
		{
			name:     "not due before the default period has elapsed since start",
			analysis: db.Analysis{StartDate: timePtr(now.Add(-time.Hour))},
			statuses: db.NotifStatuses{},
			want:     false,
		},
		{
			name:     "due once the default period has elapsed since start",
			analysis: db.Analysis{StartDate: timePtr(now.Add(-DefaultPeriodicReminderPeriod - time.Minute))},
			statuses: db.NotifStatuses{},
			want:     true,
		},
		{
			name:     "paced from the last reminder rather than the start",
			analysis: db.Analysis{StartDate: timePtr(now.Add(-24 * time.Hour))},
			statuses: db.NotifStatuses{LastPeriodicWarning: timePtr(now.Add(-time.Minute))},
			want:     false,
		},
		{
			name:     "due again once the period has elapsed since the last reminder",
			analysis: db.Analysis{StartDate: timePtr(now.Add(-24 * time.Hour))},
			statuses: db.NotifStatuses{LastPeriodicWarning: timePtr(now.Add(-DefaultPeriodicReminderPeriod - time.Minute))},
			want:     true,
		},
		{
			name:     "a per-analysis period overrides the default",
			analysis: db.Analysis{StartDate: timePtr(now.Add(-2 * time.Hour))},
			statuses: db.NotifStatuses{PeriodicWarningSeconds: int64(time.Hour.Seconds())},
			want:     true,
		},
		{
			name:     "a non-positive stored period falls back to the default",
			analysis: db.Analysis{StartDate: timePtr(now.Add(-time.Hour))},
			statuses: db.NotifStatuses{PeriodicWarningSeconds: 0},
			want:     false,
		},
		{
			name:     "an analysis with no start date is never due",
			analysis: db.Analysis{},
			statuses: db.NotifStatuses{},
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, reminderDue(&tt.analysis, &tt.statuses, now))
		})
	}
}

func TestRemindStillRunning(t *testing.T) {
	const id = constants.AnalysisID("analysis-1")

	dueAnalysis := func() db.Analysis {
		a := testAnalysis(id)
		a.StartDate = timePtr(time.Now().Add(-DefaultPeriodicReminderPeriod - time.Minute))
		return a
	}

	t.Run("sends the reminder and records when it went out", func(t *testing.T) {
		database := newFakeDB()
		database.periodic = []db.Analysis{dueAnalysis()}
		notifier := &fakeNotifier{}

		newTestWorker(database, notifier).remindStillRunning(context.Background())

		assert.Equal(t, []constants.AnalysisID{id}, notifier.stillRunning)
		assert.Contains(t, database.lastPeriodic, id)
	})

	t.Run("does not record a reminder that failed to send", func(t *testing.T) {
		database := newFakeDB()
		database.periodic = []db.Analysis{dueAnalysis()}
		notifier := &fakeNotifier{err: errors.New("notification-agent is down")}

		newTestWorker(database, notifier).remindStillRunning(context.Background())

		assert.Equal(t, []constants.AnalysisID{id}, notifier.stillRunning)
		assert.NotContains(t, database.lastPeriodic, id,
			"a failed reminder must not advance the timestamp, or the retry would be delayed a full period")
	})

	t.Run("does not send a reminder that is not due yet", func(t *testing.T) {
		database := newFakeDB()
		notDue := testAnalysis(id)
		notDue.StartDate = timePtr(time.Now().Add(-time.Minute))
		database.periodic = []db.Analysis{notDue}
		notifier := &fakeNotifier{}

		newTestWorker(database, notifier).remindStillRunning(context.Background())

		assert.Empty(t, notifier.stillRunning)
		assert.NotContains(t, database.lastPeriodic, id)
	})
}

func TestSweepSurvivesAPanic(t *testing.T) {
	database := newFakeDB()
	// A nil scheduler makes terminateExpired panic when it has an expired
	// analysis to look up, which stands in for any unexpected fault.
	database.expired = []db.Analysis{testAnalysis("analysis-1")}

	w := newTestWorker(database, &fakeNotifier{})

	require.NotPanics(t, func() { w.sweep(context.Background()) },
		"a panic in the sweep must not escape and take app-exposer's API down with it")
}

// TestHandleIndeterminate covers what happens to an expired analysis the worker
// cannot locate. Marking one Completed ends it in the DE without saving its
// outputs, so the bar for doing that on an inconclusive answer is high — but
// never doing it strands the analysis in Running and holds the user's quota.
func TestHandleIndeterminate(t *testing.T) {
	const id = constants.AnalysisID("analysis-1")

	pastEnd := func(d time.Duration) *time.Time { return timePtr(time.Now().Add(-d)) }

	tests := []struct {
		name          string
		err           error
		plannedEnd    *time.Time
		wantCompleted bool
	}{
		{
			name:       "an empty scheduler is never authoritative",
			err:        operatorclient.ErrNoOperators,
			plannedEnd: pastEnd(time.Minute),
		},
		{
			name:       "an empty scheduler stays inconclusive past the grace period",
			err:        operatorclient.ErrNoOperators,
			plannedEnd: pastEnd(terminationGracePeriod + time.Hour),
		},
		{
			name:       "an unreachable operator is retried inside the grace period",
			err:        errors.New("operator unreachable"),
			plannedEnd: pastEnd(time.Minute),
		},
		{
			name:          "an unreachable operator is given up on past the grace period",
			err:           errors.New("operator unreachable"),
			plannedEnd:    pastEnd(terminationGracePeriod + time.Hour),
			wantCompleted: true,
		},
		{
			name:       "an analysis with no planned end date is never given up on",
			err:        errors.New("operator unreachable"),
			plannedEnd: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := &fakeStatusPublisher{}
			w := New(newFakeDB(), &fakeNotifier{}, nil, status, Init{})

			analysis := testAnalysis(id)
			analysis.PlannedEndDate = tt.plannedEnd

			w.handleIndeterminate(context.Background(), &analysis, tt.err)

			if tt.wantCompleted {
				assert.Equal(t, []string{string(analysis.ExternalID)}, status.succeeded)
			} else {
				assert.Empty(t, status.succeeded)
			}
		})
	}
}

// TestMarkCompletedPublishesOnlyOnce covers the guard on the one action in the
// sweep that is not claim-guarded. An expired analysis that has left every
// cluster is reconciled by publishing a Completed status, after which the DE
// transitions it and it stops being returned as expired. When that transition
// does not happen — a stalled job-status pipeline, a jobs row pinned by someone
// else's long-running transaction — the analysis stays expired forever, and an
// unguarded publish becomes one status update per sweep per replica without end.
func TestMarkCompletedPublishesOnlyOnce(t *testing.T) {
	const id = constants.AnalysisID("analysis-1")

	tests := []struct {
		name            string
		alreadyReported bool
		lookupErr       error
		wantPublished   bool
	}{
		{
			name:          "publishes when nothing has been reported yet",
			wantPublished: true,
		},
		{
			name:            "does not re-publish once a Completed status is recorded",
			alreadyReported: true,
		},
		{
			name:      "skips the analysis when the check itself fails",
			lookupErr: errors.New("connection reset"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			database := newFakeDB()
			status := &fakeStatusPublisher{}
			w := New(database, &fakeNotifier{}, nil, status, Init{})

			analysis := testAnalysis(id)
			database.completedStatusFor[analysis.ExternalID] = tt.alreadyReported
			database.completedStatusErr = tt.lookupErr

			w.markCompleted(context.Background(), &analysis)

			if tt.wantPublished {
				assert.Equal(t, []string{string(analysis.ExternalID)}, status.succeeded)
			} else {
				assert.Empty(t, status.succeeded)
			}
		})
	}
}

// TestMarkCompletedStopsRepublishingAcrossSweeps is the regression the guard
// exists for: a durable record is what makes repeated sweeps converge, so this
// asserts the behavior over several passes rather than a single call.
func TestMarkCompletedStopsRepublishingAcrossSweeps(t *testing.T) {
	const id = constants.AnalysisID("analysis-1")

	database := newFakeDB()
	status := &fakeStatusPublisher{}
	w := New(database, &fakeNotifier{}, nil, status, Init{})

	analysis := testAnalysis(id)

	for range 5 {
		w.markCompleted(context.Background(), &analysis)
		// Stands in for the status update the publish records, which is what a
		// later sweep — on this replica or any other — reads back.
		if len(status.succeeded) > 0 {
			database.completedStatusFor[analysis.ExternalID] = true
		}
	}

	assert.Equal(t, []string{string(analysis.ExternalID)}, status.succeeded,
		"five sweeps of the same stuck analysis must publish exactly one status update")
}

// TestRepairRuntime covers the sweep-driven repair of the runtime fields. It is
// the safety net that is always present: the AMQP consumer that does the same
// job only exists when amqp.uri is configured.
func TestRepairRuntime(t *testing.T) {
	t.Run("analyses missing runtime fields are initialized", func(t *testing.T) {
		database := newFakeDB()
		database.missingRuntime = []db.Analysis{testAnalysis("analysis-1"), testAnalysis("analysis-2")}

		newTestWorker(database, &fakeNotifier{}).repairRuntime(context.Background())

		assert.Equal(t,
			[]constants.AnalysisID{"analysis-1", "analysis-2"},
			database.initRuntimeFor,
		)
	})

	t.Run("an analysis with no external ID is skipped", func(t *testing.T) {
		database := newFakeDB()
		analysis := testAnalysis("analysis-1")
		analysis.ExternalID = ""
		database.missingRuntime = []db.Analysis{analysis}

		newTestWorker(database, &fakeNotifier{}).repairRuntime(context.Background())

		assert.Empty(t, database.initRuntimeFor,
			"there is no external ID to derive a subdomain from yet")
	})

	t.Run("a listing failure does not stop the sweep", func(t *testing.T) {
		database := newFakeDB()
		database.missingRuntimeErr = assert.AnError

		require.NotPanics(t, func() {
			newTestWorker(database, &fakeNotifier{}).repairRuntime(context.Background())
		})
		assert.Empty(t, database.initRuntimeFor)
	})
}
