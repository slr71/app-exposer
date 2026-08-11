package reconciler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cyverse-de/app-exposer/constants"
	"github.com/cyverse-de/app-exposer/db"
	"github.com/cyverse-de/app-exposer/operatorclient"
	"github.com/cyverse-de/app-exposer/reporting"
	"github.com/cyverse-de/messaging/v12"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetLocalIP verifies that getLocalIP returns a parseable IPv4 address.
// On machines without non-loopback interfaces it falls back to "127.0.0.1",
// which is also a valid IPv4 address.
func TestGetLocalIP(t *testing.T) {
	ip := getLocalIP()

	require.NotEmpty(t, ip, "getLocalIP should never return an empty string")

	parsed := net.ParseIP(ip)
	require.NotNil(t, parsed, "getLocalIP returned %q which is not a parseable IP address", ip)

	require.NotNil(t, parsed.To4(), "getLocalIP returned %q which is not an IPv4 address", ip)
}

// TestNewReconciler verifies that New returns a non-nil Reconciler and that
// the ip and hostname fields are populated (they are set unconditionally in
// the constructor).
func TestNewReconciler(t *testing.T) {
	scheduler := operatorclient.NewScheduler(nil)

	r := New(&fakeReconcilerDB{}, scheduler, nil)

	require.NotNil(t, r)
	// ip is populated by getLocalIP, which always returns a non-empty string.
	assert.NotEmpty(t, r.ip, "Reconciler.ip should be populated by New")
	// hostname comes from os.Hostname, which may return "" in exotic
	// environments — we only verify the field is set (no panic).
	_ = r.hostname
}

// fakeReconcilerDB implements db.ReconcilerDB for unit tests. It records
// calls so assertions can be made against the side effects the reconciler
// produces, and per-method errors can be injected to exercise failure paths.
type fakeReconcilerDB struct {
	operators []db.Operator
	statuses  map[constants.ExternalID]messaging.JobState // externalID -> latest status

	// uri is what URI() returns. Default ("") exercises the
	// LISTEN-disabled fast path in startListener.
	uri string

	// Injected errors (nil by default).
	listErr   error
	statusErr error // returned from GetLatestStatusByExternalID
	insertErr error
	claimErr  error

	// Controls which operator ClaimAndReconcile passes to the callback. When
	// nil, ClaimAndReconcile returns without invoking the callback (mimics
	// "no operator due for reconciliation").
	claimOp *db.Operator

	// Recorded side effects.
	inserts           []db.JobStatusUpdate
	listOperatorsHits int
	claimHits         int
}

func (f *fakeReconcilerDB) URI() string { return f.uri }

func (f *fakeReconcilerDB) ListOperators(_ context.Context) ([]db.Operator, error) {
	f.listOperatorsHits++
	if f.listErr != nil {
		return nil, f.listErr
	}
	// Return a defensive copy to keep test assertions independent of
	// any mutations the production code might attempt on the slice.
	out := make([]db.Operator, len(f.operators))
	copy(out, f.operators)
	return out, nil
}

func (f *fakeReconcilerDB) ClaimAndReconcile(_ context.Context, _ string, _ time.Duration, fn func(tx *sqlx.Tx, op *db.Operator) error) error {
	f.claimHits++
	if f.claimErr != nil {
		return f.claimErr
	}
	if f.claimOp == nil {
		// No operator due for reconciliation — production returns
		// sql.ErrNoRows from the real implementation.
		return sql.ErrNoRows
	}
	// The reconciler treats tx as opaque, so passing nil is safe; the fake's
	// downstream methods ignore tx too.
	return fn(nil, f.claimOp)
}

func (f *fakeReconcilerDB) GetLatestStatusByExternalID(_ context.Context, _ *sqlx.Tx, externalID constants.ExternalID) (messaging.JobState, error) {
	if f.statusErr != nil {
		return "", f.statusErr
	}
	status, ok := f.statuses[externalID]
	if !ok {
		return "", sql.ErrNoRows
	}
	return status, nil
}

func (f *fakeReconcilerDB) InsertJobStatusUpdate(_ context.Context, _ *sqlx.Tx, update *db.JobStatusUpdate) error {
	if f.insertErr != nil {
		return f.insertErr
	}
	f.inserts = append(f.inserts, *update)
	return nil
}

// newTestReconciler wires a Reconciler against the supplied fake DB and an
// empty scheduler. Callers that need operator clients should extend the
// scheduler via its Sync method after construction.
func newTestReconciler(t *testing.T, fake *fakeReconcilerDB) *Reconciler {
	t.Helper()
	sched := operatorclient.NewScheduler(nil)
	return New(fake, sched, nil)
}

func TestSyncOperators(t *testing.T) {
	tests := []struct {
		name          string
		operators     []db.Operator
		listErr       error
		wantErr       bool
		wantClientCnt int
	}{
		{
			name:          "empty operators syncs zero clients",
			operators:     nil,
			wantClientCnt: 0,
		},
		{
			name: "populated operators produces matching scheduler clients",
			operators: []db.Operator{
				{Name: "op-a", URL: "http://a.example.invalid", Priority: 0},
				{Name: "op-b", URL: "http://b.example.invalid", Priority: 1},
			},
			wantClientCnt: 2,
		},
		{
			name:    "ListOperators error propagates",
			listErr: errors.New("db down"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeReconcilerDB{
				operators: tt.operators,
				listErr:   tt.listErr,
			}
			r := newTestReconciler(t, fake)

			err := r.SyncOperators(context.Background())
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Len(t, r.scheduler.Clients(), tt.wantClientCnt)
			assert.Equal(t, 1, fake.listOperatorsHits)
		})
	}
}

func TestReconcileAnalysis(t *testing.T) {
	tests := []struct {
		name        string
		pod         reporting.PodInfo
		priorStatus messaging.JobState // optional; injected via statuses map
		hasPrior    bool
		statusErr   error
		insertErr   error
		wantInserts int
		wantStatus  messaging.JobState // only checked when wantInserts == 1
		wantErrIs   error              // expected errors.Is match; nil means no error
	}{
		{
			name: "skips pod without analysis-id",
			pod: reporting.PodInfo{
				MetaInfo: reporting.MetaInfo{ExternalID: "ext-1"},
				Phase:    "Running",
			},
			wantInserts: 0,
		},
		{
			name: "skips pod without external-id",
			pod: reporting.PodInfo{
				MetaInfo: reporting.MetaInfo{AnalysisID: "an-1"},
				Phase:    "Running",
			},
			wantInserts: 0,
		},
		{
			// Bootstraps the job_status_updates row from the cluster state
			// when nothing else has published one — the only way an
			// analysis that never reached an available replica, and so
			// never triggered the operator's informer, leaves Submitted.
			name: "seeds initial status when no prior row exists",
			pod: reporting.PodInfo{
				MetaInfo: reporting.MetaInfo{AnalysisID: "an-1", ExternalID: "ext-1"},
				Phase:    "Running",
			},
			hasPrior:    false,
			wantInserts: 1,
			wantStatus:  messaging.RunningState,
		},
		{
			name: "seeds initial Failed status when no prior row exists",
			pod: reporting.PodInfo{
				MetaInfo: reporting.MetaInfo{AnalysisID: "an-1", ExternalID: "ext-1"},
				Phase:    "Failed",
				Message:  "pod OOMKilled",
			},
			hasPrior:    false,
			wantInserts: 1,
			wantStatus:  messaging.FailedState,
		},
		{
			name: "records update on status change",
			pod: reporting.PodInfo{
				MetaInfo: reporting.MetaInfo{AnalysisID: "an-1", ExternalID: "ext-1"},
				Phase:    "Running",
			},
			priorStatus: messaging.SubmittedState,
			hasPrior:    true,
			wantInserts: 1,
			wantStatus:  messaging.RunningState,
		},
		{
			name: "no-op when cluster matches DB",
			pod: reporting.PodInfo{
				MetaInfo: reporting.MetaInfo{AnalysisID: "an-1", ExternalID: "ext-1"},
				Phase:    "Running",
			},
			priorStatus: messaging.RunningState,
			hasPrior:    true,
			wantInserts: 0,
		},
		{
			name: "records Failed transition",
			pod: reporting.PodInfo{
				MetaInfo: reporting.MetaInfo{AnalysisID: "an-1", ExternalID: "ext-1"},
				Phase:    "Failed",
				Message:  "pod OOMKilled",
			},
			priorStatus: messaging.RunningState,
			hasPrior:    true,
			wantInserts: 1,
			wantStatus:  messaging.FailedState,
		},
		{
			name: "propagates non-ErrNoRows status lookup error",
			pod: reporting.PodInfo{
				MetaInfo: reporting.MetaInfo{AnalysisID: "an-1", ExternalID: "ext-1"},
				Phase:    "Running",
			},
			statusErr: errors.New("db timeout"),
			wantErrIs: errors.New("db timeout"), // only used for error check, not Is
		},
		{
			name: "propagates insert error",
			pod: reporting.PodInfo{
				MetaInfo: reporting.MetaInfo{AnalysisID: "an-1", ExternalID: "ext-1"},
				Phase:    "Running",
			},
			priorStatus: messaging.SubmittedState,
			hasPrior:    true,
			insertErr:   errors.New("insert failed"),
			wantErrIs:   errors.New("insert failed"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeReconcilerDB{
				statuses:  map[constants.ExternalID]messaging.JobState{},
				statusErr: tt.statusErr,
				insertErr: tt.insertErr,
			}
			if tt.hasPrior {
				fake.statuses[tt.pod.ExternalID] = tt.priorStatus
			}
			r := newTestReconciler(t, fake)

			err := r.reconcileAnalysis(context.Background(), nil, tt.pod)
			if tt.wantErrIs != nil {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrIs.Error())
				return
			}
			require.NoError(t, err)
			require.Len(t, fake.inserts, tt.wantInserts)
			if tt.wantInserts == 1 {
				ins := fake.inserts[0]
				assert.Equal(t, tt.pod.ExternalID, ins.ExternalID)
				assert.Equal(t, tt.wantStatus, ins.Status)
				// Message defaulting is covered by TestRecordStatusUpdate.
			}
		})
	}
}

func TestRecordStatusUpdate(t *testing.T) {
	tests := []struct {
		name        string
		externalID  constants.ExternalID
		status      messaging.JobState
		message     string
		wantMessage string
	}{
		{
			name:        "empty message gets default formatting",
			externalID:  "ext-1",
			status:      messaging.RunningState,
			message:     "",
			wantMessage: "Status changed to Running",
		},
		{
			name:        "explicit message is preserved verbatim",
			externalID:  "ext-2",
			status:      messaging.FailedState,
			message:     "container OOMKilled",
			wantMessage: "container OOMKilled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fake := &fakeReconcilerDB{}
			r := newTestReconciler(t, fake)
			// Pin ip/hostname so assertions don't depend on the host.
			r.ip = "10.0.0.1"
			r.hostname = "test-host"

			err := r.recordStatusUpdate(context.Background(), nil, tt.externalID, tt.status, tt.message)
			require.NoError(t, err)
			require.Len(t, fake.inserts, 1)

			ins := fake.inserts[0]
			assert.Equal(t, tt.externalID, ins.ExternalID)
			assert.Equal(t, tt.status, ins.Status)
			assert.Equal(t, tt.wantMessage, ins.Message)
			assert.Equal(t, "10.0.0.1", ins.SentFrom)
			assert.Equal(t, "test-host", ins.SentFromHostname)
			assert.Greater(t, ins.SentOn, int64(0), "SentOn should be a unix millis timestamp")
		})
	}
}

// newListingServer returns an httptest.Server that serves GET /analyses
// with the supplied ResourceInfo. Other routes return 404, which would
// surface as a test failure if the reconciler reached them unexpectedly.
func newListingServer(t *testing.T, info *reporting.ResourceInfo) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/analyses", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(info)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestReconcileNext(t *testing.T) {
	t.Run("happy path records transitions", func(t *testing.T) {
		info := &reporting.ResourceInfo{
			Pods: []reporting.PodInfo{
				{
					MetaInfo: reporting.MetaInfo{AnalysisID: "an-1", ExternalID: "ext-1"},
					Phase:    "Running",
				},
				{
					MetaInfo: reporting.MetaInfo{AnalysisID: "an-2", ExternalID: "ext-2"},
					Phase:    "Failed",
				},
				{
					// Missing labels — should be silently skipped.
					Phase: "Running",
				},
			},
		}
		srv := newListingServer(t, info)

		opID := uuid.New()
		fake := &fakeReconcilerDB{
			statuses: map[constants.ExternalID]messaging.JobState{
				"ext-1": messaging.SubmittedState, // transition Submitted -> Running
				"ext-2": messaging.RunningState,   // transition Running -> Failed
			},
			claimOp: &db.Operator{ID: opID, Name: "op-a", URL: srv.URL},
		}
		r := newTestReconciler(t, fake)
		// Wire the scheduler to know about the claimed operator. The id
		// must match claimOp.ID because ReconcileNext looks up the
		// scheduler client by id, not name.
		require.NoError(t, r.scheduler.Sync([]operatorclient.OperatorAdminSummary{
			{ID: opID, OperatorConfig: operatorclient.OperatorConfig{Name: "op-a", URL: srv.URL}, AcceptingLaunches: true},
		}))

		err := r.ReconcileNext(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 1, fake.claimHits)
		require.Len(t, fake.inserts, 2, "only labeled pods with status changes should insert")

		byExternal := map[constants.ExternalID]messaging.JobState{}
		for _, ins := range fake.inserts {
			byExternal[ins.ExternalID] = ins.Status
		}
		assert.Equal(t, messaging.RunningState, byExternal["ext-1"])
		assert.Equal(t, messaging.FailedState, byExternal["ext-2"])
	})

	t.Run("returns descriptive error when operator is not in scheduler", func(t *testing.T) {
		fake := &fakeReconcilerDB{
			claimOp: &db.Operator{ID: uuid.New(), Name: "op-missing", URL: "http://unused.invalid"},
		}
		r := newTestReconciler(t, fake)
		// Scheduler intentionally has no clients.

		err := r.ReconcileNext(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "op-missing")
		assert.Contains(t, err.Error(), "not found in scheduler")
	})

	t.Run("wraps operator listing failure with operator name", func(t *testing.T) {
		// Serve 500 from the operator listing endpoint.
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "internal", http.StatusInternalServerError)
		}))
		t.Cleanup(srv.Close)

		opID := uuid.New()
		fake := &fakeReconcilerDB{
			claimOp: &db.Operator{ID: opID, Name: "op-a", URL: srv.URL},
		}
		r := newTestReconciler(t, fake)
		require.NoError(t, r.scheduler.Sync([]operatorclient.OperatorAdminSummary{
			{ID: opID, OperatorConfig: operatorclient.OperatorConfig{Name: "op-a", URL: srv.URL}, AcceptingLaunches: true},
		}))

		err := r.ReconcileNext(context.Background())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "op-a", "error should identify the failing operator")
	})

	t.Run("propagates sql.ErrNoRows when no operator is due", func(t *testing.T) {
		fake := &fakeReconcilerDB{} // claimOp nil → returns sql.ErrNoRows
		r := newTestReconciler(t, fake)

		err := r.ReconcileNext(context.Background())
		require.Error(t, err)
		assert.True(t, errors.Is(err, sql.ErrNoRows), "expected sql.ErrNoRows, got %v", err)
	})
}

func TestStartListenerNoDBURI(t *testing.T) {
	// With dbURI empty, startListener must short-circuit: return a
	// usable (buffered) channel without spawning a goroutine or touching
	// pq.NewListener. A regression here would re-introduce a goroutine
	// leak on every reconciler construction in environments where the
	// DB URI isn't threaded through.
	r := newTestReconciler(t, &fakeReconcilerDB{})
	// r.dbURI is "" because newTestReconciler passes "" to New().

	ch := r.startListener(context.Background())
	require.NotNil(t, ch, "startListener must always return a non-nil channel")

	// Confirm the channel is inert — nothing should ever arrive.
	select {
	case <-ch:
		t.Fatal("unexpected notification on inert channel")
	case <-time.After(10 * time.Millisecond):
	}
}

func TestStartListenerContextAlreadyCanceled(t *testing.T) {
	// A pre-canceled context should cause the goroutine to exit on its
	// first iteration without attempting Listen(). We can't observe that
	// directly without plumbing an injectable listener factory, so we
	// settle for verifying the channel is returned and no panic occurs.
	// A real-world test would be an integration test against Postgres.
	r := &Reconciler{db: &fakeReconcilerDB{uri: "postgres://nobody@127.0.0.1:1/nope"}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ch := r.startListener(ctx)
	require.NotNil(t, ch)
	// Give the goroutine a brief window to observe the cancellation and
	// exit. If it's stuck in Listen(), the -race test would still pass;
	// this is a smoke test, not a leak check.
	time.Sleep(50 * time.Millisecond)
}

func TestNextBackoff(t *testing.T) {
	tests := []struct {
		name string
		cur  time.Duration
		max  time.Duration
		want time.Duration
	}{
		{"doubles below ceiling", 5 * time.Second, time.Minute, 10 * time.Second},
		{"doubles up to ceiling", 30 * time.Second, time.Minute, time.Minute},
		{"caps at ceiling", 40 * time.Second, time.Minute, time.Minute},
		{"stays at ceiling when already there", time.Minute, time.Minute, time.Minute},
		{"zero doubles to zero", 0, time.Minute, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, nextBackoff(tt.cur, tt.max))
		})
	}
}
