package operatorclient

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// schedulerWithConfigs builds a Scheduler whose operators come from the
// given OperatorConfig list, synthesizing a deterministic UUID per slot so
// older tests authored against the config-only signature continue to read
// naturally. Tests that need to assert a specific id should construct
// OperatorAdminSummary values directly and call Sync.
func schedulerWithConfigs(t *testing.T, configs []OperatorConfig) *Scheduler {
	t.Helper()
	summaries := make([]OperatorAdminSummary, len(configs))
	for i, cfg := range configs {
		summaries[i] = OperatorAdminSummary{
			ID:                uuid.New(),
			OperatorConfig:    cfg,
			AcceptingLaunches: true,
		}
	}
	s := NewScheduler(nil)
	require.NoError(t, s.Sync(summaries))
	return s
}

// mockOperatorServer creates a test HTTP server that simulates an operator.
// capacitySlots controls how many available slots are reported.
// rejectLaunch causes the launch endpoint to return 409.
func mockOperatorServer(capacitySlots int, rejectLaunch bool) *httptest.Server {
	return mockOperatorServerWithStatuses(capacitySlots, rejectLaunch, 0, 0, "")
}

// mockOperatorServerWithVendor is a mock that reports a specific GPU
// vendor and is otherwise healthy with the given capacity.
func mockOperatorServerWithVendor(capacitySlots int, vendor string) *httptest.Server {
	return mockOperatorServerWithStatuses(capacitySlots, false, 0, 0, vendor)
}

// mockOperatorServerWithModels is a mock that advertises a specific set
// of supported GPU models (and an Nvidia vendor) and is otherwise healthy
// with the given capacity.
func mockOperatorServerWithModels(capacitySlots int, models []string) *httptest.Server {
	return mockOperatorServerFull(capacitySlots, false, 0, 0, "nvidia", models)
}

// mockOperatorServerWithStatuses is the explicit variant that also lets a
// test inject non-2xx responses for the capacity and launch endpoints —
// used by cases that exercise error-classification paths in the scheduler.
// A capacityStatus or launchStatus of 0 means "use the default behavior"
// (respect the slots / rejectLaunch arguments). vendor populates the
// CapacityResponse.GPUVendor field; "" means "operator does not report
// a vendor" (treated as compatible by the scheduler).
func mockOperatorServerWithStatuses(capacitySlots int, rejectLaunch bool, capacityStatus, launchStatus int, vendor string) *httptest.Server {
	return mockOperatorServerFull(capacitySlots, rejectLaunch, capacityStatus, launchStatus, vendor, nil)
}

// mockOperatorServerFull is the most explicit form: same as
// mockOperatorServerWithStatuses plus a SupportedGPUModels list to
// advertise. The narrower helpers above delegate here so the per-test
// signatures stay readable.
func mockOperatorServerFull(capacitySlots int, rejectLaunch bool, capacityStatus, launchStatus int, vendor string, models []string) *httptest.Server {
	var launchCount atomic.Int32

	mux := http.NewServeMux()
	mux.HandleFunc("/capacity", func(w http.ResponseWriter, r *http.Request) {
		if capacityStatus != 0 {
			http.Error(w, "injected capacity failure", capacityStatus)
			return
		}
		resp := CapacityResponse{
			MaxAnalyses:        10,
			RunningAnalyses:    10 - capacitySlots,
			AvailableSlots:     capacitySlots,
			GPUVendor:          vendor,
			SupportedGPUModels: models,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp) //nolint:errcheck
	})
	mux.HandleFunc("/analyses", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		launchCount.Add(1)
		if launchStatus != 0 {
			http.Error(w, "injected launch failure", launchStatus)
			return
		}
		if rejectLaunch {
			http.Error(w, "at capacity", http.StatusConflict)
			return
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
	})

	return httptest.NewServer(mux)
}

func TestSchedulerLaunchAnalysis(t *testing.T) {
	tests := []struct {
		name      string
		operators []struct {
			slots  int
			reject bool
		}
		wantOperator string // expected operator name, empty if error
		wantErr      error
	}{
		{
			name: "first operator has capacity",
			operators: []struct {
				slots  int
				reject bool
			}{
				{slots: 5, reject: false},
				{slots: 5, reject: false},
			},
			wantOperator: "op-0",
		},
		{
			name: "first at capacity, second accepts",
			operators: []struct {
				slots  int
				reject bool
			}{
				{slots: 0, reject: false},
				{slots: 5, reject: false},
			},
			wantOperator: "op-1",
		},
		{
			name: "first returns 409, second accepts",
			operators: []struct {
				slots  int
				reject bool
			}{
				{slots: 5, reject: true}, // Has capacity but race condition
				{slots: 5, reject: false},
			},
			wantOperator: "op-1",
		},
		{
			name: "all operators exhausted",
			operators: []struct {
				slots  int
				reject bool
			}{
				{slots: 0, reject: false},
				{slots: 0, reject: false},
			},
			wantErr: ErrAllOperatorsExhausted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var configs []OperatorConfig
			var servers []*httptest.Server

			for i, op := range tt.operators {
				srv := mockOperatorServer(op.slots, op.reject)
				servers = append(servers, srv)
				configs = append(configs, OperatorConfig{
					Name: fmt.Sprintf("op-%d", i),
					URL:  srv.URL,
				})
			}
			defer func() {
				for _, srv := range servers {
					srv.Close()
				}
			}()

			scheduler := schedulerWithConfigs(t, configs)

			bundle := &AnalysisBundle{AnalysisID: "test-123"}
			_, operatorName, err := scheduler.LaunchAnalysis(context.Background(), bundle)

			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantOperator, operatorName)
			}
		})
	}
}

// TestSchedulerLaunchSkipsDrainingOperators verifies that operators with
// AcceptingLaunches=false are skipped for new launches: a following operator
// that is accepting receives the analysis, and when every operator is draining
// the scheduler reports ErrAllOperatorsDraining rather than a misleading
// capacity error.
func TestSchedulerLaunchSkipsDrainingOperators(t *testing.T) {
	type op struct {
		accepting bool
		slots     int
	}
	tests := []struct {
		name         string
		operators    []op
		wantOperator string
		wantErr      error
	}{
		{
			name:         "draining first operator is skipped for accepting second",
			operators:    []op{{accepting: false, slots: 5}, {accepting: true, slots: 5}},
			wantOperator: "op-1",
		},
		{
			name:      "all draining returns ErrAllOperatorsDraining",
			operators: []op{{accepting: false, slots: 5}, {accepting: false, slots: 5}},
			wantErr:   ErrAllOperatorsDraining,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			summaries := make([]OperatorAdminSummary, 0, len(tt.operators))
			var servers []*httptest.Server
			for i, o := range tt.operators {
				srv := mockOperatorServer(o.slots, false)
				servers = append(servers, srv)
				summaries = append(summaries, OperatorAdminSummary{
					ID:                uuid.New(),
					OperatorConfig:    OperatorConfig{Name: fmt.Sprintf("op-%d", i), URL: srv.URL},
					AcceptingLaunches: o.accepting,
				})
			}
			defer func() {
				for _, srv := range servers {
					srv.Close()
				}
			}()

			scheduler := NewScheduler(nil)
			require.NoError(t, scheduler.Sync(summaries))

			_, operatorName, err := scheduler.LaunchAnalysis(context.Background(), &AnalysisBundle{AnalysisID: "drain-test"})

			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantOperator, operatorName)
			}
		})
	}
}

// TestSchedulerLaunchAllCapacityChecksFail exercises the "every operator
// errored on capacity check" branch — as distinct from "every operator is
// at capacity," which the table above already covers. LaunchAnalysis must
// surface the capacity-failure case rather than falling back to the plain
// ErrAllOperatorsExhausted shape.
func TestSchedulerLaunchAllCapacityChecksFail(t *testing.T) {
	srv0 := mockOperatorServerWithStatuses(0, false, http.StatusInternalServerError, 0, "")
	defer srv0.Close()
	srv1 := mockOperatorServerWithStatuses(0, false, http.StatusInternalServerError, 0, "")
	defer srv1.Close()

	scheduler := schedulerWithConfigs(t, []OperatorConfig{
		{Name: "op-0", URL: srv0.URL},
		{Name: "op-1", URL: srv1.URL},
	})

	_, _, err := scheduler.LaunchAnalysis(context.Background(), &AnalysisBundle{AnalysisID: "test"})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrAllOperatorsExhausted)
	assert.Contains(t, err.Error(), "failed capacity check", "message should distinguish capacity-failure from at-capacity")
}

// TestSchedulerLaunchDrainingPlusCapacityFail covers the mixed pool where one
// operator is draining and every remaining operator fails its capacity check.
// The draining operator is folded into the accounted-for total so the precise
// "failed capacity check" diagnostic still fires rather than falling through to
// the bare ErrAllOperatorsExhausted.
func TestSchedulerLaunchDrainingPlusCapacityFail(t *testing.T) {
	draining := mockOperatorServer(5, false) // healthy, but not accepting launches
	defer draining.Close()
	broken := mockOperatorServerWithStatuses(0, false, http.StatusInternalServerError, 0, "")
	defer broken.Close()

	scheduler := NewScheduler(nil)
	require.NoError(t, scheduler.Sync([]OperatorAdminSummary{
		{ID: uuid.New(), OperatorConfig: OperatorConfig{Name: "op-drain", URL: draining.URL}, AcceptingLaunches: false},
		{ID: uuid.New(), OperatorConfig: OperatorConfig{Name: "op-broken", URL: broken.URL}, AcceptingLaunches: true},
	}))

	_, _, err := scheduler.LaunchAnalysis(context.Background(), &AnalysisBundle{AnalysisID: "test"})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrAllOperatorsExhausted)
	assert.Contains(t, err.Error(), "failed capacity check", "draining operators must not suppress the capacity-failure diagnostic")
}

// TestSchedulerLaunchTransientErrorFalthrough covers the case added along
// with isTransientLaunchError: the first operator accepts capacity but then
// returns a 5xx on Launch. The scheduler should fall through to the next
// operator rather than aborting the whole launch.
func TestSchedulerLaunchTransientErrorFalthrough(t *testing.T) {
	srv0 := mockOperatorServerWithStatuses(5, false, 0, http.StatusBadGateway, "")
	defer srv0.Close()
	srv1 := mockOperatorServer(5, false)
	defer srv1.Close()

	scheduler := schedulerWithConfigs(t, []OperatorConfig{
		{Name: "op-0", URL: srv0.URL},
		{Name: "op-1", URL: srv1.URL},
	})

	_, name, err := scheduler.LaunchAnalysis(context.Background(), &AnalysisBundle{AnalysisID: "test"})
	require.NoError(t, err)
	assert.Equal(t, "op-1", name, "scheduler must fall through on transient 5xx launch errors")
}

// TestSchedulerLaunchNonTransientAborts guards the other side of the
// classification: a 400 from Launch is a request we built wrong, so it must
// abort the scheduling loop rather than walk every operator producing the
// same failure.
func TestSchedulerLaunchNonTransientAborts(t *testing.T) {
	srv0 := mockOperatorServerWithStatuses(5, false, 0, http.StatusBadRequest, "")
	defer srv0.Close()
	srv1 := mockOperatorServer(5, false) // would succeed if we reached it
	defer srv1.Close()

	scheduler := schedulerWithConfigs(t, []OperatorConfig{
		{Name: "op-0", URL: srv0.URL},
		{Name: "op-1", URL: srv1.URL},
	})

	_, _, err := scheduler.LaunchAnalysis(context.Background(), &AnalysisBundle{AnalysisID: "test"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "op-0", "error must name the operator that failed")
	assert.Contains(t, err.Error(), "400", "error must surface the HTTP status")
}

// TestSchedulerLaunchAllTransient covers the case where every operator is
// healthy-looking on capacity but fails on Launch with a transient 5xx. The
// scheduler should surface the last underlying error so the caller can
// tell the difference from the plain "all at capacity" shape.
func TestSchedulerLaunchAllTransient(t *testing.T) {
	srv0 := mockOperatorServerWithStatuses(5, false, 0, http.StatusServiceUnavailable, "")
	defer srv0.Close()
	srv1 := mockOperatorServerWithStatuses(5, false, 0, http.StatusBadGateway, "")
	defer srv1.Close()

	scheduler := schedulerWithConfigs(t, []OperatorConfig{
		{Name: "op-0", URL: srv0.URL},
		{Name: "op-1", URL: srv1.URL},
	})

	_, _, err := scheduler.LaunchAnalysis(context.Background(), &AnalysisBundle{AnalysisID: "test"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "could accept", "message should distinguish from at-capacity")

	var statusErr *HTTPStatusError
	require.ErrorAs(t, err, &statusErr, "last transient error must be preserved in the chain")
	assert.True(t, statusErr.Transient(), "preserved error should be a transient status error")
}

// TestSchedulerLaunchVendorMismatchAllAMD covers the case where the
// analysis requests an Nvidia GPU and every available operator reports
// AMD. The scheduler must surface ErrNoCompatibleOperator rather than
// silently routing the analysis to a cluster that would mis-transform
// the bundle's GPU resources.
func TestSchedulerLaunchVendorMismatchAllAMD(t *testing.T) {
	srv0 := mockOperatorServerWithVendor(5, "amd")
	defer srv0.Close()
	srv1 := mockOperatorServerWithVendor(5, "amd")
	defer srv1.Close()

	scheduler := schedulerWithConfigs(t, []OperatorConfig{
		{Name: "amd-0", URL: srv0.URL},
		{Name: "amd-1", URL: srv1.URL},
	})

	bundle := gpuBundle("nvidia.com/gpu", "main", "requests")
	_, _, err := scheduler.LaunchAnalysis(context.Background(), bundle)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoCompatibleOperator)
	assert.Contains(t, err.Error(), "nvidia", "error must name the requested vendor")
}

// TestSchedulerLaunchVendorMismatchSkipsToCompatible exercises the
// "first operator's vendor doesn't match, second one does" path. The
// scheduler must skip the AMD operator and successfully land on the
// Nvidia one.
func TestSchedulerLaunchVendorMismatchSkipsToCompatible(t *testing.T) {
	srv0 := mockOperatorServerWithVendor(5, "amd")
	defer srv0.Close()
	srv1 := mockOperatorServerWithVendor(5, "nvidia")
	defer srv1.Close()

	scheduler := schedulerWithConfigs(t, []OperatorConfig{
		{Name: "amd-op", URL: srv0.URL},
		{Name: "nvidia-op", URL: srv1.URL},
	})

	bundle := gpuBundle("nvidia.com/gpu", "main", "requests")
	_, name, err := scheduler.LaunchAnalysis(context.Background(), bundle)
	require.NoError(t, err)
	assert.Equal(t, "nvidia-op", name, "must skip AMD operator and land on Nvidia one")
}

// TestSchedulerLaunchNoGPURequestSkipsVendorCheck covers the common
// "this analysis has no GPU requirement" case. The scheduler must NOT
// reject AMD-only operators just because the deployment has no GPU
// requests at all — vendor filtering applies only when the bundle
// asks for a specific vendor.
func TestSchedulerLaunchNoGPURequestSkipsVendorCheck(t *testing.T) {
	srv0 := mockOperatorServerWithVendor(5, "amd")
	defer srv0.Close()
	srv1 := mockOperatorServerWithVendor(5, "amd")
	defer srv1.Close()

	scheduler := schedulerWithConfigs(t, []OperatorConfig{
		{Name: "amd-0", URL: srv0.URL},
		{Name: "amd-1", URL: srv1.URL},
	})

	bundle := gpuBundle("", "main", "requests")
	_, name, err := scheduler.LaunchAnalysis(context.Background(), bundle)
	require.NoError(t, err)
	assert.Equal(t, "amd-0", name, "no-GPU bundle must accept first capacity-passing operator")
}

// TestSchedulerLaunchOperatorWithoutVendorIsCompatible covers the
// backwards-compatibility case: an older operator (or one with no GPU
// configured) reports GPUVendor="". The scheduler must treat that as
// compatible regardless of what the bundle requests, since the
// pre-vendor operator's response is indistinguishable from a vendor-
// agnostic cluster.
func TestSchedulerLaunchOperatorWithoutVendorIsCompatible(t *testing.T) {
	srv0 := mockOperatorServerWithVendor(5, "")
	defer srv0.Close()

	scheduler := schedulerWithConfigs(t, []OperatorConfig{
		{Name: "no-vendor", URL: srv0.URL},
	})

	bundle := gpuBundle("nvidia.com/gpu", "main", "requests")
	_, name, err := scheduler.LaunchAnalysis(context.Background(), bundle)
	require.NoError(t, err)
	assert.Equal(t, "no-vendor", name)
}

// gpuModelLaunchBundle builds a bundle whose deployment carries the given
// GPU model node-affinity values and (when nvidiaGPU is true) an
// nvidia.com/gpu resource request. Used by the model-routing scheduler
// tests so the bundle exercises both RequestedGPUVendor (still nvidia)
// and RequestedGPUModels (the supplied list).
func gpuModelLaunchBundle(models []string) *AnalysisBundle {
	b := gpuModelBundle([][]string{models})
	// Add an nvidia.com/gpu request so the vendor check sees the bundle as
	// an Nvidia GPU job; otherwise RequestedGPUVendor returns "" and
	// vendorCompatible treats every operator as a match, so the vendor
	// check is never exercised.
	b.Deployment.Spec.Template.Spec.Containers = []apiv1.Container{{
		Name: "main",
		Resources: apiv1.ResourceRequirements{
			Requests: apiv1.ResourceList{apiv1.ResourceName(gpuResourceNvidia): resource.MustParse("1")},
		},
	}}
	return b
}

// TestSchedulerLaunchModelMismatchAllDisjoint covers the case where the
// analysis requests a GPU model that no operator advertises. The scheduler
// must surface ErrNoCompatibleModel — distinct from
// ErrNoCompatibleOperator so callers can tell "right vendor, wrong model"
// apart from "wrong vendor".
func TestSchedulerLaunchModelMismatchAllDisjoint(t *testing.T) {
	srv0 := mockOperatorServerWithModels(5, []string{"NVIDIA-A10G"})
	defer srv0.Close()
	srv1 := mockOperatorServerWithModels(5, []string{"NVIDIA-L4"})
	defer srv1.Close()

	scheduler := schedulerWithConfigs(t, []OperatorConfig{
		{Name: "op-a10g", URL: srv0.URL},
		{Name: "op-l4", URL: srv1.URL},
	})

	bundle := gpuModelLaunchBundle([]string{"NVIDIA-H100"})
	_, _, err := scheduler.LaunchAnalysis(context.Background(), bundle)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoCompatibleModel)
	assert.Contains(t, err.Error(), "NVIDIA-H100", "error must name the requested model")
}

// TestSchedulerLaunchMixedVendorAndModelMismatch covers a fleet where one
// operator is the wrong vendor and another is the right vendor but the wrong
// model, while the analysis requests a model no operator can satisfy. The
// model guard counts the vendor-mismatched operator too, so the mixed case
// still resolves to the most specific routing error, ErrNoCompatibleModel.
func TestSchedulerLaunchMixedVendorAndModelMismatch(t *testing.T) {
	srvAMD := mockOperatorServerWithVendor(5, "amd")
	defer srvAMD.Close()
	srvL4 := mockOperatorServerWithModels(5, []string{"NVIDIA-L4"})
	defer srvL4.Close()

	scheduler := schedulerWithConfigs(t, []OperatorConfig{
		{Name: "op-amd", URL: srvAMD.URL},
		{Name: "op-l4", URL: srvL4.URL},
	})

	bundle := gpuModelLaunchBundle([]string{"NVIDIA-H100"})
	_, _, err := scheduler.LaunchAnalysis(context.Background(), bundle)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoCompatibleModel)
	assert.Contains(t, err.Error(), "NVIDIA-H100", "error must name the requested model")
}

// TestSchedulerLaunchModelMismatchSkipsToCompatible exercises the
// "first operator's model doesn't match, second one does" path. The
// scheduler must skip the A10G-only operator and land on the L4 one.
func TestSchedulerLaunchModelMismatchSkipsToCompatible(t *testing.T) {
	srv0 := mockOperatorServerWithModels(5, []string{"NVIDIA-A10G"})
	defer srv0.Close()
	srv1 := mockOperatorServerWithModels(5, []string{"NVIDIA-L4", "NVIDIA-L40S"})
	defer srv1.Close()

	scheduler := schedulerWithConfigs(t, []OperatorConfig{
		{Name: "op-a10g", URL: srv0.URL},
		{Name: "op-l4", URL: srv1.URL},
	})

	bundle := gpuModelLaunchBundle([]string{"NVIDIA-L4"})
	_, name, err := scheduler.LaunchAnalysis(context.Background(), bundle)
	require.NoError(t, err)
	assert.Equal(t, "op-l4", name, "must skip A10G-only operator and land on the one that advertises L4")
}

// TestSchedulerLaunchOperatorWithoutModelsIsCompatible covers the
// backwards-compatibility case: an operator that has not been upgraded
// reports an empty SupportedGPUModels list. The scheduler must treat
// that as compatible regardless of what model the bundle requests,
// since "I don't advertise" is indistinguishable from
// "I support everything" for routing-policy purposes.
func TestSchedulerLaunchOperatorWithoutModelsIsCompatible(t *testing.T) {
	srv0 := mockOperatorServerWithVendor(5, "nvidia")
	defer srv0.Close()

	scheduler := schedulerWithConfigs(t, []OperatorConfig{
		{Name: "no-models", URL: srv0.URL},
	})

	bundle := gpuModelLaunchBundle([]string{"NVIDIA-A10G"})
	_, name, err := scheduler.LaunchAnalysis(context.Background(), bundle)
	require.NoError(t, err)
	assert.Equal(t, "no-models", name)
}

// TestSchedulerLaunchNoModelRequestSkipsModelCheck covers the common
// "this analysis has no model preference" case. The scheduler must NOT
// reject operators just because the deployment has no model affinity at
// all — model filtering applies only when the bundle requests one.
func TestSchedulerLaunchNoModelRequestSkipsModelCheck(t *testing.T) {
	srv0 := mockOperatorServerWithModels(5, []string{"NVIDIA-A10G"})
	defer srv0.Close()

	scheduler := schedulerWithConfigs(t, []OperatorConfig{
		{Name: "op-a10g", URL: srv0.URL},
	})

	// Bundle has an Nvidia GPU request but no model affinity.
	bundle := gpuBundle("nvidia.com/gpu", "main", "requests")
	_, name, err := scheduler.LaunchAnalysis(context.Background(), bundle)
	require.NoError(t, err)
	assert.Equal(t, "op-a10g", name)
}

func TestSchedulerNoOperators(t *testing.T) {
	scheduler := NewScheduler(nil)

	_, _, err := scheduler.LaunchAnalysis(context.Background(), &AnalysisBundle{AnalysisID: "test"})
	assert.ErrorIs(t, err, ErrNoOperators)

	scheduler = NewScheduler(nil)
	require.NoError(t, scheduler.Sync(nil))

	_, _, err = scheduler.LaunchAnalysis(context.Background(), &AnalysisBundle{AnalysisID: "test"})
	assert.ErrorIs(t, err, ErrNoOperators)
}

func TestSchedulerClientByName(t *testing.T) {
	srv := mockOperatorServer(5, false)
	defer srv.Close()

	scheduler := schedulerWithConfigs(t, []OperatorConfig{
		{Name: "test-op", URL: srv.URL},
	})

	client := scheduler.ClientByName("test-op")
	assert.NotNil(t, client)
	assert.Equal(t, "test-op", client.Name())

	client = scheduler.ClientByName("nonexistent")
	assert.Nil(t, client)
}

// TestSchedulerClientByID covers the id-keyed variant. It also verifies
// that ClientByID continues to return the right client across a Sync
// that renames the operator — the rename-safety guarantee the id-based
// API was introduced to provide.
func TestSchedulerClientByID(t *testing.T) {
	srv := mockOperatorServer(5, false)
	defer srv.Close()

	id := uuid.New()
	scheduler := NewScheduler(nil)
	require.NoError(t, scheduler.Sync([]OperatorAdminSummary{
		{ID: id, OperatorConfig: OperatorConfig{Name: "test-op", URL: srv.URL}, AcceptingLaunches: true},
	}))

	client := scheduler.ClientByID(id)
	require.NotNil(t, client)
	assert.Equal(t, "test-op", client.Name())
	assert.Equal(t, id, client.ID())

	assert.Nil(t, scheduler.ClientByID(uuid.New()), "unknown id must return nil")

	// Rename the operator (same id) and verify the lookup still works.
	require.NoError(t, scheduler.Sync([]OperatorAdminSummary{
		{ID: id, OperatorConfig: OperatorConfig{Name: "renamed", URL: srv.URL}, AcceptingLaunches: true},
	}))
	client = scheduler.ClientByID(id)
	require.NotNil(t, client, "id-keyed lookup must survive a rename")
	assert.Equal(t, "renamed", client.Name())
}

// TestSchedulerLaunchUnlimitedCapacity verifies that an operator reporting
// AvailableSlots=-1 (unlimited capacity) accepts launches normally.
func TestSchedulerLaunchUnlimitedCapacity(t *testing.T) {
	// Build a mock server that always reports unlimited capacity
	// (AvailableSlots=-1, MaxAnalyses=0, RunningAnalyses=5).
	mux := http.NewServeMux()
	mux.HandleFunc("/capacity", func(w http.ResponseWriter, r *http.Request) {
		resp := CapacityResponse{
			MaxAnalyses:     0,
			RunningAnalyses: 5,
			AvailableSlots:  -1,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp) //nolint:errcheck
	})
	mux.HandleFunc("/analyses", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"}) //nolint:errcheck
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	scheduler := schedulerWithConfigs(t, []OperatorConfig{
		{Name: "unlimited-op", URL: srv.URL},
	})

	bundle := &AnalysisBundle{AnalysisID: "unlimited-test-456"}
	_, operatorName, err := scheduler.LaunchAnalysis(context.Background(), bundle)

	require.NoError(t, err)
	assert.Equal(t, "unlimited-op", operatorName)
}

// TestSchedulerSyncAndSetTokenSource verifies that Sync replaces the operator
// list and SetTokenSource updates the token source. It also exercises the
// RW-mutex by running Sync and SetTokenSource concurrently to detect races
// under `go test -race`.
func TestSchedulerSyncAndSetTokenSource(t *testing.T) {
	// --- initial state: one operator ---
	srv0 := mockOperatorServer(5, false)
	defer srv0.Close()

	origSummaries := []OperatorAdminSummary{
		{ID: uuid.New(), OperatorConfig: OperatorConfig{Name: "original-op", URL: srv0.URL}, AcceptingLaunches: true},
	}
	scheduler := NewScheduler(nil)
	require.NoError(t, scheduler.Sync(origSummaries))

	// The original operator must be findable.
	require.NotNil(t, scheduler.ClientByName("original-op"), "original-op should exist before Sync")

	// --- Sync with two new operators ---
	srv1 := mockOperatorServer(3, false)
	defer srv1.Close()
	srv2 := mockOperatorServer(3, false)
	defer srv2.Close()

	newSummaries := []OperatorAdminSummary{
		{ID: uuid.New(), OperatorConfig: OperatorConfig{Name: "new-op-0", URL: srv1.URL}, AcceptingLaunches: true},
		{ID: uuid.New(), OperatorConfig: OperatorConfig{Name: "new-op-1", URL: srv2.URL}, AcceptingLaunches: true},
	}
	require.NoError(t, scheduler.Sync(newSummaries))

	// The old operator must be gone; new ones must be present.
	assert.Nil(t, scheduler.ClientByName("original-op"), "original-op should be absent after Sync")
	assert.NotNil(t, scheduler.ClientByName("new-op-0"), "new-op-0 should exist after Sync")
	assert.NotNil(t, scheduler.ClientByName("new-op-1"), "new-op-1 should exist after Sync")

	// --- race-condition stress test ---
	// Run Sync and SetTokenSource concurrently to exercise the RW-mutex under
	// `-race`. Each goroutine performs 100 iterations.
	var wg sync.WaitGroup
	const iterations = 100

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range iterations {
			// Alternate between the two operator lists so Sync exercises a
			// real list-shape change (1 ↔ 2 operators) under churn.
			summaries := newSummaries
			if i%2 == 1 {
				summaries = origSummaries
			}
			if err := scheduler.Sync(summaries); err != nil {
				t.Errorf("Sync failed during race: %v", err)
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		// staticTokenSource is a minimal oauth2.TokenSource for testing.
		ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "test-token"})
		for range iterations {
			scheduler.SetTokenSource(ts)
		}
	}()

	wg.Wait()

	// After concurrent churn the scheduler must still be operational.
	assert.NotNil(t, scheduler.Clients(), "scheduler should still have clients after concurrent Sync/SetTokenSource")
}

// TestFindAnalysisWithNoOperators pins an empty scheduler as an indeterminate
// answer rather than a miss. Callers read a nil client with a nil error as
// "genuinely not running anywhere" — for the expiration worker that means
// marking the analysis Completed, which must not happen on the strength of an
// answer no operator was ever asked for.
func TestFindAnalysisWithNoOperators(t *testing.T) {
	client, err := NewScheduler(nil).FindAnalysis(context.Background(), "analysis-1")

	assert.Nil(t, client)
	assert.ErrorIs(t, err, ErrNoOperators)
}
