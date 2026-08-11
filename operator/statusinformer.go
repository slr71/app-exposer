package operator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/cyverse-de/app-exposer/constants"
	"github.com/cyverse-de/messaging/v12"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// Lease tuning matches the controller-runtime defaults — short enough that
// failover takes ~15s, long enough to ride out brief API-server hiccups
// without thrashing.
const (
	statusLeaseDuration = 15 * time.Second
	statusRenewDeadline = 10 * time.Second
	statusRetryPeriod   = 2 * time.Second
)

// leaderLoopBackoff is the pause between leader-election attempts when the
// previous attempt returned (lost leadership or failed to acquire). Keeps the
// loop from hot-spinning if the API server briefly rejects lease writes.
const leaderLoopBackoff = 5 * time.Second

// StatusInformerConfig wires up StatusInformer. All fields are required.
type StatusInformerConfig struct {
	Clientset      kubernetes.Interface
	Publisher      *StatusPublisher
	Namespace      string // namespace to watch for VICE deployments
	LeaseNamespace string // namespace to hold the coordination lease
	LeaseName      string // lease object name (shared across operator replicas)
	Identity       string // unique per-replica identity, e.g. pod name
}

// StatusInformer runs a Kubernetes Deployment informer that publishes VICE
// analysis status updates to job-status-listener. Leader-elected via a
// coordination.k8s.io Lease so multi-replica operator deployments don't
// publish duplicate updates.
type StatusInformer struct {
	cfg StatusInformerConfig

	mu        sync.Mutex
	lastState map[constants.ExternalID]messaging.JobState
}

// NewStatusInformer validates the config and returns a StatusInformer ready
// to Run.
func NewStatusInformer(cfg StatusInformerConfig) (*StatusInformer, error) {
	if cfg.Clientset == nil {
		return nil, errors.New("clientset is required")
	}
	if cfg.Publisher == nil {
		return nil, errors.New("publisher is required")
	}
	if cfg.Namespace == "" {
		return nil, errors.New("namespace is required")
	}
	if cfg.LeaseNamespace == "" {
		return nil, errors.New("lease namespace is required")
	}
	if cfg.LeaseName == "" {
		return nil, errors.New("lease name is required")
	}
	if cfg.Identity == "" {
		return nil, errors.New("identity is required")
	}
	return &StatusInformer{
		cfg:       cfg,
		lastState: make(map[constants.ExternalID]messaging.JobState),
	}, nil
}

// Run blocks until ctx is done. Repeatedly attempts to acquire leadership; the
// informer only runs while this replica holds the lease.
func (s *StatusInformer) Run(ctx context.Context) error {
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      s.cfg.LeaseName,
			Namespace: s.cfg.LeaseNamespace,
		},
		Client: s.cfg.Clientset.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: s.cfg.Identity,
		},
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
			Lock:            lock,
			LeaseDuration:   statusLeaseDuration,
			RenewDeadline:   statusRenewDeadline,
			RetryPeriod:     statusRetryPeriod,
			ReleaseOnCancel: true,
			Name:            s.cfg.LeaseName,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(leaderCtx context.Context) {
					log.Infof("acquired status-publisher leadership (identity=%s); starting informer", s.cfg.Identity)
					s.resetState()
					s.runInformer(leaderCtx)
				},
				OnStoppedLeading: func() {
					log.Warnf("lost status-publisher leadership (identity=%s); informer stopped", s.cfg.Identity)
				},
				OnNewLeader: func(identity string) {
					if identity != s.cfg.Identity {
						log.Infof("observed new status-publisher leader: %s", identity)
					}
				},
			},
		})

		// RunOrDie returned — either leadership was lost or ctx is done.
		// Pause briefly so we don't hot-loop if the API server is rejecting
		// lease writes.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(leaderLoopBackoff):
		}
	}
}

// resetState clears the in-memory phase cache. Called when this replica
// becomes leader so that a freshly-elected leader doesn't suppress updates
// based on a stale view from a prior leadership term.
func (s *StatusInformer) resetState() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastState = make(map[constants.ExternalID]messaging.JobState)
}

// runInformer starts a Deployment informer and blocks until leaderCtx is
// done. Should only be invoked from the leader-election OnStartedLeading
// callback so that exactly one replica is publishing at a time.
func (s *StatusInformer) runInformer(leaderCtx context.Context) {
	set := labels.Set(map[string]string{
		constants.AppTypeLabel: string(constants.Interactive),
	})

	factory := informers.NewSharedInformerFactoryWithOptions(
		s.cfg.Clientset,
		0,
		informers.WithNamespace(s.cfg.Namespace),
		informers.WithTweakListOptions(func(opts *metav1.ListOptions) {
			opts.LabelSelector = set.AsSelector().String()
		}),
	)

	informer := factory.Apps().V1().Deployments().Informer()
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			dep, ok := obj.(*appsv1.Deployment)
			if !ok {
				log.Warnf("status informer add: unexpected object type %T", obj)
				return
			}
			s.handleAddOrUpdate(leaderCtx, dep)
		},
		UpdateFunc: func(_, newObj any) {
			dep, ok := newObj.(*appsv1.Deployment)
			if !ok {
				log.Warnf("status informer update: unexpected object type %T", newObj)
				return
			}
			s.handleAddOrUpdate(leaderCtx, dep)
		},
		DeleteFunc: func(obj any) {
			dep, ok := obj.(*appsv1.Deployment)
			if !ok {
				// On final-state-unknown the cache delivers a
				// DeletedFinalStateUnknown wrapper rather than the typed
				// object; pull the Deployment out if it's there.
				tombstone, isTomb := obj.(cache.DeletedFinalStateUnknown)
				if !isTomb {
					log.Warnf("status informer delete: unexpected object type %T", obj)
					return
				}
				dep, ok = tombstone.Obj.(*appsv1.Deployment)
				if !ok {
					log.Warnf("status informer delete tombstone: unexpected object type %T", tombstone.Obj)
					return
				}
			}
			s.handleDelete(leaderCtx, dep)
		},
	})
	if err != nil {
		log.Errorf("status informer: AddEventHandler failed: %v", err)
		return
	}

	factory.Start(leaderCtx.Done())
	if !cache.WaitForCacheSync(leaderCtx.Done(), informer.HasSynced) {
		log.Warn("status informer cache sync did not complete (leadership likely lost)")
		return
	}
	log.Info("status informer cache synced; publishing updates")

	<-leaderCtx.Done()
}

// handleAddOrUpdate publishes a Running update when a Deployment is observed
// with an available replica and we haven't already published Running for it.
// Add events for already-running analyses are coalesced via the lastState
// cache so re-syncs after leader handoff don't generate duplicate updates
// for any particular replica's lifetime — duplicates across leader handoffs
// are bounded but possible (see plans/push-based-status-updates.md).
func (s *StatusInformer) handleAddOrUpdate(ctx context.Context, dep *appsv1.Deployment) {
	if dep.DeletionTimestamp != nil {
		// A delete is in flight; let DeleteFunc handle the terminal state.
		return
	}

	externalID, ok := dep.Labels[constants.ExternalIDLabel]
	if !ok || externalID == "" {
		log.Warnf("status informer: deployment %s/%s missing %s label", dep.Namespace, dep.Name, constants.ExternalIDLabel)
		return
	}

	if dep.Status.AvailableReplicas < 1 {
		// Pod isn't ready yet; wait for the next event. The initial
		// Running update will fire on the first event where the
		// deployment reports an available replica.
		return
	}

	analysisName := dep.Labels[constants.AppNameLabel]
	message := fmt.Sprintf("deployment %s is running for analysis %s", dep.Name, analysisName)
	s.publishIfChanged(ctx, constants.ExternalID(externalID), messaging.RunningState, message)
}

// handleDelete publishes a Succeeded update when a Deployment is removed, on
// the assumption that deletion implies a clean finish. Known gap: a crashed
// pod (replicas 1 → 0) generates no event here
// because handleAddOrUpdate skips on AvailableReplicas < 1, so the eventual
// delete reports Succeeded over what was actually a failure. The reconciler
// safety net catches this only if it observes the Failed pod phase before
// the pod is gone; closing the gap fully needs a pod-level informer.
func (s *StatusInformer) handleDelete(ctx context.Context, dep *appsv1.Deployment) {
	externalID, ok := dep.Labels[constants.ExternalIDLabel]
	if !ok || externalID == "" {
		log.Warnf("status informer: deleted deployment %s/%s missing %s label", dep.Namespace, dep.Name, constants.ExternalIDLabel)
		return
	}

	analysisName := dep.Labels[constants.AppNameLabel]
	message := fmt.Sprintf("deployment %s has been deleted for analysis %s", dep.Name, analysisName)
	s.publishIfChanged(ctx, constants.ExternalID(externalID), messaging.SucceededState, message)

	// Drop the cache entry so a future re-launch of the same external ID
	// (unlikely, but possible) doesn't suppress the next Running update.
	s.mu.Lock()
	delete(s.lastState, constants.ExternalID(externalID))
	s.mu.Unlock()
}

// publishIfChanged posts a status update if the cached last-published state
// for this external ID differs from the new state. Updates the cache on
// successful publish; a failed publish leaves the cache unchanged so the
// next event retries.
func (s *StatusInformer) publishIfChanged(ctx context.Context, externalID constants.ExternalID, state messaging.JobState, message string) {
	s.mu.Lock()
	prev, known := s.lastState[externalID]
	s.mu.Unlock()

	if known && prev == state {
		return
	}

	if err := s.cfg.Publisher.Publish(ctx, externalID, state, message); err != nil {
		log.Errorf("status informer: publishing %s for %s failed (will retry on next event): %v", state, externalID, err)
		return
	}

	s.mu.Lock()
	s.lastState[externalID] = state
	s.mu.Unlock()
}
