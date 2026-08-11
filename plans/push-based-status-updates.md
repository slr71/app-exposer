# Push-based VICE status updates via leader-elected operator informer

## Context

VICE analyses now run across multiple Kubernetes clusters (the `multi-cluster` work). The current status-update flow:

- **Local cluster:** `vice-status-listener` runs a K8s Deployment informer on `app-type=interactive` and POSTs Running/Success/Fail to `job-status-listener`. Sees only the QA cluster where it runs.
- **Remote clusters:** No informer is reachable, so `app-exposer`'s reconciler (`reconciler/reconciler.go`) polls every 30s, calls each operator's `/analyses` endpoint, and inserts rows directly into `job_status_updates`. Includes the PR #147 "initial seed" path that bootstraps the first row for new analyses.

Problems:
- **Latency floor**: remote-cluster transitions are invisible for up to ~30s + claim TTL skew.
- **Polling cost**: every operator gets a `GET /analyses` every 30s, plus a DB pass per pod.
- **Two code paths for the same job**: vice-status-listener and the reconciler implement the same idea twice.

Goal: replace the reconciler's hot path with low-latency push from each operator while keeping a slow safety net for missed events.

## Design

Add a leader-elected pod informer inside each vice-operator. On observed phase transitions, POST authenticated status updates to a new endpoint on `job-status-listener`. The reconciler stays but its interval moves from 30s → 5 min as a self-healing backstop.

### Why not the alternatives

- **vice-proxy push** — blind to OOMKilled, eviction, ImagePullBackOff (proxy doesn't start). Forces the reconciler to keep doing real work, so doesn't shrink anything.
- **Operator informer without leader election** — multi-replica operators spam N copies of every update. Receiver-side dedup hides DB rows but not wire traffic and log noise.
- **Delete the reconciler entirely** — every dropped HTTP push, leader-failover gap (~15s), or operator outage becomes a stuck analysis until a human intervenes. The cost of keeping it as a slow loop is tiny.

## Architecture

```
vice-operator (replica with leadership)
  └─ pod informer (apps/v1 Deployment, label app-type=interactive)
       └─ on add/update/delete with phase change
            └─ statusPublisher.Post(externalID, state, message, host)
                 └─ POST https://<de>/job/<externalID>/status
                      └─ job-status-listener publishes to AMQP → existing pipeline
```

### Authentication

**Deferred.** Operators POST to `job-status-listener` on its existing unauthenticated `/{uuid}/status` endpoint (the same endpoint `vice-status-listener` already uses). The listener is already exposed externally via the `discoenv` Gateway at `/job/*` with TLS termination, so remote operators can reach it.

A separate follow-up change will add Keycloak client-credentials auth + a `vice-status-publisher` role gate. That change is independent: we can lock it down after the new path is in place and the existing `vice-status-listener` is retired (which simplifies the auth migration because there's only one caller to switch over).

### Leader election

- Use `k8s.io/client-go/tools/leaderelection` with a `coordination.k8s.io/Lease` named e.g. `vice-operator-status-publisher` in the operator's namespace.
- Lease duration 15s, renew deadline 10s, retry period 2s (the controller-runtime defaults).
- Non-leaders continue to serve HTTP requests (launch, exit, listing, status); only the publisher loop is gated.

### Informer

- Pattern lifts directly from `vice-status-listener/main.go:185,219,267`:
  - `AddFunc` → Running
  - `DeleteFunc` → Succeeded (clean shutdown via save-and-exit) or Failed (need to disambiguate from deployment annotations / pod phase before deletion)
  - `UpdateFunc` → Running on transitions to Available
- We watch Deployments (matching the existing pattern), but consider also a pod informer for OOMKilled detection. Recommend Deployment-only for v1 to keep parity with vice-status-listener; the reconciler still catches pod-level events via the operator's `Listing()` mapping in `reconciler/reconciler.go:407-420`.

### Duplicates on leader handoff

When a new leader takes over, its informer's `AddFunc` fires for every existing pod — replaying the most recent state. With nothing persisted across leaders, the publisher can't tell "I saw this already." Expected impact: one duplicate update per running analysis per leader churn (rare, bounded). The existing pipeline already tolerates duplicate Running publishes — `vice-status-listener` does the same thing on every Deployment update. Receiver-side dedup is the right long-term fix and lands with the auth follow-up.

### Reconciler

- Bump `DefaultReconcileInterval` (`reconciler/reconciler.go:107`) from 30s → 5 min.
- Keep the PR #147 initial-seed path — still the only thing that catches an analysis that failed before its operator's informer saw it (e.g. ImagePullBackOff before any Deployment-add event from the operator's POV).
- Sync interval stays at 5 min.

### vice-status-listener retirement

**Done, 2026-08-11.** The new operator-internal informer covers the local cluster too, so `vice-status-listener` became redundant and has been retired. `operator/statusinformer.go` is now the only per-cluster status publisher; the standalone service, its deployments role, and its GitHub repo are gone.

The soak ran far longer than the week originally recommended — the informer went live in QA on 2026-05-18 and both publishers ran side by side for ~3 months. During that window the listener emitted 4-5 duplicate `Running` updates per analysis (one per Deployment update event) against the informer's one, and the remote `ai-sandboxes-qa` operator — which never had a listener — demonstrated the push path standing on its own.

One deliberate behavior change came with the retirement: the listener published `Running` the instant the Deployment object appeared, while the informer waits for `AvailableReplicas >= 1`. Analyses now stay in `Submitted` through image pull and report `Running` when the pod is actually ready.

## Files

### `app-exposer` (this repo)

- `operator/statuspublisher.go` (new) — HTTP client wrapping `job-status-listener` POSTs; reuse the bounded `http.Client` pattern from `operator/handlers.go:30` and `operator/transfers.go:34-39`.
- `operator/statusinformer.go` (new) — informer + leader election + change-detection; reuse phase-mapping logic from `reconciler/reconciler.go:407-420` (extract into a shared helper in `reconciler/` so both consumers use the same mapping).
- `cmd/vice-operator/startup.go` — wire informer factory, leader-election runnable, publisher config; same lifecycle as the existing kube-client setup.
- `cmd/vice-operator/main.go` — new flags: `--status-listener-url`, `--lease-namespace`, `--lease-name`, `--cluster-name` (used as `Hostname` in the status payload).
- `cmd/vice-operator/app.go` — pass publisher into the operator struct so handlers can also emit ad-hoc updates (e.g. on save-and-exit success) if needed later.
- `reconciler/reconciler.go` — bump `DefaultReconcileInterval` to `5 * time.Minute`.
- `reconciler/phase.go` (new, extracted) — `MapPodPhaseToStatus` so the informer and the reconciler agree.

### Deployments (`deployments/` ansible)

- Operator deployment templates: add lease RBAC (`coordination.k8s.io/leases` get/create/update on its own namespace), env var for the listener URL.
- No HTTPRoute change needed — `job-status-listener` is already exposed at `/job/*` via the `discoenv` Gateway with TLS (per `deployments/ansible/roles/kubernetes_ingress/tasks/gateway_api.yml:244-256,334-339`).

## Reused code & patterns

- `noRedirectHTTPClient` / `transferHTTPClient` bounded-timeout pattern from `operator/handlers.go:30` and `operator/transfers.go:34-39`.
- Phase mapping from `reconciler/reconciler.go:407-420` — extracted to a shared helper.
- vice-status-listener's `add/update/delete` informer wiring as the structural template for the operator-internal informer.
- `vice-status-listener/main.go:54-105` `JSLPublisher.postStatus()` body shape — the new publisher posts the same `{Host, State, Message}` JSON to the same endpoint.
- `k8s.io/client-go/tools/leaderelection` — standard controller pattern, no new dependency since client-go is already imported.

## Verification

End-to-end, per cluster:

1. **Happy path latency.** Launch a VICE analysis; observe `job_status_updates` shows `Running` within 1–2s of pod ready (vs. 0–30s today). Check listener logs for one POST per transition.
2. **Leader election.** Scale operator to 3 replicas. Confirm only the leader's logs show informer events. `kubectl get lease -n <ns> vice-operator-status-publisher` shows the holder. Delete the leader pod — failover takes <30s, no duplicate updates land.
3. **No duplicate spam in steady state.** Trigger a Deployment update on the leader; confirm exactly one POST per real transition.
4. **Reconciler safety net.** Stop the operator, change a pod's phase directly in K8s, restart the operator. Within 5 min the reconciler should backfill the row.
5. **Initial seed survives.** Launch an analysis where the pod fails before reaching Available (use a deliberately broken image). Confirm the reconciler's `errors.Is(err, sql.ErrNoRows)` path still fires the seed insert.
6. **Bounded duplicates on failover.** During failover (test #2), record any duplicate publishes — verify they're at most one per analysis and the downstream pipeline tolerates them.

Tests to add:
- Unit: `operator/statusinformer_test.go` for change-detection logic; `operator/statuspublisher_test.go` for the POST body shape and timeout behavior. Table-driven per CLAUDE.md.
- Integration: extend `reconciler/reconciler_test.go` to assert the bumped interval doesn't break the initial-seed path.

## Open questions (decide before implementation)

- **Cluster identifier in payload** — `vice-status-listener`'s current body is `{Host, State, Message}` with `Host` being the deployment hostname. We could put the cluster name there for the new path, or extend the body. Recommend keeping body identical to vice-status-listener for now and adding a cluster field with the auth follow-up.
- **Auth follow-up scope** — token/role/dedup all land together in a separate PR. Note in the implementation that the publisher's HTTP client is positioned to drop in a token-injecting transport later without restructuring.
