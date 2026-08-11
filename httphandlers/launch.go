package httphandlers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/cyverse-de/app-exposer/common"
	"github.com/cyverse-de/app-exposer/constants"
	"github.com/cyverse-de/app-exposer/incluster"
	"github.com/cyverse-de/app-exposer/operatorclient"
	"github.com/cyverse-de/app-exposer/permissions"
	"github.com/cyverse-de/model/v10"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
)

// LaunchAppHandler validates a VICE analysis job and, if valid, accepts it for
// asynchronous launch. Returns 200 immediately after validation passes; the
// actual resource creation and operator scheduling happen in a background
// goroutine. This allows the caller's database transaction to commit (making
// the jobs row visible) before app-exposer attempts to update it.
//
// Idempotent — returns 200 if the analysis is already running.
//
//	@ID				launch-app
//	@Summary		Launch a VICE analysis
//	@Description	Validates the job and accepts it for asynchronous launch.
//	@Description	A 200 response means the job was accepted, not that it has been
//	@Description	fully deployed — resource creation and operator scheduling proceed
//	@Description	in the background.
//	@Accept			json
//	@Param			request						body	AnalysisLaunch	true	"The request body containing the analysis details"
//	@Param			disable-resource-tracking	query	boolean			false	"Bypass resource tracking"	default(false)
//	@Success		200
//	@Failure		400	{object}	common.ErrorResponse
//	@Failure		500	{object}	common.ErrorResponse
//	@Router			/vice/launch [post]
func (h *HTTPHandlers) LaunchAppHandler(c echo.Context) error {
	ctx := c.Request().Context()

	job := &model.Job{MountDataStore: true}
	if err := c.Bind(job); err != nil {
		return err
	}

	// Fast-path idempotency check: only consult the DB, not all operators.
	// For new launches the DB has no record, so a fan-out search would hit
	// every operator and get "not found" from all of them — wasted work that
	// becomes a problem during workshops with many concurrent launches.
	//
	// GetOperatorID normalizes sql.ErrNoRows to (uuid.Nil, nil); a non-nil
	// error here is a real DB fault. Treating it as "not running" would
	// silently drop the idempotency guard and can cause double-launches
	// during concurrent-launch bursts when the DB briefly blips.
	operatorID, err := h.apps.GetOperatorID(ctx, constants.AnalysisID(job.ID))
	if err != nil {
		log.Errorf("idempotency lookup failed for analysis %s: %v", job.ID, err)
		return echo.NewHTTPError(http.StatusServiceUnavailable, "idempotency check unavailable")
	}
	if operatorID != uuid.Nil {
		if client := h.scheduler.ClientByID(operatorID); client != nil {
			log.Infof("analysis %s already running on operator %s, returning success", job.ID, client.Name())
			return c.NoContent(http.StatusOK)
		}
	}

	if status, err := h.incluster.ValidateJob(ctx, job); err != nil {
		if validationErr, ok := err.(common.ErrorResponse); ok {
			return validationErr
		}
		return echo.NewHTTPError(status, err.Error())
	}

	// Validation passed — accept the job and do the heavy lifting in the
	// background. Returning early lets the caller commit its DB transaction,
	// which makes the jobs row visible for SetOperatorID below.
	//
	// Gate the background goroutine on launchSemaphore so a workshop-scale
	// burst can't spawn thousands of 2-minute-lifetime goroutines each
	// holding a 30-second HTTP timeout on an operator. If the semaphore is
	// full after a bounded wait, return 503 so the client can back off
	// rather than silently queueing behind an unbounded timer.
	release, ok := h.acquireLaunchSlot()
	if !ok {
		log.Warnf("launch semaphore saturated (cap=%d); returning 503 for analysis %s", cap(h.launchSemaphore), job.ID)
		return echo.NewHTTPError(http.StatusServiceUnavailable, "too many launches in flight; retry shortly")
	}
	go func() {
		defer release()
		h.launchAsync(job)
	}()

	return c.NoContent(http.StatusOK)
}

// launchAcquireTimeout bounds how long LaunchAppHandler will wait for a
// free launch slot before giving up and returning 503. Short enough that
// a blocked client can back off, long enough to absorb brief bursts
// without thrashing. A `var` rather than a `const` so tests can shrink
// it without dynamic injection plumbing; production code never writes to it.
var launchAcquireTimeout = 5 * time.Second

// acquireLaunchSlot reserves a slot in the launch semaphore, waiting up to
// launchAcquireTimeout. On success, returns a release callback the caller
// must invoke when its background goroutine finishes. On timeout, returns
// (nil, false). Isolated from LaunchAppHandler so the gating logic is
// directly testable without having to spin up a full validation pipeline.
func (h *HTTPHandlers) acquireLaunchSlot() (release func(), ok bool) {
	select {
	case h.launchSemaphore <- struct{}{}:
		return func() { <-h.launchSemaphore }, true
	case <-time.After(launchAcquireTimeout):
		return nil, false
	}
}

// launchAsync performs the resource-intensive parts of a VICE launch in
// the background: builds the deployment spec, reserves millicores, assembles
// the analysis bundle, schedules it on an operator, and records the operator
// assignment. Uses a background context with a timeout since the originating
// HTTP request has already completed.
//
// On failure, publishes a failure status update so the analysis transitions
// to "Failed" instead of staying stuck in "Submitted" indefinitely.
func (h *HTTPHandlers) launchAsync(job *model.Job) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Pre-build the deployment locally to calculate millicores reservation
	// and validate resource requirements. NOTE: this uses app-exposer's
	// resourcing defaults, not the target operator's ResourceDefaults. For
	// spec-path launches on an operator whose defaults differ, the reserved
	// millicores may be slightly off; unifying this is Phase 4 follow-up work
	// once every operator runs the spec path.
	deployment, err := h.incluster.GetDeployment(ctx, job)
	if err != nil {
		log.Errorf("async launch %s: failed to get deployment: %v", job.ID, err)
		h.failLaunch(ctx, job, fmt.Sprintf("failed to build deployment: %v", err))
		return
	}

	millicores, err := incluster.GetMillicoresFromDeployment(deployment)
	if err != nil {
		log.Errorf("async launch %s: failed to get millicores: %v", job.ID, err)
		h.failLaunch(ctx, job, fmt.Sprintf("failed to get millicores: %v", err))
		return
	}

	if err = h.apps.SetMillicoresReserved(job, millicores); err != nil {
		log.Errorf("async launch %s: failed to set millicores reserved: %v", job.ID, err)
		h.failLaunch(ctx, job, fmt.Sprintf("failed to reserve millicores: %v", err))
		return
	}

	// Build the cluster-agnostic spec and route to an operator. Uses job.ID
	// directly because job_steps rows don't exist yet at launch time. The
	// scheduler sends the spec to spec-aware operators and falls back to a
	// legacy AnalysisBundle for older ones; the bundle is built on demand only
	// if a spec-unaware operator is actually selected.
	analysisID := constants.AnalysisID(job.ID)
	spec, err := h.incluster.BuildVICESpec(ctx, job, analysisID)
	if err != nil {
		log.Errorf("async launch %s: failed to build VICE spec: %v", job.ID, err)
		h.failLaunch(ctx, job, fmt.Sprintf("failed to build VICE spec: %v", err))
		return
	}

	legacyBundle := func() (*operatorclient.AnalysisBundle, error) {
		return h.incluster.BuildAnalysisBundle(ctx, job, analysisID)
	}

	// Derive the analysis's routing subdomain and planned end date before the
	// operator is asked to launch, which is what makes this handler the canonical
	// writer of both rather than a straggler. The expiration package's job-status
	// consumer performs the same write as a safety net, and it cannot run until
	// the analysis reports Running — so writing first here keeps that path to the
	// anomaly it is meant to report. Best-effort for the same reason as
	// SetOperatorID below: a failure is recovered by that backfill rather than
	// failing the launch.
	if _, err := h.db.InitializeRuntime(ctx, analysisID, job.UserID, job.InvocationID); err != nil {
		log.Errorf("async launch %s: failed to set the subdomain and planned end date: %v", job.ID, err)
	}

	// Route to an available operator. The scheduler returns both the id (used
	// for the DB write below) and the name (used in human-readable log lines).
	operatorID, operatorName, err := h.scheduler.LaunchAnalysisSpec(ctx, spec, legacyBundle)
	if err != nil {
		log.Errorf("async launch %s: failed to launch analysis: %v", job.ID, err)
		h.failLaunch(ctx, job, fmt.Sprintf("failed to schedule analysis on operator: %v", err))
		return
	}

	// Best-effort record of which operator is running this analysis so
	// future requests can short-circuit the operator fan-out search. No
	// retry: if it fails, the fan-out path handles routing — acceptable
	// while we have a single VICE operator, and avoids piling more writes
	// onto an already-contended jobs table.
	if err := h.apps.SetOperatorID(ctx, constants.AnalysisID(job.ID), operatorID); err != nil {
		log.Errorf("async launch %s: failed to set operator id: %v", job.ID, err)
	}

	log.Infof("async launch %s: successfully launched on operator %s (id=%s)", job.ID, operatorName, operatorID)
}

// failLaunch publishes a failure status update for a launch that failed in the
// background goroutine. Uses the job's InvocationID (external ID) since the
// status publisher expects external IDs.
func (h *HTTPHandlers) failLaunch(ctx context.Context, job *model.Job, msg string) {
	if job.InvocationID == "" {
		log.Errorf("async launch %s: cannot publish failure status — no invocation ID", job.ID)
		return
	}
	if err := h.incluster.PublishFailure(ctx, job.InvocationID, msg); err != nil {
		log.Errorf("async launch %s: failed to publish failure status: %v", job.ID, err)
	}
}

// DryRunBundleHandler builds the AnalysisBundle for a job without launching it.
// Useful for debugging, testing, and inspecting what would be sent to a
// vice-operator without any side effects.
//
//	@ID				dry-run-bundle
//	@Summary		Build an AnalysisBundle without launching
//	@Description	Accepts the same model.Job body as /vice/launch but returns the
//	@Description	assembled AnalysisBundle JSON instead of dispatching it to an operator.
//	@Description	No side effects (no ConfigMaps, no scheduling, no DB writes).
//	@Accept			json
//	@Produce		json
//	@Param			request		body		AnalysisLaunch	true	"The job to build a bundle for"
//	@Param			validate	query		boolean			false	"Run validation checks on the job"	default(false)
//	@Success		200			{object}	operatorclient.AnalysisBundle
//	@Failure		400			{object}	common.ErrorResponse
//	@Failure		500			{object}	common.ErrorResponse
//	@Router			/vice/dry-run [post]
func (h *HTTPHandlers) DryRunBundleHandler(c echo.Context) error {
	ctx := c.Request().Context()

	job := &model.Job{MountDataStore: true}
	if err := c.Bind(job); err != nil {
		return err
	}

	// Opt-in validation via query parameter.
	if c.QueryParam("validate") == "true" {
		if status, err := h.incluster.ValidateJob(ctx, job); err != nil {
			if validationErr, ok := err.(common.ErrorResponse); ok {
				return validationErr
			}
			return echo.NewHTTPError(status, err.Error())
		}
	}

	bundle, err := h.incluster.BuildAnalysisBundle(ctx, job, constants.AnalysisID(job.ID))
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, bundle)
}

// checkURLReady verifies K8s resource readiness by delegating to the
// operator running the analysis. It returns a URLReadyResponse indicating
// if the deployment, service, and routing are fully live. A non-nil error
// means the operator could not be located (e.g. DB lookup failed) and the
// caller should surface that rather than reporting "not ready".
func (h *HTTPHandlers) checkURLReady(ctx context.Context, analysisID constants.AnalysisID) (operatorclient.URLReadyResponse, error) {
	client, err := h.operatorClientForAnalysis(ctx, analysisID)
	if err != nil {
		return operatorclient.URLReadyResponse{Ready: false}, err
	}
	if client == nil {
		return operatorclient.URLReadyResponse{Ready: false}, nil
	}

	resp, err := client.URLReady(ctx, analysisID)
	if err != nil {
		return operatorclient.URLReadyResponse{Ready: false}, fmt.Errorf("operator %s url-ready check failed: %w", client.Name(), err)
	}
	return *resp, nil
}

// URLReadyHandler checks whether the VICE analysis for the given subdomain is
// accessible, verifying user permissions before performing the check.
//
//	@ID				url-ready
//	@Summary		Check if a VICE app is ready for users to access it.
//	@Description	Returns whether or not a VICE app is ready
//	@Description	for users to access it. This version will check the user's permissions
//	@Description	and return an error if they aren't allowed to access the running app.
//	@Produce		json
//	@Param			user	query		string	true	"A user's username"
//	@Param			host	path		string	true	"The subdomain of the analysis. AKA the ingress name"
//	@Success		200		{object}	operatorclient.URLReadyResponse
//	@Failure		400		{object}	common.ErrorResponse
//	@Failure		403		{object}	common.ErrorResponse
//	@Failure		404		{object}	common.ErrorResponse
//	@Failure		500		{object}	common.ErrorResponse
//	@Router			/vice/{host}/url-ready [get]
func (h *HTTPHandlers) URLReadyHandler(c echo.Context) error {
	ctx := c.Request().Context()

	user := c.QueryParam("user")
	if user == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "user query parameter must be set")
	}

	// Since some usernames don't come through the labelling process unscathed, we have to use
	// the user ID.
	fixedUser := common.FixUsername(user, h.incluster.UserSuffix)
	_, err := h.apps.GetUserID(ctx, fixedUser)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return echo.NewHTTPError(http.StatusNotFound, fmt.Sprintf("user %s not found", fixedUser))
		}
		return err
	}

	host := c.Param("host")

	params := url.Values{}
	params.Set(constants.SubdomainLabel, host)

	// Search all operators for the analysis with this subdomain.
	listing, opErrs, err := h.aggregateListing(ctx, params)
	if err != nil {
		return err
	}
	if len(listing.Deployments) == 0 {
		// Distinguish "truly missing" from "all operators unreachable": a 404
		// for a still-running analysis whose operator is down would have users
		// staring at a misleading error.
		if len(opErrs) > 0 {
			log.Errorf("url-ready lookup degraded for subdomain %s: %+v", host, opErrs)
			return echo.NewHTTPError(http.StatusBadGateway, "operator listing unavailable")
		}
		return echo.NewHTTPError(http.StatusNotFound, fmt.Sprintf("no deployment found for subdomain %s", host))
	}

	externalID := listing.Deployments[0].ExternalID
	analysisID, err := h.apps.GetAnalysisIDByExternalID(ctx, externalID)
	if err != nil {
		return err
	}

	// Make sure the user has permissions to look up info about this analysis.
	p := &permissions.Permissions{
		BaseURL: h.incluster.PermissionsURL,
	}

	allowed, err := p.IsAllowed(ctx, user, string(analysisID))
	if err != nil {
		return err
	}

	if !allowed {
		return echo.NewHTTPError(http.StatusForbidden, fmt.Sprintf("user %s cannot access analysis %s", user, analysisID))
	}

	data, err := h.checkURLReady(ctx, analysisID)
	if err != nil {
		log.Errorf("url-ready check failed for analysis %s: %v", analysisID, err)
		return echo.NewHTTPError(http.StatusServiceUnavailable, "url-ready check unavailable")
	}

	return c.JSON(http.StatusOK, data)
}

// AdminURLReadyHandler checks K8s resource readiness for the given subdomain
// without user permission checks.
//
//	@ID				admin-url-ready
//	@Summary		Checks the status of a running VICE app in K8s
//	@Description	Handles requests to check the status of a running VICE app in K8s.
//	@Description	This will return an overall status and status for the individual containers in
//	@Description	the app's pod. Uses the state of the readiness checks in K8s, along with the
//	@Description	existence of the various resources created for the app.
//	@Produce		json
//	@Param			host	path		string	true	"The subdomain of the analysis"
//	@Success		200		{object}	operatorclient.URLReadyResponse
//	@Failure		400		{object}	common.ErrorResponse
//	@Failure		404		{object}	common.ErrorResponse
//	@Failure		500		{object}	common.ErrorResponse
//	@Router			/vice/admin/{host}/url-ready [get]
func (h *HTTPHandlers) AdminURLReadyHandler(c echo.Context) error {
	ctx := c.Request().Context()
	host := c.Param("host")

	params := url.Values{}
	params.Set(constants.SubdomainLabel, host)

	// Search all operators for the analysis with this subdomain.
	listing, opErrs, err := h.aggregateListing(ctx, params)
	if err != nil {
		return err
	}
	if len(listing.Deployments) == 0 {
		if len(opErrs) > 0 {
			log.Errorf("admin url-ready lookup degraded for subdomain %s: %+v", host, opErrs)
			return echo.NewHTTPError(http.StatusBadGateway, "operator listing unavailable")
		}
		return echo.NewHTTPError(http.StatusNotFound, fmt.Sprintf("no deployment found for subdomain %s", host))
	}

	externalID := listing.Deployments[0].ExternalID
	analysisID, err := h.apps.GetAnalysisIDByExternalID(ctx, externalID)
	if err != nil {
		return err
	}

	data, err := h.checkURLReady(ctx, analysisID)
	if err != nil {
		log.Errorf("url-ready check failed for analysis %s: %v", analysisID, err)
		return echo.NewHTTPError(http.StatusServiceUnavailable, "url-ready check unavailable")
	}

	return c.JSON(http.StatusOK, data)
}

// AnalysisInClusterResponse is the response body for the in-cluster check endpoints.
type AnalysisInClusterResponse struct {
	Found bool `json:"found"`
}

// AdminAnalysisInClusterByExternalID returns whether any operator claims to
// be running the analysis for the provided external ID. It translates the
// external ID to an analysis ID, then asks the scheduler; no cluster
// resources are inspected directly. Does not check if the requesting user
// has access to the analysis.
//
//	@ID				admin-analysis-in-cluster-by-external-id
//	@Summary		Returns whether any operator is running the analysis
//	@Description	Returns whether any operator claims to be running the analysis for the provided external ID, regardless of its state
//	@Produces		json
//	@Param			external-id	path		string	true	"external id"
//	@Success		200			{object}	AnalysisInClusterResponse
//	@Failure		400			{object}	common.ErrorResponse
//	@Failure		500			{object}	common.ErrorResponse
//	@Router			/vice/admin/is-deployed/external-id/{external-id} [get]
func (h *HTTPHandlers) AdminAnalysisInClusterByExternalID(c echo.Context) error {
	ctx := c.Request().Context()
	externalID := constants.ExternalID(c.Param(constants.ExternalIDLabel))
	if externalID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "external-id is not set")
	}

	analysisID, err := h.apps.GetAnalysisIDByExternalID(ctx, externalID)
	if err != nil {
		return err
	}

	client, err := h.operatorClientForAnalysis(ctx, analysisID)
	if err != nil {
		log.Errorf("operator routing unavailable for analysis %s: %v", analysisID, err)
		return echo.NewHTTPError(http.StatusServiceUnavailable, "operator routing temporarily unavailable")
	}
	return c.JSON(http.StatusOK, AnalysisInClusterResponse{Found: client != nil})
}

// AdminAnalysisInClusterByID returns whether any operator claims to be
// running the analysis with the provided analysis ID. It asks the
// scheduler; no cluster resources are inspected directly. Does not check
// if the requesting user has access to the analysis.
//
//	@ID				admin-analysis-in-cluster-by-id
//	@Summary		Returns whether any operator is running the analysis
//	@Description	Returns whether any operator claims to be running the analysis for the provided analysis ID, regardless of its state
//	@Produces		json
//	@Param			analysis-id	path		string	true	"analysis id"
//	@Success		200			{object}	AnalysisInClusterResponse
//	@Failure		400			{object}	common.ErrorResponse
//	@Failure		500			{object}	common.ErrorResponse
//	@Router			/vice/admin/is-deployed/analysis-id/{analysis-id} [get]
func (h *HTTPHandlers) AdminAnalysisInClusterByID(c echo.Context) error {
	ctx := c.Request().Context()
	analysisID := constants.AnalysisID(c.Param(constants.AnalysisIDLabel))
	if analysisID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "analysis-id is not set")
	}

	client, err := h.operatorClientForAnalysis(ctx, analysisID)
	if err != nil {
		log.Errorf("operator routing unavailable for analysis %s: %v", analysisID, err)
		return echo.NewHTTPError(http.StatusServiceUnavailable, "operator routing temporarily unavailable")
	}
	return c.JSON(http.StatusOK, AnalysisInClusterResponse{Found: client != nil})
}
