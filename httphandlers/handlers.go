package httphandlers

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/cyverse-de/app-exposer/adapter"
	"github.com/cyverse-de/app-exposer/apps"
	"github.com/cyverse-de/app-exposer/common"
	"github.com/cyverse-de/app-exposer/constants"
	"github.com/cyverse-de/app-exposer/db"
	"github.com/cyverse-de/app-exposer/incluster"
	"github.com/cyverse-de/app-exposer/operatorclient"
	"github.com/cyverse-de/app-exposer/reporting"
	"github.com/cyverse-de/model/v10"
	"github.com/google/uuid"
	"github.com/labstack/echo/v4"
	"github.com/lib/pq"
	"k8s.io/client-go/kubernetes"
)

var log = common.Log

// AnalysisLaunch is an alias for model.Analysis used as the HTTP request body
// for launching a VICE analysis.
type AnalysisLaunch model.Analysis

// HTTPHandlers holds the dependencies for all app-exposer HTTP handlers.
// launchSemaphore caps the number of in-flight launchAsync goroutines so
// workshop-scale bursts can't exhaust memory, goroutine budget, or flood
// downstream operators with simultaneous POSTs. Its capacity is set at
// construction time from the --max-concurrent-launches flag.
type HTTPHandlers struct {
	incluster       *incluster.Incluster
	apps            *apps.Apps
	clientset       kubernetes.Interface
	batchadapter    *adapter.JEXAdapter
	scheduler       *operatorclient.Scheduler
	db              *db.Database
	launchSemaphore chan struct{}
}

// DefaultMaxConcurrentLaunches bounds concurrent launchAsync goroutines per
// app-exposer instance unless --max-concurrent-launches overrides it. Set
// conservatively to contain workshop bursts without impacting steady-state
// traffic.
const DefaultMaxConcurrentLaunches = 50

// New creates an HTTPHandlers with the provided dependencies injected.
// maxConcurrentLaunches caps in-flight launchAsync goroutines; pass 0 to
// use DefaultMaxConcurrentLaunches.
func New(incluster *incluster.Incluster, apps *apps.Apps, clientset kubernetes.Interface, batchadapter *adapter.JEXAdapter, db *db.Database, maxConcurrentLaunches int) *HTTPHandlers {
	if maxConcurrentLaunches <= 0 {
		maxConcurrentLaunches = DefaultMaxConcurrentLaunches
	}
	return &HTTPHandlers{
		incluster:       incluster,
		apps:            apps,
		clientset:       clientset,
		batchadapter:    batchadapter,
		db:              db,
		launchSemaphore: make(chan struct{}, maxConcurrentLaunches),
	}
}

// SetScheduler configures the operator scheduler for multi-cluster routing.
// When set, launches and lifecycle operations are routed to remote operators.
func (h *HTTPHandlers) SetScheduler(s *operatorclient.Scheduler) {
	h.scheduler = s
	h.incluster.SetScheduler(s)
}

// GetScheduler returns the operator scheduler.
func (h *HTTPHandlers) GetScheduler() *operatorclient.Scheduler {
	return h.scheduler
}

// operatorClientForAnalysis looks up which operator is running an analysis
// and returns the corresponding client. Uses a two-step strategy:
//  1. Fast path: check the DB for a recorded operator id.
//  2. Search path: if no id is recorded or the operator with that id isn't
//     in the scheduler, search all operators in parallel via HasAnalysis.
//     Search-path hits are NOT written back to the DB — the launch handler
//     is the only writer of jobs.operator_id, by design, to keep write
//     contention off the jobs table. With a single VICE operator the
//     fan-out is cheap; revisit if we add more.
//
// A non-nil error indicates the lookup could not be completed (e.g. a
// database outage) and the caller should surface that to the client rather
// than treat the analysis as missing. Callers requiring a live client must
// treat nil-client-with-nil-error as "not found". Callers answering an
// "exists?" question may use nil-with-nil-error as "no". An empty scheduler
// is a miss rather than an error: there is nowhere for the analysis to be.
func (h *HTTPHandlers) operatorClientForAnalysis(ctx context.Context, analysisID constants.AnalysisID) (*operatorclient.Client, error) {
	// Fast path: check the DB for a recorded operator id. GetOperatorID
	// normalizes sql.ErrNoRows to (uuid.Nil, nil); a non-nil error here
	// signals a real DB fault, which we must surface instead of silently
	// falling through to fan-out search. Under burst traffic (e.g. workshop
	// launches), a brief DB blip otherwise amplifies into a fan-out storm.
	operatorID, err := h.apps.GetOperatorID(ctx, analysisID)
	if err != nil {
		return nil, fmt.Errorf("looking up operator for analysis %s: %w", analysisID, err)
	}

	if operatorID != uuid.Nil {
		client := h.scheduler.ClientByID(operatorID)
		if client != nil {
			log.Debugf("analysis %s routed to operator %q (id=%s, fast path)", analysisID, client.Name(), operatorID)
			return client, nil
		}
		log.Warnf("operator id %s not found in scheduler for analysis %s, searching all operators", operatorID, analysisID)
	} else {
		log.Debugf("no operator id recorded for analysis %s, searching all operators", analysisID)
	}

	// Search path: ask every operator in parallel whether it has this analysis.
	client, searchErr := h.searchOperatorsForAnalysis(ctx, analysisID)
	if client == nil {
		if errors.Is(searchErr, operatorclient.ErrNoOperators) {
			// There is nowhere for the analysis to be running, which answers
			// the question rather than failing it. Callers turn a non-nil error
			// into a 503, and the scheduler is legitimately empty for as long
			// as it takes the reconciler to sync the operators table after a
			// restart — a 503 there would tell the apps service to retry a
			// save-and-exit for an analysis that is genuinely gone.
			log.Warnf("no operators are registered, so analysis %s is not running anywhere", analysisID)
			return nil, nil
		}
		if searchErr != nil {
			// Some operators errored AND none reported found. We can't
			// tell whether the analysis is genuinely missing or hiding
			// on a degraded cluster, so surface the error rather than
			// letting the handler return a misleading 404.
			return nil, searchErr
		}
		log.Warnf("no operator has analysis %s", analysisID)
		return nil, nil
	}

	log.Infof("analysis %s found on operator %q (id=%s, search path)", analysisID, client.Name(), client.ID())
	return client, nil
}

// searchOperatorsForAnalysis asks every operator which one is running the given
// analysis. See Scheduler.FindAnalysis for the (nil, nil) vs (nil, err)
// semantics — a miss is only authoritative when the error is nil.
func (h *HTTPHandlers) searchOperatorsForAnalysis(ctx context.Context, analysisID constants.AnalysisID) (*operatorclient.Client, error) {
	return h.scheduler.FindAnalysis(ctx, analysisID)
}

// operatorLabelParams is the set of query parameter names that the vice-operator
// understands as K8s label selectors on the GET /analyses listing endpoint.
// Any other params (pagination, sort, phase, etc.) must be stripped before
// forwarding, as the operator passes all query params directly to the K8s API
// as label selectors and unknown keys will cause list errors.
var operatorLabelParams = map[string]struct{}{
	constants.AnalysisIDLabel: {},
	constants.AppIDLabel:      {},
	constants.AppNameLabel:    {},
	constants.ExternalIDLabel: {},
	constants.UsernameLabel:   {},
	constants.UserIDLabel:     {},
	constants.SubdomainLabel:  {},
}

// operatorListingParams returns a copy of params containing only the keys
// that the vice-operator listing endpoint understands.
func operatorListingParams(params url.Values) url.Values {
	filtered := url.Values{}
	for k, v := range params {
		if _, ok := operatorLabelParams[k]; ok {
			filtered[k] = v
		}
	}
	return filtered
}

// aggregateListing queries all configured operators in parallel, applying the
// provided filters, and merges the results into a single ResourceInfo.
// Partial results are returned if some operators are unreachable.
func (h *HTTPHandlers) aggregateListing(ctx context.Context, params url.Values) (*reporting.ResourceInfo, []OperatorError, error) {
	merged := reporting.NewResourceInfo()
	clients := h.scheduler.Clients()
	if len(clients) == 0 {
		return merged, nil, nil
	}

	opParams := operatorListingParams(params)

	type result struct {
		info *reporting.ResourceInfo
		name string
		err  error
	}
	results := make([]result, len(clients))

	var wg sync.WaitGroup
	for i, client := range clients {
		wg.Go(func() {
			info, err := client.Listing(ctx, opParams)
			results[i] = result{info: info, name: client.Name(), err: err}
		})
	}
	wg.Wait()

	var opErrs []OperatorError
	for _, r := range results {
		if r.err != nil {
			log.Errorf("error listing analyses from operator %s: %v", r.name, r.err)
			opErrs = append(opErrs, OperatorError{Operator: r.name, Error: r.err.Error()})
			continue
		}
		merged.Deployments = append(merged.Deployments, r.info.Deployments...)
		merged.Pods = append(merged.Pods, r.info.Pods...)
		merged.ConfigMaps = append(merged.ConfigMaps, r.info.ConfigMaps...)
		merged.Services = append(merged.Services, r.info.Services...)
		merged.Ingresses = append(merged.Ingresses, r.info.Ingresses...)
		merged.Routes = append(merged.Routes, r.info.Routes...)
	}

	reporting.SortByCreationTime(merged)
	return merged, opErrs, nil
}

// OperatorError represents an error returned by an operator during a listing
// request.
type OperatorError struct {
	Operator string `json:"operator"`
	Error    string `json:"error"`
}

// aggregatedFailureResponse is the JSON body returned when every configured
// operator failed and no listing data is available.
type aggregatedFailureResponse struct {
	Message        string          `json:"error"`
	OperatorErrors []OperatorError `json:"operator_errors"`
}

// respondAggregated writes the response for an aggregateListing result. When
// no results are present and at least one operator errored, it returns 502
// Bad Gateway — the alternative (200 with an empty list) silently hides a
// cluster-wide outage. Callers pass the populated response body whose type
// is expected to carry a JSON-omitempty OperatorErrors field so partial-
// success can be surfaced without changing the 200 path for healthy clusters.
func respondAggregated(c echo.Context, body any, opErrs []OperatorError, hasResults bool) error {
	if !hasResults && len(opErrs) > 0 {
		return c.JSON(http.StatusBadGateway, aggregatedFailureResponse{
			Message:        "all operators unreachable or returned errors",
			OperatorErrors: opErrs,
		})
	}
	return c.JSON(http.StatusOK, body)
}

// ResourceInfoResponse wraps reporting.ResourceInfo with the list of
// per-operator errors encountered during aggregation. Consumers that care
// only about the resource data can keep ignoring the new field thanks to
// omitempty.
type ResourceInfoResponse struct {
	reporting.ResourceInfo
	OperatorErrors []OperatorError `json:"operator_errors,omitempty"`
}

// operatorAction is a function that performs an operation on an operator client
// for a given analysis. Used by routeOperatorAction and routeAdminOperatorAction
// to eliminate boilerplate in handlers that resolve an ID and forward to an operator.
type operatorAction func(ctx context.Context, client *operatorclient.Client, analysisID constants.AnalysisID) error

// routeOperatorAction resolves an external ID to an analysis ID, finds the
// operator running it, and invokes fn. Intended for user-facing handlers that
// receive an external ID via path param "id".
func (h *HTTPHandlers) routeOperatorAction(c echo.Context, fn operatorAction) error {
	ctx := c.Request().Context()
	externalID := constants.ExternalID(c.Param("id"))

	analysisID, err := h.apps.GetAnalysisIDByExternalID(ctx, externalID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return echo.NewHTTPError(http.StatusNotFound, "analysis not found for external ID")
		}
		log.Errorf("error looking up analysis for external ID %s: %v", externalID, err)
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to look up analysis")
	}

	client, err := h.operatorClientForAnalysis(ctx, analysisID)
	if err != nil {
		log.Errorf("operator routing unavailable for analysis %s: %v", analysisID, err)
		return echo.NewHTTPError(http.StatusServiceUnavailable, "operator routing temporarily unavailable")
	}
	if client == nil {
		return echo.NewHTTPError(http.StatusNotFound, "no operator found for analysis")
	}

	return fn(ctx, client, analysisID)
}

// routeAdminOperatorAction finds the operator running the analysis identified
// by the constants.AnalysisIDLabel path param and invokes fn. Intended for admin handlers
// that receive an analysis ID directly.
func (h *HTTPHandlers) routeAdminOperatorAction(c echo.Context, fn operatorAction) error {
	ctx := c.Request().Context()
	analysisID := constants.AnalysisID(c.Param(constants.AnalysisIDLabel))

	client, err := h.operatorClientForAnalysis(ctx, analysisID)
	if err != nil {
		log.Errorf("operator routing unavailable for analysis %s: %v", analysisID, err)
		return echo.NewHTTPError(http.StatusServiceUnavailable, "operator routing temporarily unavailable")
	}
	if client == nil {
		return echo.NewHTTPError(http.StatusNotFound, "no operator found for analysis")
	}

	return fn(ctx, client, analysisID)
}

// ExternalIDResp is the response body for the AdminGetExternalIDHandler endpoint.
type ExternalIDResp struct {
	ExternalID string `json:"external_id" example:"bb52aefb-e021-4ece-89e5-fd73ce30643c"`
}

// AdminGetExternalIDHandler returns the external ID associated with the analysis ID.
// There is only one external ID for each VICE analysis, unlike non-VICE analyses.
//
//	@ID				admin-get-external-id
//	@Summary		Returns external ID
//	@Description	Returns the external ID associated with the provided analysis ID.
//	@Description	Only returns the first external ID in multi-step analyses.
//	@Produces		json
//	@Param			analysis-id	path		string	true	"analysis UUID"	minLength(36)	maxLength(36)
//	@Success		200			{object}	ExternalIDResp
//	@Failure		500			{object}	common.ErrorResponse
//	@Failure		400			{object}	common.ErrorResponse	"id parameter is empty"
//	@Router			/vice/admin/analyses/{analysis-id}/external-id [get]
func (h *HTTPHandlers) AdminGetExternalIDHandler(c echo.Context) error {
	ctx := c.Request().Context()

	analysisID := constants.AnalysisID(c.Param(constants.AnalysisIDLabel))
	if analysisID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "id parameter is empty")
	}

	externalID, err := h.incluster.GetExternalIDByAnalysisID(ctx, analysisID)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, ExternalIDResp{ExternalID: string(externalID)})
}

// AsyncData contains metadata that is computed asynchronously after job launch:
// the analysis ID, the routing subdomain, and the user's login IP.
type AsyncData struct {
	AnalysisID string `json:"analysisID"`
	Subdomain  string `json:"subdomain"`
	IPAddr     string `json:"ipAddr"`
}

// AsyncDataHandler returns data that is generated asynchronously from the job launch.
//
//	@ID				async-data
//	@Summary		Returns data that is generated asynchronously from the job launch.
//	@Description	Returns data that is applied to analyses outside of an API call.
//	@Description	The returned data is not returned asynchronously, despite the name of the call.
//	@Param			external-id	query		string	true	"External ID"
//	@Success		200			{object}	AsyncData
//	@Failure		500			{object}	common.ErrorResponse
//	@Failure		400			{object}	common.ErrorResponse
//	@Router			/vice/async-data [get]
func (h *HTTPHandlers) AsyncDataHandler(c echo.Context) error {
	ctx := c.Request().Context()
	externalID := constants.ExternalID(c.QueryParam(constants.ExternalIDLabel))
	if externalID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "external-id not set")
	}

	analysisID, err := h.apps.GetAnalysisIDByExternalID(ctx, externalID)
	if err != nil {
		log.Error(err)
		return err
	}

	var userID string

	client, err := h.operatorClientForAnalysis(ctx, analysisID)
	if err != nil {
		log.Errorf("operator routing unavailable for analysis %s: %v", analysisID, err)
		return echo.NewHTTPError(http.StatusServiceUnavailable, "operator routing temporarily unavailable")
	}
	if client == nil {
		return echo.NewHTTPError(http.StatusNotFound, "analysis not found on any operator")
	}

	// The operator status includes the deployment names. We need the labels.
	// Since we can't easily get labels from the operator Status endpoint,
	// we'll use the Listing endpoint which is more verbose but has everything.
	info, err := client.Listing(ctx, nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	for _, d := range info.Deployments {
		if d.ExternalID == externalID {
			userID = d.UserID
			break
		}
	}

	if userID == "" {
		return echo.NewHTTPError(http.StatusNotFound, "user-id not found for analysis")
	}

	subdomain := common.Subdomain(userID, string(externalID))
	ipAddr, err := h.apps.GetUserIP(ctx, userID)
	if err != nil {
		log.Error(err)
		return err
	}

	return c.JSON(http.StatusOK, AsyncData{
		AnalysisID: string(analysisID),
		Subdomain:  subdomain,
		IPAddr:     ipAddr,
	})
}

// AdminOperatorsHandler lists all operators registered in the database,
// returning each operator's id, name, URL, TLS skip-verify flag, and
// priority. The id is included so admin clients can address an operator by
// its stable UUID for PATCH/DELETE operations.
//
//	@ID				admin-list-operators
//	@Summary		Lists registered operators
//	@Description	Returns id, name, URL, tls_skip_verify, and priority for all operators in the database.
//	@Produce		json
//	@Success		200	{array}		operatorclient.OperatorAdminSummary
//	@Failure		500	{object}	common.ErrorResponse
//	@Router			/vice/admin/operators [get]
func (h *HTTPHandlers) AdminOperatorsHandler(c echo.Context) error {
	ctx := c.Request().Context()

	ops, err := h.db.ListOperatorAdminSummaries(ctx)
	if err != nil {
		log.Errorf("failed to list operators: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, ops)
}

// OperatorCapacity contains the capacity status of an operator or an error
// if it could not be reached.
type OperatorCapacity struct {
	Operator string                           `json:"operator"`
	Capacity *operatorclient.CapacityResponse `json:"capacity,omitempty"`
	Error    string                           `json:"error,omitempty"`
}

// AdminOperatorCapacitiesHandler returns the live capacity status of all
// configured operators by querying each one in parallel.
//
//	@ID				admin-operator-capacities
//	@Summary		Returns operator capacities
//	@Description	Queries each configured operator's capacity endpoint in parallel and returns the results.
//	@Produce		json
//	@Success		200	{array}		OperatorCapacity
//	@Failure		500	{object}	common.ErrorResponse
//	@Router			/vice/admin/operators/capacities [get]
func (h *HTTPHandlers) AdminOperatorCapacitiesHandler(c echo.Context) error {
	if h.scheduler == nil {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "operator scheduler not configured")
	}

	ctx := c.Request().Context()
	clients := h.scheduler.Clients()

	results := make([]OperatorCapacity, len(clients))
	var wg sync.WaitGroup

	for i, client := range clients {
		wg.Add(1)
		go func(idx int, cl *operatorclient.Client) {
			defer wg.Done()
			capResp, err := cl.Capacity(ctx)
			status := OperatorCapacity{Operator: cl.Name()}
			if err != nil {
				status.Error = err.Error()
			} else {
				status.Capacity = capResp
			}
			results[idx] = status
		}(i, client)
	}
	wg.Wait()

	return c.JSON(http.StatusOK, results)
}

// validateOperatorFields validates the optional name, url, and base_url for
// an operator request. nil pointers indicate the field is absent (a valid
// state for a partial update); non-nil pointers are validated against the
// same rules the operators table's CHECK and UNIQUE constraints enforce —
// non-whitespace text, and for the URL fields an HTTP(S) URL with a host.
// Uniqueness is checked at the DB layer rather than here. Requiredness (e.g.
// base_url at create time) is enforced by the calling handler, not here.
func validateOperatorFields(name, urlStr, baseURL *string) error {
	if name != nil && strings.TrimSpace(*name) == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "name must not be empty or whitespace")
	}
	if err := validateHTTPURL("url", urlStr); err != nil {
		return err
	}
	return validateHTTPURL("base_url", baseURL)
}

// validateHTTPURL checks that a non-nil pointer holds a non-whitespace
// HTTP(S) URL with a host. A nil pointer (absent field) is valid.
func validateHTTPURL(field string, urlStr *string) error {
	if urlStr == nil {
		return nil
	}
	if strings.TrimSpace(*urlStr) == "" {
		return echo.NewHTTPError(http.StatusBadRequest, field+" must not be empty or whitespace")
	}
	parsed, err := url.Parse(*urlStr)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return echo.NewHTTPError(http.StatusBadRequest, field+" must be a valid HTTP(S) URL")
	}
	return nil
}

// operatorWriteError maps an InsertOperator/UpdateOperatorByID error to the
// appropriate HTTP status. UNIQUE-constraint violations (SQLSTATE 23505)
// from name- or url-collisions become 409 Conflict; everything else is a
// 500. Callers handle sql.ErrNoRows separately because only update can
// return it.
func operatorWriteError(operation string, identifier string, err error) error {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && pqErr.Code == "23505" {
		log.Warnf("%s operator %q rejected by unique constraint: %v", operation, identifier, err)
		return echo.NewHTTPError(http.StatusConflict, "operator with that name or url already exists")
	}
	log.Errorf("failed to %s operator %q: %v", operation, identifier, err)
	return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
}

// CreateOperatorHandler adds a new operator to the database. The response
// echoes the persisted row's id alongside the public config fields so
// callers can address the new operator by id (e.g., for a follow-up PATCH
// or DELETE) without a separate list call.
//
//	@ID				admin-create-operator
//	@Summary		Creates a new operator
//	@Description	Adds a new operator to the database. Returns the new row including its server-assigned UUID.
//	@Accept			json
//	@Produce		json
//	@Param			body	body		operatorclient.OperatorConfig	true	"Operator to create"
//	@Success		201		{object}	operatorclient.OperatorAdminSummary
//	@Failure		400		{object}	common.ErrorResponse
//	@Failure		409		{object}	common.ErrorResponse
//	@Failure		500		{object}	common.ErrorResponse
//	@Router			/vice/admin/operators [post]
func (h *HTTPHandlers) CreateOperatorHandler(c echo.Context) error {
	ctx := c.Request().Context()

	// The request body uses operatorclient.OperatorConfig because clients
	// don't supply an id at create time — id is server-assigned. The
	// response shape (OperatorAdminSummary) carries the new id back.
	var req operatorclient.OperatorConfig
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	// name, url, and base_url are all required at create time. base_url is a
	// pointer (nullable column), so its presence is checked explicitly here;
	// name and url are value types and always "present". Format/whitespace
	// validation is left to validateOperatorFields so the error messages
	// stay consistent across all three fields.
	if req.BaseURL == nil {
		return echo.NewHTTPError(http.StatusBadRequest, "base_url is required")
	}
	if err := validateOperatorFields(&req.Name, &req.URL, req.BaseURL); err != nil {
		return err
	}

	op := &db.Operator{
		Name:          req.Name,
		URL:           req.URL,
		TLSSkipVerify: req.TLSSkipVerify,
		Priority:      req.Priority,
		BaseURL:       req.BaseURL,
	}

	created, err := h.db.InsertOperator(ctx, op)
	if err != nil {
		return operatorWriteError("insert", req.Name, err)
	}

	// Project to the admin-summary shape, dropping timestamps and
	// reconciliation state.
	return c.JSON(http.StatusCreated, created.ToOperatorAdminSummary())
}

// UpdateOperatorHandler applies a partial update to the operator with the
// given UUID. The path identifies the row by id rather than name so that
// renames don't break the path semantics: PATCH targets the same row even
// if the name field is being changed in the body.
//
//	@ID				admin-update-operator
//	@Summary		Updates an operator
//	@Description	Partial update of an operator identified by UUID. Only fields supplied in the body are changed. The response carries the row's id alongside the updated fields, mirroring the create endpoint.
//	@Accept			json
//	@Produce		json
//	@Param			id		path		string									true	"Operator UUID"
//	@Param			body	body		operatorclient.UpdateOperatorRequest	true	"Fields to update"
//	@Success		200		{object}	operatorclient.OperatorAdminSummary
//	@Failure		400		{object}	common.ErrorResponse
//	@Failure		404		{object}	common.ErrorResponse
//	@Failure		409		{object}	common.ErrorResponse
//	@Failure		500		{object}	common.ErrorResponse
//	@Router			/vice/admin/operators/id/{id} [patch]
func (h *HTTPHandlers) UpdateOperatorHandler(c echo.Context) error {
	ctx := c.Request().Context()

	idStr := c.Param("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "id must be a valid UUID")
	}

	var req operatorclient.UpdateOperatorRequest
	if err := c.Bind(&req); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	if req.Name == nil && req.URL == nil && req.TLSSkipVerify == nil && req.Priority == nil &&
		req.BaseURL == nil && req.AcceptingLaunches == nil && req.Deactivated == nil {
		return echo.NewHTTPError(http.StatusBadRequest, "request body must update at least one field")
	}

	if err := validateOperatorFields(req.Name, req.URL, req.BaseURL); err != nil {
		return err
	}

	updated, err := h.db.UpdateOperatorByID(ctx, id, db.OperatorUpdate{
		Name:              req.Name,
		URL:               req.URL,
		TLSSkipVerify:     req.TLSSkipVerify,
		Priority:          req.Priority,
		BaseURL:           req.BaseURL,
		AcceptingLaunches: req.AcceptingLaunches,
		Deactivated:       req.Deactivated,
	})
	if errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusNotFound, "operator not found")
	}
	if err != nil {
		return operatorWriteError("update", idStr, err)
	}

	return c.JSON(http.StatusOK, updated.ToOperatorAdminSummary())
}

// DeleteOperatorHandler deletes an operator by UUID. The operation is
// idempotent for operators with no associated jobs: deleting a non-existent
// operator returns 200. Deleting an operator that still has jobs referencing
// it will fail due to the jobs.operator_id ON DELETE RESTRICT FK.
//
//	@ID				admin-delete-operator
//	@Summary		Deletes an operator by id
//	@Description	Removes the operator with the given UUID. Succeeds silently if the operator does not exist. Fails if jobs still reference the operator.
//	@Param			id	path	string	true	"Operator UUID"
//	@Success		200
//	@Failure		400	{object}	common.ErrorResponse
//	@Failure		500	{object}	common.ErrorResponse
//	@Router			/vice/admin/operators/id/{id} [delete]
func (h *HTTPHandlers) DeleteOperatorHandler(c echo.Context) error {
	ctx := c.Request().Context()

	idStr := c.Param("id")
	id, err := uuid.Parse(idStr)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "id must be a valid UUID")
	}

	if err := h.db.DeleteOperatorByID(ctx, id); err != nil {
		log.Errorf("failed to delete operator %q: %v", idStr, err)
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to delete operator")
	}

	return c.NoContent(http.StatusOK)
}
