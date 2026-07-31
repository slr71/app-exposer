package operator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/cyverse-de/app-exposer/common"
	"github.com/cyverse-de/app-exposer/constants"
	"github.com/cyverse-de/app-exposer/operatorclient"
	"github.com/cyverse-de/app-exposer/reporting"
	"github.com/cyverse-de/app-exposer/vicebuild"
	"github.com/labstack/echo/v4"
	"github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	gatewayclient "sigs.k8s.io/gateway-api/pkg/client/clientset/versioned/typed/apis/v1"
)

// HTTPClient is an interface that matches http.Client's Do method.
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// noRedirectHTTPClient is used for vice-proxy requests where a redirect
// indicates an auth wall rather than a valid response.
var noRedirectHTTPClient HTTPClient = &http.Client{
	Timeout: 5 * time.Second,
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

var log = common.Log.WithFields(logrus.Fields{"package": "operator"})

// requiredParam extracts a path parameter and returns 400 if it's empty.
func requiredParam(c echo.Context, name string) (string, error) {
	v := c.Param(name)
	if v == "" {
		return "", echo.NewHTTPError(http.StatusBadRequest, name+" is required")
	}
	return v, nil
}

// Operator holds the state and dependencies for the vice-operator HTTP handlers.
type Operator struct {
	clientset           kubernetes.Interface
	gatewayClient       gatewayclient.GatewayV1Interface
	namespace           string
	gatewayNamespace    string
	gatewayName         string
	gpuVendor           GPUVendor
	gpuModels           []string          // Canonical GFD-style GPU model names this cluster can deliver; empty means model-agnostic.
	gpuModelAffinityKey string            // Node-label key this cluster uses for GPU-model affinity; empty means use the canonical nvidia.com/gpu.product.
	gpuModelMapping     map[string]string // Canonical NVIDIA-* model name → cluster-side value (e.g. NVIDIA-A10G → a10g on EKS).
	capacityCalc        *CapacityCalculator
	imageCache          ImageCacheManager
	imageRewriter       ImageRewriter // optional; manual-mirror mode supplies one
	loadingServiceName  string
	loadingServicePort  int32
	loadingTimeoutMs    int64
	baseDomain          string
	clusterConfigSecret string              // Name of the Secret holding cluster config for vice-proxy envFrom.
	egressConfig        NetworkPolicyConfig // Egress policy config for per-analysis policies.
	httpClient          HTTPClient          // Client for contacting the vice-proxy sidecar.
	userSuffix          string              // Domain suffix for usernames (e.g. "@iplantcollaborative.org").
	localStorageClass   string              // StorageClass for the per-analysis working-dir PVC; empty means cluster default.

	// Construction config for the operator-side VICESpec build path. These are
	// the cluster values vicebuild needs that the legacy transform path didn't
	// require app-exposer to know. Populated from cmd/vice-operator flags.
	porklockImage           string
	porklockTag             string
	viceProxyImage          string
	useCSIDriver            bool
	frontendBaseURL         string
	irodsZone               string
	inputPathListIdentifier string
	gatewayProvider         string
	imagePullSecretName     string
	resourceDefaults        vicebuild.ResourceDefaults
	caBundleConfigMap       string // ConfigMap in namespace holding the CA vice-proxy trusts; empty leaves the image's trust store alone.
	caBundleKey             string
	// disableSpecLaunch turns off the operator-side spec build path: the
	// operator advertises SpecVersion 0 (so app-exposer routes it a legacy
	// bundle) and rejects direct spec launches. The per-operator rollback lever.
	disableSpecLaunch bool
}

// OperatorOptions aggregates everything NewOperator needs. Held as a
// struct so new fields don't churn every caller, and so Validate()
// centralizes the required-field checks that were previously open-coded
// panics inside NewOperator.
type OperatorOptions struct {
	Clientset           kubernetes.Interface
	GatewayClient       gatewayclient.GatewayV1Interface
	Namespace           string
	GatewayNamespace    string
	GatewayName         string
	GPUVendor           GPUVendor
	GPUModels           []string          // Canonical GFD-style GPU model names this cluster can deliver; empty means model-agnostic.
	GPUModelAffinityKey string            // Node-label key this cluster uses for GPU-model affinity; empty means use the canonical nvidia.com/gpu.product.
	GPUModelMapping     map[string]string // Canonical NVIDIA-* model name → cluster-side value (e.g. NVIDIA-A10G → a10g on EKS).
	CapacityCalc        *CapacityCalculator
	ImageCache          ImageCacheManager
	ImageRewriter       ImageRewriter // optional; nil disables image-ref rewriting at launch time
	LoadingServiceName  string
	LoadingServicePort  int32
	LoadingTimeoutMs    int64
	BaseDomain          string
	ClusterConfigSecret string              // Name of the Secret holding cluster config for vice-proxy envFrom.
	EgressConfig        NetworkPolicyConfig // Egress policy config for per-analysis policies.
	UserSuffix          string              // Domain suffix for usernames (e.g. "@iplantcollaborative.org").
	LocalStorageClass   string              // StorageClass for the per-analysis working-dir PVC; empty means cluster default.

	// VICESpec construction config (see Operator). These drive the
	// operator-side build path; the legacy bundle path ignores them.
	PorklockImage           string
	PorklockTag             string
	ViceProxyImage          string
	UseCSIDriver            bool
	FrontendBaseURL         string
	IRODSZone               string
	InputPathListIdentifier string
	GatewayProvider         string
	ImagePullSecretName     string
	ResourceDefaults        vicebuild.ResourceDefaults
	CABundleConfigMap       string // ConfigMap in Namespace holding the CA vice-proxy trusts; empty leaves the image's trust store alone.
	CABundleKey             string
	DisableSpecLaunch       bool
}

// Validate confirms the wiring-critical fields are present. The caller
// (cmd/vice-operator/main.go) is expected to log.Fatal on error: these
// failures indicate a broken startup config, not a recoverable runtime
// condition. Delegates to EgressConfig.Validate for egress-specific
// checks so NetworkPolicyConfig remains the single source of truth for
// its own invariants.
func (o OperatorOptions) Validate() error {
	if o.Clientset == nil {
		return fmt.Errorf("operator: Clientset must not be nil")
	}
	if o.GatewayClient == nil {
		return fmt.Errorf("operator: GatewayClient must not be nil")
	}
	if o.Namespace == "" {
		return fmt.Errorf("operator: Namespace must not be empty")
	}
	if o.CapacityCalc == nil {
		return fmt.Errorf("operator: CapacityCalc must not be nil")
	}
	if o.ImageCache == nil {
		return fmt.Errorf("operator: ImageCache must not be nil")
	}
	if err := o.EgressConfig.Validate(); err != nil {
		return fmt.Errorf("operator: EgressConfig: %w", err)
	}
	return nil
}

// NewOperator creates a new Operator. Returns an error if opts fails
// validation so the caller can surface a clear startup failure.
func NewOperator(opts OperatorOptions) (*Operator, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}
	return &Operator{
		clientset:           opts.Clientset,
		gatewayClient:       opts.GatewayClient,
		namespace:           opts.Namespace,
		gatewayNamespace:    opts.GatewayNamespace,
		gatewayName:         opts.GatewayName,
		gpuVendor:           opts.GPUVendor,
		gpuModels:           opts.GPUModels,
		gpuModelAffinityKey: opts.GPUModelAffinityKey,
		gpuModelMapping:     opts.GPUModelMapping,
		capacityCalc:        opts.CapacityCalc,
		imageCache:          opts.ImageCache,
		imageRewriter:       opts.ImageRewriter,
		loadingServiceName:  opts.LoadingServiceName,
		loadingServicePort:  opts.LoadingServicePort,
		loadingTimeoutMs:    opts.LoadingTimeoutMs,
		baseDomain:          opts.BaseDomain,
		clusterConfigSecret: opts.ClusterConfigSecret,
		egressConfig:        opts.EgressConfig,
		httpClient:          noRedirectHTTPClient,
		userSuffix:          opts.UserSuffix,
		localStorageClass:   opts.LocalStorageClass,

		porklockImage:           opts.PorklockImage,
		porklockTag:             opts.PorklockTag,
		viceProxyImage:          opts.ViceProxyImage,
		useCSIDriver:            opts.UseCSIDriver,
		frontendBaseURL:         opts.FrontendBaseURL,
		irodsZone:               opts.IRODSZone,
		inputPathListIdentifier: opts.InputPathListIdentifier,
		gatewayProvider:         opts.GatewayProvider,
		imagePullSecretName:     opts.ImagePullSecretName,
		resourceDefaults:        opts.ResourceDefaults,
		caBundleConfigMap:       opts.CABundleConfigMap,
		caBundleKey:             opts.CABundleKey,
		disableSpecLaunch:       opts.DisableSpecLaunch,
	}, nil
}

// viceBuildConfig assembles the vicebuild.Config from the operator's cluster
// configuration. This is the single place the operator's scattered config
// fields map onto the builder's input, so the spec launch path and any future
// build callers stay consistent.
func (o *Operator) viceBuildConfig() vicebuild.Config {
	gwNamespace := o.gatewayNamespace
	if gwNamespace == "" {
		gwNamespace = o.namespace
	}
	var rewriter func(string) string
	if o.imageRewriter != nil {
		rewriter = func(ref string) string {
			if mirrored, ok := o.imageRewriter.RewriteImage(ref); ok {
				return mirrored
			}
			return ref
		}
	}
	return vicebuild.Config{
		PorklockImage:           o.porklockImage,
		PorklockTag:             o.porklockTag,
		ViceProxyImage:          o.viceProxyImage,
		UseCSIDriver:            o.useCSIDriver,
		IRODSZone:               o.irodsZone,
		LocalStorageClass:       o.localStorageClass,
		FrontendBaseURL:         o.frontendBaseURL,
		BaseDomain:              o.baseDomain,
		Namespace:               o.namespace,
		GatewayNamespace:        gwNamespace,
		GatewayName:             o.gatewayName,
		GatewayProvider:         o.gatewayProvider,
		ImagePullSecretName:     o.imagePullSecretName,
		ClusterConfigSecretName: o.clusterConfigSecret,
		UserSuffix:              o.userSuffix,
		InputPathListIdentifier: o.inputPathListIdentifier,
		GPUVendor:               string(o.gpuVendor),
		GPUModelAffinityKey:     o.gpuModelAffinityKey,
		GPUModelMapping:         o.gpuModelMapping,
		LoadingServiceName:      o.loadingServiceName,
		LoadingServicePort:      o.loadingServicePort,
		ImageRewriter:           rewriter,
		Resources:               o.resourceDefaults,
		CABundleConfigMap:       o.caBundleConfigMap,
		CABundleKey:             o.caBundleKey,
	}
}

// getAccessURL contacts the vice-proxy sidecar through its in-cluster Service
// and returns the full frontend URL. This requires the vice-proxy to be
// running and reachable within the same namespace.
func (o *Operator) getAccessURL(ctx context.Context, serviceName string) (string, error) {
	endpoint := fmt.Sprintf(
		"http://%s.%s.svc.cluster.local:%d/frontend-url",
		serviceName,
		o.namespace,
		constants.VICEProxyServicePort,
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", fmt.Errorf("failed to build request for %s: %w", endpoint, err)
	}

	resp, err := o.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to contact vice-proxy at %s: %w", endpoint, err)
	}
	defer common.CloseBody(resp)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("vice-proxy at %s returned status %d", endpoint, resp.StatusCode)
	}

	var result struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to decode vice-proxy response from %s: %w", endpoint, err)
	}

	return result.URL, nil
}

// HandleCapacity returns the current cluster capacity.
//
//	@Summary		Get cluster capacity
//	@Description	Returns the current cluster capacity including available slots,
//	@Description	allocatable CPU/memory, and current usage.
//	@Tags			capacity
//	@Produce		json
//	@Success		200	{object}	operatorclient.CapacityResponse
//	@Failure		500	{object}	common.ErrorResponse
//	@Router			/capacity [get]
func (o *Operator) HandleCapacity(c echo.Context) error {
	ctx := c.Request().Context()
	cap, err := o.capacityCalc.Calculate(ctx)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	cap.GPUVendor = string(o.gpuVendor)
	cap.SupportedGPUModels = o.gpuModels
	// Advertise spec support unless it's been switched off, in which case 0
	// tells the scheduler to send this operator a legacy bundle instead.
	if !o.disableSpecLaunch {
		cap.SpecVersion = operatorclient.CurrentVICESpecVersion
	}
	return c.JSON(http.StatusOK, cap)
}

// HandleLaunch receives an AnalysisBundle, transforms routing, and applies
// all resources to the local cluster.
//
//	@Summary		Launch a VICE analysis
//	@Description	Receives a pre-built AnalysisBundle, transforms routing for
//	@Description	this cluster, and applies all K8s resources. Returns 409 if at capacity.
//	@Tags			analyses
//	@Accept			json
//	@Produce		json
//	@Param			request	body		operatorclient.AnalysisBundle	true	"The analysis bundle to launch"
//	@Success		201		{object}	map[string]string
//	@Failure		400		{object}	common.ErrorResponse
//	@Failure		409		{object}	common.ErrorResponse
//	@Failure		500		{object}	common.ErrorResponse
//	@Router			/analyses [post]
func (o *Operator) HandleLaunch(c echo.Context) error {
	ctx := c.Request().Context()

	// Bind and validate first (cheap) before the capacity check (expensive
	// K8s API call) so malformed requests are rejected without wasted work.
	var bundle operatorclient.AnalysisBundle
	if err := c.Bind(&bundle); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	if err := bundle.Validate(); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	cap, err := o.capacityCalc.Calculate(ctx)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	if !cap.HasCapacity() {
		log.Infof("launch rejected: at capacity (analysis %s)", bundle.AnalysisID)
		return echo.NewHTTPError(http.StatusConflict, "operator at capacity")
	}

	log.Infof("launching analysis %s", bundle.AnalysisID)

	// Transform the HTTPRoute for the local cluster environment.
	if bundle.HTTPRoute != nil {
		TransformHostnames(bundle.HTTPRoute, o.baseDomain)

		gwNamespace := o.gatewayNamespace
		if gwNamespace == "" {
			gwNamespace = o.namespace
		}
		TransformGatewayNamespace(bundle.HTTPRoute, gwNamespace, o.gatewayName)

		TransformBackendToLoadingService(bundle.HTTPRoute, o.loadingServiceName, o.loadingServicePort)
	}

	// Ensure the permissions ConfigMap exists in the bundle (handles bundles
	// created before the permissions feature was added).
	EnsurePermissionsConfigMap(&bundle, o.userSuffix)

	// Inject per-analysis vice-proxy args and ensure the cluster config secret
	// is referenced as envFrom so vice-proxy gets cluster-level env vars.
	TransformViceProxyArgs(bundle.Deployment, string(bundle.AnalysisID), o.clusterConfigSecret)

	// Translate the bundle's canonical GPU-model node affinity into the
	// key and values this cluster's nodes label themselves with. Must run
	// BEFORE TransformGPUVendor, which on AMD clusters renames the
	// nvidia.com/gpu.product key out from under this transform's lookup.
	TransformGPUModels(bundle.Deployment, o.gpuModelAffinityKey, o.gpuModelMapping)

	// Rewrite GPU resource names to match the cluster's GPU vendor.
	TransformGPUVendor(bundle.Deployment, o.gpuVendor)

	// In manual-mirror mode, swap upstream image refs in the deployment's
	// containers for their mirrored counterparts. Other modes contribute
	// no rewriter; the bundle's images pass through unchanged.
	if o.imageRewriter != nil {
		TransformImageRefs(bundle.Deployment, o.imageRewriter)
	}

	if err := o.applyBundleAndEgress(ctx, &bundle); err != nil {
		// Log the full error server-side; return a generic message so cluster
		// internals don't leak in the response body (matches HandleLaunchSpec).
		log.Errorf("launch failed for analysis %s: %v", bundle.AnalysisID, err)
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to apply analysis resources; see operator logs")
	}

	log.Infof("launch succeeded for analysis %s", bundle.AnalysisID)
	return c.JSON(http.StatusCreated, map[string]string{"analysisID": string(bundle.AnalysisID)})
}

// applyBundleAndEgress applies every resource in the bundle and creates the
// per-analysis egress NetworkPolicy. Shared by the legacy object launch path
// and the VICESpec launch path, which differ only in how the bundle is
// produced. The egress policy is built operator-side because only the operator
// knows the cluster environment (blocked CIDRs, Keycloak IPs, internet-access
// setting); it keys on the deployment's analysis-id label, which
// deleteAnalysisResources also uses for cleanup.
func (o *Operator) applyBundleAndEgress(ctx context.Context, bundle *operatorclient.AnalysisBundle) error {
	if err := o.applyBundle(ctx, bundle); err != nil {
		return err
	}

	np := buildAnalysisEgressPolicy(string(bundle.AnalysisID), o.namespace, bundle.Deployment.Labels, o.egressConfig)
	if len(np.Spec.Egress) == 0 {
		log.Warnf("analysis %s egress policy has no allow rules; pods will have DNS-only egress", bundle.AnalysisID)
	}
	npClient := o.clientset.NetworkingV1().NetworkPolicies(o.namespace)
	if err := upsert(ctx, npClient, "NetworkPolicy", np.Name, np); err != nil {
		return fmt.Errorf("egress policy failed for analysis %s: %w", bundle.AnalysisID, err)
	}
	return nil
}

// HandleLaunchSpec receives a cluster-agnostic VICESpec, builds the concrete
// k8s objects for this cluster via vicebuild, and applies them. It is the
// operator-side construction path that replaces the legacy "receive pre-built
// objects + transform" flow; the two coexist during migration, selected by
// app-exposer per the operator's advertised SpecVersion.
//
//	@Summary		Launch a VICE analysis from a spec
//	@Description	Receives a cluster-agnostic VICESpec, builds the k8s objects for
//	@Description	this cluster, and applies them. Returns 409 if at capacity.
//	@Tags			analyses
//	@Accept			json
//	@Produce		json
//	@Param			request	body		operatorclient.VICESpec	true	"The analysis spec to launch"
//	@Success		201		{object}	map[string]string
//	@Failure		400		{object}	common.ErrorResponse
//	@Failure		409		{object}	common.ErrorResponse
//	@Failure		500		{object}	common.ErrorResponse
//	@Failure		503		{object}	common.ErrorResponse	"Spec launch disabled on this operator"
//	@Router			/analyses/spec [post]
func (o *Operator) HandleLaunchSpec(c echo.Context) error {
	ctx := c.Request().Context()

	// Defensive backstop: with spec launch disabled the operator advertises
	// SpecVersion 0, so the scheduler never routes a spec here. This guards
	// direct callers (admin tools, integration tests). A 503 is transient, so
	// such a caller treats it as retryable rather than a permanent failure.
	if o.disableSpecLaunch {
		return echo.NewHTTPError(http.StatusServiceUnavailable, "spec launch is disabled on this operator")
	}

	// Default MountDataStore true so callers that predate the field keep CSI
	// mount behaviour; an explicit false in the payload still takes effect.
	spec := operatorclient.VICESpec{MountDataStore: true}
	if err := c.Bind(&spec); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	if err := spec.Validate(); err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	// Reject specs newer than this operator can faithfully build. The scheduler
	// should already have skipped us via the advertised SpecVersion; this is the
	// defensive backstop, returning 400 so app-exposer fails fast rather than
	// the operator silently mis-building.
	if spec.SpecVersion > operatorclient.CurrentVICESpecVersion {
		return echo.NewHTTPError(http.StatusBadRequest,
			fmt.Sprintf("unsupported spec version %d; this operator supports up to %d (upgrade the operator to launch this analysis)",
				spec.SpecVersion, operatorclient.CurrentVICESpecVersion))
	}

	cap, err := o.capacityCalc.Calculate(ctx)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	if !cap.HasCapacity() {
		log.Infof("spec launch rejected: at capacity (analysis %s)", spec.AnalysisID)
		return echo.NewHTTPError(http.StatusConflict, "operator at capacity")
	}

	log.Infof("launching analysis %s from spec", spec.AnalysisID)

	cfg := o.viceBuildConfig()
	bundle, err := cfg.BuildBundle(&spec)
	if err != nil {
		// Log the full error (may contain iRODS paths and internal mount
		// layout) server-side; return a generic message so cluster internals
		// don't leak in the response body.
		log.Errorf("building objects for analysis %s failed: %v; this usually means a malformed spec (e.g. an unrecognized input type)", spec.AnalysisID, err)
		return echo.NewHTTPError(http.StatusBadRequest, "failed to build k8s objects from spec; see operator logs")
	}

	if err := o.applyBundleAndEgress(ctx, bundle); err != nil {
		// Log the full error (k8s resource/namespace detail) server-side; return
		// a generic message so cluster internals don't leak in the response body.
		log.Errorf("spec launch failed for analysis %s: %v", spec.AnalysisID, err)
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to apply analysis resources; see operator logs")
	}

	log.Infof("spec launch succeeded for analysis %s", spec.AnalysisID)
	return c.JSON(http.StatusCreated, map[string]string{"analysisID": string(spec.AnalysisID)})
}

// HandleExit deletes all K8s resources associated with an analysis by its
// analysis-id label.
//
//	@Summary		Exit (delete) a VICE analysis
//	@Description	Deletes all K8s resources associated with an analysis.
//	@Tags			analyses
//	@Param			analysis-id	path	string	true	"The analysis ID"
//	@Success		200
//	@Failure		400	{object}	common.ErrorResponse
//	@Failure		500	{object}	common.ErrorResponse
//	@Router			/analyses/{analysis-id} [delete]
func (o *Operator) HandleExit(c echo.Context) error {
	ctx := c.Request().Context()
	analysisID, err := requiredParam(c, constants.AnalysisIDLabel)
	if err != nil {
		return err
	}

	log.Infof("exiting analysis %s", analysisID)

	if err := o.deleteAnalysisResources(ctx, analysisID); err != nil {
		log.Errorf("exit failed for analysis %s: %v", analysisID, err)
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	log.Infof("exit complete for analysis %s", analysisID)
	return c.NoContent(http.StatusOK)
}

// HandleSwapRoute manually triggers the route swap for an analysis, pointing
// its HTTPRoute at the analysis Service regardless of readiness.
//
//	@Summary		Manually swap route to analysis service
//	@Description	Swaps the HTTPRoute backend from the loading page service to
//	@Description	the analysis Service. Idempotent.
//	@Tags			analyses
//	@Param			analysis-id	path	string	true	"The analysis ID"
//	@Success		200
//	@Failure		400	{object}	common.ErrorResponse
//	@Failure		500	{object}	common.ErrorResponse
//	@Router			/analyses/{analysis-id}/swap-route [post]
func (o *Operator) HandleSwapRoute(c echo.Context) error {
	ctx := c.Request().Context()
	analysisID, err := requiredParam(c, constants.AnalysisIDLabel)
	if err != nil {
		return err
	}

	log.Infof("manual route swap requested for analysis %s", analysisID)

	if err := o.SwapRoute(ctx, analysisID); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusOK)
}

// HandleListing lists interactive (VICE) resources in the operator's namespace,
// optionally filtered by label key-value pairs provided in the query string.
//
//	@Summary		List running VICE analyses
//	@Description	Returns interactive (VICE) resources in the operator's namespace
//	@Description	including deployments, pods, configmaps, services, and routes.
//	@Description	Query parameters are used as label filters.
//	@Tags			analyses
//	@Produce		json
//	@Success		200	{object}	reporting.ResourceInfo
//	@Failure		500	{object}	common.ErrorResponse
//	@Router			/analyses [get]
func (o *Operator) HandleListing(c echo.Context) error {
	ctx := c.Request().Context()
	filter := common.FilterMap(c.Request().URL.Query())

	log.Debugf("listing interactive resources with filter: %v", filter)

	// Build label selector starting with the mandatory app-type=interactive label.
	ls := labels.Set{constants.AppTypeLabel: string(constants.Interactive)}
	for k, v := range filter {
		ls[k] = v
	}
	opts := metav1.ListOptions{LabelSelector: ls.AsSelector().String()}

	result := reporting.NewResourceInfo()

	// Deployments
	deps, err := o.clientset.AppsV1().Deployments(o.namespace).List(ctx, opts)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	for _, d := range deps.Items {
		result.Deployments = append(result.Deployments, *reporting.DeploymentInfoFrom(&d))
	}

	// Pods
	pods, err := o.clientset.CoreV1().Pods(o.namespace).List(ctx, opts)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	for _, p := range pods.Items {
		result.Pods = append(result.Pods, *reporting.PodInfoFrom(&p))
	}

	// ConfigMaps
	cms, err := o.clientset.CoreV1().ConfigMaps(o.namespace).List(ctx, opts)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	for _, cm := range cms.Items {
		result.ConfigMaps = append(result.ConfigMaps, *reporting.ConfigMapInfoFrom(&cm))
	}

	// Services
	svcs, err := o.clientset.CoreV1().Services(o.namespace).List(ctx, opts)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	for _, svc := range svcs.Items {
		result.Services = append(result.Services, *reporting.ServiceInfoFrom(&svc))
	}

	// HTTPRoutes
	routes, err := o.gatewayClient.HTTPRoutes(o.namespace).List(ctx, opts)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	for _, route := range routes.Items {
		result.Routes = append(result.Routes, *reporting.RouteInfoFrom(&route))
	}

	return c.JSON(http.StatusOK, result)
}

// RegenerateResponse summarizes the results of a network policy regeneration.
type RegenerateResponse struct {
	Updated int      `json:"updated"`
	Errors  []string `json:"errors,omitempty"`
}

// HandleRegenerateNetworkPolicies rebuilds and upserts per-analysis egress
// NetworkPolicies for all running analyses using the operator's current
// configuration. This allows admins to roll out config changes (blocked CIDRs,
// Keycloak IPs, internet access setting, etc.) to already-running analyses
// without restarting them.
//
//	@Summary		Regenerate per-analysis network policies
//	@Description	Rebuilds egress NetworkPolicies for all running analyses to
//	@Description	match the operator's current configuration. Returns 207
//	@Description	Multi-Status when some analyses failed to regenerate; the
//	@Description	Errors field lists them.
//	@Tags			network-policies
//	@Produce		json
//	@Success		200	{object}	RegenerateResponse	"All regenerated successfully"
//	@Success		207	{object}	RegenerateResponse	"Partial success"
//	@Failure		500	{object}	common.ErrorResponse
//	@Router			/regenerate-network-policies [post]
func (o *Operator) HandleRegenerateNetworkPolicies(c echo.Context) error {
	ctx := c.Request().Context()
	log.Info("regenerating per-analysis network policies")

	// List all VICE deployments to discover running analyses and their labels.
	viceSelector := labels.Set{constants.AppTypeLabel: string(constants.Interactive)}.AsSelector().String()
	deps, err := o.clientset.AppsV1().Deployments(o.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: viceSelector,
	})
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	npClient := o.clientset.NetworkingV1().NetworkPolicies(o.namespace)
	var updated int
	var errs []string

	for _, dep := range deps.Items {
		analysisID := dep.Labels[constants.AnalysisIDLabel]
		if analysisID == "" {
			log.Warnf("deployment %s has no analysis-id label, skipping", dep.Name)
			continue
		}

		bundleLabels := dep.Labels
		np := buildAnalysisEgressPolicy(analysisID, o.namespace, bundleLabels, o.egressConfig)
		if err := upsert(ctx, npClient, "NetworkPolicy", np.Name, np); err != nil {
			log.Errorf("regenerating egress policy for analysis %s: %v", analysisID, err)
			errs = append(errs, fmt.Sprintf("analysis %s: %v", analysisID, err))
			continue
		}
		updated++
		log.Debugf("regenerated egress policy for analysis %s", analysisID)
	}

	log.Infof("network policy regeneration complete: %d updated, %d errors", updated, len(errs))
	// Use 207 Multi-Status when any regeneration failed so automation
	// that checks status codes notices partial failure. Matches the
	// pattern used by bulkImageOp.
	status := http.StatusOK
	if len(errs) > 0 {
		status = http.StatusMultiStatus
	}
	return c.JSON(status, RegenerateResponse{
		Updated: updated,
		Errors:  errs,
	})
}
