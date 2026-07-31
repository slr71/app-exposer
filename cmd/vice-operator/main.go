// Package main implements the vice-operator binary, a minimal K8s operator
// that receives pre-built resource bundles from app-exposer and applies them
// to the local cluster.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/url"
	"os"

	"github.com/cyverse-de/app-exposer/common"
	"github.com/cyverse-de/app-exposer/constants"
	"github.com/cyverse-de/app-exposer/operator"
	"github.com/cyverse-de/app-exposer/vicebuild"
	"github.com/cyverse-de/go-mod/viceauth"
	"github.com/sirupsen/logrus"
	resourcev1 "k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes"
)

var log = common.Log

func main() {
	var (
		kubeconfig            string
		namespace             string
		port                  int
		gpuVendorFlag         string
		maxAnalyses           int
		nodeLabelSelector     string
		logLevel              string
		apiAuth               bool
		apiAuthIssuerURL      string
		apiAuthClientID       string
		adminRole             string
		adminEntitlementsRaw  string
		viceBaseURL           string
		clusterConfigSecret   string
		imagePullSecret       string
		registryServer        string
		registryUsername      string
		registryPassword      string
		loadingPort           int
		loadingServiceName    string
		loadingServicePort    int
		loadingTimeoutMs      int64
		operatorPodSelector   string
		gatewayNamespace      string
		gatewayName           string
		gatewayClassName      string
		gatewayEntryPort      int
		gatewaySkipCreation   bool
		keycloakBaseURL       string
		keycloakRealm         string
		keycloakClientID      string
		keycloakClientSecret  string
		disableViceProxyAuth  bool
		swaggerClientID       string
		swaggerClientSecret   string
		swaggerCookieSecret   string
		publicURL             string
		stateHMACSecret       string
		apiSubdomain          string
		apiServiceName        string
		serviceCIDR           string
		blockedCIDRs          stringSliceFlag
		egressPodExceptions   stringSliceFlag
		egressHostExceptions  stringSliceFlag
		egressCIDRExceptions  stringSliceFlag
		ingressPodExceptions  stringSliceFlag
		gpuModels             stringSliceFlag
		gpuModelMapping       nameValueMapFlag
		gpuModelAffinityKey   string
		disableInternetAccess bool
		userSuffix            string
		localStorageClass     string
		caBundleConfigMap     string
		caBundleKey           string
		statusListenerURL     string
		statusLeaseNamespace  string
		statusLeaseName       string
		clusterName           string
		imageCacheMode        string
		imageCacheSchedule    string
		reposFile             string

		// VICESpec construction config (operator-side build path).
		porklockImage           string
		porklockTag             string
		viceProxyImage          string
		useCSIDriver            bool
		irodsZone               string
		inputPathListIdentifier string
		gatewayProvider         string

		// Resource default/clamp policy ("what this cluster grants"). Flag
		// names and defaults mirror app-exposer so a cluster behaves identically
		// on the spec path and the legacy object path.
		defaultCPURequest            string
		defaultCPULimit              string
		disableCPULimit              bool
		defaultMemRequest            string
		defaultMemLimit              string
		disableMemLimit              bool
		defaultStorageRequest        string
		viceProxyCPURequest          string
		viceProxyCPULimit            string
		disableViceProxyCPULimit     bool
		viceProxyMemRequest          string
		viceProxyMemLimit            string
		disableViceProxyMemLimit     bool
		viceProxyStorageRequest      string
		viceProxyStorageLimit        string
		disableViceProxyStorageLimit bool

		disableSpecLaunch bool
	)

	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig (empty for in-cluster)")
	flag.StringVar(&namespace, "namespace", "vice-apps", "Namespace for VICE resources")
	flag.IntVar(&port, "port", 60001, "Listen port")
	flag.StringVar(&gpuVendorFlag, "gpu-vendor", "nvidia", "GPU vendor: nvidia or amd")
	flag.Var(&gpuModels, "gpu-model", "Canonical (GFD-style) GPU model this cluster can deliver, e.g. NVIDIA-A10G (repeatable). Advertised to the scheduler so model-specific analyses route here. Empty means model-agnostic.")
	flag.Var(&gpuModelMapping, "gpu-model-mapping", "NAME:VALUE pair mapping a canonical GPU model name to this cluster's node-label value, e.g. NVIDIA-A10G:a10g on EKS (repeatable). Applied during launch to rewrite the deployment's GPU model affinity.")
	flag.StringVar(&gpuModelAffinityKey, "gpu-model-affinity-key", "", "Node-label key this cluster uses for GPU-model affinity (e.g. eks.amazonaws.com/instance-gpu-name on EKS). Empty leaves the bundle's canonical nvidia.com/gpu.product key untouched.")
	flag.IntVar(&maxAnalyses, "max-analyses", 50, "Max concurrent analyses (0 disables the limit for autoscaling clusters)")
	flag.StringVar(&nodeLabelSelector, "node-label-selector", "", "Filter schedulable nodes by label")
	flag.StringVar(&logLevel, "log-level", "info", "Log level")
	flag.BoolVar(&apiAuth, "api-auth", true, "Enable OIDC JWT Bearer auth for the API")
	flag.StringVar(&apiAuthIssuerURL, "api-auth-issuer-url", "", "OIDC issuer URL for API auth (e.g. https://keycloak.example.com/realms/cyverse)")
	flag.StringVar(&apiAuthClientID, "api-auth-client-id", "", "Expected client ID (azp claim) for API auth")
	flag.StringVar(&adminRole, "admin-role", "vice-operator", "Realm role that grants API access (or ADMIN_ROLE env var)")
	flag.StringVar(&adminEntitlementsRaw, "admin-entitlements", "", "Comma-separated list of entitlement-claim values that grant API access (or ADMIN_ENTITLEMENTS env var)")
	flag.StringVar(&viceBaseURL, "vice-base-url", "https://cyverse.run", "Base URL for VICE, stored in the cluster config secret")
	flag.StringVar(&clusterConfigSecret, "cluster-config-secret", "cluster-config-secret", "Name of the K8s Secret holding cluster config")
	flag.StringVar(&imagePullSecret, "image-pull-secret", "vice-image-pull-secret", "Name of the K8s image pull Secret")
	flag.StringVar(&registryServer, "registry-server", "", "Docker registry server (e.g. harbor.cyverse.org)")
	flag.StringVar(&registryUsername, "registry-username", "", "Docker registry username")
	flag.StringVar(&registryPassword, "registry-password", "", "Docker registry password (or REGISTRY_PASSWORD env var)")
	flag.IntVar(&loadingPort, "loading-port", 8080, "Listen port for loading page server")
	flag.StringVar(&loadingServiceName, "loading-service-name", "vice-operator-loading", "Name of the loading page service")
	flag.IntVar(&loadingServicePort, "loading-service-port", 80, "Port of the loading page service")
	flag.Int64Var(&loadingTimeoutMs, "loading-timeout-ms", 600000, "Loading page timeout in milliseconds")
	flag.StringVar(&operatorPodSelector, "operator-pod-selector", "", "Pod selector for vice-operator services (e.g. app=vice-operator-local); if set, ensures API and loading services exist at startup")
	flag.StringVar(&gatewayNamespace, "gateway-namespace", "", "Namespace of the Gateway resource (defaults to --namespace)")
	flag.StringVar(&gatewayName, "gateway-name", "vice", "Name of the Gateway resource")
	flag.StringVar(&gatewayClassName, "gateway-class-name", "traefik", "GatewayClass name for the Gateway resource")
	flag.IntVar(&gatewayEntryPort, "gateway-entrypoint-port", 8000, "Entrypoint port on the Gateway listener (must match the gateway controller's internal port)")
	flag.BoolVar(&gatewaySkipCreation, "gateway-skip-creation", false, "Skip creation of the Gateway resource (use when attaching to a pre-existing Gateway)")
	flag.StringVar(&keycloakBaseURL, "keycloak-base-url", "", "Keycloak base URL for vice-proxy auth")
	flag.StringVar(&keycloakRealm, "keycloak-realm", "", "Keycloak realm for vice-proxy auth")
	flag.StringVar(&keycloakClientID, "keycloak-client-id", "", "OIDC client ID for vice-proxy auth")
	flag.StringVar(&keycloakClientSecret, "keycloak-client-secret", "", "OIDC client secret for vice-proxy auth (or KEYCLOAK_CLIENT_SECRET env var)")
	flag.BoolVar(&disableViceProxyAuth, "disable-vice-proxy-auth", false, "Disable auth in vice-proxy")
	flag.StringVar(&swaggerClientID, "swagger-client-id", "", "OAuth2 client ID for the Swagger UI login flow (must support authorization code flow in Keycloak)")
	flag.StringVar(&swaggerClientSecret, "swagger-client-secret", "", "OAuth2 client secret for the Swagger UI login flow (or SWAGGER_CLIENT_SECRET env var)")
	flag.StringVar(&swaggerCookieSecret, "swagger-cookie-secret", "", "Secret for signing session cookies (random string; auto-generated if empty; or SWAGGER_COOKIE_SECRET env var)")
	flag.StringVar(&publicURL, "public-url", "", "Public base URL of this operator (e.g. https://vice-operator-qa.cyverse.org); combined with the vice-users callback path to form the static OAuth redirect_uri")
	flag.StringVar(&stateHMACSecret, "state-hmac-secret", "", "Shared HMAC secret for signing the vice-proxy OAuth state parameter (or STATE_HMAC_SECRET env var); must be stable across operator restarts")
	flag.StringVar(&apiSubdomain, "api-subdomain", "vice-api", "Subdomain prefix for the vice-operator API HTTPRoute; combined with --vice-base-url host to form the full hostname")
	flag.StringVar(&apiServiceName, "api-service-name", "vice-operator", "K8s Service name for the vice-operator API HTTPRoute backend")
	flag.StringVar(&serviceCIDR, "service-cidr", "", "Cluster service CIDR to block in egress (auto-detected from kubernetes API server if empty)")
	flag.Var(&blockedCIDRs, "blocked-cidr", "Additional CIDRs to block in egress (repeatable)")
	flag.Var(&egressPodExceptions, "egress-pod-exception", "Pod selector label (key=value) to allow egress to (repeatable)")
	flag.Var(&egressHostExceptions, "egress-host-exception", "Hostname or IP that analyses should be able to reach; resolved to IPs at startup (repeatable)")
	flag.Var(&egressCIDRExceptions, "egress-cidr-exception", "CIDR (e.g. 10.0.0.0/8) that analyses should be able to reach (repeatable)")
	flag.Var(&ingressPodExceptions, "ingress-pod-exception", "Cross-namespace ingress source as kubernetes.io/metadata.name=<ns>,pod-label=val (repeatable). The kubernetes.io/metadata.name pair selects the namespace; remaining pairs select pods.")
	flag.BoolVar(&disableInternetAccess, "disable-internet-access", false, "Block analysis pods from reaching the public internet; only DNS, explicit host/CIDR exceptions, and pod exceptions are allowed")
	flag.StringVar(&userSuffix, "user-suffix", constants.DefaultUserSuffix, "Domain suffix appended to usernames if not already present")
	flag.StringVar(&localStorageClass, "local-storage-class", "", "StorageClass for the per-analysis working-dir PVC (e.g. openebs-hostpath, gp3); empty means use the cluster's default storage class")
	flag.StringVar(&caBundleConfigMap, "ca-bundle-configmap", "", "ConfigMap in --namespace holding the CA that vice-proxy must trust to reach Keycloak; needed only where the DE's certificates chain to a private CA. Empty leaves the vice-proxy image's trust store alone.")
	flag.StringVar(&caBundleKey, "ca-bundle-key", constants.CABundleKey, "Key within --ca-bundle-configmap holding the PEM bundle")
	flag.StringVar(&statusListenerURL, "status-listener-url", "", "Base URL of job-status-listener (e.g. https://de.example.org/job); empty disables the push-based status informer and falls back to the reconciler's polling")
	flag.StringVar(&statusLeaseNamespace, "status-lease-namespace", "", "Namespace for the status-publisher coordination Lease (defaults to --namespace)")
	flag.StringVar(&statusLeaseName, "status-lease-name", "vice-operator-status-publisher", "Name of the coordination Lease used to elect the status-publisher leader")
	flag.StringVar(&clusterName, "cluster-name", "", "Cluster identifier sent as the Host field on status updates (defaults to the operator's hostname)")
	flag.StringVar(&imageCacheMode, "image-cache-mode", "daemonset", "Image cache implementation: 'daemonset' for one DaemonSet per image (node-local containerd warming), 'cron' for one CronJob per image (periodic pulls), or 'manual-mirror' for read-only mapping driven by --repos-file (rewrites bundle image refs at launch time)")
	flag.StringVar(&imageCacheSchedule, "image-cache-schedule", "0 2 * * *", "Cron schedule applied to every cached image when --image-cache-mode=cron (5-field standard cron expression in UTC)")
	flag.StringVar(&reposFile, "repos-file", "", "Path to a JSON object mapping upstream image refs to mirrored refs (required when --image-cache-mode=manual-mirror; rejected otherwise)")

	// VICESpec construction config (operator-side build path).
	flag.StringVar(&porklockImage, "porklock-image", "discoenv/vice-file-transfers", "Image for the porklock file-transfer/init containers")
	flag.StringVar(&porklockTag, "porklock-tag", "latest", "Tag for the porklock image")
	flag.StringVar(&viceProxyImage, "vice-proxy-image", "harbor.cyverse.org/de/vice-proxy:latest", "Image (with tag) for the vice-proxy sidecar")
	flag.BoolVar(&useCSIDriver, "use-csi-driver", false, "Use the iRODS CSI driver for data volumes instead of porklock file transfers")
	flag.StringVar(&irodsZone, "irods-zone", "cyverse", "iRODS zone name")
	flag.StringVar(&inputPathListIdentifier, "input-path-list-identifier", "# application/vnd.de.multi-input-path-list+csv; version=1", "First line of the porklock input-path-list file")
	flag.StringVar(&gatewayProvider, "gateway-provider", "traefik", "Gateway API provider for HTTPRoute construction (e.g. traefik)")

	// Resource default/clamp policy. Defaults mirror app-exposer's flags.
	flag.StringVar(&defaultCPURequest, "default-cpu-resource-request", "1000m", "Default CPU request for an analysis")
	flag.StringVar(&defaultCPULimit, "default-cpu-resource-limit", "2000m", "Default CPU limit for an analysis")
	flag.BoolVar(&disableCPULimit, "disable-cpu-resource-limit", false, "Disable the analysis CPU limit")
	flag.StringVar(&defaultMemRequest, "default-memory-resource-request", "2Gi", "Default memory request for an analysis")
	flag.StringVar(&defaultMemLimit, "default-memory-resource-limit", "8Gi", "Default memory limit for an analysis")
	flag.BoolVar(&disableMemLimit, "disable-memory-resource-limit", false, "Disable the analysis memory limit")
	flag.StringVar(&defaultStorageRequest, "default-storage-resource-request", "1Gi", "Default ephemeral storage request for an analysis")
	flag.StringVar(&viceProxyCPURequest, "vice-proxy-cpu-resource-request", "100m", "CPU request for the vice-proxy sidecar")
	flag.StringVar(&viceProxyCPULimit, "vice-proxy-cpu-resource-limit", "200m", "CPU limit for the vice-proxy sidecar")
	flag.BoolVar(&disableViceProxyCPULimit, "disable-vice-proxy-cpu-resource-limit", false, "Disable the vice-proxy CPU limit")
	flag.StringVar(&viceProxyMemRequest, "vice-proxy-memory-resource-request", "100Mi", "Memory request for the vice-proxy sidecar")
	flag.StringVar(&viceProxyMemLimit, "vice-proxy-memory-resource-limit", "200Mi", "Memory limit for the vice-proxy sidecar")
	flag.BoolVar(&disableViceProxyMemLimit, "disable-vice-proxy-memory-resource-limit", false, "Disable the vice-proxy memory limit")
	flag.StringVar(&viceProxyStorageRequest, "vice-proxy-storage-resource-request", "16Gi", "Ephemeral storage request for the vice-proxy sidecar")
	flag.StringVar(&viceProxyStorageLimit, "vice-proxy-storage-resource-limit", "100Gi", "Ephemeral storage limit for the vice-proxy sidecar")
	flag.BoolVar(&disableViceProxyStorageLimit, "disable-vice-proxy-storage-resource-limit", true, "Disable the vice-proxy storage limit")

	flag.BoolVar(&disableSpecLaunch, "disable-spec-launch", false, "Disable the operator-side VICESpec build path: advertise spec support off so app-exposer sends legacy bundles, and reject direct spec launches. Per-operator rollback lever during migration.")

	flag.Parse()

	// Allow secrets to come from environment variables when not set on the
	// command line. Avoids exposing them in process listings.
	envFallback := func(val *string, envKey string) {
		if *val == "" {
			*val = os.Getenv(envKey)
		}
	}
	envFallback(&registryPassword, "REGISTRY_PASSWORD")
	envFallback(&keycloakClientSecret, "KEYCLOAK_CLIENT_SECRET")
	envFallback(&swaggerClientSecret, "SWAGGER_CLIENT_SECRET")
	envFallback(&swaggerCookieSecret, "SWAGGER_COOKIE_SECRET")
	envFallback(&stateHMACSecret, "STATE_HMAC_SECRET")
	envFallback(&adminEntitlementsRaw, "ADMIN_ENTITLEMENTS")
	// --admin-role has a non-empty default, so the envFallback "is empty?"
	// trick can't distinguish "user passed default" from "user didn't pass
	// anything." Walk visited flags to detect explicit-set, then apply the
	// env override only when the user wasn't explicit. This makes the
	// explicit flag always win over the env var.
	adminRoleSet := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "admin-role" {
			adminRoleSet = true
		}
	})
	if !adminRoleSet {
		if v := os.Getenv("ADMIN_ROLE"); v != "" {
			adminRole = v
		}
	}
	adminEntitlements := parseAdminEntitlements(adminEntitlementsRaw)

	// Validate OIDC auth flags.
	if apiAuth && (apiAuthIssuerURL == "" || apiAuthClientID == "") {
		log.Fatal("--api-auth-issuer-url and --api-auth-client-id are required when --api-auth is enabled")
	}

	// Validate vice-base-url is a proper HTTP(S) URL.
	parsedURL, parseErr := url.Parse(viceBaseURL)
	if parseErr != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Host == "" {
		log.Fatalf("--vice-base-url must be a valid HTTP(S) URL, got %q", viceBaseURL)
	}

	// Extract the base domain from vice-base-url for hostname rewriting and
	// wildcard route creation (e.g. "https://my-own-domain.org" → "my-own-domain.org").
	// Use Hostname() to strip any port, since HTTPRoute hostnames should not include ports.
	baseDomain := parsedURL.Hostname()

	// Validate registry flags: all three required together, or none.
	registryFlagsSet := registryServer != "" || registryUsername != "" || registryPassword != ""
	if registryFlagsSet && (registryServer == "" || registryUsername == "" || registryPassword == "") {
		log.Fatal("--registry-server, --registry-username, and --registry-password must all be provided together")
	}

	// Configure log level.
	level, err := logrus.ParseLevel(logLevel)
	if err != nil {
		log.Fatalf("invalid log level %q: %v", logLevel, err)
	}
	logrus.SetLevel(level)

	clientset, gwClient, err := buildKubeClients(kubeconfig)
	if err != nil {
		log.Fatalf("%v", err)
	}

	// operatorCallbackURL is the single static redirect_uri vice-proxy sends to
	// Keycloak for the vice-users client. vice-proxy carries the real app URL
	// in a signed state blob; this operator relays the authorization code back
	// to it (see handleViceUsersCallback). Left empty when --public-url is
	// unset so vice-proxy's own startup validation can flag the gap.
	var operatorCallbackURL string
	if publicURL != "" {
		parsedPublicURL, parseErr := url.Parse(publicURL)
		if parseErr != nil {
			log.Fatalf("--public-url must be a valid URL, got %q: %v", publicURL, parseErr)
		}
		operatorCallbackURL = parsedPublicURL.JoinPath(viceUsersCallbackPath).String()
	}

	// Build the cluster config map from flags. All keys are always written so
	// that stale values from a previous run are overwritten. The secret update
	// replaces the entire data map — omitting a key here removes it from the
	// secret.
	clusterConfig := map[string]string{
		"VICE_BASE_URL":          viceBaseURL,
		"KEYCLOAK_BASE_URL":      keycloakBaseURL,
		"KEYCLOAK_REALM":         keycloakRealm,
		"KEYCLOAK_CLIENT_ID":     keycloakClientID,
		"KEYCLOAK_CLIENT_SECRET": keycloakClientSecret,
		// Static OAuth redirect_uri and the shared secret signing the state
		// parameter that round-trips through the operator's callback relay.
		"OPERATOR_CALLBACK_URL": operatorCallbackURL,
		"STATE_HMAC_SECRET":     stateHMACSecret,
		// Propagated to vice-proxy (via EnvFrom) so it can grant admins
		// access to running analyses regardless of the per-analysis
		// allowed-users list.
		"ADMIN_ENTITLEMENTS": adminEntitlementsRaw,
	}
	if disableViceProxyAuth {
		clusterConfig["DISABLE_AUTH"] = "true"
	} else {
		clusterConfig["DISABLE_AUTH"] = "false"

		// Warn early if auth is enabled but required settings are missing,
		// since vice-proxy pods will crash-loop with a fatal validation error.
		if keycloakBaseURL == "" || keycloakRealm == "" || keycloakClientID == "" || keycloakClientSecret == "" {
			log.Warn("auth is enabled (--disable-vice-proxy-auth not set) but one or more Keycloak flags are empty; vice-proxy pods will fail to start")
		}
		if publicURL == "" || stateHMACSecret == "" {
			log.Warn("auth is enabled but --public-url or --state-hmac-secret is empty; vice-proxy pods will fail to start without both")
		}
	}

	// Ensure cluster config secret so vice-proxy containers can reference it
	// as env vars via EnvFrom.
	mustEnsure("cluster config secret", func(ctx context.Context) error {
		return operator.EnsureClusterConfigSecret(ctx, clientset, namespace, clusterConfigSecret, clusterConfig)
	})

	// Ensure the image pull secret so pods can pull from private registries.
	if registryServer != "" {
		mustEnsure("image pull secret", func(ctx context.Context) error {
			return operator.EnsureImagePullSecret(ctx, clientset, namespace, imagePullSecret, registryServer, registryUsername, registryPassword)
		})
	}

	// Ensure the operator's K8s Services and API HTTPRoute exist so traffic
	// can reach the operator through the Gateway.
	if operatorPodSelector != "" {
		selector, selectorErr := parseSelector(operatorPodSelector)
		if selectorErr != nil {
			log.Fatalf("invalid --operator-pod-selector: %v", selectorErr)
		}

		// Loading page service: exposes loadingServicePort, routes to the
		// container's loadingPort (the port the loading server actually binds).
		mustEnsure("loading service", func(ctx context.Context) error {
			return operator.EnsureService(ctx, clientset, namespace, loadingServiceName, int32(loadingServicePort), int32(loadingPort), selector)
		})

		// API service (port → port, same as the operator's listen port).
		mustEnsure("API service", func(ctx context.Context) error {
			return operator.EnsureService(ctx, clientset, namespace, apiServiceName, int32(port), int32(port), selector)
		})

		// API HTTPRoute: makes the vice-operator API accessible through
		// the Gateway (e.g. for HAProxy / tailscale serve).
		apiHostname := fmt.Sprintf("%s.%s", apiSubdomain, parsedURL.Hostname())
		mustEnsure("API HTTPRoute", func(ctx context.Context) error {
			gwNS := gatewayNamespace
			if gwNS == "" {
				gwNS = namespace
			}
			return operator.EnsureAPIRoute(ctx, gwClient, namespace, gwNS, gatewayName, apiHostname, apiServiceName, int32(port))
		})
	}

	// Ensure the Gateway resource exists so HTTPRoutes can attach to it.
	// We skip this if --gateway-skip-creation is set, or if --gateway-namespace
	// points to a different namespace than the operator.
	shouldEnsureGateway := !gatewaySkipCreation && (gatewayNamespace == "" || gatewayNamespace == namespace)
	if shouldEnsureGateway {
		mustEnsure("gateway", func(ctx context.Context) error {
			return operator.EnsureGateway(ctx, gwClient, namespace, gatewayName, gatewayClassName, int32(gatewayEntryPort))
		})
	} else {
		log.Infof("skipping Gateway creation (skip-flag=%v, namespace=%s, gateway-namespace=%s)",
			gatewaySkipCreation, namespace, gatewayNamespace)
	}

	// Ensure the wildcard default HTTPRoute so subdomain requests arriving
	// before the analysis-specific HTTPRoute is created get a waiting page
	// instead of nothing. Only applies when the operator manages its own
	// Gateway (external clusters); in the local cluster where
	// --gateway-skip-creation is set, vice-default-backend handles this.
	if shouldEnsureGateway && operatorPodSelector != "" {
		mustEnsure("default wildcard HTTPRoute", func(ctx context.Context) error {
			gwNS := gatewayNamespace
			if gwNS == "" {
				gwNS = namespace
			}
			return operator.EnsureDefaultRoute(ctx, gwClient, namespace, gwNS, gatewayName, baseDomain, loadingServiceName, int32(loadingServicePort))
		})
	}

	// Ensure the CORS middleware exists so HTTPRoutes can reference it.
	mustEnsure("CORS middleware", func(ctx context.Context) error {
		return operator.EnsureCORSMiddleware(ctx, clientset, namespace)
	})

	// Build the egress / network-policy configuration from flags. Auto-
	// detects service CIDR, parses pod and ingress exceptions, resolves
	// Keycloak / host-exception CIDRs.
	egressConfig, err := buildEgressConfig(context.Background(), clientset, egressInputs{
		namespace:             namespace,
		serviceCIDR:           serviceCIDR,
		blockedCIDRs:          blockedCIDRs,
		egressPodExceptions:   egressPodExceptions,
		egressHostExceptions:  egressHostExceptions,
		egressCIDRExceptions:  egressCIDRExceptions,
		ingressPodExceptions:  ingressPodExceptions,
		disableInternetAccess: disableInternetAccess,
		operatorPodSelector:   operatorPodSelector,
		keycloakBaseURL:       keycloakBaseURL,
		disableViceProxyAuth:  disableViceProxyAuth,
	})
	if err != nil {
		log.Fatalf("%v", err)
	}

	mustEnsure("network policies", func(ctx context.Context) error {
		return operator.EnsureNamespacePolicies(ctx, clientset, egressConfig)
	})

	gpuVendor, err := operator.ParseGPUVendor(gpuVendorFlag)
	if err != nil {
		log.Fatalf("invalid GPU vendor: %v", err)
	}

	capacityCalc, err := operator.NewCapacityCalculator(clientset, namespace, maxAnalyses, nodeLabelSelector)
	if err != nil {
		log.Fatalf("creating capacity calculator: %v", err)
	}
	imageCache, imageRewriter, err := buildImageCache(imageCacheMode, imageCacheSchedule, reposFile, clientset, namespace, imagePullSecret)
	if err != nil {
		log.Fatalf("%v", err)
	}
	parseQty := func(name, s string) resourcev1.Quantity {
		q, err := resourcev1.ParseQuantity(s)
		if err != nil {
			log.Fatalf("invalid quantity for %s (%q): %v", name, s, err)
		}
		return q
	}
	resourceDefaults := vicebuild.ResourceDefaults{
		DefaultCPURequest:   parseQty("--default-cpu-resource-request", defaultCPURequest),
		DefaultCPULimit:     parseQty("--default-cpu-resource-limit", defaultCPULimit),
		DefaultMemRequest:   parseQty("--default-memory-resource-request", defaultMemRequest),
		DefaultMemLimit:     parseQty("--default-memory-resource-limit", defaultMemLimit),
		DefaultStorage:      parseQty("--default-storage-resource-request", defaultStorageRequest),
		DoCPULimit:          !disableCPULimit,
		DoMemLimit:          !disableMemLimit,
		ViceProxyCPURequest: parseQty("--vice-proxy-cpu-resource-request", viceProxyCPURequest),
		ViceProxyCPULimit:   parseQty("--vice-proxy-cpu-resource-limit", viceProxyCPULimit),
		ViceProxyMemRequest: parseQty("--vice-proxy-memory-resource-request", viceProxyMemRequest),
		ViceProxyMemLimit:   parseQty("--vice-proxy-memory-resource-limit", viceProxyMemLimit),
		ViceProxyStorage:    parseQty("--vice-proxy-storage-resource-request", viceProxyStorageRequest),
		ViceProxyStorageLim: parseQty("--vice-proxy-storage-resource-limit", viceProxyStorageLimit),
		DoViceProxyCPULimit: !disableViceProxyCPULimit,
		DoViceProxyMemLimit: !disableViceProxyMemLimit,
		DoViceProxyStorage:  !disableViceProxyStorageLimit,
	}

	op, err := operator.NewOperator(operator.OperatorOptions{
		Clientset:           clientset,
		GatewayClient:       gwClient,
		Namespace:           namespace,
		GatewayNamespace:    gatewayNamespace,
		GatewayName:         gatewayName,
		GPUVendor:           gpuVendor,
		GPUModels:           gpuModels,
		GPUModelAffinityKey: gpuModelAffinityKey,
		GPUModelMapping:     gpuModelMapping,
		CapacityCalc:        capacityCalc,
		ImageCache:          imageCache,
		ImageRewriter:       imageRewriter,
		LoadingServiceName:  loadingServiceName,
		LoadingServicePort:  int32(loadingServicePort),
		LoadingTimeoutMs:    loadingTimeoutMs,
		BaseDomain:          baseDomain,
		ClusterConfigSecret: clusterConfigSecret,
		EgressConfig:        egressConfig,
		UserSuffix:          userSuffix,
		LocalStorageClass:   localStorageClass,
		PorklockImage:       porklockImage,
		PorklockTag:         porklockTag,
		ViceProxyImage:      viceProxyImage,
		UseCSIDriver:        useCSIDriver,
		// REDIRECT_URL for each analysis is built from the vice base URL — the
		// same value whose hostname is the routing domain (baseDomain), since a
		// redirect must land on the host its HTTPRoute serves. No separate
		// frontend-base-url flag is needed.
		FrontendBaseURL:         viceBaseURL,
		IRODSZone:               irodsZone,
		InputPathListIdentifier: inputPathListIdentifier,
		GatewayProvider:         gatewayProvider,
		ImagePullSecretName:     imagePullSecret,
		ResourceDefaults:        resourceDefaults,
		CABundleConfigMap:       caBundleConfigMap,
		CABundleKey:             caBundleKey,
		DisableSpecLaunch:       disableSpecLaunch,
	})
	if err != nil {
		log.Fatalf("failed to construct operator: %v", err)
	}

	// One OIDC discovery roundtrip populates both the API token verifier and
	// the Swagger UI OAuth2 config so all endpoint URLs stay in sync with the
	// issuer's published metadata.
	provider, err := buildOIDCProvider(context.Background(), apiAuthIssuerURL, apiAuth)
	if err != nil {
		log.Fatalf("%v", err)
	}
	verifier := buildAPIVerifier(provider, apiAuthIssuerURL, apiAuthClientID)

	swaggerCfg, err := buildSwaggerAuthConfig(provider, swaggerClientID, swaggerClientSecret, swaggerCookieSecret)
	if err != nil {
		log.Fatalf("%v", err)
	}

	// The vice-users OAuth callback relay is enabled only when a state HMAC
	// secret is configured; baseDomain bounds where the relay may redirect.
	var viceUsersCfg *ViceUsersAuthConfig
	if stateHMACSecret != "" {
		viceUsersCfg = &ViceUsersAuthConfig{
			StateCodec: viceauth.NewCodec([]byte(stateHMACSecret)),
			BaseDomain: baseDomain,
		}
	}

	app := NewApp(AppConfig{
		Operator:          op,
		Verifier:          verifier,
		ExpectedClientID:  apiAuthClientID,
		SwaggerCfg:        swaggerCfg,
		AdminRole:         adminRole,
		AdminEntitlements: adminEntitlements,
		ViceUsersCfg:      viceUsersCfg,
	})
	loadingApp := NewLoadingApp(op)

	apiAddr := fmt.Sprintf(":%d", port)
	loadingAddr := fmt.Sprintf(":%d", loadingPort)

	log.Infof("vice-operator listening on %s (loading page on %s, namespace=%s, gpu-vendor=%s, vice-base-url=%s, max-analyses=%d)",
		apiAddr, loadingAddr, namespace, gpuVendorFlag, viceBaseURL, maxAnalyses)

	// Start loading page server in a goroutine.
	go func() {
		log.Infof("loading page server starting on %s", loadingAddr)
		if err := loadingApp.Start(loadingAddr); err != nil {
			log.Fatalf("loading page server failed: %v", err)
		}
	}()

	// Start the leader-elected status informer in a goroutine when the
	// listener URL is configured. When the URL is empty (e.g. local dev or a
	// deployment that wants to keep relying on the reconciler's polling), the
	// informer is skipped entirely.
	if statusListenerURL != "" {
		startStatusInformer(clientset, namespace, statusListenerURL, statusLeaseNamespace, statusLeaseName, clusterName)
	} else {
		log.Info("--status-listener-url is empty; status informer disabled (reconciler polling will remain the sole status source)")
	}

	// API server blocks on the main goroutine.
	if err := app.Start(apiAddr); err != nil {
		log.Error(err)
		os.Exit(1)
	}
}

// startStatusInformer constructs the publisher + informer and launches the
// leader-election loop in a background goroutine. Validation failures are
// fatal — they indicate a misconfigured deployment that wouldn't recover by
// retrying.
func startStatusInformer(clientset kubernetes.Interface, namespace, listenerURL, leaseNamespace, leaseName, clusterName string) {
	identity, err := os.Hostname()
	if err != nil || identity == "" {
		log.Fatalf("status informer requires a hostname for the lease identity: %v", err)
	}
	host := clusterName
	if host == "" {
		host = identity
	}

	publisher, err := operator.NewStatusPublisher(listenerURL, host)
	if err != nil {
		log.Fatalf("status informer: %v", err)
	}

	if leaseNamespace == "" {
		leaseNamespace = namespace
	}

	informer, err := operator.NewStatusInformer(operator.StatusInformerConfig{
		Clientset:      clientset,
		Publisher:      publisher,
		Namespace:      namespace,
		LeaseNamespace: leaseNamespace,
		LeaseName:      leaseName,
		Identity:       identity,
	})
	if err != nil {
		log.Fatalf("status informer: %v", err)
	}

	log.Infof("status informer starting (listener=%s, lease=%s/%s, identity=%s, host=%s)",
		listenerURL, leaseNamespace, leaseName, identity, host)
	go func() {
		if err := informer.Run(context.Background()); err != nil && !errors.Is(err, context.Canceled) {
			log.Errorf("status informer exited: %v", err)
		}
	}()
}
