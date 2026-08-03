// Package vicebuild constructs the concrete Kubernetes objects for a VICE
// analysis from a cluster-agnostic operatorclient.VICESpec plus a cluster
// Config. It is the operator-side counterpart to app-exposer's incluster
// builders: where incluster builds cluster-agnostic objects that the operator
// then rewrites via its transform layer, vicebuild folds those transforms into
// construction and emits cluster-correct objects directly.
//
// vicebuild carries no DB, apps, or model.Job dependency — every analysis value
// arrives resolved in the VICESpec, and every cluster value arrives in Config.
// That is what lets the operator import it.
package vicebuild

import (
	"github.com/cyverse-de/app-exposer/common"
	"github.com/cyverse-de/app-exposer/constants"
	"k8s.io/apimachinery/pkg/api/resource"
)

var log = common.Log

// Config holds the cluster-specific values the builders need — the operator's
// equivalent of the old incluster.Init fields plus the values its transform
// layer used to inject. The operator populates this once from its own
// configuration; nothing here is analysis-specific.
type Config struct {
	// Container images.
	PorklockImage  string
	PorklockTag    string
	ViceProxyImage string

	// Storage and iRODS.
	UseCSIDriver bool
	IRODSZone    string
	// LocalStorageClass is applied to the per-analysis working-dir PVC. Empty
	// means "use the cluster default" (the old TransformWorkingDirStorageClass).
	LocalStorageClass string

	// Frontend URL and routing domain.
	FrontendBaseURL string
	// BaseDomain is the cluster's hostname domain (e.g. "cyverse.run",
	// "localhost"); the HTTPRoute hostname is "<subdomain>.<BaseDomain>". This
	// is what TransformHostnames used to rewrite in.
	BaseDomain string

	// Namespaces and gateway. Namespace is where the analysis resources live;
	// GatewayNamespace/GatewayName identify the Gateway the HTTPRoute attaches
	// to (the old TransformGatewayNamespace). GatewayProvider selects any
	// provider-specific HTTPRoute decoration (e.g. "traefik").
	Namespace        string
	GatewayNamespace string
	GatewayName      string
	GatewayProvider  string

	// Secrets.
	ImagePullSecretName     string
	ClusterConfigSecretName string

	// CABundleConfigMap names a ConfigMap in Namespace holding the CA that
	// vice-proxy must trust to reach Keycloak — needed only where the DE's
	// certificates chain to a private CA. Empty adds no volume and no mount,
	// leaving clusters with publicly-trusted certificates unaffected.
	// CABundleKey is the key within it; empty means constants.CABundleKey.
	CABundleConfigMap string
	CABundleKey       string

	// UserSuffix is appended to the submitter username where Keycloak expects
	// the fully-qualified form (permissions ConfigMap, CSI proxy user).
	UserSuffix string

	// InputPathListIdentifier is the first line of the porklock input-path-list
	// file; it identifies the list format to the transfer tool.
	InputPathListIdentifier string

	// GPU mapping. GPUVendor is the cluster's vendor ("nvidia"/"amd"); the
	// builders emit that vendor's resource name and affinity keys directly
	// (the old TransformGPUVendor). GPUModelAffinityKey and GPUModelMapping
	// translate canonical GFD model names onto the cluster's node-label scheme
	// (the old TransformGPUModels); a zero key + empty mapping is the identity.
	GPUVendor           string
	GPUModelAffinityKey string
	GPUModelMapping     map[string]string

	// Loading-page service the HTTPRoute backend points at until the analysis
	// is ready (the old TransformBackendToLoadingService).
	LoadingServiceName string
	LoadingServicePort int32

	// ImageRewriter, when non-nil, rewrites container image refs to their
	// mirrored counterparts (the old TransformImageRefs, manual-mirror mode).
	// Nil is the identity.
	ImageRewriter func(string) string

	// Resources holds the default/clamp policy for analysis and vice-proxy
	// resource requirements — "what this cluster grants." These were
	// app-exposer's resourcing package-level defaults; per the design they are
	// cluster policy and live operator-side.
	Resources ResourceDefaults
}

// ResourceDefaults is the cluster's resource default/clamp policy. The analysis
// asks ride raw in the VICESpec; these fill in unset asks and bound the result.
// The Do*Limit toggles mirror the old resourcing flags: when false, that limit
// is omitted entirely (no ceiling imposed).
type ResourceDefaults struct {
	DefaultCPURequest resource.Quantity
	DefaultCPULimit   resource.Quantity
	DefaultMemRequest resource.Quantity
	DefaultMemLimit   resource.Quantity
	DefaultStorage    resource.Quantity
	DoCPULimit        bool
	DoMemLimit        bool

	ViceProxyCPURequest resource.Quantity
	ViceProxyCPULimit   resource.Quantity
	ViceProxyMemRequest resource.Quantity
	ViceProxyMemLimit   resource.Quantity
	ViceProxyStorage    resource.Quantity
	ViceProxyStorageLim resource.Quantity
	DoViceProxyCPULimit bool
	DoViceProxyMemLimit bool
	DoViceProxyStorage  bool
}

// CABundleKeyOrDefault returns key, or the conventional ca.crt when empty. It
// is the single rule for resolving --ca-bundle-key, shared by the pod builder
// here and the operator's startup validation.
func CABundleKeyOrDefault(key string) string {
	if key == "" {
		return constants.CABundleKey
	}
	return key
}

func (c *Config) caBundleKey() string {
	return CABundleKeyOrDefault(c.CABundleKey)
}

// rewriteImage applies the configured image rewriter, or returns ref unchanged
// when no rewriter is configured.
func (c *Config) rewriteImage(ref string) string {
	if c.ImageRewriter == nil {
		return ref
	}
	return c.ImageRewriter(ref)
}
