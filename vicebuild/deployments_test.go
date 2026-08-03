package vicebuild

import (
	"path"
	"strings"
	"testing"

	"github.com/cyverse-de/app-exposer/constants"
	"github.com/cyverse-de/app-exposer/operatorclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiv1 "k8s.io/api/core/v1"
)

// findContainer returns the named container from a slice, failing the test if
// it is absent.
func findContainer(t *testing.T, containers []apiv1.Container, name string) apiv1.Container {
	t.Helper()
	for _, c := range containers {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("container %q not found", name)
	return apiv1.Container{}
}

func gpuSpec(vendor string, count int64, models ...string) *operatorclient.GPUSpec {
	return &operatorclient.GPUSpec{Vendor: vendor, Count: count, Models: models}
}

// TestDeploymentGPUVendorFolded confirms TransformGPUVendor is folded into
// construction: the analysis container carries the cluster vendor's GPU
// resource name with requests == limits, and no foreign vendor name leaks in.
func TestDeploymentGPUVendorFolded(t *testing.T) {
	tests := []struct {
		name      string
		vendor    string
		wantName  apiv1.ResourceName
		otherName apiv1.ResourceName
	}{
		{name: "nvidia", vendor: operatorclient.GPUVendorNvidia, wantName: "nvidia.com/gpu", otherName: "amd.com/gpu"},
		{name: "amd", vendor: operatorclient.GPUVendorAMD, wantName: "amd.com/gpu", otherName: "nvidia.com/gpu"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.GPUVendor = tt.vendor
			spec := testSpec()
			spec.GPU = gpuSpec(tt.vendor, 2)

			dep := cfg.Deployment(spec)
			analysis := findContainer(t, dep.Spec.Template.Spec.Containers, constants.AnalysisContainerName)

			req := analysis.Resources.Requests[tt.wantName]
			lim := analysis.Resources.Limits[tt.wantName]
			assert.Equal(t, int64(2), req.Value(), "GPU request count")
			assert.True(t, req.Equal(lim), "GPU requests must equal limits for extended resources")

			_, hasReq := analysis.Resources.Requests[tt.otherName]
			_, hasLim := analysis.Resources.Limits[tt.otherName]
			assert.False(t, hasReq || hasLim, "foreign GPU vendor name %q must not appear", tt.otherName)
		})
	}
}

// TestDeploymentGPUModelAffinityFolded confirms TransformGPUModels is folded in:
// the model-affinity key and values reflect the cluster's node-label scheme.
func TestDeploymentGPUModelAffinityFolded(t *testing.T) {
	tests := []struct {
		name       string
		vendor     string
		key        string
		mapping    map[string]string
		models     []string
		wantKey    string
		wantValues []string
		wantNoTerm bool
	}{
		{
			name:       "nvidia default key identity",
			vendor:     operatorclient.GPUVendorNvidia,
			models:     []string{"NVIDIA-A10G"},
			wantKey:    constants.GPUModelAffinityKey,
			wantValues: []string{"NVIDIA-A10G"},
		},
		{
			name:       "eks key + value mapping",
			vendor:     operatorclient.GPUVendorNvidia,
			key:        "eks.amazonaws.com/instance-gpu-name",
			mapping:    map[string]string{"NVIDIA-A10G": "a10g"},
			models:     []string{"NVIDIA-A10G"},
			wantKey:    "eks.amazonaws.com/instance-gpu-name",
			wantValues: []string{"a10g"},
		},
		{
			name:    "amd renames canonical key",
			vendor:  operatorclient.GPUVendorAMD,
			models:  []string{"some-amd-model"},
			wantKey: amdModelAffinityKey,
			// no mapping → identity passthrough of values
			wantValues: []string{"some-amd-model"},
		},
		{
			name:       "unmapped values drop the model term",
			vendor:     operatorclient.GPUVendorNvidia,
			key:        "eks.amazonaws.com/instance-gpu-name",
			mapping:    map[string]string{"NVIDIA-L4": "l4"},
			models:     []string{"NVIDIA-A10G"},
			wantNoTerm: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.GPUVendor = tt.vendor
			cfg.GPUModelAffinityKey = tt.key
			cfg.GPUModelMapping = tt.mapping
			spec := testSpec()
			spec.GPU = gpuSpec(tt.vendor, 1, tt.models...)

			dep := cfg.Deployment(spec)
			terms := dep.Spec.Template.Spec.Affinity.NodeAffinity.
				RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions

			var modelExpr *apiv1.NodeSelectorRequirement
			for i := range terms {
				if terms[i].Key == tt.wantKey {
					modelExpr = &terms[i]
				}
				// the canonical "gpu=true" device selector must always be present
			}
			assert.Contains(t, keysOf(terms), constants.GPUAffinityKey, "base gpu selector present")

			if tt.wantNoTerm {
				for _, term := range terms {
					assert.NotEqual(t, tt.key, term.Key, "model term should be dropped when all values unmapped")
				}
				return
			}
			require.NotNil(t, modelExpr, "expected model affinity term with key %q", tt.wantKey)
			assert.Equal(t, tt.wantValues, modelExpr.Values)
		})
	}
}

func keysOf(reqs []apiv1.NodeSelectorRequirement) []string {
	out := make([]string, 0, len(reqs))
	for _, r := range reqs {
		out = append(out, r.Key)
	}
	return out
}

// TestDeploymentNoGPU confirms a non-GPU analysis gets no GPU resources,
// tolerations, or affinity terms.
func TestDeploymentNoGPU(t *testing.T) {
	dep := testConfig().Deployment(testSpec())
	analysis := findContainer(t, dep.Spec.Template.Spec.Containers, constants.AnalysisContainerName)
	for name := range analysis.Resources.Requests {
		assert.NotContains(t, string(name), "gpu")
	}
	assert.NotContains(t, keysOf(dep.Spec.Template.Spec.Affinity.NodeAffinity.
		RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions),
		constants.GPUAffinityKey)
}

// TestViceProxyArgsFolded confirms TransformViceProxyArgs is folded in: the
// vice-proxy container carries per-analysis args, the cluster-config envFrom,
// and the permissions mount, all at construction time.
func TestViceProxyArgsFolded(t *testing.T) {
	cfg := testConfig()
	cfg.ClusterConfigSecretName = "cluster-config"
	spec := testSpec()
	spec.Container.Ports = []int{8888}

	dep := cfg.Deployment(spec)
	proxy := findContainer(t, dep.Spec.Template.Spec.Containers, constants.VICEProxyContainerName)

	assert.Equal(t, []string{"vice-proxy"}, proxy.Command)
	assert.Contains(t, proxy.Args, "--analysis-id")
	assert.Contains(t, proxy.Args, string(spec.AnalysisID))
	assert.Contains(t, proxy.Args, "http://localhost:8888")

	require.Len(t, proxy.EnvFrom, 1)
	assert.Equal(t, "cluster-config", proxy.EnvFrom[0].SecretRef.Name)

	var hasPermsMount bool
	for _, vm := range proxy.VolumeMounts {
		if vm.Name == constants.PermissionsVolumeName {
			hasPermsMount = true
		}
	}
	assert.True(t, hasPermsMount, "permissions volume mount present")
}

// TestViceProxyCABundle confirms the CA bundle reaches vice-proxy as both a
// volume and SSL_CERT_FILE only when configured, so clusters with
// publicly-trusted certificates keep an unchanged pod spec.
func TestViceProxyCABundle(t *testing.T) {
	for _, tt := range []struct {
		name      string
		configMap string
		key       string
		wantEnv   string // empty means no volume, no mount, and no env var
	}{
		{name: "unset", configMap: "", wantEnv: ""},
		{name: "default key", configMap: "local-ca", wantEnv: "/etc/de-ca/ca.crt"},
		{name: "explicit key", configMap: "local-ca", key: "bundle.pem", wantEnv: "/etc/de-ca/bundle.pem"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testConfig()
			cfg.CABundleConfigMap = tt.configMap
			cfg.CABundleKey = tt.key

			dep := cfg.Deployment(testSpec())
			proxy := findContainer(t, dep.Spec.Template.Spec.Containers, constants.VICEProxyContainerName)

			var gotEnv string
			for _, e := range proxy.Env {
				if e.Name == "SSL_CERT_FILE" {
					gotEnv = e.Value
				}
			}
			assert.Equal(t, tt.wantEnv, gotEnv)

			var mounted, volumed bool
			for _, vm := range proxy.VolumeMounts {
				if vm.Name == constants.CABundleVolumeName {
					mounted = true
					assert.True(t, vm.ReadOnly)
				}
			}
			for _, v := range dep.Spec.Template.Spec.Volumes {
				if v.Name == constants.CABundleVolumeName {
					volumed = true
					require.NotNil(t, v.ConfigMap)
					assert.Equal(t, tt.configMap, v.ConfigMap.Name)
					// Projecting only the configured key is what turns a
					// mismatched --ca-bundle-key into a pod that fails to start.
					key := path.Base(tt.wantEnv)
					assert.Equal(t, []apiv1.KeyToPath{{Key: key, Path: key}}, v.ConfigMap.Items)
				}
			}
			assert.Equal(t, tt.wantEnv != "", mounted, "CA volume mount presence")
			assert.Equal(t, tt.wantEnv != "", volumed, "CA volume presence")
		})
	}
}

// TestImageRefsFolded confirms TransformImageRefs is folded in: a configured
// rewriter is applied to every container image.
func TestImageRefsFolded(t *testing.T) {
	cfg := testConfig()
	cfg.PorklockImage = "porklock"
	cfg.PorklockTag = "latest"
	cfg.ViceProxyImage = "vice-proxy:latest"
	cfg.ImageRewriter = func(ref string) string { return "mirror.example.com/" + ref }
	spec := testSpec()

	dep := cfg.Deployment(spec)
	for _, c := range append(dep.Spec.Template.Spec.InitContainers, dep.Spec.Template.Spec.Containers...) {
		assert.Truef(t, strings.HasPrefix(c.Image, "mirror.example.com/"), "image %q for container %q not rewritten", c.Image, c.Name)
	}
}

// TestDeploymentCSIvsNonCSI confirms the init container and sidecar set switches
// on the cluster's CSI capability and the per-job MountDataStore flag.
func TestDeploymentCSIvsNonCSI(t *testing.T) {
	t.Run("CSI enabled", func(t *testing.T) {
		cfg := testConfig()
		cfg.UseCSIDriver = true
		dep := cfg.Deployment(testSpec())
		require.Len(t, dep.Spec.Template.Spec.InitContainers, 1)
		assert.Equal(t, constants.WorkingDirInitContainerName, dep.Spec.Template.Spec.InitContainers[0].Name)
		// No file-transfers sidecar under CSI.
		for _, c := range dep.Spec.Template.Spec.Containers {
			assert.NotEqual(t, constants.FileTransfersContainerName, c.Name)
		}
	})
	t.Run("CSI disabled", func(t *testing.T) {
		cfg := testConfig()
		cfg.UseCSIDriver = false
		dep := cfg.Deployment(testSpec())
		require.Len(t, dep.Spec.Template.Spec.InitContainers, 1)
		assert.Equal(t, constants.FileTransfersInitContainerName, dep.Spec.Template.Spec.InitContainers[0].Name)
		findContainer(t, dep.Spec.Template.Spec.Containers, constants.FileTransfersContainerName)
	})
	t.Run("CSI enabled, mount opted out", func(t *testing.T) {
		// MountDataStore=false on a CSI cluster should fall back to porklock,
		// same as if the CSI driver were disabled.
		cfg := testConfig()
		cfg.UseCSIDriver = true
		spec := testSpec()
		spec.MountDataStore = false
		dep := cfg.Deployment(spec)
		require.Len(t, dep.Spec.Template.Spec.InitContainers, 1)
		assert.Equal(t, constants.FileTransfersInitContainerName, dep.Spec.Template.Spec.InitContainers[0].Name,
			"porklock init container used when mount opted out")
		findContainer(t, dep.Spec.Template.Spec.Containers, constants.FileTransfersContainerName)
	})
}
