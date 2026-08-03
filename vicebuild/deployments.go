package vicebuild

import (
	"fmt"
	"net/url"
	"path"

	"github.com/cyverse-de/app-exposer/common"
	"github.com/cyverse-de/app-exposer/constants"
	"github.com/cyverse-de/app-exposer/operatorclient"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// amdModelAffinityKey is the AMD node-label key the old TransformGPUVendor
// renamed the canonical GPU-model key to on AMD clusters.
const amdModelAffinityKey = "amd.com/gpu.product"

func analysisPorts(spec *operatorclient.VICESpec) []apiv1.ContainerPort {
	ports := make([]apiv1.ContainerPort, 0, len(spec.Container.Ports))
	for i, p := range spec.Container.Ports {
		ports = append(ports, apiv1.ContainerPort{
			ContainerPort: int32(p),
			Name:          fmt.Sprintf("tcp-a-%d", i),
			Protocol:      apiv1.ProtocolTCP,
		})
	}
	return ports
}

func uidPtr(spec *operatorclient.VICESpec) *int64 {
	return constants.Int64Ptr(int64(spec.Container.UID))
}

// inputStagingContainer is the porklock init container that stages inputs on
// non-CSI clusters.
func (c *Config) inputStagingContainer(spec *operatorclient.VICESpec) apiv1.Container {
	return apiv1.Container{
		Name:            constants.FileTransfersInitContainerName,
		Image:           c.rewriteImage(fmt.Sprintf("%s:%s", c.PorklockImage, c.PorklockTag)),
		Command:         append(fileTransferCommand(spec), "--no-service"),
		ImagePullPolicy: apiv1.PullAlways,
		WorkingDir:      constants.InputPathListMountPath,
		VolumeMounts:    c.fileTransfersVolumeMounts(spec),
		Ports: []apiv1.ContainerPort{
			{Name: constants.FileTransfersPortName, ContainerPort: constants.FileTransfersPort, Protocol: apiv1.ProtocolTCP},
		},
		SecurityContext: &apiv1.SecurityContext{
			RunAsUser:  uidPtr(spec),
			RunAsGroup: uidPtr(spec),
			Capabilities: &apiv1.Capabilities{
				Drop: []apiv1.Capability{"SETPCAP", "AUDIT_WRITE", "KILL", "SETGID", "SETUID", "NET_BIND_SERVICE", "SYS_CHROOT", "SETFCAP", "FSETID", "MKNOD"},
				Add:  []apiv1.Capability{"NET_ADMIN", "NET_RAW"},
			},
		},
	}
}

// workingDirPrepContainer initializes the working-dir volume on CSI clusters,
// reusing the porklock image for its shell.
func (c *Config) workingDirPrepContainer(spec *operatorclient.VICESpec) apiv1.Container {
	return apiv1.Container{
		Name:            constants.WorkingDirInitContainerName,
		Image:           c.rewriteImage(fmt.Sprintf("%s:%s", c.PorklockImage, c.PorklockTag)),
		Command:         []string{"init_working_dir.sh", constants.CSIDriverLocalMountPath, c.zoneMountPath()},
		ImagePullPolicy: apiv1.PullAlways,
		WorkingDir:      constants.WorkingDirInitContainerMountPath,
		VolumeMounts: []apiv1.VolumeMount{
			{Name: workingDirVolumeName(spec), MountPath: constants.WorkingDirInitContainerMountPath, ReadOnly: false},
		},
		SecurityContext: &apiv1.SecurityContext{
			RunAsUser:  uidPtr(spec),
			RunAsGroup: uidPtr(spec),
			Capabilities: &apiv1.Capabilities{
				Drop: []apiv1.Capability{"SETPCAP", "AUDIT_WRITE", "KILL", "SETGID", "SETUID", "NET_BIND_SERVICE", "SYS_CHROOT", "SETFCAP", "FSETID", "NET_RAW", "MKNOD"},
			},
		},
	}
}

func (c *Config) initContainers(spec *operatorclient.VICESpec) []apiv1.Container {
	if c.UseCSIDriver && spec.MountDataStore {
		return []apiv1.Container{c.workingDirPrepContainer(spec)}
	}
	return []apiv1.Container{c.inputStagingContainer(spec)}
}

// analysisContainer builds the interactive analysis container.
func (c *Config) analysisContainer(spec *operatorclient.VICESpec) apiv1.Container {
	env := make([]apiv1.EnvVar, 0, len(spec.Environment)+3)
	for k, v := range spec.Environment {
		env = append(env, apiv1.EnvVar{Name: k, Value: v})
	}

	// Build the per-analysis frontend URL: <subdomain>.<FrontendBaseURL host>.
	frontURL, err := url.Parse(c.FrontendBaseURL)
	if err != nil {
		log.Warnf("failed to parse FrontendBaseURL %q: %v", c.FrontendBaseURL, err)
		frontURL = &url.URL{}
	}
	frontURL.Host = fmt.Sprintf("%s.%s", common.Subdomain(spec.UserID, string(spec.ExternalID)), frontURL.Host)

	env = append(env,
		apiv1.EnvVar{Name: "REDIRECT_URL", Value: frontURL.String()},
		apiv1.EnvVar{Name: "IPLANT_USER", Value: spec.Submitter},
		apiv1.EnvVar{Name: "IPLANT_EXECUTION_ID", Value: string(spec.ExternalID)},
	)

	resources := c.analysisRequirements(spec)
	container := apiv1.Container{
		Name:            constants.AnalysisContainerName,
		Image:           c.rewriteImage(fmt.Sprintf("%s:%s", spec.Container.Image, spec.Container.Tag)),
		ImagePullPolicy: apiv1.PullAlways,
		Env:             env,
		Resources:       resources,
		VolumeMounts:    c.podVolumeMounts(spec),
		Ports:           analysisPorts(spec),
		SecurityContext: &apiv1.SecurityContext{RunAsUser: uidPtr(spec), RunAsGroup: uidPtr(spec)},
		ReadinessProbe: &apiv1.Probe{
			InitialDelaySeconds: 0,
			TimeoutSeconds:      30,
			SuccessThreshold:    1,
			FailureThreshold:    10,
			PeriodSeconds:       31,
			ProbeHandler: apiv1.ProbeHandler{
				HTTPGet: &apiv1.HTTPGetAction{
					Port:   intstr.FromInt32(int32(spec.Container.Ports[0])),
					Scheme: apiv1.URISchemeHTTP,
					Path:   "/",
				},
			},
		},
	}
	if spec.Container.EntryPoint != "" {
		container.Command = []string{spec.Container.EntryPoint}
	}
	if spec.Container.WorkingDir != "" {
		container.WorkingDir = spec.Container.WorkingDir
	}
	if len(spec.Container.Arguments) != 0 {
		container.Args = append(container.Args, spec.Container.Arguments...)
	}
	return container
}

// viceProxyContainer builds the vice-proxy sidecar with per-analysis args, the
// cluster-config-secret envFrom, the permissions mount, and any configured CA
// bundle — folding in the old TransformViceProxyArgs.
func (c *Config) viceProxyContainer(spec *operatorclient.VICESpec) apiv1.Container {
	backendURL := "http://localhost:60000"
	if len(spec.Container.Ports) > 0 {
		backendURL = fmt.Sprintf("http://localhost:%d", spec.Container.Ports[0])
	} else {
		log.Warnf("analysis %s has no ports; vice-proxy backend defaulting to %s", spec.AnalysisID, backendURL)
	}

	container := apiv1.Container{
		Name:    constants.VICEProxyContainerName,
		Image:   c.rewriteImage(c.ViceProxyImage),
		Command: []string{"vice-proxy"},
		Args: []string{
			"--analysis-id", string(spec.AnalysisID),
			"--backend-url", backendURL,
			"--ws-backend-url", backendURL,
			"--listen-addr", fmt.Sprintf("0.0.0.0:%d", constants.VICEProxyPort),
		},
		ImagePullPolicy: apiv1.PullAlways,
		Ports: []apiv1.ContainerPort{
			{Name: constants.VICEProxyPortName, ContainerPort: constants.VICEProxyPort, Protocol: apiv1.ProtocolTCP},
		},
		VolumeMounts: []apiv1.VolumeMount{
			{Name: constants.PermissionsVolumeName, MountPath: constants.PermissionsMountPath, ReadOnly: true},
		},
		SecurityContext: &apiv1.SecurityContext{
			RunAsUser:  uidPtr(spec),
			RunAsGroup: uidPtr(spec),
			Capabilities: &apiv1.Capabilities{
				Drop: []apiv1.Capability{"SETPCAP", "AUDIT_WRITE", "KILL", "SETGID", "SETUID", "SYS_CHROOT", "SETFCAP", "FSETID", "NET_RAW", "MKNOD"},
			},
		},
		Resources: c.viceProxyRequirements(),
		ReadinessProbe: &apiv1.Probe{
			ProbeHandler: apiv1.ProbeHandler{
				HTTPGet: &apiv1.HTTPGetAction{
					Port:   intstr.FromInt32(constants.VICEProxyPort),
					Scheme: apiv1.URISchemeHTTP,
					Path:   "/url-ready",
				},
			},
		},
	}

	if c.ClusterConfigSecretName != "" {
		optional := true
		container.EnvFrom = []apiv1.EnvFromSource{
			{SecretRef: &apiv1.SecretEnvSource{
				LocalObjectReference: apiv1.LocalObjectReference{Name: c.ClusterConfigSecretName},
				Optional:             &optional,
			}},
		}
	}

	// SSL_CERT_FILE is enough on its own: vice-proxy is a cgo-free Go binary
	// that never sets its own RootCAs, so crypto/x509 picks this up for the
	// back-channel token exchange with Keycloak. It *replaces* the image's
	// default bundle file rather than adding to it — public roots survive only
	// because crypto/x509 also scans /etc/ssl/certs as a directory, where the
	// vice-proxy image happens to leave ca-certificates.crt. Publish a full
	// bundle if that ever stops being true.
	if c.CABundleConfigMap != "" {
		container.VolumeMounts = append(container.VolumeMounts, apiv1.VolumeMount{
			Name:      constants.CABundleVolumeName,
			MountPath: constants.CABundleMountPath,
			ReadOnly:  true,
		})
		container.Env = append(container.Env, apiv1.EnvVar{
			Name:  "SSL_CERT_FILE",
			Value: path.Join(constants.CABundleMountPath, c.caBundleKey()),
		})
	}
	return container
}

func (c *Config) fileTransfersContainer(spec *operatorclient.VICESpec) apiv1.Container {
	return apiv1.Container{
		Name:            constants.FileTransfersContainerName,
		Image:           c.rewriteImage(fmt.Sprintf("%s:%s", c.PorklockImage, c.PorklockTag)),
		Command:         fileTransferCommand(spec),
		ImagePullPolicy: apiv1.PullAlways,
		WorkingDir:      constants.InputPathListMountPath,
		VolumeMounts:    c.fileTransfersVolumeMounts(spec),
		Ports: []apiv1.ContainerPort{
			{Name: constants.FileTransfersPortName, ContainerPort: constants.FileTransfersPort, Protocol: apiv1.ProtocolTCP},
		},
		SecurityContext: &apiv1.SecurityContext{
			RunAsUser:  uidPtr(spec),
			RunAsGroup: uidPtr(spec),
			Capabilities: &apiv1.Capabilities{
				Drop: []apiv1.Capability{"SETPCAP", "AUDIT_WRITE", "KILL", "SETGID", "SETUID", "NET_BIND_SERVICE", "SYS_CHROOT", "SETFCAP", "FSETID", "NET_RAW", "MKNOD"},
			},
		},
		ReadinessProbe: &apiv1.Probe{
			ProbeHandler: apiv1.ProbeHandler{
				HTTPGet: &apiv1.HTTPGetAction{
					Port:   intstr.FromInt32(constants.FileTransfersPort),
					Scheme: apiv1.URISchemeHTTP,
					Path:   "/",
				},
			},
		},
	}
}

// containers returns the pod's containers: vice-proxy, the file-transfers
// sidecar (when not using CSI mounts), then the analysis container.
func (c *Config) containers(spec *operatorclient.VICESpec) []apiv1.Container {
	out := []apiv1.Container{c.viceProxyContainer(spec)}
	if !c.UseCSIDriver || !spec.MountDataStore {
		out = append(out, c.fileTransfersContainer(spec))
	}
	out = append(out, c.analysisContainer(spec))
	return out
}

func (c *Config) imagePullSecrets() []apiv1.LocalObjectReference {
	if c.ImagePullSecretName != "" {
		return []apiv1.LocalObjectReference{{Name: c.ImagePullSecretName}}
	}
	return []apiv1.LocalObjectReference{}
}

// resolvedGPUModelAffinityKey returns the node-label key for GPU-model affinity
// on this cluster, folding in TransformGPUModels (key override) and
// TransformGPUVendor (AMD rename). The canonical key is used unless the cluster
// overrides it via Config.GPUModelAffinityKey; an AMD cluster that hasn't
// overridden the key gets the AMD rename. Distinct from the raw
// Config.GPUModelAffinityKey field, which may be empty.
func (c *Config) resolvedGPUModelAffinityKey() string {
	if c.GPUModelAffinityKey != "" && c.GPUModelAffinityKey != constants.GPUModelAffinityKey {
		return c.GPUModelAffinityKey
	}
	if c.GPUVendor == operatorclient.GPUVendorAMD {
		return amdModelAffinityKey
	}
	return constants.GPUModelAffinityKey
}

// gpuModelValues maps canonical GPU model names onto this cluster's node-label
// values, folding in TransformGPUModels. An empty mapping is the identity;
// values absent from a non-empty mapping are dropped.
func (c *Config) gpuModelValues(models []string) []string {
	if len(c.GPUModelMapping) == 0 {
		return models
	}
	out := make([]string, 0, len(models))
	for _, m := range models {
		if mapped, ok := c.GPUModelMapping[m]; ok {
			out = append(out, mapped)
		}
	}
	return out
}

// nodeSelectorRequirements builds the required node-affinity terms: the base
// analysis selector, plus GPU device and GPU-model selectors when a GPU is
// requested.
func (c *Config) nodeSelectorRequirements(spec *operatorclient.VICESpec) []apiv1.NodeSelectorRequirement {
	reqs := []apiv1.NodeSelectorRequirement{
		{Key: constants.AnalysisAffinityKey, Operator: apiv1.NodeSelectorOperator(constants.AnalysisAffinityOperator)},
	}
	if spec.GPU == nil {
		return reqs
	}
	reqs = append(reqs, apiv1.NodeSelectorRequirement{
		Key:      constants.GPUAffinityKey,
		Operator: apiv1.NodeSelectorOperator(constants.GPUAffinityOperator),
		Values:   []string{constants.GPUAffinityValue},
	})
	if values := c.gpuModelValues(spec.GPU.Models); len(values) > 0 {
		reqs = append(reqs, apiv1.NodeSelectorRequirement{
			Key:      c.resolvedGPUModelAffinityKey(),
			Operator: apiv1.NodeSelectorOperator(constants.GPUModelAffinityOperator),
			Values:   values,
		})
	}
	return reqs
}

func (c *Config) tolerations(spec *operatorclient.VICESpec) []apiv1.Toleration {
	tolerations := []apiv1.Toleration{
		{Key: "analysis", Operator: apiv1.TolerationOpExists},
	}
	if spec.GPU != nil {
		tolerations = append(tolerations, apiv1.Toleration{
			Key:      constants.GPUTolerationKey,
			Operator: apiv1.TolerationOperator(constants.GPUTolerationOperator),
			Value:    constants.GPUTolerationValue,
			Effect:   apiv1.TaintEffect(constants.GPUTolerationEffect),
		})
	}
	return tolerations
}

// Deployment builds the analysis Deployment with all cluster-specific values
// (GPU vendor/model, images, resources) baked in.
func (c *Config) Deployment(spec *operatorclient.VICESpec) *appsv1.Deployment {
	labels := BuildLabels(spec)
	autoMount := false

	preferredSchedTerms := []apiv1.PreferredSchedulingTerm{
		{
			Weight: 1,
			Preference: apiv1.NodeSelectorTerm{
				MatchExpressions: []apiv1.NodeSelectorRequirement{
					{Key: "vice", Operator: apiv1.NodeSelectorOpExists},
				},
			},
		},
	}

	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:   deploymentName(spec),
			Labels: labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: constants.Int32Ptr(1),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{constants.ExternalIDLabel: string(spec.ExternalID)},
			},
			Template: apiv1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: apiv1.PodSpec{
					Hostname:                     common.Subdomain(spec.UserID, string(spec.ExternalID)),
					RestartPolicy:                apiv1.RestartPolicyAlways,
					Volumes:                      c.podVolumes(spec),
					InitContainers:               c.initContainers(spec),
					Containers:                   c.containers(spec),
					ImagePullSecrets:             c.imagePullSecrets(),
					AutomountServiceAccountToken: &autoMount,
					DNSConfig: &apiv1.PodDNSConfig{
						Options: []apiv1.PodDNSConfigOption{
							{Name: "ndots", Value: constants.StringPtr("1")},
						},
					},
					SecurityContext: &apiv1.PodSecurityContext{
						RunAsUser:  uidPtr(spec),
						RunAsGroup: uidPtr(spec),
						FSGroup:    uidPtr(spec),
					},
					Tolerations: c.tolerations(spec),
					Affinity: &apiv1.Affinity{
						PodAntiAffinity: &apiv1.PodAntiAffinity{
							PreferredDuringSchedulingIgnoredDuringExecution: []apiv1.WeightedPodAffinityTerm{
								{
									Weight: 100,
									PodAffinityTerm: apiv1.PodAffinityTerm{
										LabelSelector: &metav1.LabelSelector{
											MatchLabels: map[string]string{constants.AppTypeLabel: string(constants.Interactive)},
										},
										TopologyKey: "kubernetes.io/hostname",
									},
								},
							},
						},
						NodeAffinity: &apiv1.NodeAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
								NodeSelectorTerms: []apiv1.NodeSelectorTerm{
									{MatchExpressions: c.nodeSelectorRequirements(spec)},
								},
							},
							PreferredDuringSchedulingIgnoredDuringExecution: preferredSchedTerms,
						},
					},
				},
			},
		},
	}
}
