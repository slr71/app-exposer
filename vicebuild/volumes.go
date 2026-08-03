package vicebuild

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/cyverse-de/app-exposer/constants"
	"github.com/cyverse-de/app-exposer/operatorclient"
	apiv1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// defaultStorageCapacity is the floor/placeholder capacity for the working-dir
// PVC and the CSI data volume, matching the incluster default.
var defaultStorageCapacity = resourcev1.MustParse("5Gi")

// IRODSFSPathMapping is one mount entry for the iRODS CSI driver. Mirrors
// incluster.IRODSFSPathMapping; redefined here so vicebuild stays independent
// of the legacy package.
type IRODSFSPathMapping struct {
	IRODSPath           string `json:"irods_path"`
	MappingPath         string `json:"mapping_path"`
	ResourceType        string `json:"resource_type"` // file or dir
	ReadOnly            bool   `json:"read_only"`
	CreateDir           bool   `json:"create_dir"`
	IgnoreNotExistError bool   `json:"ignore_not_exist_error"`
}

func (c *Config) zoneMountPath() string {
	return fmt.Sprintf("%s/%s", constants.CSIDriverLocalMountPath, c.IRODSZone)
}

// inputPathMappings builds the CSI mount entries for the analysis's inputs.
// Uses the full input list (not just the ticketless subset): CSI mounts every
// input regardless of ticket status.
func inputPathMappings(spec *operatorclient.VICESpec) ([]IRODSFSPathMapping, error) {
	mappings := []IRODSFSPathMapping{}
	occupied := map[string]string{} // mount path -> irods path

	for _, input := range spec.Inputs {
		if input.IRODSPath == "" {
			continue
		}
		var resourceType string
		switch strings.ToLower(input.Type) {
		case "fileinput", "multifileselector":
			resourceType = "file"
		case "folderinput":
			resourceType = "dir"
		default:
			return nil, fmt.Errorf("unknown step input type - %s", input.Type)
		}

		mountPath := fmt.Sprintf("%s/%s", constants.CSIDriverInputVolumeMountPath, filepath.Base(input.IRODSPath))
		if existing, ok := occupied[mountPath]; ok {
			return nil, fmt.Errorf("tried to mount an input file %s at %s already used by - %s", input.IRODSPath, mountPath, existing)
		}
		occupied[mountPath] = input.IRODSPath

		mappings = append(mappings, IRODSFSPathMapping{
			IRODSPath:           input.IRODSPath,
			MappingPath:         mountPath,
			ResourceType:        resourceType,
			ReadOnly:            true,
			CreateDir:           false,
			IgnoreNotExistError: true,
		})
	}
	return mappings, nil
}

func (c *Config) sharedPathMapping() IRODSFSPathMapping {
	sharedHomeFullPath := fmt.Sprintf("/%s/home/shared", c.IRODSZone)
	return IRODSFSPathMapping{
		IRODSPath:           sharedHomeFullPath,
		MappingPath:         sharedHomeFullPath,
		ResourceType:        "dir",
		IgnoreNotExistError: true,
	}
}

// persistentVolumes builds the CSI data PersistentVolume when the cluster
// supports CSI and the analysis requests data-store mounts. Returns nil
// otherwise — porklock handles data transfer on the non-CSI path.
func (c *Config) persistentVolumes(spec *operatorclient.VICESpec) ([]*apiv1.PersistentVolume, error) {
	if !c.UseCSIDriver || !spec.MountDataStore {
		return nil, nil
	}

	mappings := []IRODSFSPathMapping{}
	inputMappings, err := inputPathMappings(spec)
	if err != nil {
		return nil, err
	}
	mappings = append(mappings, inputMappings...)
	mappings = append(mappings, IRODSFSPathMapping{
		IRODSPath:           spec.OutputDirectory,
		MappingPath:         constants.CSIDriverOutputVolumeMountPath,
		ResourceType:        "dir",
		ReadOnly:            false,
		CreateDir:           true,
		IgnoreNotExistError: true,
	})
	if spec.UserHome != "" {
		mappings = append(mappings, IRODSFSPathMapping{
			IRODSPath:    spec.UserHome,
			MappingPath:  spec.UserHome,
			ResourceType: "dir",
		})
	}
	mappings = append(mappings, c.sharedPathMapping())

	mappingsJSON, err := json.Marshal(mappings)
	if err != nil {
		return nil, err
	}

	labels := BuildLabels(spec)
	labels["volume-name"] = csiDataVolumeName(spec)

	volmode := apiv1.PersistentVolumeFilesystem
	uid := fmt.Sprintf("%d", spec.Container.UID)
	pv := &apiv1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name:   csiDataVolumeName(spec),
			Labels: labels,
		},
		Spec: apiv1.PersistentVolumeSpec{
			Capacity:                      apiv1.ResourceList{apiv1.ResourceStorage: defaultStorageCapacity},
			VolumeMode:                    &volmode,
			AccessModes:                   []apiv1.PersistentVolumeAccessMode{apiv1.ReadWriteMany},
			PersistentVolumeReclaimPolicy: apiv1.PersistentVolumeReclaimRetain,
			StorageClassName:              constants.CSIDriverStorageClassName,
			PersistentVolumeSource: apiv1.PersistentVolumeSource{
				CSI: &apiv1.CSIPersistentVolumeSource{
					Driver:       constants.CSIDriverName,
					VolumeHandle: csiDataVolumeHandle(spec),
					VolumeAttributes: map[string]string{
						"client":              "irodsfuse",
						"path_mapping_json":   string(mappingsJSON),
						"no_permission_check": "true",
						// iRODS proxy access expects the bare username without the
						// domain suffix (e.g. "someuser", not the @-qualified form).
						"clientUser": strings.SplitN(spec.Submitter, "@", 2)[0],
						"uid":        uid,
						"gid":        uid,
					},
				},
			},
		},
	}
	return []*apiv1.PersistentVolume{pv}, nil
}

// persistentVolumeCapacity returns the working-dir PVC capacity: the analysis's
// disk ask, floored at the default.
func persistentVolumeCapacity(spec *operatorclient.VICESpec) resourcev1.Quantity {
	capacity := max(defaultStorageCapacity.Value(), spec.Resources.MinDiskBytes)
	return *resourcev1.NewQuantity(capacity, resourcev1.BinarySI)
}

// volumeClaims builds the working-dir PVC (with the cluster's storage class
// folded in — the old TransformWorkingDirStorageClass) and, when CSI is
// enabled, the iRODS data-volume claim.
func (c *Config) volumeClaims(spec *operatorclient.VICESpec) []*apiv1.PersistentVolumeClaim {
	labels := BuildLabels(spec)

	workingDir := &apiv1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:   workingDirVolumeName(spec),
			Labels: labels,
		},
		Spec: apiv1.PersistentVolumeClaimSpec{
			AccessModes: []apiv1.PersistentVolumeAccessMode{apiv1.ReadWriteOnce},
			Resources: apiv1.VolumeResourceRequirements{
				Requests: apiv1.ResourceList{apiv1.ResourceStorage: persistentVolumeCapacity(spec)},
			},
		},
	}
	// Fold in TransformWorkingDirStorageClass: empty means "cluster default".
	if c.LocalStorageClass != "" {
		sc := c.LocalStorageClass
		workingDir.Spec.StorageClassName = &sc
	}
	claims := []*apiv1.PersistentVolumeClaim{workingDir}

	if c.UseCSIDriver && spec.MountDataStore {
		storageClass := constants.CSIDriverStorageClassName
		claims = append(claims, &apiv1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:   csiDataVolumeClaimName(spec),
				Labels: labels,
			},
			Spec: apiv1.PersistentVolumeClaimSpec{
				AccessModes:      []apiv1.PersistentVolumeAccessMode{apiv1.ReadWriteMany},
				StorageClassName: &storageClass,
				VolumeName:       csiDataVolumeName(spec),
				Resources: apiv1.VolumeResourceRequirements{
					Requests: apiv1.ResourceList{apiv1.ResourceStorage: defaultStorageCapacity},
				},
			},
		})
	}
	return claims
}

// defaultWorkingDir is the working-dir volume mount path used when the job
// submission did not specify a container working directory. Mirrors
// model.Container.WorkingDirectory()'s default; the spec carries the raw
// (possibly empty) WorkingDir so the analysis container's WorkingDir field is
// only set when the job actually specified one (otherwise the image's own
// WORKDIR applies), while the volume still mounts at a known path.
const defaultWorkingDir = "/de-app-work"

// workingDirMountPath is the in-container path the working-dir volume mounts at.
// It resolves the default when the spec carries no explicit working directory.
func workingDirMountPath(spec *operatorclient.VICESpec) string {
	if spec.Container.WorkingDir == "" {
		return defaultWorkingDir
	}
	return spec.Container.WorkingDir
}

// hasTicketlessInputs reports whether the input-path-list volume/mount is
// needed (porklock staging only runs for ticketless inputs).
func hasTicketlessInputs(spec *operatorclient.VICESpec) bool {
	return len(spec.InputPathListPaths) > 0
}

// podVolumeMounts returns the analysis container's volume mounts for the
// persistent and CSI data volumes plus shared memory.
func (c *Config) podVolumeMounts(spec *operatorclient.VICESpec) []apiv1.VolumeMount {
	mounts := []apiv1.VolumeMount{
		{
			Name:      workingDirVolumeName(spec),
			MountPath: workingDirMountPath(spec),
			ReadOnly:  false,
		},
	}
	if c.UseCSIDriver && spec.MountDataStore {
		mounts = append(mounts, apiv1.VolumeMount{
			Name:      csiDataVolumeClaimName(spec),
			MountPath: constants.CSIDriverLocalMountPath,
		})
	}
	if spec.Resources.SharedMemoryBytes != nil {
		mounts = append(mounts, apiv1.VolumeMount{
			Name:      constants.SharedMemoryVolumeName,
			MountPath: constants.ShmDevice,
			ReadOnly:  false,
		})
	}
	return mounts
}

// podVolumes returns the pod-level Volumes for the analysis Deployment.
func (c *Config) podVolumes(spec *operatorclient.VICESpec) []apiv1.Volume {
	volumes := []apiv1.Volume{}

	useCSI := c.UseCSIDriver && spec.MountDataStore

	if !useCSI && hasTicketlessInputs(spec) {
		volumes = append(volumes, apiv1.Volume{
			Name: constants.InputPathListVolumeName,
			VolumeSource: apiv1.VolumeSource{
				ConfigMap: &apiv1.ConfigMapVolumeSource{
					LocalObjectReference: apiv1.LocalObjectReference{Name: inputPathListConfigMapName(spec)},
				},
			},
		})
	}

	volumes = append(volumes, apiv1.Volume{
		Name: workingDirVolumeName(spec),
		VolumeSource: apiv1.VolumeSource{
			PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{ClaimName: workingDirVolumeName(spec)},
		},
	})
	if useCSI {
		volumes = append(volumes, apiv1.Volume{
			Name: csiDataVolumeClaimName(spec),
			VolumeSource: apiv1.VolumeSource{
				PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{ClaimName: csiDataVolumeClaimName(spec)},
			},
		})
	} else {
		volumes = append(volumes, apiv1.Volume{
			Name: constants.PorklockConfigVolumeName,
			VolumeSource: apiv1.VolumeSource{
				Secret: &apiv1.SecretVolumeSource{SecretName: constants.PorklockConfigSecretName},
			},
		})
	}

	volumes = append(volumes,
		apiv1.Volume{
			Name: constants.ExcludesVolumeName,
			VolumeSource: apiv1.VolumeSource{
				ConfigMap: &apiv1.ConfigMapVolumeSource{
					LocalObjectReference: apiv1.LocalObjectReference{Name: excludesConfigMapName(spec)},
				},
			},
		},
		apiv1.Volume{
			Name: constants.PermissionsVolumeName,
			VolumeSource: apiv1.VolumeSource{
				ConfigMap: &apiv1.ConfigMapVolumeSource{
					LocalObjectReference: apiv1.LocalObjectReference{Name: permissionsConfigMapName(spec)},
				},
			},
		},
	)

	// Naming the key in Items makes a mismatched --ca-bundle-key fail the pod
	// with a kubelet event. Mounting the whole ConfigMap instead would leave
	// SSL_CERT_FILE pointing at a missing file, which crypto/x509 ignores
	// silently — reproducing the untrusted-CA error the bundle is there to fix.
	if c.CABundleConfigMap != "" {
		key := c.caBundleKey()
		volumes = append(volumes, apiv1.Volume{
			Name: constants.CABundleVolumeName,
			VolumeSource: apiv1.VolumeSource{
				ConfigMap: &apiv1.ConfigMapVolumeSource{
					LocalObjectReference: apiv1.LocalObjectReference{Name: c.CABundleConfigMap},
					Items:                []apiv1.KeyToPath{{Key: key, Path: key}},
				},
			},
		})
	}

	if spec.Resources.SharedMemoryBytes != nil {
		size := resourcev1.NewQuantity(*spec.Resources.SharedMemoryBytes, resourcev1.BinarySI)
		volumes = append(volumes, apiv1.Volume{
			Name: constants.SharedMemoryVolumeName,
			VolumeSource: apiv1.VolumeSource{
				EmptyDir: &apiv1.EmptyDirVolumeSource{Medium: "Memory", SizeLimit: size},
			},
		})
	}

	return volumes
}
