package incluster

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/cyverse-de/model/v8"
	apiv1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	defaultStorageCapacity, _ = resourcev1.ParseQuantity("5Gi")
)

// IRODSFSPathMapping defines a single path mapping that can be used by the iRODS CSI driver to create a mount point.
type IRODSFSPathMapping struct {
	IRODSPath           string `yaml:"irods_path" json:"irods_path"`
	MappingPath         string `yaml:"mapping_path" json:"mapping_path"`
	ResourceType        string `yaml:"resource_type" json:"resource_type"` // file or dir
	ReadOnly            bool   `yaml:"read_only" json:"read_only"`
	CreateDir           bool   `yaml:"create_dir" json:"create_dir"`
	IgnoreNotExistError bool   `yaml:"ignore_not_exist_error" json:"ignore_not_exist_error"`
}

func (i *Incluster) getZoneMountPath() string {
	return fmt.Sprintf("%s/%s", csiDriverLocalMountPath, i.IRODSZone)
}

func (i *Incluster) getCSIDataVolumeHandle(job *model.Job) string {
	return fmt.Sprintf("%s-handle-%s", csiDriverDataVolumeNamePrefix, job.InvocationID)
}

func (i *Incluster) getCSIDataVolumeName(job *model.Job) string {
	return fmt.Sprintf("%s-%s", csiDriverDataVolumeNamePrefix, job.InvocationID)
}

func (i *Incluster) getCSIDataVolumeClaimName(job *model.Job) string {
	return fmt.Sprintf("%s-%s", csiDriverDataVolumeClaimNamePrefix, job.InvocationID)
}

func (i *Incluster) getInputPathMappings(job *model.Job) ([]IRODSFSPathMapping, error) {
	mappings := []IRODSFSPathMapping{}
	// mark if the mapping path is already occupied
	// key = mount path, val = irods path
	mappingMap := map[string]string{}

	// Mount the input and output files.
	for _, step := range job.Steps {
		for _, stepInput := range step.Config.Inputs {
			irodsPath := stepInput.IRODSPath()
			if len(irodsPath) > 0 {
				var resourceType string
				if strings.ToLower(stepInput.Type) == "fileinput" {
					resourceType = "file"
				} else if strings.ToLower(stepInput.Type) == "multifileselector" {
					resourceType = "file"
				} else if strings.ToLower(stepInput.Type) == "folderinput" {
					resourceType = "dir"
				} else {
					// unknown
					return nil, fmt.Errorf("unknown step input type - %s", stepInput.Type)
				}

				mountPath := fmt.Sprintf("%s/%s", csiDriverInputVolumeMountPath, filepath.Base(irodsPath))
				// check if mountPath is already used by other input
				if existingIRODSPath, ok := mappingMap[mountPath]; ok {
					// exists - error
					return nil, fmt.Errorf("tried to mount an input file %s at %s already used by - %s", irodsPath, mountPath, existingIRODSPath)
				}
				mappingMap[mountPath] = irodsPath

				mapping := IRODSFSPathMapping{
					IRODSPath:           irodsPath,
					MappingPath:         mountPath,
					ResourceType:        resourceType,
					ReadOnly:            true,
					CreateDir:           false,
					IgnoreNotExistError: true,
				}

				mappings = append(mappings, mapping)
			}
		}
	}
	return mappings, nil
}

func (i *Incluster) getOutputPathMapping(job *model.Job) IRODSFSPathMapping {
	// mount a single collection for output
	return IRODSFSPathMapping{
		IRODSPath:           job.OutputDirectory(),
		MappingPath:         csiDriverOutputVolumeMountPath,
		ResourceType:        "dir",
		ReadOnly:            false,
		CreateDir:           true,
		IgnoreNotExistError: true,
	}
}

func (i *Incluster) getHomePathMapping(job *model.Job) IRODSFSPathMapping {
	// mount a single collection for home
	return IRODSFSPathMapping{
		IRODSPath:           job.UserHome,
		MappingPath:         job.UserHome,
		ResourceType:        "dir",
		ReadOnly:            false,
		CreateDir:           false,
		IgnoreNotExistError: false,
	}
}

func (i *Incluster) getSharedPathMapping() IRODSFSPathMapping {
	// mount a single collection for shared data
	sharedHomeFullPath := fmt.Sprintf("/%s/home/shared", i.IRODSZone)

	return IRODSFSPathMapping{
		IRODSPath:           sharedHomeFullPath,
		MappingPath:         sharedHomeFullPath,
		ResourceType:        "dir",
		ReadOnly:            false,
		CreateDir:           false,
		IgnoreNotExistError: true,
	}
}

func (i *Incluster) getCSIDataVolumeLabels(ctx context.Context, job *model.Job) (map[string]string, error) {
	labels, err := i.labelsFromJob(ctx, job)
	if err != nil {
		return nil, err
	}

	labels["volume-name"] = i.getCSIDataVolumeName(job)
	return labels, nil
}

// getPersistentVolumes returns the PersistentVolumes for the VICE analysis. It does
// not call the k8s API.
func (i *Incluster) getPersistentVolumes(ctx context.Context, job *model.Job) ([]*apiv1.PersistentVolume, error) {
	if i.UseCSIDriver && job.MountDataStore {
		dataPathMappings := []IRODSFSPathMapping{}

		// input output path
		inputPathMappings, err := i.getInputPathMappings(job)
		if err != nil {
			return nil, err
		}
		dataPathMappings = append(dataPathMappings, inputPathMappings...)

		outputPathMapping := i.getOutputPathMapping(job)
		dataPathMappings = append(dataPathMappings, outputPathMapping)

		// home path
		if job.UserHome != "" {
			homePathMapping := i.getHomePathMapping(job)
			dataPathMappings = append(dataPathMappings, homePathMapping)
		}

		// shared path
		sharedPathMapping := i.getSharedPathMapping()
		dataPathMappings = append(dataPathMappings, sharedPathMapping)

		// convert path mappings into json
		dataPathMappingsJSONBytes, err := json.Marshal(dataPathMappings)
		if err != nil {
			return nil, err
		}

		volmode := apiv1.PersistentVolumeFilesystem
		persistentVolumes := []*apiv1.PersistentVolume{}

		dataVolumeLabels, err := i.getCSIDataVolumeLabels(ctx, job)
		if err != nil {
			return nil, err
		}

		dataVolume := &apiv1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:   i.getCSIDataVolumeName(job),
				Labels: dataVolumeLabels,
			},
			Spec: apiv1.PersistentVolumeSpec{
				Capacity: apiv1.ResourceList{
					apiv1.ResourceStorage: defaultStorageCapacity,
				},
				VolumeMode: &volmode,
				AccessModes: []apiv1.PersistentVolumeAccessMode{
					apiv1.ReadWriteMany,
				},
				PersistentVolumeReclaimPolicy: apiv1.PersistentVolumeReclaimRetain,
				StorageClassName:              csiDriverStorageClassName,
				PersistentVolumeSource: apiv1.PersistentVolumeSource{
					CSI: &apiv1.CSIPersistentVolumeSource{
						Driver:       csiDriverName,
						VolumeHandle: i.getCSIDataVolumeHandle(job),
						VolumeAttributes: map[string]string{
							"client":              "irodsfuse",
							"path_mapping_json":   string(dataPathMappingsJSONBytes),
							"no_permission_check": "true",
							// use proxy access
							"clientUser": job.Submitter,
							"uid":        fmt.Sprintf("%d", job.Steps[0].Component.Container.UID),
							"gid":        fmt.Sprintf("%d", job.Steps[0].Component.Container.UID),
						},
					},
				},
			},
		}

		persistentVolumes = append(persistentVolumes, dataVolume)
		return persistentVolumes, nil
	}

	return nil, nil
}

// getPersistentVolumeClaims returns the PersistentVolumes for the VICE analysis. It does
// not call the k8s API.
func (i *Incluster) getPersistentVolumeClaims(ctx context.Context, job *model.Job) ([]*apiv1.PersistentVolumeClaim, error) {
	if i.UseCSIDriver && job.MountDataStore {
		labels, err := i.labelsFromJob(ctx, job)
		if err != nil {
			return nil, err
		}

		storageclassname := csiDriverStorageClassName
		volumeClaims := []*apiv1.PersistentVolumeClaim{}

		dataVolumeClaim := &apiv1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{
				Name:   i.getCSIDataVolumeClaimName(job),
				Labels: labels,
			},
			Spec: apiv1.PersistentVolumeClaimSpec{
				AccessModes: []apiv1.PersistentVolumeAccessMode{
					apiv1.ReadWriteMany,
				},
				StorageClassName: &storageclassname,
				VolumeName:       i.getCSIDataVolumeName(job),
				Resources: apiv1.VolumeResourceRequirements{
					Requests: apiv1.ResourceList{
						apiv1.ResourceStorage: defaultStorageCapacity,
					},
				},
			},
		}

		volumeClaims = append(volumeClaims, dataVolumeClaim)
		return volumeClaims, nil
	}

	return nil, nil
}

// getPersistentVolumeSources returns the volumes for the VICE analysis. It does
// not call the k8s API.
func (i *Incluster) getPersistentVolumeSources(job *model.Job) ([]*apiv1.Volume, error) {
	if i.UseCSIDriver && job.MountDataStore {
		volumes := []*apiv1.Volume{}

		dataVolume := &apiv1.Volume{
			Name: i.getCSIDataVolumeClaimName(job),
			VolumeSource: apiv1.VolumeSource{
				PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{
					ClaimName: i.getCSIDataVolumeClaimName(job),
				},
			},
		}

		volumes = append(volumes, dataVolume)
		return volumes, nil
	}

	return nil, nil
}

// getPersistentVolumeMounts returns the volume mount for the VICE analysis. It does
// not call the k8s API.
func (i *Incluster) getPersistentVolumeMounts(job *model.Job) []*apiv1.VolumeMount {
	if i.UseCSIDriver && job.MountDataStore {
		volumeMounts := []*apiv1.VolumeMount{}

		dataVolumeMount := &apiv1.VolumeMount{
			Name:      i.getCSIDataVolumeClaimName(job),
			MountPath: csiDriverLocalMountPath,
		}

		volumeMounts = append(volumeMounts, dataVolumeMount)
		return volumeMounts
	}

	return nil
}
