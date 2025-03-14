package batch

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/argoproj/argo-workflows/v3/cmd/argo/commands/client"
	workflowpkg "github.com/argoproj/argo-workflows/v3/pkg/apiclient/workflow"
	v1alpha1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/cyverse-de/app-exposer/imageinfo"
	"github.com/cyverse-de/app-exposer/resourcing"
	"github.com/cyverse-de/model/v8"
	"github.com/gosimple/slug"
	apiv1 "k8s.io/api/core/v1"
	resourcev1 "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/kubernetes"
)

var (
	defaultStorageCapacity, _ = resourcev1.ParseQuantity("5Gi")
	defaultVolumeName         = "workdir"
	statusRunning             = "running"
)

type BatchSubmissionOpts struct {
	FileTransferImage      string
	FileTransferLogLevel   string
	FileTransferWorkingDir string
	StatusSenderImage      string
	ExternalID             string
}

type WorkflowMaker struct {
	getter    imageinfo.InfoGetter
	analysis  *model.Analysis
	clientset kubernetes.Interface
}

// NewWorkflowMaker creates a new instance of WorkflowMaker
func NewWorkflowMaker(getter imageinfo.InfoGetter, analysis *model.Analysis, clientset kubernetes.Interface) *WorkflowMaker {
	return &WorkflowMaker{
		getter:    getter,
		analysis:  analysis,
		clientset: clientset,
	}
}

// stepTemplates creates a list of templates based on the steps
// defined in the analysis description.
func (w *WorkflowMaker) stepTemplates() ([]v1alpha1.Template, error) {
	var templates []v1alpha1.Template

	for idx, step := range w.analysis.Steps {
		var (
			sourceParts []string
			source      string
		)

		// If there's an entrypoint defined, it needs to be the command in the script.
		if step.Component.Container.EntryPoint != "" {
			sourceParts = append(sourceParts, step.Component.Container.EntryPoint)
		} else {
			// It's possible that the integrator didn't bother to include the entrypoint
			// in the definition, causing us to depend on the entrypoint defined in the
			// image. We need to get the entrypoint in the image (if it exists), so that
			// we can include it in the generated script that the image will run.
			if w.getter.IsInRepo(step.Component.Container.Image.Name) {
				project, image, _, err := w.getter.RepoParts(step.Component.Container.Image.Name)
				if err != nil {
					return nil, err
				}

				harborInfo, err := w.getter.GetInfo(project, image, step.Component.Container.Image.Tag)
				if err != nil {
					return nil, err
				}

				if strings.TrimSpace(harborInfo.Entrypoint) != "" {
					sourceParts = append(sourceParts, harborInfo.Entrypoint)
				}
			}
		}

		// Add the arguments to the source. If may include the tool
		// executable, it may already have been added to the source as the
		// entrypoint.
		sourceParts = append(sourceParts, step.Arguments()...)

		// If the StdoutPath is not empty, then stdout of the command needs to go to
		// a named file.
		if step.StdoutPath != "" {
			sourceParts = append(sourceParts, fmt.Sprintf("> %s", step.StdoutPath))
		} else {
			// If the StdoutPath is empty, then write out stdout to a file
			sourceParts = append(sourceParts, fmt.Sprintf("> logs/step-%d.stdout.log", idx))
		}

		// If the StderrPath is not empty, then stderr of the command needs to go to
		// a named file.
		if step.StderrPath != "" {
			sourceParts = append(sourceParts, fmt.Sprintf("2> %s", step.StderrPath))
		} else {
			// if the StderrPath is empty, then write out the stderr to a file
			sourceParts = append(sourceParts, fmt.Sprintf("2> logs/step-%d.stderr.log", idx))
		}

		// Assemble the source string for the script template.
		source = strings.Join(sourceParts, " ")

		stTmpl := v1alpha1.Template{
			Name: fmt.Sprintf("step-%d", idx),
			Script: &v1alpha1.ScriptTemplate{
				Source: source,
				Container: apiv1.Container{
					Resources: *resourcing.Requirements(w.analysis),
					Image: fmt.Sprintf(
						"%s:%s",
						step.Component.Container.Image.Name,
						step.Component.Container.Image.Tag,
					),
					Command:    []string{"bash"},
					WorkingDir: step.Component.Container.WorkingDirectory(),
					VolumeMounts: []apiv1.VolumeMount{
						{
							Name:      defaultVolumeName,
							MountPath: step.Component.Container.WorkingDirectory(),
						},
					},
				},
			},
		}

		// This is used to mount host paths into the container.
		for volumeIdx, volume := range step.Component.Container.Volumes {
			stTmpl.Script.Container.VolumeMounts = append(
				stTmpl.Script.Container.VolumeMounts,
				apiv1.VolumeMount{
					Name:      fmt.Sprintf("step-%d-%d", idx, volumeIdx),
					MountPath: volume.ContainerPath,
					ReadOnly:  volume.ReadOnly,
				},
			)
		}

		// If the step is expected to mount a volume from another container
		// then we need to add volume mounts for the corresponding secrets.
		// Data containers are no longer supported and are replaced by static
		// secrets that get mounted into the container.
		for _, volumeFrom := range step.Component.Container.VolumesFrom {
			var containerPath string
			if volumeFrom.ContainerPath == "" {
				containerPath = "/mnt/discoenv"
			} else {
				containerPath = volumeFrom.ContainerPath
			}
			stTmpl.Script.Container.VolumeMounts = append(
				stTmpl.Script.Container.VolumeMounts,
				apiv1.VolumeMount{
					Name:      volumeFrom.NamePrefix,
					MountPath: containerPath,
					ReadOnly:  volumeFrom.ReadOnly,
				},
			)
		}

		templates = append(templates, stTmpl)
	}

	return templates, nil
}

// runStepsTemplates generates a list of templates that orchestrate the logic
// of a workflow.
func (w *WorkflowMaker) runStepsTemplates() ([]v1alpha1.Template, error) {
	// We generate a sequence of parallel steps consisting of single steps to
	// force the steps to run in sequence. Looks nicer in YAML than it does in
	// in code form.
	var templates []v1alpha1.Template
	var runSteps []v1alpha1.ParallelSteps

	stepTemplates, err := w.stepTemplates()
	if err != nil {
		return templates, err
	}

	runSteps = append(
		runSteps,
		v1alpha1.ParallelSteps{
			Steps: []v1alpha1.WorkflowStep{
				{
					Name:     "init-working-dir",
					Template: "init-working-dir",
				},
			},
		},
		v1alpha1.ParallelSteps{
			Steps: []v1alpha1.WorkflowStep{
				*w.sendStatusWorkflowStep(
					"downloading-files-status",
					"downloading files",
					"running",
					"begin-downloading-files",
				),
			},
		},
		v1alpha1.ParallelSteps{
			Steps: []v1alpha1.WorkflowStep{
				{
					Name:     "download-files",
					Template: "download-files",
				},
			},
		},
		v1alpha1.ParallelSteps{
			Steps: []v1alpha1.WorkflowStep{
				*w.sendStatusWorkflowStep(
					"done-downloading",
					"done downloading inputs",
					"running",
					"done-downloading-files",
				),
			},
		},
	)

	for idx, st := range stepTemplates {
		runningName := fmt.Sprintf("running-%d", idx)
		runningMsg := fmt.Sprintf("running %d", idx)
		stepTmplName := fmt.Sprintf("step-%d", idx)
		doneRunningName := fmt.Sprintf("done-running-%d", idx)
		doneRunningMsg := fmt.Sprintf("done running %d", idx)

		runSteps = append(runSteps,
			v1alpha1.ParallelSteps{
				Steps: []v1alpha1.WorkflowStep{
					*w.sendStatusWorkflowStep(
						runningName,
						runningMsg,
						statusRunning,
						fmt.Sprintf("begin-step-%d", idx),
					),
				},
			},
			v1alpha1.ParallelSteps{
				Steps: []v1alpha1.WorkflowStep{
					{
						Name:     st.Name,
						Template: stepTmplName,
					},
				},
			},
			v1alpha1.ParallelSteps{
				Steps: []v1alpha1.WorkflowStep{
					*w.sendStatusWorkflowStep(
						doneRunningName,
						doneRunningMsg,
						statusRunning,
						fmt.Sprintf("done-step-%d", idx),
					),
				},
			},
		)
	}

	templates = append(templates, v1alpha1.Template{
		Name:  "analysis-steps",
		Steps: runSteps,
	})

	templates = append(templates, stepTemplates...)

	return templates, nil
}

// exitHandlerTemplate returns the template definition for the
// steps taken when the workflow exits.
func (w *WorkflowMaker) exitHandlerTemplate() *v1alpha1.Template {
	return &v1alpha1.Template{
		Name: "analysis-exit-handler",
		Steps: []v1alpha1.ParallelSteps{
			{
				Steps: []v1alpha1.WorkflowStep{
					*w.sendStatusWorkflowStep(
						"uploading-files-status",
						"uploading files",
						statusRunning,
						"uploading-files",
					),
				},
			},
			{
				Steps: []v1alpha1.WorkflowStep{
					{
						Name:     "upload-files",
						Template: "upload-files",
					},
				},
			},
			{
				Steps: []v1alpha1.WorkflowStep{
					*w.sendStatusWorkflowStep(
						"finished-status",
						"sending final status",
						"{{workflow.status}}",
						"finished-status",
					),
				},
			},
			{
				Steps: []v1alpha1.WorkflowStep{
					{
						Name:     "cleanup",
						Template: "send-cleanup",
					},
				},
			},
		},
	}
}

func (w *WorkflowMaker) sendStatusWorkflowStep(name, msg, state, prefix string) *v1alpha1.WorkflowStep {
	return &v1alpha1.WorkflowStep{
		Name:     name,
		Template: "send-status",
		Arguments: v1alpha1.Arguments{
			Parameters: []v1alpha1.Parameter{
				{
					Name:  "message",
					Value: v1alpha1.AnyStringPtr(msg),
				},
				{
					Name:  "state",
					Value: v1alpha1.AnyStringPtr(state),
				},
				{
					Name:  "log-prefix",
					Value: v1alpha1.AnyStringPtr(prefix),
				},
			},
		},
	}
}

// sendStatusTemplate returns the template definition for the steps that send
// status updates to the DE backend. The init-working-dir template must be run
// before this template, though they can be defined in any order.
func (w *WorkflowMaker) sendStatusTemplate(opts *BatchSubmissionOpts) *v1alpha1.Template {
	return &v1alpha1.Template{
		Name: "send-status",
		Inputs: v1alpha1.Inputs{
			Parameters: []v1alpha1.Parameter{
				{
					Name: "message",
				},
				{
					Name: "state",
				},
				{
					Name: "log-prefix",
				},
			},
		},
		Script: &v1alpha1.ScriptTemplate{
			Source: `
				curl -v -H "Content-Type: application/json" -d '{
				    "job_uuid" : "{{workflow.parameters.job_uuid}}",
     				"hostname" : "batch",
         			"message": "{{inputs.parameters.message}}",
            		"state" : "{{inputs.parameters.state}}"
     			}' http://webhook-eventsource-svc.argo-events/batch > logs/{{inputs.parameters.log-prefix}}-send-status.stdout.log 2> logs/{{inputs.parameters.log-prefix}}-send-status.stderr.log
			`,
			Container: apiv1.Container{
				Image:      opts.StatusSenderImage,
				Command:    []string{"bash"},
				WorkingDir: opts.FileTransferWorkingDir,
				VolumeMounts: []apiv1.VolumeMount{
					{
						Name:      defaultVolumeName,
						MountPath: opts.FileTransferWorkingDir,
					},
				},
			},
		},
	}
}

// sendCleanupEvent returns the template definition for the steps that send
// status updates to the DE backend. The init-working-dir template must be run
// before this template, though they can be defined in any order.
func (w *WorkflowMaker) sendCleanupEventTemplate(opts *BatchSubmissionOpts) *v1alpha1.Template {
	return &v1alpha1.Template{
		Name: "send-cleanup",
		Script: &v1alpha1.ScriptTemplate{
			Source: `
				curl -v -H "Content-Type: application/json" -d '{"uuid":"{{workflow.parameters.job_uuid}}"}' http://webhook-eventsource-svc.argo-events/batch/cleanup > logs/send-cleanup-notif.stdout.log 2> logs/send-cleanup-notif.stderr.log
			`,
			Container: apiv1.Container{
				Image:      opts.StatusSenderImage,
				Command:    []string{"bash"},
				WorkingDir: opts.FileTransferWorkingDir,
				VolumeMounts: []apiv1.VolumeMount{
					{
						Name:      defaultVolumeName,
						MountPath: opts.FileTransferWorkingDir,
					},
				},
			},
		},
	}
}

// initWorkingDirectoryTemplate initializes the working directory
func (w *WorkflowMaker) initWorkingDirectoryTemplate(opts *BatchSubmissionOpts) *v1alpha1.Template {
	return &v1alpha1.Template{
		Name: "init-working-dir",
		Container: &apiv1.Container{
			Image: opts.FileTransferImage,
			Command: []string{
				"mkdir",
			},
			Args: []string{
				"-p",
				"logs/",
			},
			WorkingDir: opts.FileTransferWorkingDir,
			VolumeMounts: []apiv1.VolumeMount{
				{
					Name:      defaultVolumeName,
					MountPath: opts.FileTransferWorkingDir,
				},
			},
		},
	}
}

// downloadFilesTemplate returns a template definition for the steps that
// download files from the data store into the working directory volume.
// The init-working-dir template must be run before this template, though
// they can be defined in any order.
func (w *WorkflowMaker) downloadFilesTemplate(opts *BatchSubmissionOpts) *v1alpha1.Template {
	var inputFilesAndFolders []string

	for _, stepInput := range w.analysis.Inputs() {
		inputFilesAndFolders = append(
			inputFilesAndFolders,
			stepInput.IRODSPath(),
		)
	}

	args := []string{
		"gocmd",
		fmt.Sprintf("--log_level=%s", opts.FileTransferLogLevel),
		"get",
	}
	args = append(args, inputFilesAndFolders...)
	args = append(args, "> logs/downloads.stdout.log")
	args = append(args, "2> logs/downloads.stderr.log")

	return &v1alpha1.Template{
		Name: "download-files",
		Script: &v1alpha1.ScriptTemplate{
			Source: strings.Join(args, " "),
			Container: apiv1.Container{
				Image:      opts.FileTransferImage,
				WorkingDir: opts.FileTransferWorkingDir,
				Command:    []string{"bash"},
				VolumeMounts: []apiv1.VolumeMount{
					{
						Name:      defaultVolumeName,
						MountPath: opts.FileTransferWorkingDir,
					},
				},
				Env: []apiv1.EnvVar{
					{
						Name:  "IRODS_CLIENT_USER_NAME",
						Value: "{{workflow.parameters.username}}",
					},
					{
						Name: "IRODS_HOST",
						ValueFrom: &apiv1.EnvVarSource{
							ConfigMapKeyRef: &apiv1.ConfigMapKeySelector{
								Key: "IRODS_HOST",
								LocalObjectReference: apiv1.LocalObjectReference{
									Name: "irods-config",
								},
							},
						},
					},
					{
						Name: "IRODS_PORT",
						ValueFrom: &apiv1.EnvVarSource{
							ConfigMapKeyRef: &apiv1.ConfigMapKeySelector{
								Key: "IRODS_PORT",
								LocalObjectReference: apiv1.LocalObjectReference{
									Name: "irods-config",
								},
							},
						},
					},
					{
						Name: "IRODS_USER_NAME",
						ValueFrom: &apiv1.EnvVarSource{
							ConfigMapKeyRef: &apiv1.ConfigMapKeySelector{
								Key: "IRODS_USER_NAME",
								LocalObjectReference: apiv1.LocalObjectReference{
									Name: "irods-config",
								},
							},
						},
					},
					{
						Name: "IRODS_USER_PASSWORD",
						ValueFrom: &apiv1.EnvVarSource{
							ConfigMapKeyRef: &apiv1.ConfigMapKeySelector{
								Key: "IRODS_USER_PASSWORD",
								LocalObjectReference: apiv1.LocalObjectReference{
									Name: "irods-config",
								},
							},
						},
					},
					{
						Name: "IRODS_ZONE_NAME",
						ValueFrom: &apiv1.EnvVarSource{
							ConfigMapKeyRef: &apiv1.ConfigMapKeySelector{
								Key: "IRODS_ZONE_NAME",
								LocalObjectReference: apiv1.LocalObjectReference{
									Name: "irods-config",
								},
							},
						},
					},
				},
			},
		},
	}
}

// uploadFilesTemplate returns a template used for the steps that uploads
// files to the data store. The init-working-dir template must be run
// before this template, though they can be defined in any order.
func (w *WorkflowMaker) uploadFilesTemplate(opts *BatchSubmissionOpts) *v1alpha1.Template {
	args := []string{
		"gocmd",
		fmt.Sprintf("--log_level=%s", opts.FileTransferLogLevel),
		"put",
		"-f",
		"--no_root",
		".",
		"{{workflow.parameters.output-folder}}",
		"> logs/uploads.stdout.log",
		"2> logs/uploads.stderr.log",
	}

	return &v1alpha1.Template{
		Name: "upload-files",
		Script: &v1alpha1.ScriptTemplate{
			Source: strings.Join(args, " "),
			Container: apiv1.Container{
				Image:      opts.FileTransferImage,
				Command:    []string{"bash"},
				WorkingDir: opts.FileTransferWorkingDir,
				VolumeMounts: []apiv1.VolumeMount{
					{
						Name:      defaultVolumeName,
						MountPath: opts.FileTransferWorkingDir,
					},
				},
				Env: []apiv1.EnvVar{
					{
						Name:  "IRODS_CLIENT_USER_NAME",
						Value: "{{workflow.parameters.username}}",
					},
					{
						Name: "IRODS_HOST",
						ValueFrom: &apiv1.EnvVarSource{
							ConfigMapKeyRef: &apiv1.ConfigMapKeySelector{
								Key: "IRODS_HOST",
								LocalObjectReference: apiv1.LocalObjectReference{
									Name: "irods-config",
								},
							},
						},
					},
					{
						Name: "IRODS_PORT",
						ValueFrom: &apiv1.EnvVarSource{
							ConfigMapKeyRef: &apiv1.ConfigMapKeySelector{
								Key: "IRODS_PORT",
								LocalObjectReference: apiv1.LocalObjectReference{
									Name: "irods-config",
								},
							},
						},
					},
					{
						Name: "IRODS_USER_NAME",
						ValueFrom: &apiv1.EnvVarSource{
							ConfigMapKeyRef: &apiv1.ConfigMapKeySelector{
								Key: "IRODS_USER_NAME",
								LocalObjectReference: apiv1.LocalObjectReference{
									Name: "irods-config",
								},
							},
						},
					},
					{
						Name: "IRODS_USER_PASSWORD",
						ValueFrom: &apiv1.EnvVarSource{
							ConfigMapKeyRef: &apiv1.ConfigMapKeySelector{
								Key: "IRODS_USER_PASSWORD",
								LocalObjectReference: apiv1.LocalObjectReference{
									Name: "irods-config",
								},
							},
						},
					},
					{
						Name: "IRODS_ZONE_NAME",
						ValueFrom: &apiv1.EnvVarSource{
							ConfigMapKeyRef: &apiv1.ConfigMapKeySelector{
								Key: "IRODS_ZONE_NAME",
								LocalObjectReference: apiv1.LocalObjectReference{
									Name: "irods-config",
								},
							},
						},
					},
				},
			},
		},
	}
}

// NewWorkflow returns a defintion of a workflow that executes a DE batch
// analyis. It does not submit the workflow to the cluster.
func (w *WorkflowMaker) NewWorkflow(ctx context.Context, opts *BatchSubmissionOpts) (*v1alpha1.Workflow, error) {
	var workflowTemplates []v1alpha1.Template
	stepsTemplates, err := w.runStepsTemplates()
	if err != nil {
		return nil, err
	}
	workflowTemplates = append(workflowTemplates, stepsTemplates...)
	workflowTemplates = append(
		workflowTemplates,
		*w.initWorkingDirectoryTemplate(opts),
		*w.exitHandlerTemplate(),
		*w.sendStatusTemplate(opts),
		*w.sendCleanupEventTemplate(opts),
		*w.downloadFilesTemplate(opts),
		*w.uploadFilesTemplate(opts),
	)

	workflow := v1alpha1.Workflow{
		TypeMeta: v1.TypeMeta{
			Kind:       "Workflow",
			APIVersion: "argoproj.io/v1alpha1",
		},
		ObjectMeta: v1.ObjectMeta{
			GenerateName: "batch-analysis-", // TODO: Make this configurable
			Namespace:    "argo",
			Labels: map[string]string{
				"job-uuid":    w.analysis.InvocationID,
				"external-id": w.analysis.InvocationID,
			},
		},
		Spec: v1alpha1.WorkflowSpec{
			ServiceAccountName: "argo-executor",         // TODO: Make this configurable
			Entrypoint:         "analysis-steps",        // TODO: Make this a const
			OnExit:             "analysis-exit-handler", // TODO: Make this a const
			Tolerations: []apiv1.Toleration{
				{
					Key:      "analysis",
					Operator: apiv1.TolerationOpExists,
				},
			},
			Affinity: &apiv1.Affinity{
				NodeAffinity: &apiv1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &apiv1.NodeSelector{
						NodeSelectorTerms: []apiv1.NodeSelectorTerm{
							{
								MatchExpressions: []apiv1.NodeSelectorRequirement{
									{
										Key:      "analysis",
										Operator: apiv1.NodeSelectorOpExists,
									},
								},
							},
						},
					},
					PreferredDuringSchedulingIgnoredDuringExecution: []apiv1.PreferredSchedulingTerm{
						{
							Weight: 1,
							Preference: apiv1.NodeSelectorTerm{
								MatchExpressions: []apiv1.NodeSelectorRequirement{
									{
										Key:      "batch",
										Operator: apiv1.NodeSelectorOpExists,
									},
								},
							},
						},
					},
				},
			},
			Arguments: v1alpha1.Arguments{
				Parameters: []v1alpha1.Parameter{
					{
						Name:  "username",
						Value: v1alpha1.AnyStringPtr(w.analysis.Submitter),
					},
					{
						Name:  "output-folder",
						Value: v1alpha1.AnyStringPtr(w.analysis.OutputDirectory()),
					},
					{
						Name:  "job_uuid",
						Value: v1alpha1.AnyStringPtr(w.analysis.InvocationID),
					},
				},
			},
			VolumeClaimTemplates: []apiv1.PersistentVolumeClaim{
				{
					ObjectMeta: v1.ObjectMeta{
						Name: defaultVolumeName,
					},
					Spec: apiv1.PersistentVolumeClaimSpec{
						AccessModes: []apiv1.PersistentVolumeAccessMode{
							apiv1.ReadWriteOnce,
						},
						Resources: apiv1.VolumeResourceRequirements{
							Requests: apiv1.ResourceList{
								apiv1.ResourceStorage: defaultStorageCapacity,
							},
						},
					},
				},
			},
			Templates: workflowTemplates,
		},
	}

	if err := w.addHostPathVolumes(&workflow); err != nil {
		return &workflow, err
	}

	if err := w.addDataContainers(ctx, &workflow); err != nil {
		return &workflow, err
	}

	return &workflow, nil
}

func (w *WorkflowMaker) addHostPathVolumes(workflow *v1alpha1.Workflow) error {
	// If the analysis uses volumes, it needs a host path mounted into the container
	// as a volume. We define the volumes up front here and the individual workflow
	// steps can then use them in their VolumeMounts.
	if w.analysis.UsesVolumes() {
		// Add a node selector that limits the hosts to those with the NFS mounts
		// available.
		me := &workflow.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions

		*me = append(*me, apiv1.NodeSelectorRequirement{
			Key:      "has-nfs",
			Operator: apiv1.NodeSelectorOpIn,
			Values:   []string{"true"},
		})

		// Add the volume definitions
		workflow.Spec.Volumes = []apiv1.Volume{}
		hostPathType := apiv1.HostPathDirectory // We only suppport directories for now, and they need to be predefined.

		for stepIndex, step := range w.analysis.Steps {
			for volumeIndex, volume := range step.Component.Container.Volumes {
				workflow.Spec.Volumes = append(
					workflow.Spec.Volumes,
					apiv1.Volume{
						Name: fmt.Sprintf("step-%d-%d", stepIndex, volumeIndex), // Important when referencing the volumes in the steps VolumeMounts
						VolumeSource: apiv1.VolumeSource{
							HostPath: &apiv1.HostPathVolumeSource{
								Path: volume.HostPath,
								Type: &hostPathType,
							},
						},
					},
				)
			}
		}
	}
	return nil
}

// addDataContainers will add the secret volumes to the workflow based on the
// data container definitions in the analysis definition. Modifies the workflow
// and doesn't return a new one. Returns an error if the operation fails.
func (w *WorkflowMaker) addDataContainers(ctx context.Context, workflow *v1alpha1.Workflow) error {
	readOnly := int32(0555)

	// If the analysis uses data containers, we need to add the secrets as mountable volumes.
	if len(w.analysis.DataContainers()) > 0 {
		for _, volumeFrom := range w.analysis.DataContainers() {
			// Slugify the secret name and replace any underscores with dashes.
			secretName := slug.Substitute(slug.Make(fmt.Sprintf("%s-%s", volumeFrom.NamePrefix, volumeFrom.Tag)), map[string]string{"_": "-"})

			secret, err := w.getSecret(ctx, secretName, workflow.Namespace)
			if err != nil {
				return err
			}

			// Each data key should have a corresponding annotation telling us the path
			// the value should be mounted at.
			items := []apiv1.KeyToPath{}
			for key := range maps.Keys(secret.Data) {
				items = append(items, apiv1.KeyToPath{
					Key:  key,
					Path: secret.ObjectMeta.Annotations[key],
				})
			}

			workflow.Spec.Volumes = append(workflow.Spec.Volumes, apiv1.Volume{
				Name: volumeFrom.NamePrefix, // Keep the name of the volume easy, just the name prefix
				VolumeSource: apiv1.VolumeSource{
					Secret: &apiv1.SecretVolumeSource{
						SecretName:  secretName, // Include the slugified version info in the SecretName
						DefaultMode: &readOnly,  // read and execute. Some secrets contain scripts.
						Items:       items,
					},
				},
			})
		}
	}
	return nil
}

func (w *WorkflowMaker) getSecret(ctx context.Context, name, namespace string) (*apiv1.Secret, error) {
	return w.clientset.CoreV1().Secrets(namespace).Get(ctx, name, v1.GetOptions{})
}

func (w *WorkflowMaker) SubmitWorkflow(ctx context.Context, serviceClient workflowpkg.WorkflowServiceClient, workflow *v1alpha1.Workflow) (*v1alpha1.Workflow, error) {
	creationOptions := &metav1.CreateOptions{}

	return serviceClient.CreateWorkflow(ctx, &workflowpkg.WorkflowCreateRequest{
		Namespace:     workflow.Namespace,
		Workflow:      workflow,
		ServerDryRun:  false,
		CreateOptions: creationOptions,
	})
}

func ListWorkflows(ctx context.Context, serviceClient workflowpkg.WorkflowServiceClient, namespace, labelKey, externalID string) (*v1alpha1.WorkflowList, error) {
	req, err := labels.NewRequirement(labelKey, selection.Equals, []string{externalID})
	if err != nil {
		return nil, err
	}
	return serviceClient.ListWorkflows(ctx, &workflowpkg.WorkflowListRequest{
		Namespace: namespace,
		ListOptions: &v1.ListOptions{
			LabelSelector: req.String(),
		},
	})
}

func StopWorkflows(ctx context.Context, serviceClient workflowpkg.WorkflowServiceClient, namespace, labelKey, externalID string) ([]v1alpha1.Workflow, error) {
	var retval []v1alpha1.Workflow
	workflows, err := ListWorkflows(ctx, serviceClient, namespace, labelKey, externalID)
	if err != nil {
		return nil, err
	}
	for _, workflow := range workflows.Items {
		if slices.Contains([]string{"Running", "Pending"}, string(workflow.Status.Phase)) {
			_, err := serviceClient.StopWorkflow(ctx, &workflowpkg.WorkflowStopRequest{
				Namespace: namespace,
				Name:      workflow.GetName(),
			})
			if err != nil {
				return nil, err
			}
		}

		if _, err = serviceClient.DeleteWorkflow(ctx, &workflowpkg.WorkflowDeleteRequest{
			Namespace: workflow.GetNamespace(),
			Name:      workflow.GetName(),
		}); err != nil {
			return nil, err
		}

		retval = append(retval, workflow)
	}
	return retval, nil
}

// SubmitWorkflow submits a workflow (probably created by GenerateWorkflow()) to the cluster.
// It does not wait for the workflow to complete. The context passed in needs to be the same
// one returned by NewWorkflowServiceClient.
func SubmitWorkflow(ctx context.Context, serviceClient workflowpkg.WorkflowServiceClient, workflow *v1alpha1.Workflow) (*v1alpha1.Workflow, error) {
	creationOptions := &metav1.CreateOptions{}

	return serviceClient.CreateWorkflow(ctx, &workflowpkg.WorkflowCreateRequest{
		Namespace:     workflow.Namespace,
		Workflow:      workflow,
		ServerDryRun:  false,
		CreateOptions: creationOptions,
	})
}

// StopWorkflow stops and deletes a workflow. The operation is asynchronous.
func StopWorkflow(ctx context.Context, serviceClient workflowpkg.WorkflowServiceClient, namespace, name string) (*v1alpha1.Workflow, error) {
	return serviceClient.StopWorkflow(ctx, &workflowpkg.WorkflowStopRequest{
		Namespace: namespace,
		Name:      name,
	})
}

// NewWorkflowServiceClient creates a WorkflowServiceClient that can be used to submit
// a workflow to the cluster with SubmitWorkflow().
func NewWorkflowServiceClient(c context.Context) (context.Context, workflowpkg.WorkflowServiceClient, error) {
	ctx, apiClient, err := client.NewAPIClient(c)
	if err != nil {
		return c, nil, err
	}
	serviceClient := apiClient.NewWorkflowServiceClient()
	return ctx, serviceClient, err
}
