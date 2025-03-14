package incluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sync"
	"time"

	"github.com/cyverse-de/model/v8"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/pkg/errors"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	// RequestedStatus means the the transfer has been requested but hasn't started
	RequestedStatus = "requested"

	// DownloadingStatus means that a downloading request is running
	DownloadingStatus = "downloading"

	// UploadingStatus means that an uploading request is running
	UploadingStatus = "uploading"

	// FailedStatus means that the transfer request failed
	FailedStatus = "failed"

	//CompletedStatus means that the transfer request succeeded
	CompletedStatus = "completed"
)

type transferResponse struct {
	UUID   string `json:"uuid"`
	Status string `json:"status"`
	Kind   string `json:"kind"`
}

// fileTransferCommand returns a []string containing the command to fire up the vice-file-transfers service.
func fileTransferCommand(job *model.Job) []string {
	retval := []string{
		"/vice-file-transfers",
		"--listen-port", "60001",
		"--user", job.Submitter,
		"--excludes-file", path.Join(excludesMountPath, excludesFileName),
		"--path-list-file", path.Join(inputPathListMountPath, inputPathListFileName),
		"--upload-destination", job.OutputDirectory(),
		"--irods-config", irodsConfigFilePath,
		"--invocation-id", job.InvocationID,
	}
	for _, fm := range job.FileMetadata {
		retval = append(retval, fm.Argument()...)
	}
	return retval
}

// fileTransferVolumeMounts returns the list of VolumeMounts needed by the fileTransfer
// container in the VICE analysis pod. Each VolumeMount should correspond to one of the
// Volumes returned by the deploymentVolumes() function. This does not call the k8s API.
func (i *Incluster) fileTransfersVolumeMounts(job *model.Job) []apiv1.VolumeMount {
	retval := []apiv1.VolumeMount{
		{
			Name:      porklockConfigVolumeName,
			MountPath: porklockConfigMountPath,
			ReadOnly:  true,
		},
		{
			Name:      fileTransfersVolumeName,
			MountPath: fileTransfersInputsMountPath,
			ReadOnly:  false,
		},
		{
			Name:      excludesVolumeName,
			MountPath: excludesMountPath,
			ReadOnly:  true,
		},
	}

	if len(job.FilterInputsWithoutTickets()) > 0 {
		retval = append(retval, apiv1.VolumeMount{
			Name:      inputPathListVolumeName,
			MountPath: inputPathListMountPath,
			ReadOnly:  true,
		})
	}

	return retval
}

func requestTransfer(ctx context.Context, svc apiv1.Service, reqpath string) (*transferResponse, error) {
	var (
		bodybytes []byte
		bodyerr   error
		jsonerr   error
		reqerr    error
	)

	xferresp := &transferResponse{}
	svcurl := url.URL{}

	svcurl.Scheme = "http"
	svcurl.Host = fmt.Sprintf("%s.%s:%d", svc.Name, svc.Namespace, fileTransfersPort)
	svcurl.Path = reqpath

	req, reqerr := http.NewRequestWithContext(ctx, http.MethodPost, svcurl.String(), nil)
	if reqerr != nil {
		return nil, errors.Wrapf(reqerr, "error POSTing to %s", svcurl.String())
	}

	resp, posterr := httpClient.Do(req)
	if posterr != nil {
		return nil, errors.Wrapf(posterr, "error POSTing to %s", svcurl.String())
	}
	if resp == nil {
		return nil, fmt.Errorf("response from %s was nil", svcurl.String())
	}

	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 399 {
		return nil, errors.Wrapf(posterr, "download request to %s returned %d", svcurl.String(), resp.StatusCode)
	}

	if bodybytes, bodyerr = io.ReadAll(resp.Body); bodyerr != nil {
		return nil, errors.Wrapf(bodyerr, "reading body from %s failed", svcurl.String())
	}

	if jsonerr = json.Unmarshal(bodybytes, xferresp); jsonerr != nil {
		return nil, errors.Wrapf(jsonerr, "error unmarshalling json from %s", svcurl.String())
	}

	return xferresp, nil
}

func getTransferDetails(ctx context.Context, svc apiv1.Service, reqpath string) (*transferResponse, error) {
	var (
		bodybytes []byte
		bodyerr   error
		jsonerr   error
		reqerr    error
		posterr   error
	)

	xferresp := &transferResponse{}
	svcurl := url.URL{}

	svcurl.Scheme = "http"
	svcurl.Host = fmt.Sprintf("%s.%s:%d", svc.Name, svc.Namespace, fileTransfersPort)
	svcurl.Path = reqpath

	req, reqerr := http.NewRequestWithContext(ctx, http.MethodGet, svcurl.String(), nil)
	if reqerr != nil {
		return nil, errors.Wrapf(reqerr, "error on GET %s", svcurl.String())
	}

	resp, posterr := httpClient.Do(req)
	if posterr != nil {
		return nil, errors.Wrapf(posterr, "error on GET %s", svcurl.String())
	}
	if resp == nil {
		return nil, fmt.Errorf("response from GET %s was nil", svcurl.String())
	}

	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 399 {
		return nil, errors.Wrapf(posterr, "status request to %s returned %d", svcurl.String(), resp.StatusCode)
	}

	if bodybytes, bodyerr = io.ReadAll(resp.Body); bodyerr != nil {
		return nil, errors.Wrapf(bodyerr, "reading body from %s failed", svcurl.String())
	}

	if jsonerr = json.Unmarshal(bodybytes, xferresp); jsonerr != nil {
		return nil, errors.Wrapf(jsonerr, "error unmarshalling json from %s", svcurl.String())
	}

	return xferresp, nil
}

func isFinished(status string) bool {
	switch status {
	case FailedStatus:
		return true
	case CompletedStatus:
		return true
	default:
		return false
	}
}

// deploymentUsesCSIDriver determines whether or not a deployment uses the iRODS CSI Driver. Each new deployment will
// have a label associated with it that indicates whether or not it's using the CSI driver. Some old pods without the
// label may still be running, though. If a pod without the label is encountered then this function falls back to the
// UseCSIDriver configuration setting.
func (i *Incluster) deploymentUsesCSIDriver(ctx context.Context, externalID string) (bool, error) {
	var usesCSIDriver bool

	// Look up the deployment.
	customLabels := map[string]string{"external-id": externalID}
	deploymentList, err := i.deploymentList(ctx, i.ViceNamespace, customLabels, []string{})
	if err != nil {
		return usesCSIDriver, errors.Wrap(err, "unable to determine if the deployment uses the iRODS CSI driver")
	}
	if deploymentList == nil || len(deploymentList.Items) == 0 {
		return usesCSIDriver, fmt.Errorf("no deployment found for external id: %s", externalID)
	}
	deployment := deploymentList.Items[0]

	// Determine whether the deployment uses the CSI driver.
	labelValue := deployment.Labels["use-csi-driver"]
	if labelValue == "" {
		usesCSIDriver = i.UseCSIDriver
	} else {
		usesCSIDriver = labelValue == "true"
	}

	return usesCSIDriver, nil
}

// doFileTransfer handles requests to initial file transfers for a VICE
// analysis. We only need the ID of the job, nothing is required in the
// body of the request.
func (i *Incluster) doFileTransfer(ctx context.Context, externalID, reqpath, kind string, async bool) error {
	ctx, span := otel.Tracer(otelName).Start(ctx, "doFileTransfer")
	defer span.End()

	// Determine if the deployment uses the CSI driver.
	usesCSIDriver, err := i.deploymentUsesCSIDriver(ctx, externalID)
	if err != nil {
		return err
	}

	if usesCSIDriver {
		// if we use CSI Driver, file transfer is not required.
		msg := fmt.Sprintf("%s succeeded for job %s", kind, externalID)

		log.Info(msg)

		if successerr := i.statusPublisher.Running(ctx, externalID, msg); successerr != nil {
			log.Error(successerr)
		}

		return nil
	}

	log.Infof("starting %s transfers for job %s", kind, externalID)

	// Make sure that the list of services only comes from the VICE namespace.
	svcclient := i.clientset.CoreV1().Services(i.ViceNamespace)

	// Filter the list of services so only those tagged with an external-id are
	// returned. external-id is the job ID assigned by the apps service and is
	// not the same as the analysis ID.
	set := labels.Set(map[string]string{
		"external-id": externalID,
	})

	svclist, err := svcclient.List(ctx, metav1.ListOptions{
		LabelSelector: set.AsSelector().String(),
	})
	if err != nil {
		return err
	}

	if len(svclist.Items) < 1 {
		return fmt.Errorf("no services with a label of 'external-id=%s' were found", externalID)
	}

	// It's technically possibly for multiple services to provide file transfer services,
	// so we should block until all of them are complete. We're using a WaitGroup to
	// coordinate the file transfers, since they occur in separate goroutines.
	var wg sync.WaitGroup

	for _, svc := range svclist.Items {

		if !async {
			wg.Add(1)
		}

		go func(ctx context.Context, svc apiv1.Service) {
			separatedSpanContext := trace.SpanContextFromContext(ctx)
			outerCtx := trace.ContextWithSpanContext(context.Background(), separatedSpanContext)
			ctx, span := otel.Tracer(otelName).Start(outerCtx, "service iteration")
			defer span.End()

			if !async {
				defer wg.Done()
			}

			log.Infof("%s transfer for %s", kind, externalID)

			transferObj, xfererr := requestTransfer(ctx, svc, reqpath)
			if xfererr != nil {
				log.Error(xfererr)
				err = xfererr
				return
			}

			currentStatus := transferObj.Status

			var (
				sentUploadStatus   = false
				sentDownloadStatus = false
			)

			for !isFinished(currentStatus) {
				// Set it again here to catch the new values set farther down.
				currentStatus = transferObj.Status

				switch currentStatus {
				case FailedStatus:
					msg := fmt.Sprintf("%s failed for job %s", kind, externalID)

					err = errors.New(msg)

					log.Error(err)

					if failerr := i.statusPublisher.Running(ctx, externalID, msg); failerr != nil {
						log.Error(failerr)
					}

					return
				case CompletedStatus:
					msg := fmt.Sprintf("%s succeeded for job %s", kind, externalID)

					log.Info(msg)

					if successerr := i.statusPublisher.Running(ctx, externalID, msg); successerr != nil {
						log.Error(successerr)
					}

					return
				case RequestedStatus:
					msg := fmt.Sprintf("%s requested for job %s", kind, externalID)

					if requestederr := i.statusPublisher.Running(ctx, externalID, msg); requestederr != nil {
						log.Error(err)
					}

				case UploadingStatus:
					if !sentUploadStatus {
						msg := fmt.Sprintf("%s is in progress for job %s", kind, externalID)

						log.Info(msg)

						if uploadingerr := i.statusPublisher.Running(ctx, externalID, msg); uploadingerr != nil {
							log.Error(err)
						}

						sentUploadStatus = true
					}
				case DownloadingStatus:
					if !sentDownloadStatus {
						msg := fmt.Sprintf("%s is in progress for job %s", kind, externalID)

						log.Info(msg)

						if downloadingerr := i.statusPublisher.Running(ctx, externalID, msg); downloadingerr != nil {
							log.Error(err)
						}

						sentDownloadStatus = true
					}
				default:
					err = fmt.Errorf("unknown status from %s: %s", svc.Spec.ClusterIP, transferObj.Status)

					log.Error(err)

					return // return and not break because we want to fail out
				}

				fullreqpath := path.Join(reqpath, transferObj.UUID)

				transferObj, xfererr = getTransferDetails(ctx, svc, fullreqpath)
				if xfererr != nil {
					log.Error(errors.Wrapf(xfererr, "error getting transfer details for transferObj %s", fullreqpath))
					err = xfererr
					return
				}

				if transferObj == nil {
					log.Error("transferObj is nil")
					return
				}

				time.Sleep(5 * time.Second)
			}
		}(ctx, svc)
	}

	// Block until all of the file transfers are complete. There usually will only
	// be a single goroutine to wait for, but we should support more.
	if !async {
		wg.Wait()
	}

	return err
}
