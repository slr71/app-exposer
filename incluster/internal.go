package incluster

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cyverse-de/app-exposer/apps"
	"github.com/cyverse-de/app-exposer/common"
	"github.com/cyverse-de/app-exposer/permissions"
	"github.com/cyverse-de/app-exposer/quota"

	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/cyverse-de/model/v8"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"github.com/labstack/echo/v4"
)

var log = common.Log
var httpClient = http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}
var otelName = "github.com/cyverse-de/app-exposer/incluster"

// Init contains configuration for configuring an *Incluster.
type Init struct {
	PorklockImage                 string
	PorklockTag                   string
	UseCSIDriver                  bool
	InputPathListIdentifier       string
	TicketInputPathListIdentifier string
	ImagePullSecretName           string
	ViceProxyImage                string
	FrontendBaseURL               string
	ViceDefaultBackendService     string
	ViceDefaultBackendServicePort int
	GetAnalysisIDService          string
	CheckResourceAccessService    string
	VICEBackendNamespace          string
	AppsServiceBaseURL            string
	ViceNamespace                 string
	JobStatusURL                  string
	UserSuffix                    string
	PermissionsURL                string
	KeycloakBaseURL               string
	KeycloakRealm                 string
	KeycloakClientID              string
	KeycloakClientSecret          string
	IRODSZone                     string
	IngressClass                  string
	NATSEncodedConn               *nats.EncodedConn
}

// Incluster contains information and operations for launching VICE apps inside the
// local k8s cluster.
type Incluster struct {
	Init
	clientset       kubernetes.Interface
	db              *sqlx.DB
	statusPublisher AnalysisStatusPublisher
	apps            *apps.Apps
	quotaEnforcer   *quota.Enforcer
}

// New creates a new *Incluster.
func New(init *Init, db *sqlx.DB, clientset kubernetes.Interface, apps *apps.Apps) *Incluster {
	return &Incluster{
		Init:      *init,
		db:        db,
		clientset: clientset,
		statusPublisher: &JSLPublisher{
			statusURL: init.JobStatusURL,
		},
		apps:          apps,
		quotaEnforcer: quota.NewEnforcer(clientset, db, init.NATSEncodedConn, init.UserSuffix),
	}
}

// labelsFromJob returns a map[string]string that can be used as labels for K8s resources.
func (i *Incluster) labelsFromJob(ctx context.Context, job *model.Job) (map[string]string, error) {
	name := []rune(job.Name)

	var stringmax int
	if len(name) >= 63 {
		stringmax = 62
	} else {
		stringmax = len(name) - 1
	}

	ipAddr, err := i.apps.GetUserIP(ctx, job.UserID)
	if err != nil {
		return nil, err
	}

	return map[string]string{
		"external-id":    job.InvocationID,
		"app-name":       common.LabelValueString(job.AppName),
		"app-id":         job.AppID,
		"username":       common.LabelValueString(job.Submitter),
		"user-id":        job.UserID,
		"analysis-name":  common.LabelValueString(string(name[:stringmax])),
		"app-type":       "interactive",
		"subdomain":      IngressName(job.UserID, job.InvocationID),
		"login-ip":       ipAddr,
		"use-csi-driver": fmt.Sprintf("%t", i.UseCSIDriver && job.MountDataStore),
	}, nil
}

// UpsertExcludesConfigMap uses the Job passed in to assemble the ConfigMap
// containing the files that should not be uploaded to iRODS. It then calls
// the k8s API to create the ConfigMap if it does not already exist or to
// update it if it does.
func (i *Incluster) UpsertExcludesConfigMap(ctx context.Context, job *model.Job) error {
	excludesCM, err := i.excludesConfigMap(ctx, job)
	if err != nil {
		return err
	}

	cmclient := i.clientset.CoreV1().ConfigMaps(i.ViceNamespace)

	_, err = cmclient.Get(ctx, excludesConfigMapName(job), metav1.GetOptions{})
	if err != nil {
		log.Info(err)
		_, err = cmclient.Create(ctx, excludesCM, metav1.CreateOptions{})
		if err != nil {
			return err
		}
	} else {
		_, err = cmclient.Update(ctx, excludesCM, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
	}
	return nil
}

// UpsertInputPathListConfigMap uses the Job passed in to assemble the ConfigMap
// containing the path list of files to download from iRODS for the VICE analysis.
// It then uses the k8s API to create the ConfigMap if it does not already exist or to
// update it if it does.
func (i *Incluster) UpsertInputPathListConfigMap(ctx context.Context, job *model.Job) error {
	inputCM, err := i.inputPathListConfigMap(ctx, job)
	if err != nil {
		return err
	}

	cmclient := i.clientset.CoreV1().ConfigMaps(i.ViceNamespace)

	_, err = cmclient.Get(ctx, inputPathListConfigMapName(job), metav1.GetOptions{})
	if err != nil {
		_, err = cmclient.Create(ctx, inputCM, metav1.CreateOptions{})
		if err != nil {
			return err
		}
	} else {
		_, err = cmclient.Update(ctx, inputCM, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
	}

	return nil
}

// UpsertDeployment uses the Job passed in to assemble a Deployment for the
// VICE analysis. If then uses the k8s API to create the Deployment if it does
// not already exist or to update it if it does.
func (i *Incluster) UpsertDeployment(ctx context.Context, deployment *appsv1.Deployment, job *model.Job) error {
	var err error
	depclient := i.clientset.AppsV1().Deployments(i.ViceNamespace)

	_, err = depclient.Get(ctx, job.InvocationID, metav1.GetOptions{})
	if err != nil {
		_, err = depclient.Create(ctx, deployment, metav1.CreateOptions{})
		if err != nil {
			return err
		}
	} else {
		_, err = depclient.Update(ctx, deployment, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
	}

	// Create the persistent volumes and persistent volume claims for the job.
	volumes, err := i.getPersistentVolumes(ctx, job)
	if err != nil {
		return err
	}

	volumeclaims, err := i.getPersistentVolumeClaims(ctx, job)
	if err != nil {
		return err
	}

	if len(volumes) > 0 {
		pvclient := i.clientset.CoreV1().PersistentVolumes()

		for _, volume := range volumes {
			_, err = pvclient.Get(ctx, volume.GetName(), metav1.GetOptions{})
			if err != nil {
				_, err = pvclient.Create(ctx, volume, metav1.CreateOptions{})
				if err != nil {
					return err
				}
			} else {
				_, err = pvclient.Update(ctx, volume, metav1.UpdateOptions{})
				if err != nil {
					return err
				}
			}
		}
	}

	if len(volumeclaims) > 0 {
		pvcclient := i.clientset.CoreV1().PersistentVolumeClaims(i.ViceNamespace)

		for _, volumeClaim := range volumeclaims {
			_, err = pvcclient.Get(ctx, volumeClaim.GetName(), metav1.GetOptions{})
			if err != nil {
				_, err = pvcclient.Create(ctx, volumeClaim, metav1.CreateOptions{})
				if err != nil {
					return err
				}
			} else {
				_, err = pvcclient.Update(ctx, volumeClaim, metav1.UpdateOptions{})
				if err != nil {
					return err
				}
			}
		}
	}

	// Create the pod disruption budget for the job.
	pdb, err := i.createPodDisruptionBudget(ctx, job)
	if err != nil {
		return err
	}
	pdbClient := i.clientset.PolicyV1().PodDisruptionBudgets(i.ViceNamespace)
	_, err = pdbClient.Get(ctx, job.InvocationID, metav1.GetOptions{})
	if err != nil {
		_, err = pdbClient.Create(ctx, pdb, metav1.CreateOptions{})
		if err != nil {
			return err
		}
	}

	// Create the service for the job.
	svc, err := i.getService(ctx, job)
	if err != nil {
		return err
	}
	svcclient := i.clientset.CoreV1().Services(i.ViceNamespace)
	_, err = svcclient.Get(ctx, job.InvocationID, metav1.GetOptions{})
	if err != nil {
		_, err = svcclient.Create(ctx, svc, metav1.CreateOptions{})
		if err != nil {
			return err
		}
	}

	// Create the ingress for the job
	ingress, err := i.getIngress(ctx, job, svc, i.Init.IngressClass)
	if err != nil {
		return err
	}

	ingressclient := i.clientset.NetworkingV1().Ingresses(i.ViceNamespace)
	_, err = ingressclient.Get(ctx, ingress.Name, metav1.GetOptions{})
	if err != nil {
		_, err = ingressclient.Create(ctx, ingress, metav1.CreateOptions{})
		if err != nil {
			return err
		}
	}

	return nil
}

func getMillicoresFromDeployment(deployment *appsv1.Deployment) (*apd.Decimal, error) {
	var (
		analysisContainer *apiv1.Container
		millicores        *apd.Decimal
		millicoresString  string
		err               error
	)
	containers := deployment.Spec.Template.Spec.Containers

	found := false

	for _, container := range containers {
		if container.Name == analysisContainerName {
			analysisContainer = &container
			found = true
			break
		}
	}

	if !found {
		return nil, errors.New("could not find the analysis container in the deployment")
	}

	millicoresString = analysisContainer.Resources.Limits[apiv1.ResourceCPU].ToUnstructured().(string)

	millicores, _, err = apd.NewFromString(millicoresString)
	if err != nil {
		return nil, err
	}

	millicoresPerCPU := apd.New(1000, 0)

	_, err = apd.BaseContext.Mul(millicores, millicores, millicoresPerCPU)
	if err != nil {
		return nil, err
	}

	log.Debugf("%s millicores reservation found", millicores.String())

	return millicores, nil
}

// LaunchAppHandler is the HTTP handler that orchestrates the launching of a VICE analysis inside
// the k8s cluster. This get passed to the router to be associated with a route. The Job
// is passed in as the body of the request.
func (i *Incluster) LaunchAppHandler(c echo.Context) error {
	var (
		job *model.Job
		err error
	)

	ctx := c.Request().Context()

	job = &model.Job{}

	if err = c.Bind(job); err != nil {
		return err
	}

	if status, err := i.quotaEnforcer.ValidateJob(ctx, job, i.ViceNamespace); err != nil {
		if validationErr, ok := err.(common.ErrorResponse); ok {
			return validationErr
		}
		return echo.NewHTTPError(status, err.Error())
	}

	// Create the excludes file ConfigMap for the job.
	if err = i.UpsertExcludesConfigMap(ctx, job); err != nil {
		return err
	}

	// Create the input path list config map
	if err = i.UpsertInputPathListConfigMap(ctx, job); err != nil {
		return err
	}

	deployment, err := i.getDeployment(ctx, job)
	if err != nil {
		return err
	}

	millicores, err := getMillicoresFromDeployment(deployment)
	if err != nil {
		return err
	}

	if err = i.apps.SetMillicoresReserved(job, millicores); err != nil {
		return err
	}

	// Create the deployment for the job.
	if err = i.UpsertDeployment(ctx, deployment, job); err != nil {
		return err
	}

	return nil
}

// TriggerDownloadsHandler handles requests to trigger file downloads.
func (i *Incluster) TriggerDownloadsHandler(c echo.Context) error {
	return i.doFileTransfer(c.Request().Context(), c.Param("id"), downloadBasePath, downloadKind, true)
}

// AdminTriggerDownloadsHandler handles requests to trigger file downloads
// without requiring user information in the request and also operates from
// the analysis UUID rather than the external ID. For use with tools that
// require the caller to have administrative privileges.
func (i *Incluster) AdminTriggerDownloadsHandler(c echo.Context) error {
	var err error
	ctx := c.Request().Context()

	analysisID := c.Param("analysis-id")

	externalID, err := i.getExternalIDByAnalysisID(ctx, analysisID)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	return i.doFileTransfer(ctx, externalID, downloadBasePath, downloadKind, true)
}

// TriggerUploadsHandler handles requests to trigger file uploads.
func (i *Incluster) TriggerUploadsHandler(c echo.Context) error {
	return i.doFileTransfer(c.Request().Context(), c.Param("id"), uploadBasePath, uploadKind, true)
}

// AdminTriggerUploadsHandler handles requests to trigger file uploads without
// requiring user information in the request, while also operating from the
// analysis UUID rather than the external UUID. For use with tools that
// require the caller to have administrative privileges.
func (i *Incluster) AdminTriggerUploadsHandler(c echo.Context) error {
	var err error
	ctx := c.Request().Context()

	analysisID := c.Param("analysis-id")

	externalID, err := i.getExternalIDByAnalysisID(ctx, analysisID)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	return i.doFileTransfer(ctx, externalID, uploadBasePath, uploadKind, true)
}

func (i *Incluster) doExit(ctx context.Context, externalID string) error {
	set := labels.Set(map[string]string{
		"external-id": externalID,
	})

	listoptions := metav1.ListOptions{
		LabelSelector: set.AsSelector().String(),
	}

	// Delete the pod disruption budget
	pdbClient := i.clientset.PolicyV1().PodDisruptionBudgets(i.ViceNamespace)
	pdbList, err := pdbClient.List(ctx, listoptions)
	if err != nil {
		return err
	}

	for _, pdb := range pdbList.Items {
		if err = pdbClient.Delete(ctx, pdb.Name, metav1.DeleteOptions{}); err != nil {
			log.Error(err)
		}
	}

	// Delete the ingress
	ingressclient := i.clientset.NetworkingV1().Ingresses(i.ViceNamespace)
	ingresslist, err := ingressclient.List(ctx, listoptions)
	if err != nil {
		return err
	}

	for _, ingress := range ingresslist.Items {
		if err = ingressclient.Delete(ctx, ingress.Name, metav1.DeleteOptions{}); err != nil {
			log.Error(err)
		}
	}

	// Delete the service
	svcclient := i.clientset.CoreV1().Services(i.ViceNamespace)
	svclist, err := svcclient.List(ctx, listoptions)
	if err != nil {
		return err
	}

	for _, svc := range svclist.Items {
		if err = svcclient.Delete(ctx, svc.Name, metav1.DeleteOptions{}); err != nil {
			log.Error(err)
		}
	}

	// Delete the deployment
	depclient := i.clientset.AppsV1().Deployments(i.ViceNamespace)
	deplist, err := depclient.List(ctx, listoptions)
	if err != nil {
		return err
	}

	for _, dep := range deplist.Items {
		if err = depclient.Delete(ctx, dep.Name, metav1.DeleteOptions{}); err != nil {
			log.Error(err)
		}
	}

	// Delete volumes used by the deployment
	// Delete persistent volume claims.
	// This will automatically delete persistent volumes associated with them.
	pvcclient := i.clientset.CoreV1().PersistentVolumeClaims(i.ViceNamespace)
	pvclist, err := pvcclient.List(ctx, listoptions)
	if err != nil {
		return err
	}

	for _, pvc := range pvclist.Items {
		if err = pvcclient.Delete(ctx, pvc.Name, metav1.DeleteOptions{}); err != nil {
			log.Error(err)
		}
	}

	// Persistent volumes with "Retain" reclaim policy should be deleted manually
	// Persistent volumes created via CSI Driver only supports "Retain" reclaim policy
	pvclient := i.clientset.CoreV1().PersistentVolumes()
	pvlist, err := pvclient.List(ctx, listoptions)
	if err != nil {
		return err
	}

	for _, pv := range pvlist.Items {
		if err = pvclient.Delete(ctx, pv.Name, metav1.DeleteOptions{}); err != nil {
			log.Error(err)
		}
	}

	// Delete the input files list and the excludes list config maps
	cmclient := i.clientset.CoreV1().ConfigMaps(i.ViceNamespace)
	cmlist, err := cmclient.List(ctx, listoptions)
	if err != nil {
		return err
	}

	log.Infof("number of configmaps to be deleted for %s: %d", externalID, len(cmlist.Items))

	for _, cm := range cmlist.Items {
		log.Infof("deleting configmap %s for %s", cm.Name, externalID)
		if err = cmclient.Delete(ctx, cm.Name, metav1.DeleteOptions{}); err != nil {
			log.Error(err)
		}
	}

	return nil
}

// ExitHandler terminates the VICE analysis deployment and cleans up
// resources asscociated with it. Does not save outputs first. Uses
// the external-id label to find all of the objects in the configured
// namespace associated with the job. Deletes the following objects:
// ingresses, services, deployments, and configmaps.
func (i *Incluster) ExitHandler(c echo.Context) error {
	return i.doExit(c.Request().Context(), c.Param("id"))
}

// AdminExitHandler terminates the VICE analysis based on the analysisID and
// and should not require any user information to be provided. Otherwise, the
// documentation for VICEExit applies here as well.
func (i *Incluster) AdminExitHandler(c echo.Context) error {
	var err error
	ctx := c.Request().Context()

	analysisID := c.Param("analysis-id")

	externalID, err := i.getExternalIDByAnalysisID(ctx, analysisID)
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}

	return i.doExit(ctx, externalID)
}

// getIDFromHost returns the external ID for the running VICE app, which
// is assumed to be the same as the name of the ingress.
func (i *Incluster) getIDFromHost(ctx context.Context, host string) (string, error) {
	ingressclient := i.clientset.NetworkingV1().Ingresses(i.ViceNamespace)
	ingresslist, err := ingressclient.List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", err
	}

	for _, ingress := range ingresslist.Items {
		for _, rule := range ingress.Spec.Rules {
			if rule.Host == host {
				return ingress.Name, nil
			}
		}
	}

	return "", fmt.Errorf("no ingress found for host %s", host)
}

// URLReadyHandler returns whether or not a VICE app is ready
// for users to access it. This version will check the user's permissions
// and return an error if they aren't allowed to access the running app.
func (i *Incluster) URLReadyHandler(c echo.Context) error {
	var (
		ingressExists bool
		serviceExists bool
		podReady      bool
	)

	ctx := c.Request().Context()

	user := c.QueryParam("user")
	if user == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "user query parameter must be set")
	}

	// Since some usernames don't come through the labelling process unscathed, we have to use
	// the user ID.
	fixedUser := i.fixUsername(user)
	_, err := i.apps.GetUserID(ctx, fixedUser)
	if err != nil {
		if err == sql.ErrNoRows {
			return echo.NewHTTPError(http.StatusNotFound, fmt.Sprintf("user %s not found", fixedUser))
		}
		return err
	}

	host := c.Param("host")

	// Use the name of the ingress to retrieve the externalID
	id, err := i.getIDFromHost(ctx, host)
	if err != nil {
		return err
	}

	// If getIDFromHost returns without an error, then the ingress exists
	// since the ingresses are looked at for the host.
	ingressExists = true

	set := labels.Set(map[string]string{
		"external-id": id,
	})

	listoptions := metav1.ListOptions{
		LabelSelector: set.AsSelector().String(),
	}

	// check the service existence
	svcclient := i.clientset.CoreV1().Services(i.ViceNamespace)
	svclist, err := svcclient.List(ctx, listoptions)
	if err != nil {
		return err
	}
	if len(svclist.Items) > 0 {
		serviceExists = true
	}

	// Check pod status through the deployment
	depclient := i.clientset.AppsV1().Deployments(i.ViceNamespace)
	deplist, err := depclient.List(ctx, listoptions)
	if err != nil {
		return err
	}
	for _, dep := range deplist.Items {
		if dep.Status.ReadyReplicas > 0 {
			podReady = true
		}
	}

	data := map[string]bool{
		"ready": ingressExists && serviceExists && podReady,
	}

	analysisID, err := i.apps.GetAnalysisIDByExternalID(ctx, id)
	if err != nil {
		return err
	}

	// Make sure the user has permissions to look up info about this analysis.
	p := &permissions.Permissions{
		BaseURL: i.PermissionsURL,
	}

	allowed, err := p.IsAllowed(ctx, user, analysisID)
	if err != nil {
		return err
	}

	if !allowed {
		return echo.NewHTTPError(http.StatusForbidden, fmt.Sprintf("user %s cannot access analysis %s", user, analysisID))
	}

	return c.JSON(http.StatusOK, data)
}

// AdminURLReadyHandler handles requests to check the status of a running VICE app in K8s.
// This will return an overall status and status for the individual containers in
// the app's pod. Uses the state of the readiness checks in K8s, along with the
// existence of the various resources created for the app.
func (i *Incluster) AdminURLReadyHandler(c echo.Context) error {
	var (
		ingressExists bool
		serviceExists bool
		podReady      bool
	)

	ctx := c.Request().Context()
	host := c.Param("host")

	// Use the name of the ingress to retrieve the externalID
	id, err := i.getIDFromHost(ctx, host)
	if err != nil {
		return echo.NewHTTPError(http.StatusNotFound, err.Error())
	}

	// If getIDFromHost returns without an error, then the ingress exists
	// since the ingresses are looked at for the host.
	ingressExists = true

	set := labels.Set(map[string]string{
		"external-id": id,
	})

	listoptions := metav1.ListOptions{
		LabelSelector: set.AsSelector().String(),
	}

	// check the service existence
	svcclient := i.clientset.CoreV1().Services(i.ViceNamespace)
	svclist, err := svcclient.List(ctx, listoptions)
	if err != nil {
		return err
	}
	if len(svclist.Items) > 0 {
		serviceExists = true
	}

	// Check pod status through the deployment
	depclient := i.clientset.AppsV1().Deployments(i.ViceNamespace)
	deplist, err := depclient.List(ctx, listoptions)
	if err != nil {
		return err
	}
	for _, dep := range deplist.Items {
		if dep.Status.ReadyReplicas > 0 {
			podReady = true
		}
	}

	data := map[string]bool{
		"ready": ingressExists && serviceExists && podReady,
	}

	return c.JSON(http.StatusOK, data)
}

// SaveAndExitHandler handles requests to save the output files in iRODS and then exit.
// The exit portion will only occur if the save operation succeeds. The operation is
// performed inside of a goroutine so that the caller isn't waiting for hours/days for
// output file transfers to complete.
func (i *Incluster) SaveAndExitHandler(c echo.Context) error {
	log.Info("save and exit called")

	// Since file transfers can take a while, we should do this asynchronously by default.
	go func(ctx context.Context, c echo.Context) {
		var err error
		separatedSpanContext := trace.SpanContextFromContext(ctx)
		outerCtx := trace.ContextWithSpanContext(context.Background(), separatedSpanContext)
		ctx, span := otel.Tracer(otelName).Start(outerCtx, "SaveAndExitHandler goroutine")
		defer span.End()

		externalID := c.Param("id")

		log.Infof("calling doFileTransfer for %s", externalID)

		// Trigger a blocking output file transfer request.
		if err = i.doFileTransfer(ctx, externalID, uploadBasePath, uploadKind, false); err != nil {
			log.Error(errors.Wrap(err, "error doing file transfer")) // Log but don't exit. Possible to cancel a job that hasn't started yet
		}

		log.Infof("calling VICEExit for %s", externalID)

		if err = i.doExit(ctx, externalID); err != nil {
			log.Error(errors.Wrapf(err, "error triggering analysis exit for %s", externalID))
		}

		log.Infof("after VICEExit for %s", externalID)
	}(c.Request().Context(), c)

	log.Info("leaving save and exit")

	return nil
}

// AdminSaveAndExitHandler handles requests to save the output files in iRODS and
// then exit. This version of the call operates based on the analysis ID and does
// not require user information to be required by the caller. Otherwise, the docs
// for the VICESaveAndExit function apply here as well.
func (i *Incluster) AdminSaveAndExitHandler(c echo.Context) error {
	log.Info("admin save and exit called")

	// Since file transfers can take a while, we should do this asynchronously by default.
	go func(ctx context.Context, c echo.Context) {
		var (
			err        error
			externalID string
		)

		separatedSpanContext := trace.SpanContextFromContext(ctx)
		outerCtx := trace.ContextWithSpanContext(context.Background(), separatedSpanContext)
		ctx, span := otel.Tracer(otelName).Start(outerCtx, "AdminSaveAndExitHandler goroutine")
		defer span.End()

		log.Debug("calling doFileTransfer")

		analysisID := c.Param("analysis-id")

		if externalID, err = i.getExternalIDByAnalysisID(ctx, analysisID); err != nil {
			log.Error(err)
			return
		}

		// Trigger a blocking output file transfer request.
		if err = i.doFileTransfer(ctx, externalID, uploadBasePath, uploadKind, false); err != nil {
			log.Error(errors.Wrap(err, "error doing file transfer")) // Log but don't exit. Possible to cancel a job that hasn't started yet
		}

		log.Debug("calling VICEExit")

		if err = i.doExit(ctx, externalID); err != nil {
			log.Error(err)
		}

		log.Debug("after VICEExit")
	}(c.Request().Context(), c)

	log.Info("admin leaving save and exit")
	return nil
}

const updateTimeLimitSQL = `
	UPDATE ONLY jobs
	   SET planned_end_date = old_value.planned_end_date + interval '72 hours'
	  FROM (SELECT planned_end_date FROM jobs WHERE id = $2) AS old_value
	 WHERE jobs.id = $2
	   AND jobs.user_id = $1
 RETURNING jobs.planned_end_date
`

const getTimeLimitSQL = `
	SELECT planned_end_date
	  FROM jobs
	 WHERE jobs.id = $2
	   AND jobs.user_id = $1
`

const getUserIDSQL = `
	SELECT users.id
	  FROM users
	 WHERE username = $1
`

// TimeLimitUpdateHandler handles requests to update the time limit on an already running VICE app.
func (i *Incluster) TimeLimitUpdateHandler(c echo.Context) error {
	ctx := c.Request().Context()
	log.Info("update time limit called")

	var (
		err  error
		id   string
		user string
	)

	// user is required
	user = c.QueryParam("user")
	if user == "" {
		return echo.NewHTTPError(http.StatusForbidden, "user is not set")
	}

	// id is required
	id = c.Param("analysis-id")
	if id == "" {
		idErr := echo.NewHTTPError(http.StatusBadRequest, "id parameter is empty")
		log.Error(idErr)
		return idErr
	}

	outputMap, err := i.updateTimeLimit(ctx, user, id)
	if err != nil {
		log.Error(err)
		return err
	}

	return c.JSON(http.StatusOK, outputMap)

}

// AdminTimeLimitUpdateHandler is basically the same as VICETimeLimitUpdate
// except that it doesn't require user information in the request.
func (i *Incluster) AdminTimeLimitUpdateHandler(c echo.Context) error {
	ctx := c.Request().Context()
	var (
		err  error
		id   string
		user string
	)
	// id is required
	id = c.Param("analysis-id")
	if id == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "id parameter is empty")
	}

	user, _, err = i.apps.GetUserByAnalysisID(ctx, id)
	if err != nil {
		return err
	}

	outputMap, err := i.updateTimeLimit(ctx, user, id)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, outputMap)
}

// GetTimeLimitHandler implements the handler for getting the current time limit from the database.
func (i *Incluster) GetTimeLimitHandler(c echo.Context) error {
	ctx := c.Request().Context()
	log.Info("get time limit called")

	var (
		err        error
		analysisID string
		user       string
		userID     string
	)

	// user is required
	user = c.QueryParam("user")
	if user == "" {
		return echo.NewHTTPError(http.StatusForbidden, "user is not set")
	}

	// analysisID is required
	analysisID = c.Param("analysis-id")
	if analysisID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "id parameter is empty")
	}

	// Could use this to get the username, but we need to not break other services.
	_, userID, err = i.apps.GetUserByAnalysisID(ctx, analysisID)
	if err != nil {
		return err
	}

	outputMap, err := i.getTimeLimit(ctx, userID, analysisID)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, outputMap)
}

// AdminGetTimeLimitHandler is the same as VICEGetTimeLimit but doesn't require
// any user information in the request.
func (i *Incluster) AdminGetTimeLimitHandler(c echo.Context) error {
	ctx := c.Request().Context()
	log.Info("get time limit called")

	var (
		err        error
		analysisID string
		userID     string
	)

	// analysisID is required
	analysisID = c.Param("analysis-id")
	if analysisID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "id parameter is empty")
	}

	// Could use this to get the username, but we need to not break other services.
	_, userID, err = i.apps.GetUserByAnalysisID(ctx, analysisID)
	if err != nil {
		return err
	}

	outputMap, err := i.getTimeLimit(ctx, userID, analysisID)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, outputMap)
}

func dbTimestampToLocal(t time.Time) time.Time {
	_, offset := time.Now().Zone()
	t = t.Local()
	t = t.Add(time.Duration(offset*-1) * time.Second)
	return t
}

func (i *Incluster) getTimeLimit(ctx context.Context, userID, id string) (map[string]string, error) {
	var err error

	var timeLimit pq.NullTime
	if err = i.db.QueryRowContext(ctx, getTimeLimitSQL, userID, id).Scan(&timeLimit); err != nil {
		return nil, errors.Wrapf(err, "error retrieving time limit for user %s on analysis %s", userID, id)
	}

	outputMap := map[string]string{}
	if timeLimit.Valid {
		v, err := timeLimit.Value()
		if err != nil {
			return nil, errors.Wrapf(err, "error getting time limit for user %s on analysis %s", userID, id)
		}
		outputMap["time_limit"] = fmt.Sprintf("%d", dbTimestampToLocal(v.(time.Time)).Unix())
	} else {
		outputMap["time_limit"] = "null"
	}

	return outputMap, nil
}

func (i *Incluster) updateTimeLimit(ctx context.Context, user, id string) (map[string]string, error) {
	var (
		err    error
		userID string
	)

	if !strings.HasSuffix(user, userSuffix) {
		user = fmt.Sprintf("%s%s", user, userSuffix)
	}

	if err = i.db.QueryRowContext(ctx, getUserIDSQL, user).Scan(&userID); err != nil {
		return nil, errors.Wrapf(err, "error looking user ID for %s", user)
	}

	var newTimeLimit pq.NullTime
	if err = i.db.QueryRowContext(ctx, updateTimeLimitSQL, userID, id).Scan(&newTimeLimit); err != nil {
		return nil, errors.Wrapf(err, "error extending time limit for user %s on analysis %s", userID, id)
	}

	outputMap := map[string]string{}
	if newTimeLimit.Valid {
		v, err := newTimeLimit.Value()
		if err != nil {
			return nil, errors.Wrapf(err, "error getting new time limit for user %s on analysis %s", userID, id)
		}
		outputMap["time_limit"] = fmt.Sprintf("%d", dbTimestampToLocal(v.(time.Time)).Unix())
	} else {
		return nil, errors.Wrapf(err, "the time limit for analysis %s was null after extension", id)
	}

	return outputMap, nil
}

// AdminGetExternalIDHandler returns the external ID associated with the analysis ID.
// There is only one external ID for each VICE analysis, unlike non-VICE analyses.
func (i *Incluster) AdminGetExternalIDHandler(c echo.Context) error {
	var (
		err        error
		analysisID string
		externalID string
	)

	ctx := c.Request().Context()

	// analysisID is required
	analysisID = c.Param("analysis-id")
	if analysisID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "id parameter is empty")
	}

	externalID, err = i.getExternalIDByAnalysisID(ctx, analysisID)
	if err != nil {
		return err
	}

	outputMap := map[string]string{
		"externalID": externalID,
	}

	return c.JSON(http.StatusOK, outputMap)
}
