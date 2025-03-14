package incluster

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"strings"

	"github.com/cyverse-de/app-exposer/apps"
	"github.com/cyverse-de/app-exposer/common"
	"github.com/cyverse-de/app-exposer/permissions"
	"github.com/labstack/echo/v4"
	"github.com/pkg/errors"
	v1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
)

func getListSelector(customLabels map[string]string) labels.Selector {
	allLabels := map[string]string{
		"app-type": "interactive",
	}
	maps.Copy(allLabels, customLabels)

	set := labels.Set(allLabels)

	return set.AsSelector()
}

// getListOptions returns a ListOptions for listing a resource that has the
// labels provided in customLabels, but is missing the labels provided in missingLabels.
func getListOptions(customLabels map[string]string, missingLabels []string) metav1.ListOptions {
	// Get the selector populated with the labels that should be present
	s := getListSelector(customLabels)

	// the list of requirements for labels that should be missing from the objects
	// in the listing.
	reqs := []labels.Requirement{}

	// populate the requirements
	for _, missingLabel := range missingLabels {
		newReq, err := labels.NewRequirement(missingLabel, selection.DoesNotExist, []string{})
		if err != nil {
			log.Error(err)
		} else {
			reqs = append(reqs, *newReq)
		}
	}

	s = s.Add(reqs...)

	return metav1.ListOptions{
		LabelSelector: s.String(),
	}
}

func (i *Incluster) deploymentList(ctx context.Context, namespace string, customLabels map[string]string, missingLabels []string) (*v1.DeploymentList, error) {
	listOptions := getListOptions(customLabels, missingLabels)

	depList, err := i.clientset.AppsV1().Deployments(namespace).List(ctx, listOptions)
	if err != nil {
		return nil, err
	}

	return depList, nil
}

func (i *Incluster) podList(ctx context.Context, namespace string, customLabels map[string]string, missingLabels []string) (*corev1.PodList, error) {
	listOptions := getListOptions(customLabels, missingLabels)

	podList, err := i.clientset.CoreV1().Pods(namespace).List(ctx, listOptions)
	if err != nil {
		return nil, err
	}

	return podList, nil
}

func (i *Incluster) configmapsList(ctx context.Context, namespace string, customLabels map[string]string, missingLabels []string) (*corev1.ConfigMapList, error) {
	listOptions := getListOptions(customLabels, missingLabels)

	cfgList, err := i.clientset.CoreV1().ConfigMaps(namespace).List(ctx, listOptions)
	if err != nil {
		return nil, err
	}

	return cfgList, nil
}

func (i *Incluster) serviceList(ctx context.Context, namespace string, customLabels map[string]string, missingLabels []string) (*corev1.ServiceList, error) {
	listOptions := getListOptions(customLabels, missingLabels)

	svcList, err := i.clientset.CoreV1().Services(namespace).List(ctx, listOptions)
	if err != nil {
		return nil, err
	}

	return svcList, nil
}

func (i *Incluster) ingressList(ctx context.Context, namespace string, customLabels map[string]string, missingLabels []string) (*netv1.IngressList, error) {
	listOptions := getListOptions(customLabels, missingLabels)

	client := i.clientset.NetworkingV1().Ingresses(namespace)
	ingList, err := client.List(ctx, listOptions)
	if err != nil {
		return nil, err
	}

	return ingList, nil
}

func filterMap(values url.Values) map[string]string {
	q := map[string]string{}

	for k, v := range values {
		q[k] = v[0]
	}

	return q
}

// MetaInfo contains useful information provided by multiple resource types.
type MetaInfo struct {
	Name              string `json:"name"`
	Namespace         string `json:"namespace"`
	AnalysisName      string `json:"analysisName"`
	AppName           string `json:"appName"`
	AppID             string `json:"appID"`
	ExternalID        string `json:"externalID"`
	UserID            string `json:"userID"`
	Username          string `json:"username"`
	CreationTimestamp string `json:"creationTimestamp"`
}

// DeploymentInfo contains information returned about a Deployment.
type DeploymentInfo struct {
	MetaInfo
	Image   string   `json:"image"`
	Command []string `json:"command"`
	Port    int32    `json:"port"`
	User    int64    `json:"user"`
	Group   int64    `json:"group"`
}

func deploymentInfo(deployment *v1.Deployment) *DeploymentInfo {
	var (
		user    int64
		group   int64
		image   string
		port    int32
		command []string
	)

	labels := deployment.GetObjectMeta().GetLabels()
	containers := deployment.Spec.Template.Spec.Containers

	for _, container := range containers {
		if container.Name == "analysis" {
			image = container.Image
			command = container.Command
			port = container.Ports[0].ContainerPort
			user = *container.SecurityContext.RunAsUser
			group = *container.SecurityContext.RunAsGroup
		}

	}

	return &DeploymentInfo{
		MetaInfo: MetaInfo{
			Name:              deployment.GetName(),
			Namespace:         deployment.GetNamespace(),
			AnalysisName:      labels["analysis-name"],
			AppName:           labels["app-name"],
			AppID:             labels["app-id"],
			ExternalID:        labels["external-id"],
			UserID:            labels["user-id"],
			Username:          labels["username"],
			CreationTimestamp: deployment.GetCreationTimestamp().String(),
		},

		Image:   image,
		Command: command,
		Port:    port,
		User:    user,
		Group:   group,
	}
}

// PodInfo tracks information about the pods for a VICE analysis.
type PodInfo struct {
	MetaInfo
	Phase                 string                   `json:"phase"`
	Message               string                   `json:"message"`
	Reason                string                   `json:"reason"`
	ContainerStatuses     []corev1.ContainerStatus `json:"containerStatuses"`
	InitContainerStatuses []corev1.ContainerStatus `json:"initContainerStatuses"`
}

func podInfo(pod *corev1.Pod) *PodInfo {
	labels := pod.GetObjectMeta().GetLabels()

	return &PodInfo{
		MetaInfo: MetaInfo{
			Name:              pod.GetName(),
			Namespace:         pod.GetNamespace(),
			AnalysisName:      labels["analysis-name"],
			AppName:           labels["app-name"],
			AppID:             labels["app-id"],
			ExternalID:        labels["external-id"],
			UserID:            labels["user-id"],
			Username:          labels["username"],
			CreationTimestamp: pod.GetCreationTimestamp().String(),
		},
		Phase:                 string(pod.Status.Phase),
		Message:               pod.Status.Message,
		Reason:                pod.Status.Reason,
		ContainerStatuses:     pod.Status.ContainerStatuses,
		InitContainerStatuses: pod.Status.InitContainerStatuses,
	}
}

// ConfigMapInfo contains useful info about a config map.
type ConfigMapInfo struct {
	MetaInfo
	Data map[string]string `json:"data"`
}

func configMapInfo(cm *corev1.ConfigMap) *ConfigMapInfo {
	labels := cm.GetObjectMeta().GetLabels()

	return &ConfigMapInfo{
		MetaInfo: MetaInfo{
			Name:              cm.GetName(),
			Namespace:         cm.GetNamespace(),
			AnalysisName:      labels["analysis-name"],
			AppName:           labels["app-name"],
			AppID:             labels["app-id"],
			ExternalID:        labels["external-id"],
			UserID:            labels["user-id"],
			Username:          labels["username"],
			CreationTimestamp: cm.GetCreationTimestamp().String(),
		},
		Data: cm.Data,
	}
}

// ServiceInfoPort contains information about a service's Port.
type ServiceInfoPort struct {
	Name           string `json:"name"`
	NodePort       int32  `json:"nodePort"`
	TargetPort     int32  `json:"targetPort"`
	TargetPortName string `json:"targetPortName"`
	Port           int32  `json:"port"`
	Protocol       string `json:"protocol"`
}

// ServiceInfo contains info about a service
type ServiceInfo struct {
	MetaInfo
	Ports []ServiceInfoPort `json:"ports"`
}

func serviceInfo(svc *corev1.Service) *ServiceInfo {
	labels := svc.GetObjectMeta().GetLabels()

	ports := svc.Spec.Ports
	svcInfoPorts := []ServiceInfoPort{}

	for _, port := range ports {
		svcInfoPorts = append(svcInfoPorts, ServiceInfoPort{
			Name:           port.Name,
			NodePort:       port.NodePort,
			TargetPort:     port.TargetPort.IntVal,
			TargetPortName: port.TargetPort.String(),
			Port:           port.Port,
			Protocol:       string(port.Protocol),
		})
	}

	return &ServiceInfo{
		MetaInfo: MetaInfo{
			Name:              svc.GetName(),
			Namespace:         svc.GetNamespace(),
			AnalysisName:      labels["analysis-name"],
			AppName:           labels["app-name"],
			AppID:             labels["app-id"],
			ExternalID:        labels["external-id"],
			UserID:            labels["user-id"],
			Username:          labels["username"],
			CreationTimestamp: svc.GetCreationTimestamp().String(),
		},

		Ports: svcInfoPorts,
	}
}

// IngressInfo contains useful Ingress VICE info.
type IngressInfo struct {
	MetaInfo
	DefaultBackend string              `json:"defaultBackend"`
	Rules          []netv1.IngressRule `json:"rules"`
}

func ingressInfo(ingress *netv1.Ingress) *IngressInfo {
	labels := ingress.GetObjectMeta().GetLabels()

	return &IngressInfo{
		MetaInfo: MetaInfo{
			Name:              ingress.GetName(),
			Namespace:         ingress.GetNamespace(),
			AnalysisName:      labels["analysis-name"],
			AppName:           labels["app-name"],
			AppID:             labels["app-id"],
			ExternalID:        labels["external-id"],
			UserID:            labels["user-id"],
			Username:          labels["username"],
			CreationTimestamp: ingress.GetCreationTimestamp().String(),
		},
		Rules: ingress.Spec.Rules,
		DefaultBackend: fmt.Sprintf(
			"%s:%d",
			ingress.Spec.DefaultBackend.Service.Name,
			ingress.Spec.DefaultBackend.Service.Port.Number,
		),
	}
}

func (i *Incluster) getFilteredDeployments(ctx context.Context, filter map[string]string) ([]DeploymentInfo, error) {
	depList, err := i.deploymentList(ctx, i.ViceNamespace, filter, []string{})
	if err != nil {
		return nil, err
	}

	deployments := []DeploymentInfo{}

	for _, dep := range depList.Items {
		info := deploymentInfo(&dep)
		deployments = append(deployments, *info)
	}

	return deployments, nil
}

// FilterableDeploymentsHandler lists all of the deployments.
func (i *Incluster) FilterableDeploymentsHandler(c echo.Context) error {
	ctx := c.Request().Context()
	filter := filterMap(c.Request().URL.Query())

	deployments, err := i.getFilteredDeployments(ctx, filter)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string][]DeploymentInfo{
		"deployments": deployments,
	})
}

func (i *Incluster) getFilteredPods(ctx context.Context, filter map[string]string) ([]PodInfo, error) {
	podList, err := i.podList(ctx, i.ViceNamespace, filter, []string{})
	if err != nil {
		return nil, err
	}

	pods := []PodInfo{}

	for _, pod := range podList.Items {
		info := podInfo(&pod)
		pods = append(pods, *info)
	}

	return pods, nil
}

// FilterablePodsHandler returns a listing of the pods in a VICE analysis.
func (i *Incluster) FilterablePodsHandler(c echo.Context) error {
	ctx := c.Request().Context()
	filter := filterMap(c.Request().URL.Query())

	pods, err := i.getFilteredPods(ctx, filter)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string][]PodInfo{
		"pods": pods,
	})
}

func (i *Incluster) getFilteredConfigMaps(ctx context.Context, filter map[string]string) ([]ConfigMapInfo, error) {
	cmList, err := i.configmapsList(ctx, i.ViceNamespace, filter, []string{})
	if err != nil {
		return nil, err
	}

	cms := []ConfigMapInfo{}

	for _, cm := range cmList.Items {
		info := configMapInfo(&cm)
		cms = append(cms, *info)
	}

	return cms, nil
}

// FilterableConfigMapsHandler lists configmaps in use by VICE apps.
func (i *Incluster) FilterableConfigMapsHandler(c echo.Context) error {
	ctx := c.Request().Context()
	filter := filterMap(c.Request().URL.Query())

	cms, err := i.getFilteredConfigMaps(ctx, filter)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string][]ConfigMapInfo{
		"configmaps": cms,
	})
}

func (i *Incluster) getFilteredServices(ctx context.Context, filter map[string]string) ([]ServiceInfo, error) {
	svcList, err := i.serviceList(ctx, i.ViceNamespace, filter, []string{})
	if err != nil {
		return nil, err
	}

	svcs := []ServiceInfo{}

	for _, svc := range svcList.Items {
		info := serviceInfo(&svc)
		svcs = append(svcs, *info)
	}

	return svcs, nil
}

// FilterableServicesHandler lists services in use by VICE apps.
func (i *Incluster) FilterableServicesHandler(c echo.Context) error {
	ctx := c.Request().Context()
	filter := filterMap(c.Request().URL.Query())

	svcs, err := i.getFilteredServices(ctx, filter)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string][]ServiceInfo{
		"services": svcs,
	})
}

func (i *Incluster) getFilteredIngresses(ctx context.Context, filter map[string]string) ([]IngressInfo, error) {
	ingList, err := i.ingressList(ctx, i.ViceNamespace, filter, []string{})
	if err != nil {
		return nil, err
	}

	ingresses := []IngressInfo{}

	for _, ingress := range ingList.Items {
		info := ingressInfo(&ingress)
		ingresses = append(ingresses, *info)
	}

	return ingresses, nil
}

// FilterableIngressesHandler lists ingresses in use by VICE apps.
func (i *Incluster) FilterableIngressesHandler(c echo.Context) error {
	ctx := c.Request().Context()
	filter := filterMap(c.Request().URL.Query())

	ingresses, err := i.getFilteredIngresses(ctx, filter)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, map[string][]IngressInfo{
		"ingresses": ingresses,
	})
}

// ResourceInfo contains all of the k8s resource information about a running VICE analysis
// that we know of and care about.
type ResourceInfo struct {
	Deployments []DeploymentInfo `json:"deployments"`
	Pods        []PodInfo        `json:"pods"`
	ConfigMaps  []ConfigMapInfo  `json:"configMaps"`
	Services    []ServiceInfo    `json:"services"`
	Ingresses   []IngressInfo    `json:"ingresses"`
}

func (i *Incluster) fixUsername(username string) string {
	return common.FixUsername(username, i.UserSuffix)
}

func (i *Incluster) doResourceListing(ctx context.Context, filter map[string]string) (*ResourceInfo, error) {
	deployments, err := i.getFilteredDeployments(ctx, filter)
	if err != nil {
		return nil, err
	}

	pods, err := i.getFilteredPods(ctx, filter)
	if err != nil {
		return nil, err
	}

	cms, err := i.getFilteredConfigMaps(ctx, filter)
	if err != nil {
		return nil, err
	}

	svcs, err := i.getFilteredServices(ctx, filter)
	if err != nil {
		return nil, err
	}

	ingresses, err := i.getFilteredIngresses(ctx, filter)
	if err != nil {
		return nil, err
	}

	return &ResourceInfo{
		Deployments: deployments,
		Pods:        pods,
		ConfigMaps:  cms,
		Services:    svcs,
		Ingresses:   ingresses,
	}, nil
}

// AdminDescribeAnalysisHandler returns a listing entry for a single analysis
// asssociated with the host/subdomain passed in as 'host' from the URL.
func (i *Incluster) AdminDescribeAnalysisHandler(c echo.Context) error {
	ctx := c.Request().Context()
	host := c.Param("host")

	filter := map[string]string{
		"subdomain": host,
	}

	listing, err := i.doResourceListing(ctx, filter)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}
	return c.JSON(http.StatusOK, listing)

}

// DescribeAnalysisHandler returns a listing entry for a single analysis associated
// with the host/subdomain passed in as 'host' from the URL.
func (i *Incluster) DescribeAnalysisHandler(c echo.Context) error {
	ctx := c.Request().Context()

	log.Info("in DescribeAnalysisHandler")
	host := c.Param("host")
	user := c.QueryParam("user")
	if user == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "user query parameter must be set")
	}

	log.Infof("user: %s, user suffix: %s, host: %s", user, i.UserSuffix, host)

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

	log.Infof("2 user: %s, user suffix: %s, host: %s", user, i.UserSuffix, host)

	filter := map[string]string{
		"subdomain": host,
	}

	listing, err := i.doResourceListing(ctx, filter)
	if err != nil {
		return err
	}

	// the permissions checks occur after the listing because it's possible for the listing to happen
	// before the subdomain is set in the database, causing an error to get percolated up to the UI.
	// Waiting until the Deployments list contains at least one item should guarantee that the subdomain
	// is set in the database.
	if len(listing.Deployments) > 0 {
		externalID := listing.Deployments[0].ExternalID
		analysisID, err := i.apps.GetAnalysisIDByExternalID(ctx, externalID)
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
	}

	return c.JSON(http.StatusOK, listing)
}

// FilterableResourcesHandler returns all of the k8s resources associated with a VICE analysis
// but checks permissions to see if the requesting user has permission to access the resource.
func (i *Incluster) FilterableResourcesHandler(c echo.Context) error {
	ctx := c.Request().Context()
	user := c.QueryParam("user")
	if user == "" {
		return echo.NewHTTPError(http.StatusBadRequest, "user query parameter must be set")
	}

	// Since some usernames don't come through the labelling process unscathed, we have to use
	// the user ID.
	user = i.fixUsername(user)
	userID, err := i.apps.GetUserID(ctx, user)
	if err != nil {
		if err == sql.ErrNoRows {
			return echo.NewHTTPError(http.StatusNotFound, fmt.Sprintf("user %s not found", user))
		}
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	filter := filterMap(c.Request().URL.Query())
	delete(filter, "user")

	filter["user-id"] = userID

	log.Debugf("user ID is %s", userID)

	listing, err := i.doResourceListing(ctx, filter)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, listing)

}

// AdminFilterableResourcesHandler returns all of the k8s resources associated with a VICE analysis.
func (i *Incluster) AdminFilterableResourcesHandler(c echo.Context) error {
	ctx := c.Request().Context()
	filter := filterMap(c.Request().URL.Query())

	listing, err := i.doResourceListing(ctx, filter)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, err.Error())
	}

	return c.JSON(http.StatusOK, listing)
}

func populateAnalysisID(ctx context.Context, a *apps.Apps, existingLabels map[string]string) (map[string]string, error) {
	if _, ok := existingLabels["analysis-id"]; !ok {
		externalID, ok := existingLabels["external-id"]
		if !ok {
			return existingLabels, fmt.Errorf("missing external-id key")
		}
		analysisID, err := a.GetAnalysisIDByExternalID(ctx, externalID)
		if err != nil {
			log.Debug(errors.Wrapf(err, "error getting analysis id for external id %s", externalID))
		} else {
			existingLabels["analysis-id"] = analysisID
		}
	}
	return existingLabels, nil
}

func populateSubdomain(existingLabels map[string]string) map[string]string {
	if _, ok := existingLabels["subdomain"]; !ok {
		if externalID, ok := existingLabels["external-id"]; ok {
			if userID, ok := existingLabels["user-id"]; ok {
				existingLabels["subdomain"] = IngressName(userID, externalID)
			}
		}
	}

	return existingLabels
}

func populateLoginIP(ctx context.Context, a *apps.Apps, existingLabels map[string]string) (map[string]string, error) {
	if _, ok := existingLabels["login-ip"]; !ok {
		if userID, ok := existingLabels["user-id"]; ok {
			ipAddr, err := a.GetUserIP(ctx, userID)
			if err != nil {
				return existingLabels, err
			}
			existingLabels["login-ip"] = ipAddr
		}
	}

	return existingLabels, nil
}

func (i *Incluster) relabelDeployments(ctx context.Context) []error {
	filter := map[string]string{} // Empty on purpose. Only filter based on interactive label.
	errors := []error{}

	deployments, err := i.deploymentList(ctx, i.ViceNamespace, filter, []string{"subdomain"})
	if err != nil {
		errors = append(errors, err)
		return errors
	}

	for _, deployment := range deployments.Items {
		existingLabels := deployment.GetLabels()

		existingLabels = populateSubdomain(existingLabels)

		existingLabels, err = populateLoginIP(ctx, i.apps, existingLabels)
		if err != nil {
			errors = append(errors, err)
		}

		existingLabels, err = populateAnalysisID(ctx, i.apps, existingLabels)
		if err != nil {
			errors = append(errors, err)
		}

		deployment.SetLabels(existingLabels)
		_, err = i.clientset.AppsV1().Deployments(i.ViceNamespace).Update(ctx, &deployment, metav1.UpdateOptions{})
		if err != nil {
			errors = append(errors, err)
		}
	}

	return errors
}

func (i *Incluster) relabelConfigMaps(ctx context.Context) []error {
	filter := map[string]string{} // Empty on purpose. Only filter based on interactive label.
	errors := []error{}

	cms, err := i.configmapsList(ctx, i.ViceNamespace, filter, []string{"subdomain"})
	if err != nil {
		errors = append(errors, err)
		return errors
	}

	for _, configmap := range cms.Items {
		existingLabels := configmap.GetLabels()

		existingLabels = populateSubdomain(existingLabels)

		existingLabels, err = populateLoginIP(ctx, i.apps, existingLabels)
		if err != nil {
			errors = append(errors, err)
		}

		existingLabels, err = populateAnalysisID(ctx, i.apps, existingLabels)
		if err != nil {
			errors = append(errors, err)
		}

		configmap.SetLabels(existingLabels)
		_, err = i.clientset.CoreV1().ConfigMaps(i.ViceNamespace).Update(ctx, &configmap, metav1.UpdateOptions{})
		if err != nil {
			errors = append(errors, err)
		}
	}

	return errors
}

func (i *Incluster) relabelServices(ctx context.Context) []error {
	filter := map[string]string{} // Empty on purpose. Only filter based on interactive label.
	errors := []error{}

	svcs, err := i.serviceList(ctx, i.ViceNamespace, filter, []string{"subdomain"})
	if err != nil {
		errors = append(errors, err)
		return errors
	}

	for _, service := range svcs.Items {
		existingLabels := service.GetLabels()

		existingLabels = populateSubdomain(existingLabels)

		existingLabels, err = populateLoginIP(ctx, i.apps, existingLabels)
		if err != nil {
			errors = append(errors, err)
		}

		existingLabels, err = populateAnalysisID(ctx, i.apps, existingLabels)
		if err != nil {
			errors = append(errors, err)
		}

		service.SetLabels(existingLabels)
		_, err = i.clientset.CoreV1().Services(i.ViceNamespace).Update(ctx, &service, metav1.UpdateOptions{})
		if err != nil {
			errors = append(errors, err)
		}
	}

	return errors
}

func (i *Incluster) relabelIngresses(ctx context.Context) []error {
	filter := map[string]string{} // Empty on purpose. Only filter based on interactive label.
	errors := []error{}

	ingresses, err := i.ingressList(ctx, i.ViceNamespace, filter, []string{"subdomain"})
	if err != nil {
		errors = append(errors, err)
		return errors
	}

	for _, ingress := range ingresses.Items {
		existingLabels := ingress.GetLabels()

		existingLabels = populateSubdomain(existingLabels)

		existingLabels, err = populateLoginIP(ctx, i.apps, existingLabels)
		if err != nil {
			errors = append(errors, err)
		}

		existingLabels, err = populateAnalysisID(ctx, i.apps, existingLabels)
		if err != nil {
			errors = append(errors, err)
		}

		ingress.SetLabels(existingLabels)
		client := i.clientset.NetworkingV1().Ingresses(i.ViceNamespace)
		_, err = client.Update(ctx, &ingress, metav1.UpdateOptions{})
		if err != nil {
			errors = append(errors, err)
		}
	}

	return errors
}

// ApplyAsyncLabels ensures that the required labels are applied to all running VICE analyses.
// This is useful to avoid race conditions between the DE database and the k8s cluster,
// and also for adding new labels to "old" analyses during an update.
func (i *Incluster) ApplyAsyncLabels(ctx context.Context) []error {
	errors := []error{}

	labelDepsErrors := i.relabelDeployments(ctx)
	if len(labelDepsErrors) > 0 {
		errors = append(errors, labelDepsErrors...)
	}

	labelCMErrors := i.relabelConfigMaps(ctx)
	if len(labelCMErrors) > 0 {
		errors = append(errors, labelCMErrors...)
	}

	labelSVCErrors := i.relabelServices(ctx)
	if len(labelSVCErrors) > 0 {
		errors = append(errors, labelSVCErrors...)
	}

	labelIngressesErrors := i.relabelIngresses(ctx)
	if len(labelIngressesErrors) > 0 {
		errors = append(errors, labelIngressesErrors...)
	}

	return errors
}

// ApplyAsyncLabelsHandler is the http handler for triggering the application
// of labels on running VICE analyses.
func (i *Incluster) ApplyAsyncLabelsHandler(c echo.Context) error {
	ctx := c.Request().Context()
	errs := i.ApplyAsyncLabels(ctx)

	if len(errs) > 0 {
		var errMsg strings.Builder
		for _, err := range errs {
			log.Error(err)
			fmt.Fprintf(&errMsg, "%s\n", err.Error())
		}

		return c.String(http.StatusInternalServerError, errMsg.String())
	}
	return c.NoContent(http.StatusOK)
}

// GetAsyncData returns the data that would be applied as labels as a
// JSON-encoded map instead.
