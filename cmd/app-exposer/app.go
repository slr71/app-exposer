package main

import (
	"net/http"
	"net/url"
	"time"

	"github.com/cyverse-de/app-exposer/adapter"
	"github.com/cyverse-de/app-exposer/apps"
	"github.com/cyverse-de/app-exposer/common"
	"github.com/cyverse-de/app-exposer/db"
	"github.com/cyverse-de/app-exposer/httphandlers"
	"github.com/cyverse-de/app-exposer/incluster"
	"github.com/cyverse-de/app-exposer/instantlaunches"
	"github.com/cyverse-de/app-exposer/operatorclient"
	"github.com/cyverse-de/app-exposer/outcluster"
	"github.com/knadh/koanf"
	"go.opentelemetry.io/contrib/instrumentation/github.com/labstack/echo/otelecho"
	"k8s.io/client-go/kubernetes"
	gatewayclient "sigs.k8s.io/gateway-api/pkg/client/clientset/versioned/typed/apis/v1"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"

	_ "github.com/cyverse-de/app-exposer/docs"
	echoSwagger "github.com/swaggo/echo-swagger"
)

// ExposerApp encapsulates the overall application-logic, tying together the
// REST-like API with the underlying Kubernetes API. All of the HTTP handlers
// are methods for an ExposerApp instance.
type ExposerApp struct {
	outcluster      *outcluster.Outcluster
	incluster       *incluster.Incluster
	handlers        *httphandlers.HTTPHandlers
	namespace       string
	clientset       kubernetes.Interface
	router          *echo.Echo
	dbase           *db.Database
	instantlaunches *instantlaunches.App
}

// ExposerAppInit contains configuration settings for creating a new ExposerApp.
type ExposerAppInit struct {
	Namespace               string // The namespace that the route settings are added to.
	ViceNamespace           string // The namespace containing the running VICE apps.
	ViceProxyImage          string
	ViceDomain              string
	UserSuffix              string
	IRODSZone               string
	ClientSet               kubernetes.Interface
	GatewayClient           *gatewayclient.GatewayV1Client
	batchadapter            *adapter.JEXAdapter
	dbase                   *db.Database
	MaxConcurrentLaunches   int
	ImagePullSecretName     string
	ClusterConfigSecretName string
	BypassUsers             []string
	SubscriptionsURL        *url.URL
}

//	@title			app-exposer
//	@version		1.0
//	@description	The app-exposer API for the Discovery Environment's VICE feature.
//
//	@license.name	3-Clause BSD License
//	@license.url	https://github.com/cyverse-de/app-exposer?tab=License-1-ov-file#readme
//
//	@host			localhost:60000
//	@BasePath		/
//
// NewExposerApp creates and returns a newly instantiated *ExposerApp.
func NewExposerApp(init *ExposerAppInit, apps *apps.Apps, c *koanf.Koanf) *ExposerApp {
	jobStatusURL := jobStatusBase(c)

	metadataBaseURL := c.String("metadata.base")
	if metadataBaseURL == "" {
		metadataBaseURL = "http://metadata"
	}

	appsServiceBaseURL := c.String("apps.base")
	if appsServiceBaseURL == "" {
		appsServiceBaseURL = "http://apps"
	}

	permissionsURL := c.String("permissions.base")
	if permissionsURL == "" {
		permissionsURL = "http://permissions"
	}

	timeLimitExtensionStr := c.String("vice.time-limit-extension")
	if timeLimitExtensionStr == "" {
		timeLimitExtensionStr = "4h"
	}
	timeLimitExtensionDuration, err := time.ParseDuration(timeLimitExtensionStr)
	if err != nil {
		log.Fatalf("invalid vice.time-limit-extension value %q: %s", timeLimitExtensionStr, err)
	}
	if timeLimitExtensionDuration <= 0 {
		log.Fatalf("vice.time-limit-extension must be positive, got %q", timeLimitExtensionStr)
	}
	timeLimitExtensionSeconds := int64(timeLimitExtensionDuration.Seconds())
	log.Infof("VICE time limit extension set to %s (%d seconds)", timeLimitExtensionStr, timeLimitExtensionSeconds)

	deNamespace := c.String("vice.backend-namespace")

	inclusterInit := &incluster.Init{
		ViceNamespace:                 init.ViceNamespace,
		ViceDomain:                    init.ViceDomain,
		PorklockImage:                 c.String("vice.file-transfers.image"),
		PorklockTag:                   c.String("vice.file-transfers.tag"),
		UseCSIDriver:                  c.Bool("vice.use_csi_driver"),
		InputPathListIdentifier:       c.String("path_list.file_identifier"),
		TicketInputPathListIdentifier: c.String("tickets_path_list.file_identifier"),
		ImagePullSecretName:           init.ImagePullSecretName,
		ViceProxyImage:                init.ViceProxyImage,
		FrontendBaseURL:               c.String("k8s.frontend.base"),
		VICEBackendNamespace:          deNamespace,
		AppsServiceBaseURL:            appsServiceBaseURL,
		JobStatusURL:                  jobStatusURL,
		UserSuffix:                    init.UserSuffix,
		PermissionsURL:                permissionsURL,
		IRODSZone:                     init.IRODSZone,
		GatewayProvider:               c.String("vice.gateway_provider"),
		ClusterConfigSecretName:       init.ClusterConfigSecretName,
		SubscriptionsURL:              init.SubscriptionsURL,
		BypassUsers:                   init.BypassUsers,
		TimeLimitExtensionSeconds:     timeLimitExtensionSeconds,
	}

	incluster := incluster.New(inclusterInit, init.dbase.SQLX(), init.ClientSet, init.GatewayClient, apps)

	app := &ExposerApp{
		outcluster: outcluster.New(init.ClientSet, init.GatewayClient, deNamespace, init.Namespace, init.ViceDomain),
		incluster:  incluster,
		namespace:  init.Namespace,
		clientset:  init.ClientSet,
		router:     echo.New(),
		dbase:      init.dbase,
		handlers:   httphandlers.New(incluster, apps, init.ClientSet, init.batchadapter, init.dbase, init.MaxConcurrentLaunches),
	}

	// Operator scheduler starts empty; the reconciler's initial
	// SyncOperators call (in main.go) populates it from the operators
	// table, which is the source of truth for operator configuration.
	scheduler := operatorclient.NewScheduler(nil)
	app.handlers.SetScheduler(scheduler)
	log.Info("operator scheduler initialized; awaiting initial DB sync")

	app.router.Use(otelecho.Middleware("app-exposer"))

	ilInit := &instantlaunches.Init{
		UserSuffix:      init.UserSuffix,
		MetadataBaseURL: metadataBaseURL,
		PermissionsURL:  permissionsURL,
	}

	app.router.HTTPErrorHandler = func(err error, c echo.Context) {
		code := http.StatusInternalServerError
		var body any

		switch err := err.(type) {
		case common.ErrorResponse:
			code = http.StatusBadRequest
			body = err
		case *common.ErrorResponse:
			code = http.StatusBadRequest
			body = err
		case *echo.HTTPError:
			echoErr := err
			code = echoErr.Code
			body = common.NewErrorResponse(err)
		default:
			body = common.NewErrorResponse(err)
		}

		c.JSON(code, body) // nolint:errcheck
	}

	app.router.GET("/", app.Greeting).Name = "greeting"
	app.router.GET("/docs/*", echoSwagger.WrapHandler)

	batchGroup := app.router.Group("/batch")
	batchGroup.Use(middleware.Logger())
	batchGroup.GET("", app.handlers.BatchHomeHandler)
	batchGroup.GET("/", app.handlers.BatchHomeHandler)
	batchGroup.POST("", app.handlers.BatchLaunchHandler)
	batchGroup.POST("/", app.handlers.BatchLaunchHandler)
	batchGroup.POST("/cleanup", app.handlers.BatchStopByUUID)
	batchGroup.DELETE("/stop/:id", app.handlers.BatchStopHandler)

	vice := app.router.Group("/vice")
	vice.Use(middleware.Logger())
	vice.POST("/launch", app.handlers.LaunchAppHandler)
	vice.POST("/dry-run", app.handlers.DryRunBundleHandler)
	vice.GET("/async-data", app.handlers.AsyncDataHandler)
	vice.GET("/listing", app.handlers.FilterableResourcesHandler)
	vice.POST("/:id/download-input-files", app.handlers.TriggerDownloadsHandler)
	vice.POST("/:id/save-output-files", app.handlers.TriggerUploadsHandler)
	vice.POST("/:id/exit", app.handlers.ExitHandler)
	vice.POST("/:id/save-and-exit", app.handlers.SaveAndExitHandler)
	vice.GET("/:analysis-id/pods", app.handlers.PodsHandler)
	vice.GET("/:analysis-id/logs", app.handlers.LogsHandler)
	vice.POST("/:analysis-id/time-limit", app.handlers.TimeLimitUpdateHandler)
	vice.GET("/:analysis-id/time-limit", app.handlers.GetTimeLimitHandler)
	vice.PUT("/:analysis-id/permissions", app.handlers.UpdatePermissionsHandler)
	vice.GET("/:host/url-ready", app.handlers.URLReadyHandler)
	vice.GET("/:host/description", app.handlers.DescribeAnalysisHandler)

	vicelisting := vice.Group("/listing")
	vicelisting.GET("/", app.handlers.FilterableResourcesHandler)
	vicelisting.GET("/deployments", app.handlers.FilterableDeploymentsHandler)
	vicelisting.GET("/pods", app.handlers.FilterablePodsHandler)
	vicelisting.GET("/configmaps", app.handlers.FilterableConfigMapsHandler)
	vicelisting.GET("/services", app.handlers.FilterableServicesHandler)
	vicelisting.GET("/routes", app.handlers.FilterableRoutesHandler)

	viceadmin := vice.Group("/admin")
	viceadmin.GET("/operators", app.handlers.AdminOperatorsHandler)
	viceadmin.POST("/operators", app.handlers.CreateOperatorHandler)
	viceadmin.GET("/operators/capacities", app.handlers.AdminOperatorCapacitiesHandler)
	viceadmin.DELETE("/operators/id/:id", app.handlers.DeleteOperatorHandler)
	viceadmin.PATCH("/operators/id/:id", app.handlers.UpdateOperatorHandler)
	viceadmin.GET("/listing", app.handlers.AdminFilterableResourcesHandler)
	viceadmin.GET("/operator-listing", app.handlers.AdminOperatorListingHandler)
	viceadmin.POST("/terminate-all", app.handlers.TerminateAllAnalysesHandler)
	viceadmin.GET("/:host/description", app.handlers.AdminDescribeAnalysisHandler)
	viceadmin.GET("/:host/url-ready", app.handlers.AdminURLReadyHandler)
	viceadmin.GET("/:host/canonical-url", app.handlers.AdminCanonicalURLHandler)
	viceadmin.GET("/is-deployed/external-id/:external-id", app.handlers.AdminAnalysisInClusterByExternalID)
	viceadmin.GET("/is-deployed/analysis-id/:analysis-id", app.handlers.AdminAnalysisInClusterByID)

	viceanalyses := viceadmin.Group("/analyses")
	viceanalyses.GET("/", app.handlers.AdminFilterableResourcesHandler)
	viceanalyses.POST("/:analysis-id/download-input-files", app.handlers.AdminTriggerDownloadsHandler)
	viceanalyses.POST("/:analysis-id/save-output-files", app.handlers.AdminTriggerUploadsHandler)
	viceanalyses.POST("/:analysis-id/exit", app.handlers.AdminExitHandler)
	viceanalyses.POST("/:analysis-id/save-and-exit", app.handlers.AdminSaveAndExitHandler)
	viceanalyses.GET("/:analysis-id/time-limit", app.handlers.AdminGetTimeLimitHandler)
	viceanalyses.POST("/:analysis-id/time-limit", app.handlers.AdminTimeLimitUpdateHandler)
	viceanalyses.GET("/:analysis-id/external-id", app.handlers.AdminGetExternalIDHandler)

	svc := app.router.Group("/service")
	svc.Use(middleware.Logger())
	svc.POST("/:name", app.outcluster.CreateServiceHandler)
	svc.PUT("/:name", app.outcluster.UpdateServiceHandler)
	svc.GET("/:name", app.outcluster.GetServiceHandler)
	svc.DELETE("/:name", app.outcluster.DeleteServiceHandler)

	endpoint := app.router.Group("/endpoint")
	endpoint.Use(middleware.Logger())
	endpoint.POST("/:name", app.outcluster.CreateEndpointHandler)
	endpoint.PUT("/:name", app.outcluster.UpdateEndpointHandler)
	endpoint.GET("/:name", app.outcluster.GetEndpointHandler)
	endpoint.DELETE("/:name", app.outcluster.DeleteEndpointHandler)

	routes := app.router.Group("/routes")
	routes.Use(middleware.Logger())
	routes.POST("/:name", app.outcluster.CreateRouteHandler)
	routes.PUT("/:name", app.outcluster.UpdateRouteHandler)
	routes.GET("/:name", app.outcluster.GetRouteHandler)
	routes.DELETE("/:name", app.outcluster.DeleteRouteHandler)

	ilgroup := app.router.Group("/instantlaunches")
	ilgroup.Use(middleware.Logger())
	app.instantlaunches = instantlaunches.New(app.dbase.SQLX(), ilgroup, ilInit)

	return app
}

// Greeting lets the caller know that the service is up and should be receiving
// requests.
func (e *ExposerApp) Greeting(context echo.Context) error {
	return context.String(http.StatusOK, "Hello from app-exposer.")
}
