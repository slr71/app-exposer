package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/knadh/koanf"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"

	"github.com/cyverse-de/app-exposer/adapter"
	"github.com/cyverse-de/app-exposer/apps"
	"github.com/cyverse-de/app-exposer/common"
	"github.com/cyverse-de/app-exposer/db"
	"github.com/cyverse-de/app-exposer/expiration"
	"github.com/cyverse-de/app-exposer/httphandlers"
	"github.com/cyverse-de/app-exposer/imageinfo"
	"github.com/cyverse-de/app-exposer/millicores"
	"github.com/cyverse-de/app-exposer/quota"
	"github.com/cyverse-de/app-exposer/reconciler"
	"github.com/cyverse-de/app-exposer/resourcing"
	"github.com/cyverse-de/go-mod/cfg"
	"github.com/cyverse-de/go-mod/logging"
	"github.com/cyverse-de/go-mod/otelutils"
	"github.com/pkg/errors"
	"golang.org/x/oauth2/clientcredentials"
	resourcev1 "k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog" // pull in to set klog output to stderr
	gatewayclient "sigs.k8s.io/gateway-api/pkg/client/clientset/versioned/typed/apis/v1"

	"github.com/uptrace/opentelemetry-go-extra/otelsql"
	"github.com/uptrace/opentelemetry-go-extra/otelsqlx"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"
)

var log = common.Log

const serviceName = "app-exposer"

func init() {
	// Set klog, used by the k8s client, to use its "log to stderr"
	// functionality. Otherwise, it'll crash without a /tmp directory, and
	// it's not great it's allowed to write files by default anyway. With
	// thanks to https://github.com/coredns/coredns/pull/2529
	klogFlags := flag.NewFlagSet("klog", flag.ExitOnError)
	klog.InitFlags(klogFlags)
	logtostderr := klogFlags.Lookup("logtostderr")
	logtostderr.Value.Set("true") //nolint:errcheck
}

func wrapOtelTransport(rt http.RoundTripper) http.RoundTripper {
	return otelhttp.NewTransport(rt)
}

func main() {
	log.Logger.SetReportCaller(true)

	var (
		err        error
		kubeconfig *string
		c          *koanf.Koanf
		dbconn     *sqlx.DB

		configPath                           = flag.String("config", cfg.DefaultConfigPath, "Path to the config file")
		dotEnvPath                           = flag.String("dotenv-path", cfg.DefaultDotEnvPath, "Path to the dotenv file")
		envPrefix                            = flag.String("env-prefix", cfg.DefaultEnvPrefix, "The prefix for environment variables")
		namespace                            = flag.String("namespace", "default", "The namespace scope this process operates on for non-VICE calls")
		viceNamespace                        = flag.String("vice-namespace", "vice-apps", "The namespace that VICE apps are launched within")
		listenPort                           = flag.Int("port", 60000, "(optional) The port to listen on")
		viceProxy                            = flag.String("vice-proxy", "harbor.cyverse.org/de/vice-proxy", "The image name of the proxy to use for VICE apps. The image tag is set in the config.")
		transferImage                        = flag.String("transfer-image", "harbor.cyverse.org/de/gocmd:latest", "(optional) Image used to transfer files to/from the data store")
		transferWorkingDir                   = flag.String("transfer-working-dir", "/de-app-work", "The working directory within the file transfer image.")
		transferLogLevel                     = flag.String("transfer-log-level", "debug", "The log level of the output of the file transfer tool.")
		statusSenderImage                    = flag.String("status-sender-image", "harbor.cyverse.org/de/url-import:latest", "The image used to send status updates. Must container curl.")
		userSuffix                           = flag.String("user-suffix", "@iplantcollaborative.org", "The user suffix for all users in the DE installation")
		defaultMillicores                    = flag.Float64("default-millicores", 4000.0, "The default number of millicores reserved for an analysis.")
		argoWorkflowNS                       = flag.String("workflow-namespace", "argo", "The namespace Argo Workflows run in.")
		defaultCPUResourceRequest            = flag.String("default-cpu-resource-request", "1000m", "The default CPU resource request for an analysis.")
		defaultCPUResourceLimit              = flag.String("default-cpu-resource-limit", "2000m", "The default CPU resource limit for an analysis.")
		disableCPUResourceLimit              = flag.Bool("disable-cpu-resource-limit", false, "Disable CPU resource limit for an analysis.")
		defaultMemoryResourceRequest         = flag.String("default-memory-resource-request", "2Gi", "The default memory resource request for an analysis.")
		defaultMemoryResourceLimit           = flag.String("default-memory-resource-limit", "8Gi", "The default memory resource limit for an analysis.")
		disableMemoryResourceLimit           = flag.Bool("disable-memory-resource-limit", false, "Disable memory resource limit for an analysis.")
		defaultStorageResourceRequest        = flag.String("default-storage-resource-request", "1Gi", "The default storage resource request for an analysis.")
		viceProxyCPUResourceRequest          = flag.String("vice-proxy-cpu-resource-request", "100m", "The default CPU resource request for the vice proxy.")
		viceProxyCPUResourceLimit            = flag.String("vice-proxy-cpu-resource-limit", "200m", "The default CPU resource limit for the vice proxy.")
		disableViceProxyCPUResourceLimit     = flag.Bool("disable-vice-proxy-cpu-resource-limit", false, "Disable CPU resource limit for the vice proxy.")
		viceProxyMemoryResourceRequest       = flag.String("vice-proxy-memory-resource-request", "100Mi", "The default memory resource request for the vice proxy.")
		viceProxyMemoryResourceLimit         = flag.String("vice-proxy-memory-resource-limit", "200Mi", "The default memory resource limit for the vice proxy.")
		disableViceProxyMemoryResourceLimit  = flag.Bool("disable-vice-proxy-memory-resource-limit", false, "Disable memory resource limit for the vice proxy.")
		viceProxyStorageResourceRequest      = flag.String("vice-proxy-storage-resource-request", "16Gi", "The default storage resource request for the vice proxy.")
		viceProxyStorageResourceLimit        = flag.String("vice-proxy-storage-resource-limit", "100Gi", "The default storage resource limit for the vice proxy.")
		disableViceProxyStorageResourceLimit = flag.Bool("disable-vice-proxy-storage-resource-limit", true, "Disable storage resource limit for the vice proxy.")
		logLevel                             = flag.String("log-level", "warn", "One of trace, debug, info, warn, error, fatal, or panic.")
		batchExitHandlerImage                = flag.String("batch-exit-handler-image", "harbor.cyverse.org/de/batch-exit-handler:latest", "The image to use for the exitHandler in batch workflows")
		clusterConfigSecret                  = flag.String("cluster-config-secret", "cluster-config-secret", "Name of a Secret to inject as env vars into the vice-proxy container via envFrom. Used to provide cluster-specific config for multi-cluster deployments.")
		maxConcurrentLaunches                = flag.Int("max-concurrent-launches", httphandlers.DefaultMaxConcurrentLaunches, "Upper bound on in-flight VICE launches per app-exposer instance; bursts beyond this return 503 so clients can back off.")
		expirationSweepInterval              = flag.Duration("expiration-sweep-interval", expiration.DefaultSweepInterval, "How often to check running analyses for expiry warnings and terminations.")
		expiryWarning                        = flag.Duration("expiry-warning", expiration.DefaultExpiryWarning, "How far ahead of an analysis's planned end date to send the short-notice expiry warning.")
	)

	var tracerCtx, cancel = context.WithCancel(context.Background())
	defer cancel()
	shutdown := otelutils.TracerProviderFromEnv(tracerCtx, serviceName, func(e error) { log.Fatal(e) })
	defer shutdown()

	// if cluster is set, then
	if cluster := os.Getenv("CLUSTER"); cluster != "" {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	} else {
		// If the home directory exists, then assume that the kube config will be read
		// from ~/.kube/config.
		if home := os.Getenv("HOME"); home != "" {
			kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
		} else {
			// If the home directory doesn't exist, then allow the user to specify a path.
			kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
		}
	}

	flag.Parse()
	logging.SetupLogging(*logLevel)

	log := log.WithFields(logrus.Fields{"context": "main"})

	if !strings.HasPrefix(*userSuffix, "@") {
		*userSuffix = fmt.Sprintf("@%s", *userSuffix)
	}

	log.Infof("Reading config from %s", *configPath)
	if _, err = os.Open(*configPath); err != nil {
		log.Fatalf("failed to open config file %s: %v", *configPath, err)
	}

	c, err = cfg.Init(&cfg.Settings{
		EnvPrefix:   *envPrefix,
		ConfigPath:  *configPath,
		DotEnvPath:  *dotEnvPath,
		StrictMerge: false,
		FileType:    cfg.YAML,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Infof("Done reading config from %s", *configPath)

	// Make sure the db.uri URL is parseable
	if _, err = url.Parse(c.String("db.uri")); err != nil {
		log.Fatal(errors.Wrap(err, "Can't parse db.uri in the config file"))
	}

	// Make sure the frontend base URL is parseable.
	if _, err = url.Parse(c.String("k8s.frontend.base")); err != nil {
		log.Fatal(errors.Wrap(err, "Can't parse k8s.frontend.base in the config file"))
	}

	// Make sure that the iRODS zone isn't empty.
	zone := c.String("irods.zone")
	if zone == "" {
		log.Fatal("The iRODS zone must be specified in the config file")
	}

	harborURL := c.String("harbor.url")
	if harborURL == "" {
		harborURL = "https://harbor.cyverse.org/api/v2.0"
	}

	harborUser := c.String("harbor.user")
	if harborUser == "" {
		log.Fatal("The harbor.user setting must be specified in the config file")
	}

	harborPass := c.String("harbor.pass")
	if harborPass == "" {
		log.Fatal("The harbor.pass setting must be specified in the config file")
	}

	viceDomain := c.String("vice.domain")
	if viceDomain == "" {
		log.Fatal("The vice.domain setting must be specified in the configuration file")
	}

	viceGatewayProvider := c.String("vice.gateway_provider")
	if viceGatewayProvider == "" {
		log.Fatal("The vice.gateway_provider setting must be specified in the configuration file")
	}

	setter := func(unparsedQuantity string, setFn func(qn resourcev1.Quantity)) {
		v, err := resourcev1.ParseQuantity(unparsedQuantity)
		if err != nil {
			log.Fatal(err)
		}
		setFn(v)
	}

	setter(*defaultCPUResourceRequest, resourcing.SetDefaultCPUResourceRequest)
	setter(*defaultCPUResourceLimit, resourcing.SetDefaultCPUResourceLimit)
	setter(*defaultMemoryResourceRequest, resourcing.SetDefaultMemResourceRequest)
	setter(*defaultMemoryResourceLimit, resourcing.SetDefaultMemResourceLimit)
	setter(*defaultStorageResourceRequest, resourcing.SetDefaultStorageRequest)
	setter(*viceProxyCPUResourceRequest, resourcing.SetVICEProxyCPUResourceRequest)
	setter(*viceProxyCPUResourceLimit, resourcing.SetVICEProxyCPUResourceLimit)
	setter(*viceProxyMemoryResourceRequest, resourcing.SetVICEProxyMemResourceRequest)
	setter(*viceProxyMemoryResourceLimit, resourcing.SetVICEProxyMemResourceLimit)
	setter(*viceProxyStorageResourceRequest, resourcing.SetVICEProxyStorageRequest)
	setter(*viceProxyStorageResourceLimit, resourcing.SetVICEProxyStorageLimit)
	resourcing.SetDoDefaultCPUResourceLimit(!*disableCPUResourceLimit)
	resourcing.SetDoDefaultMemResourceLimit(!*disableMemoryResourceLimit)
	resourcing.SetDoVICEProxyCPUResourceLimit(!*disableViceProxyCPUResourceLimit)
	resourcing.SetDoVICEProxyMemResourceLimit(!*disableViceProxyMemoryResourceLimit)
	resourcing.SetDoVICEProxyStorageLimit(!*disableViceProxyStorageResourceLimit)

	infoGetter, err := imageinfo.NewHarborInfoGetter(
		harborURL,
		harborUser,
		harborPass,
	)
	if err != nil {
		log.Fatal(err)
	}

	subscriptionsBase := c.String("subscriptions.base")
	if subscriptionsBase == "" {
		subscriptionsBase = "http://subscriptions"
	}
	subscriptionsURL, err := url.Parse(subscriptionsBase)
	if err != nil {
		log.Fatal(errors.Wrapf(err, "error parsing subscriptions.base URL %s", subscriptionsBase))
	}
	log.Infof("subscriptions base URL is %s", subscriptionsURL.String())

	// Print error and exit if *kubeconfig is not empty and doesn't actually
	// exist. If *kubeconfig is blank, then the app may be running inside the
	// cluster, so let things proceed.
	if *kubeconfig != "" {
		_, err = os.Stat(*kubeconfig)
		if err != nil {
			if os.IsNotExist(err) {
				log.Fatalf("config %s does not exist", *kubeconfig)
			}
			log.Fatal(errors.Wrapf(err, "error stat'ing the kubeconfig %s", *kubeconfig))
		}
	}

	log.Printf("namespace is set to %s\n", *namespace)
	log.Printf("listen port is set to %d\n", *listenPort)
	log.Printf("kubeconfig is set to '%s', and may be blank", *kubeconfig)

	var config *rest.Config
	if *kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", *kubeconfig)
		if err != nil {
			log.Fatal(errors.Wrapf(err, "error building config from flags using kubeconfig %s", *kubeconfig))
		}
	} else {
		// If the home directory doesn't exist and the user doesn't specify a path,
		// then assume that we're running inside a cluster.
		config, err = rest.InClusterConfig()
		if err != nil {
			log.Fatal(errors.Wrapf(err, "error loading the config inside the cluster"))
		}
	}

	config.Wrap(wrapOtelTransport)

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal(errors.Wrap(err, "error creating clientset from config"))
	}

	gatewayClient, err := gatewayclient.NewForConfig(config)
	if err != nil {
		log.Fatal(errors.Wrap(err, "error creating gateway API clientset from config"))
	}

	var proxyImage string
	proxyTag := c.String("interapps.proxy.tag")
	if proxyTag == "" {
		proxyImage = *viceProxy
	} else {
		proxyImage = fmt.Sprintf("%s:%s", *viceProxy, proxyTag)
	}

	dbURI := c.String("db.uri")
	dbconn = otelsqlx.MustConnect("postgres", dbURI,
		otelsql.WithAttributes(semconv.DBSystemPostgreSQL))

	a := apps.NewApps(dbconn, *userSuffix)
	go a.Run()
	defer a.Finish()

	// Set up the database abstraction needed for batch functionality. dbase
	// wraps the same connection as dbconn and also carries the URI so the
	// reconciler can open a dedicated LISTEN connection without having the
	// URI threaded separately.
	dbase := db.New(dbconn, dbURI)

	// Create millicores handler needed for batch functionality.
	detector, err := millicores.New(dbase, *defaultMillicores)
	if err != nil {
		log.Fatal(err)
	}

	imagePullSecretName := c.String("vice.image-pull-secret")

	// Load the resource tracking bypass users from config and normalize them
	bypassUsers := c.Strings("resource_tracking.bypass_users")
	// Normalize all usernames by adding the user suffix if not present
	for i, user := range bypassUsers {
		bypassUsers[i] = common.FixUsername(user, *userSuffix)
	}
	if len(bypassUsers) == 0 {
		log.Info("Resource tracking bypass is disabled: no users in bypass whitelist")
	} else {
		log.Infof("Resource tracking bypass is enabled for %d user(s): %v", len(bypassUsers), bypassUsers)
	}

	// Create the app that handles batch functionality.
	jexAdapterInit := &adapter.Init{
		LogPath:                c.String("condor.log_path"),
		IRODSBase:              c.String("irods.base"),
		FilterFiles:            strings.Split(c.String("condor.filter_files"), ","),
		FileTransferImage:      *transferImage,
		FileTransferWorkingDir: *transferWorkingDir,
		FileTransferLogLevel:   *transferLogLevel,
		StatusSenderImage:      *statusSenderImage,
		Namespace:              *argoWorkflowNS,
		ImagePullSecretName:    imagePullSecretName,
		BatchExitHandlerImage:  *batchExitHandlerImage,
		BypassUsers:            bypassUsers,
		UserSuffix:             *userSuffix,
	}
	enforcer := quota.NewEnforcer(clientset, dbconn, a, subscriptionsURL, *userSuffix)
	jexAdapter := adapter.New(jexAdapterInit, a, detector, infoGetter, enforcer, clientset)

	exposerInit := &ExposerAppInit{
		Namespace:               *namespace,
		ViceNamespace:           *viceNamespace,
		ViceProxyImage:          proxyImage,
		ViceDomain:              c.String("vice.domain"),
		UserSuffix:              *userSuffix,
		IRODSZone:               zone,
		ClientSet:               clientset,
		GatewayClient:           gatewayClient,
		batchadapter:            jexAdapter,
		dbase:                   dbase,
		ImagePullSecretName:     imagePullSecretName,
		ClusterConfigSecretName: *clusterConfigSecret,
		BypassUsers:             bypassUsers,
		MaxConcurrentLaunches:   *maxConcurrentLaunches,
		SubscriptionsURL:        subscriptionsURL,
	}

	// app is the base app-exposer functionality.
	app := NewExposerApp(
		exposerInit,
		a,
		c,
	)

	// Build an OAuth2 token source for authenticating with vice-operator
	// instances using the Keycloak client credentials grant. Keycloak is
	// required in every environment we run in (including local dev), so
	// missing config is a fatal startup error rather than a silent warning.
	kcBaseURL := c.String("vice.keycloak.base_url")
	kcRealm := c.String("vice.keycloak.realm")
	kcClientID := c.String("vice.keycloak.client_id")
	kcClientSecret := c.String("vice.keycloak.client_secret")
	if kcBaseURL == "" || kcRealm == "" || kcClientID == "" || kcClientSecret == "" {
		log.Fatal("vice.keycloak.* settings incomplete; base_url, realm, client_id, and client_secret are all required")
	}
	tokenURL := fmt.Sprintf("%s/realms/%s/protocol/openid-connect/token", kcBaseURL, kcRealm)
	ccConfig := &clientcredentials.Config{
		ClientID:     kcClientID,
		ClientSecret: kcClientSecret,
		TokenURL:     tokenURL,
	}
	tokenSource := ccConfig.TokenSource(context.Background())
	log.Infof("Keycloak OIDC client credentials configured (client_id=%s, token_url=%s)", kcClientID, tokenURL)

	// Initialize and start the status reconciliation worker.
	reconciler := reconciler.New(dbase, app.handlers.GetScheduler(), tokenSource)
	go reconciler.Run(context.Background())

	// Initialize and start the analysis expiration worker, which warns users
	// about analyses nearing their time limit and terminates the ones that pass
	// it. This ran as the standalone `timelord` service before it moved here.
	startExpirationWorker(context.Background(), c, dbase, app.handlers.GetScheduler(), jobStatusBase(c), *expirationSweepInterval, *expiryWarning)

	log.Printf("listening on port %d", *listenPort)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *listenPort), app.router))
}
