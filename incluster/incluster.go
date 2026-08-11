package incluster

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/cockroachdb/apd"
	"github.com/cyverse-de/app-exposer/apps"
	"github.com/cyverse-de/app-exposer/common"
	"github.com/cyverse-de/app-exposer/constants"
	"github.com/cyverse-de/app-exposer/db"
	"github.com/cyverse-de/app-exposer/incluster/jobinfo"
	"github.com/cyverse-de/app-exposer/operatorclient"
	"github.com/cyverse-de/app-exposer/quota"

	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/cyverse-de/model/v10"
	appsv1 "k8s.io/api/apps/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	gatewayclient "sigs.k8s.io/gateway-api/pkg/client/clientset/versioned/typed/apis/v1"
)

const otelName = "github.com/cyverse-de/app-exposer/incluster"

var log = common.Log
var httpClient = http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}

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
	ViceDomain                    string
	VICEBackendNamespace          string
	AppsServiceBaseURL            string
	ViceNamespace                 string
	JobStatusURL                  string
	UserSuffix                    string
	PermissionsURL                string
	IRODSZone                     string
	GatewayProvider               string
	ClusterConfigSecretName       string
	SubscriptionsURL              *url.URL
	BypassUsers                   []string
	TimeLimitExtensionSeconds     int64
}

// Incluster contains information and operations for launching VICE apps inside the
// local k8s cluster.
type Incluster struct {
	Init
	clientset       kubernetes.Interface
	gatewayClient   *gatewayclient.GatewayV1Client
	db              *sqlx.DB
	statusPublisher AnalysisStatusPublisher
	apps            *apps.Apps
	quotaEnforcer   *quota.Enforcer
	jobInfo         jobinfo.JobInfo
}

// New creates a new *Incluster.
func New(init *Init, db *sqlx.DB, clientset kubernetes.Interface, gatewayClient *gatewayclient.GatewayV1Client, apps *apps.Apps) *Incluster {
	return &Incluster{
		Init:          *init,
		db:            db,
		clientset:     clientset,
		gatewayClient: gatewayClient,
		statusPublisher: &JSLPublisher{
			statusURL: init.JobStatusURL,
		},
		apps:          apps,
		quotaEnforcer: quota.NewEnforcer(clientset, db, apps, init.SubscriptionsURL, init.UserSuffix),
		jobInfo:       jobinfo.NewJobInfo(apps),
	}
}

// SetScheduler configures the operator scheduler for multi-cluster operations.
func (i *Incluster) SetScheduler(s *operatorclient.Scheduler) {
	i.quotaEnforcer.SetScheduler(s)
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

// GetMillicoresFromDeployment extracts the CPU limit from the analysis
// container in the given Deployment and converts it to millicores.
func GetMillicoresFromDeployment(deployment *appsv1.Deployment) (*apd.Decimal, error) {
	var (
		analysisContainer *apiv1.Container
		millicores        *apd.Decimal
		millicoresString  string
		err               error
	)
	containers := deployment.Spec.Template.Spec.Containers

	found := false

	for _, container := range containers {
		if container.Name == constants.AnalysisContainerName {
			analysisContainer = &container
			found = true
			break
		}
	}

	if !found {
		return nil, errors.New("could not find the analysis container in the deployment")
	}

	cpuQty, ok := analysisContainer.Resources.Limits[apiv1.ResourceCPU]
	if !ok {
		return nil, errors.New("analysis container has no CPU limit set")
	}
	millicoresString = cpuQty.ToUnstructured().(string)

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

// Both statements return planned_end_date as stored rather than converting it to
// an epoch in SQL. The column is naive and holds wall-clock time in the
// deployment's zone, so any conversion has to name that zone; doing it with
// current_setting('TimeZone') used the database session's zone instead and
// returned an epoch off by the difference between the two — seven hours on a US
// deployment against a UTC database. plannedEndEpoch converts it correctly.

const updateTimeLimitSQL = `
	UPDATE ONLY jobs
	   SET planned_end_date = old_value.planned_end_date + interval '1 second' * $3
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

// TimeLimit holds the epoch timestamp (as a string) for when a VICE analysis
// will reach its planned end date.
type TimeLimit struct {
	TimeLimit string `json:"time_limit"`
}

// plannedEndEpoch converts a planned end date read from the naive jobs column
// into a Unix epoch, reporting false when the column is NULL. The relabeling is
// shared with the expiration worker so the two cannot disagree about what these
// timestamps mean — see db/timestamps.go.
//
// Rounded rather than truncated because the column carries fractional seconds
// and the EXTRACT(EPOCH ...)::bigint this replaced rounded them. The zone was the
// bug; the second this API reports should not move with the fix.
func plannedEndEpoch(plannedEnd sql.NullTime) (int64, bool) {
	if !plannedEnd.Valid {
		return 0, false
	}
	return db.InLocalZone(&plannedEnd.Time).Round(time.Second).Unix(), true
}

// GetTimeLimit returns the planned end date (as a Unix epoch string) for the
// analysis with the given ID, run by the given user.
func (i *Incluster) GetTimeLimit(ctx context.Context, userID string, id constants.AnalysisID) (*TimeLimit, error) {
	var err error

	var plannedEnd sql.NullTime
	if err = i.db.QueryRowContext(ctx, getTimeLimitSQL, userID, id).Scan(&plannedEnd); err != nil {
		return nil, errors.Wrapf(err, "error retrieving time limit for user %s on analysis %s", userID, id)
	}

	retval := &TimeLimit{}
	if epoch, ok := plannedEndEpoch(plannedEnd); ok {
		retval.TimeLimit = fmt.Sprintf("%d", epoch)
	} else {
		retval.TimeLimit = "null"
	}

	return retval, nil
}

// UpdateTimeLimit extends the planned end date for the given analysis by the
// configured time limit extension duration and returns the new epoch value.
func (i *Incluster) UpdateTimeLimit(ctx context.Context, user string, id constants.AnalysisID) (*TimeLimit, error) {
	var (
		err    error
		userID string
	)

	if !strings.HasSuffix(user, i.UserSuffix) {
		user = fmt.Sprintf("%s%s", user, i.UserSuffix)
	}

	if err = i.db.QueryRowContext(ctx, getUserIDSQL, user).Scan(&userID); err != nil {
		return nil, errors.Wrapf(err, "error looking user ID for %s", user)
	}

	var plannedEnd sql.NullTime
	if err = i.db.QueryRowContext(ctx, updateTimeLimitSQL, userID, id, i.TimeLimitExtensionSeconds).Scan(&plannedEnd); err != nil {
		return nil, errors.Wrapf(err, "error extending time limit for user %s on analysis %s", userID, id)
	}

	retval := &TimeLimit{}
	epoch, ok := plannedEndEpoch(plannedEnd)
	if !ok {
		return nil, fmt.Errorf("the time limit for analysis %s was null after extension", id)
	}
	retval.TimeLimit = fmt.Sprintf("%d", epoch)

	return retval, nil
}

// isUserInBypassWhitelist checks if the given username is in the resource tracking bypass whitelist.
func (i *Incluster) isUserInBypassWhitelist(username string) bool {
	normalizedUser := common.FixUsername(username, i.UserSuffix)
	return slices.Contains(i.BypassUsers, normalizedUser)
}

// ValidateJob checks that the analysis does not exceed quota limits,
// bypassing the check for users in the configured bypass whitelist.
// Returns an HTTP status code and error when validation fails.
func (i *Incluster) ValidateJob(ctx context.Context, job *model.Job) (int, error) {
	if i.isUserInBypassWhitelist(job.Submitter) {
		log.Infof("Resource tracking disabled for user %s (in bypass whitelist), skipping validation for job %s", job.Submitter, job.InvocationID)
		return http.StatusOK, nil
	}
	log.Infof("Resource tracking enabled for user %s, validating job %s", job.Submitter, job.InvocationID)
	return i.quotaEnforcer.ValidateJob(ctx, job, i.ViceNamespace)
}
