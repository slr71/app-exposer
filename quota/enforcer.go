package quota

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"

	"github.com/cyverse-de/app-exposer/apps"
	"github.com/cyverse-de/app-exposer/common"
	"github.com/cyverse-de/go-mod/gotelnats"
	"github.com/cyverse-de/go-mod/pbinit"
	"github.com/cyverse-de/model/v8"
	"github.com/cyverse-de/p/go/qms"
	"github.com/jmoiron/sqlx"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"

	v1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
)

var log = common.Log

func shouldCountStatus(status string) bool {
	countIt := true

	skipStatuses := []string{
		"Failed",
		"Completed",
		"Canceled",
	}

	for _, s := range skipStatuses {
		if status == s {
			countIt = false
		}
	}

	return countIt
}

type Enforcer struct {
	clientset  kubernetes.Interface
	db         *sqlx.DB
	apps       *apps.Apps
	nec        *nats.EncodedConn
	userDomain string
}

func NewEnforcer(clientset kubernetes.Interface, db *sqlx.DB, ec *nats.EncodedConn, userDomain string) *Enforcer {
	return &Enforcer{
		clientset:  clientset,
		db:         db,
		nec:        ec,
		userDomain: userDomain,
	}
}

func (e *Enforcer) countJobsForUser(ctx context.Context, namespace, username string) (int, error) {
	set := labels.Set(map[string]string{
		"username": username,
	})

	listoptions := metav1.ListOptions{
		LabelSelector: set.AsSelector().String(),
	}

	depclient := e.clientset.AppsV1().Deployments(namespace)
	deplist, err := depclient.List(ctx, listoptions)
	if err != nil {
		return 0, err
	}

	countedDeployments := []v1.Deployment{}

	for _, deployment := range deplist.Items {
		var (
			externalID, analysisID, analysisStatus string
			ok                                     bool
		)

		labels := deployment.GetLabels()

		// If we don't have the external-id on the deployment, count it.
		if externalID, ok = labels["external-id"]; !ok {
			countedDeployments = append(countedDeployments, deployment)
			continue
		}

		if analysisID, err = e.apps.GetAnalysisIDByExternalID(ctx, externalID); err != nil {
			// If we failed to get it from the database, count it because it
			// shouldn't be running.
			log.Error(err)
			countedDeployments = append(countedDeployments, deployment)
			continue
		}

		analysisStatus, err = e.apps.GetAnalysisStatus(ctx, analysisID)
		if err != nil {
			// If we failed to get the status, then something is horribly wrong.
			// Count the analysis.
			log.Error(err)
			countedDeployments = append(countedDeployments, deployment)
			continue
		}

		// If the running state is Failed, Completed, or Canceled, don't
		// count it because it's probably in the process of shutting down
		// or the database and the cluster are out of sync which is not
		// the user's fault.
		if shouldCountStatus(analysisStatus) {
			countedDeployments = append(countedDeployments, deployment)
		}
	}

	return len(countedDeployments), nil
}

const getJobLimitForUserSQL = `
	SELECT concurrent_jobs FROM job_limits
	WHERE launcher = regexp_replace($1, '-', '_')
`

func (e *Enforcer) getJobLimitForUser(username string) (*int, error) {
	var jobLimit int
	err := e.db.QueryRow(getJobLimitForUserSQL, username).Scan(&jobLimit)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &jobLimit, nil
}

const getDefaultJobLimitSQL = `
	SELECT concurrent_jobs FROM job_limits
	WHERE launcher IS NULL
`

func (e *Enforcer) getDefaultJobLimit() (int, error) {
	var defaultJobLimit int
	if err := e.db.QueryRow(getDefaultJobLimitSQL).Scan(&defaultJobLimit); err != nil {
		return 0, err
	}
	return defaultJobLimit, nil
}

func (e *Enforcer) getResourceOveragesForUser(ctx context.Context, username string) (*qms.OverageList, error) {
	var err error

	subject := "cyverse.qms.user.overages.get"

	req := &qms.AllUserOveragesRequest{
		Username: common.FixUsername(username, e.userDomain),
	}

	_, span := pbinit.InitAllUserOveragesRequest(req, subject)
	defer span.End()

	resp := pbinit.NewOverageList()

	if err = gotelnats.Request(
		ctx,
		e.nec,
		subject,
		req,
		resp,
	); err != nil {
		return nil, err
	}

	return resp, nil
}

func buildLimitError(code, msg string, defaultJobLimit, jobCount int, jobLimit *int) error {
	return common.ErrorResponse{
		ErrorCode: code,
		Message:   msg,
		Details: &map[string]interface{}{
			"defaultJobLimit": defaultJobLimit,
			"jobCount":        jobCount,
			"jobLimit":        jobLimit,
		},
	}
}

func validateJobLimits(user string, defaultJobLimit, jobCount int, jobLimit *int, overages *qms.OverageList) (int, error) {
	switch {

	// Jobs are disabled by default and the user has not been granted permission yet.
	case jobLimit == nil && defaultJobLimit <= 0:
		code := "ERR_PERMISSION_NEEDED"
		msg := fmt.Sprintf("%s has not been granted permission to run jobs yet", user)
		return http.StatusBadRequest, buildLimitError(code, msg, defaultJobLimit, jobCount, jobLimit)

	// Jobs have been explicitly disabled for the user.
	case jobLimit != nil && *jobLimit <= 0:
		code := "ERR_FORBIDDEN"
		msg := fmt.Sprintf("%s is not permitted to run jobs", user)
		return http.StatusBadRequest, buildLimitError(code, msg, defaultJobLimit, jobCount, jobLimit)

	// The user is using and has reached the default job limit.
	case jobLimit == nil && jobCount >= defaultJobLimit:
		code := "ERR_LIMIT_REACHED"
		msg := fmt.Sprintf("%s is already running %d or more concurrent jobs", user, defaultJobLimit)
		return http.StatusBadRequest, buildLimitError(code, msg, defaultJobLimit, jobCount, jobLimit)

	// The user has explicitly been granted the ability to run jobs and has reached the limit.
	case jobLimit != nil && jobCount >= *jobLimit:
		code := "ERR_LIMIT_REACHED"
		msg := fmt.Sprintf("%s is already running %d or more concurrent jobs", user, *jobLimit)
		return http.StatusBadRequest, buildLimitError(code, msg, defaultJobLimit, jobCount, jobLimit)

	case overages != nil && len(overages.Overages) != 0:
		var inOverage bool
		code := "ERR_RESOURCE_OVERAGE"
		details := make(map[string]interface{})

		for _, ov := range overages.Overages {
			if ov.Usage >= ov.Quota && ov.ResourceName == "cpu.hours" {
				inOverage = true
				details[ov.ResourceName] = fmt.Sprintf("quota: %f, usage: %f", ov.Quota, ov.Usage)
			}
		}

		if inOverage {
			msg := fmt.Sprintf("%s has resource overages.", user)
			return http.StatusBadRequest, common.ErrorResponse{
				ErrorCode: code,
				Message:   msg,
				Details:   &details,
			}
		}

		return http.StatusOK, nil

	// In every other case, we can permit the job to be launched.
	default:
		return http.StatusOK, nil
	}
}

func (e *Enforcer) ValidateJob(ctx context.Context, job *model.Job, namespace string) (int, error) {
	// Get the username
	usernameLabelValue := common.LabelValueString(job.Submitter)
	user := job.Submitter

	// Validate the number of concurrent jobs for the user.
	jobCount, err := e.countJobsForUser(ctx, namespace, usernameLabelValue)
	if err != nil {
		return http.StatusInternalServerError, errors.Wrapf(err, "unable to determine the number of jobs that %s is currently running", user)
	}
	jobLimit, err := e.getJobLimitForUser(user)
	if err != nil {
		return http.StatusInternalServerError, errors.Wrapf(err, "unable to determine the concurrent job limit for %s", user)
	}
	defaultJobLimit, err := e.getDefaultJobLimit()
	if err != nil {
		return http.StatusInternalServerError, errors.Wrapf(err, "unable to determine the default concurrent job limit")
	}
	overages, err := e.getResourceOveragesForUser(ctx, user)
	if err != nil {
		return http.StatusInternalServerError, errors.Wrapf(err, "unable to get list of resource overages for user %s", user)
	}

	return validateJobLimits(user, defaultJobLimit, jobCount, jobLimit, overages)
}
