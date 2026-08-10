// Package notifications sends the DE's user-facing notifications about VICE
// analysis runtime — the one-day and one-hour expiry warnings, the periodic
// "still running" reminder, and the notice that an analysis was terminated for
// exceeding its time limit. Notifications are POSTed to the notification-agent
// service, which fans them out to the DE UI and to email.
package notifications

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/cyverse-de/app-exposer/common"
	"github.com/cyverse-de/app-exposer/db"
	"github.com/cyverse-de/app-exposer/iplantgroups"
	"github.com/sirupsen/logrus"
)

var log = common.Log.WithFields(logrus.Fields{"package": "notifications"})

// DefaultTimeout bounds a single POST to notification-agent.
const DefaultTimeout = 30 * time.Second

// notificationPath is the notification-agent endpoint that accepts a
// notification for delivery.
const notificationPath = "/notification"

// Payload is the analysis-specific body carried by an analysis notification.
// The JSON field names are the wire contract with notification-agent and the
// DE UI; do not rename them.
type Payload struct {
	AnalysisID            string `json:"analysisid"`
	AnalysisName          string `json:"analysisname"`
	AnalysisDescription   string `json:"analysisdescription"`
	AnalysisStatus        string `json:"analysisstatus"`
	StartDate             string `json:"startdate"`
	AnalysisResultsFolder string `json:"analysisresultsfolder"`
	RunDuration           string `json:"runduration"`
	EndDuration           string `json:"endduration"`
	AccessURL             string `json:"access_url"`
	Email                 string `json:"email_address"`
	Action                string `json:"action"`
	User                  string `json:"user"`
}

// Notification is the message POSTed to notification-agent.
type Notification struct {
	Type          string   `json:"type"`
	User          string   `json:"user"`
	Subject       string   `json:"subject"`
	Message       string   `json:"message"`
	Email         bool     `json:"email"`
	EmailTemplate string   `json:"email_template"`
	Payload       *Payload `json:"payload"`
}

// SubjectLookup resolves a DE username to the profile fields a notification
// needs. *iplantgroups.Client satisfies it.
type SubjectLookup interface {
	GetSubject(ctx context.Context, username string) (*iplantgroups.Subject, error)
}

// AnalysisNotifier sends the notifications the DE emits about a VICE analysis's
// remaining runtime. *Notifier is the production implementation; the interface
// exists so the background worker that drives these can be tested without a
// notification-agent.
type AnalysisNotifier interface {
	// NotifyTerminated reports that the analysis was canceled for exceeding
	// its time limit.
	NotifyTerminated(ctx context.Context, analysis *db.Analysis) error

	// NotifyExpiringSoon warns that the analysis is about to expire.
	NotifyExpiringSoon(ctx context.Context, analysis *db.Analysis) error

	// NotifyStillRunning is the periodic reminder that the analysis is up.
	NotifyStillRunning(ctx context.Context, analysis *db.Analysis) error
}

// Compile-time check that *Notifier satisfies the interface its consumers use.
var _ AnalysisNotifier = (*Notifier)(nil)

// Notifier builds and sends analysis notifications.
type Notifier struct {
	notificationURL *url.URL
	frontendBaseURL *url.URL
	subjects        SubjectLookup
	httpClient      *http.Client
}

// New returns a Notifier that POSTs to the notification-agent at
// notificationAgentBase. frontendBaseURL is the DE's VICE base URL, used to
// build the access URL included in each notification; an empty value omits the
// access URL. subjects resolves the recipient's email address.
func New(notificationAgentBase, frontendBaseURL string, subjects SubjectLookup) (*Notifier, error) {
	notifURL, err := url.Parse(notificationAgentBase)
	if err != nil {
		return nil, fmt.Errorf("parsing notification-agent base URL %q: %w", notificationAgentBase, err)
	}

	n := &Notifier{
		notificationURL: notifURL.JoinPath(notificationPath),
		subjects:        subjects,
		httpClient:      &http.Client{Timeout: DefaultTimeout},
	}

	if frontendBaseURL != "" {
		frontURL, err := url.Parse(frontendBaseURL)
		if err != nil {
			return nil, fmt.Errorf("parsing frontend base URL %q: %w", frontendBaseURL, err)
		}
		n.frontendBaseURL = frontURL
	}

	return n, nil
}

// NotifyTerminated tells the user their analysis was canceled for running past
// its planned end date.
func (n *Notifier) NotifyTerminated(ctx context.Context, analysis *db.Analysis) error {
	if analysis.PlannedEndDate == nil {
		return fmt.Errorf("analysis %s has no planned end date; cannot build termination notification", analysis.ID)
	}

	endLocal, endUTC := endTimeFormats(*analysis.PlannedEndDate)
	subject := fmt.Sprintf(killSubjectFormat, analysis.Name)
	message := fmt.Sprintf(
		killMessageFormat,
		analysis.Name,
		analysis.ID,
		endLocal,
		endUTC,
		analysis.ResultFolder,
	)

	// "Canceled" rather than the analysis's current status: the DB row still
	// reads Running at the point the termination notice goes out.
	return n.send(ctx, analysis, "Canceled", subject, message, true, statusChangeTemplate)
}

// NotifyExpiringSoon warns the user that their analysis is about to hit its
// planned end date.
func (n *Notifier) NotifyExpiringSoon(ctx context.Context, analysis *db.Analysis) error {
	if analysis.PlannedEndDate == nil {
		return fmt.Errorf("analysis %s has no planned end date; cannot build expiry warning", analysis.ID)
	}

	endLocal, endUTC := endTimeFormats(*analysis.PlannedEndDate)
	subject := fmt.Sprintf(warningSubjectFormat, analysis.Name, endLocal, endUTC)
	message := fmt.Sprintf(
		warningMessageFormat,
		analysis.Name,
		analysis.ID,
		endLocal,
		endUTC,
		analysis.ResultFolder,
	)

	return n.send(ctx, analysis, analysis.Status, subject, message, true, statusChangeTemplate)
}

// NotifyStillRunning sends the periodic reminder that a long-running analysis
// is still up. Emailing is opt-out per analysis via the submission's
// notify_periodic flag.
func (n *Notifier) NotifyStillRunning(ctx context.Context, analysis *db.Analysis) error {
	elapsed, remaining, err := n.durations(analysis)
	if err != nil {
		return err
	}

	subject := fmt.Sprintf(periodicSubjectFormat, time.Now().Format("2006-01-02 15:04"))
	message := fmt.Sprintf(periodicMessageFormat, analysis.Name, elapsed, remaining)

	return n.send(ctx, analysis, analysis.Status, subject, message, analysis.NotifyPeriodic, periodicReminderTemplate)
}

// durations returns the analysis's elapsed runtime and the time remaining
// before its planned end date, both rendered as H:MM.
func (n *Notifier) durations(analysis *db.Analysis) (elapsed, remaining string, err error) {
	if analysis.StartDate == nil {
		return "", "", fmt.Errorf("analysis %s has no start date", analysis.ID)
	}
	if analysis.PlannedEndDate == nil {
		return "", "", fmt.Errorf("analysis %s has no planned end date", analysis.ID)
	}
	return shortDuration(time.Since(*analysis.StartDate)),
		shortDuration(time.Until(*analysis.PlannedEndDate)),
		nil
}

// send assembles the notification for an analysis and POSTs it. The recipient's
// email address is resolved from iplant-groups only when the notification is
// meant to be emailed.
func (n *Notifier) send(ctx context.Context, analysis *db.Analysis, status, subject, message string, email bool, emailTemplate string) error {
	elapsed, remaining, err := n.durations(analysis)
	if err != nil {
		return err
	}

	recipient := iplantgroups.ParseID(analysis.Username)

	payload := &Payload{
		Action:                "job_status_change",
		AnalysisID:            string(analysis.ID),
		AnalysisName:          analysis.Name,
		AnalysisDescription:   analysis.Description,
		AnalysisStatus:        status,
		StartDate:             strconv.FormatInt(analysis.StartDate.UnixMilli(), 10),
		AnalysisResultsFolder: analysis.ResultFolder,
		RunDuration:           elapsed,
		EndDuration:           remaining,
		AccessURL:             n.accessURL(analysis),
		User:                  recipient,
	}

	if email {
		subj, err := n.subjects.GetSubject(ctx, analysis.Username)
		if err != nil {
			return fmt.Errorf("resolving email address for %s: %w", analysis.Username, err)
		}
		payload.Email = subj.Email
	}

	notification := &Notification{
		Type:          "analysis",
		User:          recipient,
		Subject:       subject,
		Message:       message,
		Email:         email,
		EmailTemplate: emailTemplate,
		Payload:       payload,
	}

	body, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("marshaling notification for analysis %s: %w", analysis.ID, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.notificationURL.String(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("building notification request for analysis %s: %w", analysis.ID, err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("posting notification for analysis %s: %w", analysis.ID, err)
	}
	defer common.CloseBody(resp)

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("notification for analysis %s returned %s", analysis.ID, resp.Status)
	}

	log.Infof("sent %q notification for analysis %s (email=%t)", subject, analysis.ID, email)
	return nil
}

// accessURL returns the user-facing URL of a VICE analysis, or "" when either
// no frontend base URL is configured or the analysis has no subdomain yet.
func (n *Notifier) accessURL(analysis *db.Analysis) string {
	if n.frontendBaseURL == nil || analysis.Subdomain == "" {
		return ""
	}
	accessURL := *n.frontendBaseURL
	accessURL.Host = analysis.Subdomain + "." + accessURL.Host
	return accessURL.String()
}
