package notifications

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cyverse-de/app-exposer/db"
	"github.com/cyverse-de/app-exposer/iplantgroups"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeSubjects returns a fixed subject, or an error when one is set.
type fakeSubjects struct {
	subject *iplantgroups.Subject
	err     error
	calls   int
}

func (f *fakeSubjects) GetSubject(context.Context, string) (*iplantgroups.Subject, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.subject, nil
}

func timePtr(t time.Time) *time.Time { return &t }

func testAnalysis() *db.Analysis {
	start := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	return &db.Analysis{
		ID:             "analysis-1",
		ExternalID:     "external-1",
		Name:           "my analysis",
		Description:    "does a thing",
		Status:         "Running",
		Username:       "someone@iplantcollaborative.org",
		ResultFolder:   "/iplant/home/someone/analyses/my-analysis",
		Subdomain:      "a1b2c3d4e",
		StartDate:      timePtr(start),
		PlannedEndDate: timePtr(start.Add(72 * time.Hour)),
		NotifyPeriodic: true,
	}
}

// notifierForTest returns a Notifier pointed at a test server that captures the
// request body, along with a pointer to the captured body.
func notifierForTest(t *testing.T, subjects SubjectLookup, status int, frontendBase string) (*Notifier, *[]byte) {
	t.Helper()

	var captured []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		captured = body

		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, notificationPath, r.URL.Path)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))

		w.WriteHeader(status)
	}))
	t.Cleanup(server.Close)

	notifier, err := New(server.URL, frontendBase, subjects)
	require.NoError(t, err)

	return notifier, &captured
}

// TestNotificationWireShape pins the JSON the DE's notification-agent and UI
// consume. The field names are a wire contract shared with other services, so a
// rename here would silently break notifications rather than fail a build.
func TestNotificationWireShape(t *testing.T) {
	subjects := &fakeSubjects{subject: &iplantgroups.Subject{Email: "someone@example.org"}}
	notifier, captured := notifierForTest(t, subjects, http.StatusOK, "https://cyverse.run")

	require.NoError(t, notifier.NotifyTerminated(context.Background(), testAnalysis()))

	var payload map[string]any
	require.NoError(t, json.Unmarshal(*captured, &payload))

	assert.Equal(t, "analysis", payload["type"])
	assert.Equal(t, "someone", payload["user"], "the domain suffix is stripped for notification-agent")
	assert.Equal(t, true, payload["email"])
	assert.Equal(t, statusChangeTemplate, payload["email_template"])
	assert.Contains(t, payload["subject"], "my analysis")
	assert.Contains(t, payload["message"], "my analysis")

	inner, ok := payload["payload"].(map[string]any)
	require.True(t, ok, "payload object present")

	// Every key notification-agent and the DE UI read. Compared as a set so a
	// dropped or renamed field fails loudly.
	wantKeys := []string{
		"analysisid", "analysisname", "analysisdescription", "analysisstatus",
		"startdate", "analysisresultsfolder", "runduration", "endduration",
		"access_url", "email_address", "action", "user",
	}
	gotKeys := make([]string, 0, len(inner))
	for k := range inner {
		gotKeys = append(gotKeys, k)
	}
	assert.ElementsMatch(t, wantKeys, gotKeys)

	assert.Equal(t, "analysis-1", inner["analysisid"])
	assert.Equal(t, "job_status_change", inner["action"])
	assert.Equal(t, "Canceled", inner["analysisstatus"],
		"a termination notice reports Canceled even though the DB row still reads Running")
	assert.Equal(t, "someone@example.org", inner["email_address"])
	assert.Equal(t, "https://a1b2c3d4e.cyverse.run", inner["access_url"])
	// 2026-08-10T12:00:00Z as epoch milliseconds.
	assert.Equal(t, "1786363200000", inner["startdate"], "start date is epoch milliseconds as a string")
}

func TestNotificationTypes(t *testing.T) {
	tests := []struct {
		name          string
		notify        func(*Notifier, *db.Analysis) error
		wantTemplate  string
		wantEmail     bool
		wantStatus    string
		wantSubjectIn string
	}{
		{
			name: "terminated",
			notify: func(n *Notifier, a *db.Analysis) error {
				return n.NotifyTerminated(context.Background(), a)
			},
			wantTemplate:  statusChangeTemplate,
			wantEmail:     true,
			wantStatus:    "Canceled",
			wantSubjectIn: "canceled due to time limit restrictions",
		},
		{
			name: "expiring soon",
			notify: func(n *Notifier, a *db.Analysis) error {
				return n.NotifyExpiringSoon(context.Background(), a)
			},
			wantTemplate:  statusChangeTemplate,
			wantEmail:     true,
			wantStatus:    "Running",
			wantSubjectIn: "will terminate on",
		},
		{
			name: "still running",
			notify: func(n *Notifier, a *db.Analysis) error {
				return n.NotifyStillRunning(context.Background(), a)
			},
			wantTemplate:  periodicReminderTemplate,
			wantEmail:     true,
			wantStatus:    "Running",
			wantSubjectIn: "Your analysis is still running",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subjects := &fakeSubjects{subject: &iplantgroups.Subject{Email: "someone@example.org"}}
			notifier, captured := notifierForTest(t, subjects, http.StatusOK, "https://cyverse.run")

			require.NoError(t, tt.notify(notifier, testAnalysis()))

			var payload struct {
				Subject       string `json:"subject"`
				Email         bool   `json:"email"`
				EmailTemplate string `json:"email_template"`
				Payload       struct {
					AnalysisStatus string `json:"analysisstatus"`
				} `json:"payload"`
			}
			require.NoError(t, json.Unmarshal(*captured, &payload))

			assert.Equal(t, tt.wantTemplate, payload.EmailTemplate)
			assert.Equal(t, tt.wantEmail, payload.Email)
			assert.Equal(t, tt.wantStatus, payload.Payload.AnalysisStatus)
			assert.Contains(t, payload.Subject, tt.wantSubjectIn)
		})
	}
}

// TestStillRunningRespectsNotifyPeriodic verifies that the per-analysis opt-out
// suppresses the email but still records the in-app notification, and that no
// email address is looked up when it isn't needed.
func TestStillRunningRespectsNotifyPeriodic(t *testing.T) {
	subjects := &fakeSubjects{subject: &iplantgroups.Subject{Email: "someone@example.org"}}
	notifier, captured := notifierForTest(t, subjects, http.StatusOK, "https://cyverse.run")

	analysis := testAnalysis()
	analysis.NotifyPeriodic = false

	require.NoError(t, notifier.NotifyStillRunning(context.Background(), analysis))

	var payload struct {
		Email   bool `json:"email"`
		Payload struct {
			Email string `json:"email_address"`
		} `json:"payload"`
	}
	require.NoError(t, json.Unmarshal(*captured, &payload))

	assert.False(t, payload.Email)
	assert.Empty(t, payload.Payload.Email)
	assert.Zero(t, subjects.calls, "no subject lookup is needed when the notification isn't emailed")
}

func TestNotifierErrors(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		subjects   SubjectLookup
		mutate     func(*db.Analysis)
		wantErrMsg string
	}{
		{
			name:       "a non-2xx response is an error",
			status:     http.StatusInternalServerError,
			subjects:   &fakeSubjects{subject: &iplantgroups.Subject{Email: "someone@example.org"}},
			wantErrMsg: "500",
		},
		{
			name:     "a missing planned end date is an error",
			status:   http.StatusOK,
			subjects: &fakeSubjects{subject: &iplantgroups.Subject{}},
			mutate: func(a *db.Analysis) {
				a.PlannedEndDate = nil
			},
			wantErrMsg: "no planned end date",
		},
		{
			name:     "a missing start date is an error",
			status:   http.StatusOK,
			subjects: &fakeSubjects{subject: &iplantgroups.Subject{}},
			mutate: func(a *db.Analysis) {
				a.StartDate = nil
			},
			wantErrMsg: "no start date",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			notifier, _ := notifierForTest(t, tt.subjects, tt.status, "https://cyverse.run")

			analysis := testAnalysis()
			if tt.mutate != nil {
				tt.mutate(analysis)
			}

			err := notifier.NotifyTerminated(context.Background(), analysis)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErrMsg)
		})
	}
}

// TestFailedSubjectLookupStillNotifies pins the degraded path: the DE UI
// notification is how a user learns their analysis is ending, so an email
// address that cannot be resolved downgrades the notification rather than
// dropping it.
func TestFailedSubjectLookupStillNotifies(t *testing.T) {
	notifier, captured := notifierForTest(t, &fakeSubjects{err: assert.AnError}, http.StatusOK, "https://cyverse.run")

	require.NoError(t, notifier.NotifyTerminated(context.Background(), testAnalysis()))

	var sent Notification
	require.NoError(t, json.Unmarshal(*captured, &sent))
	assert.False(t, sent.Email, "the notification should be downgraded to in-app only")
	assert.Empty(t, sent.Payload.Email)
}

func TestAccessURL(t *testing.T) {
	tests := []struct {
		name         string
		frontendBase string
		subdomain    string
		want         string
	}{
		{
			name:         "built from the frontend base and the subdomain",
			frontendBase: "https://cyverse.run",
			subdomain:    "a1b2c3d4e",
			want:         "https://a1b2c3d4e.cyverse.run",
		},
		{
			name:         "port is preserved",
			frontendBase: "https://cyverse.run:4343",
			subdomain:    "a1b2c3d4e",
			want:         "https://a1b2c3d4e.cyverse.run:4343",
		},
		{
			name:         "omitted when no frontend base is configured",
			frontendBase: "",
			subdomain:    "a1b2c3d4e",
			want:         "",
		},
		{
			name:         "omitted when the analysis has no subdomain yet",
			frontendBase: "https://cyverse.run",
			subdomain:    "",
			want:         "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			notifier, err := New("http://notification-agent", tt.frontendBase, &fakeSubjects{})
			require.NoError(t, err)

			analysis := testAnalysis()
			analysis.Subdomain = tt.subdomain

			assert.Equal(t, tt.want, notifier.accessURL(analysis))
		})
	}
}

func TestShortDuration(t *testing.T) {
	tests := []struct {
		name string
		d    time.Duration
		want string
	}{
		{name: "zero", d: 0, want: "0:00"},
		{name: "minutes only", d: 42 * time.Minute, want: "0:42"},
		{name: "rounds to the nearest minute", d: 42*time.Minute + 31*time.Second, want: "0:43"},
		{name: "single-digit hours", d: 3*time.Hour + 5*time.Minute, want: "3:05"},
		{name: "multi-digit hours", d: 72*time.Hour + 15*time.Minute, want: "72:15"},

		// The termination notice reports the time remaining on an analysis
		// that is already past its planned end date, so negative durations
		// reach users and must carry a single leading sign.
		{name: "minutes past the planned end date", d: -15 * time.Minute, want: "-0:15"},
		{name: "hours past the planned end date", d: -(time.Hour + 30*time.Minute), want: "-1:30"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, shortDuration(tt.d))
		})
	}
}
