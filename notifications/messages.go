package notifications

import (
	"fmt"
	"time"
)

// Message formats for the notifications the DE sends about a VICE analysis's
// remaining runtime. The wording is part of the user-facing contract; changing
// it changes the emails users receive.
const (
	killMessageFormat = `Analysis "%s" (%s) had a configured end date of "%s" (%s), which has passed.

Output files should be available in the %s folder in iRODS.`

	killSubjectFormat = "Analysis %s canceled due to time limit restrictions."

	warningMessageFormat = `Analysis "%s" (%s) is set to expire on "%s" (%s).

Please finish any work that is in progress. Output files will be transferred to the %s folder in iRODS when the application shuts down.`

	warningSubjectFormat = "Analysis %s will terminate on %s (%s)."

	// Parameters: analysis name, elapsed duration, duration until the planned end date.
	periodicMessageFormat = `Analysis "%s" has been running for %s and will stop in %s.`

	// Carries a timestamp so mail clients don't thread successive reminders together.
	periodicSubjectFormat = `CyVerse: Your analysis is still running (%s)`
)

// Email templates the notification-agent renders for each notification type.
const (
	statusChangeTemplate     = "analysis_status_change"
	periodicReminderTemplate = "analysis_periodic_notification"
)

// endTimeFormats renders a planned end date the two ways every message quotes
// it: in the DE's configured local zone and again in UTC.
func endTimeFormats(plannedEnd time.Time) (local, utc string) {
	return plannedEnd.Format("Mon Jan 2 15:04:05 -0700 MST 2006"), plannedEnd.UTC().Format(time.UnixDate)
}

// shortDuration renders d as H:MM, the format the DE's analysis emails use for
// both elapsed and remaining time.
func shortDuration(d time.Duration) string {
	d = d.Round(time.Minute)
	hours := d / time.Hour
	d -= hours * time.Hour
	return fmt.Sprintf("%d:%02d", hours, d/time.Minute)
}
