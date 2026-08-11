package db

import "time"

// The DE stores analysis timestamps — jobs.start_date, jobs.planned_end_date,
// notif_statuses.last_periodic_warning — in `timestamp without time zone`
// columns holding wall-clock time in the deployment's zone. Every service that
// writes them takes its zone from the same TZ setting, so the columns are
// consistent with each other but carry no offset of their own.
//
// That makes the database session's TimeZone a trap. Comparing one of these
// columns against now() converts it using the session zone, which is whatever
// the server happens to be configured for (Etc/UTC, typically) rather than the
// zone the value was written in, and reading one back through lib/pq yields a
// time.Time labeled UTC for the same reason. Either way every instant lands off
// by the local UTC offset — enough, on a US deployment against a UTC database,
// to terminate an analysis hours before its time limit.
//
// This package therefore keeps the session zone out of it entirely. Comparisons
// pass a Go instant and cast the parameter with ::timestamp — never
// ::timestamptz — so Postgres drops the offset and both sides of the comparison
// are the deployment's wall clock. Values read back are relabeled by
// InLocalZone.

// InLocalZone reinterprets a naive timestamp's wall-clock fields as local time,
// which is the zone the DE wrote them in. Exported for the callers outside this
// package that read one of these columns directly — there is one rule here, and
// a second implementation of it would drift.
func InLocalZone(t *time.Time) *time.Time {
	return inZone(t, time.Local)
}

// inZone relabels a naive timestamp's wall-clock fields as being in loc, without
// shifting them. nil passes through, since these columns are nullable.
func inZone(t *time.Time, loc *time.Location) *time.Time {
	if t == nil {
		return nil
	}

	relabeled := time.Date(
		t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second(), t.Nanosecond(),
		loc,
	)
	return &relabeled
}
