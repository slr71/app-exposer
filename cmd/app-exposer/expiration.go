package main

import (
	"context"
	"time"

	"github.com/cyverse-de/app-exposer/db"
	"github.com/cyverse-de/app-exposer/expiration"
	"github.com/cyverse-de/app-exposer/incluster"
	"github.com/cyverse-de/app-exposer/iplantgroups"
	"github.com/cyverse-de/app-exposer/notifications"
	"github.com/cyverse-de/app-exposer/operatorclient"
	"github.com/cyverse-de/messaging/v12"
	"github.com/knadh/koanf"
)

// backfillPrefetch is how many job status updates the runtime backfill will
// hold unacknowledged. Matches the prefetch the DE's other status consumers use.
const backfillPrefetch = 100

// jobStatusBase returns the job-status-listener base URL, defaulting to the
// in-cluster service name. Shared by the launch path and the expiration worker
// so both publish analysis status to the same place.
func jobStatusBase(c *koanf.Koanf) string {
	if base := c.String("vice.job-status.base"); base != "" {
		return base
	}
	return "http://job-status-listener"
}

// startExpirationWorker builds and starts the analysis expiration worker along
// with the runtime backfill that feeds it. Configuration problems here are
// fatal: without them the DE would silently stop enforcing analysis time
// limits, which is worse than failing to start.
func startExpirationWorker(
	ctx context.Context,
	c *koanf.Koanf,
	dbase *db.Database,
	scheduler *operatorclient.Scheduler,
	jobStatusURL string,
	sweepInterval, expiryWarning time.Duration,
) {
	groupsBase := c.String("iplant_groups.base")
	if groupsBase == "" {
		groupsBase = "http://iplant-groups"
	}

	groupsUser := c.String("iplant_groups.user")
	if groupsUser == "" {
		log.Fatal("iplant_groups.user must be set in the config file; iplant-groups rejects subject lookups without it, which would leave every analysis notification without an email address")
	}

	subjects, err := iplantgroups.New(groupsBase, groupsUser)
	if err != nil {
		log.Fatal(err)
	}

	notificationAgentBase := c.String("notification_agent.base")
	if notificationAgentBase == "" {
		notificationAgentBase = "http://notification-agent"
	}

	notifier, err := notifications.New(notificationAgentBase, c.String("k8s.frontend.base"), subjects)
	if err != nil {
		log.Fatal(err)
	}

	worker := expiration.New(
		dbase,
		notifier,
		scheduler,
		incluster.NewJSLPublisher(jobStatusURL),
		expiration.Init{
			SweepInterval: sweepInterval,
			ExpiryWarning: expiryWarning,
		},
	)
	go worker.Run(ctx)

	startRuntimeBackfill(ctx, c, dbase)
}

// startRuntimeBackfill subscribes to the DE's job status updates so that any
// interactive analysis reaching Running without a subdomain or planned end date
// gets one. The VICE launch handler is the canonical writer of both; this is the
// safety net for analyses that miss that write.
func startRuntimeBackfill(ctx context.Context, c *koanf.Koanf, dbase *db.Database) {
	amqpURI := c.String("amqp.uri")
	if amqpURI == "" {
		log.Warn("amqp.uri is not set, so the analysis runtime backfill is disabled; " +
			"the VICE launch handler still sets the subdomain and planned end date, but " +
			"analyses that miss that write will not be repaired")
		return
	}

	exchange := c.String("amqp.exchange.name")
	if exchange == "" {
		exchange = "de"
	}

	exchangeType := c.String("amqp.exchange.type")
	if exchangeType == "" {
		exchangeType = "topic"
	}

	backfill := expiration.NewRuntimeBackfill(dbase)

	// Connect in the background: with reconnect enabled, messaging.NewClient
	// blocks until the broker answers, and app-exposer's HTTP API has to come up
	// whether or not RabbitMQ is reachable.
	go func() {
		client, err := messaging.NewClient(amqpURI, true)
		if err != nil {
			log.Errorf("connecting to the AMQP broker for the analysis runtime backfill: %v", err)
			return
		}
		defer client.Close()

		// Listen has to be running before AddConsumer: AddConsumer hands the
		// consumer to Listen's goroutine and blocks until it is registered.
		go client.Listen()

		client.AddConsumer(
			exchange,
			exchangeType,
			expiration.QueueName,
			messaging.UpdatesKey,
			backfill.MessageHandler(),
			backfillPrefetch,
		)

		log.Infof(
			"consuming %s from exchange %s on queue %s for the analysis runtime backfill",
			messaging.UpdatesKey, exchange, expiration.QueueName,
		)

		<-ctx.Done()
	}()
}
