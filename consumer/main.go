// consumer subscribes to one target's topic and prints every message it receives, so
// the end-to-end path can be verified: target, log position, transaction id, ordering
// key and body.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/pubsub"
)

func main() {
	project := flag.String("project", envOr("PUBSUB_PROJECT_ID", "outbox-local"), "Pub/Sub project")
	target := flag.String("target", envOr("RELAY_TARGET", "target_1"), "target whose topic to subscribe to")
	topicID := flag.String("topic", envOr("PUBSUB_TOPIC", ""), "topic (default outbox-<target>)")
	subID := flag.String("sub", "", "subscription (default outbox-<target>-printer)")
	flag.Parse()
	if *topicID == "" {
		*topicID = "outbox-" + *target
	}
	if *subID == "" {
		*subID = "outbox-" + *target + "-printer"
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	ps, err := pubsub.NewClient(ctx, *project)
	if err != nil {
		log.Fatal(err)
	}
	defer ps.Close()

	topic := ps.Topic(*topicID)
	if ok, err := topic.Exists(ctx); err != nil {
		log.Fatal(err)
	} else if !ok {
		if topic, err = ps.CreateTopic(ctx, *topicID); err != nil {
			log.Fatal(err)
		}
	}
	sub := ps.Subscription(*subID)
	if ok, err := sub.Exists(ctx); err != nil {
		log.Fatal(err)
	} else if !ok {
		if sub, err = ps.CreateSubscription(ctx, *subID, pubsub.SubscriptionConfig{Topic: topic, EnableMessageOrdering: true}); err != nil {
			log.Fatal(err)
		}
	}

	// One goroutine and one outstanding message so the printed order is the delivery order.
	sub.ReceiveSettings.NumGoroutines = 1
	sub.ReceiveSettings.MaxOutstandingMessages = 1
	n := 0
	log.Printf("waiting for messages on %s/%s", *topicID, *subID)
	err = sub.Receive(ctx, func(_ context.Context, m *pubsub.Message) {
		n++
		fmt.Printf("%5d target=%s lsn=%s xid=%s key=%q %s\n", n, m.Attributes["target"], m.Attributes["pg_lsn"], m.Attributes["pg_xid"], m.OrderingKey, m.Data)
		m.Ack()
	})
	if err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
