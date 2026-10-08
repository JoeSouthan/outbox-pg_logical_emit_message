// tablerelay is the conventional outbox relay used as the benchmark baseline. In a
// transaction it claims a batch of rows for one target with FOR UPDATE SKIP LOCKED,
// deletes them, publishes to Pub/Sub, and commits only after the broker has accepted
// every message. A crash before commit rolls the delete back so the rows are
// republished: at-least-once, same contract as the log-message relay.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/jackc/pgx/v5"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	dsn := flag.String("dsn", envOr("RELAY_DSN", "postgres://outbox:outbox@postgres:5432/outbox"), "database DSN")
	target := flag.String("target", envOr("RELAY_TARGET", "target_1"), "outbox_items.target this relay serves")
	project := flag.String("project", envOr("PUBSUB_PROJECT_ID", "outbox-local"), "Pub/Sub project")
	topicID := flag.String("topic", envOr("PUBSUB_TOPIC", ""), "Pub/Sub topic (default outbox-<target>)")
	batch := flag.Int("batch", 1000, "rows per transaction")
	sinkName := flag.String("sink", envOr("RELAY_SINK", "simulated"), "where to publish: simulated | pubsub")
	sinkLatency := flag.Duration("sink-latency", envDurationOr("RELAY_SINK_LATENCY", 200*time.Millisecond), "simulated sink: cost of one publish batch")
	poll := flag.Duration("poll", 100*time.Millisecond, "sleep when the table is empty")
	flag.Parse()
	if *topicID == "" {
		*topicID = "outbox-" + *target
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var topic *pubsub.Topic // nil means the simulated sink
	switch *sinkName {
	case "pubsub":
		ps, err := pubsub.NewClient(ctx, *project)
		if err != nil {
			log.Fatal(err)
		}
		defer ps.Close()
		topic = ps.Topic(*topicID)
		if ok, err := topic.Exists(ctx); err != nil {
			log.Fatal(err)
		} else if !ok {
			if created, err := ps.CreateTopic(ctx, *topicID); err == nil {
				topic = created
			} else if status.Code(err) != codes.AlreadyExists {
				log.Fatal(err)
			}
		}
		topic.EnableMessageOrdering = true
		topic.PublishSettings.CountThreshold = *batch
		topic.PublishSettings.DelayThreshold = 50 * time.Millisecond
		defer topic.Stop()
	case "simulated":
		log.Printf("sink=simulated: %s per publish batch, nothing is sent", *sinkLatency)
	default:
		log.Fatalf("unknown sink %q (pubsub | simulated)", *sinkName)
	}

	conn, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(context.Background())
	log.Printf("relaying outbox_items target=%s topic=%s batch=%d", *target, *topicID, *batch)

	// The claim is a MATERIALIZED CTE joined into the DELETE so the locking subquery is
	// evaluated exactly once. Written as `id IN (subquery)` the planner can choose to
	// rescan the subquery per candidate row, which with stale statistics after a bulk
	// load turns a 1,000-row claim into minutes of CPU.
	const claim = `
		WITH claimed AS MATERIALIZED (
			SELECT id FROM outbox_items
			WHERE target = $1
			ORDER BY id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		DELETE FROM outbox_items o
		USING claimed c
		WHERE o.id = c.id
		RETURNING o.id, o.payload`

	total := 0
	for ctx.Err() == nil {
		n, err := relayBatch(ctx, conn, topic, *sinkLatency, claim, *target, *batch)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			log.Printf("batch failed, will retry: %v", err)
			time.Sleep(*poll)
			continue
		}
		if n == 0 {
			time.Sleep(*poll)
			continue
		}
		total += n
		log.Printf("published %d (table) total=%d", n, total)
	}
}

func relayBatch(ctx context.Context, conn *pgx.Conn, topic *pubsub.Topic, simLatency time.Duration, claim, target string, batch int) (int, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(context.Background())

	rows, err := tx.Query(ctx, claim, target, batch)
	if err != nil {
		return 0, err
	}
	var results []*pubsub.PublishResult
	n := 0
	for rows.Next() {
		var id int64
		var payload string
		if err := rows.Scan(&id, &payload); err != nil {
			rows.Close()
			return 0, err
		}
		// Same ordering-key behaviour as the log-message relay, so the two relays are
		// comparable: the payload's "key" orders delivery per aggregate.
		var probe struct {
			Key string `json:"key"`
		}
		_ = json.Unmarshal([]byte(payload), &probe)
		msg := &pubsub.Message{
			Data:        []byte(payload),
			Attributes:  map[string]string{"outbox_id": fmt.Sprint(id), "target": target},
			OrderingKey: probe.Key,
		}
		n++
		if topic != nil {
			results = append(results, topic.Publish(ctx, msg))
		}
	}
	rows.Close()
	if rows.Err() != nil {
		return 0, rows.Err()
	}
	if n == 0 {
		return 0, nil
	}
	if topic == nil {
		// Simulated Pub/Sub round trip for this batch.
		select {
		case <-time.After(simLatency):
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	for _, r := range results {
		if _, err := r.Get(ctx); err != nil {
			return 0, err
		}
	}
	// Everything is in Pub/Sub; now the delete may become permanent.
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return n, nil
}

func envDurationOr(k string, d time.Duration) time.Duration {
	if v := os.Getenv(k); v != "" {
		if p, err := time.ParseDuration(v); err == nil {
			return p
		}
	}
	return d
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
