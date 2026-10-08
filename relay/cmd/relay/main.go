// relay streams pg_logical_emit_message records from a logical replication slot and
// publishes them to Pub/Sub in batches. One relay serves one target: it keeps only
// messages whose prefix equals the target, uses a slot named after the target, and
// publishes to that target's topic. Other targets' messages are decoded and skipped,
// so each target costs one slot and one pass over the stream. It acknowledges WAL back to Postgres only after
// a batch has been published, so a crash replays from the last confirmed commit:
// at-least-once delivery, in commit order.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type config struct {
	sink        string
	sinkLatency time.Duration
	dsn         string
	journal     bool
	journalDSN  string
	stateDir    string
	stateFile   string
	slot        string
	publication string
	prefix      string
	project     string
	topic       string
	batchSize   int
	flushEvery  time.Duration
	statusEvery time.Duration
}

type pending struct {
	// Messages decoded but not yet published.
	msgs []*pubsub.Message
	// Commit LSN of the last transaction whose messages are all in msgs. Only whole
	// transactions are acknowledged, so a batch boundary mid-transaction leaves this at
	// the previous commit.
	ackLSN pglogrepl.LSN
	// Commit LSN of the transaction currently being decoded.
	inTx bool
}

func main() {
	var cfg config
	flag.StringVar(&cfg.dsn, "dsn", env("RELAY_DSN", "postgres://outbox:outbox@postgres:5432/outbox?replication=database"), "replication DSN (must include replication=database, must not go via PgBouncer)")
	flag.StringVar(&cfg.prefix, "target", env("RELAY_TARGET", "target_1"), "target this relay serves: the logical-message prefix")
	flag.BoolVar(&cfg.journal, "journal", env("RELAY_JOURNAL", "") == "true", "recover a lost slot by replaying outbox_journal from the last acknowledged position")
	flag.StringVar(&cfg.journalDSN, "journal-dsn", env("RELAY_JOURNAL_DSN", "postgres://outbox:outbox@postgres:5432/outbox"), "ordinary (non-replication) DSN used to read outbox_journal")
	flag.StringVar(&cfg.stateDir, "state-dir", env("RELAY_STATE_DIR", "."), "directory for the per-target state file holding the last acknowledged LSN")
	flag.StringVar(&cfg.slot, "slot", env("RELAY_SLOT", ""), "replication slot name (default outbox_relay_<target>)")
	flag.StringVar(&cfg.publication, "publication", env("RELAY_PUBLICATION", "outbox_pub"), "pgoutput publication name")
	flag.StringVar(&cfg.project, "project", env("PUBSUB_PROJECT_ID", "outbox-local"), "Pub/Sub project")
	flag.StringVar(&cfg.topic, "topic", env("PUBSUB_TOPIC", ""), "Pub/Sub topic (default outbox-<target>)")
	flag.StringVar(&cfg.sink, "sink", env("RELAY_SINK", "simulated"), "where to publish: simulated (fixed latency per batch, nothing sent) | pubsub (real project; needs credentials)")
	flag.DurationVar(&cfg.sinkLatency, "sink-latency", envDuration("RELAY_SINK_LATENCY", 200*time.Millisecond), "simulated sink: cost of one publish batch, standing in for a Pub/Sub round trip")
	flag.IntVar(&cfg.batchSize, "batch", 1000, "publish when this many messages are buffered")
	flag.DurationVar(&cfg.flushEvery, "flush", 500*time.Millisecond, "publish buffered messages at least this often")
	flag.DurationVar(&cfg.statusEvery, "status", 5*time.Second, "send standby status to Postgres at least this often")
	flag.Parse()
	if cfg.slot == "" {
		cfg.slot = "outbox_relay_" + cfg.prefix
	}
	if cfg.topic == "" {
		cfg.topic = "outbox-" + cfg.prefix
	}
	cfg.stateFile = filepath.Join(cfg.stateDir, "relay_"+cfg.prefix+".lsn")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("relay: %v", err)
	}
}

func run(ctx context.Context, cfg config) error {
	topic, err := newSink(ctx, cfg)
	if err != nil {
		return err
	}
	defer topic.Close()

	conn, err := pgconn.Connect(ctx, cfg.dsn)
	if err != nil {
		return fmt.Errorf("replication connect: %w", err)
	}
	defer conn.Close(context.Background())

	lastAcked, hasState := readState(cfg.stateFile)
	startLSN, created, err := ensureSlot(ctx, conn, cfg.slot)
	if err != nil {
		return err
	}
	published := 0
	if created && hasState {
		// The slot is gone but we know where we got to. Everything committed between
		// lastAcked and the new slot's consistent point was never streamed to us.
		if !cfg.journal {
			log.Printf("WARNING slot %s was missing; last acknowledged %s, new slot starts at %s: events in that window are lost (no -journal)", cfg.slot, lastAcked, startLSN)
		} else {
			n, err := replayJournal(ctx, cfg, topic, lastAcked, startLSN)
			if err != nil {
				return fmt.Errorf("journal replay: %w", err)
			}
			published += n
			log.Printf("replayed %d from outbox_journal for window (%s, %s] total=%d", n, lastAcked, startLSN, published)
		}
	}
	if !created && hasState {
		log.Printf("state file says last acknowledged %s; slot will resume from its own confirmed position", lastAcked)
	}

	// proto_version 2 is the first that carries logical decoding messages; messages 'true'
	// asks pgoutput to include them. Both need Postgres 14+.
	pluginArgs := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", cfg.publication),
		"messages 'true'",
	}
	if err := pglogrepl.StartReplication(ctx, conn, cfg.slot, startLSN, pglogrepl.StartReplicationOptions{PluginArgs: pluginArgs}); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}
	log.Printf("streaming target=%s slot=%s from %s publication=%s topic=%s", cfg.prefix, cfg.slot, startLSN, cfg.publication, cfg.topic)

	p := &pending{ackLSN: startLSN}
	var (
		confirmed  = startLSN // last LSN we told Postgres it may discard
		nextStatus = time.Now().Add(cfg.statusEvery)
		nextFlush  = time.Now().Add(cfg.flushEvery)
		currentXid uint32 // xid of the transaction being decoded, from its BEGIN
	)
	if created {
		// A fresh slot: nothing before its consistent point can ever be streamed, so
		// that is our acknowledged position from now on.
		writeState(cfg.stateFile, startLSN)
	}

	flush := func(reason string) error {
		if len(p.msgs) == 0 {
			return nil
		}
		if err := topic.PublishAll(ctx, p.msgs); err != nil {
			return fmt.Errorf("publish: %w", err)
		}
		published += len(p.msgs)
		log.Printf("published %d (%s) total=%d ack=%s", len(p.msgs), reason, published, p.ackLSN)
		p.msgs = p.msgs[:0]
		// Everything up to ackLSN is now durable in Pub/Sub, so Postgres may discard it.
		if p.ackLSN > confirmed {
			confirmed = p.ackLSN
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: confirmed}); err != nil {
				return fmt.Errorf("status update: %w", err)
			}
			writeState(cfg.stateFile, confirmed)
			nextStatus = time.Now().Add(cfg.statusEvery)
		}
		return nil
	}

	for {
		if time.Now().After(nextFlush) {
			if err := flush("timer"); err != nil {
				return err
			}
			nextFlush = time.Now().Add(cfg.flushEvery)
		}
		if time.Now().After(nextStatus) {
			if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: confirmed}); err != nil {
				return fmt.Errorf("status update: %w", err)
			}
			nextStatus = time.Now().Add(cfg.statusEvery)
		}

		deadline := nextFlush
		if nextStatus.Before(deadline) {
			deadline = nextStatus
		}
		rctx, cancel := context.WithDeadline(ctx, deadline)
		raw, err := conn.ReceiveMessage(rctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("receive: %w", err)
		}

		cd, ok := raw.(*pgproto3.CopyData)
		if !ok {
			if e, isErr := raw.(*pgproto3.ErrorResponse); isErr {
				return fmt.Errorf("server error: %s", e.Message)
			}
			continue
		}

		switch cd.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			ka, err := pglogrepl.ParsePrimaryKeepaliveMessage(cd.Data[1:])
			if err != nil {
				return err
			}
			if ka.ReplyRequested {
				nextStatus = time.Now()
			}
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
			if err != nil {
				return err
			}
			msg, err := pglogrepl.ParseV2(xld.WALData, false)
			if err != nil {
				return fmt.Errorf("parse: %w", err)
			}
			switch m := msg.(type) {
			case *pglogrepl.BeginMessage:
				p.inTx = true
				currentXid = m.Xid
			case *pglogrepl.LogicalDecodingMessageV2:
				if m.Prefix != cfg.prefix {
					continue
				}
				p.msgs = append(p.msgs, toPubsub(m, xld.WALStart, currentXid))
				// A size flush may land mid-transaction. That is safe: ackLSN still
				// points at the previous commit, so a crash replays this transaction
				// and Pub/Sub sees duplicates, never gaps.
				if len(p.msgs) >= cfg.batchSize {
					if err := flush("size"); err != nil {
						return err
					}
					nextFlush = time.Now().Add(cfg.flushEvery)
				}
			case *pglogrepl.CommitMessage:
				p.inTx = false
				p.ackLSN = m.TransactionEndLSN
			}
		}
	}
}

func toPubsub(m *pglogrepl.LogicalDecodingMessageV2, lsn pglogrepl.LSN, xid uint32) *pubsub.Message {
	// m.Content aliases pgconn's receive buffer, which is reused by the next
	// ReceiveMessage call. Copy it or the body is overwritten before publish.
	content := bytes.Clone(m.Content)
	attrs := map[string]string{
		"pg_lsn":           lsn.String(),
		"pg_xid":           fmt.Sprint(xid),
		"target":           m.Prefix,
		"pg_transactional": fmt.Sprint(m.Transactional),
	}
	// If the payload is a JSON object with a "type" or "key" we surface them as
	// attributes so subscribers can filter or order without decoding the body.
	var probe struct {
		Type string `json:"type"`
		Key  string `json:"key"`
	}
	if json.Unmarshal(content, &probe) == nil {
		if probe.Type != "" {
			attrs["type"] = probe.Type
		}
		if probe.Key != "" {
			attrs["key"] = probe.Key
		}
	}
	return &pubsub.Message{Data: content, Attributes: attrs, OrderingKey: probe.Key}
}

// sink is where a batch goes. pubsubSink publishes for real. simulatedSink stands in
// for Pub/Sub with a fixed cost per batch, so a benchmark measures the database and
// relay path with a realistic publish round trip instead of the emulator's throughput.
type sink interface {
	PublishAll(ctx context.Context, msgs []*pubsub.Message) error
	Close()
}

func newSink(ctx context.Context, cfg config) (sink, error) {
	switch cfg.sink {
	case "simulated":
		log.Printf("sink=simulated: %s per publish batch, nothing is sent", cfg.sinkLatency)
		return simulatedSink{latency: cfg.sinkLatency}, nil
	case "pubsub":
		ps, err := pubsub.NewClient(ctx, cfg.project)
		if err != nil {
			return nil, fmt.Errorf("pubsub client: %w", err)
		}
		t := ps.Topic(cfg.topic)
		ok, err := t.Exists(ctx)
		if err != nil {
			return nil, fmt.Errorf("topic exists: %w", err)
		}
		if !ok {
			created, err := ps.CreateTopic(ctx, cfg.topic)
			switch {
			case err == nil:
				t = created
				log.Printf("created topic %s", cfg.topic)
			case status.Code(err) == codes.AlreadyExists:
				// another relay created it between Exists and CreateTopic
			default:
				return nil, fmt.Errorf("create topic: %w", err)
			}
		}
		t.EnableMessageOrdering = true
		t.PublishSettings.CountThreshold = cfg.batchSize
		t.PublishSettings.DelayThreshold = 50 * time.Millisecond
		return &pubsubSink{client: ps, topic: t}, nil
	}
	return nil, fmt.Errorf("unknown sink %q (pubsub | simulated)", cfg.sink)
}

type pubsubSink struct {
	client *pubsub.Client
	topic  *pubsub.Topic
}

func (s *pubsubSink) PublishAll(ctx context.Context, msgs []*pubsub.Message) error {
	results := make([]*pubsub.PublishResult, len(msgs))
	for i, m := range msgs {
		results[i] = s.topic.Publish(ctx, m)
	}
	for _, r := range results {
		if _, err := r.Get(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *pubsubSink) Close() { s.topic.Stop(); s.client.Close() }

type simulatedSink struct{ latency time.Duration }

func (s simulatedSink) PublishAll(ctx context.Context, msgs []*pubsub.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	select {
	case <-time.After(s.latency):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (simulatedSink) Close() {}

// ensureSlot creates the slot if missing. It returns the LSN to start from and whether
// the slot was created now (which, after the first run, means it had been lost).
func ensureSlot(ctx context.Context, conn *pgconn.PgConn, slot string) (pglogrepl.LSN, bool, error) {
	res, err := pglogrepl.CreateReplicationSlot(ctx, conn, slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
	if err == nil {
		lsn, perr := pglogrepl.ParseLSN(res.ConsistentPoint)
		if perr != nil {
			return 0, false, perr
		}
		log.Printf("created slot %s at %s", slot, lsn)
		return lsn, true, nil
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "42710" { // duplicate_object
		return 0, false, fmt.Errorf("create slot: %w", err)
	}
	// Slot exists. READ_REPLICATION_SLOT only works for physical slots, but for a
	// logical slot START_REPLICATION at 0/0 means "from the slot's confirmed_flush_lsn",
	// so the server resumes exactly where the last ack left off.
	log.Printf("resuming slot %s from its confirmed_flush_lsn", slot)
	return 0, false, nil
}

// replayJournal publishes journal rows for this target with lsn in (from, to], in
// position order, through the same topic. Rows are read over an ordinary connection.
// Note that lsn is where the message was written, not where its transaction
// committed; a long transaction can commit after a higher position was acknowledged.
// The window is therefore widened by one status interval's worth of positions on the
// low side in production, and consumers dedupe on the message id regardless.
func replayJournal(ctx context.Context, cfg config, topic sink, from, to pglogrepl.LSN) (int, error) {
	conn, err := pgx.Connect(ctx, cfg.journalDSN)
	if err != nil {
		return 0, err
	}
	defer conn.Close(context.Background())
	rows, err := conn.Query(ctx, `
		SELECT payload, lsn::text
		FROM outbox_journal
		WHERE target = $1 AND lsn > $2::pg_lsn AND lsn <= $3::pg_lsn
		ORDER BY lsn, id`, cfg.prefix, from.String(), to.String())
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var msgs []*pubsub.Message
	for rows.Next() {
		var payload, lsn string
		if err := rows.Scan(&payload, &lsn); err != nil {
			return 0, err
		}
		m := &pglogrepl.LogicalDecodingMessageV2{}
		m.Prefix = cfg.prefix
		m.Content = []byte(payload)
		m.Transactional = true
		l, _ := pglogrepl.ParseLSN(lsn)
		msg := toPubsub(m, l, 0)
		msg.Attributes["replayed"] = "true"
		msgs = append(msgs, msg)
	}
	if rows.Err() != nil {
		return 0, rows.Err()
	}
	// Replay in relay-sized batches so it costs the same per message as streaming.
	for i := 0; i < len(msgs); i += cfg.batchSize {
		j := min(i+cfg.batchSize, len(msgs))
		if err := topic.PublishAll(ctx, msgs[i:j]); err != nil {
			return 0, err
		}
	}
	return len(msgs), nil
}

func readState(path string) (pglogrepl.LSN, bool) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, false
	}
	lsn, err := pglogrepl.ParseLSN(strings.TrimSpace(string(b)))
	if err != nil {
		return 0, false
	}
	return lsn, true
}

func writeState(path string, lsn pglogrepl.LSN) {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(lsn.String()+"\n"), 0o644); err != nil {
		log.Printf("state: %v", err)
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		log.Printf("state: %v", err)
	}
}

func envDuration(k string, def time.Duration) time.Duration {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func env(k, def string) string {
	if v := strings.TrimSpace(os.Getenv(k)); v != "" {
		return v
	}
	return def
}
