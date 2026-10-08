// producer is a load generator. Each worker runs transactions that insert business
// rows (payments) and emit one outbox event per row through the chosen transport:
//
//	table    INSERT INTO outbox_items                     (conventional outbox)
//	wal      SELECT pg_logical_emit_message(...)          (log message, no table)
//	journal  INSERT INTO outbox_journal (..., lsn) with the emit inside the VALUES
//	         (log message for delivery, append-only journal for recovery)
//
// Every statement in a transaction is pipelined in one batch so each worker keeps a
// server backend busy; the worker count sets the concurrency. It talks to PgBouncer in
// transaction mode using the extended protocol with unnamed statements only.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type config struct {
	dsn           string
	mode          string
	targets       []string
	events        int
	perTxn        int
	workers       int
	rollbackEvery int
	padBytes      int
	jsonOut       bool
}

type result struct {
	Mode        string   `json:"mode"`
	Targets     []string `json:"targets"`
	Events      int      `json:"events"`
	PerTxn      int      `json:"per_txn"`
	Workers     int      `json:"workers"`
	Txns        int      `json:"txns"`
	Committed   int      `json:"committed"`
	RolledBack  int      `json:"rolled_back"`
	ElapsedS    float64  `json:"elapsed_s"`
	EventsPerS  float64  `json:"events_per_s"`
	TxnsPerS    float64  `json:"txns_per_s"`
	TxnMsP50    float64  `json:"txn_ms_p50"`
	TxnMsP95    float64  `json:"txn_ms_p95"`
	TxnMsP99    float64  `json:"txn_ms_p99"`
	TxnMsMax    float64  `json:"txn_ms_max"`
	Errors      int      `json:"errors"`
	Via         string   `json:"via"`
	PayloadSize int      `json:"payload_bytes_approx"`
}

func main() {
	var cfg config
	var targets string
	flag.StringVar(&cfg.dsn, "dsn", env("PRODUCER_DSN", "postgres://outbox:outbox@pgbouncer:5432/outbox"), "database DSN (normally PgBouncer)")
	flag.StringVar(&cfg.mode, "mode", env("OUTBOX_MODE", "wal"), "transport: table | wal | journal")
	flag.StringVar(&targets, "targets", env("OUTBOX_TARGETS", "target_1"), "comma-separated targets, assigned round-robin per transaction")
	flag.IntVar(&cfg.events, "events", envInt("PRODUCER_EVENTS", 100000), "total events to emit")
	flag.IntVar(&cfg.perTxn, "per-txn", envInt("PRODUCER_PER_TXN", 10), "payments (and events) per transaction")
	flag.IntVar(&cfg.workers, "workers", envInt("PRODUCER_WORKERS", 32), "concurrent workers / connections")
	flag.IntVar(&cfg.rollbackEvery, "rollback-every", envInt("PRODUCER_ROLLBACK_EVERY", 0), "roll back every Nth transaction (0 = never)")
	flag.IntVar(&cfg.padBytes, "pad", envInt("PRODUCER_PAD", 0), "extra payload bytes per event")
	flag.BoolVar(&cfg.jsonOut, "json", false, "print result as JSON")
	flag.Parse()
	cfg.targets = strings.Split(targets, ",")

	if err := run(context.Background(), cfg); err != nil {
		log.Fatalf("producer: %v", err)
	}
}

func run(ctx context.Context, cfg config) error {
	pc, err := pgxpool.ParseConfig(cfg.dsn)
	if err != nil {
		return err
	}
	// Unnamed prepared statements only: safe behind PgBouncer in transaction mode.
	pc.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec
	pc.MaxConns = int32(cfg.workers)
	pc.MinConns = int32(cfg.workers)
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping: %w", err)
	}

	emitSQL, err := emitStatement(cfg.mode)
	if err != nil {
		return err
	}
	const paymentSQL = `INSERT INTO payments (reference, amount_cents) VALUES ($1, $2)`

	txns := (cfg.events + cfg.perTxn - 1) / cfg.perTxn
	var next atomic.Int64
	var committed, rolledBack, errs atomic.Int64
	latencies := make([][]float64, cfg.workers)
	pad := strings.Repeat("x", cfg.padBytes)

	started := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < cfg.workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				n := int(next.Add(1))
				if n > txns {
					return
				}
				target := cfg.targets[n%len(cfg.targets)]
				rollback := cfg.rollbackEvery > 0 && n%cfg.rollbackEvery == 0
				t0 := time.Now()
				err := runTxn(ctx, pool, paymentSQL, emitSQL, target, n, cfg.perTxn, pad, rollback)
				latencies[w] = append(latencies[w], float64(time.Since(t0).Microseconds())/1000.0)
				switch {
				case err != nil:
					errs.Add(1)
					if errs.Load() <= 5 {
						log.Printf("txn %d: %v", n, err)
					}
				case rollback:
					rolledBack.Add(int64(cfg.perTxn))
				default:
					committed.Add(int64(cfg.perTxn))
				}
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(started)

	var all []float64
	for _, l := range latencies {
		all = append(all, l...)
	}
	sort.Float64s(all)
	pct := func(p float64) float64 {
		if len(all) == 0 {
			return 0
		}
		i := int(float64(len(all))*p) - 1
		if i < 0 {
			i = 0
		}
		return all[i]
	}
	r := result{
		Mode: cfg.mode, Targets: cfg.targets, Events: cfg.events, PerTxn: cfg.perTxn, Workers: cfg.workers,
		Txns: txns, Committed: int(committed.Load()), RolledBack: int(rolledBack.Load()),
		ElapsedS: round2(elapsed.Seconds()), EventsPerS: round2(float64(cfg.events) / elapsed.Seconds()),
		TxnsPerS: round2(float64(txns) / elapsed.Seconds()),
		TxnMsP50: round2(pct(0.50)), TxnMsP95: round2(pct(0.95)), TxnMsP99: round2(pct(0.99)), TxnMsMax: round2(pct(1.0)),
		Errors: int(errs.Load()), Via: redact(cfg.dsn), PayloadSize: 190 + cfg.padBytes,
	}
	if cfg.jsonOut {
		b, _ := json.Marshal(r)
		fmt.Println(string(b))
	} else {
		fmt.Printf("mode=%s targets=%v events=%d txns=%d workers=%d committed=%d rolled_back=%d errors=%d elapsed=%.2fs events/s=%.0f p50=%.2fms p95=%.2fms p99=%.2fms via=%s\n",
			r.Mode, r.Targets, r.Events, r.Txns, r.Workers, r.Committed, r.RolledBack, r.Errors, r.ElapsedS, r.EventsPerS, r.TxnMsP50, r.TxnMsP95, r.TxnMsP99, r.Via)
		if cfg.mode != "table" {
			fmt.Printf("expect the %s relay to publish exactly %d messages\n", strings.Join(cfg.targets, "/"), r.Committed)
		}
	}
	return nil
}

type rollbackMarker struct{}

func (rollbackMarker) Error() string { return "rollback" }

func runTxn(ctx context.Context, pool *pgxpool.Pool, paymentSQL, emitSQL, target string, n, perTxn int, pad string, rollback bool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())

	b := &pgx.Batch{}
	for i := 0; i < perTxn; i++ {
		ref := fmt.Sprintf("P%d-%d", n, i)
		b.Queue(paymentSQL, ref, 100+i)
		b.Queue(emitSQL, target, payload(target, ref, pad))
	}
	br := tx.SendBatch(ctx, b)
	for i := 0; i < b.Len(); i++ {
		if _, err := br.Exec(); err != nil {
			br.Close()
			return err
		}
	}
	if err := br.Close(); err != nil {
		return err
	}
	if rollback {
		return tx.Rollback(ctx)
	}
	return tx.Commit(ctx)
}

func emitStatement(mode string) (string, error) {
	switch mode {
	case "table":
		return `INSERT INTO outbox_items (target, payload) VALUES ($1, $2)`, nil
	case "wal":
		return `SELECT pg_logical_emit_message(true, $1, $2::text)`, nil
	case "journal":
		return `INSERT INTO outbox_journal (target, payload, lsn) VALUES ($1, $2, pg_logical_emit_message(true, $1, $2::text))`, nil
	}
	return "", fmt.Errorf("unknown mode %q (table | wal | journal)", mode)
}

// payload mirrors what an application would emit: an id for dedupe, a type, the
// target, an ordering key and the business fields.
func payload(target, ref, pad string) string {
	m := map[string]any{
		"id":         newID(),
		"type":       "payment.created",
		"target":     target,
		"key":        ref,
		"emitted_at": time.Now().UTC().Format(time.RFC3339Nano),
		"payload":    map[string]any{"reference": ref, "amount_cents": 100},
	}
	if pad != "" {
		m["pad"] = pad
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func round2(f float64) float64 { return float64(int(f*100+0.5)) / 100 }

func redact(dsn string) string {
	if i := strings.Index(dsn, "@"); i > 0 {
		if j := strings.Index(dsn, "//"); j >= 0 && j < i {
			return dsn[:j+2] + dsn[i+1:]
		}
	}
	return dsn
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envInt(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return d
}
