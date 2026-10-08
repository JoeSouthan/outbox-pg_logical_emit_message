#!/usr/bin/env bash
# Benchmark harness. Runs the same producer workload through each outbox transport and
# records producer, relay and database effects. Everything runs in containers; this
# script only drives docker compose from the host.
#
#   bench/run.sh [events] [per_txn] [workers]       defaults 100000 10 32
#   BENCH_MODES="table wal journal" BENCH_SCENARIOS="concurrent backlog" to narrow
#
# Scenarios:
#   concurrent  relay running while the producer runs (steady state)
#   backlog     producer runs with the relay stopped, then the relay drains (burst)
#   slotloss    (journal only) producer runs with the relay stopped, the slot is dropped
#               to simulate a failover, the relay restarts and recovers from the journal
#
# Requires `docker compose up -d` and `docker compose --profile bench build`.
# One JSON line per run is appended to bench/results.jsonl; logs go to bench/logs/.
set -euo pipefail
cd "$(dirname "$0")/.."

EVENTS=${1:-100000}
PER_TXN=${2:-10}
WORKERS=${3:-32}
MODES=${BENCH_MODES:-"table wal journal"}
SCENARIOS=${BENCH_SCENARIOS:-"concurrent backlog slotloss"}
LOGDIR=${BENCH_LOGDIR:-bench/logs}
# simulated: each publish batch costs BENCH_SINK_LATENCY (default 200ms) and nothing is
# sent, standing in for a Pub/Sub round trip. pubsub: publish to a real project.
SINK=${BENCH_SINK:-simulated}
SINK_LATENCY=${BENCH_SINK_LATENCY:-200ms}
RELAY_CONTAINER=outbox-bench-relay
TARGET=target_1
SLOT="outbox_relay_$TARGET"
mkdir -p "$LOGDIR" state

sql() { docker compose exec -T postgres psql -U outbox -d outbox -Atq -c "$1"; }

outbox_table() { case "$1" in table) echo outbox_items;; journal) echo outbox_journal;; *) echo outbox_items;; esac; }

reset_db() {
  sql "TRUNCATE outbox_items, outbox_journal, payments" >/dev/null
  sql "VACUUM ANALYZE outbox_items" >/dev/null
  sql "VACUUM ANALYZE outbox_journal" >/dev/null
  sql "VACUUM ANALYZE payments" >/dev/null
  sql "SELECT pg_stat_reset()" >/dev/null
  sql "SELECT pg_stat_reset_shared('wal')" >/dev/null
  # Every run starts from a fresh slot and no relay state, so "ensure slot exists"
  # really creates one (and writes the state file the slot-loss scenario relies on).
  sql "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE NOT active" >/dev/null
  rm -f state/relay_*.lsn
}

start_relay() { # mode logfile
  docker rm -f "$RELAY_CONTAINER" >/dev/null 2>&1 || true
  local se=(-e RELAY_SINK="$SINK" -e RELAY_SINK_LATENCY="$SINK_LATENCY")
  case "$1" in
    table)
      docker compose run --rm -T --name "$RELAY_CONTAINER" "${se[@]}" \
        -e RELAY_DSN=postgres://outbox:outbox@postgres:5432/outbox relay /tablerelay > "$2" 2>&1 & ;;
    wal)
      docker compose run --rm -T --name "$RELAY_CONTAINER" "${se[@]}" relay /relay > "$2" 2>&1 & ;;
    journal)
      docker compose run --rm -T --name "$RELAY_CONTAINER" "${se[@]}" -e RELAY_JOURNAL=true relay /relay > "$2" 2>&1 & ;;
  esac
  RELAY_PID=$!
  for _ in $(seq 1 100); do grep -q -a -E 'streaming|relaying' "$2" 2>/dev/null && break; sleep 0.2; done
}

stop_relay() {
  if [ -n "${RELAY_PID:-}" ]; then
    docker stop -t 5 "$RELAY_CONTAINER" >/dev/null 2>&1 || true
    kill "$RELAY_PID" 2>/dev/null || true; wait "$RELAY_PID" 2>/dev/null || true; RELAY_PID=""
  fi
}

run_producer() { # mode tag -> the producer's JSON line; stderr kept in a log
  docker compose run --rm -T -e OUTBOX_MODE="$1" producer -events "$EVENTS" -per-txn "$PER_TXN" -workers "$WORKERS" -json 2> "$LOGDIR/producer_$2.log" | tail -1
}

# Samples dead tuples, outbox size, slot lag and postgres container CPU every 0.5s.
start_sampler() { # outfile table
  (
    while true; do
      d=$(sql "SELECT coalesce(sum(n_dead_tup),0) FROM pg_stat_user_tables WHERE relname LIKE '$2%'" 2>/dev/null || echo 0)
      s=$(sql "SELECT pg_total_relation_size('$2')" 2>/dev/null || echo 0)
      l=$(sql "SELECT coalesce(max(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)),0)::bigint FROM pg_replication_slots" 2>/dev/null || echo 0)
      c=$(docker stats --no-stream --format '{{.CPUPerc}}' outbox-postgres-1 2>/dev/null | tr -d '%' || echo 0)
      echo "$d $s $l ${c:-0}"
      sleep 0.5
    done
  ) > "$1" 2>/dev/null &
  SAMPLER_PID=$!
}
stop_sampler() { kill "$SAMPLER_PID" 2>/dev/null || true; wait "$SAMPLER_PID" 2>/dev/null || true; }

wait_drained() { # logfile expected t0 -> seconds until the relay reports total >= expected
  local log=$1 expected=$2 t0=$3
  while true; do
    total=$(grep -a -o 'total=[0-9]*' "$log" | tail -1 | cut -d= -f2)
    if [ "${total:-0}" -ge "$expected" ]; then break; fi
    if [ $(( $(date +%s) - t0 )) -gt 900 ]; then echo "timeout waiting for relay" >&2; break; fi
    sleep 0.2
  done
  python3 -c "import time; print(round(time.time()-$t0, 2))"
}

db_stats() { # table
  sql "SELECT json_build_object(
        'wal_records', w.wal_records, 'wal_fpi', w.wal_fpi, 'wal_bytes', w.wal_bytes,
        'outbox_tup_ins', (SELECT coalesce(sum(n_tup_ins),0) FROM pg_stat_user_tables WHERE relname LIKE '$1%'),
        'outbox_tup_del', (SELECT coalesce(sum(n_tup_del),0) FROM pg_stat_user_tables WHERE relname LIKE '$1%'),
        'outbox_dead_end', (SELECT coalesce(sum(n_dead_tup),0) FROM pg_stat_user_tables WHERE relname LIKE '$1%'),
        'outbox_autovacuum', (SELECT coalesce(sum(autovacuum_count),0) FROM pg_stat_user_tables WHERE relname LIKE '$1%'),
        'outbox_bytes_end', pg_total_relation_size('$1'),
        'payments_tup_ins', coalesce(p.n_tup_ins,0),
        'db_blks_read', d.blks_read, 'db_blks_hit', d.blks_hit,
        'db_tup_returned', d.tup_returned, 'db_tup_fetched', d.tup_fetched,
        'db_xact_commit', d.xact_commit, 'db_xact_rollback', d.xact_rollback)
      FROM pg_stat_wal w, pg_stat_database d
      LEFT JOIN pg_stat_user_tables p ON p.relname='payments'
      WHERE d.datname='outbox'"
}

run_one() { # mode scenario
  local mode=$1 scenario=$2 tag="${1}_${2}" tbl
  tbl=$(outbox_table "$mode")
  local rlog="$LOGDIR/relay_$tag.log" slog="$LOGDIR/samples_$tag.txt"
  local backlog_dead="" backlog_bytes="" backlog_lag="" t1=""
  echo "== $tag events=$EVENTS per_txn=$PER_TXN workers=$WORKERS sink=$SINK($SINK_LATENCY)" >&2
  reset_db
  start_sampler "$slog" "$tbl"
  t0=$(date +%s)
  case "$scenario" in
    concurrent)
      start_relay "$mode" "$rlog"
      producer=$(run_producer "$mode" "$tag")
      drain=$(wait_drained "$rlog" "$EVENTS" "$t0") ;;
    backlog|slotloss)
      if [ "$mode" != table ]; then start_relay "$mode" "$LOGDIR/relay_${tag}_slot.log"; stop_relay; fi   # slot + state exist
      producer=$(run_producer "$mode" "$tag")
      backlog_dead=$(sql "SELECT coalesce(sum(n_dead_tup),0) FROM pg_stat_user_tables WHERE relname LIKE '$tbl%'")
      backlog_bytes=$(sql "SELECT pg_total_relation_size('$tbl')")
      backlog_lag=$(sql "SELECT coalesce(max(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)),0)::bigint FROM pg_replication_slots")
      if [ "$scenario" = slotloss ]; then sql "SELECT pg_drop_replication_slot('$SLOT')" >/dev/null; fi
      t1=$(date +%s)
      start_relay "$mode" "$rlog"
      drain=$(wait_drained "$rlog" "$EVENTS" "$t1") ;;
  esac
  sleep 1
  stats=$(db_stats "$tbl")
  stop_relay
  stop_sampler
  python3 - "$mode" "$scenario" "$producer" "$drain" "$stats" "$slog" "$backlog_dead" "$backlog_bytes" "$backlog_lag" "$rlog" "$tbl" <<'PY'
import json, sys
mode, scenario, producer, drain, stats, slog, bdead, bbytes, blag, rlog, tbl = sys.argv[1:]
import os
r = {"mode": mode, "scenario": scenario, "sink": os.environ.get("BENCH_SINK", "simulated"),
     "sink_latency": os.environ.get("BENCH_SINK_LATENCY", "200ms"), "outbox_table": tbl if mode != "wal" else None}
r.update(json.loads(producer))
r["drain_s"] = float(drain)
r.update(json.loads(stats))
rows = [l.split() for l in open(slog) if l.strip()]
if rows:
    r["outbox_dead_max"] = max(int(x[0]) for x in rows)
    r["outbox_bytes_max"] = max(int(x[1]) for x in rows)
    r["slot_lag_max"] = max(int(x[2]) for x in rows)
    cpus = [float(x[3]) for x in rows if len(x) > 3]
    r["pg_cpu_pct_max"] = max(cpus) if cpus else None
    r["pg_cpu_pct_avg"] = round(sum(cpus)/len(cpus), 1) if cpus else None
if bdead:
    r["backlog_dead_tup"] = int(bdead); r["backlog_outbox_bytes"] = int(bbytes); r["backlog_slot_lag"] = int(blag)
log = open(rlog, errors="replace").read()
r["relay_batches"] = log.count("published ")
import re
m = re.search(r"replayed (\d+) from outbox_journal", log)
r["relay_replayed"] = int(m.group(1)) if m else 0
print(json.dumps(r))
PY
}

for mode in $MODES; do
  for scenario in $SCENARIOS; do
    if [ "$scenario" = slotloss ] && [ "$mode" != journal ]; then continue; fi
    run_one "$mode" "$scenario" | tee -a bench/results.jsonl
  done
done
echo "done; results in bench/results.jsonl" >&2
