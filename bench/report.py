#!/usr/bin/env python3
"""Render bench/results.jsonl (and bench/producer_only.jsonl if present) as markdown.

Runs with the same (mode, scenario) are averaged; the count of runs is shown.
Usage: bench/report.py > tables.md
"""
import json
import os
import statistics
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))


def load(name):
    path = os.path.join(HERE, name)
    if not os.path.exists(path):
        return []
    return [json.loads(l) for l in open(path) if l.strip()]


def mb(b):
    return f"{b / 1_000_000:.2f} MB"


def avg(rows, key):
    vals = [r[key] for r in rows if r.get(key) is not None]
    return statistics.mean(vals) if vals else None


def fmt(v, kind="n"):
    if v is None:
        return "-"
    if kind == "mb":
        return mb(v)
    if kind == "f1":
        return f"{v:.1f}"
    if kind == "f2":
        return f"{v:.2f}"
    return f"{v:,.0f}"


def group(rows):
    g = defaultdict(list)
    for r in rows:
        g[(r["mode"], r["scenario"])].append(r)
    return g


def table(title, cols, groups, order):
    print(f"\n### {title}\n")
    print("| metric | " + " | ".join(f"{m} / {s}" for m, s in order) + " |")
    print("|---|" + "---|" * len(order))
    for label, key, kind in cols:
        cells = [fmt(avg(groups.get(k, []), key), kind) for k in order]
        print(f"| {label} | " + " | ".join(cells) + " |")


def main():
    rows = load("results.jsonl")
    if rows:
        g = group(rows)
        order = [k for k in [("table", "concurrent"), ("wal", "concurrent"), ("journal", "concurrent"),
                             ("table", "backlog"), ("wal", "backlog"), ("journal", "backlog"),
                             ("journal", "slotloss")] if k in g]
        n = {k: len(v) for k, v in g.items()}
        ev = rows[0]["events"]
        print(f"Events per run: {ev:,} in transactions of {rows[0]['per_txn']} with {rows[0]['workers']} producer workers; "
              "runs averaged per column: " + ", ".join(f"{m}/{s}={n[(m, s)]}" for m, s in order) + ".")
        table("Producer (Go, via PgBouncer)", [
            ("throughput, events/s", "events_per_s", "n"),
            ("throughput, transactions/s", "txns_per_s", "n"),
            ("transaction latency p50, ms", "txn_ms_p50", "f2"),
            ("transaction latency p95, ms", "txn_ms_p95", "f2"),
            ("transaction latency p99, ms", "txn_ms_p99", "f2"),
            ("errors", "errors", "n"),
        ], g, order)
        table("Relay", [
            ("drain time, s (backlog/slotloss: relay start to last publish; concurrent: producer start to last publish)", "drain_s", "f2"),
            ("publish batches", "relay_batches", "n"),
            ("events replayed from journal", "relay_replayed", "n"),
        ], g, order)
        table("Database", [
            ("WAL bytes written", "wal_bytes", "mb"),
            ("WAL records", "wal_records", "n"),
            ("outbox rows inserted", "outbox_tup_ins", "n"),
            ("outbox rows deleted", "outbox_tup_del", "n"),
            ("outbox dead tuples, peak", "outbox_dead_max", "n"),
            ("outbox dead tuples, end", "outbox_dead_end", "n"),
            ("outbox table+index size, peak", "outbox_bytes_max", "mb"),
            ("outbox table+index size, end", "outbox_bytes_end", "mb"),
            ("autovacuum runs on outbox", "outbox_autovacuum", "n"),
            ("buffer accesses (blks_hit)", "db_blks_hit", "n"),
            ("transactions committed", "db_xact_commit", "n"),
            ("slot lag, peak", "slot_lag_max", "mb"),
            ("postgres container CPU, avg %", "pg_cpu_pct_avg", "f1"),
            ("postgres container CPU, max %", "pg_cpu_pct_max", "f1"),
        ], g, order)



if __name__ == "__main__":
    main()
