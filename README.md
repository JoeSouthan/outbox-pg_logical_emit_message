# outbox: transactional events via `pg_logical_emit_message`

A prototype of a transactional outbox that needs **no outbox table** for delivery.
Producers write events into the Postgres write-ahead log inside their business
transaction; a relay decodes them from a logical replication slot and publishes batches
to Google Pub/Sub, or to a simulated sink that costs a fixed latency per batch. A third
transport adds an append-only journal so a relay can recover a lost slot. See [DESIGN.md](DESIGN.md) for the reasoning and trade-offs and
[BENCHMARK.md](BENCHMARK.md) for measurements against a conventional outbox table.

```mermaid
flowchart LR
  P[producer<br/>Go, N workers] -->|txn: INSERT payments<br/>+ emit event| B[PgBouncer<br/>transaction mode]
  B --> PG[(Postgres 18<br/>wal_level=logical)]
  PG -->|slot outbox_relay_target_1<br/>pgoutput, empty publication| R1[relay target_1]
  PG -->|slot outbox_relay_target_2| R2[relay target_2]
  R1 -->|batches, ack LSN after publish| T1[[sink: simulated 200ms/batch<br/>or Pub/Sub topic outbox-target_1]]
  R2 --> T2[[sink for target_2]]
  T1 -. real Pub/Sub only .-> C1[consumer target_1]
```

Every event carries a **target**. One relay serves one target: it has its own replication
slot, keeps only that prefix, and publishes to that target's topic. Adding a destination
is adding a relay.

## Transports

| mode | producer statement | delivery | recovery after slot loss | table churn |
|---|---|---|---|---|
| `table` | `INSERT INTO outbox_items` | relay polls, `FOR UPDATE SKIP LOCKED`, publish, `DELETE`, commit | n/a, rows wait in the table | insert + delete per event: dead tuples, vacuum, bloat |
| `wal` | `SELECT pg_logical_emit_message(true, target, payload)` | relay streams the slot, publishes, acks commit LSN | none: the unacknowledged window is lost | none |
| `journal` | `INSERT INTO outbox_journal (…, lsn) VALUES (…, pg_logical_emit_message(…))` | same as `wal` | relay replays `outbox_journal` between its last acked LSN and the new slot's start | insert only; daily partitions dropped, never deleted, no dead tuples |

Properties this demonstrates:

- **Atomic with business data.** A rolled-back transaction's events are never decoded.
- **Commit order.** Events arrive in commit order, so there is no id-gap problem.
- **Zero or append-only churn.** `wal` writes no rows; `journal` writes one row per event
  and never updates or deletes it.
- **Pooler friendly on the producer side.** The producer goes through PgBouncer in
  transaction mode. Relays connect directly (replication protocol).
- **At-least-once.** The relay acknowledges WAL only after Pub/Sub accepts a batch, and
  only up to whole-transaction boundaries. A crash replays, never drops. Consumers must
  dedupe on the message `id`. Non-transactional messages have no COMMIT record, so they
  never advance the ack and are replayed on restart until a later commit passes them.
- **Self-healing with `journal`.** After a failover that loses the slot, the relay
  recreates it and replays the journal for the lost window, then streams.

## Layout

| Path | What |
|---|---|
| `docker-compose.yml` | Postgres 18 (`wal_level=logical`), PgBouncer (transaction mode), and the three Go services; no Pub/Sub emulator |
| `postgres/init.sql` | empty publication `outbox_pub`, `payments`, `outbox_items` (table transport), `outbox_journal` (partitioned by day) |
| `relay/` | Go: `/relay` (slot management, pgoutput decoding, batching, publish, LSN ack, journal recovery) and `/tablerelay` (table-outbox baseline); both publish to Pub/Sub or to a simulated sink |
| `producer/` | Go load generator: worker pool, pipelined transactions, three transports, latency percentiles |
| `consumer/` | Go: subscribes to a target's topic on a real Pub/Sub project and prints what arrives |
| `bench/run.sh` | benchmark harness; `bench/report.py` renders results; findings in [BENCHMARK.md](BENCHMARK.md) |

Each service has its own Dockerfile; nothing is published on host ports and nothing
runs outside containers. `mise.toml` pins Go for development only.

## Sinks

There is no Pub/Sub emulator in the stack: its single Java process became the bottleneck
at a few hundred messages per second with ordering keys. Both relays therefore have two
sinks. `simulated` (the default) costs `RELAY_SINK_LATENCY` per publish batch, 200 ms by
default, and sends nothing; it stands in for a Pub/Sub round trip so the relay's
behaviour and the benchmark reflect a realistic publish cost without a broker.
`pubsub` publishes to a real project: set `RELAY_SINK=pubsub`, `PUBSUB_PROJECT_ID`, and
mount credentials via `GOOGLE_APPLICATION_CREDENTIALS`. The consumer only works with the
real sink.

## Run it

```sh
docker compose up -d                                     # postgres, pgbouncer
docker compose --profile bench build                     # relay, producer, consumer images

docker compose run --rm -e RELAY_JOURNAL=true relay &    # relay for target_1 with journal recovery
docker compose run --rm producer -mode journal -events 10000 -workers 16 -rollback-every 10
# committed=9000 rolled_back=1000 ... expect the target_1 relay to publish exactly 9000 messages
# relay log: published 1000 (size) total=... up to total=9000, ack=0/...
```

Simulate a failover that loses the slot, then watch the relay recover from the journal:

```sh
docker compose stop relay 2>/dev/null; docker stop $(docker ps -q --filter ancestor=outbox-relay)
docker compose run --rm producer -mode journal -events 500 -workers 8     # committed while no relay
docker compose exec postgres psql -U outbox -d outbox -c "select pg_drop_replication_slot('outbox_relay_target_1')"
docker compose run --rm -e RELAY_JOURNAL=true relay
# created slot outbox_relay_target_1 at 0/…
# replayed 500 from outbox_journal for window (0/…, 0/…] total=500
```

Other combinations:

```sh
docker compose run --rm -e RELAY_TARGET=target_2 relay &                 # second target: own slot, own topic
docker compose run --rm producer -mode wal -targets target_1,target_2 -events 1000
docker compose run --rm -e RELAY_DSN=postgres://outbox:outbox@postgres:5432/outbox relay /tablerelay &
docker compose run --rm producer -mode table -events 1000
docker compose run --rm producer -h                                      # all flags
```

Useful queries while it runs:

```sql
-- docker compose exec postgres psql -U outbox -d outbox
select slot_name, active, confirmed_flush_lsn,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as lag
from pg_replication_slots;
select relname, n_tup_ins, n_tup_del, n_dead_tup from pg_stat_user_tables
where relname in ('payments', 'outbox_items') or relname like 'outbox_journal%';
```

Stop the relay, produce more, and watch `lag` grow: that WAL is retained for the slot
until the relay returns and acks. `max_slot_wal_keep_size` in the compose file caps it,
after which Postgres invalidates the slot. With `wal` those events are lost; with
`journal` the relay recovers them.

## Benchmark

`bench/run.sh [events] [per_txn] [workers]` (defaults 100000, 10, 32) runs the same
producer workload through each transport in a steady-state scenario (relay running), a
burst scenario (relay stopped while producing, then draining) and, for `journal`, a
slot-loss scenario (slot dropped before the relay restarts). Relays use the simulated
sink (`BENCH_SINK`, `BENCH_SINK_LATENCY`). It records producer throughput and latency,
relay drain time, write-ahead log volume, dead tuples, outbox table size, vacuum
activity and sampled Postgres CPU. One JSON line per run goes to
`bench/results.jsonl`; `bench/report.py` renders the tables. The harness is a shell
script on the host that only drives `docker compose run`. Findings are in
[BENCHMARK.md](BENCHMARK.md).

## How the relay works

```mermaid
sequenceDiagram
  participant PG as Postgres
  participant R as relay (target_1)
  participant PS as sink (Pub/Sub or simulated)
  R->>PG: CREATE_REPLICATION_SLOT outbox_relay_target_1 LOGICAL pgoutput (or resume)
  R->>PG: START_REPLICATION proto_version 2, publication outbox_pub, messages true
  loop stream
    PG-->>R: BEGIN xid
    PG-->>R: MESSAGE prefix=target_1 payload
    PG-->>R: COMMIT lsn
    R->>R: buffer, flush on batch size or interval
    R->>PS: publish batch (ordering key = payload.key)
    PS-->>R: all acked
    R->>PG: standby status: last fully published COMMIT lsn
    R->>R: write state file
  end
```

1. Create the slot, or start at LSN `0/0` to resume from `confirmed_flush_lsn`. With
   `-journal`, a slot that had to be recreated while a state file exists triggers a
   replay of `outbox_journal` rows with `lsn` in (last acked, new slot start].
2. Stream with `proto_version '2'`, `publication_names 'outbox_pub'`, `messages 'true'`.
   The publication has no tables, so only BEGIN, MESSAGE and COMMIT records arrive.
3. Each MESSAGE whose prefix equals the relay's target becomes a Pub/Sub message on
   `outbox-<target>`; other prefixes are skipped. `target`, `type`, `key`, LSN and xid
   become attributes.
4. Flush on `-batch` size or `-flush` interval. After every result is confirmed, send a
   standby status update with the last COMMIT LSN fully contained in what has been
   published, and persist it to the state file.

Requirements: Postgres 14+ for pgoutput messages (the function itself is 9.6+), a
role with `REPLICATION`, `wal_level=logical`, a free `max_replication_slots`.

## Journal recovery and position tracking

The journal exists for one case: the relay comes back and its slot is gone. Everything
else about it is designed so that case can be handled exactly, without reading the
journal at any other time.

**What the producer writes.** One statement per event:

```sql
INSERT INTO outbox_journal (target, payload, lsn)
VALUES ($1, $2, pg_logical_emit_message(true, $1, $2));
```

`pg_logical_emit_message` returns the log position of the record it just wrote, and the
INSERT stores it in the row. Both happen inside the business transaction, so a rollback
discards both. Nothing ever updates or deletes a journal row. The table is partitioned by
day on `created_at` and old partitions are dropped after a retention period, so it costs
one insert per event and never produces a dead tuple.

**What the relay tracks.** Two positions, kept in two places:

| position | where it lives | what it means |
|---|---|---|
| `confirmed_flush_lsn` | the slot, in Postgres | Postgres may discard log before this; the relay advances it with a standby status update after a batch is published |
| last acknowledged LSN | the relay's state file, `state/relay_<target>.lsn` | the relay's own copy of the same number, written atomically (temp file and rename) every time it acknowledges |

In steady state the two agree. The state file exists because the slot is the thing that
disappears in a failover, and the number it held is the only thing that defines the
lost window. In production this file belongs on durable storage outside the database
that just failed over: a small table in another database, an object store, or the relay's
own persistent volume.

**The acknowledgement rule.** The relay only acknowledges up to the end of a source
transaction whose messages have all been published. A batch that flushes in the middle
of a transaction publishes those messages but leaves the acknowledged position at the
previous commit, so a crash replays that whole transaction. Non-transactional messages
have no commit record and never advance the position. This is what makes delivery
at-least-once rather than at-most-once, and why consumers dedupe on the message id.

**What happens on start.** The relay tries to create its slot, then branches on two
facts: did the slot already exist, and does a state file exist.

```mermaid
flowchart TD
  S([relay starts]) --> C{slot exists?}
  C -->|yes| R[resume at 0/0, i.e. the slot's confirmed_flush_lsn]
  C -->|no, created now| F{state file exists?}
  F -->|no| N[first run, nothing before the slot's start can be streamed, write state = start]
  F -->|yes, journal mode| J[replay journal rows with lsn in last acked to slot start, then stream]
  F -->|yes, wal mode| W[log a warning, the window is lost, then stream]
  R --> T[stream, publish, ack, write state]
  N --> T
  J --> T
  W --> T
```

The slot-exists path is the normal restart. The created-now path with a state file is a
failover or an operator dropping the slot: the gap between the state file's position
and the new slot's consistent point is exactly what was committed while no slot was
retaining log, and the journal holds it.

**The replay itself.** Over an ordinary connection, not the replication one:

```sql
SELECT payload, lsn::text
FROM outbox_journal
WHERE target = $1 AND lsn > $2::pg_lsn AND lsn <= $3::pg_lsn
ORDER BY lsn, id;
```

`$2` is the state file's position and `$3` the new slot's consistent point. The rows go
through the same sink in relay-sized batches, with a `replayed=true` attribute, and the
state file is then set to the consistent point. Only after that does streaming begin, so
a crash during replay simply replays again from the same position. The benchmark's
slot-loss run replayed exactly 100,000 rows this way in 24.4 seconds.

**The boundary edge.** The stored `lsn` is where the message was written, which is
before its transaction's commit record. A transaction that wrote its message early and
committed late can have a position below one the relay has already acknowledged, while
still being streamed correctly in normal operation because streaming is commit-ordered.
After a slot loss, though, replaying strictly from the acknowledged position would miss
it. Two mitigations: widen the window downward by a margin at least as large as the
longest transaction the producers run, and rely on consumer dedupe for the duplicates
that margin creates. The prototype replays from the exact position; a production relay
should apply the margin.

**Per target.** Each target has its own slot, its own state file and its own replay
query, because each relay acknowledges independently. A failover therefore produces one
recovery per relay, each bounded by its own last acknowledgement.

**Retention is the one knob.** The journal can only recover what it still holds. Keep
partitions for longer than the longest relay outage you intend to survive, and alert on
journal partition age against that, the same way you alert on slot lag.
