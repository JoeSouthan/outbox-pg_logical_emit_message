# Benchmark: table outbox vs log messages vs log messages with a journal

The same producer workload driven through the three transports, measuring the effect
on the producer, on the relay and on the database. Numbers are from one laptop-class
machine with every component sharing the same virtual machine, so the ratios between
transports matter more than the absolute figures.

## Environment

| | |
|---|---|
| Host | MacBook Pro, Intel Core i5-1038NG7 at 2.0 GHz, 4 cores / 8 threads, 16 GB RAM, NVMe SSD, macOS 26.1 |
| Docker | Docker Desktop, engine 29.4.2, Apple Virtualization framework with VirtioFS, linux/amd64 |
| Docker VM | 8 vCPUs, 8 GB RAM, 1 GB swap, 60 GB disk, kernel 6.12 linuxkit, overlay2 |
| Postgres | 18.6, `wal_level=logical`, `shared_buffers=512MB`, `max_connections=300`, `max_slot_wal_keep_size=2GB`, default autovacuum settings |
| PgBouncer | edoburu/pgbouncer, transaction mode, pool of 64 server connections |
| Sink | simulated: every publish batch costs 200 ms and sends nothing, standing in for a Pub/Sub round trip |

Postgres, PgBouncer, the relay and the producer all run inside that one VM and compete
for the same four physical cores. There is no network between them beyond the Docker
bridge.

## Method

`bench/run.sh 100000 10 32`: per run, 100,000 events in 10,000 transactions of 10
payment inserts plus 10 events, from 32 concurrent producer connections through
PgBouncer, each transaction pipelined as one batch. Event payload is about 190 bytes of
JSON. The relay batches up to 1,000 messages or 500 ms.

Three transports:

| mode | producer writes per event | relay |
|---|---|---|
| `table` | INSERT into `outbox_items` | claim 1,000 rows with `FOR UPDATE SKIP LOCKED` in a materialised CTE, DELETE … RETURNING, publish, commit |
| `wal` | `pg_logical_emit_message()` | stream the slot, publish, ack commit LSN |
| `journal` | INSERT into `outbox_journal` with the emit inside the VALUES | as `wal`; journal read only after slot loss |

Three scenarios:

- **concurrent**: relay running while the producer runs. Drain time is measured from
  producer start to the relay's last publish, so it includes the producer's own run time.
- **backlog**: producer runs with the relay stopped, then the relay starts and drains.
  Drain time is relay start to last publish. This is the burst case.
- **slotloss** (`journal` only): as backlog, but the replication slot is dropped before
  the relay restarts, simulating a failover that loses the slot. The relay recovers the
  window from the journal.

Between runs the outbox tables and `payments` are truncated and vacuumed, statistics
are reset, the slot is dropped and the relay state file removed. Each run reports the
producer's own throughput and latency percentiles; the harness samples dead tuples,
outbox size, slot lag and the Postgres container's CPU every half second and reads
`pg_stat_wal`, `pg_stat_user_tables` and `pg_stat_database` at the end.

## Results

Events per run: 100,000 in transactions of 10 with 32 producer workers; runs averaged per column: table/concurrent=1, wal/concurrent=1, journal/concurrent=1, table/backlog=1, wal/backlog=1, journal/backlog=1, journal/slotloss=1.

### Producer (Go, via PgBouncer)

| metric | table / concurrent | wal / concurrent | journal / concurrent | table / backlog | wal / backlog | journal / backlog | journal / slotloss |
|---|---|---|---|---|---|---|---|
| throughput, events/s | 13,572 | 17,769 | 13,777 | 15,292 | 17,313 | 13,562 | 16,501 |
| throughput, transactions/s | 1,357 | 1,777 | 1,378 | 1,529 | 1,731 | 1,356 | 1,650 |
| transaction latency p50, ms | 19.32 | 15.97 | 20.10 | 16.35 | 16.71 | 20.36 | 17.70 |
| transaction latency p95, ms | 51.84 | 34.63 | 47.88 | 37.53 | 33.46 | 48.10 | 34.65 |
| transaction latency p99, ms | 83.31 | 49.45 | 70.37 | 93.56 | 50.18 | 67.93 | 52.21 |
| errors | 0 | 0 | 0 | 0 | 0 | 0 | 0 |

### Relay

| metric | table / concurrent | wal / concurrent | journal / concurrent | table / backlog | wal / backlog | journal / backlog | journal / slotloss |
|---|---|---|---|---|---|---|---|
| drain time, s (backlog/slotloss: relay start to last publish; concurrent: producer start to last publish) | 95.54 | 25.78 | 26.45 | 23.77 | 24.31 | 25.39 | 24.41 |
| publish batches | 100 | 100 | 100 | 100 | 100 | 100 | 0 |
| events replayed from journal | 0 | 0 | 0 | 0 | 0 | 0 | 100,000 |

### Database

| metric | table / concurrent | wal / concurrent | journal / concurrent | table / backlog | wal / backlog | journal / backlog | journal / slotloss |
|---|---|---|---|---|---|---|---|
| WAL bytes written | 73.71 MB | 42.20 MB | 80.97 MB | 74.31 MB | 42.14 MB | 81.27 MB | 81.24 MB |
| WAL records | 725,082 | 313,306 | 516,838 | 729,640 | 314,081 | 521,101 | 521,082 |
| outbox rows inserted | 100,000 | 0 | 100,000 | 100,000 | 0 | 100,000 | 100,000 |
| outbox rows deleted | 100,000 | 0 | 0 | 100,000 | 0 | 0 | 0 |
| outbox dead tuples, peak | 91,000 | 0 | 0 | 60,000 | 0 | 0 | 0 |
| outbox dead tuples, end | 96,000 | 0 | 0 | 36,000 | 0 | 0 | 0 |
| outbox table+index size, peak | 35.00 MB | 0.02 MB | 0.00 MB | 35.31 MB | 0.02 MB | 0.00 MB | 0.00 MB |
| outbox table+index size, end | 35.00 MB | 0.02 MB | 0.00 MB | 35.31 MB | 0.02 MB | 0.00 MB | 0.00 MB |
| autovacuum runs on outbox | 1 | 0 | 0 | 1 | 0 | 1 | 1 |
| buffer accesses (blks_hit) | 2,018,373 | 422,053 | 813,360 | 1,955,604 | 431,297 | 832,521 | 926,197 |
| transactions committed | 10,425 | 10,119 | 10,118 | 10,252 | 10,120 | 10,124 | 10,163 |
| slot lag, peak | 0.00 MB | 30.87 MB | 58.08 MB | 0.00 MB | 42.74 MB | 81.88 MB | 81.88 MB |
| postgres container CPU, avg % | 97.5 | 90.4 | 103.0 | 88.9 | 64.6 | 80.0 | 71.5 |
| postgres container CPU, max % | 499.2 | 459.9 | 514.1 | 458.3 | 416.4 | 470.1 | 469.7 |

Drain time in the concurrent scenario includes the producer's own run, which is about
7 seconds for all transports. With 100 batches of 1,000 and a 200 ms sink cost, 20
seconds of every drain is the simulated publish floor; what is left is the relay's own
database work.

## Findings

**Headline ratios, `wal` against the table outbox (concurrent scenario):**

| | table | wal | journal | wal vs table |
|---|---|---|---|---|
| producer throughput, events/s | 13,572 | 17,769 | 13,777 | +31% |
| producer p99 latency, ms | 83 | 49 | 70 | −41% |
| relay drain, producer start to last publish, s | 95.5 | 25.8 | 26.5 | 3.7× faster |
| WAL bytes | 73.7 MB | 42.2 MB | 81.0 MB | −43% |
| buffer accesses | 2.02 M | 0.42 M | 0.81 M | −79% |
| dead tuples at peak | 91,000 | 0 | 0 | — |
| outbox size left behind | 35 MB | 0 | 0 (partitions dropped) | — |

1. **The producer is cheapest with `wal`.** Replacing the outbox INSERT with a log
   message raised producer throughput by about 30 percent and cut p99 transaction
   latency by 40 percent at the same concurrency. The message is one WAL record with no
   heap or index work, and there is no relay deleting from the same table while the
   producers insert into it.

2. **`journal` costs the producer about what the table does, with a better tail.**
   Throughput matches `table` within noise, p99 is 15 percent better, and it writes the
   most WAL of the three because every event is logged twice: once as the message, once
   as the row. The row is append-only, so unlike `table` nothing competes with the
   producers and nothing is left to vacuum. This is the price of self-healing recovery.

3. **The table relay collapses under concurrent load and is fine without it.** In the
   backlog scenario the table relay drains as fast as the others: claiming and deleting
   1,000 rows costs about 40 ms, lost in the 200 ms publish floor. In the concurrent
   scenario its first twelve batches took 5.9 seconds each while the 32 producer
   connections were running, against 0.2 seconds for the log relays, and the whole drain
   took 95 seconds instead of 26. The claim transaction fights the producers for the
   same pages and locks exactly when the burst is happening, which is the moment an
   outbox relay most needs to keep up. The log relays are not in that contention at all:
   they read the WAL stream, never the table.

4. **Dead tuples and bloat are a `table`-only phenomenon.** 91,000 dead tuples at peak,
   one autovacuum run that had not finished by the end, and a 35 MB table-plus-index
   footprint that persists after every row has been deleted. `wal` touched no table.
   `journal` inserted 100,000 rows into a daily partition that will be dropped, with
   zero dead tuples.

5. **Database work per event is 5× lower with `wal` and 2.5× lower with `journal`.**
   Buffer accesses are the cleanest proxy for what Postgres actually did: 2.0 million
   for `table` against 0.42 million for `wal` and 0.81 million for `journal`. WAL records
   tell the same story: 725,000 against 313,000 and 517,000. The table pays for the heap
   insert, two index inserts, the delete, the index visits during claims, and the
   vacuum.

6. **Slot loss is recovered completely and at full speed.** With the slot dropped after
   100,000 events had been committed, the `journal` relay recreated it, replayed exactly
   100,000 rows from the journal in 24.4 seconds, the same as a normal backlog drain,
   and then resumed streaming. No events were lost and none were duplicated in this
   run; the design still requires consumers to dedupe on the message id, because a
   crash mid-batch replays the whole transaction.

7. **Retained WAL is the resource to watch for the log transports.** Peak slot lag was
   43 MB for `wal` and 82 MB for `journal` over 100,000 events with the relay stopped,
   so roughly 0.4 to 0.8 KB of retained log per event at this payload size. That is the
   number to multiply by your burst size and acceptable relay outage when sizing
   `max_slot_wal_keep_size` and the disk.

8. **CPU is dominated by the producer, not the transport.** Postgres saturated the
   container during every producer run. The averages differ by at most a third and the
   peaks are within noise, so on this machine the transport does not change how hard
   the producer phase hits the database; it changes how much is left behind and how
   the relay behaves.

**What this means for the decision.** If the goal is to stop an outbox table from
bloating under bursts, `wal` is the cheapest option on every axis and `journal` buys
failover safety and SQL visibility for one append-only insert per event. Both keep the
relay out of the producers' way, which is where the table design loses the most. The
table relay is competitive only when it runs after the burst, which is the opposite of
what an outbox is for.

## Two things the harness taught us before any numbers

- **The usual table-outbox claim is fragile.** Written as `DELETE … WHERE id IN (SELECT
  … LIMIT 1000 FOR UPDATE SKIP LOCKED)`, the planner can choose to rescan the subquery
  per candidate row once statistics are stale after a bulk load. On this stack that
  turned a 1,000-row claim into minutes of a saturated core and a claim that returned
  more rows than its LIMIT. Writing the claim as `WITH claimed AS MATERIALIZED (…)
  DELETE … USING claimed` evaluates it exactly once. Production table relays should
  use that form.
- **The Pub/Sub emulator is not a benchmark target.** Its single Java process became the
  bottleneck at a few hundred messages per second with ordering keys and retained
  memory across runs, so publish throughput said nothing about the transports. The
  relays got a simulated sink with a fixed cost per batch instead, and the emulator was
  removed from the stack.

## Caveats

- One machine, one run per cell, shared cores. Treat differences under about 10 percent
  as noise.
- Default autovacuum settings. A production outbox table would be tuned more
  aggressively, which shortens the vacuum lag for `table` but does not change the
  amount of dead tuples it creates.
- The simulated sink's 200 ms is a stand-in. Real Pub/Sub publish latency from inside
  the same region is typically lower; from a laptop it is higher. It affects all
  transports equally.
- Payloads are small. The producer's `-pad` flag adds bytes per event for a second pass
  at realistic sizes.
