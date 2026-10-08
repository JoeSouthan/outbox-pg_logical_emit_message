-- An empty publication: pgoutput requires one, but logical decoding messages are not
-- tied to any table, so we deliberately publish no tables. Only BEGIN / MESSAGE / COMMIT
-- records reach the relay.
CREATE PUBLICATION outbox_pub;

-- Demo business table written by the producer.
CREATE TABLE payments (
  id           bigserial PRIMARY KEY,
  reference    text        NOT NULL,
  amount_cents integer     NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now()
);

-- Transport "table": the conventional outbox. A `target` column picks the destination;
-- a relay selects a batch FOR UPDATE SKIP LOCKED, publishes, deletes and commits.
CREATE TABLE outbox_items (
  id         bigserial PRIMARY KEY,
  target     text        NOT NULL,
  payload    text        NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX outbox_items_target_id ON outbox_items (target, id);

-- Transport "journal": delivery still goes through the log message; this append-only
-- table exists only for recovery and visibility. Nothing updates or deletes rows: the
-- table is range-partitioned by day and old partitions are dropped after a retention
-- period, so it never produces dead tuples. `lsn` is the position of the message record,
-- returned by pg_logical_emit_message in the same INSERT, which is what lets a relay
-- replay exactly the window it lost.
CREATE TABLE outbox_journal (
  id         bigserial   NOT NULL,
  target     text        NOT NULL,
  payload    text        NOT NULL,
  lsn        pg_lsn      NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
) PARTITION BY RANGE (created_at);
CREATE INDEX outbox_journal_target_lsn ON outbox_journal (target, lsn);

-- Daily partitions for a window around now plus a default catch-all. A real deployment
-- would create these on a schedule (pg_partman or cron) and drop the old ones.
DO $$
DECLARE d date;
BEGIN
  FOR d IN SELECT generate_series(current_date - 1, current_date + 7, '1 day')::date LOOP
    EXECUTE format('CREATE TABLE outbox_journal_%s PARTITION OF outbox_journal FOR VALUES FROM (%L) TO (%L)',
                   to_char(d, 'YYYYMMDD'), d, d + 1);
  END LOOP;
END $$;
CREATE TABLE outbox_journal_default PARTITION OF outbox_journal DEFAULT;
