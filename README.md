# pg-koldstore

> Keep PostgreSQL fast by moving old rows to cold Parquet storage while queries keep using the same table.

**Status: early development - not production ready.** The extension builds and the core flow works, but recovery, export/import, and background flush behavior are still being hardened.

**Mission:** Let PostgreSQL run for years without babysitting data growth. Hand it cheap, expandable storage for history, keep the hot working set small, and stop letting cold rows that nobody needs slow the cluster down.

`pg-koldstore` is a PostgreSQL extension named `koldstore`. You create a normal heap table, migrate it into KoldStore management, and keep querying that table with regular SQL. KoldStore keeps recent rows in PostgreSQL and writes flushed rows to Parquet files on filesystem, S3/MinIO, GCS, or Azure Blob storage.

Reads use a PostgreSQL custom scan named `KoldMergeScan`. It reads hot heap rows, reads cold Parquet segments when needed, and merges by primary key so the newest visible row wins.

## Why KoldStore?

KoldStore extends PostgreSQL instead of replacing it. Applications keep using the same SQL, drivers, ORMs, transactions, replication, and operational tooling while PostgreSQL gains a transparent cold-storage layer for historical rows.

- Built for PostgreSQL application tables, not only analytics or time-series workloads. KoldStore targets tables such as messages, notifications, audit logs, AI memory, user activity, and IoT events.
- Reduces the primary PostgreSQL storage footprint by moving older rows from expensive database storage into lower-cost object storage.
- Keeps the hot PostgreSQL working set smaller, which can reduce index size, VACUUM work, backup volume, and the amount of data scanned by common OLTP queries.
- Preserves PostgreSQL's native heap storage. KoldStore does not require a custom table access method or a replacement database engine.
- Uses open Apache Parquet files for archived data, so cold rows can be read by engines such as DuckDB, Spark, DataFusion, Polars, PyArrow, Trino, and ClickHouse.
- Avoids vendor lock-in by storing historical data in open formats on storage you control.
- Avoids partition explosion. Historical data can move to object storage without creating thousands or millions of PostgreSQL partitions.
- Supports incremental adoption on existing tables, so applications do not need a schema redesign or database migration just to start moving old rows cold.
- Works with bring-your-own storage backends, including local filesystem, Amazon S3-compatible storage, Google Cloud Storage, Azure Blob, and MinIO.
- Optimizes for immutable historical data by writing cold rows into Parquet segments while recent changes stay in PostgreSQL-managed hot storage and metadata.
- Creates a future analytics path: the same archived Parquet files can later feed data lake, analytics, or AI pipelines without exporting the data again.



## Compared With Other Approaches


| Approach                            | What you keep                                 | Tradeoff                                                         |
| ----------------------------------- | --------------------------------------------- | ---------------------------------------------------------------- |
| **KoldStore**                       | Same PostgreSQL table, SQL, drivers, and ORMs | Older rows move to open Parquet; hot heap stays small            |
| Bigger PostgreSQL disk / partitions | Familiar ops                                  | Historical rows still inflate heap, indexes, VACUUM, and backups |
| Time-series or analytics DB         | Columnar scan performance                     | New system, new query model, app migration                       |
| Custom table AM / fork              | Deeper engine control                         | Leaves stock PostgreSQL storage and tooling                      |
| Proprietary archive tier            | Managed cold storage                          | Vendor format lock-in; harder to read with DuckDB/Spark/etc.     |


KoldStore is for application tables that grow forever but are still queried through normal SQL. It is not a replacement database and not a time-series-only product.

### Storage comparison

After older rows are flushed, PostgreSQL keeps a smaller hot working set. Cold data lives in zstd Parquet outside the primary heap (filesystem / S3-compatible / GCS / Azure).

The harness ([`tests/storage/`](tests/storage/)) uses a wide (~50 column) table from [`tests/storage/schema.sql`](tests/storage/schema.sql). Sample below: **10,000,000 rows**, `hot_row_limit = 100000`, `max_rows_per_file = 1000000` (~9.9M rows flushed, zstd Parquet). Numbers vary by machine; re-run for your hardware.

How to read the table (Postgres-oriented):

- **Hot-only queries** are timed **before flush**, so both heaps still hold all 10M rows — that isolates `KoldMergeScan` overhead vs a plain index lookup, not “smaller heap wins.”
- **Hot+cold queries** and **`VACUUM (FULL, ANALYZE)`** are timed **after flush**, when the managed heap is the hot working set only.
- **Dead tuples** come from `pg_stat_user_tables.n_dead_tup` after the same update/delete sample, **before flush** — so both sides match here. The maintenance win shows up in post-flush VACUUM time / heap size, not in that pre-flush counter.
- Autovacuum counters are **not** shown: this harness is too short for autovacuum to run, so `autovacuum_count` stays 0 on both sides and would be misleading.
- **Backup size / restore time** are TODO until the harness measures `pg_dump` / `pg_restore` (or basebackup) of the PostgreSQL database only — cold Parquet is outside the cluster and would be protected separately.

| Operation | PostgreSQL only | PostgreSQL + KoldStore | Storage win |
| --- | --- | --- | --- |
| insert speed† | 69k ops/s | 23k ops/s | — |
| update speed† | 6.8k ops/s | 5.5k ops/s | — |
| delete speed† | 1.0M ops/s | 38k ops/s | — |
| query hot only (before flush) | 1.6k ops/s | 1.3k ops/s | — |
| query with hot+cold (after flush) | 1.5k ops/s | 127 ops/s‡ | — |
| VACUUM time (after flush) | 131 s | 6.4 s | **95%** |
| dead tuples after workload | 2000 (live≈10M) | 2000 (live≈10M) | — |
| index storage | 415 MiB | 11.4 MiB | **97%** |
| table storage | 5.45 GiB | 61 MiB (+ 597 MiB cold Parquet) | **99%** |
| total PG backup size | TODO | TODO | — |
| restore time | TODO | TODO | — |

PostgreSQL heap + index after flush: **5.85 GiB → 72 MiB** (**99% smaller**). Point lookups on hot and cold PKs still return the same rows as the unmanaged baseline (`KoldMergeScan`).

† DML is slower under KoldStore because `manage_table` installs capture triggers that maintain the latest-state change-log mirror (`koldstore.<table>__cl`: one row per PK with `seq` / `op`). That is the cost of flush cutoffs and change cursors. The payoff is a smaller hot heap/indexes, cheaper VACUUM, and (planned) `changes_since` so sync/cache consumers can follow changes **without** a second CDC pipeline (no Debezium, logical-replication slot, or extra app-owned triggers).

‡ Hot+cold PK lookups open matching Parquet segments (min/max prune + row-group stats / bloom). At this scale each surviving segment is ~1M wide rows, so footer open + merge-scan setup dominates vs a pure B-tree probe; streaming execution and tighter segment sizing are follow-ups.

```bash
# Table above: 10M rows / 100k hot (~30 min on a laptop; release-pg extension).
scripts/run-storage-comparison.sh --rows 10000000 --hot-limit 100000
# Faster local smoke (defaults: 100k rows / 10k hot):
scripts/run-storage-comparison.sh
```

## How It Works

You keep using a normal PostgreSQL table. KoldStore tracks latest-state changes, flushes older rows to Parquet, and merges hot + cold rows on read so applications do not change their SQL.

1. `manage_table` registers the table and creates a small change-log mirror.
2. `flush_table` moves older rows to Parquet and prunes them from the hot heap when safe.
3. `SELECT` on the original table uses `KoldMergeScan` so the newest visible row wins.

Details live in the architecture docs:

- [Architecture overview](docs/architecture.md)
- [Manage table](docs/architecture/manage-table.md)
- [Flushing](docs/architecture/flushing-table.md)
- [Scanning](docs/architecture/scanning-table.md)
- [DML capture](docs/architecture/dml-table.md)



## Quick Start

This example uses local filesystem storage so you can try the extension without S3 or Docker.

```sql
CREATE EXTENSION koldstore;

SELECT koldstore.register_storage(
  name         => 'local-dev',
  storage_type => 'filesystem',
  base_path    => '/tmp/koldstore-demo',
  credentials  => '{}'::jsonb,
  config       => '{}'::jsonb
);

CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE app.messages (
  id bigint PRIMARY KEY,
  account_id bigint NOT NULL,
  title text NOT NULL,
  body text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO app.messages (id, account_id, title, body)
SELECT
  gs,
  gs % 3,
  'message-' || lpad(gs::text, 6, '0'),
  'hello from row ' || gs
FROM generate_series(1, 1012) AS gs;
```

Manage the existing table:

```sql
SELECT koldstore.manage_table(
  table_name        => 'app.messages',
  storage           => 'local-dev',
  hot_row_limit     => 1000,
  min_flush_rows    => 1,
  max_rows_per_file => 500,
  migration_order_by => 'id'
) AS manage_job_id;
```

`hot_row_limit` keeps at most 1000 primary keys hot. When the mirror tracks more than 1000 keys, `koldstore.flush_table` moves only the oldest excess rows by mirror `seq` to cold storage. The low `min_flush_rows` keeps this small tutorial predictable; production tables usually use `1000` or higher.

Sample result:

```text
            manage_job_id            
--------------------------------------
 2c2bcf44-d6ea-4b3e-b62c-cfaf18ad5225
```

Management runs inline for this table. The returned UUID is the `migrate_backfill` job id in `koldstore.jobs`.

The application table is still the table you created:

```sql
SELECT id, account_id, title
FROM app.messages
ORDER BY id
LIMIT 3;
```

```text
 id | account_id |     title
----+------------+----------------
  1 |          1 | message-000001
  2 |          2 | message-000002
  3 |          0 | message-000003
```

Management also creates `koldstore.messages__cl`, a latest-state mirror with one metadata row per primary key. It stores the key columns plus `seq` and `op`; it does not duplicate full application row payloads.

```sql
SELECT id, seq, op
FROM koldstore.messages__cl
ORDER BY id
LIMIT 3;
```

```text
 id |        seq         | op
----+--------------------+----
  1 | 332882280164896768 |  1
  2 | 332882280169091072 |  1
  3 | 332882280173285376 |  1
```

With 1012 keys and `hot_row_limit => 1000`, all rows are still hot immediately after management. Nothing is cold yet:

```sql
SELECT
  (koldstore.describe_table(table_name => 'app.messages')::jsonb->>'hot_rows')::int AS hot_rows,
  (koldstore.describe_table(table_name => 'app.messages')::jsonb->>'mirror_rows')::int AS mirror_rows,
  (koldstore.describe_table(table_name => 'app.messages')::jsonb->>'cold_row_count')::int AS cold_row_count;
```

```text
 hot_rows | mirror_rows | cold_row_count
----------+-------------+----------------
     1012 |        1012 |              0
```



## Flush To Cold

`koldstore.flush_table` evaluates the configured flush policy, runs the flush inline, and returns the flush job id. With `hot_row_limit => 1000` and 1012 tracked keys, only the 12 oldest mirror entries move to cold storage; the newest 1000 keys stay hot.

```sql
SELECT koldstore.flush_table(table_name => 'app.messages') AS flush_job_id;
```

```text
             flush_job_id             
--------------------------------------
 e30eb374-a9db-4ff1-97d3-72f8511dfc60
```

```sql
SELECT rows_flushed
FROM koldstore.jobs
WHERE id = 'e30eb374-a9db-4ff1-97d3-72f8511dfc60'::uuid;
```

```text
 rows_flushed
--------------
           12
```

The application table still returns all rows through `KoldMergeScan`:

```sql
SELECT count(*) FROM app.messages;
```

```text
 count
-------
  1012
```

Cold files are written below the storage root using the table namespace and name:

```text
/tmp/koldstore-demo/app/messages/
  manifest.json
  batch-1.parquet
```

### Schedule periodic flush with pg_cron

Flush is on-demand today. Until the built-in smart scheduler lands, use
[pg_cron](https://github.com/citusdata/pg_cron) to call `flush_table` on a
schedule. Policy-aware flushes are safe to run often: when nothing is eligible,
the job completes with `rows_flushed = 0`.

Install and enable `pg_cron` (requires `shared_preload_libraries = 'pg_cron'`
and a restart), then schedule a table:

```sql
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Flush one managed table every 5 minutes.
SELECT cron.schedule(
  'koldstore-flush-messages',
  '*/5 * * * *',
  $$SELECT koldstore.flush_table(table_name => 'app.messages')$$
);
```

To flush every active managed table:

```sql
SELECT cron.schedule(
  'koldstore-flush-all',
  '*/5 * * * *',
  $$
  SELECT koldstore.flush_table(table_name => s.table_oid)
  FROM koldstore.schemas s
  WHERE s.active
  $$
);
```

Inspect or remove jobs with `cron.job` / `cron.unschedule(...)`. Pick an
interval that matches how fast the hot set grows; `min_flush_rows` still gates
whether a flush writes cold segments.



## View Table And Management Stats

Use `koldstore.describe_table` for a quick hot/cold/manifest snapshot:

```sql
SELECT jsonb_pretty(koldstore.describe_table(table_name => 'app.messages'));
```

After the sample flush you should see roughly `hot_rows = 1000`,
`cold_row_count = 12`, and `manifest_state = 'in_sync'`.

Field meanings, sample JSON, and job-progress queries are in
[`koldstore.describe_table`](docs/sql-api.md#koldstoredescribe_table).



## Explain A Managed Query

KoldStore-managed reads show up in `EXPLAIN` as `Custom Scan (KoldMergeScan)`.

```sql
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, BUFFERS OFF)
SELECT id, title
FROM app.messages
WHERE title = 'message-000007';
```

Sample output:

```text
Custom Scan (KoldMergeScan) on messages (actual rows=1 loops=1)
  Filter: (title = 'message-000007'::text)
  Manifest: app/messages/manifest.json, 0.479 ms
  Parquet segment: app/messages/batch-1.parquet, 12 rows, 0.485 ms
 Planning Time: 0.025 ms
 Execution Time: 7.884 ms
```

The result is still normal SQL:

```sql
SELECT id, title
FROM app.messages
WHERE title = 'message-000007';
```

```text
 id |     title
----+----------------
  7 | message-000007
```



## Shared And User Tables

`manage_table` supports two table types.


| Type     | Use when                                        | Cold layout                          |
| -------- | ----------------------------------------------- | ------------------------------------ |
| `shared` | Every query may see the same table-wide archive | `{namespace}/{tableName}/`           |
| `user`   | Rows are scoped to a tenant or user             | `{namespace}/{tableName}/{scopeId}/` |


User-scoped tables require a `scope_column` and a session `koldstore.user_id` value. Reads and writes fail closed when the scope is missing or mismatched.

```sql
SELECT koldstore.manage_table(
  table_name     => 'app.user_messages',
  storage        => 'local-dev',
  hot_row_limit  => 1000,
  table_type     => 'user',
  scope_column   => 'user_id',
  migration_order_by => 'id'
);

SET koldstore.user_id = 'user-123';
```



## Storage Backends


| Provider             | `storage_type` | Example `base_path`         |
| -------------------- | -------------- | --------------------------- |
| Local filesystem     | `filesystem`   | `/var/lib/koldstore`        |
| Amazon S3 / MinIO    | `s3`           | `s3://bucket/prefix/`       |
| Google Cloud Storage | `gcs`          | `gs://bucket/prefix/`       |
| Azure Blob           | `azure`        | `azure://container/prefix/` |


Example S3-compatible registration:

```sql
SELECT koldstore.register_storage(
  name         => 'local-minio',
  storage_type => 's3',
  base_path    => 's3://koldstore-test/',
  credentials  => '{"access_key_id":"minioadmin","secret_access_key":"minioadmin"}'::jsonb,
  config       => '{"endpoint":"http://localhost:9000","region":"us-east-1","path_style":true}'::jsonb
);
```



## Current Requirements

- PostgreSQL 15-18.
- Managed tables need a primary key.
- Supported column types are currently limited to `boolean`, integer types, `real`, `double precision`, `text`, `varchar`, `uuid`, `jsonb`, and `timestamptz`.
- `pgrx` is the recommended local development loop.
- Docker is used for packaging and final runtime smoke checks, not as the main correctness loop.



## Current Limitations

- This is not production ready.
- Cold storage is not WAL-protected. Back up PostgreSQL and the cold storage prefix together.
- `UNIQUE` constraints and foreign keys are enforced on **hot rows only**. After flush, cold Parquet is not checked on normal `INSERT`/`UPDATE`, so duplicates and FK gaps across hot+cold are possible. See [Limitations](docs/limitations.md#unique-and-foreign-key-constraints).
- `koldstore.manage_table` rejects non-PK `UNIQUE` constraints and foreign keys when `hot_row_limit` is set (flush enabled). Use hot-only management or drop those constraints first.
- Primary-key value changes and primary-key definition changes on managed tables are not implemented yet (On the roadmap)
- PostgreSQL indexes cover hot rows only. Flushed rows live in Parquet, not in PostgreSQL-owned indexes.
- If a query needs cold data and the cold storage backend is unavailable, the query errors instead of returning partial hot-only results.
- Export/import, compaction, and richer cold-storage policies are still being built.



## Roadmap

Planned after the 0.1 hot/cold baseline:

- **Smart flush scheduler** — trigger flushes automatically from inside
  KoldStore without relying on `pg_cron`
- **Improve `KoldMergeScan`** — streaming execution, tighter cold lookups, and
  broader planner pushdown
- **Deleted index in manifest** — track flushed delete markers in the
  object-store manifest for tombstone routing and faster cold PK lookups
- **Finish change-log APIs** — public `changes_since` / change-cursor SQL
  surface on top of the `__cl` mirror
- **Storage file type** — a datatype to upload and fetch files directly from
  registered cold storage
- **Import / export** — table-level archive import and export of managed data
- **Backup / restore** — coordinated PostgreSQL + cold-storage backup and
  restore workflows

More deferred work (compaction, alter-table, schema evolution, and so on) is
tracked in the [project roadmap](docs/roadmap.md).



## In Development

### Change cursors (`changes_since`)

Managing a table already creates a **latest-state change-log mirror** (`koldstore.<table>__cl`): one row per primary key with a monotonic `seq` and `op` (`INSERT` / `UPDATE` / `DELETE`). KoldStore installs the capture triggers once at `manage_table` so flush can cut by `seq` and scans know which keys are still hot. The mirror is **not** an append-only history of every intermediate update (a later `UPDATE` overwrites the previous mirror row for that PK).

That same mirror is the foundation for **incremental sync / catch-up consumers** without standing up a separate CDC stack. Downstream jobs should not need Debezium, logical replication slots, WAL decoding plugins, or additional application triggers just to answer “what changed since cursor X?” — the cursor metadata is already maintained for flush.

Planned SQL surface:

```sql
-- Resume from the last seq you processed.
SELECT *
FROM koldstore.changes_since(
  table_name => 'app.messages',
  since_seq  => 332882280164896768,
  limit_rows => 1000
);
```

That returns the latest state per primary key with `seq > since_seq` (including deletes), ordered by `seq`, so sync jobs, caches, search indexes, and downstream services can poll incrementally instead of rescanning the whole table. The merge library already implements the cursor logic; the public SQL function is not exposed yet.

Until then you can inspect the hot mirror directly (same semantics for keys still in the hot working set):

```sql
SELECT id, seq, op
FROM koldstore.messages__cl
WHERE seq > 332882280164896768
ORDER BY seq
LIMIT 1000;
```

Note: today’s `__cl` mirror is **latest-state**, not an append-only WAL of every intermediate update. `changes_since` is aimed at “catch me up to current state since this cursor,” not full temporal audit replay. Cold-flushed keys are represented through flush/manifest metadata; the public cursor API will document how hot + cold changes are unified.



## Development

Useful local commands:

```bash
cargo test --workspace
cargo pgrx install -p pg_koldstore --no-default-features --features pg16
scripts/run-pg-e2e.sh 16
scripts/run-storage-comparison.sh
```

Project docs:

- [SQL API](docs/sql-api.md)
- [Architecture overview](docs/architecture.md)
- [Limitations](docs/limitations.md)



## License

Apache License 2.0. Copyright 2026 KalamDB.

See [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0).