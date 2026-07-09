# pg-koldstore

> Keep PostgreSQL fast by moving old rows to cold Parquet storage while queries keep using the same table.

**Status: early development - not production ready.** The extension builds and the core flow works, but recovery, export/import, and background flush behavior are still being hardened.

<p align="center">
  <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/rust-1.96%2B-orange.svg" alt="Rust 1.96+" /></a>
  <a href="https://www.apache.org/licenses/LICENSE-2.0"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License" /></a>
  <a href="https://github.com/kalamdb/pg-kalam/actions/workflows/pg-koldstore-ci.yml"><img src="https://github.com/kalamdb/pg-kalam/actions/workflows/pg-koldstore-ci.yml/badge.svg" alt="CI" /></a>
  <a href="https://github.com/kalamdb/pg-kalam/actions/workflows/pg-koldstore-ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/kalamdb/pg-kalam/pg-koldstore-ci.yml?branch=main&amp;label=tests" alt="Tests" /></a>
  <a href="https://github.com/kalamdb/pg-kalam/releases"><img src="https://img.shields.io/github/v/release/kalamdb/pg-kalam?display_name=tag&amp;label=extension" alt="Extension Version" /></a>
  <a href="https://github.com/kalamdb/pg-kalam/actions/workflows/release.yml"><img src="https://github.com/kalamdb/pg-kalam/actions/workflows/release.yml/badge.svg" alt="Release" /></a>
</p>

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

- Does not replace PostgreSQL with another database.
- Does not require changing PostgreSQL's table storage engine.
- Does not force years of historical data to remain inside the PostgreSQL heap.
- Does not rely on proprietary columnar storage formats.
- Does not force users into time-series-only data models.
- Does not require millions of PostgreSQL partitions to organize historical rows.
- Does not lock archived data into a vendor-specific format.
- Keeps the hot path close to normal PostgreSQL tables while adding cold storage only where it is enabled.
- Reduces the operational pressure that historical rows put on backup size, VACUUM, indexes, and primary database storage.
- Uses open Parquet files that modern analytics engines can read directly.
- Can evolve into a full storage lifecycle layer while remaining transparent to applications.

## How It Works

```text
Application SQL
    |
    v
Normal PostgreSQL table
    |  AFTER ROW capture triggers
    v
koldstore.<table>__cl mirror
    |  flush_table()
    v
Parquet segment + manifest.json

SELECT from the original table -> Custom Scan (KoldMergeScan)
```

The user table stays clean. Management creates a companion latest-state mirror table in the `koldstore` schema. For `app.messages`, that mirror is `koldstore.messages__cl`.

The mirror stores one row per primary key with KoldStore metadata:


| Column              | Purpose                                                    |
| ------------------- | ---------------------------------------------------------- |
| primary key columns | Same shape as the source table primary key                 |
| `seq`               | Latest-state conflict-free sequence used for flush cutoffs and change cursors |
| `op`                | `1 = insert`, `2 = update`, `3 = delete`                   |
| `commit_lsn`        | Optional PostgreSQL LSN for diagnostics                    |


Flush writes the mirror-selected rows to a Parquet batch, updates `manifest.json`, records segment metadata in `koldstore.cold_segments`, and prunes flushed rows from the hot heap when safe.

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
  order_column      => 'id'
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



## View Table And Management Stats

Use `koldstore.describe_table` to see what is hot, what is cold, and whether anything is still pending.

```sql
SELECT jsonb_pretty(koldstore.describe_table(table_name => 'app.messages'));
```

Sample result after the flush:

```json
{
  "jobs": [
    {
      "id": "e30eb374-a9db-4ff1-97d3-72f8511dfc60",
      "phase": "finished",
      "status": "completed",
      "job_type": "flush",
      "updated_at": "2026-07-07T16:56:10.123456+03:00",
      "rows_flushed": 12,
      "checkpoint_seq": 332882280212668416,
      "rows_processed": 12,
      "checkpoint_commit_seq": 332882280212668416
    },
    {
      "id": "2c2bcf44-d6ea-4b3e-b62c-cfaf18ad5225",
      "phase": "finished",
      "status": "completed",
      "job_type": "migrate_backfill",
      "updated_at": "2026-07-07T16:56:09.987654+03:00",
      "rows_flushed": 0,
      "checkpoint_seq": 0,
      "rows_processed": 1012,
      "checkpoint_commit_seq": 0
    }
  ],
  "hot_rows": 1000,
  "mirror_rows": 1000,
  "cold_row_count": 12,
  "cold_segment_count": 1,
  "heap_size_bytes": 442368,
  "table_size_bytes": 606208,
  "index_size_bytes": 16384,
  "manifest_state": "in_sync",
  "manifest_max_seq": 332882280212668416,
  "pending_jobs": 0,
  "storage_binding": "4a3b2ab3-5ea8-4761-9e37-1a2f98b128e4",
  "last_error": null
}
```

The fields to watch most often are:


| Field                | Meaning                                         |
| -------------------- | ----------------------------------------------- |
| `hot_rows`           | Rows still present in the PostgreSQL heap       |
| `mirror_rows`        | Primary keys tracked in the `__cl` mirror       |
| `cold_row_count`     | Rows already copied to active cold segments     |
| `cold_segment_count` | Active Parquet segment count                    |
| `manifest_state`     | `in_sync` means catalog and manifest agree      |
| `manifest_max_seq`   | Highest mirror `seq` represented in cold data   |
| `pending_jobs`       | Pending or running KoldStore jobs for the table |
| `jobs`               | Recent job ids, phases, and progress counters   |
| `last_error`         | Last manifest or storage error, if any          |


For job-level progress, inspect `koldstore.jobs`:

```sql
SELECT job_type, status, phase, rows_processed, rows_flushed, error_trace
FROM koldstore.jobs
WHERE table_oid = 'app.messages'::regclass
ORDER BY created_at DESC
LIMIT 5;
```



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
  order_column   => 'id'
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
- Primary-key value changes and primary-key definition changes on managed tables are not implemented.
- PostgreSQL indexes cover hot rows only. Flushed rows live in Parquet, not in PostgreSQL-owned indexes.
- If a query needs cold data and the cold storage backend is unavailable, the query errors instead of returning partial hot-only results.
- Export/import, compaction, and richer cold-storage policies are still being built.



## Development

Useful local commands:

```bash
cargo test --workspace
cargo pgrx install -p pg_koldstore --no-default-features --features pg16
scripts/run-pg-e2e.sh 16
```

Project docs:
- [SQL API](docs/sql-api.md)
- [Architecture overview](docs/architecture.md)
- [Limitations](docs/limitations.md)



## License

Apache License 2.0. Copyright 2026 KalamDB.

See [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0).
