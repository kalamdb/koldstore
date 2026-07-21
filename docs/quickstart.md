# Quick Start (detailed)

This walkthrough expands the short Docker demo in the [README](../README.md).
It configures KoldStore's two storage tiers: PostgreSQL for the hot working set
and local Parquet files for cold historical rows. Local filesystem storage lets
you try the extension without S3.

## 0. Shared preload (required)

KoldStore installs planner hooks in `_PG_init`. Those hooks must exist in
**every** backend, or managed `SELECT`s silently fall back to heap-only scans
after flush (missing cold rows).

```bash
# Ubuntu / Debian example (merge with any existing preload list)
echo "shared_preload_libraries = 'koldstore'" | \
  sudo tee /etc/postgresql/16/main/conf.d/koldstore.conf
sudo systemctl restart postgresql@16-main   # reload is NOT enough
```

Docker release images already set this. Confirm:

```sql
SHOW shared_preload_libraries;          -- must include koldstore
SELECT koldstore.preload_status();      -- loaded_via_shared_preload = true
```

`session_preload_libraries` is **not** sufficient.

## 1. Create the extension and register storage

```sql
CREATE EXTENSION koldstore;

SELECT koldstore.register_storage(
  name         => 'local-dev',
  storage_type => 'filesystem',
  base_path    => '/tmp/koldstore-demo',
  credentials  => '{}'::jsonb,
  config       => '{}'::jsonb
);
```

## 2. Create and load a normal heap table

```sql
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

## 3. Manage the table

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

`hot_row_limit` keeps at most 1000 primary keys hot. When the mirror tracks
more than 1000 keys, `koldstore.flush_table` moves only the oldest excess rows
by mirror `seq` to cold storage. The low `min_flush_rows` keeps this small
tutorial predictable; production tables usually use `1000` or higher.

Sample result:

```text
            manage_job_id
--------------------------------------
 2c2bcf44-d6ea-4b3e-b62c-cfaf18ad5225
```

Management runs inline for this table. The returned UUID is the
`migrate_backfill` job id in `koldstore.jobs`.

### Choose strict or async mirror capture

The example above uses `strict`, the default. Strict capture updates the heap
and latest-state mirror in the same transaction, so successful DML is visible
through the managed table immediately and both writes roll back together.

Choose `async` when foreground DML throughput is more important than an
immediately current mirror. To do that, use this call **instead of** the Step 3
call above (a table cannot be managed twice):

```sql
SELECT koldstore.manage_table(
  table_name          => 'app.messages',
  storage             => 'local-dev',
  hot_row_limit       => 1000,
  min_flush_rows      => 1,
  max_rows_per_file   => 500,
  migration_order_by  => 'id',
  mirror_capture_mode => 'async'
) AS manage_job_id;
```

Async mode requires PostgreSQL to start with `wal_level=logical`. This server
setting and restart are the only manual administrator step. Do **not** create a
publication or logical slot yourself: `CREATE EXTENSION` ensures the empty
publication, and the first async `manage_table` creates the database's slot
before changing the table or KoldStore catalogs. Provisioning is idempotent.

Async source transactions commit before mirror work. A database worker normally
applies committed primary-key changes within its 100 ms polling interval. Use
the explicit fence before work that must observe every source commit visible at
the start of the call:

```sql
SELECT koldstore.wait_for_async_mirror();
```

`flush_table` performs this catch-up automatically. To remove the logical slot
and publication, first unmanage every async table and then run:

```sql
SELECT koldstore.disable_async_mirror();
```

The cleanup function is idempotent and refuses to run while an active async
table depends on the infrastructure. A later async `manage_table` recreates it
automatically. See [Mirror capture modes](architecture/mirror-capture-modes.md)
for consistency, WAL-retention, monitoring, and recovery details.

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

## 4. Latest-state mirror (optional inspection)

Management also creates `koldstore.messages__cl`, a latest-state mirror with
one metadata row per primary key. It stores the key columns plus `seq` and
`op`; it does not duplicate full application row payloads. In strict mode the
mirror commits or rolls back with application DML. In async mode it follows
committed WAL and may briefly lag; the worker and consistency fence close that
gap before flush or any caller-selected strong boundary. Full semantics live
in the [architecture docs](architecture.md).

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

With 1012 keys and `hot_row_limit => 1000`, all rows are still hot immediately
after management. Nothing is cold yet:

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

## 5. Flush to cold

`koldstore.flush_table` evaluates the configured flush policy, runs the flush
inline, and returns the flush job id. With `hot_row_limit => 1000` and 1012
tracked keys, only the 12 oldest mirror entries move to cold storage; the
newest 1000 keys stay hot.

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
  001/segment-0001-<8-hex>.parquet
```

Schedule periodic flush with [pg_cron](operations/scheduling.md).

## 6. Describe table stats

```sql
SELECT jsonb_pretty(koldstore.describe_table(table_name => 'app.messages'));
```

After the sample flush you should see roughly `hot_rows = 1000`,
`cold_row_count = 12`, and `manifest_state = 'in_sync'`.

Field meanings, sample JSON, and job-progress queries are in
[`koldstore.describe_table`](sql-api.md#koldstoredescribe_table).

## 7. Explain a managed query

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
  Parquet segment: app/messages/001/segment-0001-<8-hex>.parquet, 12 rows, 0.485 ms
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

## Shared and user tables

`manage_table` supports two table types.

| Type     | Use when                                        | Cold layout                          |
| -------- | ----------------------------------------------- | ------------------------------------ |
| `shared` | Every query may see the same table-wide archive | `{namespace}/{tableName}/`           |
| `user`   | Rows are scoped to a tenant or user             | `{namespace}/{tableName}/{scopeId}/` |

User-scoped tables require a `scope_column` and a session `koldstore.user_id`
value. Reads and writes fail closed when the scope is missing or mismatched.

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

## Storage backends

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
