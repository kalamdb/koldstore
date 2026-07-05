# pg-koldstore

> **Keep PostgreSQL fast. Archive flushed rows to low-cost object storage. Query both as one.**

**Status: early development — not production ready.** The extension builds and the core design is in place, but we're still hardening flush recovery, export/import, and the background worker. Treat this as experimental until we ship a 1.0.

<p align="center">
  <a href="https://www.rust-lang.org/"><img src="https://img.shields.io/badge/rust-1.96%2B-orange.svg" alt="Rust 1.96+" /></a>
  <a href="https://www.apache.org/licenses/LICENSE-2.0"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="License" /></a>
  <a href="https://github.com/kalamdb/pg-kalam/actions/workflows/pg-koldstore-ci.yml"><img src="https://github.com/kalamdb/pg-kalam/actions/workflows/pg-koldstore-ci.yml/badge.svg" alt="CI" /></a>
  <a href="https://github.com/kalamdb/pg-kalam/actions/workflows/pg-koldstore-ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/kalamdb/pg-kalam/pg-koldstore-ci.yml?branch=main&amp;label=tests" alt="Tests" /></a>
  <a href="https://github.com/kalamdb/pg-kalam/releases"><img src="https://img.shields.io/github/v/release/kalamdb/pg-kalam?display_name=tag&amp;label=extension" alt="Extension Version" /></a>
  <a href="https://github.com/kalamdb/pg-kalam/actions/workflows/release.yml"><img src="https://github.com/kalamdb/pg-kalam/actions/workflows/release.yml/badge.svg" alt="Release" /></a>
</p>

`pg-koldstore` is a PostgreSQL extension (`koldstore`) that lets you keep recent rows in the heap and push older ones to Parquet on object storage — S3 (including MinIO), GCS, Azure Blob, or a local path. You still query the same table name; reads go through a `KoldstoreMergeScan` custom scan that merges hot heap rows with cold segments.

PostgreSQL stays in charge of transactions, locking, and permissions. Cold files follow a kalamdb-compatible manifest + Parquet layout so they can be read outside the database too.

---

## The problem

Tables grow. Indexes grow with them. Backups get slower, VACUUM takes longer, and you're paying for terabytes of history that almost nobody touches in day-to-day queries.

The usual fixes — archive tables, cron dumps, time partitions, a separate data lake — work, but they're another thing to operate. pg-koldstore tries to fold archival into PostgreSQL itself: same SQL, same table name, cold data on cheap storage.

---

## How it works

```
                 PostgreSQL

        ┌─────────────────────────┐
        │      Hot Storage        │
        │   (normal heap table)   │
        │                         │
        │ Active rows             │
        │ One row per PK          │
        └──────────┬──────────────┘
                   │
              flush job
                   │
                   ▼
        ┌─────────────────────────┐
        │      Cold Storage       │
        │  Parquet + manifest     │
        │  filesystem / S3 / GCS  │
        │  / Azure                │
        └─────────────────────────┘

   SELECT  →  KoldstoreMergeScan merges by primary key
```

You create a normal table, register storage, and call `koldstore.migrate_table`. Inserts and updates hit the heap like always. KoldStore tracks latest change state in an internal per-table `koldstore.<table>__cl` mirror instead of adding metadata columns to your table. A flush writes eligible hot rows and mirror metadata to Parquet, updates the manifest, and drops flushed rows from the heap when safe. Delete markers are preserved when cold might still have an older version of the same key.

---

## Shared vs user tables

When you migrate a table you pick `table_type => 'shared'` or `'user'`. They behave differently on cold storage layout and access control.

**Shared** — one cold prefix for the whole table. Good for app-wide data: audit logs, product catalog history, anything where every query is allowed to see every row. Cold path looks like `{namespace}/{tableName}/`.

**User** — data is scoped to a tenant or user. You pass a `scope_column` (e.g. `user_id`) and set `koldstore.user_id` in the session. Reads and writes fail if the GUC isn't set or doesn't match the row. Cold files land under `{namespace}/{tableName}/{scopeId}/`, so each tenant's archive is separate in object storage.

Why bother with two types? Multi-tenant apps often want per-user isolation and per-user cold layout. In PostgreSQL you'd reach for `PARTITION BY LIST (user_id)` — but that doesn't scale to millions of users. Partition counts blow up, planner overhead grows, and managing that many child tables is painful. pg-koldstore pushes that partitioning problem to object storage prefixes instead of PostgreSQL catalog entries. The heap stays one table; cold storage fans out by scope.

---

## Quick start

```sql
CREATE EXTENSION koldstore;

SELECT koldstore.register_storage(
  'local-minio',
  's3',
  's3://koldstore-test/',
  '{"access_key_id":"minioadmin","secret_access_key":"minioadmin"}'::jsonb,
  '{"endpoint":"http://localhost:9000","region":"us-east-1","path_style":true}'::jsonb
);

CREATE TABLE chat.messages (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL,
  body text NOT NULL,
  created_at timestamptz DEFAULT now()
);

SELECT koldstore.migrate_table(
  table_name   => 'chat.messages',
  table_type   => 'user',
  storage_name => 'local-minio',
  flush_policy => 'rows:1000',
  scope_column => 'user_id'
);

SET koldstore.user_id = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';

INSERT INTO chat.messages (user_id, body)
VALUES ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'hello');

SELECT koldstore.flush_table('chat.messages', force => true);
SELECT * FROM chat.messages;
```

### Flush policies (today)

Right now retention is driven by mirror row count by default, with optional row-age duration policies:

```
rows:<count>
duration:<age>
```

Set it at migration or later with `koldstore.set_flush_policy`. `rows:N` is the default hot-row-limit policy: KoldStore checks pending latest-state rows in the table's internal change-log mirror and flushes the oldest rows by `seq` when the limit is exceeded. `duration:1d` or `duration:5d` flushes mirror rows whose `changed_at` is older than that duration. If an `interval:<seconds>` spelling remains supported, it means row age in seconds, not time since the last flush. You can also call `koldstore.flush_table` or `koldstore.flush_pending()` yourself.

### Cold storage policies (planned)

We want something closer to how you'd think about retention — archive rows that match a condition:

```sql
-- not implemented yet; syntax subject to change
CREATE COLD STORAGE POLICY messages_archive
  ON chat.messages
  WHERE created_at < now() - interval '7 days';
```

That would let you say "keep the last week in PostgreSQL, push everything older to cold" without tuning row-count thresholds. Today you approximate this with flush frequency and how much hot data you're willing to hold; column-based policies are on the roadmap for v0.3.

### Where cold files land (user table)

```
chat/messages/
  aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa/
    manifest.json
    batch-0.parquet
  bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb/
    manifest.json
    batch-0.parquet
```

---

## What works in v0.1

- `CREATE EXTENSION koldstore` on PostgreSQL 15–18
- `koldstore.migrate_table` for shared and user-scoped tables
- Storage registration: filesystem, S3, GCS, Azure (MinIO via S3-compatible config)
- Manual flush + `rows:N` and `duration:S` policies
- `KoldstoreMergeScan` for merged hot/cold reads
- Hot DML without object-store reads on the write path
- Cold-only helpers: `hydrate_pk`, `update_row`, `delete_row`
- Latest-state change feed via `changes_since`
- Demigration with optional rehydration
- Operator functions: `table_status`, `validate_cold_storage`, `recover_segments`

Things still rough around the edges: background flush needs `shared_preload_libraries = 'koldstore'`, export/import isn't finished, and cold storage isn't in WAL — you need to back up PostgreSQL and your object-store prefixes together.

---

## Requirements

Managed tables need a **primary key**. Clean-schema migration does not add KoldStore columns to the user table. Instead, KoldStore creates an internal per-table change-log mirror such as `koldstore.messages__cl` with the same primary-key columns plus:

| Column | What it's for |
|--------|---------------|
| `seq` | Latest-state version used for flush cutoffs and change-feed cursors |
| `op` | `1 = INSERT`, `2 = UPDATE`, `3 = DELETE` |
| `changed_at` | Change timestamp |
| `commit_lsn` | Optional PostgreSQL LSN for diagnostics |

`koldstore.changes_since` reads latest-state changes from the mirror and flushed cold metadata. It is not a full event-history feed; repeated changes to one primary key may be collapsed to the latest available state.

### Type support

v0.1 only allows a small set of types: `boolean`, integer types, `real`, `double precision`, `text`, `varchar`, `uuid`, `jsonb`, `timestamptz`.

That's not because PostgreSQL can't store other types — it's because every type we support has to map cleanly to Arrow/Parquet, round-trip through flush and merge-scan, and have tests. `numeric`, `bytea`, arrays, `json` (non-`jsonb`), enums, ranges, geometry, and most extension types aren't wired up yet. We'll expand the matrix as we go; migration will reject unsupported columns up front rather than silently corrupting data.

### Known limitations

- FK constraints with flush enabled need `options.allow_fk_hot_only => true`; native FK checks only see hot rows
- If a query needs cold data and object storage is down, you get an error — we don't fall back to hot-only results
- Altering primary-key values or primary-key definitions on managed tables is not implemented yet.
- Custom PostgreSQL indexes do not move to cold storage. When rows are flushed out of the heap, their entries disappear from PostgreSQL-owned indexes.
- pgvector indexes are hot-only. HNSW and IVFFlat indexes only cover rows still resident in the PostgreSQL table; flushed vector values may live in cold files in future vector support, but they are not included in the live pgvector index.
- ParadeDB/BM25 and other extension indexes follow the same rule: they index PostgreSQL-resident rows, not external Parquet cold files, unless Kalam builds a separate cold index for them.

---

## Storage backends

| Provider | `storage_type` | Example `base_path` |
|----------|----------------|---------------------|
| Local filesystem | `filesystem` | `file:///var/koldstore/` |
| Amazon S3 / MinIO | `s3` | `s3://bucket/prefix/` |
| Google Cloud Storage | `gcs` | `gs://bucket/prefix/` |
| Azure Blob | `azure` | `azure://...` or `abfs://...` |


---

## Example use cases

- Chat and messaging history
- Notifications and activity feeds
- Audit and event logs
- AI conversation history
- IoT telemetry
- User timelines
- Analytics history retained cheaply but still queryable through SQL

---

## Roadmap

**v0.1 (now)** — extension skeleton, migrate/flush/merge-scan, user-scoped layouts, basic operator tooling.

**v0.2** — background flush worker, real `EXPORT TABLE`, harder failure recovery.

**v0.3** — `IMPORT TABLE`, segment compaction, column-based cold storage policies (the `CREATE COLD STORAGE POLICY` idea above), more type coverage.

**Future** — cold vector search with Kalam-managed segment indexes, likely using USearch as a custom vector index stored as sidecar files beside Parquet segments, for example `segment-0001.parquet` plus `segment-0001.usearch`.

**v1.0** — production guidance, monitoring, backup/PITR docs, benchmarks.

---

## License

Apache License 2.0. Copyright 2026 KalamDB.

See [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0).

---

## Contributing

Bug reports, ideas, and PRs welcome. This is early — if something looks wrong in the docs or the code, open an issue.
