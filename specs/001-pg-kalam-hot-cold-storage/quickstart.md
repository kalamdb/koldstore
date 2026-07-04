# Quickstart: Validate pg-koldstore Hot/Cold Storage

**Branch**: `001-pg-koldstore-hot-cold-storage`
**Purpose**: Runnable validation scenarios for the corrected MVP architecture.

## References

- [data-model.md](./data-model.md)
- [contracts/sql-api.md](./contracts/sql-api.md)
- [contracts/koldstore-merge-scan.md](./contracts/koldstore-merge-scan.md)
- [contracts/dml-rewrite.md](./contracts/dml-rewrite.md)
- [contracts/test-plan.md](./contracts/test-plan.md)

## Prerequisites

- PostgreSQL 15+.
- `koldstore` extension built and installed.
- MinIO or another object-store-compatible test backend.
- For built-in flush worker testing: `shared_preload_libraries = 'koldstore'` and PostgreSQL restarted.

```bash
cargo pgrx install --release -p pg_koldstore --no-default-features --features pg16 --pg-config "$(cargo pgrx info pg-config 16)"
docker compose -f tests/docker-compose.yml up -d
```

## Setup

```sql
CREATE EXTENSION koldstore;

SELECT koldstore.register_storage(
  'local-minio',
  's3',
  's3://koldstore-test/',
  '{"access_key_id":"minioadmin","secret_access_key":"minioadmin"}'::jsonb,
  '{"endpoint":"http://localhost:9000","region":"us-east-1","path_style":true}'::jsonb
);
```

## Scenario 1: Greenfield Table Management (P1)

```sql
CREATE SCHEMA IF NOT EXISTS app;

CREATE TABLE app.shared_items (
  id bigint PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
  title text NOT NULL,
  value integer,
  created_at timestamptz DEFAULT now()
);

SELECT koldstore.migrate_table(
  table_name => 'app.shared_items',
  table_type => 'shared',
  storage_name => 'local-minio',
  flush_policy => 'rows:1000,interval:60'
);

-- Expected shape:
-- (table_oid, table_type, storage_id, schema_version, scope_column)
-- table_type = shared, schema_version = 1, scope_column IS NULL

INSERT INTO app.shared_items (title, value) VALUES ('hello', 1);

SELECT id, title, _seq, _commit_seq, _deleted
FROM app.shared_items;
```

Expected:

- `_seq`, `_commit_seq`, `_deleted` exist.
- Primary key is still `id`, not `(id, _seq)`.
- One row exists for the inserted PK.

## Scenario 2: Migrate Existing Table (P1)

```sql
CREATE SCHEMA IF NOT EXISTS chat;

CREATE TABLE chat.messages (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  body text NOT NULL,
  created_at timestamptz DEFAULT now()
);

INSERT INTO chat.messages (body) VALUES ('hello'), ('world');

SELECT koldstore.migrate_table(
  table_name => 'chat.messages',
  table_type => 'shared',
  storage_name => 'local-minio',
  flush_policy => NULL
);

\d chat.messages
```

Expected:

- Primary key remains on `id`.
- `_seq`, `_commit_seq`, and `_deleted` are present.
- `SELECT count(*) FROM chat.messages` returns 2.

## Scenario 3: KoldstoreMergeScan Active (P1)

```sql
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM chat.messages WHERE id = (SELECT id FROM chat.messages LIMIT 1);
```

Expected:

- Plan contains `Custom Scan (KoldstoreMergeScan)`.
- There is no standalone heap-only path as the final managed-table scan.

## Scenario 4: Hot DML Keeps One Row Per PK (P1)

```sql
WITH r AS (SELECT id FROM chat.messages LIMIT 1)
UPDATE chat.messages
SET body = 'updated-hot'
WHERE id = (SELECT id FROM r);

WITH r AS (SELECT id FROM chat.messages LIMIT 1)
SELECT id, count(*)
FROM chat.messages
WHERE id = (SELECT id FROM r)
GROUP BY id;
```

Expected:

- Count is 1 for the PK.
- `_seq` and `_commit_seq` advanced.
- No duplicate hot rows exist for the PK.

## Scenario 5: Flush to Cold + Merged Query (P1/P2)

```sql
SELECT koldstore.set_flush_policy('chat.messages', 'rows:2');

INSERT INTO chat.messages (body)
SELECT 'msg-' || g FROM generate_series(1, 5) g;

SELECT koldstore.flush_table('chat.messages', force => true);

SELECT status, error_trace
FROM system.jobs
ORDER BY created_at DESC
LIMIT 1;

SELECT sync_state
FROM koldstore.manifest
WHERE table_oid = 'chat.messages'::regclass::oid;

SELECT count(*) FROM chat.messages;
SELECT * FROM koldstore.table_status('chat.messages');
```

Expected:

- Flush job completed.
- Manifest sync state is `in_sync`.
- Object store contains `manifest.json` and `batch-*.parquet`.
- Logical row count matches expected current rows.

## Scenario 6: Tombstone Only When Cold May Contain PK (P2)

```sql
-- Choose a row that was flushed and has no live hot row.
SELECT koldstore.delete_row(
  'chat.messages',
  pk => '{"id":"<cold-only-uuid>"}'::jsonb
);

SELECT *
FROM chat.messages
WHERE id = '<cold-only-uuid>';

SELECT commit_seq, op, deleted, pk
FROM koldstore.changes_since('chat.messages', 0)
WHERE pk @> '{"id":"<cold-only-uuid>"}'::jsonb
ORDER BY commit_seq DESC
LIMIT 1;
```

Expected:

- Default SELECT returns 0 rows for that PK.
- Change feed shows a delete event ordered by `_commit_seq`.
- No Parquet scan is required on the default delete path if local PK hint is sufficient.

## Scenario 7: Cold-Only Update Requires Hydration (P2)

```sql
-- Standard SQL cold-only UPDATE is not transparent in MVP.
UPDATE chat.messages
SET body = 'should-not-update-cold'
WHERE id = '<cold-only-uuid>';

SELECT koldstore.hydrate_pk(
  'chat.messages',
  '{"id":"<cold-only-uuid>"}'::jsonb
);

UPDATE chat.messages
SET body = 'updated-after-hydrate'
WHERE id = '<cold-only-uuid>';
```

Expected:

- The first UPDATE affects 0 rows unless the row is already hot.
- Hydration brings exactly one row to hot storage.
- The second UPDATE succeeds and keeps one hot row for the PK.

## Scenario 8: User-Scoped Security (P2)

```sql
CREATE TABLE app.notes (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL,
  content text
);

SELECT koldstore.migrate_table(
  'app.notes',
  'user',
  'local-minio',
  NULL,
  'user_id'
);

SELECT * FROM app.notes;
-- ERROR: koldstore.user_id is not set

SET koldstore.user_id = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';

INSERT INTO app.notes (user_id, content)
VALUES ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'user A note');

INSERT INTO app.notes (user_id, content)
VALUES ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'user B note');
-- ERROR or policy violation
```

Expected:

- Missing scope fails closed.
- Cross-scope write is denied.

## Scenario 9: Demigration Rehydrates (P2)

```sql
SELECT koldstore.demigrate_table(
  'chat.messages',
  rehydrate => true,
  drop_cold => false
);

EXPLAIN (COSTS OFF) SELECT * FROM chat.messages;
DELETE FROM chat.messages WHERE id = '<some-id>';
```

Expected:

- Plan no longer contains `KoldstoreMergeScan`.
- Table contains current logical rows from hot+cold at demigration time.
- DELETE is normal physical PostgreSQL DML.
- Cold artifacts remain unless `drop_cold => true`.

## Scenario 10: COPY and Export (P2)

```sql
COPY (SELECT * FROM app.shared_items) TO STDOUT WITH CSV HEADER;

SELECT koldstore_exec('EXPORT TABLE app.shared_items');
```

Expected:

- `COPY (SELECT ...)` uses merged logical SELECT.
- `koldstore_exec EXPORT` includes Parquet and manifest artifacts.

## Automated Test Commands

```bash
cargo test
tests/e2e/run_pg_matrix.sh
```

The E2E runner starts pgrx-managed PostgreSQL, installs `koldstore`, recreates the test database, then runs `cargo nextest run -p e2e --test-threads 1`. Select a PostgreSQL version with the first argument, for example `tests/e2e/run_pg_matrix.sh 17`, or with `KOLDSTORE_E2E_PGVERSION`.

## Failure Indicators

| Symptom | Likely cause |
|---------|--------------|
| `USING koldstore` appears in tests | Stale table-AM assumption. |
| Duplicate hot PK rows | DML hook or migration PK bug. |
| Change feed ordered by `_seq` | Incorrect cursor; should use `_commit_seq`. |
| Cold object read during hot UPDATE | DML path violates performance contract. |
| Mutable app filter pushed before merge | Predicate safety bug. |
| Missing cold rows | Segment visibility or manifest sync bug. |
| Cross-scope data visible | RLS/scope enforcement bug. |
| Demigrated table missing cold-only rows | Rehydrate path bug. |

## Out of Scope for This Quickstart

- Custom table access method.
- Full DataFusion cold engine.
- Transparent standard SQL UPDATE of cold-only rows.
- Global hot+cold FK/UNIQUE enforcement.
- Vector indexes and stream tables.
