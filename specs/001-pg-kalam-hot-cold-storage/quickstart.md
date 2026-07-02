# Quickstart: Validate pg-kalam Hot/Cold Storage

**Branch**: `001-pg-kalam-hot-cold-storage`  
**Purpose**: Runnable validation scenarios proving end-to-end behavior. Implementation details belong in `tasks.md`.

**References**:

- [data-model.md](./data-model.md) — entities and merge rules
- [contracts/sql-api.md](./contracts/sql-api.md) — SQL surface
- [contracts/kalam-merge-scan.md](./contracts/kalam-merge-scan.md) — scan behavior
- [contracts/manifest-schema.json](./contracts/manifest-schema.json) — cold manifest format

---

## Prerequisites

- PostgreSQL 15+ (16+ recommended)
- Rust toolchain (for building extension via pgrx)
- Docker (for MinIO integration tests)
- `pg_kalam` extension built and installed

```bash
# From repo root (after implementation exists)
cargo pgrx install --release
```

---

## Environment Setup

### 1. Start PostgreSQL + MinIO

```bash
docker compose -f tests/docker-compose.yml up -d
```

Expected: PostgreSQL on `localhost:5432`, MinIO on `localhost:9000`.

### 2. Create extension and storage

```sql
CREATE EXTENSION pg_kalam;

SELECT kalam.register_storage(
  'local-minio',
  's3',
  's3://kalam-test/',
  '{"access_key_id":"minioadmin","secret_access_key":"minioadmin"}'::jsonb,
  '{"endpoint":"http://localhost:9000","region":"us-east-1","path_style":true}'::jsonb
);
```

**Expected**: Returns UUID; row visible in `kalam.storage` (credentials restricted).

---

## Scenario 0: Create Kalam Table (P1 — primary path)

```sql
CREATE TABLE app.shared_items (
  id BIGINT PRIMARY KEY DEFAULT SNOWFLAKE_ID(),
  title TEXT NOT NULL,
  value INTEGER,
  created_at TIMESTAMP DEFAULT NOW()
) USING kalamdb WITH (
  type = 'shared',
  flush_policy = 'rows:1000,interval:60',
  storage_id = 'local-minio'
);

INSERT INTO app.shared_items (title, value) VALUES ('hello', 1);
SELECT kalam_version();
```

**Verify**: `_seq`/`_deleted` present; row readable; extension version returned.

---

## Scenario 1: Migrate Existing Table (P1)

```sql
CREATE TABLE chat.messages (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  body text NOT NULL,
  created_at timestamptz DEFAULT now()
);

INSERT INTO chat.messages (body) VALUES ('hello'), ('world');

SELECT kalam.migrate_table(
  'chat.messages',
  'shared',
  'local-minio',
  NULL  -- hot-only initially
);
```

**Verify**:

```sql
\d chat.messages
-- Must show _seq, _deleted columns

SELECT id, body, _seq, _deleted FROM chat.messages;
-- 2 rows, _deleted = false, distinct _seq values
```

---

## Scenario 2: KalamMergeScan Active (P2)

```sql
EXPLAIN (VERBOSE, COSTS OFF)
SELECT * FROM chat.messages WHERE id = (SELECT id FROM chat.messages LIMIT 1);
```

**Expected**:

- Plan contains `Custom Scan (KalamMergeScan)` on `chat.messages`
- No standalone `Seq Scan` on `chat.messages` as the only access path

---

## Scenario 3: Flush to Cold + Merged Query (P1/P2)

```sql
SELECT kalam.set_flush_policy('chat.messages', 'rows:2');

-- Insert until >2 hot rows
INSERT INTO chat.messages (body) SELECT 'msg-' || g FROM generate_series(1, 5) g;

-- Trigger flush (or wait for background worker)
SELECT kalam.flush_table('chat.messages');

-- Poll job
SELECT status, error_trace FROM system.jobs ORDER BY created_at DESC LIMIT 1;
-- Expected: completed

SELECT sync_state FROM kalam.manifest WHERE table_oid = 'chat.messages'::regclass::oid;
-- Expected: in_sync
```

**Verify cold artifacts** (MinIO console or `mc ls`):

- `kalam-test/chat.messages/manifest.json`
- `kalam-test/chat.messages/batch-*.parquet`

**Verify merged read**:

```sql
SELECT count(*) FROM chat.messages;
-- Expected: total logical rows (hot + cold), excluding tombstones

SELECT kalam.table_status('chat.messages');
-- cold_segment_count > 0
```

---

## Scenario 4: Version Resolution (P2)

```sql
-- Pick a row, note id
WITH r AS (SELECT id FROM chat.messages LIMIT 1)
UPDATE chat.messages SET body = 'updated-hot' WHERE id = (SELECT id FROM r);

SELECT body FROM chat.messages WHERE id = (SELECT id FROM r LIMIT 1);
-- Expected: 'updated-hot' (hot _seq wins over older cold version)
```

---

## Scenario 5: User-Scoped Security (P2)

```sql
CREATE TABLE app.notes (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id uuid NOT NULL,
  content text
);

SELECT kalam.migrate_table('app.notes', 'user_scoped', 'local-minio', NULL, 'user_id');

INSERT INTO app.notes (user_id, content) VALUES
  ('aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa', 'user A note'),
  ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'user B note');
```

**Missing scope → error**:

```sql
SELECT * FROM app.notes;
-- ERROR: kalam scope not set
```

**Wrong scope → no rows / denied**:

```sql
SET kalam.user_id = 'aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa';
SELECT kalam_user_id();
SELECT content FROM app.notes;
-- 1 row: user A note only
```

**Cross-scope write denied** (with RLS):

```sql
INSERT INTO app.notes (user_id, content)
VALUES ('bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb', 'attack');
-- ERROR or policy violation
```

---

## Scenario 6: Cold-Only Update via kalam.update (P2)

After flush removes a row from hot heap:

```sql
SELECT kalam.delete('chat.messages', '{"id": "<cold-only-uuid>"}'::jsonb);
-- Returns tombstone _seq

SELECT * FROM chat.messages WHERE id = '<cold-only-uuid>';
-- 0 rows (tombstone wins)
```

---

## Scenario 7: Operability (P3)

```sql
SELECT * FROM kalam.backup_manifest('chat.messages');
SELECT * FROM kalam.validate_cold_storage('chat.messages');
```

**Expected**: All segments `checksum_ok = true`, `manifest_ok = true`.

---

## Scenario 8: FILE Column (P3)

```sql
CREATE TABLE docs.files (
  id uuid PRIMARY KEY,
  payload kalam.file
);

SELECT kalam.migrate_table('docs.files', 'shared', 'local-minio');

-- After file_upload implemented:
-- SELECT kalam.file_upload('docs.files', 'payload', 'test.txt', 'text/plain', 'hello'::bytea);
```

**Expected**: Blob in object storage; JSON reference in row; manifest `files` state updated.

---

## Automated Test Commands

```bash
# Rust unit tests
cargo test

# PostgreSQL regression (after tests/sql/ exists)
cargo pgrx test

# Integration suite
./tests/integration/run.sh
```

**CI gate**: All scenarios 0–5 and 9 must pass before MVP release.

---

## Scenario 9: Cold Delete Tombstone (P2)

```sql
-- After flush, row exists only in cold
SELECT kalam.delete('chat.messages', '{"id": "<cold-only-id>"}'::jsonb);
SELECT * FROM chat.messages WHERE id = '<cold-only-id>';
-- 0 rows (tombstone overrides cold version)
```

---

## Scenario 10: Export/Import via kalam_exec (P2)

```sql
SELECT kalam_exec('EXPORT TABLE app.shared_items');
-- import into new table per kalamdb transfer format
```

---

## Scenario 11: DROP TABLE cleans storage (P2)

```sql
DROP TABLE app.shared_items;
-- manifest + Parquet prefix removed from object store
```

---

## Failure Indicators

| Symptom | Likely cause |
|---------|--------------|
| Seq Scan on managed table | Planner hook not removing vanilla paths |
| Missing cold rows | Segment visibility MVCC bug or manifest not synced |
| Cross-tenant data visible | RLS not applied on cold DataFusion path |
| Flush stuck in `error` | Object store credentials or network |
| Duplicate PK in query results | Merge resolver bug |
| Cold delete still visible in default SELECT | Tombstone not winning PK merge |
| Delete missing from change-feed | Tombstone filtered incorrectly in changelog mode |

---

## Scenario 12: Change-feed includes deletes (P2)

```sql
INSERT INTO app.shared_items (title) VALUES ('item-1');
UPDATE app.shared_items SET title = 'item-1-upd' WHERE title = 'item-1';
DELETE FROM app.shared_items WHERE title = 'item-1-upd';

-- Default view: row gone
SELECT count(*) FROM app.shared_items WHERE title LIKE 'item-1%';
-- 0

-- Change-feed: tombstone visible
SELECT _seq, _deleted, title FROM kalam.changes_since('app.shared_items', 0);
-- 3 rows: insert (_deleted=false), update (_deleted=false), delete (_deleted=true)
```

---

## Out of Scope for This Quickstart

- Transparent UPDATE/DELETE on cold-only rows via standard SQL
- Parquet compaction
- Vector index cold paths
- Multi-node replica flush (primary only)
- Full realtime subscription transport (change-feed `_seq` visibility is in scope)
