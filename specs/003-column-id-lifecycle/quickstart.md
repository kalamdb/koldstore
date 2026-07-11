# Quickstart: Validate Catalog Column Identity & Segment Lifecycle

**Feature**: `003-column-id-lifecycle`  
**Date**: 2026-07-11

Hard cutover: recreate managed tables / wipe local cold prefixes used in tests. Do not expect old `batch-*.parquet` or name-keyed stats to work.

## Prerequisites

- Workspace builds with existing pgrx toolchain
- Local pgrx Postgres available (`cargo pgrx` workflow)
- Sibling KalamDB checkout at `/Users/jamal/git/KalamDB` for design reference (not a runtime dependency)

## 1. Library checks

```bash
cargo test -p koldstore-catalog -p koldstore-schema -p koldstore-parquet -p koldstore-manifest -p koldstore-flush
```

Expect: column_id / footer-stats / lifecycle / `segment-NNNN` unit tests green; no tests asserting `batch-` or name-keyed stats.

## 2. Install extension into pgrx Postgres

Use the repo’s normal pgrx install path (same as other features), then create extension `koldstore`.

## 3. Smoke: manage, flush, naming, column_id

```sql
CREATE TABLE items (
  id bigint PRIMARY KEY,
  title text,
  created_at timestamptz
);
-- manage via existing koldstore manage API
-- insert rows, flush

SELECT object_path, status, column_stats
FROM koldstore.segments
ORDER BY created_at;
-- expect: .../segment-0001.parquet (padded), status = published
-- expect: column_stats keys are numeric column ids, not "title"
```

## 4. Smoke: rename survives cold read

```sql
ALTER TABLE items RENAME COLUMN created_at TO sent_at;
-- flush again if required by refresh-on-flush behavior
SELECT id, sent_at FROM items ORDER BY id;
-- historical cold values still present under sent_at
```

## 5. Smoke: add / drop id reuse

```sql
ALTER TABLE items ADD COLUMN note text;
-- flush; note gets new column_id
ALTER TABLE items DROP COLUMN note;
ALTER TABLE items ADD COLUMN tag text;
-- tag column_id != note's former id
```

## 6. E2E suite (local)

```bash
# use repo scripts for pgrx e2e once tests exist under tests/e2e
./scripts/run-pg-e2e.sh
```

Focus: schema evolution, column_id stability, segment lifecycle (see [contracts/test-plan.md](./contracts/test-plan.md)).

## Success signals

- Catalog API returns active + historical versions with `column_id`
- New cold objects use `segment-{NNNN}.parquet` only
- Stats keyed by `column_id`; footer-derived (no duplicate encode bounds path)
- Rename/add/drop/compatible alter behave like KalamDB identity rules
- Lifecycle states are the new enum only
