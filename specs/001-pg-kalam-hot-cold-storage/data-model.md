# Data Model: pg-kalam Hot/Cold Storage

**Branch**: `001-pg-kalam-hot-cold-storage`  
**Date**: 2026-07-02

## Overview

pg-kalam models each managed table as a **logical append-versioned relation**:

```text
Logical Row (default SELECT) = resolve_by_pk( hot ∪ cold ); winner = max(_seq); hide if _deleted
Change-feed (realtime)       = all appended rows ordered by _seq; tombstones visible as delete events
```

Hot data lives in the PostgreSQL heap. Cold data lives in immutable Parquet segments on object storage. Metadata in PostgreSQL catalogs bridges snapshots, flush scheduling, and segment visibility.

---

## Entity Relationship

```text
kalam.storage ──registers──► StorageRegistration
system.schemas ──defines──► ManagedTableDefinition
ManagedTableDefinition ──binds──► StorageRegistration
ManagedTableDefinition ──has──► ManifestCache (kalam.manifest)
ManagedTableDefinition ──produces──► ColdSegment (pg_kalam.cold_segments)
ManagedTableDefinition ──schedules──► BackgroundJob (system.jobs)
ManagedTable ──contains──► LogicalRow (hot and/or cold versions)
ManagedTable ──may have──► FileReference (FILE column type)
```

---

## 1. Managed Table (Application-Facing)

### Shared table

| Attribute | Type | Rules |
|-----------|------|-------|
| `table_oid` | `oid` | PostgreSQL relation OID after migration |
| `table_type` | `enum` | `shared` |
| `primary_key` | column set | Required at migration; used for merge and flush dedup |
| `_seq` | `bigint` | System column; monotonic version id (snowflake-style) |
| `_deleted` | `boolean` | System column; soft-delete / tombstone flag |
| `flush_policy` | `text` | Optional: `rows:N`, `interval:seconds`, or combined |
| `schema_version` | `int` | Current version in `system.schemas` |
| `storage_id` | `uuid` | FK to `kalam.storage` |
| `access_level` | `text` | Role/policy configuration for shared tables |

### User-scoped table

Same as shared, plus:

| Attribute | Type | Rules |
|-----------|------|-------|
| `table_type` | `enum` | `user_scoped` |
| `scope_column` | `name` | Default `user_id`; extensible to `tenant_id`, etc. |
| `session_scope` | session GUC | Required before any query/DML; fail closed if unset |

**State**: A table is `hot_only` (no flush policy), `active` (hot+cold), or `migrating`.

**Validation**:

- Migration rejected without primary key
- User-scoped INSERT without scope → rejected
- Only KalamMergeScan may read managed tables (planner enforced)

---

## 2. Logical Row Version

Represents one version of a business row (hot or cold).

| Field | Type | Source | Notes |
|-------|------|--------|-------|
| PK columns | app types | heap/Parquet | Merge key |
| `_seq` | `bigint` | both | Winner selection |
| `_deleted` | `boolean` | both | Tombstone suppresses older versions |
| App columns | per schema | both | Older segments may omit new columns → NULL |
| `schema_version` | `int` | cold segment metadata | Column evolution |
| `location` | `enum` | derived | `hot`, `cold`, or `both` (during overlap) |

**Merge rules (default application read)**:

1. Group by primary key
2. Keep version with highest `_seq`
3. If winner has `_deleted = true`, row hidden from default SELECT
4. Hot version beats cold at equal `_seq` (should not occur if seq monotonic per table)

**Change-feed rules (realtime readiness)**:

1. Return all version rows with `_seq > watermark` in monotonic order
2. Include tombstones (`_deleted = true`) as delete events — do not collapse away
3. Every INSERT/UPDATE/DELETE appends a new row with a new `_seq` (never in-place)

**Delete on cold-only row**: Append hot tombstone via `kalam.delete()` with new `_seq`; visible in change-feed, hidden in default PK merge.

**State transitions**:

```text
INSERT → hot version (new _seq)
UPDATE → new hot version (append, not in-place)
DELETE → hot tombstone version (_deleted=true)
FLUSH → cold Parquet version written; hot data removed per retention
```

---

## 3. Storage Registration (`kalam.storage`)

| Column | Type | Constraints |
|--------|------|-------------|
| `id` | `uuid` | PK |
| `name` | `text` | UNIQUE, NOT NULL |
| `storage_type` | `text` | `filesystem`, `s3`, `gcs`, `azure` |
| `base_path` | `text` | NOT NULL |
| `credentials` | `jsonb` | Encrypted/restricted; admin-only |
| `config` | `jsonb` | Endpoint, region, path-style, etc. |
| `shared_path_template` | `text` | e.g. `{namespace}/{tableName}/` |
| `user_path_template` | `text` | e.g. `{namespace}/{tableName}/{scopeId}/` |
| `created_at` | `timestamptz` | |
| `updated_at` | `timestamptz` | |

**Validation**: Credential rotation updates row; existing cold artifacts unchanged.

---

## 4. Table Schema Registry (`system.schemas`)

| Column | Type | Constraints |
|--------|------|-------------|
| `id` | `uuid` | PK |
| `table_oid` | `oid` | NOT NULL |
| `version` | `int` | Monotonic per table |
| `table_type` | `text` | `shared` \| `user_scoped` |
| `columns` | `jsonb` | Name, type, nullable, system flag |
| `indexed_columns` | `jsonb` | Columns with PG indexes → Parquet bloom filters |
| `stats_columns` | `jsonb` | Columns with min/max in manifest (PK + `_seq` + indexed) |
| `primary_key` | `jsonb` | Column list |
| `options` | `jsonb` | flush_policy, compression, retention_hours |
| `storage_id` | `uuid` | FK |
| `access_level` | `text` | nullable |
| `scope_column` | `text` | nullable |
| `created_at` | `timestamptz` | |

**Validation**: New schema version on column add; Parquet segments carry `schema_version`.

---

## 5. Manifest Cache (`kalam.manifest`)

Local cache; **not** source of truth over object-store `manifest.json`.

| Column | Type | Constraints |
|--------|------|-------------|
| `id` | `uuid` | PK |
| `table_oid` | `oid` | NOT NULL |
| `scope_key` | `text` | NULL for shared; scope id for user-scoped |
| `manifest_path` | `text` | Object store path |
| `etag` | `text` | Last known |
| `sync_state` | `text` | `in_sync`, `pending_write`, `syncing`, `stale`, `error` |
| `last_refreshed_at` | `timestamptz` | |
| `last_error` | `text` | nullable |
| `segment_count` | `int` | denormalized |
| `max_seq` | `bigint` | denormalized |

**State transitions**:

```text
DML on table → pending_write
Flush start → syncing
Flush commit + manifest update → in_sync
Manifest/object mismatch → stale
Flush failure → error (hot data intact)
```

---

## 6. Cold Segment (`pg_kalam.cold_segments`)

PostgreSQL-side segment registry for MVCC visibility and pruning.

| Column | Type | Constraints |
|--------|------|-------------|
| `segment_id` | `uuid` | PK |
| `table_oid` | `oid` | NOT NULL |
| `scope_key` | `text` | nullable |
| `object_path` | `text` | NOT NULL |
| `batch_number` | `int` | NOT NULL |
| `min_seq` | `bigint` | NOT NULL |
| `max_seq` | `bigint` | NOT NULL |
| `row_count` | `bigint` | |
| `byte_size` | `bigint` | |
| `schema_version` | `int` | NOT NULL |
| `compression` | `text` | `none`, `snappy`, `zstd` |
| `created_lsn` | `pg_lsn` | |
| `created_xid` | `xid` | |
| `commit_seq` | `bigint` | kalam commit ordering |
| `status` | `text` | `pending`, `active`, `deleting`, `deleted` |
| `manifest_etag` | `text` | |
| `created_at` | `timestamptz` | |

**Visibility rule**: KalamMergeScan includes segment iff `status = 'active'` and segment commit is visible to current snapshot.

**Pruning rule**: Skip segment when query bounds on `_seq` (or time-derived bounds) do not intersect `[min_seq, max_seq]`.

---

## 7. Background Job (`system.jobs`)

| Column | Type | Constraints |
|--------|------|-------------|
| `id` | `uuid` | PK |
| `job_type` | `text` | `flush`, `compact`, `cleanup`, `backup`, `restore` |
| `status` | `text` | `pending`, `running`, `completed`, `failed`, `cancelled` |
| `table_oid` | `oid` | nullable |
| `scope_key` | `text` | nullable |
| `parameters` | `jsonb` | Policy, batch size, paths |
| `idempotency_key` | `text` | UNIQUE per active job |
| `attempts` | `int` | default 0 |
| `max_attempts` | `int` | |
| `error_trace` | `text` | nullable |
| `started_at` | `timestamptz` | |
| `completed_at` | `timestamptz` | |
| `created_at` | `timestamptz` | |

---

## 8. PK Registry (`pg_kalam.pk_registry`) — Optional MVP

For flush deduplication and conflict detection (not global UNIQUE constraint replacement).

| Column | Type | Constraints |
|--------|------|-------------|
| `table_oid` | `oid` | PK (composite) |
| `pk_hash` | `bytea` | PK (composite) |
| `latest_seq` | `bigint` | |
| `deleted` | `boolean` | |
| `updated_at` | `timestamptz` | |

---

## 9. FILE Reference (Column Type)

Stored in-row as `jsonb`; blob in object storage.

| Field | Type | Required |
|-------|------|----------|
| `id` | `uuid` | yes |
| `subfolder` | `text` | yes |
| `name` | `text` | yes |
| `size` | `bigint` | yes |
| `mime` | `text` | yes |
| `checksum` | `text` | yes |
| `shard` | `int` | optional |

**Path routing**: Shared table → `{namespace}/{tableName}/files/{subfolder}/`; user-scoped → includes `{scopeId}/`.

**Manifest `files` state**: Tracks subfolder rotation and counts (kalamdb-compatible).

---

## 10. Object-Store Manifest (`manifest.json`)

Source of truth on object storage. Structure matches kalamdb (see `contracts/manifest-schema.json`).

Key arrays:

- `segments[]`: Parquet batch metadata (`batch`, `path`, `min_seq`, `max_seq`, `row_count`, `schema_version`, compression)
- `files{}`: subfolder rotation state for FILE columns
- `vector_indexes[]`: placeholder for future; out of MVP scope

---

## 11. Parquet Segment File

| Property | Value |
|----------|-------|
| Naming | `batch-{N}.parquet` |
| Write pattern | temp file → atomic rename |
| Sort order | By `_seq` ascending |
| Columns | PK + `_seq` + `_deleted` + app columns per `schema_version` |
| Bloom filters | PK columns only (row_count ≥ 1024, FPP 0.01) — kalamdb `parquet/writer.rs` |
| Column stats | PK + `_seq` min/max in manifest `column_stats` |
| Immutability | Segments never updated in place; compaction rewrites post-MVP |

---

## Catalog Schema Namespace

| Schema | Purpose |
|--------|---------|
| `kalam` | `storage`, `manifest` |
| `system` | `schemas`, `jobs` |
| `pg_kalam` | `cold_segments`, `pk_registry`, internal config |

Extension creates schemas on `CREATE EXTENSION pg_kalam`. DDL event trigger records `ALTER TABLE` on managed tables and increments `system.schemas.version`.

**DROP TABLE**: Removes catalog rows and object-storage prefix for the table.

---

## Index Recommendations

| Table | Index | Purpose |
|-------|-------|---------|
| `pg_kalam.cold_segments` | `(table_oid, scope_key, status)` | Segment lookup for merge scan |
| `pg_kalam.cold_segments` | `(table_oid, min_seq, max_seq)` | Pruning |
| `kalam.manifest` | `(table_oid, scope_key)` | Flush discovery |
| `system.jobs` | `(status, job_type)` | Worker polling |
| `pg_kalam.pk_registry` | `(table_oid, pk_hash)` | Flush dedup |

Managed application tables: index on `(pk..., _seq DESC)` on hot heap recommended for hot path performance.
