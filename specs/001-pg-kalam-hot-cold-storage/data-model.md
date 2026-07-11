# Data Model: pg-koldstore Hot/Cold Storage

**Branch**: `001-pg-koldstore-hot-cold-storage`
**Date**: 2026-07-03

## Overview

pg-koldstore models a managed table as:

```text
logical current row = winner_by_pk(one hot heap row, newest cold row); hide if tombstone
change feed        = koldstore.row_events ordered by _commit_seq
```

The PostgreSQL heap is the hot store and keeps at most one row per logical primary key. Cold storage is immutable Parquet on object storage. PostgreSQL catalog tables hold the metadata needed to make external objects visible safely.

## Entity Relationship

```text
koldstore.storage
  -> system.schemas
  -> koldstore.manifest
  -> koldstore.segments
  -> koldstore.cold_pk_hints
  -> koldstore.row_events

managed app table
  -> one hot row per PK
  -> optional hot tombstone per PK when cold may contain an older row
```

## Managed Table

| Attribute | Type | Rules |
|-----------|------|-------|
| `table_oid` | `oid` | PostgreSQL heap relation. |
| `table_type` | `text` | `shared` or `user`. |
| `primary_key` | column set | Required; preserved as the heap primary key. |
| `_seq` | `bigint` | Row/effect version id. |
| `_commit_seq` | `bigint` | Commit-order watermark. |
| `_deleted` | `boolean` | Tombstone mask for older cold rows. |
| `scope_column` | `name` | Required for user tables; app column or `_user_id`. |
| `flush_policy` | `text` | Optional; no policy means hot-only. |
| `storage_id` | `uuid` | References `koldstore.storage`. |
| `schema_version` | `int` | Current schema registry version. |

Validation:

- Migration is rejected without a primary key.
- Migration preserves the primary key; it does not change it to `(pk, _seq)`.
- User-scoped tables fail closed when `koldstore.user_id` is unset.
- Tables with native FKs and flush enabled are rejected by default unless the operator explicitly accepts hot-only FK semantics.

## Hot Row

Represents the current hot overlay for one PK.

| Field | Meaning |
|-------|---------|
| App PK columns | Logical identity and native heap primary key. |
| App columns | Current hot values or tombstone payload defaults. |
| `_seq` | New value on insert/update/delete/revive. |
| `_commit_seq` | Allocated under the transaction-scoped pg-koldstore commit-order lock and committed with the row. |
| `_deleted` | `false` for live overlay; `true` for tombstone. |

Rules:

- Exactly zero or one hot heap row per PK.
- If `_deleted = true`, the row masks older cold rows and is hidden from default SELECT.
- Hot tombstones are retained only while a cold segment may contain an older row for that PK.

## Cold Row

Cold rows live in Parquet segments and are immutable.

| Field | Meaning |
|-------|---------|
| App PK columns | Merge key. |
| App columns | Values at flush time. |
| `_seq` | Version id at flush time. |
| `_commit_seq` | Commit watermark of the flushed hot row. |
| `_deleted` | Usually false; retained cold tombstones only if event retention policy requires them. |
| `schema_version` | Segment schema version. |

Cold rows are never directly updated. Compaction writes replacement segments and swaps manifest suffixes.

## Merge Rules

Default SELECT:

1. Use the hot row when it exists and is newer than the cold winner.
2. Otherwise use the newest cold row by `_seq` / `_commit_seq`.
3. If the winner is `_deleted = true`, return no logical row.
4. Apply mutable app-column predicates after the winner is chosen.

Change feed:

1. Read `koldstore.row_events`.
2. Filter by `commit_seq > since_commit_seq`.
3. Return events ordered by `commit_seq`.
4. Return a gap error if events have expired.

## Storage Registration (`koldstore.storage`)

| Column | Type | Notes |
|--------|------|-------|
| `id` | `uuid` | PK. |
| `name` | `text` | Unique. |
| `storage_type` | `text` | `filesystem`, `s3`, `gcs`, `azure`. |
| `base_path` | `text` | Object-store prefix. |
| `credentials` | `jsonb` | Admin-only, encrypted/restricted. |
| `config` | `jsonb` | Endpoint, region, path-style, etc. |
| `shared_path_template` | `text` | Shared table path. |
| `user_path_template` | `text` | Scoped table path. |

Credential rotation changes future access only; existing object paths are not rewritten.

## Schema Registry (`system.schemas`)

| Column | Type | Notes |
|--------|------|-------|
| `id` | `uuid` | PK. |
| `table_oid` | `oid` | Managed heap relation. |
| `version` | `int` | Monotonic. |
| `table_type` | `text` | `shared` or `user`. |
| `columns` | `jsonb` | App and system columns. |
| `primary_key` | `jsonb` | Preserved app PK columns. |
| `scope_column` | `name` | Nullable for shared tables. |
| `indexed_columns` | `jsonb` | Source for cold stats/bloom decisions. |
| `type_matrix` | `jsonb` | Supported type/coercion metadata. |
| `options` | `jsonb` | Flush, compression, retention, FK policy. |
| `storage_id` | `uuid` | References `koldstore.storage`. |

`ALTER TABLE` on a managed table increments schema version through event-trigger support.

## Manifest Cache (`koldstore.manifest`)

Local cache and scheduler state; object-store `manifest.json` remains the cold source of truth.

| Column | Type | Notes |
|--------|------|-------|
| `table_oid` | `oid` | Managed table. |
| `scope_key` | `text` | Null for shared. |
| `manifest_path` | `text` | Object path. |
| `etag` / `generation` | `text` | Backend-specific identity. |
| `sync_state` | `text` | `in_sync`, `pending_write`, `syncing`, `stale`, `error`. |
| `segment_count` | `int` | Denormalized. |
| `max_seq` | `bigint` | Highest segment `_seq`. |
| `max_commit_seq` | `bigint` | Highest segment `_commit_seq`. |
| `last_error` | `text` | Nullable. |

Normal DML only marks `pending_write`; it does not rewrite object-store manifests.

## Cold Segment (`koldstore.segments`)

| Column | Type | Notes |
|--------|------|-------|
| `segment_id` | `uuid` | PK. |
| `table_oid` | `oid` | Managed table. |
| `scope_key` | `text` | Nullable. |
| `object_path` | `text` | Final Parquet object path. |
| `batch_number` | `int` | Flush sequence. |
| `min_seq` / `max_seq` | `bigint` | Segment version bounds. |
| `min_commit_seq` / `max_commit_seq` | `bigint` | Segment commit bounds. |
| `row_count` | `bigint` | Segment rows. |
| `byte_size` | `bigint` | Object size. |
| `schema_version` | `int` | Segment schema. |
| `column_stats` | `jsonb` | PK, `_seq`, `_commit_seq`, indexed/immutable columns. |
| `status` | `text` | `pending`, `active`, `compacted`, `deleted`. |
| `manifest_etag` | `text` | Manifest identity that published this segment. |
| `created_xid` | `xid` | PostgreSQL catalog visibility aid. |
| `created_lsn` | `pg_lsn` | Recovery/debugging. |

KoldstoreMergeScan includes only active segment rows visible to the current snapshot.

## Cold PK Hints (`koldstore.cold_pk_hints`)

Local metadata used to avoid object-store reads on DML.

| Column | Type | Notes |
|--------|------|-------|
| `table_oid` | `oid` | Managed table. |
| `scope_key` | `text` | Nullable. |
| `pk_hash` | `bytea` | Hash of logical PK. |
| `segment_id` | `uuid` | Candidate segment. |
| `hint_kind` | `text` | `exact`, `bloom`, `range`. |
| `latest_seq` | `bigint` | Best known cold seq. |
| `latest_commit_seq` | `bigint` | Best known cold commit seq. |

Exact hints can preserve rowcount semantics for cold-only DELETE. Bloom/range hints are may-contain and can only drive idempotent explicit tombstone APIs.

## Row Events (`koldstore.row_events`)

Append-only event log for `changes_since`.

| Column | Type | Notes |
|--------|------|-------|
| `table_oid` | `oid` | Managed table. |
| `scope_key` | `text` | Nullable. |
| `pk_hash` | `bytea` | PK hash. |
| `pk_json` | `jsonb` | PK values. |
| `op` | `text` | `insert`, `update`, `delete`, `revive`. |
| `seq` | `bigint` | Row/effect id. |
| `commit_seq` | `bigint` | Commit-order cursor. |
| `deleted` | `boolean` | Delete/tombstone flag. |
| `row_image_json` | `jsonb` | Optional payload per retention/projection policy. |
| `txid` | `xid8` | Source transaction. |
| `created_at` | `timestamptz` | Event write time. |

## Object-Store Manifest (`manifest.json`)

Object-store source of truth for committed cold artifacts. Required segment metadata includes:

- path
- row count and byte size
- schema version
- `_seq` min/max
- `_commit_seq` min/max
- column stats
- bloom/filter metadata references
- checksum

The manifest is the visibility boundary for object-store files. Temp files and unmanifested final files are recovery garbage.

## Parquet Segment

| Property | Value |
|----------|-------|
| Naming | `batch-N.parquet` or compaction-specific name. |
| Publish | Backend-safe temp/final write; no portable atomic rename assumption. |
| Columns | PK, `_seq`, `_commit_seq`, `_deleted`, app columns for schema version. |
| Sort | `_seq` or PK/_seq depending on flush policy; metadata must record ordering. |
| Bloom | PK columns when supported and row count justifies it. |
| Stats | PK, `_seq`, `_commit_seq`, indexed/immutable columns. |

## Index Recommendations

| Relation | Index |
|----------|-------|
| app table | existing primary key preserved. |
| app table | optional hot filter indexes based on workload. |
| `koldstore.segments` | `(table_oid, scope_key, status)`. |
| `koldstore.segments` | `(table_oid, min_commit_seq, max_commit_seq)`. |
| `koldstore.cold_pk_hints` | `(table_oid, scope_key, pk_hash)`. |
| `koldstore.row_events` | `(table_oid, scope_key, commit_seq)`. |
| `koldstore.manifest` | `(table_oid, scope_key)`. |
