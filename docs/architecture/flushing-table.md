# Flush Table Workflow

This document describes the synchronous `koldstore.flush_table` path: how mirror
rows become Parquet segments, how the catalog and manifest are updated, and how
hot/mirror rows are pruned after a successful write.

**SQL entrypoint:** `koldstore.flush_table(table_name regclass) → uuid`  
**Orchestrator:** `crates/pg_koldstore/src/sql/flush/execute.rs`  
**PG-free logic:** `crates/koldstore-flush/`, `crates/koldstore-parquet/`

Related: `koldstore.enqueue_flush_job(force => true)` sets the force flag on a
pending job; `flush_table` then runs it inline.

All flushes are **table-wide** (`scope_key = ''` in catalog).

---

## Overview

```mermaid
flowchart TD
  A["flush_table_pg"] --> B["lock_table_job"]
  B --> C["ensure_flush_job + mark_running"]
  C --> D["prepare_flush_context"]
  D --> E["refresh_active_schema_if_changed?"]
  E --> F["resolve_flush_stats"]
  F --> G{row_count == 0?}
  G -->|yes| H["mark_completed(0)"]
  G -->|no| I["stream_write_flush_batches"]
  I --> J["stream_flush_chunks\nSPI fetch → Arrow → Parquet"]
  J --> K["persist_flush_segments_batch"]
  K --> L["prune_flushed_hot_rows\nseq-range DELETE"]
  L --> M["apply_flush_row_count_deltas"]
  M --> N["manifest reconcile if needed"]
  N --> O["finalize_flush\nmanifest.json + catalog"]
```

Internal mirror/hot SQL runs under `with_custom_scan_disabled` so the flush path
does not recurse into `KoldMergeScan`.

---

## Phase 1 — Job lock and context

### 1.1 Advisory lock

Same transaction lock as manage: `lock_table_job(table_oid)`.

### 1.2 Inline flush job

`jobs.rs` manages one inline job per flush call:

| Step | Planner (`koldstore-flush/table_jobs.rs`) | Effect |
|------|------------------------------------------|--------|
| Lookup | `plan_lookup_active_inline_flush_job` | Reuse pending/running job |
| Insert | `plan_insert_inline_flush_job` | New UUID, `force: false` in payload |
| Running | `plan_mark_inline_flush_job_running` | |
| Completed / failed | `plan_mark_*` | Always returns job UUID to caller |

**Job lookup serde:** SQL returns `jsonb_build_object('id', …, 'force', …)::text`
→ `PendingFlushJobWire { id, force }` via `serde_json`.

The `force` flag comes from an existing job payload (e.g. prior
`enqueue_flush_job(force=true)`). `flush_table` itself has no `force` parameter.

### 1.3 Prepared context

`prepare_flush_context` resolves:

| Field | Source |
|-------|--------|
| `RelationContext` | namespace, table name |
| `FlushStorageContext` | `base_path`, `compression`, `schema_version` |
| `ManagedTableSnapshot` | mirror relation, PK columns |
| Catalog columns | `migration_catalog` |
| `indexed_columns` | PK ∪ catalog indexed columns (deduped) |
| `max_rows_per_file` | active flush policy + GUC floor |

### 1.4 Schema evolution gate

`refresh_active_schema_if_changed(table_oid)` — if the active schema version
changed, context is rebuilt. On error the job is marked failed and the UUID is
still returned.

---

## Phase 2 — Stats resolution (what to flush)

`resolve_flush_stats` (`spi.rs`) returns
`ResolvedFlushSelection { stats: FlushStats, mirror_ops: Option<Vec<i16>> }`.

### Path A — Force flush (`force = true`)

1. `mirror_flush_stats` — full mirror `COUNT(*)` + seq bounds
2. If delete-only rows ≤ 4096 (`FORCE_TOMBSTONE_ONLY_CAP`):
   - `mirror_op_stats(op=3)` + `mirror_ops: Some([3])` (tombstone-only fetch/cleanup)
3. Else flush entire mirror

### Path B — Policy flush (normal)

1. **`mirror_pending_row_count`** — O(1) read from `koldstore.manifest.mirror_row_count`
   (falls back to `mirror_flush_stats` if manifest missing)

2. Load **`FlushPolicy`** from `koldstore.schemas.options`:
   - SPI → `pgrx::JsonB` → `FlushPolicy::from_value`

3. **`policy_flush_row_count(pending, policy)`** — pure math:
   - If `pending ≤ hot_row_limit` → 0
   - Else flush `excess` in `min_flush_rows` chunks (with half-chunk partial rule)

4. **`mirror_oldest_rows_cutoff(table_oid, flush_count)`**:
   - `ORDER BY seq ASC LIMIT 1 OFFSET (N-1)` → `max_seq` cutoff
   - Returns `(selected_count, max_seq)`
   - Fallback if counters overshoot: live `mirror_flush_stats` + capped cutoff

Policy-path `FlushStats` uses `min_seq = 0`; `commit_seq` equals mirror `seq`.

### Mirror stats serde (fallback / force paths)

```sql
SELECT jsonb_build_object(
  'row_count', count(*),
  'min_seq', COALESCE(min("seq"), 0),
  'max_seq', COALESCE(max("seq"), 0),
  ...
)::text
```

Rust: `serde_json::from_str` → `MirrorSeqStats` → `FlushStats`.

---

## Phase 3 — Early exit

If `selection.stats.row_count == 0`:

- `mark_flush_job_completed(0, 0, 0)`
- No Parquet, no cleanup, no manifest file write

---

## Phase 4 — Streaming encode and segment write

`stream_write_flush_batches` (`execute.rs`).

### 4.1 Setup

- Manifest paths: `{base_path}/{namespace}/{table}/manifest.json`
- Load existing manifest from disk: `serde_json::from_str<Manifest>` or new shared manifest
- `next_flush_batch_number` from `koldstore.cold_segments`
- Create table prefix directories once
- Build `StreamEncodeInput` (columns, Parquet schema, `max_seq`, optional `mirror_ops`)

### 4.2 Mirror fetch (SPI → typed rows)

**SQL planner:** `plan_mirror_flush_selection_batch` (`koldstore-flush/ops.rs`)

```sql
SELECT <app cols from hot/mirror join>,
       mirror."seq", mirror."op", (mirror."op" = 3) AS deleted
FROM koldstore.{table}__cl AS mirror
LEFT JOIN ONLY {schema}.{table} AS hot ON <pk join>
WHERE mirror."seq" <= $1          -- max_seq cutoff
  AND mirror."seq" > $2           -- keyset lower bound
  [AND mirror."op" = 3]           -- optional force tombstone filter
ORDER BY mirror."seq" ASC
LIMIT $3                          -- 8192 rows per SPI round trip
```

**Fetcher:** `mirror_fetch.rs::fetch_mirror_batch`

**SPI decode → `FlushMirrorRow`** (ordinal access, no per-column name lookup):

| PG type | `FlushColumnValue` |
|---------|-------------------|
| bool | `Bool` |
| int2/4/8 | `Int16` / `Int32` / `Int64` |
| float4/8 | `Float32` / `Float64` |
| text, numeric, bytea, text[] | `Utf8(String)` |
| uuid | `Utf8(uuid string)` |
| jsonb | `Utf8` (string or `serde_json::to_string`) |
| timestamptz | `TimestamptzMicros` (PG epoch µs + Unix offset; no string parse) |

Column layout: ordinals `1..N` = catalog columns, `N+1` = `seq`, `N+2` = `op`.

Non-PK column values for live rows come from the hot heap join. Delete mirror
rows (`op = 3`) carry PK values from mirror only.

### 4.3 Arrow encode

`stream_flush_chunks` (`koldstore-flush/encode.rs`):

1. Fetch page of up to 8192 rows (`FLUSH_MIRROR_FETCH_BATCH_SIZE`)
2. `CleanColdRecordBatchBuilder::push_typed_row` per row
   - App columns + metadata: `seq`, `op`, `deleted`, `schema_version`
   - Tracks `indexed_bounds` as `serde_json::Value` min/max per indexed column
3. When chunk reaches `max_rows_per_file` → `FlushWriteChunk`
4. Callback writes Parquet segment

**No per-row cleanup JSON** is built in the encode loop. `cleanup_row_json` in
`batch_builder.rs` exists for tests/legacy only.

### 4.4 Parquet write

`write_flush_segment_file` (`segment_write.rs`):

1. Path: `{namespace}/{table}/batch-{n}.parquet`
2. `write_parquet_segment_file` — Arrow `RecordBatch` → native Parquet
   - Column statistics on `seq` + indexed columns
   - Bloom filters on PK columns
   - Compression from storage context (default `zstd`)
3. `column_stats` JSON for catalog:
   ```json
   { "seq": {"min": N, "max": M}, "created_at": {"min": "...", "max": "..."} }
   ```
4. In-memory `manifest.append_segment(...)`
5. Collect `WrittenFlushSegment` (new `segment_id = Uuid::new_v4()`)

### 4.5 Validation

`validate_flush_row_selection(stats.row_count, rows_written)` — counts must match.

---

## Phase 5 — Catalog batch insert

`persist_flush_segments_batch` — **one SPI round trip** for all segments:

- Native PostgreSQL arrays: `uuid[]`, `text[]`, `integer[]`, `bigint[]`, `jsonb[]`
- `INSERT … SELECT FROM unnest(…)` into `koldstore.cold_segments`
- CTE inserts matching `koldstore.cold_pk_hints` (`pk_hash = md5(object_path)`)

`column_stats` crosses SPI as `pgrx::JsonB` per segment (already
`serde_json::Value` in Rust).

---

## Phase 6 — Seq-range cleanup

`prune_flushed_hot_rows` (`spi.rs`) — **production path uses seq-range DELETE,
not JSON cleanup**.

`plan_seq_range_cleanup` (`cleanup.rs`):

```sql
WITH removed_mirror AS (
  DELETE FROM koldstore.{table}__cl AS mirror
  WHERE mirror."seq" <= $1 [AND mirror."op" = …]
  RETURNING <pk cols>, seq, op
),
deleted_hot AS (
  DELETE FROM ONLY {schema}.{table} AS hot
  USING removed_mirror
  WHERE removed_mirror."op" IN (1, 2)
    AND <pk join>
  RETURNING 1
)
SELECT count(removed_mirror), count(deleted_hot)
```

- Bind parameter: single `bigint max_seq`
- Runs under `SET LOCAL session_replication_role = replica`
- Mirror rows removed first; hot rows removed only for `op IN (1,2)` (insert/update)
- Delete tombstones (`op = 3`) stay in cold after flush; mirror copy is removed

`plan_clean_schema_cleanup` (JSON `jsonb_to_recordset`) remains for tests only.

---

## Phase 7 — Manifest counter deltas

`apply_flush_row_count_deltas` → `koldstore.internal_apply_flush_row_counts`:

```sql
UPDATE koldstore.manifest SET
  mirror_row_count = GREATEST(0, mirror_row_count - mirror_pruned),
  hot_row_count    = GREATEST(0, hot_row_count - hot_pruned),
  cold_row_count   = GREATEST(0, cold_row_count + cold_rows_added)
WHERE table_oid = $1 AND scope_key = ''
```

Four native `bigint` SPI parameters — no JSON.

---

## Phase 8 — Manifest reconciliation

If in-memory `manifest.segments.len() != active_cold_segment_count`:

- Rebuild from catalog: `plan_active_cold_segments_for_manifest_json`
- SQL → `jsonb_agg` text → `Vec<CatalogManifestSegmentRow>` → `Manifest`

Guards against drift between streamed manifest and catalog truth.

---

## Phase 9 — Finalize

| Step | Serde |
|------|-------|
| Write `manifest.json` | `serde_json::to_vec(&Manifest)` to object-store path |
| Upsert `koldstore.manifest` | native SPI: path, generation UUID, segment_count, max_seq |
| Complete job | native SPI bigints |
| Invalidate cache | `catalog::cache::invalidate_table` |

### `manifest.json` shape (`koldstore-manifest`)

`Manifest` and `ManifestSegment` are `Serialize`/`Deserialize`:

- `segments[]`: `path`, seq/commit ranges, `row_count`, `byte_size`, `schema_version`
- `column_stats`: `BTreeMap<String, {min, max: serde_json::Value}>`
- Watermarks: `max_seq`, `max_commit_seq`

After finalize, `sync_state` becomes `in_sync` (via upsert SQL).

---

## Serde boundary summary

| Boundary | Format |
|----------|--------|
| Job lookup | JSON text `{id, force}` |
| Flush policy | `JsonB` → `FlushPolicy` |
| Manifest counters | JSON text `{hot_row_count, mirror_row_count, …}` |
| Mirror stats (fallback) | JSON text → `MirrorSeqStats` |
| Mirror row fetch | SPI heap tuples → `FlushMirrorRow` (typed, no JSON) |
| Arrow / Parquet | `FlushColumnValue` → Arrow builders → binary Parquet |
| Segment catalog insert | native PG arrays + `jsonb[]` stats |
| Cleanup | single `bigint max_seq` |
| Counter deltas | 4× `bigint` |
| Manifest file | `serde_json` bytes |

---

## Key constants

| Constant | Value | Location |
|----------|-------|----------|
| Mirror fetch batch | 8192 | `FLUSH_MIRROR_FETCH_BATCH_SIZE` |
| Force tombstone cap | 4096 | `FORCE_TOMBSTONE_ONLY_CAP` |
| Scope | `scope_key = ''` | all flush SQL |

---

## Crate map

| Concern | Location |
|---------|----------|
| Orchestration | `pg_koldstore/src/sql/flush/execute.rs` |
| Stats, cleanup, catalog SPI | `pg_koldstore/src/sql/flush/spi.rs` |
| Mirror fetch/decode | `pg_koldstore/src/sql/flush/mirror_fetch.rs` |
| Encode loop | `koldstore-flush/src/encode.rs` |
| Mirror selection SQL | `koldstore-flush/src/ops.rs` |
| Seq-range cleanup | `koldstore-flush/src/cleanup.rs` |
| Parquet write | `koldstore-parquet/src/writer.rs`, `batch_builder.rs` |
| Manifest model | `koldstore-manifest/src/model/` |
