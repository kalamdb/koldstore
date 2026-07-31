# Roadmap

KoldStore 0.1 focuses on reliable hot/cold table management, sequence-ordered
flushes, and correct `KoldMergeScan` reads. The following features are deferred
until after that baseline is stable.

## Priority near-term

1. **Scoped storage** — place each `scope_column` value in its own cold folder
2. **Change API** — `changes_since` / change-cursor SQL for real-time catch-up
3. **Compaction** — combine small cold segments to reduce object count and scan cost
4. **Backup / export** — KoldStore-aware dump, restore, and table/scope archives

## Scoped storage

Today cold segments use a **table-wide** layout (no per-scope object prefixes):

```text
{namespace}/{table}/{folder:03}/segment-….parquet
```

User-scoped tables already enforce scope in hot DML/reads via RLS and
`koldstore.user_id`, but flushed objects are not yet partitioned by scope value.

**Goal:** for `table_type => 'user'`, write and read cold data under one folder
per scope value:

```text
{namespace}/{table}/{scopeId}/{folder:03}/segment-….parquet
```

Why this matters:

- Physically separates tenant/user data in object storage
- Makes per-user prune, backup, export, and deletion straightforward
- Lets merge scan open only the folders for the active session scope

Registration already accepts a `scoped_path_tmpl` default of
`{namespace}/{tableName}/{scopeId}/`; flush, manifest, and scan need to honor
it end-to-end.

## Change API (`changes_since`)

Managing a table creates a **latest-state change-log mirror**
(`koldstore.<table>__cl`): one row per primary key with a monotonic `seq` and
`op` (`INSERT` / `UPDATE` / `DELETE`). Committed WAL is applied by the async
mirror worker so flush can cut by `seq` and scans know which keys are still
hot. The mirror is not an append-only history of every intermediate update (a
later `UPDATE` overwrites the previous mirror row for that PK).

Shipped SQL surface (KalamDB subscribe-compatible):

```sql
-- Resume (from / from_seq_id)
SELECT seq, op, pk, deleted, row_image, source
FROM koldstore.changes_since(
  table_name => 'app.messages',
  since_seq  => 332882280164896768,
  limit_rows => 1000
);

-- Newest-N rewind (last_rows); delivered oldest→newest
SELECT seq, op, pk, deleted, source
FROM koldstore.changes_since(
  table_name => 'app.messages',
  since_seq  => 0,
  limit_rows => 1000,
  last_rows  => 100
);
```

Precedence matches KalamDB: when `since_seq > 0`, resume mode wins and
`last_rows` is ignored. Otherwise `last_rows` rewinds to the newest N retained
changes. `since_seq = 0` with no `last_rows` means from the start of retained
history. A positive cursor older than the retained floor raises a retention-gap
error.

Note: today’s feed is **latest-state**, not an append-only WAL.
`changes_since` targets “catch me up to current state since this cursor,” not
full temporal audit replay.

## Compaction

Frequent flushes can leave many small Parquet segments. Compaction merges those
into fewer, larger files under the same table (and scope) prefix so cold scans
open less objects and object-store LIST/GET overhead drops.

Planned shape:

- Background or on-demand compact jobs that rewrite small segments
- Manifest/CAS publication shared with flush finalize (see
  [ADR-004](decisions/004-segment-publication-protocol.md))
- Preserve correctness for concurrent scans and `changes_since` cursors
- Prefer compacting within a single scope folder once scoped storage lands

Size-aware writing via `target_file_size_mb` reduces how often compaction is
needed; compaction remains the safety net for already-written small files.

## Backup / export

Hot rows live in PostgreSQL; cold segments live in object storage. Plain
`pg_dump` / base backup alone cannot recover a managed table. Backup and export
must be **KoldStore-aware** and keep both tiers consistent.

**Goal:**

- Coordinated backup/restore of PostgreSQL catalog + cold prefixes with a
  matching manifest generation (see [backup-and-operations.md](backup-and-operations.md))
- Table-level (and later scope-level) **export** of managed hot+cold data into a
  portable archive (manifest + Parquet)
- Matching **import** to rehydrate a managed table with ownership, conflict, and
  schema-compatibility rules defined end to end
- Scoped storage should make per-tenant backup/export a natural subset once
  cold folders are per `scopeId`

Today: `koldstore.backup_manifest` and validation helpers exist;
`EXPORT TABLE` is the intended archive boundary; `IMPORT TABLE` is still
rejected until those rules land.

## Other near-term product surface

- **Improve `KoldMergeScan`** — prioritize cold PK point-lookup latency
  (backend footer/reader cache, cold-native emit without JSON merge), then
  spillable exact winner state and broader planner pushdown. Cold payloads
  stream by non-overlapping sequence-range groups, and mixed-scan hot JSON is
  paged in SPI batches instead of being retained for the full scan. The exact
  PK seen-set remains in RAM (compact, payload-free) until spill lands. See
  [performance](performance.md).
- **Storage file datatype** — upload and fetch files directly from registered
  cold storage backends.

Built-in row-limit auto-flush scheduling is available on the database worker
(`koldstore.flush_check_interval_seconds`, per-table `auto_flush`). Time-based
`max_flush_interval` and predicate move policies remain deferred. See
[operations/scheduling.md](operations/scheduling.md).

## Storage layout and pruning

- **Footer-derived packed catalog stats** — implemented. Finalized Parquet
  metadata supplies Sort Key V1 segment bounds and row-group arrays; scalar
  SQL candidate lookup is refined in Rust before opening an object. See
  [ADR-002](decisions/002-footer-derived-catalog-stats.md).
- Operator-configurable `pruning_columns` and `bloom_filter_columns`.
- Configurable `flush_order_by`; flush selection is always ordered by mirror
  `seq` today.

## Table management and flush policy

- `koldstore.alter_table` for changing managed-table settings after
  registration.
- Time- or age-based flush triggers such as `max_flush_interval`.
- Background scheduling, richer retry controls, and operational policy tuning.

## Query execution

- Parallel custom-scan execution once scoped cold folders land.
- Additional predicate, projection, and ordering pushdown.

## Other post-0.1 work

- Segment lifecycle tooling, validation, and repair automation.
- Explicit cold-row DML APIs (`hydrate_pk`, `update_row`, `delete_row`).
- Broader schema evolution and primary-key change support.
- Production hardening, observability, and performance tuning.
