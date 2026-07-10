# KoldMergeScan Redesign

Date: 2026-07-10

## Purpose

Redesign `KoldMergeScan` so managed-table reads remain PostgreSQL-compatible
while cold data is read incrementally with bounded memory. The work also
clarifies the SQL and configuration surface needed for the 0.1 release without
pulling deferred storage-policy features into the initial implementation.

## Architecture decisions

### Keep PostgreSQL as the hot-row authority

The application table remains a normal PostgreSQL heap. PostgreSQL owns
transactions, locking, visibility, RLS, and hot-row DML. KoldStore owns cold
Parquet segments, manifest metadata, and winner resolution across hot and cold
versions.

### Stream scan execution

Move cold-segment reading and hot/cold merge work out of bulk
`BeginCustomScan` materialization. `BeginCustomScan` prepares immutable scan
metadata and readers; `ExecCustomScan` advances the merge and emits rows
incrementally. Scan-owned memory must remain bounded and be reset as rows and
segment batches are consumed.

The executor state should use focused Rust domain types for scan identity,
segment cursors, primary keys, projected columns, and row ownership. PostgreSQL
Datum conversion stays at the extension boundary.

### Preserve deterministic winner semantics

For duplicate primary keys, the visible winner is selected by:

1. highest sequence,
2. highest commit sequence,
3. hot row on an exact tie.

Tombstones and mirror state must prevent stale cold versions from becoming
visible. Rescan must reproduce the same results without leaking stale iterator
or memory-context state.

### Push down only proven-safe work

Use catalog segment statistics, Parquet row-group statistics, bloom filters,
and projection to avoid unnecessary object reads and decoding. PostgreSQL keeps
responsibility for residual qualification unless a predicate has an explicitly
supported, semantics-preserving pushdown implementation. Unsupported security
or visibility conditions fail closed rather than returning partial results.

### Keep catalog and object-store boundaries explicit

Catalog snapshots and manifests define the active cold segment set. Reader
limits are enforced per backend, object-store failures are surfaced, and scans
never silently fall back to incomplete hot-only results when cold rows are
required. Cache invalidation follows schema, manifest, migration, and flush
publication changes.

## SQL API decisions for this phase

- Rename the manage-time backfill hint from `order_column` to
  `migration_order_by`. Persist new configuration under that name while
  accepting legacy `order_column` JSON during catalog decoding.
- Add optional `target_file_size_mb` to `manage_table` and
  `ManageTableOptions`. It is a configuration hint until size-aware writing is
  implemented.
- Add `force boolean DEFAULT false` directly to `flush_table`.
- Keep flush ordering fixed to mirror `seq`.
- Defer `pruning_columns`, `bloom_filter_columns`,
  `koldstore.alter_table`, `flush_order_by`, compaction, and age-based flush
  triggers.

## Delivery sequence

1. Stabilize typed scan/catalog boundaries and preserve existing correctness
   tests.
2. Introduce iterator-based cold reads and bounded scan state.
3. Integrate hot rows, tombstones, and deterministic winner resolution.
4. Add safe pruning/projection pushdown and detailed `EXPLAIN` diagnostics.
5. Validate rescans, errors, RLS behavior, object-store limits, and memory use.
6. Benchmark hot-only, point lookup, selective mixed, and full mixed scans
   before making the streaming path the default.

## Verification

Use local pgrx-managed PostgreSQL for correctness and integration tests.
Required coverage includes hot-only and mixed scans, duplicate winners,
tombstones, NULL values, projection, filters, rescans, user scope, unavailable
cold storage, malformed metadata, and bounded-memory scans over multiple
segments. Docker remains a final packaging smoke test only.

Deferred work is tracked in [the project roadmap](../roadmap.md).
