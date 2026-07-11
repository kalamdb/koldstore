# Contract: Segment Lifecycle

**Feature**: `003-column-id-lifecycle`

## States (cold files in `koldstore.segments`)

`staged` | `published` | `superseded` | `deleting` | `deleted` | `orphaned`

| State | Meaning | Query visible? |
|-------|---------|----------------|
| `staged` | Temp cold object written and validated; not yet in published snapshot | No |
| `published` | Manifest/snapshot commit succeeded (completed) | Yes |
| `superseded` | Replaced (e.g. compaction) | No |
| `deleting` | Retention passed; delete in progress | No |
| `deleted` | Object removed (or delete acknowledged) | No |
| `orphaned` | No valid catalog/manifest owner / crash leftover | No |

## Pending reservations (`koldstore.pending`)

Approximate flush reservations live in a separate table, not as segment statuses:

| Column | Meaning |
|--------|---------|
| `(table_oid, scope_key)` PK | One open reservation per live scope |
| `row_count` | Approximate rows from in-memory / reconciled counters |
| `schema_version` | Active schema at last upsert |
| `updated_at` | Last counter sync |

## Counter → Pre-Flush → Flush Workflow

Normative initiation path (User scoped and Shared unscoped share one mechanism):

```text
DML in PostgreSQL
  → mirror the row
  → increment in-memory map key: (table_id, Optional<scope_value>)
     (Shared tables use Optional::None; User tables use scope column value)
  → do NOT create/update koldstore.pending or segment rows on each insert

Operator initiates flush for a table (manual flush in this phase)
  → pre-flush gathers in-memory counter keys for that table_id
  → upsert koldstore.pending for every non-zero key (create or update row_count)

Flush job
  → load koldstore.pending for the table
  → select scopes with row_count > hot_row_limit (or all non-zero when force / no policy)
  → write Parquet/object storage (filter by scope_value when present)
       → validate checksum + footer metadata
       → insert koldstore.segments with status = published (via staged write path as needed)
       → prune hot/mirror rows
       → DELETE flushable scopes from koldstore.pending
```

Crash before or during publish: resume job; publish exactly once or leave
`staged`/`orphaned` under lease rules — recoverable without losing hot rows or
creating duplicate query-visible cold rows. Pending reservations remain until
a successful flush clears them.

Multiple scopes for the same table MAY be pending or flushing concurrently.

## Flush Publish Path (per flushable pending scope)

```text
koldstore.pending row (pre-flush upserted)
  -> write temp object
  -> validate checksum + footer metadata
  -> status = staged (cold file only)
  -> manifest/snapshot publish
  -> status = published
  -> prune hot/mirror rows for that scope/range
  -> DELETE from koldstore.pending for flushed scopes
```

## Compaction / Supersede

```text
new segment reaches published
  -> old segment status = superseded
  -> after retention: deleting -> deleted
```

## Orphan Reconciliation

Objects or catalog rows without a valid manifest reference and without an active owning job lease → `orphaned` → durable cleanup job.

In-memory counters are advisory for thresholding; after process restart,
pre-flush/flush MUST reconcile from durable mirror/hot state.

## Visibility

Merge scan candidate sets include only `published` segments for the current manifest generation.

## Hard Cutover

Replace SQL `CHECK (status IN ('pending','active','compacted','deleted'))` (old meanings) and Rust status variants with the cold-file lifecycle set above. Pending is not a segment status. Update all writers/readers/tests in the same change; do not accept legacy status strings (`active`, `compacted` as old semantics).
