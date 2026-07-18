# Roadmap

KoldStore 0.1 focuses on reliable hot/cold table management, sequence-ordered
flushes, and correct `KoldMergeScan` reads. The following features are deferred
until after that baseline is stable.

## Near-term product surface

- **Improve `KoldMergeScan`** — prioritize cold PK point-lookup latency
  (backend footer/reader cache, cold-native emit without JSON merge), then
  remaining streaming polish, bounded-memory execution, rescans, and broader
  planner pushdown. See [performance](performance.md).
- **Finish change-log APIs** — public `changes_since` / change-cursor SQL on
  top of the latest-state `__cl` mirror (see below).
- **Storage file datatype** — upload and fetch files directly from registered
  cold storage backends.
- **Import / export** — table-level archive import and export of managed data.
- **Backup / restore** — coordinated PostgreSQL + cold-storage backup and
  restore workflows.

Built-in row-limit auto-flush scheduling is available on the database worker
(`koldstore.flush_check_interval_seconds`, per-table `auto_flush`). Time-based
`max_flush_interval` and predicate move policies remain deferred. See
[operations/scheduling.md](operations/scheduling.md).

## Change cursors (`changes_since`)

Managing a table already creates a **latest-state change-log mirror**
(`koldstore.<table>__cl`): one row per primary key with a monotonic `seq` and
`op` (`INSERT` / `UPDATE` / `DELETE`). Capture triggers are installed at
`manage_table` so flush can cut by `seq` and scans know which keys are still
hot. The mirror is not an append-only history of every intermediate update (a
later `UPDATE` overwrites the previous mirror row for that PK).

That mirror is the foundation for incremental sync / catch-up consumers without
a separate CDC stack. Planned SQL surface:

```sql
SELECT *
FROM koldstore.changes_since(
  table_name => 'app.messages',
  since_seq  => 332882280164896768,
  limit_rows => 1000
);
```

That returns the latest state per primary key with `seq > since_seq` (including
deletes), ordered by `seq`. The merge library already implements the cursor
logic; the public SQL function is not exposed yet.

Until then you can inspect the hot mirror directly for keys still in the hot
working set:

```sql
SELECT id, seq, op
FROM koldstore.messages__cl
WHERE seq > 332882280164896768
ORDER BY seq
LIMIT 1000;
```

Note: today’s `__cl` mirror is **latest-state**, not an append-only WAL.
`changes_since` targets “catch me up to current state since this cursor,” not
full temporal audit replay. Cold-flushed keys are represented through
flush/manifest metadata; the public cursor API will document how hot + cold
changes are unified.

## Storage layout and pruning

- **Footer-derived catalog segment stats** — stop hand-maintaining
  `indexed_bounds` during encode; after Parquet write, extract min/max from
  footer chunk statistics into `column_stats` (catalog still owns
  prune-before-open). Accepted in
  [ADR-002](decisions/002-footer-derived-catalog-stats.md); schedule after
  cold PK scan wins. `byte_size` already comes from publish metadata only.
- Operator-configurable `pruning_columns` and `bloom_filter_columns`.
- Segment compaction and small-file combining.
- Size-aware segment writing based on `target_file_size_mb`.
- Configurable `flush_order_by`; flush selection is always ordered by mirror
  `seq` today.

## Table management and flush policy

- `koldstore.alter_table` for changing managed-table settings after
  registration.
- Time- or age-based flush triggers such as `max_flush_interval`.
- Background scheduling, richer retry controls, and operational policy tuning.

## Query execution

- User-scoped cold-segment loading and parallel custom-scan execution.
- Additional predicate, projection, and ordering pushdown.

## Other post-0.1 work

- Segment lifecycle tooling, validation, and repair automation.
- Explicit cold-row DML APIs (`hydrate_pk`, `update_row`, `delete_row`).
- Broader schema evolution and primary-key change support.
- Production hardening, observability, and performance tuning.
