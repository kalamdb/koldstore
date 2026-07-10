# KoldstoreMergeScan Contract

**Version**: 0.3.0 (planning)
**Branch**: `001-pg-koldstore-hot-cold-storage`

`KoldstoreMergeScan` is the SELECT/read architecture for managed tables. It is still the correct PostgreSQL extension point because managed tables are logical relations made from a hot heap overlay plus cold Parquet segments.

It is **not** a table access method and it does **not** make cold rows directly updateable.

## Planner Phase

### Hook

Use `set_rel_pathlist_hook` registered from `_PG_init`.

### Detection

A relation is managed when koldstore catalog metadata marks its `table_oid` active.

### Path Construction

1. Let PostgreSQL build normal hot heap paths.
2. Choose the best hot child path (Index/Bitmap/Seq).
3. Replace user-visible heap-only paths with one `CustomPath`.
4. Store in `custom_private`:
   - table oid
   - logical PK columns
   - scope key
   - system column attnums
   - safe pruning quals
   - residual quals
   - visible cold segment hints
   - RLS/security quals

Hot child paths remain inside `custom_paths` only. They are not selectable as final paths for a managed read because they would omit cold rows.

## Predicate Safety

KoldstoreMergeScan MUST classify quals before cold pruning or pushdown.

| Qual class | Can prune before merge? | Reason |
|------------|-------------------------|--------|
| PK equality / PK IN | yes | Identifies candidate keys. |
| Scope column equality from `koldstore.user_id` | yes | Security and path partitioning. |
| `seq` ranges | yes | Version metadata exists in segment and row-group stats. |
| Immutable partition columns recorded as cold stats | maybe | Only when schema marks the column immutable after insert. |
| Mutable app columns | no, residual only | Filtering cold rows before winner selection can hide an older row while a newer hot/cold winner fails the predicate. |
| RLS quals | must be enforced | Translate only safe quals; otherwise evaluate in PostgreSQL after fetching required columns or fail closed for security. |

Default rule: if a qual is not proven safe for pruning, it is a residual PostgreSQL expression evaluated after hot/cold winner resolution.

### Mirror overlay

Unflushed mirror rows participate immediately:

- `op` 1/2 → skip cold for that PK; hot child returns the live row
- `op` 3 → skip cold for that PK; row is invisible
- no mirror row → cold version may be visible

Committed deletes must never require a later flush to become invisible.

### Sequence vs commit cursor

`seq` is a row-version / effect identity. It is not a commit-order cursor.
Change-feed APIs must not claim gap-free commit ordering from `seq` alone.

## Plan Phase

`PlanCustomPath` builds a `CustomScan`:

| Field | Content |
|-------|---------|
| `custom_plans` | hot child plan |
| `custom_exprs` | residual expressions and security expressions requiring PostgreSQL evaluation |
| `custom_private` | serialized table, scope, segment, projection, and pruning metadata |
| `scanrelid` | managed relation index |
| `custom_scan_tlist` | heap-compatible output target list |

## Execution Phase

### BeginCustomScan

1. Initialize hot child plan.
2. Capture active PostgreSQL snapshot.
3. Load managed table metadata from `system.schemas`.
4. Load visible `koldstore.cold_segments` rows for the snapshot.
5. Prune segments with safe PK/scope/_seq/_commit_seq predicates.
6. Open cold streams through `koldstore-parquet` direct Arrow/Parquet reader.
7. Initialize merge resolver state from `koldstore-merge`.

### ExecCustomScan

1. Pull rows from the hot child and selected cold streams.
2. Convert rows into a shared row representation.
3. Resolve one winner per PK:
   - compare `_seq`
   - use `_commit_seq` as the committed ordering/tie-break metadata
   - hot wins exact ties, which should only happen during recovery overlap
4. Apply tombstone masking.
5. Apply residual quals and security quals through PostgreSQL expression evaluation.
6. Emit `TupleTableSlot` rows to the parent plan.

### End / Rescan

- Shut down hot child.
- Drop cold streams and object-store handles.
- Reset merge state on rescan.
- Keep memory allocations in the scan memory context.

## Merge Rules

Default application read:

```text
for each candidate row from hot and cold:
  pk = extract_pk(row)
  if no winner for pk or row._seq is newer:
    winner[pk] = row

for each winner:
  if winner._deleted:
    skip
  if residual quals pass:
    emit
```

Important: hot heap has at most one row per PK, but cold may contain older versions and compacted segments.

Change-feed read:

```text
for each event in koldstore.row_events
where event.commit_seq > watermark:
  emit event ordered by commit_seq
```

`changes_since` does not reconstruct history by scanning duplicate hot rows because duplicate hot rows are forbidden.

## Cold Reader

MVP cold reads use `koldstore-parquet`, not full DataFusion.

Required capabilities:

- Object-store backed async Parquet stream.
- Projection by needed columns.
- Segment pruning from manifest / `koldstore.cold_segments`.
- Row-group pruning by PK bloom filters and `_seq` / `_commit_seq` stats.
- Footer and metadata reads before column chunk reads.

This mirrors kalamdb's direct reader pattern in `kalamdb-filestore/src/parquet/reader.rs` without embedding DataFusion's SQL planner or physical execution engine.

## MVCC Segment Visibility

Include a cold segment only when:

- `koldstore.cold_segments.status = 'active'`
- the segment catalog row is visible to the PostgreSQL snapshot
- the manifest generation recorded for the segment is the current committed generation for that scope
- the segment is not superseded by a visible compaction/deletion row

Object storage is not MVCC-aware; PostgreSQL catalog visibility is the authority for which segment paths a query may read.

## RLS Contract

- User-scoped tables require `koldstore.user_id` before planning.
- Scope pruning is applied before opening cold objects.
- Security quals that cannot be safely translated must be evaluated by PostgreSQL against fetched columns.
- If a security qual cannot be enforced, planning fails closed.

## ORDER BY / LIMIT

MVP prioritizes correctness:

| Pattern | Behavior |
|---------|----------|
| No ORDER BY/LIMIT | Merge all candidates needed by residual quals. |
| PK equality | Hot index + cold PK pruning; only candidate segments opened. |
| ORDER BY/LIMIT | Parent PostgreSQL nodes sort/limit after complete logical merge unless a safe top-K proof exists. |
| Aggregate pushdown | Not supported in MVP. |
| Parallel custom scan | Not supported in MVP. |

## Error Conditions

| Condition | Behavior |
|-----------|----------|
| Object store unreachable and cold segments required | ERROR; no partial hot-only results. |
| Corrupt Parquet | ERROR with segment path in detail. |
| Unsupported type in cold segment | ERROR unless schema registry supplies a safe coercion. |
| `koldstore.enable_merge_scan = off` | ERROR on managed table read. |

## References

- PostgreSQL Custom Scan paths/plans/execution: https://www.postgresql.org/docs/current/custom-scan.html
- kalamdb Parquet reader pattern: `../kalamdb/backend/crates/kalamdb-filestore/src/parquet/reader.rs`
- kalamdb manifest query path: `../kalamdb/docs/architecture/manifest.md`
