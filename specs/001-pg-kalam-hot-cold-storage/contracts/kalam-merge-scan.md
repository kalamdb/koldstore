# KalamMergeScan Contract

**Version**: 0.1.0 (planning)  
**Branch**: `001-pg-kalam-hot-cold-storage`

Behavioral contract for the PostgreSQL Custom Scan Provider `KalamMergeScan`.

---

## Planner Phase

### Hook

`set_rel_pathlist_hook` — registered in `_PG_init`.

### Detection

Relation is Kalam-managed when `pg_kalam` catalog marks `table_oid` as managed.

### Path Construction

1. PostgreSQL generates normal heap paths (Seq/Index/Bitmap)
2. pg-kalam selects **best hot path** as internal child
3. pg-kalam **removes** vanilla heap-only paths from `rel->pathlist`
4. pg-kalam adds `CustomPath` with:
   - `methods` → KalamMergeScan path methods
   - `custom_paths` → `[best_hot_path]`
   - `custom_private` → `{ table_oid, pk_cols, quals, scope_key, segment_hints, rls_quals }`
   - `pathtarget` → compatible with parent plan

### Cost Model (MVP)

Penalty hot-only paths to infinity (unreachable). Custom path cost = hot_child_cost + estimated_cold_bytes * factor. Refine post-MVP with segment stats.

---

## Plan Phase

`PlanCustomPath` → `CustomScan` node:

| Field | Content |
|-------|---------|
| `custom_plans` | `[hot_plan]` |
| `custom_exprs` | Expressions needing PG evaluation fixups |
| `custom_private` | Serialized merge metadata |
| `scanrelid` | Managed relation index |
| `custom_scan_tlist` | Output target list matching heap descriptor |

---

## Execution Phase

### BeginCustomScan

1. Initialize hot child plan (`ExecInitNode`)
2. Open query snapshot (`GetActiveSnapshot`)
3. Load managed table metadata from `system.schemas`
4. Query `pg_kalam.cold_segments` for segments visible to snapshot
5. Prune segments by query quals on `_seq` / segment bounds
6. Build DataFusion cold scan (projection + filter pushdown)
7. Initialize merge resolver state (PK → winner buffer)

### ExecCustomScan

Loop until output slot ready or exhausted:

1. Pull batch from hot child and/or cold DataFusion stream
2. For each row: extract PK, `_seq`, `_deleted`, payload columns
3. Merge resolver: per PK keep max `_seq`; apply tombstone rule
4. Apply RLS/security quals to cold rows before admitting to merge
5. Emit next visible row as `TupleTableSlot`

Returns `NULL` slot when complete.

### EndCustomScan

1. Shutdown hot child
2. Drop DataFusion resources
3. Free merge state in appropriate memory context

### ReScanCustomScan

Reset hot child + cold iterator + merge state for rescan (nested loop joins, CTEs).

---

## Merge Resolver Rules

### Default mode (`kalam.changelog = off`)

```
for each row r from hot ∪ cold:
  pk = extract_pk(r)
  if pk not in map or r._seq > map[pk]._seq:
    map[pk] = r

for output:
  for each pk in map ordered per plan:
    if map[pk]._deleted:
      skip   # hidden from logical application view
    if passes residual quals:
      emit map[pk]
```

### Change-feed mode (`kalam.changelog = on` or `changes_since`)

```
for each row r from hot ∪ cold where r._seq > watermark:
  if passes quals:
    emit r   # includes tombstones (_deleted=true) as delete events
order by _seq
```

No PK collapse in change-feed mode — every appended version is visible.

**Tombstone contract**: `DELETE` and `kalam.delete()` append a row with `_deleted = true` and a **new** `_seq`. Default reads hide it; change-feed reads expose it for realtime.

**Tie-break**: Hot wins if `_seq` equal (invariant: seq generator prevents duplicates per table).

---

## Pruning (kalamdb-compatible)

### Segment-level (manifest)

- Use `min_seq`/`max_seq` and per-column `column_stats` min/max from `manifest.json`
- Skip non-`committed` segments
- Pattern: kalamdb `manifest/planner.rs`

### Row-group-level (Parquet footer)

- Read footer/metadata only; do not scan full files for pruning decisions
- PK bloom filter pruning: `with_pk_bloom_values`
- `_seq` range pruning: row-group column statistics min/max
- Enable DataFusion: `with_parquet_bloom_filter_pruning(true)`, `with_parquet_page_index_pruning(true)`
- Pattern: kalamdb `parquet/reader.rs`, `datafusion_session.rs`

### DataFusion scope (minimal binary)

Include only: Parquet scan, projection, filter pushdown, bloom/page-index pruning. Exclude: SQL parser, full optimizer, aggregate/join execution inside DataFusion.

---

## RLS Contract

- Security quals attached during planning are stored in `custom_private`
- Cold path: translate qual to DataFusion `Expr` where possible
- Unsupported qual expressions: fetch extra columns and evaluate in PostgreSQL `custom_exprs`
- **Fail closed**: if cold qual cannot be applied safely → error at plan time (not silent over-read)

---

## ORDER BY / LIMIT (MVP)

| Pattern | Behavior |
|---------|----------|
| No ORDER BY/LIMIT | Stream merge; emit as resolved |
| ORDER BY / LIMIT | Full merge into sort-capable state OR delegate sort to parent node after full scan |
| Unsafe pushdown | NOT permitted in MVP |

Document in EXPLAIN verbose when full merge required for correctness.

---

## MVCC Segment Visibility

Include cold segment when ALL hold:

- `status = 'active'`
- Segment committed transaction visible to `GetActiveSnapshot()`
- Not superseded by deleting segment record in snapshot

Exclude `pending` segments (in-flight flush temp files).

---

## Supported Query Patterns (MVP)

| Pattern | Support |
|---------|---------|
| `SELECT *` / column list | Yes |
| `WHERE pk = ?` | Yes (hot index + cold filter) |
| `WHERE scope_col = ?` | Yes (user-scoped) |
| `WHERE _seq` range | Yes (segment pruning) |
| `WHERE created_at` range | Yes if column mapped to seq bounds |
| `ORDER BY created_at DESC LIMIT N` | Yes (correctness via full merge + sort) |
| Join above scan | Yes (KalamMergeScan as child; join with Kalam + PostgreSQL tables) |
| Aggregate pushdown into cold | No (MVP) |
| Parallel custom scan | No (MVP) |
| Serializable cross-store guarantees | No (MVP) |

---

## Error Conditions

| Condition | Behavior |
|-----------|----------|
| Object store unreachable | ERROR on cold read (fail closed per FR-022b) |
| Schema version mismatch | Coerce with NULL defaults per data model |
| Corrupt Parquet | ERROR with segment path in detail |
| `kalam.enable_merge_scan = off` | ERROR: managed table requires KalamMergeScan |

---

## Internal Child Path Contract

Hot child path is **not** user-selectable. It exists only inside `CustomScan.custom_plans`. PostgreSQL may choose Index Scan vs Seq Scan for hot portion based on quals.

---

## Licensing Note

Implementation must be original code using PostgreSQL Custom Scan API. Do not copy Citus (AGPL) source.
