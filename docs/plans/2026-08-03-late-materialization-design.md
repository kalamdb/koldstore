# Late Materialization Design (Phase 6+)

**Status:** Design approved (2026-08-03)  
**Parent:** [progressive hot–cold query design](2026-08-03-progressive-hot-cold-query-design.md)  
**Plan tracker:** [progressive plan Phase 6+](2026-08-03-progressive-hot-cold-query.md#phase-6-later-track-only)

## Goal

For `OrderedProgressive` cold expand, load a **compete projection** first
(order key + primary key + forced cold metadata), resolve/sort/LIMIT against
hot, then open Parquet again for **body columns** only when cold winners will
be emitted.

**What counts:** cold-heavy `ORDER BY … LIMIT` must do less Parquet column I/O
(and ideally wall time) than today’s one-shot full projection. Profile counters
prove the skip; they are not the product.

## Non-goals

- Parquet page index / Bloom (separate Phase 6+ item)
- Per-emitted-row body fetch
- Changing `GeneralMerge` or `UnorderedHotFirst` cold projection
- Planner / `custom_scan_tlist` redesign beyond the ordered path already in tree

## Approach

**Compete-then-body re-read** on ordered expand only, reusing existing
`ParquetReadOptions::with_columns` and competitive row-group selection.

```text
maybe_enter_ordered_buffer
  ├─ apply_competitive_row_groups (unchanged)
  ├─ drain hot winners (unchanged)
  ├─ next_batch(Compete)
  │     cold_rows_from_segments(compete_columns)
  │     overlay + resolve_cold_batch
  │     buffer winners (compete-only row_image)
  ├─ sort buffer by leading key
  └─ hydrate_body(surviving RGs) only if a cold winner may emit
        merge body fields into row_image for emit
```

### Compete projection

- Leading order column (segment-order or PK leading attnum from the path)
- All primary-key columns
- Forced reader meta: `seq` / `op` / `deleted` / `schema_version`
- Residual cold prune/filter columns only if already required for correctness
  of the compete pass (prefer keeping compete small)

### Body projection

- Remaining scan projection columns not in compete
- Hydration: one body pass over competitive RGs that contribute buffered cold
  winners (not per-row in v1)
- If LIMIT is satisfied from hot after sort and no cold winner is emitted →
  **body opens = 0**

### Fail-open

- If compete cannot encode the leading key → single `Full` open (today’s path)
- If body set is empty or nearly equals full projection → single `Full` open
  (no double-read regression for narrow `SELECT id ORDER BY id`)

## Call sites

| Piece | Role |
| --- | --- |
| `ColdRowStream` | Keep full `projection_columns` for emit; add `compete_columns`; `next_batch` phase `Compete` \| `Full` |
| `cold_rows_from_segments` | Honor phase via `with_columns` |
| `MergeRowStream::maybe_enter_ordered_buffer` | Compete batches → sort → conditional hydrate |
| `ColdReadProfile` / EXPLAIN | Compete vs body open counts (and optional column lists at higher verbosity) |

`GeneralMerge` / unordered paths keep `Full` only.

## EXPLAIN / profile

On ordered path, expose at least:

- `Cold Compete Opens`
- `Cold Body Opens`

Hot-dominates after compete (or no cold emit) → body opens stay **0**.

## Efficiency guardrails

- Compete set must not accidentally include wide body columns
- Body pass only when cold winners will be emitted
- Prefer one Full open over Compete+Body when the projection is already narrow
- Do not regress locked hot-only / `cold_side_proven_empty` paths

## Testing

1. **Correctness:** existing ordered LIMIT, three_state equality, user-scope,
   exact-PK stay green
2. **Regression e2e:** wide `body` + many cold rows + `ORDER BY id LIMIT n`
   - body opens = 0 when result is hot-only after compete
   - body opens ≥ 1 only when cold rows are emitted
   - compete open excludes `body` when assertable via EXPLAIN/profile
3. Optional micro-assert on first-open column list

## Delivery notes

- Edit catalog SQL only if needed (not expected for this cut)
- Update `docs/architecture/scanning-table.md` when behavior lands
- Page index / Bloom remains a follow-on Phase 6+ item
