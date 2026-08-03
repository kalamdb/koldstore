# Progressive Hot–Cold Query Architecture

Date: 2026-08-03  
Status: approved for implementation planning  
Branch context: `feature/wal-only-capture-71` and successors

## Purpose

Replace the current “one unordered `KoldMergeScan` that exhausts hot, then
cold” model with a **hot-biased, proof-driven, progressive** execution engine.
PostgreSQL remains the global relational planner. KoldStore offers multiple
accurate `CustomPath` alternatives (ordering, startup/total cost,
parameterization). Parent `Limit` drives laziness: optimized paths are lazy
producers, not special LIMIT executors.

**Core principle:** never open cold data merely because cold data exists. Open
it only when catalog or lower-level bounds prove it can still affect the next
logical result.

This design supersedes the mixed-path assumptions in
[2026-07-10-merge-scan-redesign.md](2026-07-10-merge-scan-redesign.md) where they
conflict (SPI JSON hot paging as the default mixed reader; single unordered
custom path; eager full-hot-before-cold). Locked hot-only / cold-proven-empty /
exact-PK probe contracts from `AGENTS.md` and `.cursor/rules/hot-only-merge-scan.mdc`
are retained.

## Goals

- Path portfolio with real `pathkeys`, startup cost, and total cost so
  `ORDER BY … LIMIT` can win without an external `Sort` when KoldStore can
  provide the order.
- Native PostgreSQL hot child as the default hot access (Datum slots /
  `ExecProcNode`), not SPI `to_jsonb` keyset paging.
- Ordered progressive merge: compare hot candidates to cold catalog bounds;
  open Parquet only when competitive.
- Deferred mirror overlay until a cold candidate is actually needed.
- One maintainable strategy module; delete superseded code as each strategy
  lands; document public path/executor entry points with `//!` / `///`.
- Keep APIs **scope-ready** (`scope_key` plumbed, single-scope assumption) without
  implementing per-user partition product behavior in this effort.

## Non-goals (this effort)

- Global DataFusion SQL planner inside the extension.
- Casual rewrite of locked hot-only / cold-proven-empty / exact-PK hit paths.
- Multi-scope union scans (out of scope forever for the progressive paths:
  one query binds one `scope_key`).
- Shipping per-user scoped manifests / partition product (future; design only).
- Aggregate upper paths, join pushdown, Star-Tree rollups, deletion vectors
  (later phases after ordered scanning is stable).

## Path portfolio

| Strategy | Intended shape | Cold behavior |
| --- | --- | --- |
| `ExactPrimaryKey` | `WHERE id = ?` | Probe hot first; bounds/Bloom only after miss |
| `UnorderedHotFirst` | `LIMIT N` without ordering | Emit visible hot first; defer cold |
| `OrderedProgressive` | Supported immutable `ORDER BY` ± `LIMIT` | Bound-gated frontier expansion |
| `GeneralMerge` | Unsupported order/expr/metadata | Conservative full logical merge |
| Aggregate (later) | Safe metadata/partial aggregates | Separate upper-path work |

**Not a CustomPath strategy:** when predicates or an empty manifest prove cold
cannot contribute, the planner keeps **native** Index/Seq/Bitmap paths and
installs no `KoldMergeScan` (locked hot-only early return). That behavior was
sometimes labeled `ProvenHotOnly` in drafts; it is not a portfolio tag and must
not wrap the heap in a custom scan.

Each offered KoldStore path is a complete logical-table path. Unwrapped
heap-only paths must not remain selectable after cold publication. Distinctions
among useful native hot children are preserved by wrapping each and calling
`add_path`.

Unsafe unwrapped heap paths and leftover `partial_pathlist` entries are still
cleared so Gather / Gather Merge cannot omit cold rows.

## Module layout (target)

Consolidate strategy identity in one place under the merge-scan tree:

```text
crates/koldstore-merge/src/scan/
  strategy.rs          # KoldPathStrategy, OrderedPathSpec, costing helpers (PG-free)
  path.rs              # path replacement / portfolio decisions (evolve existing)
  ordered_frontier.rs  # pure bound/actual comparison + frontier state
  ordered_merge.rs     # progressive merge loop (PG-free)

crates/pg_koldstore/src/merge_scan/pg/
  path_strategy/       # planner hook portfolio: wrap native paths, set pathkeys
    mod.rs
    portfolio.rs
    cost.rs
  hot_cursor.rs        # native / typed Datum ordered hot cursor
  cold_frontier.rs     # catalog-backed ordered frontier (SPI to catalogs only)
  execute.rs           # dispatcher by strategy; delete dead emit modes as retired
```

Public modules and logic-bearing functions carry `//!` / `///` docs (purpose,
invariants, `# Errors`). EXPLAIN labels and emit-path enums stay aligned with
`KoldPathStrategy` — no parallel naming systems.

**Cleanup rule:** when a strategy covers a query shape, remove the superseded
implementation (SPI JSON page reader for that shape, eager full-hot-before-cold,
unused EXPLAIN branches). Do not leave “new path beside old path forever.”

## Hot access

Priority order for hot rows:

1. Native PostgreSQL child (`EmitPath::HotChild` / `ExecProcNode`) whenever
   correct for the strategy.
2. Typed Datum SPI cursor under trusted merge identity only when the native
   child would apply RLS/security filtering before winner resolution and that
   would risk stale cold resurrection.
3. Never default mixed merge to `to_jsonb` keyset paging.

Exact-PK hot hit and proven-hot-only delegation remain locked performance paths.

Winner resolution and stale-version suppression occur before final user `WHERE`
and RLS via `ExecScan`.

## Ordered progressive execution

### Immutable ordering identity

Initially support only:

- Primary-key order.
- Configured immutable segment-order column.
- Composite orderings whose PostgreSQL comparison semantics KoldStore can prove.

Internally refine user order with PK tie-break:

```text
ORDER BY created_at DESC
→ ORDER BY created_at DESC, id DESC
```

Invariant: all versions of one logical PK share the same optimized order
identity `(order columns, primary key)`. Mutable/unsupported order columns fall
back to `GeneralMerge`.

### Merger

```text
peek hot actual  vs  cold best-possible bound
  HotStrictlyWins → resolve/emit hot, no Parquet
  ColdMayWinOrTie → expand frontier (segment → row group → metadata → payload)
  OnlyColdRemains → resolve cold group with deferred mirror/hot PK probes
```

For descending order, emit an actual candidate only when it strictly outranks
every unopened source’s maximum possible composite key (ascending uses minima).
Ties require inspecting tying sources before emit.

Finalize one ordering identity at a time and release its state (frontier-based
resolver, not a global seen-PK HashMap for the full table).

### Cold frontier

`OrderedColdFrontier` pages `cold_segment_order_index` for
`table_oid + scope_key + sort_order_id`. Preparing the frontier does not open
Parquet. Expansion is demand-driven.

Metadata-first cold reads: order key + PK + seq (+ needed filter cols) before
body/payload.

### Deferred mirror

- Hot-only top-N: zero mirror rows.
- Cold candidate batch: batched `pk, seq, op, …` lookup; retain seq for
  mirror-vs-cold resolution.
- Eager all-tombstone load remains only for `GeneralMerge` where cheaper or
  still required.

## Single-scope forward compatibility

**Now:** plumb `scope_key` through strategy, frontier, snapshot, and catalog
queries. Default `''` matches today’s catalogs. Assume **one query → one
scope**; never union scopes in a progressive scan. Missing/ambiguous scope
fails closed when scope resolution is required.

**Future (not this effort):** per-user / partition manifests as denser
`scope_key` values under the same APIs. Queries remain single-scope so planning
and I/O stay confined to one manifest and one segment set — that is the
performance property to preserve in API shape, not implement as product yet.

Existing tables already key manifest / segments / segment index by
`(table_oid, scope_key)`.

## Catalog changes (ordered path)

Keep `cold_segment_index` for single-column predicate pruning. Add composite
order index conceptually:

```sql
CREATE TABLE koldstore.cold_segment_order_index (
    segment_id uuid NOT NULL REFERENCES koldstore.cold_segments(segment_id)
        ON DELETE CASCADE,
    table_oid oid NOT NULL,
    scope_key text NOT NULL DEFAULT '',
    sort_order_id integer NOT NULL,
    codec_version smallint NOT NULL,
    min_composite_key bytea,
    max_composite_key bytea,
    row_group_min_composite_keys bytea[] NOT NULL,
    row_group_max_composite_keys bytea[] NOT NULL,
    physically_sorted boolean NOT NULL,
    bounds_exact boolean NOT NULL,
    PRIMARY KEY (segment_id, sort_order_id)
);
-- indexes on (table_oid, scope_key, sort_order_id, min|max key, segment_id)
```

Edit `crates/pg_koldstore/sql/koldstore--0.1.0.sql` directly (no upgrade edge
while in development). Flush publishes order-index rows for the segment’s
scope; prefer writing segments in configured order → PK → seq DESC within
version groups so bounds prune well.

Do not copy full Parquet page indexes / Bloom bitsets into catalogs; store
presence flags and cache footer metadata by object identity later.

## Snapshot and WAL fence (correctness risk)

Scan pins a coherent view (Postgres snapshot, manifest generation for the
bound scope, cold published max seq, mirror visibility boundary). Publication
generation must not change mid-scan; mirror must not omit deletes visible to
the hot snapshot nor apply invisible later deletes. Ordered merger must not
hide this dependency; concurrency tests are required as progressive paths
expand cold lazily.

## Query-shape coverage

Every query is either optimized safely or executed by correct `GeneralMerge`.
Not every query avoids cold I/O.

| Shape | Path |
| --- | --- |
| Exact PK | ExactPrimaryKey |
| Cold proven empty | Native plan-time early return (no KoldMergeScan) |
| `LIMIT` no order | UnorderedHotFirst |
| Supported `ORDER BY` ± `LIMIT`/`OFFSET` | OrderedProgressive (parent Limit consumes) |
| Mutable/expression order | GeneralMerge + PostgreSQL Sort |
| Aggregates / joins | Default merge + PG upper/join planning; specialized paths later |

Predicate classes: trusted scope equality (future) and immutable PK/order
ranges may prune sources; mutable column predicates and full RLS stay residual
after current-version resolution.

## EXPLAIN contract (target)

```text
Custom Scan (KoldMergeScan)
  Strategy: Ordered Progressive
  Output Order: created_at DESC, id DESC
  Hot Access: Native PostgreSQL Index Cursor
  Cold Frontier Source: koldstore.cold_segment_order_index
  Parquet Segments Opened: 0
  Mirror Rows Read: 0
  Cold Skip Reason: next hot key outranks maximum cold key
```

Visual plan continues to use KoldStore-internal diagnostic nodes; catalog
lookups are not described as runtime `manifest.json` reads.

## Performance acceptance (hot dominates)

For ~1k hot / ~99k cold and supported `ORDER BY … LIMIT 5` when hot bounds win:

- No external `Sort`
- Small adaptive hot fetch
- 0 Parquet opens, 0 mirror rows
- No global full-table seen-PK set
- Exact-PK hot hit still one native/typed probe with no cold/mirror init

## Delivery order

1. Path portfolio + pathkeys + costing (strategy module in one place)
2. Native/typed hot cursor; retire SPI JSON for covered shapes
3. Segment ordered frontier + zero-Parquet hot-dominance proof
4. Row-group expansion + deferred mirror/hot PK probes
5. Unordered `LIMIT` hot-first
6. Page index / late materialization
7. Later: partial aggregates, joins/runtime filters, scoped partition product

## Documentation updates when behavior lands

- This design doc (source of truth for the redesign)
- `docs/architecture/scanning-table.md` — planner portfolio, progressive merge,
  EXPLAIN, hot access
- Code `//!` / `///` on new strategy, frontier, and cursor modules
- Short forward-compat note on single-scope `scope_key` only — no premature
  product docs for per-user partitions

## Correctness matrix (essential)

Ordering (PK / segment-order, ASC/DESC, ties, NULLS); limits (const, param,
OFFSET, residual, RLS); source mix; versions; RLS hiding newer hot; scope_key
plumbing with default `''`; generation/cache; concurrency/flush/mirror lag;
rescan; fallback; exact-PK non-regression.

Particularly: newer hot hidden by RLS must not resurrect older cold; deletes
visible to the statement snapshot must suppress cold; equal order keys across
segments must yield deterministic valid top-N without duplicate PKs.
