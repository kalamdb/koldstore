# Scanning Managed Tables

Managed tables remain ordinary PostgreSQL heaps. A read uses PostgreSQL's native
plan when published cold storage cannot contribute; otherwise the
KoldMergeScan custom scan combines hot heap rows, cold Parquet rows, and the
unflushed mirror overlay. This document describes the current planner and
executor contract.

Planner hook: crates/pg_koldstore/src/merge_scan/pg.rs
Merge semantics: crates/koldstore-merge/src/core/
Cold reader: crates/koldstore-parquet/src/reader.rs

## Correctness boundary

    shared preload -> planner hook -> native hot plan OR KoldMergeScan

    Never: an accidental hot-only plan when published cold rows can match

KoldStore registers its planner hook through shared_preload_libraries. The hook
runs for each base relation in a SELECT, so joins can mix managed and unmanaged
tables. Unmanaged relations, relations in a database without the extension
catalog, and extension-internal SPI queries retain PostgreSQL's normal planning.

If a table is managed and cold storage might contribute, the hook removes both
ordinary final and partial paths and installs a single KoldMergeScan path.
Clearing partial_pathlist matters: otherwise PostgreSQL can build a Gather or
Gather Merge over a leftover hot-only path and return an incomplete ordered or
limited result.

koldstore.enable_merge_scan = off causes a KoldMergeScan plan to error at
execution; it is not a request to silently read only the heap.

## Plan-time source choice

1. Is the base relation managed and active? If not, retain native PostgreSQL
   paths.
2. Does the compact manifest hint show zero published segments? If so, retain
   native paths: the heap is complete.
3. Do constant predicates and complete aggregate Sort Key bounds prove no cold
   segment can match? If so, retain native paths.
4. Otherwise install a **portfolio** of `KoldMergeScan` paths via `add_path`:
   - a non-ordering fallback around the cheapest native hot child
     (`GeneralMerge`, `ExactPrimaryKey`, or `UnorderedHotFirst`)
   - an `OrderedProgressive` wrapper for each native path whose leading
     pathkeys match the primary key or configured segment-order column, with
     those `pathkeys` copied onto the custom path so PostgreSQL can avoid an
     external `Sort` for supported `ORDER BY` / `LIMIT`

Cold-proven-empty predicates keep native Index/Seq/Bitmap paths with no
`KoldMergeScan` wrapper (plan-time early return; not a CustomPath strategy).

Unsafe unwrapped heap paths and leftover `partial_pathlist` entries are
cleared so Gather / Gather Merge cannot omit cold rows. Strategy identity and
an empty default `scope_key` are stored in custom private data (forward-compat
for single-scope partitions).

The planner stores both present and absent managed-table lookups in a bounded
backend cache. For a managed table it reads only compact catalog hints:
published segment count/generation and aggregate Sort Key bounds. It does not
open Parquet files or cache row-group arrays during planning.

Publication sends relcache invalidation, so a cached native plan is rebuilt
before newly published cold rows become visible. Missing catalog data,
unsupported types, mutable predicates, incomplete bounds, or catalog errors
remain conservative and use KoldMergeScan.

## KoldMergeScan shape

    KoldMergeScan
    |- native PostgreSQL hot child (Index / Bitmap / Seq scan)
    |- newest-first cold Parquet segment stream
    plus latest-state __cl mirror overlay

The hot child preserves PostgreSQL's index, permission, locking, and RLS
behavior. KoldStore is the coordinator; it does not replace the heap with a
custom table access method or a view rewrite.

### What is being merged

`KoldMergeScan` is a correctness merge, not a union of independently visible
tables. It resolves each primary-key identity across these sources before it
emits a row:

| Source | Role | Can override an older cold row? |
| --- | --- | --- |
| Hot heap | Current PostgreSQL row image and native access path | Yes |
| `koldstore.<schema>_<table>__cl` | Latest-state metadata: sequence and tombstone overlay | Yes; updates/inserts mask cold, deletes suppress it |
| Cold Parquet segments | Published older row images | Only when no newer hot/mirror state masks the key |

The exact-winner resolver retains primary-key identities, not complete row
images. This is why an ordinary heap-only plan is permitted only when the
planner or executor has proven that cold cannot contribute.

At plan time the cheapest native hot path becomes the custom scan's child. The
custom scan is not parallel-safe, so it owns the final scan path whenever cold
is possible. There is no DSM/parallel custom-scan implementation today.

### JSON explain contract

When a native hot child is initialized under `KoldMergeScan`, PostgreSQL owns
the child's `Plans` entry. Hot and cold diagnostics then appear as `Scan
Sources` property groups on the custom scan (`Hot Scan`, `Cold Scan`, `Mirror
Scan`), not as synthetic plan nodes:

    KoldMergeScan
    |- <native Index / Bitmap / Seq Scan>   # Plans child
    Scan Sources:
      Hot Scan / Cold Scan / Mirror Scan    # property groups

Synthetic `KoldStore Internal` plan nodes are emitted only when no native
child is present (for example SPI-backed general merge), so visualizers still
see the cold catalog → Parquet pipeline:

    KoldMergeScan
    |- KoldStore Hot Scan
    |  `- KoldStore PostgreSQL Hot Access
    `- KoldStore Cold Storage Scan
       `- KoldStore Segment Catalog Scan
          `- KoldStore Parquet Scan (one per selected segment)

Hot labels distinguish planner shape from runtime:

- `Hot Planned Access` / `Planned Access` — cheapest native child shape
  retained from planning (for example `Index Scan`).
- `Hot Actual Access` / `Actual Access` — what actually ran
  (`Native PostgreSQL Child` for `hot_child`, `OrderedProgressive` /
  `ordered_merge_native`, and `UnorderedHotFirst` / `unordered_hot_first`;
  `SPI JSON Keyset Scan` only for unordered `GeneralMerge` / `merge_stream`;
  SPI native labels for hot-only/cold-native fallbacks).
- `Hot SPI Query` — first-page SPI text for `merge_stream` JSON keyset
  paging (absent when the native child ran).
- `Strategy` — portfolio identity (`Ordered Progressive`, `General Merge`, …).

`OrderedProgressive` and `UnorderedHotFirst` widen the native hot child to a
physical relation target list so merge can read PK and full row images from
`ExecProcNode` slots under the relation-owner merge identity, then project to
the query target list after winner resolution. When cold is proven empty and
emit is `hot_child`, the widened child is copied into `ss_ScanTupleSlot` and
`ExecScan` applies the CustomScan projection (copying into
`ps_ResultTupleSlot` directly would segfault on narrow `SELECT` lists). SPI
JSON keyset paging remains the hot source only for non-ordered general merge
(and as a fallback when a native child still omits required columns, e.g.
`count(*)`).

### Late materialization (`OrderedProgressive` only)

When cold expand is required and the scan projection is wider than order key +
PK, cold Parquet opens split into:

1. **Compete** — order key + PK (plus forced cold meta) to resolve/sort against
   hot.
2. **Body** — remaining projected columns, opened only when a cold winner is
   about to emit.

Parent `LIMIT` that stops on hot-only after compete keeps **Cold Body Opens**
at 0. Narrow projections (`SELECT id ORDER BY id`) fail open to a single Full
open so compete+body never double-reads. EXPLAIN ANALYZE reports `Cold Compete
Opens`, `Cold Body Opens`, and optional `Cold Compete Columns` /
`Cold Body Columns`. `GeneralMerge` and `UnorderedHotFirst` stay on Full opens
only.

Runtime selection queries `koldstore.cold_segments` and (when bounds apply)
`koldstore.cold_segment_index`; those SPI texts appear as `Cold Segments
Query` / `Segment Index Query`. `Runtime Manifest Read` is always false for
this path. When synthetic cold nodes are emitted, Parquet segment nodes nest
under the catalog node to show that catalog prune decides which files open.
Mirror tombstone counters remain under the text `Mirror Scan` group; they are
not duplicated as a visual plan node.

## Executor fast paths

BeginCustomScan keeps two common paths out of the expensive merge setup:

1. For an uninstrumented complete-PK equality plan, it runs the native child
   first. A visible hot hit returns that child slot directly, without catalog
   lookup, Parquet open, mirror load, tuple copy, or merge-state allocation.
2. For a safe non-parameterized child, executor parameters can make the cold
   side provably empty even when plan-time literals could not. The scan then
   delegates every tuple to the native child.

EXPLAIN ANALYZE uses the same semantics but initializes profiling state so the
reported counters remain meaningful. A hot miss or any uncertain cold state
falls through to the merge pipeline.

## Merge pipeline

For a general cold-capable query, KoldStore loads the active schema/catalog,
uses local segment stats to prune cold candidates, and prepares a lazy
newest-first cold stream. It loads the mirror overlay before cold results are
allowed to surface:

| Mirror op | Cold row with same PK | Visible result |
| --- | --- | --- |
| insert or update | masked | Current hot row wins |
| delete | masked | no row |
| no mirror row | eligible | newest cold winner may surface |

Hot rows are read either from the native child (`OrderedProgressive` and
`UnorderedHotFirst`) or in bounded SPI JSON keyset pages (`GeneralMerge`).
Cold data is decoded one segment group at a time. The resolver retains exact PK
identities so the newest row wins across hot, mirror, and cold sources, and
preserves batch encounter order so ordered progressive paths can honor pathkeys
without an external `Sort` when the top-N is covered by hot.
`koldstore.max_merge_seen_keys` caps this set and fails closed when exceeded
(0 disables the cap). Parent LIMIT can stop before older cold groups are opened.

`OrderedProgressive` loads catalog composite bounds from
`koldstore.cold_segment_order_index` without opening Parquet. After each hot
page it compares actual leading Sort Key encodings to the cold frontier: when
hot strictly dominates, cold and the deferred mirror stay unread (typical
hot-dominant `ORDER BY … LIMIT`). When cold may win or tie, remaining hot and
competitive cold row groups are resolved and sorted by the leading Sort Key so
mixed top-N results stay correct without SPI JSON paging.

The regular hot+cold path materializes resolved rows into the base relation's
slot layout and lets PostgreSQL evaluate compiled user quals after winner
resolution. Catalog and mirror reads run under the extension owner; hot source
pages use the relation-owner context so RLS cannot hide a newer winner before
the invoking role's quals are applied.

## Query behavior developers rely on

| Query state | Expected plan/result behavior |
| --- | --- |
| Unmanaged relation | Native PostgreSQL plan. |
| Managed table with empty manifest | Native Index/Seq/Bitmap plan; no KoldMergeScan. |
| Cold-capable predicate | KoldMergeScan with a Hot Scan and planned native access. |
| Cold-proven-empty hot PK lookup | Native plan at plan time, or child delegation at execution time. |
| Hot PK hit while cold may exist | KoldMergeScan can return the native child slot without opening cold data. |
| ORDER BY, LIMIT, parameters, joins | Cannot bypass the merge path when cold can contribute. |

seq identifies a row effect for mirror ordering and flush cutoffs. It is not a
commit-order cursor; durable replay uses WAL LSN. For an unflushed mutation that
must mask old cold data, wait for mirror capture with
koldstore.wait_for_async_mirror() before the read.

## Cold-read controls and diagnostics

| GUC | Effect |
| --- | --- |
| koldstore.cold_reads | auto (default), on, or off; off errors if correctness requires cold data. |
| koldstore.max_open_parquet_readers | Per-backend reader cap. |
| koldstore.max_merge_seen_keys | Exact PK winner-set cap. |
| koldstore.enable_merge_scan | Required for cold-capable managed reads. |

EXPLAIN uses PostgreSQL's native explain APIs. Plain explain reports the
planned source state. EXPLAIN ANALYZE adds the emit path, native hot access,
cold segment/row-group pruning and I/O, mirror overlay effects, winner counts,
and phase timings. These counters and timers are allocated only for
instrumented execution.

## Failure handling

| Condition | Behavior |
| --- | --- |
| Extension not installed in this database | Normal PostgreSQL planning. |
| Unmanaged table | Normal PostgreSQL planning. |
| Preload absent when required | Install/manage fails closed; do not rely on a hot-only fallback. |
| Cold metadata unavailable or uncertain | Use merge path and surface the error if it cannot execute. |
| Merge scan disabled | Error rather than incomplete heap-only result. |

See [DML](dml-table.md) for overlay production and
[mirror capture](mirror-capture.md) for the consistency fence.
