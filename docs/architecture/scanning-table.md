# Scanning Managed Tables

Managed tables retain an ordinary PostgreSQL heap for hot rows. A read uses PostgreSQL's native
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

The examples below assume `public.messages` has already been managed and at
least one `flush_table` has published cold segments. Use this form to inspect
both the selected plan and the executor path:

```sql
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF)
SELECT id, body
FROM public.messages
WHERE id = 42;
```

Plain `EXPLAIN` reports the planned strategy and potential sources. `EXPLAIN
ANALYZE` also reports **Actual Access**, the emit path, cold-open counts, mirror
overlay counters, and pruning. Exact node text is PostgreSQL-version and
statistics dependent; the correctness contract does not depend on whether the
native hot child is an Index, Bitmap, or Seq Scan.

If a table is managed and cold storage might contribute, the hook removes both
ordinary final and partial paths and installs a KoldMergeScan path portfolio.
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

### Planner selection matrix

The hook runs only for a base relation in a `SELECT`. Every earlier exit keeps
PostgreSQL's original paths; no KoldStore source is read in that case.

| Condition at planning time | Example | Plan selected | Why |
| --- | --- | --- | --- |
| Not a `SELECT`, not a relation RTE, extension-internal SPI, catalog unavailable, or unmanaged table | `UPDATE public.messages ...`; `SELECT * FROM public.audit_log` | Native PostgreSQL | The hook is deliberately out of scope or the relation has no managed cold state. |
| Managed table with no published segments | Immediately after managing an empty table | Native PostgreSQL | The hot heap is complete. |
| Constant predicate proves every cold segment is outside the range | `WHERE id >= 1000000` when cold `id` max is lower | Native PostgreSQL | Catalog bounds prove cold cannot contribute. |
| Managed table with a cold-capable predicate | `WHERE id BETWEEN 1 AND 100` | `KoldMergeScan` portfolio | Cold might contain visible rows. |
| Parameter or incomplete/unknown bounds | `PREPARE q(bigint) AS SELECT * FROM public.messages WHERE id >= $1` | `KoldMergeScan` portfolio | The generic plan cannot assume a future parameter makes cold irrelevant. |
| Catalog/pruning uncertainty | Unsupported or incomplete metadata | `KoldMergeScan` or an error at execution | Correctness wins over a potentially incomplete heap-only result. |

Publication invalidates PostgreSQL relation caches. A prepared statement that
had a native plan before the first flush is therefore replanned before it can
observe cold rows. Likewise, a catalog-bound expansion invalidates a plan that
previously proved cold empty.

## Strategy portfolio and query shapes

When cold may contribute, KoldStore removes bare final and partial heap paths
and installs complete logical-table paths. PostgreSQL chooses among that
portfolio using its normal cost and pathkey rules. The strategy describes the
planned shape; the executor may still take a faster runtime path later.

| Strategy | Selected for | Query examples | Execution contract |
| --- | --- | --- | --- |
| `ExactPrimaryKey` | Equality predicates cover **every** PK column with a constant or external parameter | `WHERE id = 42`; `WHERE tenant_id = 'a' AND id = 42` for a composite PK | Probe a visible hot point hit before opening Parquet. A miss consults the mirror and cold candidates. |
| `UnorderedHotFirst` | No usable `ORDER BY` path; especially valuable with a parent `LIMIT` | `SELECT * FROM public.messages LIMIT 20`; `WHERE id >= 1 LIMIT 20` | Emit visible hot rows first and defer mirror/cold work until hot is exhausted. Without `ORDER BY`, any valid row order is permitted. |
| `OrderedProgressive` | The native path's leading key is the first PK column or the configured immutable segment-order column | `ORDER BY id DESC LIMIT 20`; `ORDER BY created_at ASC LIMIT 20` when `created_at` is the segment-order column | Compare the hot frontier with catalogued cold bounds; retain supported pathkeys so PostgreSQL can avoid an external `Sort`. |
| `GeneralMerge` | Conservative full logical merge when a General-Merge strategy is carried into execution | No normal SQL shape is guaranteed to force this tag; verify `Strategy: General Merge` in `EXPLAIN ANALYZE` | Read hot candidates in SPI JSON keyset pages, resolve them with mirror/cold rows, and let PostgreSQL apply an outer `Sort` if required. |

For an `ORDER BY` that does not match a supported ordered path, the current
portfolio normally advertises an unordered logical path and PostgreSQL adds a
`Sort` above it. The result remains correct; it simply cannot use the
ordered-progressive early-stop optimization. Do not infer a strategy from SQL
alone—inspect `Strategy` in `EXPLAIN ANALYZE`.

## KoldMergeScan shape

    KoldMergeScan
    |- native PostgreSQL hot child (Index / Bitmap / Seq scan)
    |- newest-first cold Parquet segment stream
    plus latest-state __cl mirror overlay

The hot child preserves PostgreSQL's index, permission, locking, and RLS
behavior for heap tuples. Cold rows are materialized by the custom scan, not by
the heap access method, so heap system columns, row locks, SSI predicate locks,
and native constraint checks do not apply to them. KoldStore is the coordinator;
it does not replace the heap with a custom table access method or a view rewrite.

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

### Cold-pruning and opening cases

Pruning is progressive. It begins with catalog-only checks and opens Parquet
only for the remaining candidates. Any unavailable or incomplete proof is
conservative: it expands the candidate set or raises an error, never returns a
heap-only partial result.

| Stage | Applies to | Example | Effect |
| --- | --- | --- | --- |
| Manifest segment count | Every managed `SELECT` | First query after `manage_table`, before any flush | Zero published segments keeps the native heap plan. |
| Aggregate Sort-Key bounds | Constants at planning; external parameters at execution | `WHERE id > 1000000` | Can prove cold empty and keep/delegate to the native child. |
| Segment-index bounds | Cold-capable predicates on PK, catalog-indexed, scope, or segment-order columns | `WHERE created_at >= '2026-01-01'` | Selects only segment objects whose min/max bounds overlap. |
| Packed row-group bounds | A selected segment with finer-grained index metadata | Same range inside a wide segment | Skips noncompetitive row groups even when segment-level bounds overlap. |
| Single-column PK probe | Exact equality on a single supported PK | `WHERE id = 42` | Narrows the cold point read with PK segment-index access and Parquet Bloom/min-max metadata. Composite PKs remain conservative. |
| Ordered cold frontier | Supported `ORDER BY` | `ORDER BY created_at DESC LIMIT 10` | Reads catalog composite bounds before Parquet to determine whether cold can outrank the current hot frontier. |
| User scope bounds | Managed user-scoped relation | `WHERE user_id = 'tenant-a'` | Allows scope-column pruning in addition to PostgreSQL RLS. |

`EXPLAIN ANALYZE` exposes this work through `Cold Segments Query`, `Segment
Index Query`, candidate/open counts, and, for ordered reads, compete/body open
counters. Plain `EXPLAIN` shows planned rather than accumulated runtime values.

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

### Runtime emit-path matrix

The plan's `Strategy` and the executor's `Actual Access` answer different
questions. An `Exact Primary Key` plan can execute entirely from a hot point
hit, entirely from cold after a hot miss, or delegate a prepared range query to
its native child after parameter binding proves cold empty.

| Emit path / `Actual Access` | When it runs | Typical query | Work avoided or performed |
| --- | --- | --- | --- |
| `hot_child` / Native PostgreSQL Child | Runtime bounds prove cold empty and a native child exists | `EXECUTE q(1000000)` for a prepared range above all cold bounds | Delegates tuples to the original hot child; no mirror or Parquet setup. |
| `hot_native` / SPI Native Tuple Scan | A full-PK hot probe finds the row, or cold setup yields no source without a native child | `WHERE id = 42` when the current hot heap contains `42` | No Parquet open; the point result is materialized directly. |
| `cold_native` / SPI Native Point Probe | A full-PK hot probe misses but cold may contain the key | `WHERE id = 42` after that row was flushed and pruned hot | Loads the immediate mirror overlay and only cold candidates for the point lookup. |
| `unordered_hot_first` / Native PostgreSQL Child (or SPI JSON fallback) | Unordered logical scan, commonly with `LIMIT` | `SELECT body FROM public.messages LIMIT 20` | Native hot rows flow first. Mirror and Parquet stay deferred if the parent stops before hot exhausts. |
| `ordered_merge_native` / Native PostgreSQL Child (or SPI JSON fallback) | `OrderedProgressive` needs a supported ordered path | `ORDER BY id DESC LIMIT 20` | Uses a hot cursor and cold frontier/row-group competition to stop as soon as the parent limit is satisfied correctly. |
| `merge_stream` / SPI JSON Keyset Scan | Conservative/general merge path | A plan carrying `GeneralMerge` | Reads hot rows in bounded primary-key keyset pages and resolves them against cold and mirror rows. |

The native-child rows still pass through PostgreSQL's projection, quals,
permissions, and RLS handling. KoldStore may read enough columns to resolve
winners before PostgreSQL evaluates the final user-facing projection and
residual conditions.

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

### Ordered-progressive cases

`OrderedProgressive` has several observable subcases. All preserve the SQL
ordering contract; the difference is how much hot/cold data must be read before
the next result is known.

| Situation | Example | What happens |
| --- | --- | --- |
| Hot frontier strictly dominates cold | Recent IDs remain hot: `ORDER BY id DESC LIMIT 5` | The native ordered hot child supplies the top five. Cold and the deferred mirror can remain unopened. |
| Cold can win | Old IDs or timestamps outrank hot: `ORDER BY id ASC LIMIT 5` after old rows were flushed | KoldStore opens only competitive cold row groups, resolves winners, and returns cold rows before lower-ranked hot rows. |
| Hot/cold overlap | A hot update shares a PK with a cold row | The mirror/hot winner masks the older cold version before ordering and limit are applied. |
| Wide projection with a competitive cold candidate | `SELECT id, body FROM ... ORDER BY id LIMIT 5` | It may open a narrow compete projection (order key + PK) first, then hydrate non-key body columns only for cold winners. |
| Parent limit satisfied after competition by hot winners | `SELECT id, body FROM ... ORDER BY id LIMIT 3` | `Cold Body Opens` stays zero: cold body columns are not read merely to prove they would lose. |
| Narrow projection | `SELECT id FROM ... ORDER BY id LIMIT 5` | Fails open to one full cold projection rather than double-reading compete and body columns. |

Both ascending and descending supported orderings use the same frontier rule.
An `ORDER BY` on a different expression or mutable column remains correct, but
does not receive this no-external-sort / early-stop promise.

### Query constructs covered by the merge path

KoldMergeScan is attached per managed base relation, not per top-level SQL
shape. PostgreSQL can therefore compose it with normal relational operators.
The following examples require the merge path whenever their managed input can
match cold storage:

```sql
-- Equality, ranges, IN lists, and prepared parameters.
SELECT * FROM public.messages WHERE id IN (1, 2, 3);
SELECT * FROM public.messages WHERE created_at >= now() - interval '7 days';
PREPARE message_by_id(bigint) AS
  SELECT id, body FROM public.messages WHERE id = $1;

-- Projection and residual expressions are evaluated after winner resolution.
SELECT id, payload->>'kind'
FROM public.messages
WHERE COALESCE(payload->>'kind', '') <> 'internal';

-- Aggregates, DISTINCT, joins, semi-joins, and set operations remain PostgreSQL nodes.
SELECT category, count(*) FROM public.messages GROUP BY category;
SELECT DISTINCT category FROM public.messages;
SELECT m.id, a.name
FROM public.messages AS m JOIN public.accounts AS a ON a.id = m.account_id;
SELECT m.id FROM public.messages AS m
WHERE EXISTS (SELECT 1 FROM public.accounts AS a WHERE a.id = m.account_id);
SELECT id FROM public.messages
UNION SELECT archived_id FROM public.archived_message_ids;

-- Outer and cross joins retain normal PostgreSQL semantics as well.
SELECT m.id, a.name
FROM public.messages AS m LEFT JOIN public.accounts AS a ON a.id = m.account_id;
SELECT m.id, a.name
FROM public.messages AS m RIGHT JOIN public.accounts AS a ON a.id = m.account_id;
SELECT m.id, a.name
FROM public.messages AS m FULL JOIN public.accounts AS a ON a.id = m.account_id;
SELECT m.id, t.label FROM public.messages AS m CROSS JOIN public.tags AS t;
```

For each managed base scan, winner resolution happens before PostgreSQL applies
the query's residual predicate, aggregate, join, sort, or limit. This prevents
an older cold version from satisfying a filter after a newer hot update or
tombstone should have hidden it.

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
commit-order cursor; durable replay uses WAL LSN. For a committed, unflushed
mutation that must mask old cold data, wait for mirror capture with
`koldstore.wait_for_async_mirror()` before acquiring the snapshot used by the
read. The fence cannot expose the current transaction's uncommitted changes.

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
| `SELECT ctid`, `xmin`, or another PostgreSQL system column from a cold-capable managed relation | Error: KoldMergeScan cannot materialize heap system attributes from Parquet. |
| `SELECT ... FOR UPDATE/SHARE` that can reach cold rows | Unsupported: cold rows have no heap TID to lock. |
| `SERIALIZABLE` transaction that can read cold rows | Unsupported as a PostgreSQL-equivalent guarantee: cold reads do not participate in heap predicate locking. |
| `TABLESAMPLE`, inheritance/partition routing, or `TRUNCATE ... CASCADE` involving managed cold data | Not part of the supported preview contract; these shapes must not be assumed hot+cold complete. |
| `koldstore.max_merge_seen_keys` exceeded | Error rather than dropping older keys from winner resolution. |
| `koldstore.cold_reads = off` while cold is required | Error rather than an incomplete hot-only answer. |

See [DML](dml-table.md) for overlay production and
[mirror capture](mirror-capture.md) for the consistency fence.
