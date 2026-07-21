# Scanning Table Workflow (KoldMergeScan)

This document describes how `SELECT` queries against managed tables are planned
and executed through the `KoldMergeScan` custom scan node. It covers shared
preload, planner gates, catalog caching, cold Parquet reads, native hot child
plans, mirror overlay, winner resolution, and ownership boundaries.

**Planner hook:** `set_rel_pathlist` in `crates/pg_koldstore/src/merge_scan/pg.rs`  
**Rust merge:** `crates/koldstore-merge/src/core/resolver.rs`  
**Parquet read:** `crates/koldstore-parquet/src/reader.rs`  
**Preload gate:** `crates/pg_koldstore/src/preload.rs`

---

## Design principle

```text
Primary path: shared_preload → planner hook → KoldMergeScan
If hook/preload missing: ERROR at install/manage (fail closed)
Never: silent SeqScan that returns hot-only rows for managed tables
```

PostgreSQL remains the transaction, locking, index, and hot-row authority.
KoldStore adds a custom scan that is a **merge coordinator**:

```text
KoldMergeScan
├── PostgreSQL native child plan   (IndexScan / BitmapScan / SeqScan)
├── KoldParquetScan                (segments → row groups → projected batches)
└── MirrorOverlay                  (unflushed inserts/updates/tombstones)
```

The user table stays a normal heap table. There is no custom table access method
and no unified view rewrite. JOINs are safe: the hook runs **per base RTE**, so
only managed relations become `KoldMergeScan`.

- Planner cost is `hot_child_cost + catalog + estimated cold + merge overlay`.
- Heap-only finals **and** parallel partials are replaced so managed SELECTs
  cannot silently omit cold rows (including under `ORDER BY` / Gather Merge).
- `koldstore.enable_merge_scan = off` still plans `KoldMergeScan` then **ERRORs**
  at begin — never silent heap-only.
- Unmanaged tables keep normal `Seq Scan` / `Index Scan` after a cheap in-memory
  OID cache lookup (absences are cached so SPI is not repeated).
- Shared preload is mandatory so every backend has the planner hook before any
  query. A silent heap `SeqScan` fallback for managed SELECTs is forbidden: it
  would return incomplete results after flush.

### Merge invariant

Active cold state is treated as at most one visible version per PK after
newest-first resolution. The mirror overlay masks any PK that still has an
unflushed mirror row (`op` 1/2/3). Visible cold rows can therefore be appended
alongside native hot rows without a global `DISTINCT ON` sort.

### Cursor semantics

- `seq` is a row-version / effect identity (Snowflake id allocated at statement
  time). It is **not** a commit-order cursor.
- Durable change-stream replay must use WAL LSN / logical decoding.

---

## A. Cluster boot and install

```mermaid
flowchart TD
  start[Operator installs package files]
  conf[Add koldstore to shared_preload_libraries]
  restart[Restart Postgres]
  pgInit["_PG_init under shared preload"]
  flag[Set LOADED_VIA_SHARED_PRELOAD]
  hooks[Register set_rel_pathlist + CustomScan]
  launcher[Register async mirror launcher]
  createExt["CREATE EXTENSION in database"]
  catalog[Create koldstore.schemas and catalog]
  manage[manage_table writes schemas row]
  badLoad["_PG_init without preload"]
  err[ERROR must be shared_preload]

  start --> conf --> restart --> pgInit
  pgInit --> flag --> hooks --> launcher
  launcher --> createExt --> catalog --> manage
  badLoad --> err
```

`CREATE EXTENSION` creates SQL objects only. Preload installs hooks into every
backend. Check with `SELECT koldstore.preload_status();`.

Removing preload after manage is **unsupported**: fresh backends never load the
`.so`, so no extension code can intercept heap reads.

---

## B. Per-SELECT planner gate (every base relation)

Three questions — do not collapse them:

| Check | Scope | Implementation |
|-------|--------|----------------|
| Library preloaded | Process | `_PG_init` + `LOADED_VIA_SHARED_PRELOAD` |
| Extension installed | This database | `managed_catalog_ready()` syscache for `koldstore.schemas` |
| Table managed | This OID | Backend `OptionalLookupCache` (present **and** absent) |

```mermaid
flowchart TD
  select[SELECT reaches planner]
  hook[set_rel_pathlist called]
  prev[Call previous hook if any]
  nulls{root rel rte null?}
  disabled{DISABLE_HOOK?}
  rteKind{rtekind is RTE_RELATION?}
  cmd{commandType is SELECT?}
  dbReady{managed_catalog_ready syscache?}
  cacheGet[Backend HashMap lookup by table_oid]
  cacheHit{Cache entry?}
  absent[Cached None: not managed]
  present[Cached Some snapshot]
  active{snapshot.active?}
  spiMiss[SPI load koldstore.schemas once]
  store[Insert Some or None into cache]
  heap[Keep standard SeqScan IndexScan paths]
  inject[Clear pathlist and partial_pathlist]
  custom[Install CustomPath KoldMergeScan]

  select --> hook --> prev --> nulls
  nulls -->|yes| heap
  nulls -->|no| disabled
  disabled -->|yes| heap
  disabled -->|no| rteKind
  rteKind -->|no| heap
  rteKind -->|yes| cmd
  cmd -->|no| heap
  cmd -->|yes| dbReady
  dbReady -->|no extension in this DB| heap
  dbReady -->|yes| cacheGet --> cacheHit
  cacheHit -->|hit None| absent --> heap
  cacheHit -->|hit Some| present --> active
  active -->|false| heap
  active -->|true| inject --> custom
  cacheHit -->|miss| spiMiss --> store
  store -->|None| heap
  store -->|Some active| inject
```

`manage_table` / `unmanage_table` call `invalidate_table_globally` so peer
backends drop stale negative (“not managed”) entries.

For a managed relation the planner then:

1. Picks the cheapest non-custom path as the hot child.
2. Clears `pathlist` and `partial_pathlist` (required so Gather Merge cannot
   prefer a hot-only ordered path after flush).
3. Installs one `CustomPath` whose `custom_paths` holds that child.

---

## C. Executor path

```mermaid
flowchart TD
  Begin["BeginCustomScan"]
  Guc{enable_merge_scan?}
  Err[ERROR]
  Cat[Catalog + MergeScanPlan]
  Mirror[Load mirror overlay]
  Cold[Cold prune + Parquet]
  Merge[Mask cold by mirror PKs]
  HotOnly{Cold empty?}
  Stream["ExecCustomScan: ExecProcNode child"]
  Buf["Merge + emit buffer / stream"]

  Begin --> Guc
  Guc -->|off| Err
  Guc -->|on| Cat
  Cat --> Mirror
  Cat --> Cold
  Mirror --> Merge
  Cold --> Merge
  Merge --> HotOnly
  HotOnly -->|yes + child| Stream
  HotOnly -->|merge| Buf
```

### BeginCustomScan

1. Error if `enable_merge_scan` is off.
2. Deserialize `MergeScanPlan` when present.
3. Load catalog snapshot + mirror overlay (all unflushed mirror PKs).
4. Prune cold segments from local catalog stats; open ObjectStore readers only
   for remaining candidates.
5. Filter cold rows whose PK appears in the mirror overlay.
6. Hot-only + native child → stream mode; otherwise merge and materialize.

### ExecCustomScan / End / Rescan

- Hot-child mode: `ExecProcNode` on the child.
- Buffer mode: emit the next materialized row.
- Drop scan state on end; `ExecReScan` the hot child when present.

---

## Failure modes

| Misconfiguration | Symptom | Protection |
|------------------|---------|------------|
| No shared_preload | CREATE EXTENSION / LOAD errors | `_PG_init` gate |
| Preload removed after manage | Hot-only SELECT, no error | Docs + upgrade checklist only |
| Extension not in this DB | Normal heap scans | `managed_catalog_ready` |
| Unmanaged table | Normal heap scans | Negative OID cache |
| `enable_merge_scan=off` | ERROR at BeginCustomScan | No silent heap-only |

---

## Mirror overlay rules

| Mirror op | Effect on cold | Effect on result |
|-----------|----------------|------------------|
| 1 / 2 | Skip cold for that PK | Native hot child / hot load returns the live row |
| 3 | Skip cold for that PK | Row is invisible (no hot row) |
| none | Cold may be visible | Cold winner after merge rules |

---

## GUCs

| GUC | Meaning |
|-----|---------|
| `koldstore.enable_merge_scan` | Required for managed SELECT. `off` → ERROR at scan begin. |
| `koldstore.cold_reads=auto` | Cold eligible when catalog/cost says so. |
| `koldstore.cold_reads=on` | Cold eligible; does not force unnecessary object reads. |
| `koldstore.cold_reads=off` | Hot-only; ERROR when correctness would require opening cold. |
| `koldstore.max_open_parquet_readers` | Per-backend open Parquet reader cap. |

---

## Row-level security

Native hot-child scans remain PostgreSQL-owned and apply permissions and RLS
normally. Buffered cold and hot+cold winners are materialized in the base
relation's scan-slot layout, then returned through PostgreSQL `ExecScan`.

Fixed reads of extension-owned catalogs and mirror tombstones run under the
extension owner. Buffered merge scans read the complete hot source under a
tightly scoped relation-owner context so RLS cannot hide a newer winner;
PostgreSQL then evaluates the invoking role's compiled quals on resolved tuples.

---

## EXPLAIN

`EXPLAIN` / `EXPLAIN ANALYZE` should show at least:

- Hot Plan (Index Scan / Bitmap Heap Scan / Seq Scan)
- Emit path (`hot_child` / `hot_native` / `cold_native` / `merge_buffer`)
- Hot rows / Result rows (ANALYZE)
- Candidate segments / Segments pruned / Parquet segments opened
- Bytes fetched (ANALYZE)
- Mirror Tombstones / Mirror Overrides

Example shape (ANALYZE):

```text
Custom Scan (KoldMergeScan)
  Hot Plan: Index Scan
  Emit path: cold_native
  Hot rows: 0
  Result rows: 1
  Candidate segments: 12
  Segments pruned by min/max: 10
  Parquet segments opened: 2
```

---

## Implementation notes / remaining polish

1. Overlap merge path still uses SPI JSON hot load for winner resolution when a
   full PK equality probe is not available; PK point lookups use hot-native /
   cold-native emit.
2. User-scoped cold segment loading beyond `scope_key = ''` continues to land
   with catalog scope work.
3. No DSM / parallel CustomScan workers yet.
4. Backend Parquet footer metadata is cached across scans and cleared on flush /
   managed-table invalidation.
