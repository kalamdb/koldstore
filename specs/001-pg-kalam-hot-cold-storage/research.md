# Research: pg-kalam Hot/Cold Storage

**Branch**: `001-pg-kalam-hot-cold-storage`  
**Date**: 2026-07-02  
**Status**: Complete — all planning unknowns resolved

## 1. Unified Query Architecture: KalamMergeScan

### Decision

Implement **KalamMergeScan** as a PostgreSQL **Custom Scan Provider** that is the **only valid scan path** for pg-kalam managed tables. The scan wraps a normal PostgreSQL hot child plan (Seq/Index/Bitmap Heap) and merges results with cold Parquet segments read via **DataFusion** (cold side only).

### Rationale

- PostgreSQL's Custom Scan API (`set_rel_pathlist_hook` → `CustomPath` → `CustomScan` → `ExecCustomScan`) is documented and used in production extensions (e.g., Citus adaptive execution).
- A managed table is a **logical relation** (hot heap + cold Parquet + tombstones + version resolver), not a plain heap table. Leaving vanilla heap paths selectable would return incomplete results.
- DataFusion provides Parquet scan, projection/filter pushdown, row-group pruning, and Arrow `RecordBatch` execution without making it the outer SQL planner.

### Alternatives considered

| Alternative | Rejected because |
|-------------|------------------|
| FDW for cold reads | FDW cannot replace scanning of the same heap relation; dual-table semantics break transparent SQL |
| Extension-internal Parquet reader only (spec FR-036) | Viable for MVP but weaker pushdown/pruning; DataFusion is Apache-2.0 and purpose-built for this layer |
| View-based hot/cold union | Breaks transparent DML, planner pushdown, and index use on hot tier |
| Copy Citus custom scan code | Citus is AGPL-3.0; study architecture only, implement independently |

---

## 2. Extension Language & FFI Split

### Decision

**Rust via pgrx** for extension packaging, catalog, background workers, flush orchestration, and cold engine. A **minimal C shim** for Custom Scan structs/callbacks where pgrx lacks ergonomic wrappers (`CustomPath`, `CustomScan`, `CustomScanState`, `TupleTableSlot` bridging).

### Rationale

- pgrx supports PG 15–18+, SPI, memory contexts, planner/executor hooks, and extension lifecycle.
- Custom Scan requires low-level `pg_sys` interaction; pgrx documents that not all internals are wrapped.
- Split keeps unsafe FFI localized while Rust owns manifest I/O, DataFusion, merge resolver, and object-store clients.

### Alternatives considered

| Alternative | Rejected because |
|-------------|------------------|
| Pure C extension | Higher maintenance for Parquet/object-store/DataFusion integration |
| Pure Rust through pgrx only | Custom Scan ergonomics may be painful; shim is smaller risk |
| External sidecar service for cold reads | Violates PostgreSQL-native, lightweight goal; adds network hop and ops burden |

---

## 3. DataFusion Scope (Spec Amendment)

### Decision

**Amend FR-036** for planning/implementation: DataFusion is permitted **only** as the cold Parquet execution engine inside KalamMergeScan. PostgreSQL remains the outer SQL planner (joins, aggregates, permissions, snapshots). No RocksDB, no Raft, no DataFusion-as-primary-query-engine.

### Rationale

User architecture review confirms licensing (Apache-2.0) and correct layering. Spec originally excluded DataFusion for "lightweight PostgreSQL-native" positioning; cold-side engine does not change coordination model (still catalog + background workers + advisory locks).

### Alternatives considered

| Alternative | Rejected because |
|-------------|------------------|
| `parquet` crate + custom pruning | Reinvents filter/projection/row-group logic DataFusion already provides |
| Full DataFusion SQL planning | Breaks PostgreSQL semantics, RLS, MVCC, and join ordering guarantees |

---

## 4. DML Semantics (MVP)

### Decision

**Append-versioned model** aligned with kalamdb:

- `INSERT` → new hot row with monotonic `_seq`
- `UPDATE` → insert new version row in hot heap (not in-place heap update as source of truth)
- `DELETE` → insert tombstone row (`_deleted = true`) in hot heap

**MVP contract**:

- Transparent `SELECT` through KalamMergeScan (hot + cold merge)
- Transparent `INSERT` into hot heap
- `UPDATE`/`DELETE` on rows that exist **only in cold** are **not** transparent in v1; use `kalam.update()` / `kalam.delete()` SQL functions or require hot overlay first

### Rationale

PostgreSQL `ModifyTable` expects heap `ctid` for updates/deletes. Cold rows have no heap identity. Rewriting planner/executor for transparent cold DML is v2+ work.

### Alternatives considered

| Alternative | Rejected because |
|-------------|------------------|
| SELECT + INSERT only v1 | Breaks basic app compatibility for hot-row updates |
| Full transparent UPDATE/DELETE v1 | High complexity; easy to get MVCC/ctid wrong |
| Logical view interface for all DML | Less drop-in adoption |

---

## 5. Constraint & Index Model

### Decision

Native PostgreSQL **unique indexes and foreign keys apply to hot heap only** unless pg-kalam provides global enforcement. MVP adds optional **`pg_kalam.pk_registry`** (table_oid, pk_hash, latest_seq, deleted) for flush-time deduplication and conflict detection—not a substitute for app-level uniqueness guarantees across hot+cold in v1.

Document clearly: `CREATE UNIQUE INDEX` on managed tables does **not** guarantee global hot+cold uniqueness.

### Rationale

PostgreSQL index AMs cannot see Parquet segments. False sense of security is worse than explicit limitation.

### Alternatives considered

| Alternative | Rejected because |
|-------------|------------------|
| Block UNIQUE INDEX on managed tables | Too restrictive for hot-only workloads |
| Global uniqueness index v1 | Large scope; needs distributed conflict resolution on flush |

---

## 6. MVCC & Cold Segment Visibility

### Decision

Track cold segments in **`pg_kalam.cold_segments`** with snapshot-relevant metadata:

- `segment_id`, `table_oid`, `scope_key` (nullable for shared)
- `min_seq`, `max_seq`, `created_lsn`, `created_xid`, `commit_seq`
- `status` (`pending`, `active`, `deleting`, `deleted`)
- `schema_version`, `object_path`, `row_count`, `byte_size`

KalamMergeScan reads only segments whose metadata is **visible to the current transaction snapshot** (committed before snapshot, not deleted in snapshot).

### Rationale

Parquet files are external; PostgreSQL MVCC does not apply to bytes on object storage. Segment catalog bridges transactional visibility.

### Alternatives considered

| Alternative | Rejected because |
|-------------|------------------|
| Read all active segments always | Breaks snapshot isolation for in-flight flush |
| Embed xid ranges only in manifest.json | Manifest is object-store source of truth; PG needs fast snapshot checks |

---

## 7. RLS & Security Qual Pushdown

### Decision

Re-apply **equivalent security quals** to cold DataFusion scans. Planner-attached RLS quals from PostgreSQL are captured in `CustomPath.custom_private` and translated to DataFusion filter expressions for cold reads. User-scoped tables additionally require session scope (`kalam.scope` GUC) enforced on both hot and cold paths.

### Rationale

Without cold-side RLS, Parquet segments leak cross-tenant data. Non-negotiable for user-scoped tables.

### Alternatives considered

| Alternative | Rejected because |
|-------------|------------------|
| Rely on PostgreSQL to filter after merge | Too late—cold rows may enter executor against policy |
| Store per-scope Parquet only (no RLS on cold) | Insufficient for shared tables with role-based policies |

---

## 8. ORDER BY / LIMIT Strategy (MVP)

### Decision

**Correctness first**: full hot+cold merge (with tombstone/PK resolution), then apply ORDER BY/LIMIT at PostgreSQL executor level when pushdown is unsafe. Document that naive per-side LIMIT before merge is incorrect.

**Post-MVP**: top-K optimization when sort key aligns with `_seq`/segment ordering and proven safe.

### Rationale

Tombstones and duplicate PK versions can change winner set; split LIMIT breaks correctness.

---

## 9. Flush & Background Workers

### Decision

Use **PostgreSQL background workers** + **`system.jobs`** catalog (not external orchestrator). Coordination via **advisory locks** and idempotent job keys. Flush: dedupe by PK keeping max `_seq`, write `batch-N.parquet` via temp-then-rename, update `manifest.json`, update `pg_kalam.cold_segments`, remove flushed hot rows per retention rules.

### Rationale

Matches spec FR-014–FR-019 and kalamdb artifact layout. PostgreSQL-native coordination satisfies FR-036 (no Raft).

### Alternatives considered

| Alternative | Rejected because |
|-------------|------------------|
| pg_cron external flush | Extra dependency; harder crash safety story |
| Synchronous flush on INSERT | Latency and availability risk |

---

## 10. Object Storage & Backup

### Decision

Support **filesystem, S3-compatible, GCS, Azure** via Rust object-store abstraction (e.g., `object_store` crate). Expose operability SQL: `kalam.backup_manifest()`, `kalam.validate_cold_storage()`, `kalam.recover_segments()`. Document that **base backup/PITR does not include cold objects**.

### Rationale

Operators must understand cold data is outside WAL. Explicit APIs prevent false confidence.

---

## 11. PostgreSQL Version Target

### Decision

**PostgreSQL 15+** minimum (per spec assumptions). Primary development target **PG 16/17**. Custom Scan API stable across these versions; version-specific structs isolated in C shim.

### Rationale

PG 15 is common LTS baseline; pgrx supports through 18.

---

## 12. Testing Strategy

### Decision

| Layer | Tool |
|-------|------|
| Rust unit tests | `cargo test` (manifest, merge resolver, Parquet round-trip) |
| Extension SQL | `pg_regress` + `pgtap` optional |
| Integration | Docker Compose: PostgreSQL + MinIO; scripted hot→flush→query scenarios |
| Custom scan plans | `EXPLAIN` assertions that KalamMergeScan appears and vanilla Seq Scan does not |

### Rationale

Extension correctness requires real PostgreSQL executor and object store; unit tests alone insufficient.

---

## 13. Licensing

### Decision

Implement independently using PostgreSQL extension APIs (PostgreSQL License), pgrx (MIT), DataFusion (Apache-2.0). **Do not copy Citus source** (AGPL-3.0). Study Citus/Citus custom scan behavior only.

---

## Resolved Unknowns Summary

| Unknown | Resolution |
|---------|------------|
| Query merge mechanism | KalamMergeScan Custom Scan Provider |
| Cold read engine | DataFusion (cold path only) |
| Implementation language | Rust/pgrx + C custom scan shim |
| Transparent UPDATE/DELETE on cold rows | Deferred; append-version hot DML + kalam SQL functions |
| Global uniqueness/FK | Hot-only native; document limits |
| MVCC for cold | `pg_kalam.cold_segments` + snapshot filtering |
| RLS on cold | Qual translation to DataFusion filters |
| LIMIT/ORDER BY | Full merge first in MVP |
| PG version | 15+ |
| Testing | pg_regress + cargo + MinIO integration |

---

## 14. Developer DDL: CREATE TABLE USING kalamdb

### Decision

Primary greenfield path: `CREATE TABLE ... USING kalamdb WITH (type = 'shared' | 'user', ...)`. Accepts standard PostgreSQL datatypes and `DEFAULT SNOWFLAKE_ID()`. Mirrors kalamdb `WITH (TYPE=...)` syntax used in `/Users/jamal/git/kalamdb/backend/crates/kalamdb-dialect/src/ddl/create_table/parser.rs`.

### Rationale

Developers should not require a separate migration step for new tables. `USING kalamdb` is the table access method hook point (like `USING heap`).

---

## 15. Flush & Manifest (kalamdb imitation)

### Decision

Follow kalamdb commit order exactly:

1. DML → `pending_write` only (no manifest rewrite)
2. Flush lock → `syncing`
3. Write `batch-N.parquet.tmp` → rename `batch-N.parquet`
4. Append segment with `column_stats`, PK bloom filters, `schema_version`
5. Rewrite `manifest.json` listing **all** committed segments
6. Update `kalam.manifest` → `in_sync`

Reference: `kalamdb-flush/service.rs`, `scope_writer.rs`, `storage_cached.rs`.

---

## 16. Parquet Pruning & Bloom Filters

### Decision

**Write path**: PK columns get Parquet bloom filters (≥1024 rows); PK + `_seq` get min/max in manifest `column_stats`. On migration, read PostgreSQL indexes to determine bloom/stats columns.

**Read path**: Two-layer pruning—manifest segment selection, then Parquet footer bloom/stats row-group pruning without full-file reads. Imitate `kalamdb-filestore/parquet/reader.rs` and `kalamdb-tables/manifest/planner.rs`.

---

## 17. Tombstone Deletes on Cold Rows

### Decision

Deleting a cold-only row appends a hot tombstone (`_deleted = true`, new `_seq`). Merge resolver must prefer tombstone over all older hot/cold versions. Implemented via `kalam.delete()` in MVP; standard SQL DELETE when hot version exists.

---

## 18. Schema Catalog & DDL Hooks

### Decision

Keep `system.schemas` but auto-populate: `CREATE EXTENSION` installs catalog tables; `CREATE TABLE USING kalamdb` and event triggers on `ALTER TABLE` for managed tables update schema versions. No manual catalog maintenance.

---

## 19. Binary Size & DataFusion Minimal Build

### Decision

Cargo feature flags: `datafusion` with only parquet scan + physical plan nodes needed. No DataFusion SQL frontend. Prefer `default-features = false` on heavy deps. Target: smallest extension binary practical for cold scan.

---

## 21. Realtime Change-Feed Readiness

### Decision

Dual read semantics:

- **Default merged read**: PK-level view; tombstones excluded (apps see logical current state)
- **Change-feed read**: `_seq`-ordered stream of all appended versions including tombstones (`_deleted = true`) for future realtime on kalamdb tables

Every INSERT/UPDATE/DELETE MUST append a new row with a new `_seq` (never in-place). This matches kalamdb append versioning and enables `changes_since(seq)` polling or `SET kalam.changelog = on`.

### Rationale

Realtime subscribers need delete events. Hiding tombstones in the default view is correct for apps; collapsing them away entirely would make delete propagation impossible without a separate WAL.

### Surface

- `kalam.changes_since(table, seq)` or `SET kalam.changelog = on` + `WHERE _seq > N`
- Flush retains tombstones per deleted-row retention so cold replay preserves delete events

### Decision

`kalam_exec(sql)` supports kalamdb-compatible `EXPORT TABLE` / `IMPORT TABLE` (Parquet + manifest archive). `DROP TABLE` cascades to object-storage prefix cleanup. Reference: `kalamdb-jobs/table_transfer.rs`, kalamdb pg `pgrx_entrypoint.rs`.
