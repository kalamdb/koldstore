# Crate Layout: pg-koldstore Workspace

**Version**: 0.3.0 (planning)
**Branch**: `001-pg-koldstore-hot-cold-storage`

The workspace separates pure Rust logic from PostgreSQL/pgrx glue. The goal is no duplicate merge, manifest, Parquet, or DML semantics in `pg_koldstore/src`.

## Dependency Direction

```text
koldstore-core
  -> koldstore-manifest
  -> koldstore-storage
  -> koldstore-parquet
  -> koldstore-merge
  -> koldstore-catalog
  -> pg_koldstore
```

There is no `koldstore-cold` crate in MVP. Cold Parquet reading belongs in `koldstore-parquet` next to the writer/footer/stat logic so projection, row-group pruning, bloom checks, and schema conversion are single-sourced.

## Crates

### `crates/koldstore-core`

Pure Rust shared types.

```text
src/
  lib.rs
  seq.rs             # SeqId and CommitSeq types, traits
  pk.rs              # logical PK extraction, hashing, JSON encoding
  table_kind.rs      # Shared | UserScoped
  row.rs             # HotRow, ColdRow, RowEvent, Tombstone
  filter.rs          # safe predicate classification
  error.rs
```

Rules:

- No pgrx/PostgreSQL dependency.
- No object-store or Parquet dependency.

### `crates/koldstore-manifest`

Manifest model and commit/recovery helpers.

```text
src/
  model.rs           # Manifest, SegmentMetadata, FilesState
  read.rs
  write.rs
  publish.rs         # backend-safe temp/final/manifest publish protocol
  sync_state.rs      # pending_write | syncing | in_sync | stale | error
  prune.rs           # segment-level stats pruning
```

Owns kalamdb-compatible `manifest.json` serialization. It does not read PostgreSQL catalogs.

### `crates/koldstore-storage`

Object store backends and path templates.

```text
src/
  backend.rs         # object_store-backed factory
  path_template.rs
  publish.rs         # conditional put/copy/delete helpers
```

Uses the Rust `object_store` crate. It must not expose an "atomic rename" abstraction unless a backend actually provides one.

### `crates/koldstore-parquet`

Single source for Parquet writing and reading.

```text
src/
  writer.rs          # batch files, compression, bloom/stat metadata
  reader.rs          # Arrow RecordBatch stream from object_store
  footer.rs          # row-group stats, bloom/page index metadata
  prune.rs           # row-group pruning by PK/_seq/_commit_seq
  schema.rs          # PG catalog/schema registry -> Arrow schema
```

Dependencies:

- `arrow`
- `parquet` with async/object-store support
- `object_store`

MVP does **not** depend on full Apache DataFusion. This follows the useful part of kalamdb's `kalamdb-filestore/src/parquet/reader.rs`: direct object-store Parquet streaming with projection, row-group selection, and bloom/stat checks.

DataFusion may be introduced later behind a `ColdReader` trait only if benchmarks show direct Arrow/Parquet is insufficient.

### `crates/koldstore-merge`

Single source of read resolution.

```text
src/
  resolver.rs        # hot winner vs cold winner by PK/_seq/_commit_seq
  tombstone.rs       # tombstone masks and retention decisions
  changelog.rs       # row_events -> changes_since
  quals.rs           # safe pushdown/residual classification
```

Rules:

- Default SELECT and `changes_since` use this crate.
- DML code must call tombstone helpers from this crate.
- No duplicate resolver code in `pg_koldstore/merge_scan` or `pg_koldstore/dml`.

### `crates/koldstore-catalog`

Serializable catalog row models and validation logic.

```text
src/
  schema_registry.rs
  table_meta.rs
  segments.rs
  cold_pk_hints.rs
  row_events.rs
  type_matrix.rs
```

SPI/pgrx execution stays in `pg_koldstore`; this crate owns stable model shapes and validation rules.

### `pg_koldstore`

Thin PostgreSQL integration.

```text
pg_koldstore/
  src/
    lib.rs                 # _PG_init, hooks, GUCs
    guc.rs
    hooks/
      planner.rs           # set_rel_pathlist_hook
      executor.rs          # DML guards and simple PK-delete routing
      ddl.rs               # event trigger support
      xact.rs              # transaction-scoped commit_seq lock/allocation
    sql/
      ddl.rs               # migrate, demigrate, register_storage
      dml.rs               # hydrate_pk, update_row, delete_row
      ops.rs               # flush, backup, koldstore_exec
      session.rs           # koldstore_version, koldstore_user_id, SNOWFLAKE_ID
    merge_scan/
      path.rs
      plan.rs
      exec.rs              # glue -> koldstore-merge + koldstore-parquet + FFI
    flush/
      worker.rs
      job.rs
      cleanup.rs
    migrate/
      columns.rs
      constraints.rs
      rehydrate.rs
    file/
    ffi/
  native/
    custom_scan.c
    custom_scan.h
```

Rules:

- `pg_koldstore` may use SPI, pgrx, PostgreSQL memory contexts, hooks, and unsafe FFI.
- It must delegate merge, manifest, Parquet, PK hashing, and pruning logic to pure crates.
- It must not duplicate object-store publish logic.

## Background Worker Boundary

Built-in scheduler code lives in `pg_koldstore/flush/worker.rs` because PostgreSQL background workers are extension integration. Flush job logic should be thin orchestration over:

- `koldstore-manifest` for manifest state and publish
- `koldstore-parquet` for segment writer
- `koldstore-storage` for object-store operations
- `koldstore-merge` for tombstone retention decisions

## Binary Size Strategy

| Decision | Reason |
|----------|--------|
| No DataFusion in MVP | Avoid SQL planner/optimizer dependency and duplicate Parquet pruning logic. |
| One Parquet crate for read/write | Avoid two Arrow schema conversion paths. |
| C shim only for Custom Scan API | Keep PostgreSQL internals localized. |
| Feature-gate optional codecs | Keep extension library smaller. |

## Testing Map

| Crate | Tests |
|-------|-------|
| `koldstore-core` | PK hashing, `_seq`/`_commit_seq` types, filter classification |
| `koldstore-manifest` | golden JSON, publish recovery, segment pruning |
| `koldstore-storage` | path templates, backend publish behavior |
| `koldstore-parquet` | write/read round trips, projection, row-group stats, bloom |
| `koldstore-merge` | hot-vs-cold winner, tombstones, residual qual safety, changes_since |
| `koldstore-catalog` | type matrix, FK/unique migration checks |
| `pg_koldstore` | pg_regress/pgrx tests for hooks, DML, flush, demigration |

## References

- kalamdb direct Parquet reader: `../kalamdb/backend/crates/kalamdb-filestore/src/parquet/reader.rs`
- kalamdb manifest planning: `../kalamdb/backend/crates/kalamdb-tables/src/manifest/planner.rs`
- PostgreSQL Custom Scan API: https://www.postgresql.org/docs/current/custom-scan.html
