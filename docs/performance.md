# Performance

## Benchmarks

Storage-comparison results, methodology, and throughput trade-offs live in
[benchmarks](benchmarks/README.md). Re-run with
`scripts/run-storage-comparison.sh`.

Additional suite:

```bash
cargo run -p pg-koldstore-benchmarks -- --suite all
```

That suite compares regular heap tables with managed tables for hot insert,
update, delete, PK select hot-only, PK select cold-required, flush throughput,
and demigration throughput. The post-flush cold PK gap (Parquet open + merge
setup vs B-tree) is the main read-path focus.

## Success Criteria

- SC-002a: managed foreground hot UPDATE throughput for small statements remains
  within 10 percent of a regular heap table under the same isolated workload.
- SC-002b: sustainable UPDATE throughput is measured with the WAL applier on;
  at the supported rate, backlog must remain bounded and drain within the
  documented SLO after load stops.
- SC-006: PK point lookups skip at least 90 percent of cold row groups.

## Priority order (accepted direction)

1. **Cold PK point lookups** — backend Parquet footer/reader cache; cold-native
   emit that skips the JSON merge path when a PK equality hits cold only.
   Dominates the hot+cold ops/s gap after flush.
2. **Managed mirror DML** — WAL-only capture keeps foreground DML on the heap;
   the applier maintains the latest-state mirror. Landed; index-layout and
   apply-batch follow-ups stay gated.
3. **Footer-derived packed catalog stats** — implemented. Finalized footer
   metadata supplies scalar segment bounds and aligned row-group arrays, so
   flush no longer computes JSON min/max per cell. PostgreSQL performs scalar
   candidate lookup and Rust refines row groups before object access.
   See [ADR-002](decisions/002-footer-derived-catalog-stats.md).
4. Segment sizing / page indexes / streaming merge polish — secondary levers
   once (1) lands.

Tracked on the [roadmap](roadmap.md).

## Tracing

Important span families are SQL API calls, DML hook work, flush phases, cold
reader pruning, merge execution, and object-store I/O.

Use `EXPLAIN (ANALYZE)` on managed SELECTs and inspect KoldMergeScan properties
(`Parquet segment` `read_ms`, row-group selection, bloom mode, PK probe) to
separate footer-open cost from merge/SPI overhead.

## Investigation Workflow

Start with heap baseline comparison, then inspect PostgreSQL plans, row-group
pruning, manifest state, object-store timing, RSS, and allocation counters. Use
heaptrack output and PostgreSQL memory-context snapshots when repeated scans or
flushes grow memory over time.

## Memory and small machines

KoldStore is meant to stay **lightweight on small hosts**. Flushing millions of
rows should not require hundreds of MiB of extension heap: the encode path is
streaming, and peak RSS is dominated by **one in-flight Parquet segment**, not
by the total number of rows moved cold.

### What holds memory

| Component | Typical size | Releases when idle? |
|-----------|--------------|---------------------|
| PostgreSQL `shared_buffers` | Often **128 MB** in Docker / default images | **No** — fixed for the life of the postmaster |
| Flush encode (Rust/Arrow/Parquet) | **O(`max_rows_per_file` × row width)** | Yes after the segment uploads (glibc may retain arenas until trim / backend exit) |
| SPI mirror page | ≤ **4096** decoded rows | Per page |
| Arrow row group | **1024** rows (writer default) | After each row group flush |
| Async apply staging | ≤ **8192** rows per batch | Per batch |
| Merge-scan seen-key set | Up to `koldstore.max_merge_seen_keys` (default 1M) | Per scan (fail-closed when exceeded) |

Docker/`ps` RSS that looks like “~200 MiB and not freeing” after a flush workload
is usually **`shared_buffers` still mapped**, plus a small private anon heap —
not an unbounded flush of the full table. Process RSS also **double-counts**
shared memory across backends; use proportional set size / cgroup `anon` +
`shmem` when diagnosing.

Custom global allocators (jemalloc / mimalloc) do **not** shrink
`shared_buffers` and are a poor fit as a process-wide override inside a
PostgreSQL extension. Prefer bounding segment size and Postgres GUCs.

### Flush peak is O(file), not O(millions)

Pipeline shape:

```text
SPI page (≤4096) → Arrow row group (1024) → compress into one segment buffer
  → upload → drop bytes → next segment …
```

So peak extension memory scales with **`max_rows_per_file`** (and row width /
compression overhead), repeated across passes. Product defaults keep that small:

| Knob | Default | Role |
|------|---------|------|
| `max_rows_per_file` | **1000** | Closes each Parquet segment (dominant spike) |
| `max_rows_per_flush` | **10 000** | Rows per flush pass |
| `koldstore.max_parallel_flush_jobs` | **2** | Concurrent encode spikes (use **1** on tiny boxes) |
| `koldstore.async_apply_max_rows_per_tick` | **0** (unlimited) | Cap apply drain on small machines |
| `koldstore.async_apply_max_ms_per_tick` | **0** (unlimited) | Cap apply wall time per tick |

Raising file size for fewer objects trades RAM for I/O convenience. Published
storage benches with `max_rows_per_file = 1_000_000` measured **~816 MiB** peak
RSS during flush — that is expected for huge segments, not the small-machine
contract. Demo scripts that set `max_rows_per_file` to hundreds of thousands are
throughput demos, **not** recommended defaults for low-RAM hosts.

### Recommended posture for small machines

1. Keep **`max_rows_per_file` at 1000–5000** (product default is 1000). Do not
   copy large demo/bench file sizes.
2. Keep or lower **`max_rows_per_flush`** (≤10k; try 2k–5k if RSS is still high).
3. **`SET koldstore.max_parallel_flush_jobs = 1`** so only one encode spike runs.
4. Cap async apply, for example via `ALTER DATABASE … SET`:
   `koldstore.async_apply_max_rows_per_tick = 8192` and
   `koldstore.async_apply_max_ms_per_tick = 1000` (session `SET` is not enough
   for background workers).
5. Lower **`shared_buffers`** on tiny demo containers (for example 32–64 MB);
   otherwise idle Docker RSS stays ~150–250 MiB even with a quiet extension.
6. Keep **`koldstore.max_merge_seen_keys`** fail-closed; do not set `0` on small
   RAM. Lower **`koldstore.max_open_parquet_readers`** (for example 4–8) if cold
   scans compete with flush.
7. Prefer narrower hot columns; wide `text`/`jsonb` inflates each SPI page and
   the open segment buffer.

Rough host budget: Postgres `shared_buffers` + **one** flush executor spike
(tens of MiB at defaults; much more with large files) + persistent WAL applier +
OS. See also [flushing architecture](architecture/flushing-table.md#memory-bounds)
and the manage-time options in [SQL API](sql-api.md#advanced-and-compatibility-management-koldstoremanage_table).
