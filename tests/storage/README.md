# Storage comparison

Compares a plain PostgreSQL heap table with the same wide table under KoldStore
management.

This package is excluded from the default workspace `cargo nextest` run (same as
`e2e` / `examples`) because it needs a prepared pgrx PostgreSQL. Use
`scripts/run-storage-comparison.sh`.

Schema: [`schema.sql`](schema.sql)

Order of measurement:

1. Seed + DML on both tables
2. In async mode, time a separate mirror catch-up after each DML phase
3. Snapshot dead tuples (`pg_stat_user_tables`, pre-flush)
4. **Hot-only PK lookups before flush** (both heaps still hold all rows)
5. Flush older managed rows to zstd Parquet
6. Time `VACUUM (FULL, ANALYZE)` on both heaps, then REINDEX
7. Hot+cold PK lookups + heap/index size comparison

The insert phase alternates baseline-first and managed-first 100k-row committed
batches, accumulating only each side's execution time. This avoids fixed-order
writeback/thermal bias and bounds logical-decoding transaction memory.

Both source tables have autovacuum disabled by the schema, and the harness
applies the same benchmark-only setting to the generated mirror. A long async
catch-up therefore cannot launch maintenance during a following timed phase.
The harness runs the documented explicit maintenance phase instead.

TODO rows in the printed table (not measured yet): total PG backup size, restore time.
Autovacuum counters are not printed because autovacuum is intentionally disabled
for both source relations and the generated mirror.
```bash
# Preferred: prepare + run via the wrapper (defaults: 100k rows, 10k hot, 1k DML sample):
scripts/run-storage-comparison.sh
scripts/run-storage-comparison.sh --rows 1000000 --hot-limit 50000
scripts/run-storage-comparison.sh --rows 100000 --hot-limit 10000 --dml-sample 100000
# Opt-in committed-WAL capture; wrapper prepares the server wal_level:
scripts/run-storage-comparison.sh --rows 100000 --hot-limit 10000 --dml-sample 5000 \
  --mirror-capture-mode async

# Or prepare wal_level manually, then run the test directly. CREATE EXTENSION
# and the first async manage call create the publication and slot automatically:
# Use release-pg for fair hot+cold timings (debug is ~3–7× slower; plain --release
# uses panic=abort and breaks PostgreSQL ereport/longjmp from extension hooks):
KOLDSTORE_E2E_PREPARE_ONLY=1 scripts/run-pg-e2e.sh 16
cargo pgrx install -p pg_koldstore --profile release-pg --no-default-features --features pg16 \
  --pg-config "$(cargo pgrx info pg-config 16)"
cargo pgrx stop pg16 && cargo pgrx start pg16
KOLDSTORE_STORAGE_ROWS=100000 KOLDSTORE_STORAGE_HOT_LIMIT=10000 KOLDSTORE_STORAGE_DML_SAMPLE=1000 \
  KOLDSTORE_STORAGE_MIRROR_CAPTURE_MODE=strict \
  cargo test -p storage-comparison --test pg_vs_koldstore -- --nocapture
```

The harness prints a markdown comparison table with a **Tradeoff** column
(e.g. `3% slower`, `99% smaller`) and asserts that after flush, PostgreSQL
heap and index bytes for the managed table — **including**
`koldstore.<table>__cl` and its indexes — are smaller than the unmanaged
baseline. Progress lines are always logged for seed / flush / vacuum phases so
large runs do not look hung.

`strict` is the wrapper default and includes mirror writes in foreground DML.
`async` removes that work from the measured foreground operation and reports
`async mirror catchup after ...` as separate rows. Comparing the managed
foreground number without also publishing catch-up throughput would hide the
cost rather than move it, so benchmark reports must include both.

To make those phases reproducible, the benchmark session sets the internal
`koldstore.internal_async_mirror_worker` control to `off` before `manage_table`.
Explicit fences then apply every change in the corresponding phase. The default
is `on`; production async tables apply WAL automatically.
An untimed `CHECKPOINT` precedes each compared DML side so writeback from the
previous side is not charged to the next measurement.

Visibility after flush is checked with point lookups plus `describe_table`
hot+cold counters — not `SELECT count(*)` through `KoldMergeScan`, which still
materializes the full result set and will OOM / drop the session at multi-million
row scale.

## MinIO integration tests

Create the `koldstore-test` bucket, then run the opt-in storage tests:

```bash
bash scripts/ci/start-minio.sh
KOLDSTORE_MINIO=1 cargo test -p koldstore-storage --test storage_minio
```

Defaults are `http://127.0.0.1:9000` after `scripts/ci/start-minio.sh` (or
`http://127.0.0.1:19000` for `docker/run.sh`), `minioadmin`/`minioadmin`, and
bucket `koldstore-test`. Override them with `KOLDSTORE_MINIO_ENDPOINT`,
`KOLDSTORE_MINIO_ACCESS_KEY`, `KOLDSTORE_MINIO_SECRET_KEY`, and
`KOLDSTORE_MINIO_BUCKET`.

For full PostgreSQL flush + merge-scan coverage against MinIO, enable the same
env vars and run the E2E suite (see `docs/development.md`):

```bash
KOLDSTORE_MINIO=1 cargo nextest run -p e2e --test flush_minio --test-threads 1
```
