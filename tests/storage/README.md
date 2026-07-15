# Storage comparison

Compares a plain PostgreSQL heap table with the same wide table under KoldStore
management.

This package is excluded from the default workspace `cargo nextest` run (same as
`e2e` / `examples`) because it needs a prepared pgrx PostgreSQL. Use
`scripts/run-storage-comparison.sh`.

Schema: [`schema.sql`](schema.sql)

Order of measurement:

1. Seed + DML on both tables
2. Snapshot dead tuples (`pg_stat_user_tables`, pre-flush)
3. **Hot-only PK lookups before flush** (both heaps still hold all rows)
4. Flush older managed rows to zstd Parquet
5. Time `VACUUM (FULL, ANALYZE)` on both heaps, then REINDEX
6. Hot+cold PK lookups + heap/index size comparison

TODO rows in the printed table (not measured yet): total PG backup size, restore time.
Autovacuum counters are not printed: this harness is too short for autovacuum to run.
```bash
# Preferred: prepare + run via the wrapper (defaults: 100k rows, 10k hot, 1k DML sample):
scripts/run-storage-comparison.sh
scripts/run-storage-comparison.sh --rows 1000000 --hot-limit 50000
scripts/run-storage-comparison.sh --rows 100000 --hot-limit 10000 --dml-sample 100000

# Or prepare manually, then run the test directly:
# Use release-pg for fair hot+cold timings (debug is ~3–7× slower; plain --release
# uses panic=abort and breaks PostgreSQL ereport/longjmp from extension hooks):
KOLDSTORE_E2E_PREPARE_ONLY=1 scripts/run-pg-e2e.sh 16
cargo pgrx install -p pg_koldstore --profile release-pg --no-default-features --features pg16 \
  --pg-config "$(cargo pgrx info pg-config 16)"
cargo pgrx stop pg16 && cargo pgrx start pg16
KOLDSTORE_STORAGE_ROWS=100000 KOLDSTORE_STORAGE_HOT_LIMIT=10000 KOLDSTORE_STORAGE_DML_SAMPLE=1000 \
  cargo test -p storage-comparison --test pg_vs_koldstore -- --nocapture
```

The harness prints a markdown comparison table with a **Tradeoff** column
(e.g. `35% slower`, `99% smaller`) and asserts that after flush, PostgreSQL
heap and index bytes for the managed table — **including**
`koldstore.<table>__cl` and its indexes — are smaller than the unmanaged
baseline. Progress lines are always logged for seed / flush / vacuum phases so
large runs do not look hung.

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
