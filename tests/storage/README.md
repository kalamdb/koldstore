# Storage comparison

Compares a plain PostgreSQL heap table with the same wide table under KoldStore
management.

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
# Requires a running pgrx server with koldstore installed.
# Use a --release extension for fair hot+cold timings (debug builds are ~3–7× slower):
KOLDSTORE_E2E_PREPARE_ONLY=1 scripts/run-pg-e2e.sh 16
cargo pgrx install -p pg_koldstore --release --no-default-features --features pg16 \
  --pg-config "$(cargo pgrx info pg-config 16)"
cargo pgrx stop pg16 && cargo pgrx start pg16

# Default / README table scale (100k rows, 10k hot):
KOLDSTORE_STORAGE_ROWS=100000 \
KOLDSTORE_STORAGE_HOT_LIMIT=10000 \
cargo test -p storage-comparison --test pg_vs_koldstore -- --nocapture
```

The harness prints a markdown comparison table and asserts that after flush,
PostgreSQL heap and index bytes for the managed table are smaller than the
unmanaged baseline.

## MinIO integration tests

Create the `koldstore-test` bucket, then run the opt-in storage tests:

```bash
KOLDSTORE_MINIO=1 cargo test -p koldstore-storage --test storage_minio
```

Defaults are `http://127.0.0.1:19000`, `minioadmin`/`minioadmin`, and bucket
`koldstore-test`. Override them with `KOLDSTORE_MINIO_ENDPOINT`,
`KOLDSTORE_MINIO_ACCESS_KEY`, `KOLDSTORE_MINIO_SECRET_KEY`, and
`KOLDSTORE_MINIO_BUCKET`.
