# HammerDB + KoldStore

Weekly / readiness path (`scripts/hammerdb/run.sh`):

1. Build TPROC-C schema
2. Manage **HISTORY only**
3. Timed TPROC-C run
4. Flush HISTORY
5. Fail unless `EXPLAIN` shows `KoldMergeScan` with `opened≥1` on a HISTORY PK
   lookup, and customer stays on a native index plan

## What you can claim

**Yes:** KoldStore survived HammerDB TPROC-C with selective HISTORY manage, then
proved hot+cold merge opened Parquet after flush.

**No:** TPC certification, “passes official TPC-C”, or “production safe”.
TPROC-C mostly inserts HISTORY; NOPM alone does not prove cold reads — the
post-run flush + EXPLAIN gate is what makes the claim honest.

## Weekly / readiness

```bash
KOLDSTORE_HAMMERDB_WAREHOUSES=2 \
KOLDSTORE_HAMMERDB_VU=2 \
KOLDSTORE_HAMMERDB_MINUTES=2 \
  scripts/readiness/run-hammerdb.sh 16
```

Artifacts under `target/hammerdb/`: `hammerdb.log`, `flush.log`,
`explain_post_run.txt`, `reads_post_run.json`, `summary.json`.

## Compare (baseline / hot_only / hot_cold)

```bash
KOLDSTORE_HAMMERDB_WAREHOUSES=2 \
KOLDSTORE_HAMMERDB_VU=4 \
KOLDSTORE_HAMMERDB_MINUTES=2 \
KOLDSTORE_HAMMERDB_READ_ITERS=200 \
  scripts/hammerdb/compare.sh 16
```

Writes:

- `target/hammerdb/compare/results.json`
- `target/hammerdb/compare/explain_*.txt`
- `docs/benchmarks/assets/hammerdb-{nopm,history-reads,customer-reads}.svg`

Docs: [`docs/benchmarks/hammerdb.md`](../../docs/benchmarks/hammerdb.md).
