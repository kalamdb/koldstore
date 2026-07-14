# HammerDB + KoldStore

Three-arm compare: **baseline / hot_only / hot_cold**, with EXPLAIN proof that
hot+cold merge scan opens Parquet on the flushed arm.

## Compare (recommended)

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

## Why reads are required

TPROC-C mostly **inserts** `HISTORY`. NOPM alone does not prove cold reads.
The compare harness fails the `hot_cold` arm unless `EXPLAIN` shows
`KoldMergeScan` with `opened≥1` cold segment on a `HISTORY` PK lookup.

## Success wording

Survival without crash/panic + plan proof — never "production safe".
