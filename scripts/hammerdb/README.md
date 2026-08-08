# HammerDB + KoldStore

## Weekly / readiness smoke (`scripts/hammerdb/run.sh`)

1. Build TPROC-C schema
2. Manage **HISTORY only**
3. Timed TPROC-C run
4. Flush HISTORY
5. Fail unless `EXPLAIN` shows `KoldMergeScan` with `opened≥1` on a HISTORY PK
   lookup, and customer stays on a native index plan

```bash
KOLDSTORE_HAMMERDB_WAREHOUSES=2 \
KOLDSTORE_HAMMERDB_VU=2 \
KOLDSTORE_HAMMERDB_MINUTES=2 \
  scripts/readiness/run-hammerdb.sh 16
```

Artifacts under `target/hammerdb/`.

## Deep harness (opt-in; not default CI)

`scripts/hammerdb/run-deep.sh` adds named **scale profiles**, deeper **manage
sets**, **mid-run + post-run** cold proofs, and optional **CH-benCHmark-style**
analytical queries (Citus/CH-inspired; not TPC-H certification).

```bash
scripts/hammerdb/run-deep.sh \
  --profile standard \
  --manage-set append \
  --ch after \
  16
```

| Knob | Values | Default |
| --- | --- | --- |
| `--profile` | `smoke` / `standard` / `heavy` / `custom` | `standard` |
| `--manage-set` | `history` / `append` / `broad` | `append` |
| `--ch` | `off` / `after` / `concurrent` / `only` | `after` |

Profiles (overridable via `KOLDSTORE_HAMMERDB_*`):

| Profile | WH | VU | Minutes |
| --- | ---: | ---: | ---: |
| smoke | 2 | 2 | 2 |
| standard | 10 | 8 | 10 |
| heavy | 50 | 32 | 30 |
| custom | required env | required env | required env |

GitHub: **Actions → Deep HammerDB** (`deep-hammerdb.yml`) is
`workflow_dispatch` only — never on push/PR/schedule. Weekly smoke stays on
`weekly-hammerdb.yml`.

Artifacts: `target/hammerdb-deep/summary.json` plus flush/EXPLAIN/CH JSON.

## What you can claim

**Yes (smoke):** Survived HammerDB TPROC-C with selective HISTORY manage, then
proved hot+cold merge opened Parquet after flush.

**Yes (deep):** Survived deep selective-manage TPROC-C (profile + manage set),
with mid/post-run cold proofs, and optionally completed CH-style queries.

**No:** TPC certification, “passes official TPC-C/H”, or “production safe”.

## Compare (baseline / hot_only / hot_cold)

```bash
KOLDSTORE_HAMMERDB_WAREHOUSES=2 \
KOLDSTORE_HAMMERDB_VU=4 \
KOLDSTORE_HAMMERDB_MINUTES=2 \
KOLDSTORE_HAMMERDB_READ_ITERS=200 \
  scripts/hammerdb/compare.sh 16
```

Docs: [`docs/benchmarks/hammerdb.md`](../../docs/benchmarks/hammerdb.md),
design: [`docs/plans/2026-08-08-deep-hammerdb-design.md`](../../docs/plans/2026-08-08-deep-hammerdb-design.md).
