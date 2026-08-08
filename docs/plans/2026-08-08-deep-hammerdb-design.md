# Deep HammerDB / TPROC-C + CH Design

Date: 2026-08-08

## Goals

Add an **opt-in deep** HammerDB harness that:

1. Selects **named scale profiles** (`smoke` / `standard` / `heavy` / `custom`)
2. Applies a **manage set** deeper than HISTORY-only (`history` / `append` / `broad`)
3. Proves **mid-run and post-run** flush + cold Parquet open under selective manage
4. Optionally runs **CH-benCHmark-style** analytical queries (`off` / `after` / `concurrent` / `only`)
5. Remains **out of default CI** (no push/PR/schedule); weekly smoke stays unchanged

## Non-goals

- TPC certification or “official TPC-C/H” claims
- Replacing storage-comparison RESULTS or Criterion microbenches
- Changing weekly HammerDB defaults (`2 WH / 2 VU / 2 min`, HISTORY-only)
- Requiring every deep table to manage successfully (`broad` may skip tables that fail PK/type prerequisites — logged, not silent)

## Decisions

| Topic | Choice |
| --- | --- |
| Depth target | D: deeper manage + mid-run proofs + CH modes |
| Default CI | Unchanged weekly smoke |
| Deep CI | `workflow_dispatch` only |
| Scale | Named profiles + full custom knobs |
| Claim wording | Survived deep selective-manage TPROC-C (+ optional CH), proved cold open — never TPC-certified |

## Scale profiles

| Profile | Warehouses | VU | Duration | Read iters | Intent |
| --- | ---: | ---: | ---: | ---: | --- |
| `smoke` | 2 | 2 | 2 min | 50 | Match weekly |
| `standard` | 10 | 8 | 10 min | 200 | Local / dispatch default |
| `heavy` | 50 | 32 | 30 min | 200 | Stress / publish candidate |
| `custom` | env/flags | … | … | … | Full manual control |

## Manage sets

| Set | Managed tables | Notes |
| --- | --- | --- |
| `history` | `history` | Same as weekly smoke |
| `append` | `history` + `order_line` (best-effort) | Default for deep |
| `broad` | `append` + `orders` (best-effort) | Stress / publish |

Unmanaged OLTP tables (`customer`, `stock`, …) must keep native plans.

## CH modes

| Mode | Behavior |
| --- | --- |
| `off` | TPROC-C + KoldStore proofs only |
| `after` | CH 22 queries after OLTP (+ flush) — deep default |
| `concurrent` | CH during timed TPROC-C |
| `only` | Skip timed TPROC-C; CH against built+managed schema |

CH needs TPC-C plus `region` / `nation` / `supplier` (CH-benCHmark extension tables). Seed a query-compatible dataset (Europe/Germany/Cambodia + 10k suppliers for non-smoke).

## Architecture

```
scripts/hammerdb/
  profiles.sh           # resolve profile → WH/VU/duration/iters
  manage_policy.sql     # manage set application + type/PK prep
  ch_schema.sql         # region/nation/supplier
  ch_queries.py         # 22 CH-benCHmark-style queries
  ch_runner.py          # after / concurrent / only runner
  proofs.py             # flush + EXPLAIN / cold-open gates
  run-deep.sh           # deep entrypoint
  run.sh                # weekly smoke (unchanged contract)
.github/workflows/
  deep-hammerdb.yml     # workflow_dispatch only
  weekly-hammerdb.yml   # unchanged smoke
```

### Deep run flow

1. Resolve profile + manage_set + ch_mode
2. Prepare pgrx cluster / DB; build TPROC-C at profile scale
3. Apply `manage_policy.sql`; require at least `history` managed
4. If `ch_mode != off`: install CH extension tables
5. If `ch_mode != only`: start timed TPROC-C
   - Mid-run (after rampup + ~half duration): flush managed tables → prove cold open
   - If `concurrent`: run CH alongside OLTP
6. If `ch_mode == after` or post-OLTP needed: flush → CH after
7. Post-run proofs + `summary.json`

## Proof gates (fail closed)

1. Required managed tables stay managed through the run
2. Mid-run and/or post-run flush produces ≥1 active cold segment for proved tables
3. Managed PK `EXPLAIN` shows `KoldMergeScan` with `opened ≥ 1` after flush
4. Unmanaged `customer` never uses `KoldMergeScan`
5. CH modes that run queries: all scheduled queries succeed (latencies recorded)

## Dispatch workflow inputs

| Input | Values | Default |
| --- | --- | --- |
| `profile` | smoke / standard / heavy / custom | `standard` |
| `manage_set` | history / append / broad | `append` |
| `ch_mode` | off / after / concurrent / only | `after` |
| `pg_version` | 16 | `16` |
| custom WH/VU/minutes | when `profile=custom` | — |

## Claim wording

**Allowed:** “KoldStore survived deep selective-manage HammerDB TPROC-C (profile X, manage set Y), proved mid/post-run cold Parquet open, and optionally completed CH-benCHmark-style queries.”

**Not allowed:** TPC-certified, official TPC-C/H, production-safe.

## Inspiration

- Citus `citus-benchmark`: named driver, TPROC-C ± CH flags, scale via warehouses/VU
- PostgresPro CH-benchmark: explicit OLTP/CH toggles
- Hydra: ClickBench for columnar OLAP (not used here; storage RESULTS already cover footprint)
