# Scripts layout

Everyday runners stay at the repo `scripts/` root. Helpers and specialized
suites live in subfolders so the top level stays scannable.

## Top level (CI + local day-to-day)

| Script | Purpose |
|--------|---------|
| `run-pg-e2e.sh` | Prepare pgrx cluster + run `tests/e2e` in `--mode strict` or `--mode async` |
| `run-pgrx-bench.sh` | In-process `#[pg_bench]` timings inside a live backend (`cargo pgrx bench`) |
| `run-examples.sh` | Real-world example scenarios (`--mode strict|async`) |
| `run-chat-penetration.sh` | Manual chat penetration soak (`tests/stress`, configurable minutes/packs) |
| `run-storage-comparison.sh` | Heap vs managed storage comparison (`--all-sides` or `--side pg|async|strict`) |
| `run-sql-regression.sh` | KoldStore SQL regression (`tests/sql/`) |
| `run-all-tests.sh` | Full local aggregator: unit, `#[pg_test]`, E2E strict+async, examples, storage, SQL, memory, benchmarks |
| `run-pgrx-matrix.sh` | Multi-PG local matrix |
| `find-rust-duplicates.py` | Exact normalized Rust block duplicates (`--deny` is opt-in) |

## `scripts/ci/`

GitHub Actions helpers (apt source cleanup, MinIO bootstrap, Toxiproxy+MinIO).
Not for local day-to-day use. Network-fault E2E: `scripts/ci/start-toxiproxy.sh`.

## `scripts/readiness/`

Optional / nightly / RC gates. Isolation and crash wrappers prepare the cluster
then run a focused E2E binary; SQLsmith and HammerDB skip when tools are missing.

| Script | Purpose |
|--------|---------|
| `run-isolation.sh` | Multi-session isolation schedules |
| `run-crash-recovery.sh` | Flush failpoint crash recovery |
| `run-postmaster-restart.sh` | Real `pg_ctl -m immediate` mid-flush recovery |
| `run-integrity-checks.sh` | `pg_amcheck` + catalog integrity SQL |
| `run-sqlsmith.sh` | → `scripts/sqlsmith/run.sh` |
| `run-differential-sqlsmith.sh` | → `scripts/differential/run-sqlsmith-compare.sh` |
| `run-hammerdb.sh` | → `scripts/hammerdb/run.sh` |
| `run-upstream-pg-regress.sh` | External PG `installcheck` signal |
| `run-readiness-report.sh` | JSON/Markdown readiness report |
| `run-test-with-cron.sh` | Manual `pg_cron` flush smoke |

## `scripts/sqlsmith/` / `scripts/differential/` / `scripts/hammerdb/`

Tool-specific assets (setup SQL, TCL templates, real runners). Differential
compare reuses external SQLsmith rather than vendoring a query corpus.

HammerDB compare (baseline vs HISTORY-only manage + SVG charts):

```bash
scripts/hammerdb/compare.sh 16
```

Docs: `docs/benchmarks/hammerdb.md`.

## `scripts/build/`

Release packaging (`linux.sh`, `macos-arm64.sh`, `windows.ps1`, `release-common.sh`,
`release-version.py`) and demo GIF generation.
