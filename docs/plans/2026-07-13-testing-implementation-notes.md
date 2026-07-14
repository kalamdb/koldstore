# Production-readiness testing — implementation notes (2026-07-13)

## Audit (original system)

See [2026-07-13-testing-architecture-audit.md](./2026-07-13-testing-architecture-audit.md).

## Tests converted to `#[pg_test]`

New in-server suite under `crates/pg_koldstore/src/pg_tests/` (15 tests), including:

- extension / snowflake / GUC surface
- manage / describe / flush / unmanage value checks (`before flush == after flush`)
- unsupported schema / missing PK (expected panic)
- datatype + NULL roundtrip
- mirror DML + PK mutation rejection
- EXPLAIN custom scan, hot/mixed results, prepared statements
- failpoint GUC default empty

## Intentionally left as unit / E2E

- `crates/pg_koldstore-shell-tests` — former `pg_koldstore/tests/*` shell contracts (mocks, no live PG)
- All `koldstore-*` crate `#[test]` domain logic
- `tests/e2e` — multi-session, MinIO, joins, lifecycle, isolation, crash recovery
- Examples / storage-comparison / benchmarks

## New files / scripts (high level)

| Area | Paths |
|---|---|
| Audit | `docs/plans/2026-07-13-testing-architecture-audit.md` |
| Shell tests crate | `crates/pg_koldstore-shell-tests/` |
| `#[pg_test]` | `crates/pg_koldstore/src/pg_tests/` |
| Failpoints | `crates/pg_koldstore/src/failpoints.rs` + flush instrumentation |
| SQL regression | `tests/sql/`, `scripts/run-sql-regression.sh` |
| Equality | `tests/e2e/common/equality.rs`, `tests/e2e/equality/` |
| Isolation | `tests/e2e/isolation/`, `scripts/readiness/run-isolation.sh` |
| Crash | `tests/e2e/crash/`, `scripts/readiness/run-crash-recovery.sh` |
| SQLsmith | `scripts/sqlsmith/`, `scripts/readiness/run-sqlsmith.sh`, CI install via `scripts/ci/install-sqlsmith.sh` |
| Integrity | `scripts/readiness/run-integrity-checks.sh` |
| Upstream PG | `scripts/readiness/run-upstream-pg-regress.sh`, `.github/workflows/upstream-pg-regress.yml` |
| HammerDB / report | `scripts/hammerdb/`, `scripts/readiness/run-hammerdb.sh`, `scripts/readiness/run-readiness-report.sh`, weekly `.github/workflows/weekly-hammerdb.yml` |
| CI | `.github/workflows/ci-tests.yml` (PG 15–18: `cargo pgrx test`, E2E, examples, SQL regression, storage), `.github/workflows/nightly-readiness.yml` |

## Local commands

Documented in `CONTRIBUTING.md` and `docs/development.md`.

## Remaining gaps / unjustified claims

- Full crash-point matrix and long HammerDB/SQLsmith still need tool installs + nightly runtime evidence
- `koldstore_user_id()` SQL helper remains a stub (GUC is tested)
- Nested SQL `BEGIN`/`SAVEPOINT` cannot run under the pgrx test transaction wrapper; rollback covered in E2E
- Do **not** claim “production safe”; use readiness-report wording only after gates pass under documented configs
