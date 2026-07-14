# KoldStore Testing Architecture Audit (2026-07-13)

## Summary

KoldStore historically used a two-tier model: PostgreSQL-free `#[test]` / `cargo nextest` for library and extension-shell logic, and external E2E (`tests/e2e`) against a pgrx-installed extension via `tokio-postgres`. **`#[pg_test]` was not used.** **`cargo pgrx test` was documented but intentionally avoided** because native `crates/pg_koldstore/tests/*.rs` binaries failed to link against the pg-enabled library.

This audit records the pre-change state that motivated the production-readiness testing plan.

## 1. Ordinary Rust unit tests

| Location | Approx. count | Notes |
|---|---|---|
| `crates/koldstore-*` | ~327 | Domain logic; no PostgreSQL |
| `crates/pg_koldstore/tests/` (pre-move) | ~36 | Shell/contract tests with mocks (`RecordingSpiExecutor`); **not** live Postgres |
| `tests/e2e` contract `#[test]` blocks | mixed | Some harnesses mix pure contract asserts with async PG tests |
| `tests/memory`, `benchmarks` | few | Non-PG probes / Criterion |

## 2. Tests that run inside PostgreSQL

| Mechanism | Status |
|---|---|
| `#[pg_test]` / `pgrx::pg_test` | **None** (count = 0) before this work |
| External E2E (`tests/e2e`) | Primary live PG coverage via `scripts/run-pg-e2e.sh` |
| Docker packaging smoke | `docker/test.sh` only; not the default correctness loop |

## 3. Candidates to migrate to `#[pg_test]`

Single-session SQL contracts currently covered only by E2E (or not at all):

- extension load / `koldstore_version`
- `manage_table` / `describe_table` / `flush_table` / `unmanage_table`
- unsupported schemas / missing PK errors
- supported datatype + NULL roundtrip
- transaction commit vs rollback (user + mirror)
- mirror DML, tombstones, reinsert, PK mutation rejection
- GUC/session behavior
- planner hook + `EXPLAIN` custom scan
- hot-only / cold-only / mixed result values
- prepared statements / repeated execution

## 4. Must remain external E2E

- Multi-backend concurrency
- MinIO / object-store outages and rotation
- Process crash / restart recovery
- Cross-table joins at scale
- Examples and storage-comparison workloads
- pg_cron scheduled flush

## 5. Important coverage gaps (pre-plan)

- No in-server `#[pg_test]` suite
- No real crash-mid-flush with postmaster restart
- No deterministic flush+DML isolation schedules in E2E
- No SQLsmith / HammerDB / `pg_amcheck` automation
- CI had no `pull_request` trigger (main push only)
- Failpoint matrix mostly stubbed (`tests/e2e/failure_injection.rs`)

## 6. Is `cargo pgrx test` executed in CI?

**No.** CI ran `cargo pgrx install` + `cargo nextest -p e2e`. Docs explicitly advised against `cargo pgrx test` (`CONTRIBUTING.md`, `docs/development.md`).

## 7. `pg_test` Cargo feature vs `#[pg_test]` macro

| Item | Role |
|---|---|
| Feature `pg_test = []` | Standard pgrx convention; enabled by `cargo pgrx test`. Used to shim Snowflake worker id to `0` in `session.rs` so native builds need not link backend globals. |
| Attribute `#[pg_test]` | pgrx in-server test marker; unused before this work. |

**Decision:** Keep the feature name (required by pgrx). Document the dual meaning. Do not invent a second in-server framework.

## Blocker and chosen fix

`cargo pgrx test` built `crates/pg_koldstore/tests/*.rs` against the pg-enabled library and failed linking (`CopyErrorData`, etc.).

**Fix:** Move shell/contract tests to `crates/pg_koldstore-shell-tests` (`default-features = false`). Place `#[pg_test]` modules in `crates/pg_koldstore/src/pg_tests/`.

## Target layered architecture

```text
Unit tests
pgrx PostgreSQL tests
KoldStore-specific SQL regression tests
Isolation tests
E2E tests
Crash-recovery tests
Fuzz tests
Stress benchmarks
Integrity validation
```
