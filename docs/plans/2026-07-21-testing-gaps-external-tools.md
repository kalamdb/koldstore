# Testing gaps: external-tool first (2026-07-21)

## Principle

Prefer thin in-repo adapters that **install and run** upstream tools over
vendoring corpora or rewriting PostgreSQL’s own regress/isolation frameworks.

| Gap | In-repo | External |
|-----|---------|----------|
| SQL regression | `tests/sql/{lifecycle,dml,query_semantics,errors}.sql` | Shape inspired by PostgreSQL `pg_regress` |
| Differential | `tests/e2e/equality/three_state_equality.rs` | SQLsmith via `scripts/differential/run-sqlsmith-compare.sh` |
| Isolation | `tests/e2e/isolation/schedules.rs` | (KS-specific; no `pg_isolation_regress`) |
| Crash restart | `tests/e2e/crash/postmaster_restart.rs` | PostgreSQL `pg_ctl -m immediate` |
| Network faults | `tests/e2e/suite/failure_injection.rs` | Toxiproxy Docker (`scripts/ci/start-toxiproxy.sh`) + MinIO |
| Extension upgrade | `tests/e2e/suite/extension_upgrade.rs` | PostgreSQL `ALTER EXTENSION UPDATE` (`pg_upgrade` deferred) |

## Skip / pin rules

- SQLsmith / differential: skip if `sqlsmith` missing; CI installs via
  `scripts/ci/install-sqlsmith.sh`.
- Toxiproxy / MinIO faults: skip unless `KOLDSTORE_TOXIPROXY=1` /
  `KOLDSTORE_MINIO=1`.
- Postmaster restart: skip unless `KOLDSTORE_CRASH_POSTMASTER_RESTART=1`
  (stops the shared cluster; run serial).

## Fixture naming note

Change-log / mirror relations are derived from the **unqualified** table name.
E2E fixtures that share a pooled worker DB must use unique relnames (not only
unique schemas), or `__cl` mirrors collide across leftover fixtures.

## Commands

```bash
scripts/run-sql-regression.sh 16
scripts/readiness/run-differential-sqlsmith.sh 16
scripts/readiness/run-isolation.sh 16
scripts/readiness/run-crash-recovery.sh 16
scripts/readiness/run-postmaster-restart.sh 16
# Optional network faults:
#   scripts/ci/start-toxiproxy.sh
#   cargo nextest run -p e2e -E 'test(failure_injection::)'
```
