# Production-readiness gate report (template)

> Wording rule: **never** claim “production safe” merely because gates passed.
> Approved summary when all implemented gates pass:
>
> > All currently implemented production-readiness gates passed for PostgreSQL
> > {{PG_VERSIONS}} under the documented test configurations.

## Summary

| Field | Value |
|---|---|
| Test category | {{TEST_CATEGORY}} |
| PostgreSQL version | {{PG_VERSION}} |
| Passed/failed | {{PASSED}} |
| Duration | {{DURATION}} |
| Seed (where relevant) | {{SEED}} |
| Crash/restart count | {{CRASH_RESTART_COUNT}} |
| Rows compared | {{ROWS_COMPARED}} |
| Segments checked | {{SEGMENTS_CHECKED}} |
| pg_amcheck result | {{AMCHECK_RESULT}} |
| Artifact/log locations | {{ARTIFACT_PATHS}} |
| Known exclusions | {{KNOWN_EXCLUSIONS}} |

## Gate results

| Layer | Command | Result |
|---|---|---|
| Unit | `cargo nextest … --no-default-features` | {{UNIT_RESULT}} |
| pgrx `#[pg_test]` | `cargo pgrx test pgN` | {{PG_TEST_RESULT}} |
| SQL regression | `scripts/run-sql-regression.sh` | {{SQLREG_RESULT}} |
| Isolation | `scripts/readiness/run-isolation.sh` | {{ISOLATION_RESULT}} |
| Crash/failpoints | `scripts/readiness/run-crash-recovery.sh` | {{CRASH_RESULT}} |
| SQLsmith | `scripts/readiness/run-sqlsmith.sh` | {{SQLSMITH_RESULT}} |
| Integrity | `scripts/readiness/run-integrity-checks.sh` | {{INTEGRITY_RESULT}} |
| HammerDB | `scripts/readiness/run-hammerdb.sh` | {{HAMMER_RESULT}} |

## Remaining unsupported / untested risks

{{REMAINING_RISKS}}

## Machine-readable companion

See the JSON emitted by `scripts/readiness/run-readiness-report.sh` (same fields).
