# KoldStore SQL regression

KoldStore-specific SQL contracts exercised against a pgrx-managed PostgreSQL
cluster via `scripts/run-sql-regression.sh`.

These tests intentionally do **not** replicate PostgreSQL's upstream regression
suite. They cover managed-table DDL/DML, hot+cold SELECT, empty/one-row edge
cases, and before-flush vs after-flush result equality.

## Layout

| Path | Purpose |
|---|---|
| `*.sql` | Input scripts (run with `ON_ERROR_STOP`) |
| `expected/*.out` | Normalized expected `psql` output |
| `setup.sql` | Shared fixture: storage + schemas + test GUCs (same psql session as each case) |

## Normalization rules

Unstable fields are stripped or rewritten before comparison:

1. **OIDs** — any bare OID / `oid=` / `regclass` numeric form → `<OID>`
2. **Absolute paths** — filesystem or object-store paths under temp roots → `<PATH>`
3. **Planner costs** — `cost=N..N` / `actual time=` / `rows=` timing noise → `<COST>` / `<TIME>` / `<ROWS>`
4. **Timestamps** — ISO timestamps and `now()`-derived values → `<TS>`
5. **UUIDs / job ids** — flush job identifiers → `<UUID>`
6. **Whitespace** — trailing spaces removed; blank lines collapsed

Do not assert on EXPLAIN costs, wall-clock timings, or storage absolute paths.
Assert on row values, counts, and KoldStore catalog status strings.

## Running

```bash
scripts/run-sql-regression.sh 16
```

Environment mirrors the E2E runner (`KOLDSTORE_E2E_PGPORT`, `KOLDSTORE_E2E_PGHOST`, …).
Set `KOLDSTORE_SQL_REGRESSION_UPDATE=1` to refresh expected outputs after an intentional change.
