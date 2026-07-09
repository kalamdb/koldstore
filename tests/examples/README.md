# KoldStore Real-World Examples

Runnable integration scenarios that demonstrate how KoldStore behaves on workloads similar to production SaaS apps. These tests are **not** part of the default CI E2E matrix; run them with:

```bash
scripts/run-examples.sh
```

## Scenarios

| Example | Workload | Scope column | What it demonstrates |
|---------|----------|--------------|----------------------|
| `chat_history` | Support / chat messages | `tenant_id` | Flush policy batching, hot recent reads, cold scrollback, edit/delete overlay |
| `ai_memory` | Agent session events | `workspace_id` | Large text rows, batch flush, audit queries, compliance delete tombstones |
| `iot_telemetry` | Device sensor events | `tenant_id` | Parallel device writers, `ts`-ordered flush, late-arriving data, monthly aggregates |
| `audit_events` | Immutable audit ledger | `tenant_id` | Long-retention history, regulator export, segment metadata for proof |
| `game_events` | Player / match events | `game_id` | Tournament write spikes, wave flushing, anti-cheat cold scans |

Each scenario directory contains reference SQL (`schema.sql`, `manage_table.sql`, query files) that mirrors what the Rust test executes.

Deep coverage common to every scenario:

- multi-wave flush cycles and force flush
- small Parquet files bounded by `max_rows_per_file`, verified on disk
- `koldstore.manifest` + `cold_segments` checks including multi-tenant scopes
- application indexes
- concurrent insert / update / delete clients
- cold-then-delete overlay (flush → rematerialize hot → `DELETE` → flush delete markers → merge scan hides row, prior Parquet remains)

## Sizing

Environment variables control workload size:

| Variable | Default | Meaning |
|----------|---------|---------|
| `KOLDSTORE_EXAMPLE_ROWS` | `50000` | Total rows seeded across all scopes |
| `KOLDSTORE_EXAMPLE_CLIENTS` | `8` | Parallel PostgreSQL clients for inserts |
| `KOLDSTORE_EXAMPLE_SCOPES` | `50` | Number of tenants / workspaces / games |
| `KOLDSTORE_EXAMPLE_TIMEOUT_SECS` | `600` | Per-scenario wall-clock timeout (fail fast instead of hanging) |

Example large run:

```bash
KOLDSTORE_EXAMPLE_ROWS=200000 KOLDSTORE_EXAMPLE_CLIENTS=16 scripts/run-examples.sh
```

Run one scenario:

```bash
scripts/run-examples.sh chat_history
```

## Live progress output

`scripts/run-examples.sh` always runs with `--no-capture`, so progress prints while
each scenario is still running (same style as `tests/e2e/full_lifecycle.rs`):

- scenario sizing and cold storage root at startup
- insert milestones every 10k rows (`seed messages: 10000 / 50000 rows written`)
- step timing via `log_step` (`[timestamp] [e2e] [examples] seed ... finished in 12.3s`)
- per-query timing for flush, merge-scan PK checks, overlay INSERT/DELETE (`timed_async`)
- flush row counts and on-disk Parquet/manifest paths after each wave

Note: nextest's `success-output = "immediate"` only dumps captured logs **after** a
test finishes. Live streaming requires `--no-capture` (enabled by the script).

## Requirements

- Local pgrx-managed PostgreSQL with `koldstore` installed (the script prepares this automatically)
- `cargo-nextest`

Flush policy limits in the tests are scaled down from the production values documented in each example README so flushes complete in reasonable time while preserving the same behavior. Example Parquet files are capped at 1,000 rows/file: small enough to exercise multi-file cold reads, but not so tiny that writer setup dominates every flush.

**Notes (v1):**

- `koldstore.flush_table` evaluates `hot_row_limit` against total mirror rows for the managed table (table-wide). User-scoped tables still enforce isolation via `koldstore.user_id` / RLS.
- Deleting a previously flushed PK: rematerialize it as a hot row, `DELETE`, then flush so the mirror tombstone (`op=3`) is persisted next to cold Parquet. Pure cold-only SQL `DELETE` without rematerialize/`delete_row` is not the durable path yet.
