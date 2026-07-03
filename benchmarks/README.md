# pg-koldstore benchmarks

Benchmarks compare a plain PostgreSQL heap table with an equivalent **koldstore-managed** table. Each scenario runs the same workload twice — once on heap, once after `koldstore.migrate_table` — so results isolate koldstore overhead rather than schema or query differences.

## Prerequisites

- Local pgrx PostgreSQL with the `koldstore` extension installed
- `pgbench` available on `PATH`
- A connection string, e.g. `host=127.0.0.1 port=28816 user=$USER dbname=postgres`

```bash
cargo pgrx start pg16
cargo pgrx install -p pg_koldstore --no-default-features --features pg16 \
  --pg-config "$(cargo pgrx info pg-config 16)"
```

## Running

```bash
export DATABASE_URL="host=127.0.0.1 port=28816 user=$USER dbname=postgres"

cargo run -p pg-koldstore-benchmarks -- \
  --rows 100000 \
  --clients 16 \
  --jobs 4 \
  --seconds 30 \
  --output-json target/pg-koldstore-bench.json \
  --output-html target/pg-koldstore-bench.html
```

The Rust runner creates heap and koldstore tables, migrates the koldstore table, writes custom `pgbench` scripts, parses per-transaction pgbench logs, and emits JSON/HTML with p50/p95/p99 latency, throughput, and pass/fail verdicts.

## Scenarios

### 1. Shared table — 1 million rows

Models app-wide data (audit logs, catalog history) where every query may scan the full table.

| | Heap baseline | koldstore managed |
|---|---|---|
| Table type | plain `CREATE TABLE` | `table_type => 'shared'` |
| Row count | **1,000,000** | **1,000,000** (same fixture) |
| Cold layout | n/a | `{namespace}/{tableName}/` |

**Fixture**

```sql
CREATE TABLE bench.shared_items (
  id         bigint PRIMARY KEY,
  body       text   NOT NULL,
  value      bigint NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
```

Load rows into two identical tables: `bench.heap_items` (heap) and `bench.koldstore_items` (migrated). Use `--rows 1000000` for a 1M-row high-load run.

**Queries measured**

| Query | What it tests |
|---|---|
| `SELECT * FROM … WHERE id = $1` | PK point lookup (hot-only vs hot+cold merge) |
| `UPDATE … WHERE id = $1` | Hot-row DML overhead |
| `INSERT … ON CONFLICT DO NOTHING` | Hot insert throughput under contention |

Each query runs against both tables. The report records select time (p50/p95/p99) and compares koldstore latency to the heap baseline.

### 2. User table — scoped SELECT by user

Models multi-tenant data where queries filter on a scope column (e.g. `user_id`). Cold files fan out per user under `{namespace}/{tableName}/{scopeId}/`.

| | Heap baseline | koldstore managed |
|---|---|---|
| Table type | plain `CREATE TABLE` | `table_type => 'user'`, `scope_column => 'user_id'` |
| Users | **x** (configurable, default 1,000) | **x** (same distribution) |
| Rows per user | configurable (default 1,000) | configurable (default 1,000) |
| Total rows | x × rows-per-user | x × rows-per-user |

**Fixture**

```sql
CREATE TABLE bench.user_events (
  id         bigint PRIMARY KEY,
  user_id    uuid   NOT NULL,
  body       text   NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- koldstore variant only:
SELECT koldstore.migrate_table(
  table_name   => 'bench.koldstore_user_events',
  table_type   => 'user',
  storage_name => 'local-minio',
  flush_policy => 'rows:500,interval:60',
  scope_column => 'user_id'
);
```

Seed **x** users with a skewed or uniform row distribution, flush so each user's cold prefix holds archived rows, then measure scoped SELECT time:

```sql
SET koldstore.user_id = '<target-user-uuid>';

SELECT * FROM bench.koldstore_user_events
 WHERE user_id = '<target-user-uuid>'
 ORDER BY created_at DESC
 LIMIT 100;
```

The heap baseline runs the same query without the GUC. This shows whether per-user cold layout keeps scoped reads competitive with a single heap table at scale.

**Tuning x**

| Users (x) | Use case |
|---|---|
| 100 | smoke / CI |
| 1,000 | default local run |
| 10,000+ | stress per-user cold prefix layout |

Pass `--users <x>` (when wired) to override the default.

## Comparison methodology

Every scenario follows the same pattern:

1. **Setup** — create identical schemas for heap and koldstore tables.
2. **Load** — insert the same deterministic dataset into both.
3. **Cold tier** (koldstore only) — register storage, migrate, flush until the target cold/hot ratio is reached.
4. **Warm-up** — discard the first N iterations.
5. **Measure** — run each query through `pgbench`; record latency percentiles and ops/s.
6. **Report** — emit side-by-side results with overhead ratio `koldstore / heap`.

```
┌─────────────────────┐     ┌──────────────────────────┐
│  bench.heap_*       │     │  bench.koldstore_*       │
│  (plain heap)       │     │  (migrate + flush)       │
└─────────┬───────────┘     └────────────┬─────────────┘
          │                              │
          └──────────┬───────────────────┘
                     ▼
            same queries, same row counts
                     ▼
           JSON / HTML benchmark report
```

## Success criteria

| ID | Scenario | Threshold |
|---|---|---|
| SC-002 | Hot DML (insert / update / delete on hot rows only) | koldstore ≤ 10% slower than heap |
| SC-006 | PK lookup with cold data present | ≥ 90% of row groups pruned |
| — | Shared 1M SELECT | document overhead; no hard gate yet |
| — | User scoped SELECT | document overhead; no hard gate yet |

Hot DML scenarios (`hot_insert_vs_heap`, `hot_update_vs_heap`) and hot PK select workloads run through `pgbench`. Cold-pruning checks remain modeled in Rust until the cold merge scan path is fully wired into live PostgreSQL execution.

## Output

Example report fragment:

```json
{
  "suite": "pg-koldstore",
  "results": [
    {
      "name": "hot_update_vs_heap_heap",
      "row_count": 100000,
      "p50_ms": 0.12,
      "p95_ms": 0.31,
      "p99_ms": 0.58,
      "passed": true
    },
    {
      "name": "hot_update_vs_heap_koldstore",
      "row_count": 100000,
      "p50_ms": 0.15,
      "p95_ms": 0.38,
      "p99_ms": 0.72,
      "passed": true
    }
  ]
}
```

Overhead for a scenario: `koldstore.p50_ms / heap.p50_ms`.

## Related docs

- [Performance](../docs/performance.md) — tracing and investigation workflow
- [Development](../docs/development.md) — local PostgreSQL matrix and MinIO setup
- [README](../README.md) — shared vs user table semantics
