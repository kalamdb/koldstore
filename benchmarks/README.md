# pg-koldstore benchmarks

Benchmarks compare a plain PostgreSQL heap table with an equivalent **koldstore-managed** table. Each scenario runs the same workload twice — once on heap, once after `koldstore.migrate_table` — so results isolate koldstore overhead rather than schema or query differences.

## Prerequisites

- PostgreSQL 15+ with the `koldstore` extension installed
- Object storage for cold tiers (local filesystem or MinIO via `tests/docker-compose.yml`)
- A connection string, e.g. `postgresql://postgres:postgres@localhost:5515/postgres`

```bash
docker compose -f tests/docker-compose.yml up -d
```

## Running

```bash
export DATABASE_URL=postgresql://postgres:postgres@localhost:5515/postgres

cargo run -p pg-koldstore-benchmarks -- --suite all
```

Reports are emitted as JSON (and optionally HTML) with p50/p95/p99 latency, throughput, and pass/fail verdicts.

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

Load 1M rows into two identical tables: `bench.heap_shared_items` (heap) and `bench.koldstore_shared_items` (migrated + flushed so the majority of rows live in cold storage).

**Queries measured**

| Query | What it tests |
|---|---|
| `SELECT * FROM … WHERE id = $1` | PK point lookup (hot-only vs hot+cold merge) |
| `SELECT count(*) FROM …` | Full-table aggregate over cold segments |
| `SELECT * FROM … ORDER BY created_at DESC LIMIT 100` | Range scan with sort |

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
5. **Measure** — run each query M times; record latency percentiles and ops/s.
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

Hot DML scenarios (`hot_insert_vs_heap`, `hot_update_vs_heap`, `hot_delete_vs_heap`) and cold-pruning checks live in the Rust runner alongside these table-size benchmarks. See `src/suite.rs` for the full scenario list.

## Output

Example report fragment:

```json
{
  "suite": "pg-koldstore",
  "results": [
    {
      "name": "shared_1m_pk_select_heap",
      "row_count": 1000000,
      "p50_ms": 0.12,
      "p95_ms": 0.31,
      "p99_ms": 0.58,
      "passed": true
    },
    {
      "name": "shared_1m_pk_select_koldstore",
      "row_count": 1000000,
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
