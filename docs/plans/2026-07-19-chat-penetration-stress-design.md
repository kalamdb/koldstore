# Chat Penetration Stress Design

Date: 2026-07-19

## Goals

Add a **manual CI penetration harness** that stress-tests PostgreSQL + `pg_koldstore` under a chat-like workload adapted from KalamDB `benchv2` `chat_realtime_bench`, plus continuous queries of **old chat history**.

The system must hold under:

1. Heavy parallel writers inserting wide rows
2. Aggressive flush to cold (many small Parquet files)
3. Concurrent history readers merging hot + cold
4. Elevated open-file / FD / storage pressure
5. Latency that stays within a **same-run baseline × multiplier**

Optionally (via **feature packs**), the same timed soak also exercises other koldstore surfaces: cold overlays, multi-table flush, async mirror, joins, mid-soak schema evolution, scheduler-driven flush, and S3 storage.

## Non-goals

- KalamDB USER tables, TOPICS, STREAM typing, or live subscription forwarders (map to SQL-only managed tables).
- Running on every PR or on a schedule (manual `workflow_dispatch` only).
- Replacing HammerDB / Criterion / `tests/examples/chat_history` / nightly crash+SQLsmith (those stay).
- Kitchen-sink always-on load that makes failures hard to bisect (packs default to `chat` only).
- Product changes unless the harness exposes a real bug (then fix the extension, do not weaken the test).

## Decisions

| Topic | Choice |
|-------|--------|
| Fail criteria | Correctness + resource signals + latency gates |
| Latency baseline | Same CI run: short low-concurrency probe, then soak |
| Package | New `tests/stress` (not in workspace `default-members`) |
| Trigger | `workflow_dispatch` only |
| Default intensity | Moderate (safe for `ubuntu-latest`), overridable via inputs |
| Extra koldstore coverage | Toggleable **feature packs** on one harness (not separate mega-suites) |

## Architecture: core + packs

One timed harness (seed → baseline → soak → assert/report). **`chat` is always on.** Additional packs add workers, tables, asserts, and report sections when selected.

```
KOLDSTORE_STRESS_PACKS=chat                 # default
KOLDSTORE_STRESS_PACKS=chat,cold_dml,multi_table,joins
```

Workflow input `packs` (comma-separated or multi-select) maps to that env var.

### Pack catalog

| Pack | Stresses | Behavior when enabled | Ship |
|------|----------|------------------------|------|
| **chat** | Wide managed rows, history merge-scan, aggressive small-file flush, FDs | Always on: writers + history readers + flush rider + watchdog | **v1** |
| **cold_dml** | Cold UPDATE/DELETE overlays + hot/cold visibility | Extra workers mutate *seeded/old* message IDs after they are cold; spot-check row still correct / deleted | **v1** |
| **multi_table** | Concurrent flush, jobs catalog, multi-relation open files | Sibling managed tables (`conversations` metadata, `receipts`) with light writers; flush rider round-robins relations | **v1** |
| **joins** | Hot+cold join plans under load | History-style readers join `messages` ↔ `conversations` / `receipts` with `ORDER BY` / `LIMIT` | **v1** |
| **async** | Async mirror capture under sustained DML+flush | Soak with `mirror_mode=async` + fence before visibility asserts | **v1** (mode flag; can combine with other packs) |
| **schema_evo** | Catalog + scan after ALTER under load | Mid-soak `ADD COLUMN` on a secondary table; continue DML/SELECT on new column | **later** |
| **scheduler** | Ops scheduling path (not only forced flush) | Prefer schedule / auto-flush knobs; forced flush becomes backup | **later** |
| **s3** | Object-store publish/read path | MinIO (CI service) instead of filesystem storage | **later** (separate workflow input `storage=fs\|s3`) |

**Explicitly out of this workflow (covered elsewhere):** crash failpoints, SQLsmith, full migrate/demigrate churn, HammerDB TPC-C.

### v1 default vs penetration profile

| Profile | Packs | Notes |
|---------|-------|-------|
| Default / short manual | `chat` | 5 minutes, moderate clients |
| Full v1 penetration | `chat,cold_dml,multi_table,joins` + optional `async` | Longer minutes; still GH-runner-safe defaults |
| Later heavy | above + `schema_evo,scheduler` and/or `storage=s3` | Enable when packs land |

## Package layout

| Path | Role |
|------|------|
| `tests/stress/Cargo.toml` | Package `stress`; reuse e2e common via `#[path]` like examples |
| `tests/stress/src/lib.rs` | Crate root / module docs |
| `tests/stress/tests/chat_penetration.rs` | Primary timed scenario entrypoint |
| `tests/stress/src/config.rs` | Env/knob + pack parsing |
| `tests/stress/src/packs.rs` | Pack enum, enablement, validation |
| `tests/stress/src/schema.rs` | Wide chat DDL + optional sibling tables + manage/flush policy |
| `tests/stress/src/workload.rs` | Core chat workers |
| `tests/stress/src/packs/cold_dml.rs` | Cold overlay workers + asserts (**v1**) |
| `tests/stress/src/packs/multi_table.rs` | Sibling tables + multi flush (**v1**) |
| `tests/stress/src/packs/joins.rs` | Join readers (**v1**) |
| `tests/stress/src/baseline.rs` | Baseline probe + p95 compare (ops expand with packs) |
| `tests/stress/src/watchdog.rs` | FD / RSS / connection sampling |
| `tests/stress/src/report.rs` | JSON/text report under `target/stress` (per-pack sections) |
| `.github/workflows/chat-penetration.yml` | Manual workflow |
| `scripts/run-chat-penetration.sh` | Local + CI entrypoint (prepare pgrx, run nextest) |

Later packs add modules under `src/packs/` without changing the core phase machine.

## Schema (wide rows on purpose)

### Core: managed `messages` (tenant-scoped)

| Column | Type | Why |
|--------|------|-----|
| `id` | `BIGINT PRIMARY KEY` | Identity |
| `tenant_id` | `TEXT NOT NULL` | Scope / RLS-style setting |
| `conversation_id` | `TEXT NOT NULL` | Chat thread |
| `sender_id` | `TEXT NOT NULL` | Sender |
| `body` | `TEXT NOT NULL` | Message text |
| `payload` | `JSONB NOT NULL` | Fat metadata (attachments stubs, reactions, client junk) |
| `blob` | `BYTEA NOT NULL` | Mandatory padding (default 1–4 KiB, env-tunable) |
| `created_at` | `TIMESTAMPTZ NOT NULL` | History `ORDER BY` |
| `updated_at` | `TIMESTAMPTZ NOT NULL` | Touched on edits |
| `version` | `INT NOT NULL` | Bumped on UPDATE |
| `flags` | `INT NOT NULL` | Extra predicate / filter |
| `status` | `TEXT NOT NULL` | Extra predicate |

Indexes:

- `(tenant_id, conversation_id, created_at DESC)` — primary history path
- `(tenant_id, updated_at)` — “recently edited”
- `(sender_id, created_at)` — sender timeline

Flush policy: low `hot_row_limit`, low `min_flush_rows`, low `max_rows_per_file` so many small cold files accumulate (FD / open-file pressure).

### When `multi_table` / `joins` enabled (v1)

- `conversations` — lighter managed table (tenant, conversation_id, title, updated_at, version)
- `receipts` — managed (tenant, message_id, reader_id, read_at) for join + extra flush target

## Phases

1. **Seed** — create schema (core + pack tables), manage, seed + flush waves so cold history exists.
2. **Baseline** — short low-concurrency probe of core ops (INSERT + cold-history SELECT); if packs enabled, also probe cold UPDATE and/or a join SELECT; record p95 per op.
3. **Soak** — full parallel load for `MINUTES` (core workers + pack workers).
4. **Assert + report** — visibility spot-checks (incl. pack-specific), job health, resource peaks, latency gates; write artifacts with pack list.

## Worker model (moderate defaults)

### Always on (`chat`)

| Role | Default count | Behavior |
|------|---------------|----------|
| Writers | ~24 | INSERT wide rows; ~20% hot UPDATE (`version++`, rewrite JSON snippet, `updated_at`) |
| History readers | ~8 | Tenant-scoped deep scrollback over older conversations |
| Flush rider | 1 | Periodic `koldstore.flush_table` + wait-for-jobs (round-robin if multi_table) |
| Watchdog | 1 | Sample Postgres RSS, process open FDs, active connections |

### Pack workers (v1, when enabled)

| Pack | Extra workers | Behavior |
|------|---------------|----------|
| `cold_dml` | ~4 | UPDATE/DELETE against old/cold message ids; verify overlay visibility |
| `multi_table` | ~4–8 | Light INSERT/UPDATE on `conversations` / `receipts` |
| `joins` | ~4 | Join queries with `ORDER BY` / `LIMIT` across managed relations |
| `async` | (mode) | No extra worker count; changes mirror mode + fencing |

Defaults sized for GitHub `ubuntu-latest`. Workflow inputs can raise clients, payload size, and blob size.

## Knobs

Env prefix `KOLDSTORE_STRESS_` (workflow inputs map 1:1):

| Knob | Default | Notes |
|------|---------|-------|
| `PACKS` | `chat` | Comma-separated; `chat` implied if omitted |
| `MIRROR_MODE` | `strict` | `strict` \| `async` (enabling `async` pack forces async) |
| `STORAGE` | `fs` | `fs` \| `s3` (**s3** pack/later) |
| `MINUTES` | `5` | Soak duration after baseline |
| `CLIENTS` | `24` | Writer count |
| `HISTORY_CLIENTS` | `8` | History reader count |
| `TENANTS` | `16` | Tenant fan-out |
| `CONVERSATIONS_PER_TENANT` | `8` | Conversation fan-out |
| `PAYLOAD_BYTES` | ~2 KiB | Approximate JSONB size target |
| `BYTEA_BYTES` | `2048` | Mandatory blob size |
| `LATENCY_MULTIPLIER` | `4` | Soak p95 must be ≤ baseline p95 × this (per probed op) |
| `BASELINE_SAMPLES` | ~50 | Samples per probed op during baseline |
| Flush policy ints | `hot_row_limit=2000`, `min_flush_rows=1000`, `max_rows_per_file=1000` | Product floor: `max_rows_per_file >= 1000` |

## Pass / fail

**Fail on:**

- Panic, connection death storm, or Postgres/extension becoming unresponsive
- Spot-check: missing hot or cold rows that should be visible for a known seeded conversation
- Pack-specific correctness (e.g. cold_dml overlay wrong; join missing expected rows)
- Stuck / failed flush jobs at end of soak (all managed relations in play)
- Watchdog: FD / open-file exhaustion signals, OOM-ish RSS cliff, or connection saturation past a hard cap
- Soak p95 for any **baselined** op exceeds `baseline_p95 × LATENCY_MULTIPLIER` (small absolute floor to avoid flake)

**Report (always upload on CI):** enabled packs, latency histograms per op, segment/file counts per relation, peak FD/RSS, row/op counters, baseline vs soak comparison.

## CI workflow

`.github/workflows/chat-penetration.yml`:

- `on: workflow_dispatch` with inputs: `minutes`, `packs`, `mirror_mode`, `clients`, `history_clients`, `payload_bytes`, `bytea_bytes`, `latency_multiplier` (later: `storage`)
- PG 16 + cargo-pgrx prepare (same pattern as `weekly-hammerdb.yml` / `nightly-readiness.yml`)
- Job `timeout-minutes` derived from `minutes` + fixed install/seed budget (e.g. minutes + 60)
- Run `scripts/run-chat-penetration.sh`
- Upload `target/stress` artifacts

Local loop stays pgrx-managed Postgres (no Docker dependency in `tests/` for `fs` storage), matching `AGENTS.md`. S3 pack (later) may use MinIO under `docker/` or an existing e2e MinIO helper without making the default path Docker-required.

## Relation to existing tests

| Existing | Overlap | Difference |
|----------|---------|------------|
| `tests/examples/chat_history` | Chat schema, parallel tenants, cold scrollback | Example correctness demo; not timed penetration + baseline latency gates |
| `tests/e2e/suite/async_load_soak` | Timed DML + flush | Narrow rows; no chat history readers; short default |
| `tests/e2e` cold DML / joins / schema evolution | Feature correctness | Not sustained penetration with FD/latency gates |
| `benchmarks` / HammerDB | Throughput / TPC-C | Different scenario |
| Nightly readiness | Crash, SQLsmith, integrity | Not chat penetration soak |

## Inspiration mapping (KalamDB → koldstore)

| KalamDB `chat_realtime_bench` | koldstore stress |
|-------------------------------|------------------|
| Timed minutes + concurrent conversation workers | `MINUTES` + writer/history client pools |
| USER messages / conversations | Wide managed `messages` (+ optional sibling tables via packs) |
| Topics / subscriptions / typing STREAM | Omitted; history (+ join) readers substitute “users reading old chat” |
| Message rate pacing | Writer loop rate + optional sleep |
| Auth users | `koldstore.user_id` / tenant scope setting per client |

## Implementation order

1. **v1 core:** package + chat schema/workers + baseline/watchdog/report + workflow (`PACKS=chat`)
2. **v1 packs:** `cold_dml`, `multi_table`, `joins`, `async` mode flag
3. **later:** `schema_evo`, `scheduler`, `s3`
