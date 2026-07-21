# Case: Policy-driven movement to cold storage

| | |
| --- | --- |
| Status | Proposed design (not yet implemented) |
| Scope | Row-limit, age, and row-predicate movement policies |
| Modes | Regular and partitioned managed tables; strict and async mirror capture |
| Related | [SQL API](../sql-api.md), [manage-table](../architecture/manage-table.md), [flushing-table](../architecture/flushing-table.md) |

---

## Summary

KoldStore currently models automatic flushing as a flat row-limit configuration:

```text
hot_row_limit
min_flush_rows
max_rows_per_file
target_file_size_mb
```

This works while every row is eligible and the selected work is a contiguous
oldest-by-mirror-sequence prefix. It does not cleanly describe policies such as:

```text
move rows older than 90 days
move rows where status = 'completed'
move completed rows older than 90 days
keep at most 100,000 hot rows, but evict only completed rows
```

The policy model should separate three independent decisions:

1. **Eligibility** — which live rows may move.
2. **Goal** — how many eligible rows should move.
3. **Flush mechanics** — how selected rows become Parquet files.

This separation avoids repeating `min_flush_rows`, `max_rows_per_file`, and
file-size settings in every policy variant. It also allows the legacy
`manage_table(...)` function and new `ALTER TABLE ... SET (...)` syntax to
produce the same typed internal configuration.

The recommended design is:

```text
MovePolicy
├── eligibility: Any | OlderThan | Matches | OlderThanAndMatches
├── goal: DrainEligible | KeepHotRows
└── flush: minimum batch and file-shaping settings
```

Policy evaluation remains asynchronous. An application `INSERT` or `UPDATE`
must not perform object-storage movement in the application transaction.

---

## Goals

- Preserve the current row-limit behavior without keeping a separate execution
  pipeline.
- Support age eligibility such as `created_at < now() - interval '90 days'`.
- Support a restricted row predicate such as `status = 'completed'`.
- Allow age and predicate eligibility to be combined with `AND`.
- Use one root-owned policy for regular and partitioned tables.
- Execute partitioned-table work through bounded leaf jobs.
- Keep legacy `manage_table(...)` and the proposed `ALTER TABLE ... SET (...)`
  syntax as adapters over one canonical configuration path.
- Expose policy state and progress without requiring operators to join internal
  catalogs or call many status functions.
- Preserve correctness when an eligible row changes while a flush is running.

## Non-goals for the first version

- Arbitrary SQL expressions, functions, subqueries, or joins in predicates.
- Per-partition policy overrides.
- Named reusable policy objects shared by unrelated roots.
- Immediate object-storage movement from a row-level DML trigger.
- Boolean expression trees containing unrestricted `OR` and `NOT`.
- Rewarming cold rows automatically when a policy is relaxed.

---

## Lessons from other systems

### TigerData and Timescale

TigerData exposes `add_tiering_policy(..., move_after => interval)` and runs the
policy asynchronously. Eligibility and progress are visible through tiering
views such as queued and tiered chunks. The movement unit is a chunk rather
than an arbitrary row.

The useful pattern for KoldStore is:

- make the lifecycle rule declarative;
- evaluate it asynchronously;
- expose queued, running, and completed state;
- keep movement work bounded by a physical unit.

See [TigerData: manage storage and tiering](https://www.tigerdata.com/docs/use-timescale/latest/data-tiering/enabling-data-tiering).

### Apache Doris

Doris separates storage resources from storage policies. A policy contains a
relative `cooldown_ttl` or absolute `cooldown_datetime` and can be associated
with a table or partition. It also exposes policy associations through `SHOW
STORAGE POLICY`.

The useful pattern for KoldStore is the separation between:

- the destination storage binding;
- the lifecycle condition;
- the objects using the policy;
- the operational view of policy state.

KoldStore does not need Doris-style named policy objects in the first version.
An inline root-owned policy avoids additional catalog indirection and public API
methods.

See [Apache Doris: CREATE STORAGE POLICY](https://doris.apache.org/docs/dev/sql-manual/sql-statements/cluster-management/storage-management/CREATE-STORAGE-POLICY/)
and [SHOW STORAGE POLICY](https://doris.apache.org/docs/dev/sql-manual/sql-statements/cluster-management/storage-management/SHOW-STORAGE-POLICY/).

### ClickHouse

ClickHouse TTL rules can use time expressions for movement and can use
value-based `WHERE` conditions for row deletion. TTL work is applied by
background merges rather than synchronously when a row first satisfies a rule.
ClickHouse also recommends aligning partitions with the TTL time field so whole
partitions can be handled efficiently.

The useful pattern for KoldStore is:

- distinguish the declarative row condition from the background execution;
- make age a first-class rule instead of embedding `now()` in a free-form
  predicate;
- use physical partition boundaries when they match the policy boundary.

See [ClickHouse: manage data with TTL](https://clickhouse.com/docs/guides/developer/ttl).

---

## Terminology

Use the following terms consistently:

| Term | Meaning |
| --- | --- |
| Move policy | Complete declaration of which rows may move, the desired hot state, and flush mechanics |
| Eligibility | A test determining whether a live row may be selected |
| Goal | The number of eligible rows the evaluator should try to move |
| Evaluation | Computing desired and selectable work from current state |
| Flush | Writing a stable selected set to cold files and cleaning its hot state |
| Evaluation cadence | How often the background worker evaluates policies |
| Capture trigger | Existing DML mechanism that updates the latest-state mirror; not a policy evaluator |

`older_than` and `status = 'completed'` are eligibility rules. They are not
PostgreSQL triggers and should not be called flush triggers in the internal
model.

---

## Recommended domain model

The PostgreSQL-free domain model belongs in `koldstore-common`:

```rust
pub struct MovePolicy {
    /// Which live rows may be moved.
    pub eligibility: MoveEligibility,

    /// The hot-state goal the evaluator tries to maintain.
    pub goal: MoveGoal,

    /// Batching and cold-file creation settings.
    pub flush: FlushPolicy,
}

pub enum MoveEligibility {
    /// Every live row is eligible.
    Any,

    /// The configured column is earlier than the evaluation cutoff.
    OlderThan {
        column: ColumnName,
        age: MoveAfter,
    },

    /// A restricted, typed predicate matches the current live row.
    Matches(RowPredicate),

    /// Both the age rule and predicate must match.
    OlderThanAndMatches {
        age: OlderThanRule,
        predicate: RowPredicate,
    },
}

pub enum MoveGoal {
    /// Move all rows that are currently eligible.
    DrainEligible,

    /// Move enough eligible rows to approach the live hot-row limit.
    KeepHotRows {
        limit: HotRowLimit,
    },
}

pub struct FlushPolicy {
    /// Do not start normal row movement below this ready-row count.
    pub min_rows: MinFlushRows,

    /// Cold file-shaping settings.
    pub file: FilePolicy,
}

pub struct FilePolicy {
    /// Hard maximum rows written into one Parquet file.
    pub max_rows: MaxRowsPerFile,

    /// Optional preferred compressed file size.
    pub target_size_mb: Option<TargetFileSizeMb>,
}
```

The exact names are illustrative. The important boundary is that
`HotRowLimit`, `MinFlushRows`, `MaxRowsPerFile`, column names, and intervals are
validated domain types rather than interchangeable integers and strings.

### Why `RowLimit` should not contain flush fields

The following shape should be avoided:

```rust
enum FlushPolicy {
    RowLimit {
        hot_row_limit: u64,
        min_flush_rows: u64,
        max_rows_per_file: u64,
    },
    OlderThan {
        age: Interval,
        min_flush_rows: u64,
        max_rows_per_file: u64,
    },
    Filter {
        expression: String,
        min_flush_rows: u64,
        max_rows_per_file: u64,
    },
}
```

It duplicates execution settings, makes combinations awkward, and encourages
free-form string expressions. Row limit is a goal, while minimum flush rows and
file limits are execution settings shared by every goal.

---

## Policy semantics

| Eligibility | Goal | Meaning |
| --- | --- | --- |
| `Any` | `KeepHotRows(10_000)` | Current row-limit behavior |
| `OlderThan(90 days)` | `DrainEligible` | Move every row older than 90 days |
| `Matches(status = completed)` | `DrainEligible` | Move completed rows on the next evaluation |
| `OlderThanAndMatches` | `DrainEligible` | Move only completed rows older than the age cutoff |
| `Matches(status = completed)` | `KeepHotRows(100_000)` | Reduce hot rows toward 100,000 using only completed rows |

### Row-limit calculation

`hot_row_limit` means live rows remaining in the PostgreSQL heap. It must not
mean mirror rows, because the mirror may also contain tombstones or other
pending maintenance state.

For `KeepHotRows`:

```text
desired_move_rows = max(live_hot_rows - hot_row_limit, 0)
ready_rows        = min(desired_move_rows, eligible_live_rows)
blocked_rows      = desired_move_rows - ready_rows
```

For `DrainEligible`:

```text
desired_move_rows = eligible_live_rows
ready_rows        = eligible_live_rows
blocked_rows      = 0
```

A normal evaluation creates work only when `ready_rows >= min_flush_rows`.
Forced flush remains an explicit operator override and may ignore the normal
eligibility and minimum-batch rules.

### Combining age and predicates

Age and row predicates use `AND` semantics:

```text
created_at older than 90 days
AND status = 'completed'
```

This is safer than interpreting several independently configured rules as `OR`,
which could unexpectedly move recent completed rows or old rows that are still
active.

### Selection order

- Age policies select the oldest eligible age value first, with primary-key and
  mirror sequence tie-breakers.
- Predicate-only row-limit policies select the oldest eligible mirror sequence
  first.
- The order must be deterministic and stored with the job's stable selection.
- A `NULL` age value is not eligible unless a future policy explicitly defines
  different null behavior.

---

## Proposed SQL syntax

The common operator interface remains a single `ALTER TABLE` command:

```sql
ALTER TABLE messages SET (
  koldstore_enabled = true,
  koldstore_storage = 'cold_s3',

  koldstore_order_column = 'created_at',
  koldstore_move_after = '90 days',
  koldstore_move_when = 'status = ''completed''',

  koldstore_hot_row_limit = 100000,
  koldstore_min_flush_rows = 1000,
  koldstore_max_rows_per_file = 10000
);
```

Public semantics:

- `koldstore_move_after` requires a date or timestamp
  `koldstore_order_column`.
- `koldstore_move_after` and `koldstore_move_when` combine with `AND`.
- Without `koldstore_hot_row_limit`, eligible rows use `DrainEligible`.
- With `koldstore_hot_row_limit`, the goal is `KeepHotRows`.
- Without an age or predicate rule, every row is eligible; this preserves the
  existing row-limit policy.
- `koldstore_min_flush_rows` controls the minimum normal work unit.
- `koldstore_max_rows_per_file` controls file shape, not eligibility or total
  work for a job.

`migration_order_by` should not silently become the movement age column. A
backfill ordering hint and a lifecycle condition are separate concepts even
when operators commonly configure the same column for both.

### Legacy function compatibility

The existing call remains supported:

```sql
SELECT koldstore.manage_table(
  table_name        => 'public.messages',
  storage           => 'cold_s3',
  hot_row_limit     => 100000,
  min_flush_rows    => 1000,
  max_rows_per_file => 10000
);
```

It is an adapter that constructs:

```text
eligibility = Any
goal        = KeepHotRows(100000)
flush       = { min_rows: 1000, max_rows_per_file: 10000 }
```

Both SQL entry points must build the same `ManageTablePatch` or equivalent
typed command. Validation, catalog persistence, trigger management, partition
registration, and job creation must not be duplicated between syntaxes.

---

## Restricted row predicates

`koldstore_move_when` must not be stored and executed as arbitrary SQL.

The first version should support a small row-local grammar:

```text
column = constant
column <> constant
column < constant
column <= constant
column > constant
column >= constant
column IN (constant, ...)
column IS NULL
column IS NOT NULL
condition AND condition
```

Reject:

- subqueries and joins;
- SQL functions, including `now()`;
- volatile or session-dependent expressions;
- references outside the managed row;
- unvalidated casts;
- `OR` and `NOT` in the initial version.

Age uses the first-class `OlderThan` rule so the evaluator can calculate a
cutoff, explain it in status, use indexes, and prune partitions without
interpreting a dynamic `now()` expression.

At configuration time:

1. Parse the restricted expression.
2. Resolve every referenced column.
3. Parse constants according to the column's PostgreSQL type.
4. Reject unsupported operators and types.
5. Store a normalized typed predicate, not the original SQL text.
6. Record the referenced columns so incompatible `DROP COLUMN` or type changes
   can be rejected or pause the policy safely.

The extension should warn when no useful index exists for age or predicate
selection. It should not silently create application indexes in the first
version.

---

## Evaluation and capture behavior

The policy condition becoming true does not execute a flush in the application
transaction.

The existing strict or async mirror capture path records inserts, updates, and
deletes. A background evaluator periodically resolves policies and enqueues
bounded jobs. `flush_table(...)` remains the explicit evaluate-and-run path.

For example:

```sql
UPDATE messages
SET status = 'completed'
WHERE id = 42;
```

The update changes the latest mirror sequence. The next policy evaluation joins
the mirror identity to the current heap row, sees `status = 'completed'`, and
may select it. No additional status-specific trigger is required.

If lower policy latency is needed later, capture may mark a root policy dirty
once per transaction. It must not enqueue one flush job per modified row.

### Async mirror mode

Policy evaluation in async mode must use the same mirror-consistency fence as
normal flush selection. Eligibility must be evaluated only after committed
source changes through the selection boundary have reached the mirror.

---

## Filtered selection changes the cleanup proof

The current row-limit path selects a contiguous mirror sequence prefix:

```text
seq <= max_seq
```

It can therefore clean the mirror and hot heap with one sequence-range delete.
A predicate produces a sparse set:

```text
seq 101  status = pending     not selected
seq 102  status = completed   selected
seq 103  status = pending     not selected
seq 104  status = completed   selected
```

Deleting `seq <= 104` would incorrectly remove rows 101 and 103.

Use one explicit selection boundary:

```rust
pub enum FlushSelection {
    /// Optimized current path for an unfiltered contiguous prefix.
    ContiguousPrefix {
        max_seq: SeqId,
        row_count: FlushRowCount,
    },

    /// Stable PK + selected-sequence identities for sparse eligibility.
    ExactRows(MirrorFlushSelectionSet),
}
```

The following execution stages remain shared:

```text
evaluate policy
    ↓
resolve stable selection
    ↓
stream selected rows to Parquet
    ↓
publish segment and manifest metadata
    ↓
clean exactly the committed selection
    ↓
complete job and counters
```

Only selection planning and final cleanup differ:

- `Any + KeepHotRows` may retain the sequence-prefix fast path.
- Age or predicate selection uses exact PK-and-sequence cleanup.
- The writer, Parquet encoding, storage publication, manifest update, job
  lifecycle, and progress reporting are shared.

KoldStore already has useful pieces for the exact path:

- `MirrorFlushSelectionSet` in `crates/koldstore-flush/src/job.rs`;
- typed PK-and-sequence cleanup in `crates/koldstore-flush/src/cleanup.rs`.

The active PostgreSQL adapter currently chooses sequence-range cleanup, so the
new policy work should route sparse selections through the existing exact
cleanup abstraction instead of creating a second flush executor.

### Concurrent row changes

Every exact selected identity contains:

```text
primary key + selected mirror sequence
```

Cleanup deletes the mirror row only when both values still match. If the source
row changes after selection, capture assigns a new mirror sequence. Cleanup
then skips the changed row, leaves its hot version intact, and allows a later
policy evaluation to reconsider it.

This check is required even when the predicate is immutable for a row version,
because the application may change the predicate column, age column, or any
other value after the cold file has been written.

### Tombstones

A delete mirror record has no current heap row on which to evaluate
`status = 'completed'`. Predicate evaluation therefore applies to live rows,
not tombstones.

Tombstones remain eligible maintenance work so they can mask older cold
versions. They may be batched with normal policy work, but a row predicate must
never make them permanently ineligible.

---

## Partitioned-table semantics

Configuration belongs to the logical partition root. Physical work belongs to
leaf partitions.

```text
messages                         root-owned MovePolicy
├── messages_2026_01             leaf selection/job/progress
├── messages_2026_02             leaf selection/job/progress
└── messages_2026_03             leaf selection/job/progress
```

Rules:

- Store one policy and policy version against the root.
- Validate the age and predicate columns against the root and every current
  leaf.
- Automatically validate and register future attached or created leaves.
- Compile eligibility SQL per leaf using the root policy.
- Create bounded migration and flush jobs per leaf.
- Reject leaf-level policy overrides in the first version.
- Store `root_table_oid`, `leaf_table_oid`, and `policy_version` on every leaf
  job.

`KeepHotRows` is root-wide: it describes the logical table, not a separate
allowance multiplied by the number of leaves. The root evaluator computes the
global desired movement count and allocates work oldest-first across eligible
leaves. Individual jobs remain leaf-scoped for retry, locking, and progress.

Age-aligned partitions provide an optimization. When an entire leaf range is
older than the cutoff and its predicate requirements are either absent or known
to hold, the evaluator may use a whole-leaf path. Otherwise it performs exact
row selection inside the leaf.

---

## Policy version and in-flight work

Every persisted policy has a monotonically increasing `policy_version`.

When policy configuration changes:

- new evaluations use the new version immediately;
- existing stable selections keep the version under which they were selected;
- a running job is not silently reinterpreted using a different predicate;
- status reports when an in-flight job uses an older policy version;
- future partitions inherit the latest root version.

This produces deterministic retries and avoids changing the selected set after
some Parquet files have already been published.

---

## Status API

Avoid adding separate functions for policy status, partition status, eligible
row counts, and job progress. Expose one typed view:

```sql
SELECT
  root_table,
  member_table,
  state,
  policy_version,
  hot_rows,
  eligible_rows,
  desired_move_rows,
  ready_rows,
  blocked_rows,
  selected_rows,
  rows_flushed,
  progress_percent,
  last_evaluated_at,
  next_evaluation_at,
  last_error
FROM koldstore.policy_status
WHERE root_table = 'public.messages'::regclass
ORDER BY member_table;
```

For a regular table, the view returns one row where `root_table = member_table`.
For a partitioned table, it returns one aggregate root row plus one row per leaf.

Recommended state values:

```text
caught_up
ready
running
blocked_by_filter
paused
error
```

Policy progress and job progress are different:

- policy work is open-ended because rows continue arriving;
- a job has a stable `selected_rows` denominator and can expose a meaningful
  percentage;
- `caught_up` means no work is currently ready, not that the policy is finished
  forever.

`koldstore.describe_table(...)` can remain as a backward-compatible JSON wrapper
over the same internal status query. Operators should not need direct reads from
`koldstore.jobs` for normal monitoring.

---

## Persistence and backward compatibility

The current flat `koldstore.schemas.options` representation must continue to
decode:

```json
{
  "hot_row_limit": 10000,
  "min_flush_rows": 1000,
  "max_rows_per_file": 1000,
  "target_file_size_mb": 256
}
```

When the nested move policy is absent and a positive legacy
`hot_row_limit` exists, normalize it as:

```text
eligibility = Any
goal        = KeepHotRows(hot_row_limit)
flush       = legacy min/file settings
```

New writes should persist one versioned canonical structure rather than
indefinitely writing both old and new fields. The legacy SQL function is an
input adapter; it does not require a second catalog representation.

An illustrative persisted form is:

```json
{
  "move_policy": {
    "version": 2,
    "eligibility": {
      "kind": "older_than_and_matches",
      "column": "created_at",
      "age": "90 days",
      "predicate": {
        "op": "eq",
        "column": "status",
        "value": "completed"
      }
    },
    "goal": {
      "kind": "keep_hot_rows",
      "limit": 100000
    },
    "flush": {
      "min_rows": 1000,
      "file": {
        "max_rows": 10000,
        "target_size_mb": 256
      }
    }
  }
}
```

JSON conversion remains a PostgreSQL/catalog boundary concern. Domain logic
should operate on typed values.

---

## Recommended first-version scope

Implement the smallest complete model:

1. Canonical typed `MovePolicy` with legacy flat-option decoding.
2. `Any`, `OlderThan`, `Matches`, and `OlderThanAndMatches` eligibility.
3. `DrainEligible` and root-wide `KeepHotRows` goals.
4. Equality, comparison, `IN`, null checks, and `AND` in predicates.
5. Exact selected-set cleanup for sparse policies while retaining the current
   sequence-prefix optimization for unfiltered row limits.
6. Root-owned policies and per-leaf jobs for partitioned tables.
7. One `koldstore.policy_status` view for roots, leaves, and active progress.
8. Legacy `manage_table(...)` and new `ALTER TABLE ... SET (...)` adapters over
   one command handler.

Defer named reusable policies, arbitrary SQL, leaf overrides, and event-driven
per-row enqueueing until operational evidence shows they are needed.

---

## Alternatives considered

### One enum variant per complete policy

```text
RowLimit | OlderThan | Filter
```

Rejected because batching and file fields would be repeated, combinations such
as age plus status would create more variants, and row limit is a goal rather
than row eligibility.

### Arbitrary SQL predicate string

Rejected because it introduces volatile expressions, security and quoting
concerns, schema dependency tracking, difficult progress explanations, and
unbounded planner behavior.

### Copy policy configuration into every partition

Rejected because it creates configuration drift and makes policy changes and
future partition inheritance harder. The root is the source of truth; leaves
store only execution state.

### Flush immediately from the DML trigger

Rejected because object I/O and job creation would increase application
transaction latency, create job storms, and couple policy scheduling to write
volume.

### Always use exact selected-set cleanup

Not required initially. It would simplify cleanup semantics but discard the
current efficient sequence-prefix delete for the common unfiltered row-limit
case. A small `FlushSelection` enum localizes the difference without duplicating
the executor.

---

## Decision to make before implementation

Adopt `MovePolicy = eligibility + goal + flush mechanics` as the canonical
model. Treat age and row predicates as eligibility, row limit as a goal, and
evaluation cadence as scheduler configuration. Keep one root policy, execute
partition work per leaf, and use exact PK-and-sequence cleanup whenever
eligibility produces a sparse selection.
