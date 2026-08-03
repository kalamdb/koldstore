# Progressive Hot–Cold Scan Baseline (Task 0.1)

Captured: 2026-08-03  
Branch: `feature/progressive-hot-cold-query`  
Worktree: `.worktrees/progressive-hot-cold-query`  
HEAD at capture: `bc49147edde6950d1a6c7ea692e72f5186480541`

Phase 0 baseline before `KoldPathStrategy`. No production code changes in this
task — only these notes.

## Contracts preserved (must stay green)

Source: `docs/architecture/scanning-table.md`,
`crates/pg_koldstore/src/merge_scan/pg.rs` (`set_rel_pathlist` + BeginCustomScan
fast paths), `crates/pg_koldstore/src/pg_tests/scan.inc.rs`, and
`tests/e2e/merge/user_scope_cold_pruning.rs` (read for cold-capable / prune
expectations; not executed in this baseline).

| Contract | Current behavior |
| --- | --- |
| Empty manifest / zero published segments | Retain native Index/Seq/Bitmap paths; **no** `KoldMergeScan` (`set_rel_pathlist` early return when `segment_count == 0`). |
| Cold-proven-empty (plan-time) | Aggregate Sort Key / constant predicates prove no cold match → native paths, no wrapper. |
| Cold-proven-empty (exec-time) | Parameterized / runtime prove-empty → `HotChild` / native child delegation without merge setup. |
| Exact-PK hot hit while cold may exist | `KoldMergeScan` may be planned when cold can contribute, but BeginCustomScan returns the native child slot **without** catalog lookup, Parquet open, mirror load, or merge-state init (`Emit Path: hot_child`, `Parquet Segments Opened: 0`). |
| Cold-capable predicate | Install single `KoldMergeScan` (clears `pathlist` + `partial_pathlist`); today unordered / SPI JSON keyset merge for mixed hot+cold. |
| Relcache invalidation | First published cold segment (or expanded bounds) rebuilds prepared native plans into cold-capable `KoldMergeScan`. |

Locked hot-only emit / plan-time prune paths must not be casually rewritten in
later phases (see `AGENTS.md`).

## Test commands run

Plan text used `cargo pgrx test … -- --test scan`. Current `cargo-pgrx` 0.19
rejects that form (`unexpected argument 'scan'` after `--`). Adjusted to
positional `TESTNAME` filters: `cargo pgrx test -p pg_koldstore pg15 <filter>`.

Environment: local `~/.pgrx` has pg15.18 (also 16/17/18). Baseline used **pg15**.

### 1. `koldstore-merge` lib

```bash
cargo test -p koldstore-merge --lib
```

**Result: PASS** — 2 passed; 0 failed (path replacement / partial-path clear
unit tests).

### 2. Plan-named scan filter

```bash
cargo pgrx test -p pg_koldstore pg15 scan
```

**Result: PASS** — 12 lib tests matched substring `scan` (10 unit + 2
`#[pg_test]` from `scan.inc.rs`); 66 filtered out.

`#[pg_test]` covered by this filter only:

- `pg_explain_analyze_shows_scan_merge_flow_and_phase_timing`
- `pg_merge_scan_fails_closed_when_seen_key_limit_is_exceeded`

That is **not** the full `scan.inc.rs` suite (22 pg_tests). Additional
locked-contract filters were run below.

### 3. Locked-contract `scan.inc.rs` filters (pg15)

```bash
cargo pgrx test -p pg_koldstore pg15 <filter>
# filters: native_postgresql_plan, prepared_native_plan, hot_pk_hit,
#          exact_hot_pk, merge_stream, hot_only_and_mixed,
#          parameterized_hot_range, parameterized_primary_key_miss,
#          explain_analyze_uses_native_hot_child, packed_row_group_arrays
```

**Result: ALL PASS** (no known failures).

Unique `#[pg_test]` functions verified:

| Test | Contract |
| --- | --- |
| `pg_managed_table_without_published_cold_segments_keeps_native_postgresql_plan` | Empty manifest → native |
| `pg_hot_primary_key_range_above_cold_max_keeps_native_postgresql_plan` | Cold-proven-empty → native |
| `pg_prepared_native_plan_is_invalidated_when_first_cold_segment_is_published` | Native → `KoldMergeScan` after publish |
| `pg_hot_pk_hit_skips_parquet_open_when_cold_segment_index_overlaps` | Exact-PK hot hit, no Parquet |
| `pg_exact_hot_pk_hit_avoids_merge_runtime_bookkeeping` | Exact-PK hot hit, no merge init |
| `pg_primary_key_range_pushes_hot_candidates_into_merge_stream` | Cold-capable merge stream |
| `pg_hot_only_and_mixed_hot_cold_results_match_expected_values` | Hot-only + mixed correctness |
| `pg_parameterized_hot_range_above_cold_max_skips_merge_fallback` | Runtime cold-empty / hot path |
| `pg_parameterized_primary_key_miss_above_cold_max_skips_merge_fallback` | Runtime cold-empty miss |
| `pg_explain_analyze_uses_native_hot_child_counters` | Hot-child EXPLAIN counters |
| `pg_packed_row_group_arrays_skip_parquet_when_scalar_segment_bounds_overlap` | Bound prune avoids Parquet |
| `pg_explain_analyze_shows_scan_merge_flow_and_phase_timing` | Mixed merge EXPLAIN (via `scan`) |
| `pg_merge_scan_fails_closed_when_seen_key_limit_is_exceeded` | Merge fail-closed (via `scan`) |

## Not run in this baseline

- Full `scan.inc.rs` (remaining explain JSON / mirror overlay / unmanaged /
  bound-invalidation tests beyond the locked-path set above).
- `tests/e2e/merge/user_scope_cold_pruning.rs` (needs running pgrx server /
  e2e harness; contracts noted from source only).
- `--no-schema` shortcut: `cargo pgrx test --no-schema` broke extension
  install in this pgrx-tests 0.19.2 path (`install … --no-schema` unexpected);
  re-ran without `--no-schema`.

## Known failures

None for the commands above. Suite is green for the locked-path baseline.
