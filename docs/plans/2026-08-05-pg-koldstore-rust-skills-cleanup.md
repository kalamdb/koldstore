# pg_koldstore Rust-Skills Cleanup Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make `crates/pg_koldstore` lighter and easier to maintain in **one PR**: delete unused scaffolding, collapse error/SPI boilerplate, cut needless clones, and split oversized modules — without changing merge-scan behavior or the locked hot-only path.

**Architecture:** Cleanup is layered delete → normalize → thin → split. Every phase must leave the crate compiling and behavior-identical. Prefer removing code over adding abstractions. Domain logic stays in `koldstore-*`; `pg_koldstore` remains the SPI/hooks/`pg_sys` adapter.

**Tech Stack:** Rust / pgrx, rust-skills (`anti-*`, `own-*`, `err-*`, `proj-*`), `cargo check` / `cargo pgrx test`, existing merge/flush e2e under `tests/`.

**Scope:** `crates/pg_koldstore` (+ its `native/` + shell-test fallout). Other crates (`koldstore-catalog`, storage, …) are out of scope for this PR unless a pg_koldstore change forces a tiny shared helper move.

**Non-goals / hard locks:**
- Do **not** rewrite `EmitPath::HotChild`, `cold_side_proven_empty`, plan-time early return, or hot point-hit probe (see AGENTS.md / `.cursor/rules/hot-only-merge-scan.mdc`).
- Do **not** weaken tests to hide scan bugs.
- Do **not** add features, GUCs, or new public SQL.
- Do **not** chase LTO/PGO/`opt-*` (`anti-premature-optimize`).
- Do **not** collapse `ScanProfileSink` generics in execute (intentional zero-cost ANALYZE path; high risk near HotChild).

**Branch hygiene (before starting):**
- Current worktree is on `feature/progressive-hot-cold-query` with unrelated WIP. Per AGENTS.md, **do not park cleanup on that WIP**.
- Create a clean branch from the intended base (e.g. `main` or the merge base you want reviewed against), carry only intentional cleanup commits.
- If WIP must move: `git stash push -u`, switch, then leave progressive-query WIP on its branch.

**One-PR strategy:** One PR, but **ordered phases with a green check after each**. If review pressure is high, the same commits can be split later — do not interleave unfinished phases.

**rust-skills filter for every change:**
1. Prefer delete (`anti-over-abstraction`, YAGNI).
2. Prefer borrow over clone (`own-borrow-over-clone`, `anti-clone-excessive`).
3. Prefer typed errors over `String` soup (`err-custom-type`, `err-from-impl`).
4. Prefer feature modules over mega-files (`proj-mod-by-feature`).
5. Never “simplify” into HotChild / plan-time prune.

---

## Success criteria

- Net **LOC down** in `crates/pg_koldstore/src` (excluding tests if shell tests shrink with stubs).
- No behavior change: manage/unmanage, flush, mirror apply, merge scan EXPLAIN contracts unchanged.
- `cargo check -p pg_koldstore --features pg` clean.
- Targeted verification (see Phase gates).
- PR description lists rust-skills rule ids applied and explicitly calls out locked paths left alone.

---

## Phase 0 — Delete unused scaffolding (net weight loss)

**Rules:** `anti-over-abstraction`, `proj-mod-by-feature`, `test-mock-traits`, `proj-build-rs-minimal`

### Task 0.1: Inventory + prove unused

**Files:** read-only greps across `pg_koldstore`, `pg_koldstore-shell-tests`

**Step 1:** Confirm callers for each candidate below (rg for symbol names).  
**Step 2:** Record keep/delete decision in the PR notes.  
**Step 3:** Commit nothing yet.

Candidates (high confidence from pre-pass):

| Item | Location | Action |
|------|----------|--------|
| Empty / shell-only tracing stubs (`SPAN_NAMES`, `KoldstoreSpan`, ObjectStore* counters if unused in prod) | `src/observability.rs` | Delete unused symbols; keep real async-apply metrics + whatever `_PG_init` still needs |
| `MemoryOwner` + `MEMORY_OWNER_LABELS` (accounting unused) | `src/memory.rs` | Delete owner enum/labels; keep live heap-trim APIs |
| `SpiExecutor` / `RecordingSpiExecutor` / `execute_catalog_write` | `src/spi.rs` | Delete trait + recorder + sole shell-test path |
| `KOLDSTORE_SQLSTATE` if never applied to errors | `src/spi.rs` | Delete constant + shell assertion, **or** wire it for real — do not leave half-dead |
| One-line `managed_catalog_ready` forwarder | `src/merge_scan/pg.rs` | Inline call site |
| Unused `ScanProfileSink` import | `src/merge_scan/pg.rs` | Drop import |
| Duplicate `spi_missing` / `missing_attribute` | `sql/events/mod.rs`, `sql/flush/mirror_fetch.rs` | One helper in `spi.rs` |

**Gate 0.1:** Symbol greps show zero remaining references (or only deleted shell tests updated).

### Task 0.2: Decide C custom-scan shim

**Files:** `native/custom_scan.c`, `native/custom_scan.h`, `src/merge_scan/ffi.rs`, `build.rs`, shell `merge_scan_explain`

**Step 1:** Confirm production registration is only via Rust `register_custom_scan_hooks` in `merge_scan/pg.rs`.  
**Step 2:** If C shim never hooks Postgres: remove C sources, `build.rs` cc link, `ffi.rs`, and update shell tests.  
**Step 3:** If anything external still needs the symbol, **stop** and leave a short comment — do not half-delete.

**Gate 0.2:** Extension builds; CustomScan still registers; shell tests updated or deleted.

### Task 0.3: Update shell tests for deleted scaffolding

**Files:** `crates/pg_koldstore-shell-tests/**`

**Step 1:** Remove assertions against deleted stubs.  
**Step 2:** Prefer testing real contracts (EXPLAIN labels, metrics that still exist) over mocking scaffolding.  
**Step 3:** `cargo test -p pg_koldstore-shell-tests` (or project-standard shell-test command).

**Commit:** `cleanup(pg_koldstore): remove unused shell scaffolding and dead SPI mocks`

---

## Phase 1 — Error + SPI boilerplate collapse

**Rules:** `err-custom-type`, `err-from-impl`, `err-context-chain`, `err-question-mark`, `anti-stringly-typed`

### Task 1.1: Introduce a thin adapter error

**Files:**
- Create: `crates/pg_koldstore/src/error.rs`
- Modify: `crates/pg_koldstore/src/lib.rs` (`mod error;`)

**Step 1:** Add a small enum or newtype, e.g.:

```rust
//! SPI / adapter failures mapped to `pgrx::error!` at boundaries.

#[derive(Debug)]
pub(crate) struct PgAdapterError(String);

impl std::fmt::Display for PgAdapterError { /* ... */ }
impl std::error::Error for PgAdapterError {}
impl From<String> for PgAdapterError { /* ... */ }
impl From<&str> for PgAdapterError { /* ... */ }

pub(crate) fn spi_err(error: impl ToString) -> PgAdapterError {
    PgAdapterError(error.to_string())
}
```

Keep it **minimal** — no thiserror dependency unless already in the crate graph and clearly helpful (`anti-over-abstraction`).

**Step 2:** Add `From` impls for common library errors already converted via `.to_string()` (flush/migrate/catalog) where cheap.

**Gate 1.1:** Compiles; no call sites migrated yet.

### Task 1.2: Migrate densest `Result<_, String>` modules

**Order (one module at a time, compile after each):**
1. `sql/flush/jobs.rs` + `sql/flush/spi.rs`
2. `sql/migrate/*` (after or as part of Phase 3 split)
3. `mirror/lifecycle.rs` + `mirror/apply.rs`

**Step 1:** Replace `map_err(|e| e.to_string())` with `map_err(spi_err)` / `?` via `From`.  
**Step 2:** At `#[pg_extern]` / worker boundaries, map once: `pgrx::error!("{error}")`.  
**Step 3:** Do **not** change user-visible error message text unless identical.

**Gate 1.2:** `cargo check -p pg_koldstore --features pg`

### Task 1.3: Soften panic-shaped expects in apply

**Files:** `src/mirror/apply.rs` (~ensure SQL cache)

**Step 1:** Replace `.expect("update SQL cached")`-style after fill with `get_or_insert_with` / `let-else` returning `PgAdapterError` (`pat-let-else`, `err-expect-bugs-only`).  
**Step 2:** Leave `CString::new(deterministic).expect("no NUL")` alone.

**Commit:** `cleanup(pg_koldstore): collapse SPI String errors into PgAdapterError`

---

## Phase 2 — Ownership / clone cleanup (hot+cold adapters)

**Rules:** `own-borrow-over-clone`, `anti-clone-excessive`, `mem-take-replace`, `own-cow-conditional`

### Task 2.1: Cold plan — clone once

**Files:** `src/merge_scan/pg/cold.rs` (`plan_cold_segments`, `PlannedColdSegments`, `ColdReadProfile` fill)

**Step 1:** Identify fields cloned into both profile and planned struct (`storage_type`, `base_path`, `credentials`, `config`, segment lists).  
**Step 2:** Move ownership once; build profile from moved/borrowed pieces (`mem-take-replace`).  
**Step 3:** Avoid changing prune/index selection logic.

**Gate 2.1:** Unit tests in `cold.rs`; no HotChild edits.

### Task 2.2: Hot cursor — stop per-batch clones

**Files:** `src/merge_scan/pg/hot_cursor.rs` (`next_batch`), possibly `catalog/owner.rs`

**Step 1:** Remove `pk_columns` / `catalog_columns` / `slot_attnums` clones before the owner closure.  
**Step 2:** Adjust closure/`with_relation_owner_for_merge` so it can take references.  
**Step 3:** Run merge-related `#[pg_test]` if available; else note for Phase 4 e2e.

### Task 2.3: Emit / execute clone audit (conservative)

**Files:** `src/merge_scan/pg/execute.rs`, `emit.rs`, `qual.rs`

**Step 1:** Only remove clones where the callee already takes `&T` / `&str`.  
**Step 2:** Leave materialization sites that must own Postgres datums.  
**Step 3:** **Do not** change `ScanEmitMode::HotChild` / portfolio install.

**Commit:** `cleanup(pg_koldstore): cut needless clones on cold plan and hot cursor`

---

## Phase 3 — Module splits (maintainability)

**Rules:** `proj-mod-by-feature`, `proj-flat-small`, `proj-pub-crate-internal`, `doc-module-inner`

Split is **move-only**: no logic edits in the same commit as a split when avoidable.

### Task 3.1: Split `sql/migrate/mod.rs` (~1184 LOC)

**Create under `src/sql/migrate/`:**
- `mod.rs` — re-exports + `RegClassOid` + `#[pg_extern]` wrappers only
- `manage.rs` — manage flow + validation context
- `unmanage.rs` — demigrate / unmanage
- `schema_registry.rs` — schema version insert/refresh
- `migration_jobs.rs` — enqueue/progress/complete
- `introspection_spi.rs` — SPI probes + `load_migration_catalog`

**Step 1:** Move functions with identical bodies.  
**Step 2:** Keep `//!` headers on each file.  
**Step 3:** `cargo check -p pg_koldstore --features pg`

### Task 3.2: Split `merge_scan/pg/profile.rs` (~1329 LOC)

**Create:**
- `profile/mod.rs` — types, `ScanProfiler`, `EmitPath`, `ColdReadProfile`
- `profile/explain.rs` — `explain_*` helpers and EXPLAIN formatting

**Do not** change EXPLAIN label strings (tests assert them).

### Task 3.3: Thin `merge_scan/pg.rs` (~1610 LOC)

**Create (as needed):**
- `pg/state.rs` — `ScanExecutionState` / thread-locals
- `pg/hooks.rs` — CustomScan hook registration entrypoints

Leave `execute.rs` / `cold.rs` / path_strategy behavior alone beyond imports.

### Task 3.4: Split `mirror/apply.rs` (~1109 LOC)

**Create:**
- `apply/mod.rs` — public apply entry
- `apply/batch.rs` — `apply_batch` + SQL cache
- `apply/types.rs` — type-name resolution / relation config helpers

**Commit:** `cleanup(pg_koldstore): split migrate, profile, merge_scan, and apply modules`

---

## Phase 4 — Verification (required before PR)

**Rules:** `test-descriptive-names`, cargo-pgrx skill

### Task 4.1: Compile + lint

```bash
cargo check -p pg_koldstore --features pg
cargo check -p pg_koldstore-shell-tests
cargo fmt --all -- --check
```

Optional: `cargo clippy -p pg_koldstore --features pg -- -D warnings` only if already clean on the branch baseline.

### Task 4.2: Focused behavioral tests

Prefer the smallest set that covers touched surfaces:

```bash
# library unit tests that do not need Postgres
cargo test -p pg_koldstore --features pg --lib -- --nocapture

# pgrx in-server (use project-standard invocation from .agents/skills/cargo-pgrx)
cargo pgrx test -p pg_koldstore
```

If full `pgrx test` is too slow for the session, at minimum run the pg_tests covering manage/flush/mirror/merge EXPLAIN that already exist under `src/pg_tests/`.

### Task 4.3: Diff discipline

**Step 1:** `git diff --stat` — expect deletes + moves >> new logic.  
**Step 2:** Reject any accidental HotChild / `cold_side_proven_empty` diffs; revert those hunks.  
**Step 3:** Confirm no new public SQL / catalog DDL.

### Task 4.4: Open PR

**Title:** `cleanup(pg_koldstore): delete dead scaffolding, normalize errors, thin clones, split modules`

**Body must include:**
- Summary bullets mapped to Phases 0–3
- rust-skills rule ids
- Explicit “locked hot-only merge path untouched”
- Test plan checklist from 4.1–4.2

---

## Suggested commit sequence (single PR)

1. `cleanup(pg_koldstore): remove unused shell scaffolding and dead SPI mocks` (Phase 0)
2. `cleanup(pg_koldstore): collapse SPI String errors into PgAdapterError` (Phase 1)
3. `cleanup(pg_koldstore): cut needless clones on cold plan and hot cursor` (Phase 2)
4. `cleanup(pg_koldstore): split migrate, profile, merge_scan, and apply modules` (Phase 3)

If a phase grows messy, keep the commit but do not start the next phase until check is green.

---

## Risk register

| Risk | Mitigation |
|------|------------|
| Mega-PR hard to review | Ordered commits + phase gates; PR description maps commit → phase |
| Accidental merge-scan regression | Diff ban on HotChild / plan-time prune; keep EXPLAIN strings |
| Shell tests break after stub deletion | Update/delete tests in same Phase 0 commit |
| Error message churn | Preserve `Display` text when wrapping |
| WIP on progressive-query branch | New branch; do not mix feature WIP |

---

## Explicitly deferred (follow-up PRs)

- Workspace-wide `koldstore-*` cleanup (catalog cache, storage, parquet)
- `ScanProfileSink` de-genericization
- Unifying `format_bytes_human` / `format_flush_bytes` string shapes
- GUC / catalog-cache file splits unless touched incidentally
- Wiring real SQLSTATE if product wants it (separate feature PR)

---

## How to invoke rust-skills during execution

For each task, ask:

```
/rust-skills review <files> for anti-over-abstraction, own-borrow-over-clone, err-custom-type, proj-mod-by-feature — prefer delete
```

Reject suggestions that add new traits, new dyn dispatch, or touch locked merge paths.
