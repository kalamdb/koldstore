# Engineering Guidance

## Working Tree and Git

- Keep WIP on the branch that continues the work. Before switching branches,
  stash/carry uncommitted changes; do not leave edits on an old branch.
- Do not commit merely to park WIP. Before ending after a branch switch, confirm
  with `git status` that no work was stranded.

## Catalog, Formatting, and Tests

- Edit `crates/pg_koldstore/sql/koldstore--0.1.0.sql` directly for catalog DDL.
  Do not add extension upgrade edges during development/beta; add them only for
  an intentionally supported upgrade path.
- After Rust edits, run `cargo fmt --all`; before handoff, verify with
  `cargo fmt --all -- --check`.
- Use local pgrx PostgreSQL for the normal test loop. Tests under `tests/` must
  not require Docker; Docker is a packaging smoke test only.
- A regression test must expose the real extension behavior. Fix the extension,
  not the test, and never hide missing hot or cold rows with client-side logic.

## Managed Reads and Merge Scan

- Keep hot-only paths fast: an empty manifest or a cold-proven-empty predicate
  keeps native heap paths; `EmitPath::HotChild` delegates; a hot PK hit occurs
  before a Parquet open.
- Do not rewrite or regress these paths for unrelated work. Any intentional
  change needs an explicit request, a performance/correctness rationale, and
  verification of both hot-only speed and hot+cold correctness.
- A query that cold storage can satisfy must use the merge path. `ORDER BY`,
  `LIMIT`, joins, parameters, flush, and concurrent DML must not hide visible
  hot or cold rows.

## Rust and Crate Boundaries

- Prefer small, type-safe domain types over stringly APIs; split large modules
  by responsibility and remove dead helpers when moving code.
- Follow [crate architecture](docs/architecture/crate-architecture.md):
  `koldstore-common` has no internal dependencies, `pgrx` stays in
  `pg_koldstore`, and domain logic belongs in the lowest layer that does not
  need PostgreSQL hooks, SPI, or OIDs.

## Documentation

- Update the relevant files in `docs/architecture/` in the same change whenever
  behavior or the operational contract materially changes: management or
  unmanagement, WAL/mirror capture, flush or cold storage, merge-scan selection
  or correctness, catalog/jobs, or table/schema/database DDL behavior.
- Do not add architecture-doc churn for a purely internal refactor. Document
  the user-visible behavior, invariants, and why the design is constrained; keep
  examples and plan/EXPLAIN contracts aligned with the code.
- Every crate `lib.rs` and module file starts with `//!`. Public logic-bearing
  functions document purpose, invariants, and `# Errors`; `#[pg_extern]`
  wrappers document their SQL contract and delegated library behavior.
