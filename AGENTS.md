# Agent Guidance

## Uncommitted Work Follows the Active Branch

- Never leave uncommitted changes parked on a previous branch when starting or
  switching to other work.
- If the session moves to a newer/target branch (or the user asks to continue
  elsewhere) and the working tree still has local edits, **move those changes
  onto the branch that will continue the work** before doing anything else —
  typically `git stash push -u`, checkout/switch, then `git stash pop` (or an
  equivalent carry-over). Resolve conflicts on the destination branch.
- Do not commit solely to “park” WIP unless the user explicitly asks for a
  commit. Prefer stash/carry-over so history stays clean.
- Before ending a turn that switched branches, confirm with `git status` that
  no WIP remains stranded on the old branch tip.

## Catalog SQL During Development

- Edit `crates/pg_koldstore/sql/koldstore--0.1.0.sql` directly for catalog DDL.
- Do **not** add `koldstore--<from>--<to>.sql` upgrade edges while the product
  is still in development/beta. Local installs reinstall or resync when the
  bootstrap fragment changes.
- Introduce packaged `ALTER EXTENSION … UPDATE` edges only when intentionally
  shipping a supported upgrade path.

## Formatting Is Required Before Done

- After editing Rust, run `cargo fmt --all` (or at least format the touched
  files) before ending the turn. Do not leave `cargo fmt --all -- --check`
  diffs for the user to clean up.
- Treat rustfmt output as authoritative: apply the suggested layout (line
  breaks, chaining, trailing commas, import order) rather than hand-formatting
  around it.
- If a pre-commit hook or CI fails on fmt, fix formatting in a follow-up edit
  immediately — do not ask the user to run fmt, and do not commit with
  `--no-verify` to bypass it.
- Prefer verifying with `cargo fmt --all -- --check` when claiming work is
  complete or ready to commit.

## Testing Loop

- Keep the default development and verification loop local and fast with pgrx-managed PostgreSQL.
- Tests under `tests/` should target local pgrx workflows, for example `cargo pgrx test`, `cargo pgrx install`, and pgrx-managed Postgres ports.
- Do not make `tests/` depend on Docker or Docker Compose. Docker belongs only to Docker-specific packaging and runtime checks.
- Docker-targeted scripts, Compose files, and image validation should live under `docker/` or clearly Docker-owned paths.
- Treat Docker as a final packaging smoke test, not the main correctness loop.

## Tests Must Exercise Real Bugs

- When a test reveals an extension defect, **fix the extension**. Do not weaken the test, rewrite the query to avoid the failing plan, or sort/filter in the test client to hide incorrect scan results.
- Workarounds in tests (`ORDER BY` removed, literals instead of parameters, client-side merge) are only allowed as a temporary bisect step and must be reverted once the product fix lands.
- Prefer adding a focused regression e2e that would have caught the bug (for example ordered `SELECT … LIMIT` after multi-wave flush) before calling the fix done.
- Managed-table reads must never omit hot or cold rows that should be visible, including under load, during flush, and for `ORDER BY` / `LIMIT` / parameterized plans.

## Hot-Only Merge Scan Path Is Locked

- Treat `crates/pg_koldstore/src/merge_scan/` hot-only emit and plan-time prune
  paths as performance-critical and locked:
  - plan-time early return when published cold cannot contribute
    (`cold_side_proven_empty` / empty manifest → keep native heap paths)
  - `EmitPath::HotChild` / `ScanEmitMode::HotChild` delegation
  - hot point-hit probe before Parquet open
  - catalog/cache short-circuits for absent cold segments
- Do **not** casually rewrite, “simplify,” or regress that path while fixing
  tests, EXPLAIN formatting, or unrelated scan features.
- Prefer updating tests/docs to the current contract:
  - cold-capable predicates: `KoldMergeScan` with `Hot Scan` + `Planned Access`
    + `Actual Access` (`Native PostgreSQL Child` for
    `OrderedProgressive` / `UnorderedHotFirst`; `SPI JSON Keyset Scan` only for
    `GeneralMerge` / `merge_stream`)
  - cold-proven-empty hot PK lookups: native Index/Seq/Bitmap Scan with **no**
    `KoldMergeScan` wrapper (plan-time early return; not a portfolio strategy)
- Any intentional change requires an explicit user request, a clear
  performance/correctness rationale, and verification that hot-only PK lookups
  remain fast and that hot+cold merge correctness is unchanged.

## Rust Design Preferences

- Prefer type-safe domain objects for identifiers, sequence values, table names, primary keys, and related boundaries, such as `SeqId`-style newtypes instead of raw integers or strings.
- Keep objects lightweight and explicit. Avoid broad stringly-typed APIs when a focused type or enum captures the invariant.
- Split large files by feature or responsibility when they become hard to scan.
- Split crates only when there is a clear ownership, dependency, testing, or reuse boundary.
- Favor small, composable modules over large catch-all modules.

## Crate Architecture

- Follow the layered crate layout in `docs/architecture/crate-architecture.md`.
- `koldstore-common` is the only crate with no internal `koldstore-*` dependencies.
- `pgrx` belongs only in `pg_koldstore`. Library crates must stay PostgreSQL-free.
- New domain logic goes in the lowest crate that does not need SPI, hooks, or OIDs.
- When moving code, remove dead helpers and duplicate types; do not carry unused code.

## Documentation Standard

- Every crate `lib.rs` and module file starts with a `//!` header describing ownership and purpose.
- Logic-bearing public functions need `///` docs with purpose, invariants, and `# Errors` where applicable.
- Extension `#[pg_extern]` wrappers document the SQL contract and which library crate they delegate to.
- Comments explain intent, not restate the code.
