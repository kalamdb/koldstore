# Agent Guidance

## Testing Loop

- Keep the default development and verification loop local and fast with pgrx-managed PostgreSQL.
- Tests under `tests/` should target local pgrx workflows, for example `cargo pgrx test`, `cargo pgrx install`, and pgrx-managed Postgres ports.
- Do not make `tests/` depend on Docker or Docker Compose. Docker belongs only to Docker-specific packaging and runtime checks.
- Docker-targeted scripts, Compose files, and image validation should live under `docker/` or clearly Docker-owned paths.
- Treat Docker as a final packaging smoke test, not the main correctness loop.

## Rust Design Preferences

- Prefer type-safe domain objects for identifiers, sequence values, table names, primary keys, and related boundaries, such as `SeqId`-style newtypes instead of raw integers or strings.
- Keep objects lightweight and explicit. Avoid broad stringly-typed APIs when a focused type or enum captures the invariant.
- Split large files by feature or responsibility when they become hard to scan.
- Split crates only when there is a clear ownership, dependency, testing, or reuse boundary.
- Favor small, composable modules over large catch-all modules.
