# Extension install and upgrade

KoldStore packages as a normal PostgreSQL extension named `koldstore`.

## Versioning

- Cargo / binary version: `[workspace.package].version` in the repo root
  `Cargo.toml` (also returned by `koldstore.koldstore_version()`).
- Packaged SQL `default_version`: `crates/pg_koldstore/koldstore.control` uses
  `@CARGO_VERSION@`, which `cargo pgrx install` / `package` substitutes from
  Cargo. Fresh installs therefore get `extversion` equal to the Cargo version
  (for example `0.1.4-beta.0`).
- Bootstrap catalog fragment: `crates/pg_koldstore/sql/koldstore--0.1.0.sql` is
  embedded into the generated install script; it is not the versioned install
  file name on disk after packaging.
- Upgrade scripts: `crates/pg_koldstore/sql/koldstore--<from>--<to>.sql` are
  copied next to the control file by pgrx and used by
  `ALTER EXTENSION koldstore UPDATE`.

## Install

```sql
CREATE EXTENSION koldstore;
```

Requires the shared library and control/SQL files from `cargo pgrx install` or
a release package to be present on the server.

## Upgrade

1. Install the new `.so`, `.control`, install SQL, and upgrade SQL files onto
   the PostgreSQL host (same paths `pg_config` reports for extensions).
2. In each database that has the extension:

```sql
ALTER EXTENSION koldstore UPDATE;
-- or pin the target:
-- ALTER EXTENSION koldstore UPDATE TO '0.1.4-beta.0';

SELECT extversion FROM pg_extension WHERE extname = 'koldstore';
SELECT koldstore.koldstore_version();
```

`extversion` should match the packaged default version. `koldstore_version()`
reports the loaded shared library’s Cargo version and should agree after a
correct upgrade.

When releasing a new Cargo version, add
`koldstore--<previous_cargo_version>--<new_cargo_version>.sql` and update
`PREVIOUS_EXTENSION_SQL_VERSION` in
`crates/pg_koldstore/tests/extension_upgrade.rs`. See
[release-checklist.md](../release-checklist.md).

## Production GUC baseline (async)

Prefer `ALTER DATABASE` / `ALTER SYSTEM` for background-worker GUCs (session
`SET` does not affect the worker):

| GUC | Production baseline | Notes |
|-----|---------------------|--------|
| `shared_preload_libraries` | include `koldstore` | Re-register workers after postmaster restart |
| `wal_level` | `logical` | Required for async mirror |
| `koldstore.async_mirror_max_retained_bytes` | `1073741824` (default) | Fail closed before `pg_wal` fills; raise for large catch-up, `0` only in monitored labs |
| `koldstore.flush_check_interval_seconds` | `30` (default) or tuned | Built-in auto-flush cadence |
| `koldstore.async_apply_poll_interval_ms` | `100` (default) or tuned | Apply latch poll |

Also alert on `koldstore.async_mirror_status()` (`healthy`, retained bytes,
`updated_at` age). See [scheduling.md](scheduling.md) and
[architecture/mirror-capture-async.md](../architecture/mirror-capture-async.md).
