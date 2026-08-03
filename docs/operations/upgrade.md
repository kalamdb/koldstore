# Extension install and upgrade

KoldStore packages as a normal PostgreSQL extension named `koldstore`.

## Versioning

- Cargo / binary version: `[workspace.package].version` in the repo root
  `Cargo.toml` (also returned by `koldstore.koldstore_version()`).
- Packaged SQL `default_version`: `crates/pg_koldstore/koldstore.control` uses
  `@CARGO_VERSION@`, which `cargo pgrx install` / `package` substitutes from
  Cargo. Fresh installs therefore get `extversion` equal to the Cargo version
  (for example `0.1.8-beta.0`).
- Bootstrap catalog fragment: `crates/pg_koldstore/sql/koldstore--0.1.0.sql` is
  embedded into the generated install script; it is not the versioned install
  file name on disk after packaging.
- **Development:** edit `koldstore--0.1.0.sql` directly for catalog DDL. Do not
  add `koldstore--<from>--<to>.sql` upgrade edges while the product is still
  pre-release. Local iterative installs reinstall / resync extension SQL when
  the bootstrap fragment changes (see e2e cluster harness).

## Install

`koldstore` **must** be in `shared_preload_libraries` before
`CREATE EXTENSION` (and before `manage_table`). Reload is not enough — restart
PostgreSQL after changing the preload list. `session_preload_libraries` is not
sufficient.

```bash
# Example: Ubuntu / Debian
echo "shared_preload_libraries = 'koldstore'" | \
  sudo tee /etc/postgresql/16/main/conf.d/koldstore.conf
sudo systemctl restart postgresql@16-main
```

```sql
CREATE EXTENSION koldstore;
SELECT koldstore.preload_status();  -- loaded_via_shared_preload must be true
```

Requires the shared library and control/SQL files from `cargo pgrx install` or
a release package to be present on the server.

## Upgrade (deferred during beta)

In-place `ALTER EXTENSION koldstore UPDATE` via
`koldstore--<from>--<to>.sql` edges is **not** used during the current
development / beta series. Change catalog DDL in `koldstore--0.1.0.sql` and
reinstall the extension (or let local harnesses drop/recreate when SQL is
stale).

When a supported upgrade path is introduced for a release, document the
`ALTER EXTENSION` steps here and add the packaging edge intentionally. Until
then, treat cluster major `pg_upgrade` as an ops runbook item as well.

## Production GUC baseline (async)

Prefer `ALTER DATABASE` / `ALTER SYSTEM` for background-worker GUCs (session
`SET` does not affect the worker):

| GUC | Production baseline | Notes |
|-----|---------------------|--------|
| `shared_preload_libraries` | include `koldstore` (**required**) | Merge-scan hooks + workers; removing preload after manage is unsupported |
| `wal_level` | `logical` | Required for async mirror |
| `koldstore.async_mirror_max_retained_bytes` | `1073741824` (default) | Retained-WAL health threshold; exceeding it alerts but never stops apply. Use PostgreSQL disk/slot safeguards independently; `0` disables this threshold. |
| `koldstore.flush_check_interval_seconds` | `30` (default) or tuned | Built-in auto-flush cadence |
| `koldstore.async_apply_watchdog_interval_ms` | `30000` (default) | Safety recovery for missed commit wakeups |

`koldstore.async_apply_poll_interval_ms` was removed. Managed commits wake the
worker directly; keep only the watchdog GUC above and drop any leftover
`async_apply_poll_interval_ms` lines from `postgresql.conf` / `ALTER DATABASE`.

Also alert on `koldstore.async_mirror_status()` (`healthy`, retained bytes,
`updated_at` age). See [scheduling.md](scheduling.md) and
[architecture/mirror-capture.md](../architecture/mirror-capture.md).
