# Backup and Operations

pg-koldstore stores authoritative hot rows in PostgreSQL and cold segment
artifacts in object storage. A recoverable backup must include both sides.

## Base Backup and PITR

Use normal PostgreSQL base backups and WAL archiving for catalog state, heap
rows, row events, jobs, and local manifest cache tables. Point-in-time recovery
must restore PostgreSQL to a time that is consistent with the object-store
backup generation selected for cold artifacts.

## Object Backup

Back up the configured object-store prefixes for every registered
`koldstore.storage` row. Cold artifacts are retained by default during
demigration and DROP TABLE cleanup unless an operator explicitly enables
deletion.

## Validation and Recovery

`koldstore.describe_table` summarizes hot rows, cold segment counts, manifest
state, pending jobs, storage binding, and the last recorded error.
`koldstore.backup_manifest` exports the local manifest identity required to
match a PostgreSQL backup to cold files. `koldstore.validate_cold_storage`
checks manifest, Parquet, stats, PK hint, and catalog consistency surfaces.
`koldstore.recover_segments` records idempotent recovery jobs for orphan
cleanup, final-object quarantine, catalog repair, and manifest reload.

## pg_dump, COPY, and Logical Replication

`pg_dump` and `COPY (SELECT ...) TO` export the logical merged view. `COPY FROM`
is supported only for managed shared tables unless user-scope enforcement has
an active `koldstore.user_id`. Physical cold artifacts are not represented in
plain SQL dumps.

Logical replication sees hot heap changes and SQL API effects. Consumers that
need cold-only updates should read `koldstore.changes_since` using commit-order
cursors and retention-gap handling.

## Export and Import

`koldstore_exec('EXPORT TABLE ...')` is the archive boundary for writing a
kalamdb-compatible manifest and Parquet archive. `IMPORT TABLE` is intentionally
rejected until ownership, conflict handling, and schema compatibility rules are
implemented end to end.
