# Backup and Operations

KoldStore has two durability domains: PostgreSQL owns the hot heap and local
catalog state, while cold row images live in filesystem or object-store
artifacts. The developer preview does not yet ship a coordinated backup,
restore, or point-in-time-recovery protocol across those domains.

## Current recovery boundary

A PostgreSQL base backup, WAL archive, or logical dump is not sufficient to
recover a managed table after rows have been pruned from the heap. Copying an
object prefix beside a PostgreSQL backup is also not, by itself, a consistent
snapshot: catalog publication and object writes have to agree on the same
manifest generation.

Until a generation-pinning protocol is implemented, treat managed data as
non-production data unless the application has an independent authoritative
copy. Operators experimenting with manual snapshots must capture PostgreSQL
and immutable cold prefixes, record the active manifest/catalog identities,
retain every referenced object, and validate the pairing before cutover. That
is an operator procedure, not a recovery guarantee provided by KoldStore.

## `pg_dump` and `COPY`

These commands do not all enter the planner in the same way:

- `pg_dump --data-only -t managed_table` reads the physical heap and therefore
  omits rows that exist only in cold storage.
- `COPY managed_table TO ...` likewise exports the heap relation and can omit
  cold-only rows.
- `COPY (SELECT ... FROM managed_table) TO ...` plans the query and can use
  `KoldMergeScan`, so it includes cold rows for query shapes within the
  supported scan contract.
- `COPY FROM` writes the heap. It does not provide global hot+cold uniqueness or
  conflict checking.

Do not present a plain dump or direct table `COPY` as a logical backup of a
managed relation. A KoldStore-aware backup/export workflow and explicit
failure/diagnostics for unsafe dump paths are tracked separately from the
planned operator APIs in
[#103](https://github.com/kalamdb/koldstore/issues/103).
The end-to-end backup, restore, PITR, and unsafe-dump contract is tracked in
[#126](https://github.com/kalamdb/koldstore/issues/126).

## Object lifecycle

Current `DROP TABLE` cleanup deletes cold objects inline before the PostgreSQL
DDL transaction commits. Because object storage does not roll back with
PostgreSQL, aborting the transaction can leave restored catalog state pointing
at missing objects. Durable, asynchronous post-commit garbage collection is
tracked in [#100](https://github.com/kalamdb/koldstore/issues/100).

The `drop_cold` argument to `unmanage_table` is currently planned by the SQL
surface but is not executed. Do not use it as a retention guarantee.

## Available diagnostics and planned APIs

`koldstore.table_status` reports current table, manifest, segment, job, and
async-mirror information. It is operational telemetry, not a backup manifest.

The following interfaces are planned but are not shipped SQL functions:

- `koldstore.backup_manifest`
- `koldstore.validate_cold_storage`
- packaged export/import

`koldstore.recover_segments` is a maintenance surface for orphan/pending
objects; it does not create a coordinated backup or reconstruct arbitrary
missing cold data.

Logical replication captures source-heap changes, not a portable snapshot of
the cold object set. Downstream consumers must not infer that subscribing to
the source publication reproduces a managed table's existing cold history.
