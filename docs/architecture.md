# Architecture

pg-koldstore is a PostgreSQL extension for normal heap tables. It differs from
kalamdb internals by keeping PostgreSQL as the transaction, locking, and hot-row
authority.

## PostgreSQL Heap Plus Cold Segments

Managed tables preserve the application primary key and add `_seq`,
`_commit_seq`, and `_deleted`. Hot DML stays in the heap. Flush jobs publish
cold Parquet segments and manifest records after PostgreSQL commits.

## Custom Scan Instead of DataFusion

kalamdb uses RocksDB/Raft/DataFusion internals. pg-koldstore uses PostgreSQL
planner and executor hooks plus a `KoldstoreMergeScan` Custom Scan so SQL,
MVCC, permissions, and RLS remain PostgreSQL-owned.

## Manifest Compatibility

Cold files and manifests are shaped for kalamdb-compatible readers, but publish
visibility is controlled by the PostgreSQL catalog and local manifest cache.

## Operational Boundaries

Object storage is not part of PostgreSQL WAL. Operators must back up cold
artifacts together with PostgreSQL base backups and validate manifest identity
before PITR cutover.
