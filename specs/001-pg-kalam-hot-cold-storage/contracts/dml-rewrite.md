# DML Semantics Contract: pg-koldstore

**Version**: 0.3.0 (planning)
**Branch**: `001-pg-koldstore-hot-cold-storage`

pg-koldstore keeps managed tables as ordinary PostgreSQL heap tables on the hot path. The heap stores at most one row per logical primary key. Cold Parquet is read by SELECT and by explicit cold-maintenance APIs, not by every INSERT/UPDATE/DELETE.

## Architecture

```text
PostgreSQL table heap
  - one hot row per logical PK
  - native PK/indexes for hot reads and writes
  - _seq, _commit_seq, _deleted system columns

KoldstoreMergeScan
  - SELECT only
  - merges hot winner with cold winner
  - applies tombstones after merge

DML management
  - hot-row INSERT/UPDATE/DELETE stays near native
  - system columns are filled/stamped by pg-koldstore hooks
  - cold-only UPDATE requires explicit hydrate/update API
  - cold-only DELETE can write a tombstone from PK metadata without reading Parquet
```

`KoldstoreMergeScan` is not a table access method and does not make cold rows physically updateable by PostgreSQL `ModifyTable`.

## Hot Path Semantics

| User statement | Hot row exists? | Cold may contain PK? | Physical behavior |
|----------------|-----------------|----------------------|-------------------|
| `INSERT` | no | no | Native heap insert; pg-koldstore fills `_seq`, transaction `_commit_seq`, `_deleted=false`. |
| `INSERT` | no | yes / maybe | Reject as duplicate logical PK unless caller uses explicit revive/upsert API. No object-store lookup. |
| `INSERT` | hot tombstone | yes / maybe | Revive the tombstone in place through managed upsert; no second heap row. |
| `UPDATE WHERE pk = ?` | live hot row | any | Native heap update in place; pg-koldstore advances `_seq` and stamps `_commit_seq` under the transaction commit-order lock. |
| `UPDATE WHERE pk = ?` | no | yes / maybe | Standard SQL affects 0 rows in MVP. Use `koldstore.hydrate_pk` or `koldstore.update_row(..., lookup_cold => true)`. |
| `DELETE WHERE pk = ?` | live hot row | no | Native physical delete. |
| `DELETE WHERE pk = ?` | live hot row | yes / maybe | Convert hot row to one tombstone in place. |
| `DELETE WHERE pk = ?` | no | yes / maybe | `koldstore.delete_row` inserts a PK-only tombstone without reading cold data. Standard SQL support requires exact local cold PK metadata. |
| `DELETE WHERE pk = ?` | no | no | No-op. |

The normal DML path may use local PostgreSQL metadata tables (`koldstore.segments`, `koldstore.cold_pk_hints`) but MUST NOT scan Parquet or call object storage.

## Commit-Time Stamping

Every managed mutation records:

- a new `_seq` row/effect id
- a durable `_commit_seq` assigned in commit order
- a `koldstore.row_events` entry for change-feed consumers

`_commit_seq` assignment is done under a pg-koldstore transaction-scoped commit-order lock acquired on the first managed write. The lock is held until transaction end, so later managed transactions cannot commit ahead of an earlier allocated `_commit_seq`. Rollbacks can leave gaps, which consumers must tolerate.

`changes_since` MUST use `_commit_seq`, not `_seq`.

## Cold-Only UPDATE

Transparent standard SQL UPDATE of a cold-only row is not an MVP hot-path feature.

Reason: a partial update such as:

```sql
UPDATE app.items SET status = 'done' WHERE id = 42;
```

requires the old cold tuple to reconstruct every unchanged column. Doing that transparently means reading Parquet on the DML path, which violates the near-native hot-path requirement.

Supported alternatives:

```sql
-- Pull one cold row back to hot, then normal SQL UPDATE is native.
SELECT koldstore.hydrate_pk('app.items', '{"id":42}'::jsonb);
UPDATE app.items SET status = 'done' WHERE id = 42;

-- Explicit API that opts into cold lookup.
SELECT koldstore.update_row(
  'app.items',
  pk => '{"id":42}'::jsonb,
  patch => '{"status":"done"}'::jsonb,
  lookup_cold => true
);
```

If a caller supplies a complete replacement row, pg-koldstore may avoid reading cold payload, but it still must validate cold PK existence from local metadata or exact index.

## Cold-Only DELETE

Delete only needs the PK to mask old cold versions, so it can be faster than cold UPDATE.

```sql
SELECT koldstore.delete_row('app.items', '{"id":42}'::jsonb);
```

Behavior:

1. Check hot row by native PK index.
2. If hot exists, delete physically or convert to tombstone based on local cold hints.
3. If hot does not exist, check local cold PK hints.
4. If cold may contain the PK, insert one tombstone with PK columns, `_deleted=true`, new `_seq`, and transaction `_commit_seq`.
5. Do not read Parquet on the default path.

Standard SQL `DELETE WHERE pk = ?` may use the same path only when pg-koldstore can extract a simple PK equality predicate and has exact enough local metadata to preserve rowcount semantics.

## Row Events

Because the hot heap keeps only one row per PK, change-feed history cannot live in duplicate hot rows. pg-koldstore maintains an internal append-only event table:

```text
koldstore.row_events(
  table_oid,
  scope_key,
  pk_hash,
  pk_json,
  op,              -- insert | update | delete | revive
  seq,
  commit_seq,
  deleted,
  row_image_json,  -- optional/full or configured projection
  txid,
  created_at
)
```

Retention is configurable. If event retention expires, `changes_since` must return a clear gap error rather than silently skipping events.

## Internal Guards

| Guard | Purpose |
|-------|---------|
| `koldstore.internal_system_write` | Allows pg-koldstore to set `_seq`, `_commit_seq`, `_deleted`. Superuser/internal only. |
| `koldstore.internal_flush_cleanup` | Allows flush to remove hot rows after manifest commit. Superuser/internal only. |
| `koldstore.enable_merge_scan` | Diagnostic; disabling it on managed reads errors instead of returning heap-only data. |

Application roles cannot set internal GUCs. System columns cannot be written directly unless the internal guard is active.

## MVP DML Scope

Supported:

- Hot-row INSERT, UPDATE, DELETE.
- PK-predicate DELETE through `koldstore.delete_row` for cold-only rows using local cold metadata.
- `koldstore.hydrate_pk` then native UPDATE for cold-only rows.
- User-scoped DML when `koldstore.user_id` is set and matches the scope.

Deferred:

- Transparent standard SQL UPDATE of cold-only rows without explicit hydration.
- `UPDATE ... FROM`, `DELETE USING`, complex predicates, and full `RETURNING` on rewritten cold DML.
- Native FK cascade across cold rows.
- Serializable cross-store guarantees beyond PostgreSQL heap plus committed segment visibility.

## References

- [koldstore-merge-scan.md](./koldstore-merge-scan.md)
- [migration-and-columns.md](./migration-and-columns.md)
- PostgreSQL trigger and rule references: https://www.postgresql.org/docs/current/plpgsql-trigger.html, https://www.postgresql.org/docs/current/sql-createrule.html
