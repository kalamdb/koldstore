# Chat history example

WhatsApp / Intercom-style tenant-scoped messaging with deep flush and overlay coverage.

## What the test covers

- Parallel inserts across many tenants/conversations
- Multi-wave flush cycles (`flush` → more inserts → `flush` → force flush)
- Small Parquet files bounded by `max_rows_per_file`, plus on-disk/manifest checks
- Application indexes on `(tenant_id, conversation_id, created_at)` and sender paths
- Concurrent hot UPDATE/DELETE while traffic continues
- Cold-then-delete overlay: flush a row cold, rematerialize it hot, `DELETE`, flush the tombstone, verify merge scan hides it while prior Parquet remains
- Cross-tenant isolation after one tenant deletes history

## Production policy

```sql
SELECT koldstore.manage_table(
  table_name        => 'chat.messages',
  storage           => 'local-dev',
  hot_row_limit     => 10000,
  min_flush_rows    => 1000,
  max_rows_per_file => 500,
  table_type        => 'user',
  scope_column      => 'tenant_id',
  order_column      => 'created_at'
);
```

Example runs scale these limits down while keeping the same semantics.
