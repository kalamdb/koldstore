-- pg-koldstore extension bootstrap.
-- This migration owns catalog DDL only. SQL-callable behavior is implemented
-- in Rust/pgrx modules and exposed by pgrx extension generation.
-- The koldstore schema must exist before this catalog block creates typed
-- objects under it. pgrx also emits a schema marker so schema-qualified Rust
-- functions can be generated.

CREATE SCHEMA IF NOT EXISTS koldstore;
GRANT USAGE ON SCHEMA koldstore TO PUBLIC;

CREATE TYPE koldstore.managed_table_info AS (
  table_oid oid,
  table_type text,
  storage_id uuid,
  schema_version integer,
  scope_column text
);

CREATE TYPE koldstore.dml_result AS (
  affected_rows bigint,
  tombstone_written boolean,
  cold_lookup_performed boolean
);

CREATE TYPE koldstore.change_event AS (
  commit_seq bigint,
  seq bigint,
  op text,
  pk jsonb,
  deleted boolean,
  row_image jsonb
);

CREATE TABLE IF NOT EXISTS koldstore.storage (
  id uuid PRIMARY KEY,
  name text NOT NULL UNIQUE,
  storage_type text NOT NULL CHECK (storage_type IN ('filesystem', 's3', 'gcs', 'azure')),
  base_path text NOT NULL,
  credentials jsonb NOT NULL DEFAULT '{}'::jsonb,
  config jsonb NOT NULL DEFAULT '{}'::jsonb,
  shared_path_template text NOT NULL DEFAULT '{namespace}/{tableName}/',
  user_path_template text NOT NULL DEFAULT '{namespace}/{tableName}/{scopeId}/',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS koldstore.schemas (
  id uuid PRIMARY KEY,
  table_oid oid NOT NULL,
  version integer NOT NULL,
  active boolean NOT NULL DEFAULT true,
  table_type text NOT NULL CHECK (table_type IN ('shared', 'user')),
  columns jsonb NOT NULL DEFAULT '[]'::jsonb,
  primary_key jsonb NOT NULL,
  scope_column name,
  mirror_relation regclass,
  primary_key_shape jsonb NOT NULL DEFAULT '[]'::jsonb,
  initialization_state text NOT NULL DEFAULT 'not_started'
    CHECK (initialization_state IN ('not_started', 'capturing', 'complete', 'failed')),
  indexed_columns jsonb NOT NULL DEFAULT '[]'::jsonb,
  type_matrix jsonb NOT NULL DEFAULT '{}'::jsonb,
  options jsonb NOT NULL DEFAULT '{}'::jsonb,
  storage_id uuid REFERENCES koldstore.storage(id),
  last_flush_seq bigint NOT NULL DEFAULT 0,
  last_flush_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (table_oid, version)
);

CREATE UNIQUE INDEX IF NOT EXISTS schemas_one_active_per_table_idx
  ON koldstore.schemas (table_oid)
  WHERE active;

CREATE TABLE IF NOT EXISTS koldstore.manifest (
  table_oid oid NOT NULL,
  scope_key text NOT NULL DEFAULT '',
  manifest_path text NOT NULL,
  etag text,
  generation text,
  sync_state text NOT NULL CHECK (sync_state IN ('in_sync', 'pending_write', 'syncing', 'stale', 'error')),
  segment_count integer NOT NULL DEFAULT 0,
  max_seq bigint NOT NULL DEFAULT 0,
  max_commit_seq bigint NOT NULL DEFAULT 0,
  -- PERFORMANCE: O(1) row accounting for describe/flush logging (see table_counters.rs).
  hot_row_count bigint NOT NULL DEFAULT 0,
  mirror_row_count bigint NOT NULL DEFAULT 0,
  cold_row_count bigint NOT NULL DEFAULT 0,
  last_error text,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (table_oid, scope_key)
);

CREATE INDEX IF NOT EXISTS manifest_dirty_idx
  ON koldstore.manifest (sync_state, updated_at, table_oid, scope_key)
  WHERE sync_state IN ('pending_write', 'stale', 'error');

CREATE INDEX IF NOT EXISTS manifest_scope_lookup_idx
  ON koldstore.manifest (scope_key, table_oid)
  WHERE scope_key <> '';

CREATE TABLE IF NOT EXISTS koldstore.jobs (
  id uuid PRIMARY KEY,
  table_oid oid,
  scope_key text NOT NULL DEFAULT '',
  job_type text NOT NULL,
  status text NOT NULL CHECK (status IN ('pending', 'running', 'dry_run', 'completed', 'cancelled', 'error')),
  phase text NOT NULL DEFAULT 'pending',
  priority integer NOT NULL DEFAULT 0,
  run_after timestamptz NOT NULL DEFAULT now(),
  lease_owner uuid,
  lease_expires_at timestamptz,
  lease_epoch bigint NOT NULL DEFAULT 0,
  flush_seq_upper_bound bigint,
  checkpoint_seq bigint NOT NULL DEFAULT 0,
  checkpoint_commit_seq bigint NOT NULL DEFAULT 0,
  batches_completed integer NOT NULL DEFAULT 0,
  rows_processed bigint NOT NULL DEFAULT 0,
  rows_flushed bigint NOT NULL DEFAULT 0,
  attempts integer NOT NULL DEFAULT 0,
  error_trace text,
  payload jsonb NOT NULL DEFAULT '{}'::jsonb,
  last_heartbeat_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);
-- SCALABILITY: completed/cancelled/cancelled jobs are retained forever today. Add a
-- retention/purge policy before high-frequency flush in production, or this
-- table grows without bound (one row per flush/migrate).

CREATE INDEX IF NOT EXISTS jobs_pending_idx
  ON koldstore.jobs (table_oid, scope_key, status, updated_at)
  WHERE status IN ('pending', 'running');

CREATE INDEX IF NOT EXISTS jobs_claimable_idx
  ON koldstore.jobs (status, run_after, priority DESC, updated_at, id)
  INCLUDE (table_oid, scope_key, job_type, lease_expires_at, lease_epoch, flush_seq_upper_bound)
  WHERE status IN ('pending', 'running');

CREATE INDEX IF NOT EXISTS jobs_claimable_by_type_idx
  ON koldstore.jobs (job_type, status, run_after, priority DESC, updated_at, id)
  INCLUDE (table_oid, scope_key, lease_expires_at, lease_epoch, flush_seq_upper_bound)
  WHERE status IN ('pending', 'running');

CREATE INDEX IF NOT EXISTS jobs_running_lease_idx
  ON koldstore.jobs (lease_expires_at, updated_at, id)
  WHERE status = 'running';

CREATE UNIQUE INDEX IF NOT EXISTS jobs_one_active_flush_per_scope_idx
  ON koldstore.jobs (table_oid, scope_key)
  WHERE job_type = 'flush' AND status IN ('pending', 'running');

CREATE UNIQUE INDEX IF NOT EXISTS jobs_one_active_migration_per_table_idx
  ON koldstore.jobs (table_oid)
  WHERE job_type IN ('migrate_backfill') AND status IN ('pending', 'running');

CREATE UNIQUE INDEX IF NOT EXISTS jobs_one_active_table_work_idx
  ON koldstore.jobs (table_oid)
  WHERE job_type IN ('flush', 'migrate_backfill') AND status IN ('pending', 'running');

-- Approximate flush reservations (one row per live table/scope). Not cold files.
CREATE TABLE IF NOT EXISTS koldstore.pending (
  table_oid oid NOT NULL,
  scope_key text NOT NULL DEFAULT '',
  row_count bigint NOT NULL CHECK (row_count >= 0),
  schema_version integer NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (table_oid, scope_key)
);

CREATE TABLE IF NOT EXISTS koldstore.segments (
  segment_id uuid PRIMARY KEY,
  table_oid oid NOT NULL,
  scope_key text NOT NULL DEFAULT '',
  object_path text NOT NULL,
  batch_number integer NOT NULL,
  min_seq bigint NOT NULL,
  max_seq bigint NOT NULL,
  min_commit_seq bigint NOT NULL,
  max_commit_seq bigint NOT NULL,
  row_count bigint NOT NULL,
  byte_size bigint NOT NULL,
  schema_version integer NOT NULL,
  column_stats jsonb NOT NULL DEFAULT '{}'::jsonb,
  -- column_stats mirrors segment_stats for cache-friendly segment loads.
  -- Keep both in sync on flush; do not grow this with per-row payloads.
  status text NOT NULL CHECK (status IN (
    'staged', 'published', 'superseded', 'deleting', 'deleted', 'orphaned'
  )),
  manifest_etag text,
  created_xid xid,
  created_lsn pg_lsn,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Partial key only: merge/manifest loads need heap columns (column_stats, batch_number).
CREATE INDEX IF NOT EXISTS segments_published_scope_seq_idx
  ON koldstore.segments (table_oid, scope_key, min_seq, max_seq)
  WHERE status = 'published';

CREATE INDEX IF NOT EXISTS segments_published_commit_idx
  ON koldstore.segments (table_oid, scope_key, min_commit_seq, max_commit_seq)
  WHERE status = 'published';

CREATE TABLE IF NOT EXISTS koldstore.segment_stats (
  segment_id uuid NOT NULL REFERENCES koldstore.segments(segment_id) ON DELETE CASCADE,
  table_oid oid NOT NULL,
  scope_key text NOT NULL DEFAULT '',
  column_name name NOT NULL,
  type_oid oid NOT NULL,
  min_value bytea,
  max_value bytea,
  null_count bigint,
  distinct_count bigint,
  PRIMARY KEY (segment_id, column_name)
);

CREATE INDEX IF NOT EXISTS segment_stats_lookup_idx
  ON koldstore.segment_stats (table_oid, scope_key, column_name, segment_id);

-- NOTE: Do not add per-PK catalog tables (e.g. exact cold_pk_hints). Cold
-- presence is discovered via segment_stats / Parquet stats+bloom so catalog
-- size stays O(segments × indexed columns), not O(flushed rows).

CREATE SEQUENCE IF NOT EXISTS koldstore.global_seq AS bigint;
CREATE SEQUENCE IF NOT EXISTS koldstore.global_commit_seq AS bigint;

-- PERFORMANCE: maintain O(1) row counters on koldstore.manifest (see table_counters.rs).
CREATE OR REPLACE FUNCTION koldstore.internal_ensure_manifest_row(p_table_oid oid)
RETURNS void
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, koldstore
AS $$
  INSERT INTO koldstore.manifest (
    table_oid,
    scope_key,
    manifest_path,
    sync_state
  )
  VALUES (p_table_oid, '', 'pending', 'pending_write')
  ON CONFLICT (table_oid, scope_key) DO NOTHING;
$$;

CREATE OR REPLACE FUNCTION koldstore.internal_bump_row_counts(
  p_table_oid oid,
  p_hot_delta bigint,
  p_mirror_delta bigint
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, koldstore
AS $$
BEGIN
  -- Used by commit-time counter flush and maintenance paths. DML capture triggers should call
  -- koldstore.internal_record_row_count_delta instead (in-memory, no per-row manifest IO).
  -- After a successful flush (`in_sync`), subsequent DML dirties the catalog sync_state to
  -- `pending_write` so operators and flush eligibility see hot changes.
  PERFORM koldstore.internal_ensure_manifest_row(p_table_oid);
  UPDATE koldstore.manifest
  SET
    hot_row_count = GREATEST(0, hot_row_count + p_hot_delta),
    mirror_row_count = GREATEST(0, mirror_row_count + p_mirror_delta),
    sync_state = CASE
      WHEN sync_state = 'in_sync' THEN 'pending_write'
      ELSE sync_state
    END,
    updated_at = now()
  WHERE table_oid = p_table_oid
    AND scope_key = '';
END;
$$;

CREATE OR REPLACE FUNCTION koldstore.internal_apply_flush_row_counts(
  p_table_oid oid,
  p_mirror_pruned bigint,
  p_hot_pruned bigint,
  p_cold_rows_added bigint
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, koldstore
AS $$
BEGIN
  PERFORM koldstore.internal_ensure_manifest_row(p_table_oid);
  UPDATE koldstore.manifest
  SET
    mirror_row_count = GREATEST(0, mirror_row_count - p_mirror_pruned),
    hot_row_count = GREATEST(0, hot_row_count - p_hot_pruned),
    cold_row_count = GREATEST(0, cold_row_count + p_cold_rows_added),
    updated_at = now()
  WHERE table_oid = p_table_oid
    AND scope_key = '';
END;
$$;

CREATE OR REPLACE FUNCTION koldstore.internal_refresh_row_counts(
  p_table_oid oid,
  p_hot_rows bigint,
  p_mirror_rows bigint
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, koldstore
AS $$
BEGIN
  PERFORM koldstore.internal_ensure_manifest_row(p_table_oid);
  UPDATE koldstore.manifest
  SET
    hot_row_count = GREATEST(0, p_hot_rows),
    mirror_row_count = GREATEST(0, p_mirror_rows),
    updated_at = now()
  WHERE table_oid = p_table_oid
    AND scope_key = '';
END;
$$;

REVOKE ALL ON
  koldstore.storage,
  koldstore.schemas,
  koldstore.manifest,
  koldstore.jobs,
  koldstore.pending,
  koldstore.segments,
  koldstore.segment_stats
FROM PUBLIC;
