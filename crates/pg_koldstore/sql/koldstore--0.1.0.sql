-- pg-koldstore extension bootstrap catalog fragment.
--
-- This file is embedded via `pgrx::extension_sql_file!(..., bootstrap)` and is
-- NOT the packaged `koldstore--<default_version>.sql` install script (pgrx
-- generates that from Rust + this fragment). Packaged extension version comes
-- from `koldstore.control` (`default_version = '@CARGO_VERSION@'`). Upgrade
-- scripts live beside this file as `koldstore--<from>--<to>.sql`.
--
-- This fragment owns catalog DDL only. SQL-callable behavior is implemented
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
  regular_path_tmpl text NOT NULL DEFAULT '{namespace}/{tableName}/',
  scoped_path_tmpl text NOT NULL DEFAULT '{namespace}/{tableName}/{scopeId}/',
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

-- Publications are database-scoped runtime infrastructure rather than
-- extension members. Provision after catalog tables exist so CREATE EXTENSION
-- under session/shared preload cannot trip the merge-scan planner hook (it
-- probes koldstore.schemas) while the IF EXISTS publication check is planned.
DO $koldstore_publication$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_publication
    WHERE pubname = 'koldstore_async_mirror'
  ) THEN
    CREATE PUBLICATION koldstore_async_mirror;
  END IF;
END
$koldstore_publication$;

-- Logical decoding is acknowledged one fence after mirror apply. Persisting
-- the applied LSN first makes a crash retry duplicates instead of losing rows.
CREATE TABLE IF NOT EXISTS koldstore.async_mirror_state (
  database_oid oid PRIMARY KEY,
  applied_lsn pg_lsn NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

-- Async source transactions only write the heap; logical decoding and mirror
-- writes run in the always-on database worker. Hot/mirror row counters are
-- updated by the WAL applier. The worker is started at async activation,
-- auto-restarted by postmaster on crash, and re-ensured after postmaster
-- restart (shared_preload launcher and/or the first backend transaction).

CREATE TABLE IF NOT EXISTS koldstore.manifest (
  table_oid oid NOT NULL,
  scope_key text NOT NULL DEFAULT '',
  manifest_path text NOT NULL,
  etag text,
  -- Monotonic CAS generation: flush activate bumps with WHERE generation = $expected.
  generation bigint NOT NULL DEFAULT 0,
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

CREATE TABLE IF NOT EXISTS koldstore.cold_segments (
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
  -- column_stats mirrors cold_segment_stats for cache-friendly segment loads.
  -- Keep both in sync on flush; do not grow this with per-row payloads.
  status text NOT NULL CHECK (status IN ('pending', 'active', 'compacted', 'deleted')),
  -- Object identity from publish (sha256 hex + backend etag). Set at pending insert.
  checksum text,
  object_etag text,
  created_xid xid,
  created_lsn pg_lsn,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cold_segments_active_scope_seq_idx
  ON koldstore.cold_segments (table_oid, scope_key, min_seq, max_seq)
  INCLUDE (segment_id, object_path, min_commit_seq, max_commit_seq, row_count, byte_size, schema_version, object_etag, checksum)
  WHERE status = 'active';

CREATE INDEX IF NOT EXISTS cold_segments_active_commit_idx
  ON koldstore.cold_segments (table_oid, scope_key, min_commit_seq, max_commit_seq)
  WHERE status = 'active';

-- Pending expiry / recovery: find stale uploading rows without a full table scan.
CREATE INDEX IF NOT EXISTS cold_segments_pending_created_idx
  ON koldstore.cold_segments (table_oid, created_at)
  WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS koldstore.cold_segment_stats (
  segment_id uuid NOT NULL REFERENCES koldstore.cold_segments(segment_id) ON DELETE CASCADE,
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

CREATE INDEX IF NOT EXISTS cold_segment_stats_lookup_idx
  ON koldstore.cold_segment_stats (table_oid, scope_key, column_name, segment_id);

-- NOTE: Do not add per-PK catalog tables (e.g. exact cold_pk_hints). Cold
-- presence is discovered via cold_segment_stats / Parquet stats+bloom so catalog
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
  koldstore.cold_segments,
  koldstore.cold_segment_stats
FROM PUBLIC;
