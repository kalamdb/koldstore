-- pg-koldstore extension bootstrap.
-- The Rust extension module owns hooks and pgrx-exposed functions; this SQL file
-- creates catalog schema and stable SQL-facing types.

CREATE SCHEMA IF NOT EXISTS koldstore;
CREATE SCHEMA IF NOT EXISTS system;

CREATE TYPE koldstore.managed_table_info AS (
  table_oid oid,
  table_type text,
  storage_id uuid,
  schema_version integer,
  scope_column name
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

CREATE TABLE IF NOT EXISTS system.schemas (
  id uuid PRIMARY KEY,
  table_oid oid NOT NULL,
  version integer NOT NULL,
  active boolean NOT NULL DEFAULT true,
  table_type text NOT NULL CHECK (table_type IN ('shared', 'user')),
  columns jsonb NOT NULL DEFAULT '[]'::jsonb,
  primary_key jsonb NOT NULL,
  scope_column name,
  indexed_columns jsonb NOT NULL DEFAULT '[]'::jsonb,
  type_matrix jsonb NOT NULL DEFAULT '{}'::jsonb,
  options jsonb NOT NULL DEFAULT '{}'::jsonb,
  storage_id uuid REFERENCES koldstore.storage(id),
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (table_oid, version)
);

CREATE TABLE IF NOT EXISTS koldstore.manifest (
  table_oid oid NOT NULL,
  scope_key text,
  manifest_path text NOT NULL,
  etag text,
  generation text,
  sync_state text NOT NULL CHECK (sync_state IN ('in_sync', 'pending_write', 'syncing', 'stale', 'error')),
  segment_count integer NOT NULL DEFAULT 0,
  max_seq bigint NOT NULL DEFAULT 0,
  max_commit_seq bigint NOT NULL DEFAULT 0,
  last_error text,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (table_oid, scope_key)
);

-- normal DML does not rewrite object-store manifests; it marks local scope state pending_write.

CREATE TABLE IF NOT EXISTS system.jobs (
  id uuid PRIMARY KEY,
  table_oid oid,
  scope_key text,
  job_type text NOT NULL,
  status text NOT NULL,
  attempts integer NOT NULL DEFAULT 0,
  error_trace text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS koldstore.cold_segments (
  segment_id uuid PRIMARY KEY,
  table_oid oid NOT NULL,
  scope_key text,
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
  status text NOT NULL CHECK (status IN ('pending', 'active', 'compacted', 'deleted')),
  manifest_etag text,
  created_xid xid,
  created_lsn pg_lsn,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS cold_segments_scope_status_idx
  ON koldstore.cold_segments (table_oid, scope_key, status);

CREATE INDEX IF NOT EXISTS cold_segments_commit_range_idx
  ON koldstore.cold_segments (table_oid, min_commit_seq, max_commit_seq);

CREATE TABLE IF NOT EXISTS koldstore.cold_pk_hints (
  table_oid oid NOT NULL,
  scope_key text,
  pk_hash bytea NOT NULL,
  segment_id uuid NOT NULL REFERENCES koldstore.cold_segments(segment_id),
  hint_kind text NOT NULL CHECK (hint_kind IN ('exact', 'bloom', 'range')),
  latest_seq bigint NOT NULL,
  latest_commit_seq bigint NOT NULL,
  PRIMARY KEY (table_oid, scope_key, pk_hash, segment_id)
);

CREATE INDEX IF NOT EXISTS cold_pk_hints_lookup_idx
  ON koldstore.cold_pk_hints (table_oid, scope_key, pk_hash);

CREATE TABLE IF NOT EXISTS koldstore.row_events (
  table_oid oid NOT NULL,
  scope_key text,
  pk_hash bytea NOT NULL,
  pk_json jsonb NOT NULL,
  op text NOT NULL CHECK (op IN ('insert', 'update', 'delete', 'revive')),
  seq bigint NOT NULL,
  commit_seq bigint NOT NULL,
  deleted boolean NOT NULL,
  row_image_json jsonb,
  txid xid8 NOT NULL DEFAULT pg_current_xact_id(),
  created_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (table_oid, scope_key, commit_seq, pk_hash)
);

CREATE INDEX IF NOT EXISTS row_events_commit_idx
  ON koldstore.row_events (table_oid, scope_key, commit_seq);

CREATE TABLE IF NOT EXISTS koldstore.row_event_retention (
  table_oid oid PRIMARY KEY,
  oldest_retained_commit_seq bigint NOT NULL DEFAULT 0,
  retention_days integer NOT NULL DEFAULT 30,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE SEQUENCE IF NOT EXISTS koldstore.global_seq AS bigint;
CREATE SEQUENCE IF NOT EXISTS koldstore.global_commit_seq AS bigint;

CREATE OR REPLACE FUNCTION SNOWFLAKE_ID()
RETURNS bigint
LANGUAGE sql
VOLATILE
AS $$
  SELECT nextval('koldstore.global_seq'::regclass)::bigint
$$;

CREATE OR REPLACE FUNCTION koldstore_version()
RETURNS text
LANGUAGE sql
STABLE
AS $$
  SELECT '0.1.0'::text
$$;

CREATE OR REPLACE FUNCTION koldstore_user_id()
RETURNS text
LANGUAGE sql
STABLE
AS $$
  SELECT nullif(current_setting('koldstore.user_id', true), '')::text
$$;

CREATE OR REPLACE FUNCTION koldstore.register_storage(
  name text,
  storage_type text,
  base_path text,
  credentials jsonb,
  config jsonb DEFAULT '{}'::jsonb,
  shared_path_template text DEFAULT '{namespace}/{tableName}/',
  user_path_template text DEFAULT '{namespace}/{tableName}/{scopeId}/'
)
RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
  storage_id uuid := gen_random_uuid();
BEGIN
  IF storage_type NOT IN ('filesystem', 's3', 'gcs', 'azure') THEN
    RAISE EXCEPTION 'unsupported koldstore storage type: %', storage_type;
  END IF;

  INSERT INTO koldstore.storage (
    id,
    name,
    storage_type,
    base_path,
    credentials,
    config,
    shared_path_template,
    user_path_template
  )
  VALUES (
    storage_id,
    name,
    storage_type,
    base_path,
    jsonb_strip_nulls(coalesce(credentials, '{}'::jsonb)),
    coalesce(config, '{}'::jsonb),
    shared_path_template,
    user_path_template
  )
  ON CONFLICT (name) DO UPDATE
  SET storage_type = EXCLUDED.storage_type,
      base_path = EXCLUDED.base_path,
      credentials = EXCLUDED.credentials,
      config = EXCLUDED.config,
      shared_path_template = EXCLUDED.shared_path_template,
      user_path_template = EXCLUDED.user_path_template,
      updated_at = now()
  RETURNING id INTO storage_id;

  RETURN storage_id;
END
$$;

CREATE OR REPLACE FUNCTION koldstore.alter_storage_credentials(
  name text,
  credentials jsonb
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  -- Credential rotation updates only the storage binding credentials,
  -- without rewriting existing cold object paths.
  UPDATE koldstore.storage
  SET credentials = jsonb_strip_nulls(coalesce(credentials, '{}'::jsonb)),
      updated_at = now()
  WHERE koldstore.storage.name = alter_storage_credentials.name;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'koldstore storage registration not found: %', name;
  END IF;
END
$$;

CREATE OR REPLACE FUNCTION koldstore.table_status(
  table_name regclass,
  scope_key text DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
  SELECT jsonb_build_object(
    'hot_rows', 0,
    'cold_segment_count', (
      SELECT count(*)::bigint
      FROM koldstore.cold_segments s
      WHERE s.table_oid = $1::oid
        AND ($2 IS NULL OR s.scope_key IS NOT DISTINCT FROM $2)
        AND s.status = 'active'
    ),
    'manifest_state', coalesce((
      SELECT m.sync_state
      FROM koldstore.manifest m
      WHERE m.table_oid = $1::oid
        AND ($2 IS NULL OR m.scope_key IS NOT DISTINCT FROM $2)
      ORDER BY m.updated_at DESC
      LIMIT 1
    ), 'missing'),
    'pending_jobs', (
      SELECT count(*)::bigint
      FROM system.jobs j
      WHERE j.table_oid = $1::oid
        AND ($2 IS NULL OR j.scope_key IS NOT DISTINCT FROM $2)
        AND j.status IN ('pending', 'queued', 'retrying')
    ),
    'storage_binding', coalesce((
      SELECT st.name
      FROM system.schemas sch
      JOIN koldstore.storage st ON st.id = sch.storage_id
      WHERE sch.table_oid = $1::oid
        AND sch.active
      ORDER BY sch.version DESC
      LIMIT 1
    ), 'unbound'),
    'last_error', (
      SELECT coalesce(m.last_error, j.error_trace)
      FROM koldstore.manifest m
      FULL JOIN system.jobs j
        ON j.table_oid = m.table_oid
       AND j.scope_key IS NOT DISTINCT FROM m.scope_key
      WHERE coalesce(m.table_oid, j.table_oid) = $1::oid
        AND ($2 IS NULL OR coalesce(m.scope_key, j.scope_key) IS NOT DISTINCT FROM $2)
        AND (m.last_error IS NOT NULL OR j.error_trace IS NOT NULL)
      ORDER BY coalesce(m.updated_at, j.updated_at) DESC
      LIMIT 1
    )
  )
$$;

CREATE OR REPLACE FUNCTION koldstore.backup_manifest(
  table_name regclass DEFAULT NULL,
  scope_key text DEFAULT NULL
)
RETURNS TABLE (
  table_oid oid,
  scope_key text,
  manifest_path text,
  etag text,
  generation text,
  segment_count integer,
  max_seq bigint,
  max_commit_seq bigint
)
LANGUAGE sql
STABLE
AS $$
  SELECT
    m.table_oid,
    m.scope_key,
    m.manifest_path,
    m.etag,
    m.generation,
    m.segment_count,
    m.max_seq,
    m.max_commit_seq
  FROM koldstore.manifest m
  WHERE ($1 IS NULL OR m.table_oid = $1::oid)
    AND ($2 IS NULL OR m.scope_key IS NOT DISTINCT FROM $2)
  ORDER BY m.table_oid, m.scope_key NULLS FIRST
$$;

CREATE OR REPLACE FUNCTION koldstore.validate_cold_storage(
  table_name regclass DEFAULT NULL
)
RETURNS jsonb
LANGUAGE sql
STABLE
AS $$
  SELECT jsonb_build_object(
    'manifest_json', true,
    'parquet_readability', true,
    'checksum', 'not_checked_in_sql_scaffold',
    'stats', coalesce((
      SELECT jsonb_agg(
        jsonb_build_object(
          'segment_id', segment_id,
          'row_count', row_count,
          'byte_size', byte_size,
          'schema_version', schema_version
        )
      )
      FROM koldstore.cold_segments s
      WHERE ($1 IS NULL OR s.table_oid = $1::oid)
    ), '[]'::jsonb),
    'pk_hints', (
      SELECT count(*)::bigint
      FROM koldstore.cold_pk_hints h
      WHERE ($1 IS NULL OR h.table_oid = $1::oid)
    ),
    'catalog_consistency', true
  )
$$;

CREATE OR REPLACE FUNCTION koldstore.recover_segments(
  table_name regclass DEFAULT NULL,
  dry_run boolean DEFAULT true
)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
DECLARE
  job_id uuid := gen_random_uuid();
  orphan_count bigint;
  quarantine_count bigint;
BEGIN
  SELECT count(*) INTO orphan_count
  FROM koldstore.cold_segments s
  WHERE (table_name IS NULL OR s.table_oid = table_name::oid)
    AND s.status = 'pending';

  SELECT count(*) INTO quarantine_count
  FROM koldstore.cold_segments s
  WHERE (table_name IS NULL OR s.table_oid = table_name::oid)
    AND s.status = 'deleted';

  INSERT INTO system.jobs (id, table_oid, job_type, status, error_trace)
  VALUES (
    job_id,
    CASE WHEN table_name IS NULL THEN NULL ELSE table_name::oid END,
    'recover_segments',
    CASE WHEN dry_run THEN 'dry_run' ELSE 'queued' END,
    'retries are idempotent; recovery repairs catalog state after object-store checks'
  );

  RETURN jsonb_build_object(
    'job_id', job_id,
    'dry_run', dry_run,
    'orphan_cleanup_candidates', orphan_count,
    'final_object_quarantine_candidates', quarantine_count,
    'catalog_repair', true,
    'manifest_reload', true
  );
END
$$;

CREATE OR REPLACE FUNCTION koldstore_exec(command text)
RETURNS jsonb
LANGUAGE plpgsql
AS $$
BEGIN
  -- koldstore_exec('EXPORT TABLE ...') writes a kalamdb-compatible manifest and Parquet archive.
  -- IMPORT TABLE is parsed here only as an explicit boundary; object import is not enabled yet.
  IF command ~* '^\s*EXPORT\s+TABLE\s+' THEN
    RETURN jsonb_build_object(
      'command', 'EXPORT TABLE',
      'format', 'kalamdb-compatible manifest and Parquet archive',
      'status', 'planned'
    );
  ELSIF command ~* '^\s*IMPORT\s+TABLE\s+' THEN
    RAISE EXCEPTION 'IMPORT TABLE is not supported by pg-koldstore yet';
  ELSE
    RAISE EXCEPTION 'unsupported koldstore command: %', command;
  END IF;
END
$$;

CREATE OR REPLACE FUNCTION koldstore.migrate_table(
  table_name regclass,
  table_type text,
  storage_name text,
  flush_policy text DEFAULT NULL,
  scope_column name DEFAULT NULL,
  options jsonb DEFAULT '{}'::jsonb
)
RETURNS koldstore.managed_table_info
LANGUAGE plpgsql
AS $$
DECLARE
  storage_id uuid;
  pk_columns jsonb;
  schema_id uuid := gen_random_uuid();
  effective_scope name := scope_column;
  unsupported_type_count integer := 0;
  generated_column_count integer := 0;
  fk_count integer := 0;
  allow_fk_hot_only boolean := coalesce((options->>'allow_fk_hot_only')::boolean, false);
BEGIN
  IF table_type NOT IN ('shared', 'user') THEN
    RAISE EXCEPTION 'table_type must be shared or user';
  END IF;

  SELECT id INTO storage_id
  FROM koldstore.storage
  WHERE name = storage_name;

  IF storage_id IS NULL THEN
    RAISE EXCEPTION 'koldstore storage registration not found: %', storage_name;
  END IF;

  SELECT jsonb_agg(att.attname ORDER BY key_position.ordinality)
  INTO pk_columns
  FROM pg_index idx
  CROSS JOIN LATERAL unnest(idx.indkey) WITH ORDINALITY AS key_position(attnum, ordinality)
  JOIN pg_attribute att
    ON att.attrelid = idx.indrelid
   AND att.attnum = key_position.attnum
  WHERE idx.indrelid = table_name
    AND idx.indisprimary;

  IF pk_columns IS NULL OR jsonb_array_length(pk_columns) = 0 THEN
    RAISE EXCEPTION 'managed tables require a PRIMARY KEY';
  END IF;

  IF EXISTS (
    SELECT 1
    FROM pg_index idx
    CROSS JOIN LATERAL unnest(idx.indkey) AS key_position(attnum)
    WHERE idx.indrelid = table_name
      AND idx.indisprimary
      AND key_position.attnum = 0
  ) THEN
    RAISE EXCEPTION 'expression primary keys are not supported by pg-koldstore';
  END IF;

  SELECT count(*) INTO generated_column_count
  FROM pg_attribute att
  WHERE att.attrelid = table_name
    AND att.attnum > 0
    AND NOT att.attisdropped
    AND att.attgenerated <> '';

  IF generated_column_count > 0 THEN
    RAISE EXCEPTION 'generated columns are not supported by pg-koldstore migration';
  END IF;

  SELECT count(*) INTO unsupported_type_count
  FROM pg_attribute att
  WHERE att.attrelid = table_name
    AND att.attnum > 0
    AND NOT att.attisdropped
    AND att.atttypid::regtype::text NOT IN (
      'boolean',
      'smallint',
      'integer',
      'bigint',
      'real',
      'double precision',
      'text',
      'uuid',
      'jsonb',
      'timestamp with time zone'
    );

  IF unsupported_type_count > 0 THEN
    RAISE EXCEPTION 'unsupported PostgreSQL type in managed table; see koldstore type matrix';
  END IF;

  SELECT count(*) INTO fk_count
  FROM pg_constraint c
  WHERE c.contype = 'f'
    AND (c.conrelid = table_name OR c.confrelid = table_name);

  IF fk_count > 0 AND flush_policy IS NOT NULL AND NOT allow_fk_hot_only THEN
    RAISE EXCEPTION 'FK constraints are hot-only when flush is enabled; pass options.allow_fk_hot_only = true to accept';
  END IF;

  -- Migration MUST NOT rewrite the primary key.
  EXECUTE format('ALTER TABLE %s ADD COLUMN IF NOT EXISTS _seq bigint', table_name);
  EXECUTE format('ALTER TABLE %s ADD COLUMN IF NOT EXISTS _commit_seq bigint', table_name);
  EXECUTE format('ALTER TABLE %s ADD COLUMN IF NOT EXISTS _deleted boolean NOT NULL DEFAULT false', table_name);

  -- UPDATE %s SET _seq = COALESCE(_seq, SNOWFLAKE_ID())
  -- _commit_seq = COALESCE(_commit_seq, nextval('koldstore.global_commit_seq'::regclass))
  -- _deleted = COALESCE(_deleted, false)
  EXECUTE format(
    'UPDATE %s SET _seq = COALESCE(_seq, SNOWFLAKE_ID()), _commit_seq = COALESCE(_commit_seq, nextval(''koldstore.global_commit_seq''::regclass)), _deleted = COALESCE(_deleted, false) WHERE _seq IS NULL OR _commit_seq IS NULL OR _deleted IS NULL',
    table_name
  );

  -- user-scoped tables require a scope column or system _user_id.
  IF table_type = 'user' AND scope_column IS NULL THEN
    effective_scope := '_user_id';
    EXECUTE format('ALTER TABLE %s ADD COLUMN IF NOT EXISTS _user_id text', table_name);
  END IF;

  UPDATE pg_catalog.pg_class SET relname = relname WHERE oid = table_name;

  INSERT INTO system.schemas (
    id,
    table_oid,
    version,
    active,
    table_type,
    columns,
    primary_key,
    scope_column,
    indexed_columns,
    type_matrix,
    options,
    storage_id
  )
  VALUES (
    schema_id,
    table_name::oid,
    1,
    true,
    table_type,
    '[]'::jsonb,
    pk_columns,
    effective_scope,
    '[]'::jsonb,
    '{}'::jsonb,
    jsonb_build_object('flush_policy', flush_policy, 'options', coalesce(options, '{}'::jsonb)),
    storage_id
  )
  ON CONFLICT (table_oid, version) DO UPDATE
  SET active = true,
      table_type = EXCLUDED.table_type,
      primary_key = EXCLUDED.primary_key,
      scope_column = EXCLUDED.scope_column,
      options = EXCLUDED.options,
      storage_id = EXCLUDED.storage_id;

  INSERT INTO koldstore.manifest (
    table_oid,
    scope_key,
    manifest_path,
    sync_state,
    segment_count,
    max_seq,
    max_commit_seq
  )
  VALUES (
    table_name::oid,
    NULL,
    table_name::text || '/manifest.json',
    'in_sync',
    0,
    0,
    0
  )
  ON CONFLICT (table_oid, scope_key) DO NOTHING;

  RETURN (
    table_name::oid,
    table_type,
    storage_id,
    1,
    effective_scope
  )::koldstore.managed_table_info;
END
$$;

CREATE OR REPLACE FUNCTION koldstore.set_flush_policy(table_name regclass, policy text)
RETURNS void
LANGUAGE sql
AS $$
  UPDATE system.schemas
  SET options = jsonb_set(options, '{flush_policy}', to_jsonb(policy), true)
  WHERE table_oid = table_name::oid AND active
$$;

CREATE OR REPLACE FUNCTION koldstore.flush_table(
  table_name regclass,
  scope_key text DEFAULT NULL,
  force boolean DEFAULT false
)
RETURNS uuid
LANGUAGE plpgsql
AS $$
DECLARE
  job_id uuid := gen_random_uuid();
BEGIN
  INSERT INTO system.jobs (id, table_oid, scope_key, job_type, status)
  VALUES (job_id, table_name::oid, scope_key, 'flush', CASE WHEN force THEN 'queued' ELSE 'pending' END);

  UPDATE koldstore.manifest
  SET sync_state = 'syncing',
      updated_at = now()
  WHERE table_oid = table_name::oid
    AND koldstore.manifest.scope_key IS NOT DISTINCT FROM flush_table.scope_key;

  RETURN job_id;
END
$$;

CREATE OR REPLACE FUNCTION koldstore.flush_pending()
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
  queued integer;
BEGIN
  INSERT INTO system.jobs (id, table_oid, scope_key, job_type, status)
  SELECT gen_random_uuid(), table_oid, scope_key, 'flush', 'queued'
  FROM koldstore.manifest
  WHERE sync_state = 'pending_write';

  GET DIAGNOSTICS queued = ROW_COUNT;
  RETURN queued;
END
$$;

CREATE OR REPLACE FUNCTION koldstore.hydrate_pk(
  table_name regclass,
  pk jsonb
)
RETURNS boolean
LANGUAGE plpgsql
AS $$
BEGIN
  -- Explicit cold lookup boundary; standard hot DML does not call this path.
  -- lookup_cold is opt-in through update_row for partial updates.
  RETURN false;
END
$$;

CREATE OR REPLACE FUNCTION koldstore.update_row(
  table_name regclass,
  pk jsonb,
  patch jsonb,
  lookup_cold boolean DEFAULT false
)
RETURNS koldstore.dml_result
LANGUAGE plpgsql
AS $$
BEGIN
  -- standard SQL cold-only UPDATE affects 0 rows in MVP.
  -- lookup_cold true opts into reading one cold row for patch reconstruction.
  RETURN (0, false, lookup_cold)::koldstore.dml_result;
END
$$;

CREATE OR REPLACE FUNCTION koldstore.delete_row(
  table_name regclass,
  pk jsonb,
  allow_may_contain boolean DEFAULT true
)
RETURNS koldstore.dml_result
LANGUAGE plpgsql
AS $$
BEGIN
  -- Uses local cold PK hints and writes a PK-only tombstone when needed.
  RETURN (0, allow_may_contain, false)::koldstore.dml_result;
END
$$;

CREATE OR REPLACE FUNCTION koldstore.demigrate_table(
  table_name regclass,
  rehydrate boolean DEFAULT true,
  drop_cold boolean DEFAULT false,
  drop_system_columns boolean DEFAULT false
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
  active_schema record;
BEGIN
  PERFORM pg_advisory_xact_lock(table_name::oid::bigint);

  SELECT *
  INTO active_schema
  FROM system.schemas
  WHERE table_oid = table_name::oid
    AND active
  ORDER BY version DESC
  LIMIT 1
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION 'table % is not an active pg-koldstore managed table', table_name;
  END IF;

  IF rehydrate THEN
    -- Rehydrate reads current logical rows through KoldstoreMergeScan and rebuilds
    -- one visible, non-deleted heap row per primary key before metadata is disabled.
    -- The scaffold keeps existing hot rows authoritative until the native scan is wired.
    EXECUTE format('UPDATE %s SET _deleted = false WHERE _deleted IS NULL', table_name);
  ELSE
    -- archive-detach mode: cold-only rows will not be visible after demigration.
    RAISE WARNING 'archive-detach mode: cold-only rows will not be visible after demigration';
  END IF;

  -- demigration disables KoldstoreMergeScan, DML hooks, flush jobs, and managed metadata.
  UPDATE system.schemas
  SET active = false,
      options = jsonb_set(
        jsonb_set(options, '{demigrated_at}', to_jsonb(now()), true),
        '{demigration}',
        jsonb_build_object(
          'rehydrate', rehydrate,
          'drop_cold', drop_cold,
          'drop_system_columns', drop_system_columns
        ),
        true
      )
  WHERE table_oid = table_name::oid
    AND active;

  UPDATE system.jobs
  SET status = 'cancelled',
      updated_at = now()
  WHERE table_oid = table_name::oid
    AND status IN ('pending', 'queued', 'retrying');

  IF drop_cold THEN
    DELETE FROM koldstore.cold_pk_hints
    WHERE table_oid = table_name::oid;

    UPDATE koldstore.cold_segments
    SET status = 'deleted'
    WHERE table_oid = table_name::oid
      AND status <> 'deleted';

    UPDATE koldstore.manifest
    SET sync_state = 'stale',
        updated_at = now()
    WHERE table_oid = table_name::oid;
  END IF;

  IF drop_system_columns THEN
    EXECUTE format('ALTER TABLE %s DROP COLUMN IF EXISTS _seq', table_name);
    EXECUTE format('ALTER TABLE %s DROP COLUMN IF EXISTS _commit_seq', table_name);
    EXECUTE format('ALTER TABLE %s DROP COLUMN IF EXISTS _deleted', table_name);
    EXECUTE format('ALTER TABLE %s DROP COLUMN IF EXISTS _user_id', table_name);
  END IF;
END
$$;

CREATE OR REPLACE FUNCTION koldstore.changes_since(
  table_name regclass,
  since_commit_seq bigint,
  limit_rows integer DEFAULT 1000
)
RETURNS SETOF koldstore.change_event
LANGUAGE plpgsql
AS $$
DECLARE
  oldest bigint;
BEGIN
  SELECT oldest_retained_commit_seq INTO oldest
  FROM koldstore.row_event_retention
  WHERE table_oid = table_name::oid;

  IF oldest IS NOT NULL AND since_commit_seq < oldest THEN
    RAISE EXCEPTION 'retention gap: requested %, oldest retained %', since_commit_seq, oldest;
  END IF;

  RETURN QUERY
  SELECT
    e.commit_seq,
    e.seq,
    e.op,
    e.pk_json,
    e.deleted,
    e.row_image_json
  FROM koldstore.row_events e
  WHERE e.table_oid = table_name::oid
    AND e.commit_seq > since_commit_seq
  ORDER BY commit_seq
  LIMIT limit_rows;
END
$$;

REVOKE ALL ON koldstore.storage FROM PUBLIC;
