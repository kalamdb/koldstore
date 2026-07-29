-- Upgrade koldstore from 0.1.0 to 0.1.8-beta.0.
--
-- Catalog layout in this release (also the current 0.1.0 bootstrap):
--   - koldstore.storage.id is text (8-char hex StorageId), not uuid
--   - koldstore.cold_segments.path replaces object_path (table-relative keys)
--   - koldstore.manifest no longer stores manifest_path (derived from path tmpl)
--
-- Fresh installs and pgrx test clusters already have that shape via the
-- embedded 0.1.0 bootstrap. In-place UPDATE from an older 0.1.0 catalog that
-- still has uuid storage ids / object_path / manifest_path is not supported in
-- the beta series — reinstall (DROP EXTENSION … CASCADE; CREATE EXTENSION).
--
-- SQL-callable entry points are LANGUAGE c / pgrx wrappers resolved through
-- MODULE_PATHNAME; install the matching shared library before running
-- ALTER EXTENSION koldstore UPDATE.
--
-- During the 0.1.x beta series, keep a single edge from 0.1.0 to the current
-- Cargo version (rename this file on each beta bump; do not chain beta→beta).

DO $koldstore_upgrade$
DECLARE
  storage_id_type text;
  has_object_path boolean;
  has_manifest_path boolean;
BEGIN
  SELECT format_type(a.atttypid, a.atttypmod)
    INTO storage_id_type
  FROM pg_catalog.pg_attribute a
  JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
  JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = 'koldstore'
    AND c.relname = 'storage'
    AND a.attname = 'id'
    AND NOT a.attisdropped;

  SELECT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_attribute a
    JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'koldstore'
      AND c.relname = 'cold_segments'
      AND a.attname = 'object_path'
      AND NOT a.attisdropped
  ) INTO has_object_path;

  SELECT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_attribute a
    JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
    JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'koldstore'
      AND c.relname = 'manifest'
      AND a.attname = 'manifest_path'
      AND NOT a.attisdropped
  ) INTO has_manifest_path;

  IF storage_id_type IS DISTINCT FROM 'text'
     OR has_object_path
     OR has_manifest_path THEN
    RAISE EXCEPTION
      'koldstore 0.1.8-beta.0 catalog layout requires reinstall (storage.id text, cold_segments.path, no manifest.manifest_path). DROP EXTENSION koldstore CASCADE; CREATE EXTENSION koldstore;';
  END IF;
END
$koldstore_upgrade$;
