-- Upgrade koldstore from 0.1.0 to 0.1.4-beta.0.
--
-- Catalog DDL is unchanged from the 0.1.0 bootstrap. SQL-callable entry points
-- are LANGUAGE c / pgrx wrappers resolved through MODULE_PATHNAME; install the
-- matching shared library before running ALTER EXTENSION koldstore UPDATE.
--
-- This script establishes a real ALTER EXTENSION UPDATE path so installed
-- `extversion` can track the packaged default_version (Cargo package version).

-- no catalog migrations in this step
