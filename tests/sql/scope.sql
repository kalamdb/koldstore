-- User-scoped manage + scoped SELECT after flush.
-- Uses a non-superuser role so FORCE ROW LEVEL SECURITY actually filters.
-- Roles are cluster-wide; recreate idempotently across cases on one cluster.

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sqlreg_scoped') THEN
    DROP OWNED BY sqlreg_scoped;
    DROP ROLE sqlreg_scoped;
  END IF;
END $$;
CREATE ROLE sqlreg_scoped NOINHERIT LOGIN;
GRANT USAGE ON SCHEMA sqlreg TO sqlreg_scoped;
ALTER DEFAULT PRIVILEGES IN SCHEMA sqlreg
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO sqlreg_scoped;

CREATE TABLE sqlreg.scope_notes (
  id bigint PRIMARY KEY,
  user_id text NOT NULL,
  title text NOT NULL
);

INSERT INTO sqlreg.scope_notes (id, user_id, title) VALUES
  (1, 'user-a', 'a1'),
  (2, 'user-a', 'a2'),
  (3, 'user-b', 'b1'),
  (4, 'user-b', 'b2'),
  (5, 'user-b', 'b3');

SELECT koldstore.manage_table(
  table_name => 'sqlreg.scope_notes'::regclass,
  storage => 'sqlreg_fs',
  table_type => 'user',
  scope_column => 'user_id',
  hot_row_limit => 2,
  min_flush_rows => 1,
  max_rows_per_file => 8,
  migration_order_by => 'id',
  auto_flush => false
);

ALTER TABLE sqlreg.scope_notes FORCE ROW LEVEL SECURITY;
GRANT SELECT, INSERT, UPDATE, DELETE ON sqlreg.scope_notes TO sqlreg_scoped;

SET koldstore.user_id = 'user-a';
SELECT sqlreg.flush_table('sqlreg.scope_notes'::regclass);

SET ROLE sqlreg_scoped;
SET koldstore.user_id = 'user-a';
SELECT count(*)::bigint AS user_a_count FROM sqlreg.scope_notes;
SELECT id, title FROM sqlreg.scope_notes ORDER BY id;

SET koldstore.user_id = 'user-b';
SELECT count(*)::bigint AS user_b_count FROM sqlreg.scope_notes;
SELECT id, title FROM sqlreg.scope_notes ORDER BY id;

RESET koldstore.user_id;
SELECT count(*)::bigint AS missing_scope_count FROM sqlreg.scope_notes;
RESET ROLE;
