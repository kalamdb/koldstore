-- Stable error contracts: missing PK and double-manage.
-- Catch exceptions so ON_ERROR_STOP does not abort the case file.

DO $$
BEGIN
  CREATE TABLE sqlreg.no_pk (id bigint, body text);
  BEGIN
    PERFORM koldstore.manage_table(
      table_name => 'sqlreg.no_pk'::regclass,
      storage => 'sqlreg_fs',
      hot_row_limit => 10
    );
    RAISE EXCEPTION 'expected manage_table without PK to fail';
  EXCEPTION
    WHEN OTHERS THEN
      IF position('primary key' in lower(SQLERRM)) = 0 THEN
        RAISE EXCEPTION 'unexpected error for missing PK: %', SQLERRM;
      END IF;
      RAISE NOTICE 'missing_pk_ok';
  END;
END $$;

DO $$
BEGIN
  CREATE TABLE sqlreg.dup_manage (
    id bigint PRIMARY KEY,
    body text NOT NULL
  );
  PERFORM koldstore.manage_table(
    table_name => 'sqlreg.dup_manage'::regclass,
    storage => 'sqlreg_fs',
    hot_row_limit => 10,
    min_flush_rows => 1,
    max_rows_per_file => 10,
    migration_order_by => 'id'
  );
  BEGIN
    PERFORM koldstore.manage_table(
      table_name => 'sqlreg.dup_manage'::regclass,
      storage => 'sqlreg_fs',
      hot_row_limit => 10
    );
    RAISE EXCEPTION 'expected double manage_table to fail';
  EXCEPTION
    WHEN OTHERS THEN
      IF position('already managed' in lower(SQLERRM)) = 0 THEN
        RAISE EXCEPTION 'unexpected error for double manage: %', SQLERRM;
      END IF;
      RAISE NOTICE 'double_manage_ok';
  END;
END $$;
