use koldstore_common::{
    PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal, PrimaryKeyColumnShape, PrimaryKeyShape,
};
use koldstore_migrate::{
    capture::{plan_mirror_capture, MirrorCapturePlan},
    mirror, QualifiedTableName,
};

fn pk_column(name: &str, ordinal: u16) -> PrimaryKeyColumnShape {
    PrimaryKeyColumnShape::new(
        PkColumn::new(name).unwrap(),
        PkOrdinal::new(ordinal).unwrap(),
        PgTypeOid::new(20).unwrap(),
        PgTypeName::new("bigint").unwrap(),
        PgTypmod::new(-1),
        None,
        None,
        true,
    )
}

fn capture_plan(columns: Vec<PrimaryKeyColumnShape>) -> MirrorCapturePlan {
    let source = QualifiedTableName::parse("public.messages").unwrap();
    let shape = PrimaryKeyShape::new(columns).unwrap();
    let mirror = mirror::plan_change_log_mirror(&source, &shape).unwrap();
    plan_mirror_capture(&source, &mirror.mirror_table, shape.columns()).unwrap()
}

#[test]
fn mirror_capture_upserts_insert_update_delete_latest_state() {
    let plan = capture_plan(vec![pk_column("id", 1)]);
    let sql = &plan.function.sql;

    assert!(sql.contains("IF TG_OP = 'INSERT' THEN"));
    assert!(sql.contains("ELSIF TG_OP = 'UPDATE' THEN"));
    assert!(sql.contains("ELSIF TG_OP = 'DELETE' THEN"));
    assert!(sql.contains("FROM new_rows AS src"));
    assert!(sql.contains("FROM old_rows AS src"));
    assert!(sql.contains("public.snowflake_id()"));
    assert!(sql.contains(", 1, pg_current_wal_lsn()"));
    assert!(sql.contains(", 2, pg_current_wal_lsn()"));
    assert!(sql.contains(", 3, pg_current_wal_lsn()"));
    assert!(sql.contains("ON CONFLICT (\"id\") DO UPDATE"));
    assert!(sql.contains("SET search_path = pg_catalog, koldstore"));
    assert!(sql.contains("\"seq\" = EXCLUDED.\"seq\""));
    assert!(sql.contains("\"op\" = EXCLUDED.\"op\""));
    assert!(sql.contains("\"commit_lsn\" = EXCLUDED.\"commit_lsn\""));
    assert!(!sql.contains("changed_at"));
    assert!(!sql.contains("row_data"));
    assert!(!sql.contains("cold_segment_id"));
}

#[test]
fn mirror_capture_reinsert_uses_insert_upsert_to_replace_tombstone() {
    let plan = capture_plan(vec![pk_column("id", 1)]);
    let sql = &plan.function.sql;

    let insert_branch = sql
        .split("ELSIF TG_OP = 'UPDATE' THEN")
        .next()
        .expect("insert branch exists");
    assert!(insert_branch.contains("FROM new_rows AS src"));
    assert!(insert_branch.contains(", 1, pg_current_wal_lsn()"));
    assert!(insert_branch.contains("ON CONFLICT (\"id\") DO UPDATE"));
    assert!(insert_branch.contains("\"op\" = EXCLUDED.\"op\""));
}

#[test]
fn mirror_capture_preserves_composite_pk_in_conflict_and_row_values() {
    let plan = capture_plan(vec![pk_column("tenant_id", 1), pk_column("id", 2)]);
    let sql = &plan.function.sql;

    assert!(sql.contains(
        "INSERT INTO \"koldstore\".\"messages__cl\" (\"tenant_id\", \"id\", \"seq\", \"op\", \"commit_lsn\")"
    ));
    assert!(sql.contains("src.\"tenant_id\", src.\"id\", public.snowflake_id()"));
    assert!(sql.contains("ON CONFLICT (\"tenant_id\", \"id\") DO UPDATE"));
    assert!(sql.contains("old_src.\"tenant_id\" IS NOT DISTINCT FROM new_src.\"tenant_id\""));
    assert!(sql.contains("old_src.\"id\" IS NOT DISTINCT FROM new_src.\"id\""));
}

#[test]
fn mirror_capture_installs_statement_level_after_triggers() {
    let plan = capture_plan(vec![pk_column("id", 1)]);
    let trigger_sql = plan
        .trigger_statements()
        .iter()
        .map(|statement| statement.sql.as_str())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(trigger_sql.contains("CREATE TRIGGER \"messages__cl_insert_capture\""));
    assert!(trigger_sql.contains("AFTER INSERT ON \"public\".\"messages\""));
    assert!(trigger_sql.contains("AFTER UPDATE ON \"public\".\"messages\""));
    assert!(trigger_sql.contains("AFTER DELETE ON \"public\".\"messages\""));
    assert!(trigger_sql.contains("REFERENCING NEW TABLE AS new_rows"));
    assert!(trigger_sql.contains("REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows"));
    assert!(trigger_sql.contains("REFERENCING OLD TABLE AS old_rows"));
    assert!(trigger_sql
        .contains("FOR EACH STATEMENT EXECUTE FUNCTION \"koldstore\".\"messages__cl_capture\"()"));
    assert!(!trigger_sql.contains("FOR EACH ROW"));
    assert!(!trigger_sql.contains("CONCURRENTLY"));
    assert!(!plan.function.sql.contains("COMMIT"));
}

#[test]
fn mirror_capture_allocates_a_fresh_sequence_for_each_dml_effect() {
    let plan = capture_plan(vec![pk_column("id", 1)]);
    let count = plan.function.sql.matches("public.snowflake_id()").count();

    assert_eq!(
        count, 3,
        "insert, update, and delete must each allocate a new mirror seq"
    );
}

#[test]
fn mirror_capture_cleanup_drops_triggers_before_function() {
    let plan = capture_plan(vec![pk_column("id", 1)]);

    assert!(plan.drop_triggers.sql.contains("DROP TRIGGER IF EXISTS"));
    assert!(plan
        .drop_triggers
        .sql
        .contains("\"messages__cl_insert_capture\""));
    assert!(plan
        .drop_triggers
        .sql
        .contains("\"messages__cl_update_capture\""));
    assert!(plan
        .drop_triggers
        .sql
        .contains("\"messages__cl_delete_capture\""));
    assert_eq!(
        plan.drop_function.sql,
        "DROP FUNCTION IF EXISTS \"koldstore\".\"messages__cl_capture\"()"
    );
}

#[test]
fn mirror_capture_rejects_primary_key_updates_to_prevent_stale_mirror_rows() {
    let plan = capture_plan(vec![pk_column("id", 1)]);
    let sql = &plan.function.sql;

    assert!(sql.contains("FROM old_rows AS old_src"));
    assert!(sql.contains("FROM new_rows AS new_src"));
    assert!(sql.contains("old_src.\"id\" IS NOT DISTINCT FROM new_src.\"id\""));
    assert!(sql.contains(
        "RAISE EXCEPTION 'pg-koldstore does not support primary-key updates on managed table %'"
    ));
}
