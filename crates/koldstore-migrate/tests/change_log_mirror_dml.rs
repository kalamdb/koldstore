use koldstore_common::{
    PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal, PrimaryKeyColumnShape, PrimaryKeyShape,
};
use koldstore_migrate::{
    capture::{plan_drop_mirror_dml_triggers, plan_mirror_capture, MirrorCapturePlan},
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

fn branch<'a>(sql: &'a str, start: &str, end: Option<&str>) -> &'a str {
    let after = sql
        .split(start)
        .nth(1)
        .unwrap_or_else(|| panic!("missing branch start `{start}`"));
    match end {
        Some(end) => after
            .split(end)
            .next()
            .unwrap_or_else(|| panic!("missing branch end `{end}`")),
        None => after,
    }
}

#[test]
fn update_capture_uses_new_transition_rows_and_direct_update() {
    let plan = capture_plan(vec![pk_column("id", 1)]);
    let sql = &plan.function.sql;

    assert!(plan
        .update_trigger
        .sql
        .contains("REFERENCING NEW TABLE AS new_rows"));
    assert!(!plan.update_trigger.sql.contains("OLD TABLE"));
    assert!(sql.contains("UPDATE \"koldstore\".\"messages__cl\" AS mirror"));
    assert!(sql.contains("FROM new_rows AS src"));
    assert!(!sql.contains("FROM old_rows AS old_src"));
}

#[test]
fn delete_capture_updates_the_existing_mirror_row() {
    let plan = capture_plan(vec![pk_column("id", 1)]);
    let sql = &plan.function.sql;
    let delete_branch = branch(sql, "ELSIF TG_OP = 'DELETE' THEN", None);

    assert!(delete_branch.contains("FROM old_rows AS src"));
    assert!(delete_branch.contains("SET \"op\" = 3") || delete_branch.contains("\"op\" = 3"));
    assert!(!delete_branch.contains("ON CONFLICT"));
}

#[test]
fn pk_guard_is_separate_and_only_runs_when_pk_columns_are_targeted() {
    let plan = capture_plan(vec![pk_column("tenant_id", 1), pk_column("id", 2)]);

    assert!(plan
        .pk_guard_trigger
        .sql
        .contains("BEFORE UPDATE OF \"tenant_id\", \"id\""));
    assert!(plan.pk_guard_trigger.sql.contains("FOR EACH ROW"));
    assert!(plan
        .pk_guard_function
        .sql
        .contains("OLD.\"id\" IS DISTINCT FROM NEW.\"id\""));
    assert!(plan
        .pk_guard_function
        .sql
        .contains("OLD.\"tenant_id\" IS DISTINCT FROM NEW.\"tenant_id\""));
}

#[test]
fn insert_capture_has_small_upsert_and_bulk_merge_paths() {
    let plan = capture_plan(vec![pk_column("id", 1)]);
    let sql = &plan.function.sql;

    assert!(sql.contains("OFFSET 32"));
    assert!(sql.contains("ON CONFLICT (\"id\") DO UPDATE"));
    assert!(sql.contains("MERGE INTO \"koldstore\".\"messages__cl\" AS mirror"));
    assert!(sql.contains("WHEN MATCHED THEN"));
    assert!(sql.contains("WHEN NOT MATCHED THEN"));
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
    assert!(!trigger_sql.contains("REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows"));
    assert!(trigger_sql.contains("REFERENCING OLD TABLE AS old_rows"));
    assert!(trigger_sql
        .contains("FOR EACH STATEMENT EXECUTE FUNCTION \"koldstore\".\"messages__cl_capture\"()"));
    assert!(trigger_sql.contains("BEFORE UPDATE OF \"id\""));
    assert!(trigger_sql.contains("FOR EACH ROW"));
    assert!(!trigger_sql.contains("CONCURRENTLY"));
    assert!(!plan.function.sql.contains("COMMIT"));
}

#[test]
fn mirror_capture_preserves_composite_pk_in_conflict_and_row_values() {
    let plan = capture_plan(vec![pk_column("tenant_id", 1), pk_column("id", 2)]);
    let sql = &plan.function.sql;

    assert!(
        sql.contains(
            "INSERT INTO \"koldstore\".\"messages__cl\" (\"tenant_id\", \"id\", \"seq\", \"op\")"
        ) || sql.contains("(\"tenant_id\", \"id\", \"seq\", \"op\")")
    );
    assert!(!sql.contains("commit_lsn"));
    assert!(sql.contains("src.\"tenant_id\", src.\"id\""));
    assert!(sql.contains("ON CONFLICT (\"tenant_id\", \"id\") DO UPDATE"));
    assert!(sql.contains("mirror.\"tenant_id\" = src.\"tenant_id\""));
    assert!(sql.contains("mirror.\"id\" = src.\"id\""));
}

#[test]
fn mirror_capture_allocates_a_fresh_sequence_for_each_dml_effect() {
    let plan = capture_plan(vec![pk_column("id", 1)]);
    let count = plan.function.sql.matches("public.snowflake_id()").count();

    // Small INSERT, bulk INSERT source, UPDATE, and DELETE each allocate seq.
    assert!(
        count >= 4,
        "expected at least one snowflake_id per write path, got {count}"
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
    assert!(plan
        .drop_triggers
        .sql
        .contains("\"messages__cl_pk_update_guard\""));
    assert!(plan.drop_triggers.sql.contains("\"messages__cl_aki\""));
    assert!(plan.drop_triggers.sql.contains("\"messages__cl_aku\""));
    assert!(plan.drop_triggers.sql.contains("\"messages__cl_akd\""));
    assert!(plan
        .drop_triggers
        .sql
        .contains("\"messages__cl_async_worker_kick\""));
    assert_eq!(
        plan.drop_function.sql,
        "DROP FUNCTION IF EXISTS \"koldstore\".\"messages__cl_capture\"()"
    );
    assert_eq!(
        plan.drop_pk_guard_function.sql,
        "DROP FUNCTION IF EXISTS \"koldstore\".\"messages__cl_pk_guard\"()"
    );
}

#[test]
fn mirror_capture_records_exact_reinsert_mirror_row_delta() {
    let plan = capture_plan(vec![pk_column("id", 1)]);
    let insert_branch = branch(
        &plan.function.sql,
        "IF TG_OP = 'INSERT' THEN",
        Some("ELSIF TG_OP = 'UPDATE' THEN"),
    );

    assert!(insert_branch.contains("existing_mirror_rows"));
    assert!(insert_branch.contains("affected - existing_mirror_rows"));
}

#[test]
fn create_statements_include_guard_artifacts_in_dependency_order() {
    let plan = capture_plan(vec![pk_column("id", 1)]);
    let created = plan
        .create_statements()
        .iter()
        .map(|statement| statement.operation.as_str())
        .collect::<Vec<_>>();

    assert_eq!(
        created,
        vec![
            "create change-log mirror capture function",
            "create change-log mirror primary-key guard function",
            "create change-log mirror insert capture trigger",
            "create change-log mirror update capture trigger",
            "create change-log mirror delete capture trigger",
            "create change-log mirror primary-key guard trigger",
        ]
    );
}

#[test]
fn async_capture_switch_drops_only_statement_dml_triggers() {
    let source = QualifiedTableName::parse("public.messages").unwrap();
    let mirror = QualifiedTableName::parse("koldstore.messages__cl").unwrap();
    let statement = plan_drop_mirror_dml_triggers(&source, &mirror).unwrap();

    assert!(statement.sql.contains("messages__cl_insert_capture"));
    assert!(statement.sql.contains("messages__cl_update_capture"));
    assert!(statement.sql.contains("messages__cl_delete_capture"));
    assert!(!statement.sql.contains("messages__cl_pk_update_guard"));
    assert_eq!(statement.sql.matches("DROP TRIGGER IF EXISTS").count(), 3);
}

#[test]
fn async_worker_kick_name_matches_postgres_identifier_truncation() {
    let long = "a_very_long_managed_table_name_that_nearly_fills_a_postgres_identifier__cl";
    let names = koldstore_migrate::capture::async_worker_kick_trigger_names(long);
    assert!(names.iter().all(|name| name.len() <= 63));
    assert_ne!(names[0], names[1]);
    assert_ne!(names[1], names[2]);
    assert!(names[0].ends_with("_aki"));
    assert!(names[1].ends_with("_aku"));
    assert!(names[2].ends_with("_akd"));
}
