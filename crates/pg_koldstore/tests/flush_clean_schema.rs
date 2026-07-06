use koldstore_flush::cleanup::plan_clean_schema_cleanup;
use koldstore_flush::ops::plan_mirror_flush_selection;
use koldstore_migrate::QualifiedTableName;
use pg_koldstore::spi::SqlParamType;

fn table() -> QualifiedTableName {
    QualifiedTableName::parse("app.items").unwrap()
}

fn mirror() -> QualifiedTableName {
    QualifiedTableName::parse("koldstore.items__cl").unwrap()
}

#[test]
fn mirror_backed_flush_selection_reads_mirror_and_base_rows_without_system_columns() {
    let plan = plan_mirror_flush_selection(
        &table(),
        &mirror(),
        &["id".to_string()],
        &["id".to_string(), "body".to_string()],
        None,
    )
    .unwrap();

    assert!(plan
        .statement
        .sql
        .contains("FROM \"koldstore\".\"items__cl\" AS mirror"));
    assert!(plan
        .statement
        .sql
        .contains("LEFT JOIN ONLY \"app\".\"items\" AS hot"));
    assert!(plan.statement.sql.contains("mirror.\"id\" = hot.\"id\""));
    assert!(plan.statement.sql.contains("mirror.\"seq\" <= $1::bigint"));
    assert!(plan.statement.sql.contains("mirror.\"op\""));
    assert!(plan.statement.sql.contains("mirror.\"changed_at\""));
    assert!(plan
        .statement
        .sql
        .contains("(mirror.\"op\" = 3) AS deleted"));
    assert!(plan.statement.sql.contains("ORDER BY mirror.\"seq\" ASC"));
    assert_eq!(plan.statement.param_types, vec![SqlParamType::BigInt]);

    for forbidden in ["\"_seq\"", "\"_commit_seq\"", "\"_deleted\"", "row_events"] {
        assert!(
            !plan.statement.sql.contains(forbidden),
            "mirror-backed flush selection must not use legacy fragment {forbidden}"
        );
    }
}

#[test]
fn user_scoped_flush_selection_filters_by_application_scope_column() {
    let plan = plan_mirror_flush_selection(
        &table(),
        &mirror(),
        &["tenant_id".to_string(), "id".to_string()],
        &[
            "tenant_id".to_string(),
            "id".to_string(),
            "body".to_string(),
        ],
        Some("tenant_id"),
    )
    .unwrap();

    assert!(plan
        .statement
        .sql
        .contains("\"mirror\".\"tenant_id\"::text = $2::text"));
    assert_eq!(
        plan.statement.param_types,
        vec![SqlParamType::BigInt, SqlParamType::Text]
    );
    assert!(!plan.statement.sql.contains("\"_user_id\""));
}

#[test]
fn cleanup_removes_only_selected_mirror_rows_after_manifest_commit() {
    let plan = plan_clean_schema_cleanup(&table(), &mirror(), &["id".to_string()]).unwrap();

    assert!(plan.statement.sql.contains("WITH selected AS"));
    assert!(plan
        .statement
        .sql
        .contains("DELETE FROM \"koldstore\".\"items__cl\" AS mirror"));
    assert!(plan
        .statement
        .sql
        .contains("mirror.\"id\"::text = selected.\"id\""));
    assert!(plan
        .statement
        .sql
        .contains("mirror.\"seq\" = selected.\"seq\""));
    assert!(plan
        .statement
        .sql
        .contains("DELETE FROM ONLY \"app\".\"items\" AS hot"));
    assert!(plan.statement.sql.contains("selected.\"op\" IN (1, 2)"));
    assert!(plan.statement.sql.contains("$1::jsonb"));
    assert_eq!(plan.statement.param_types, vec![SqlParamType::Jsonb]);
    assert!(!plan.statement.sql.contains("\"_deleted\""));
    assert!(!plan
        .statement
        .sql
        .contains("DELETE FROM koldstore.row_events"));
}
