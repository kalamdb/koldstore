use koldstore_common::SqlParamType;
use koldstore_flush::cleanup::plan_clean_schema_cleanup;
use koldstore_flush::ops::{plan_mirror_flush_selection, plan_mirror_flush_selection_batch};
use koldstore_migrate::QualifiedTableName;

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
    assert!(!plan.statement.sql.contains("mirror.\"changed_at\""));
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
        .contains("\"hot\".\"tenant_id\"::text = $2::text"));
    assert_eq!(
        plan.statement.param_types,
        vec![SqlParamType::BigInt, SqlParamType::Text]
    );
    assert!(!plan.statement.sql.contains("\"_user_id\""));
}

#[test]
fn batched_flush_selection_pages_by_seq_with_limit() {
    let plan = plan_mirror_flush_selection_batch(
        &table(),
        &mirror(),
        &["id".to_string()],
        &["id".to_string(), "body".to_string()],
        None,
        None,
    )
    .unwrap();

    assert!(plan.statement.sql.contains("mirror.\"seq\" > $2::bigint"));
    assert!(plan.statement.sql.contains("LIMIT $3::bigint"));
    assert!(!plan.statement.sql.contains("jsonb_agg"));
    assert_eq!(
        plan.statement.param_types,
        vec![
            SqlParamType::BigInt,
            SqlParamType::BigInt,
            SqlParamType::BigInt
        ]
    );
}

#[test]
fn batched_flush_selection_can_filter_mirror_ops() {
    let plan = plan_mirror_flush_selection_batch(
        &table(),
        &mirror(),
        &["id".to_string()],
        &["id".to_string(), "body".to_string()],
        None,
        Some(&[3]),
    )
    .unwrap();

    assert!(plan.statement.sql.contains("mirror.\"op\" = 3"));
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
    assert!(plan.statement.sql.contains("removed_mirror AS"));
    assert!(plan
        .statement
        .sql
        .contains("USING selected, removed_mirror"));
    assert!(plan
        .statement
        .sql
        .contains("DELETE FROM ONLY \"app\".\"items\" AS hot"));
    assert!(plan.statement.sql.contains("selected.\"op\" IN (1, 2)"));
    assert!(plan
        .statement
        .sql
        .contains("removed_mirror.\"seq\" = selected.\"seq\""));
    assert!(plan.statement.sql.contains("$1::jsonb"));
    assert!(plan.statement.sql.contains("mirror_pruned"));
    assert!(plan.statement.sql.contains("hot_pruned"));
    assert_eq!(plan.statement.param_types, vec![SqlParamType::Jsonb]);
    assert!(!plan.statement.sql.contains("\"_deleted\""));
    assert!(!plan
        .statement
        .sql
        .contains("DELETE FROM koldstore.row_events"));
}

#[test]
fn cleanup_deletes_mirror_and_hot_rows_in_one_atomic_statement() {
    let plan = plan_clean_schema_cleanup(&table(), &mirror(), &["id".to_string()]).unwrap();
    let sql = &plan.statement.sql;

    assert_eq!(
        sql.matches("DELETE FROM").count(),
        2,
        "cleanup must delete mirror rows in a CTE and base rows in the same statement"
    );
    assert!(
        sql.find("removed_mirror AS").expect("mirror cleanup CTE")
            < sql.find("DELETE FROM ONLY").expect("base-table cleanup"),
        "mirror rows must be removed before base rows in the unified cleanup statement"
    );
}

#[test]
fn seq_range_cleanup_deletes_by_max_seq_without_json() {
    let plan =
        koldstore_flush::plan_seq_range_cleanup(&table(), &mirror(), &["id".to_string()], None)
            .unwrap();

    assert!(plan.statement.sql.contains("mirror.\"seq\" <= $1::bigint"));
    assert!(!plan.statement.sql.contains("jsonb_to_recordset"));
    assert!(plan
        .statement
        .sql
        .contains("DELETE FROM \"koldstore\".\"items__cl\""));
    assert!(plan
        .statement
        .sql
        .contains("DELETE FROM ONLY \"app\".\"items\""));
    assert!(plan
        .statement
        .sql
        .contains("removed_mirror.\"op\" IN (1, 2)"));
    assert_eq!(plan.statement.param_types, vec![SqlParamType::BigInt]);
}

#[test]
fn seq_range_cleanup_can_filter_mirror_ops() {
    let plan = koldstore_flush::plan_seq_range_cleanup(
        &table(),
        &mirror(),
        &["id".to_string()],
        Some(&[3]),
    )
    .unwrap();

    assert!(plan.statement.sql.contains("mirror.\"op\" = 3"));
}
