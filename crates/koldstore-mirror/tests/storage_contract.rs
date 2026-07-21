use koldstore_common::{
    MirrorOperation, PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal, PrimaryKeyColumnShape,
    TableName,
};
use koldstore_mirror::{
    mirror_relation_for_source, plan_async_mirror_batch_update, plan_async_mirror_batch_upsert,
    plan_delete_selected_mirror_rows, plan_mirror_schema, plan_mirror_stats,
    plan_select_mirror_rows_after_seq, plan_upsert_mirror_row, MirrorAccess, MirrorColumn,
    SqlParamType,
};

fn pk_shape(name: &str, type_name: &str) -> PrimaryKeyColumnShape {
    PrimaryKeyColumnShape::new(
        PkColumn::new(name).unwrap(),
        PkOrdinal::new(1).unwrap(),
        PgTypeOid::new(20).unwrap(),
        PgTypeName::new(type_name).unwrap(),
        PgTypmod::new(-1),
        None,
        None,
        true,
    )
}

#[test]
fn mirror_relation_uses_clean_schema_storage_name() {
    let source = TableName::parse("app.items").unwrap();
    let mirror = mirror_relation_for_source(&source).unwrap();

    assert_eq!(mirror.table_name().as_str(), "koldstore.items__cl");
    assert_eq!(mirror.quoted(), "\"koldstore\".\"items__cl\"");
}

#[test]
fn mirror_columns_have_stable_contract_names() {
    assert_eq!(MirrorColumn::Seq.name(), "seq");
    assert_eq!(MirrorColumn::Op.name(), "op");
    assert_eq!(MirrorColumn::ALL.len(), 2);
}

#[test]
fn mirror_schema_plan_creates_exact_pk_storage_and_indexes() {
    let mirror = mirror_relation_for_source(&TableName::parse("app.items").unwrap()).unwrap();
    let plan = plan_mirror_schema(&mirror, &[pk_shape("id", "bigint")]).unwrap();

    assert_eq!(plan.collision_probe.access, MirrorAccess::ReadOnly);
    assert_eq!(plan.create_table.access, MirrorAccess::ReadWrite);
    assert!(plan
        .create_table
        .sql
        .contains("CREATE TABLE IF NOT EXISTS \"koldstore\".\"items__cl\""));
    assert!(plan.create_table.sql.contains("\"id\" bigint NOT NULL"));
    assert!(plan.create_table.sql.contains("\"seq\" bigint NOT NULL"));
    assert!(!plan.create_table.sql.contains("commit_lsn"));
    assert!(plan.create_table.sql.contains("PRIMARY KEY (\"id\")"));
    assert!(plan
        .drop_legacy_commit_lsn
        .sql
        .contains("DROP COLUMN IF EXISTS \"commit_lsn\""));
    assert!(plan
        .seq_index
        .sql
        .contains("ON \"koldstore\".\"items__cl\" (\"seq\")"));
    assert!(plan
        .tombstone_index
        .sql
        .contains("ON \"koldstore\".\"items__cl\" (\"seq\") WHERE \"op\" = 3"));
    assert_eq!(plan.create_statements().len(), 4);
}

#[test]
fn mirror_upsert_builder_returns_latest_state_write_fragment() {
    let mirror = mirror_relation_for_source(&TableName::parse("app.items").unwrap()).unwrap();
    let sql = plan_upsert_mirror_row(
        &mirror,
        &["id"],
        &["NEW.\"id\"".to_string()],
        "SNOWFLAKE_ID()",
        MirrorOperation::Update,
    )
    .unwrap();

    assert!(sql.contains("INSERT INTO \"koldstore\".\"items__cl\""));
    assert!(sql.contains("VALUES (NEW.\"id\", SNOWFLAKE_ID(), 2)"));
    assert!(sql.contains("ON CONFLICT (\"id\") DO UPDATE"));
    assert!(!sql.contains("commit_lsn"));
}

#[test]
fn async_mirror_batch_upsert_uses_typed_unnest_and_xmax_counters() {
    let sql = plan_async_mirror_batch_upsert(
        "\"koldstore\".\"items__cl\"",
        &["id"],
        &["bigint".to_string()],
    )
    .unwrap();

    assert!(sql.contains("unnest($2::text[], $3::bigint[])"));
    assert!(sql.contains("incoming.pk_0::bigint AS \"id\""));
    assert!(sql.contains("ON CONFLICT (\"id\") DO UPDATE"));
    assert!(sql.contains("RETURNING (xmax = 0) AS inserted"));
    assert!(!sql.contains("jsonb_to_recordset"));
    assert!(!sql.contains("commit_lsn"));
    assert!(!sql.contains("existing AS"));
}

#[test]
fn async_mirror_batch_update_updates_existing_rows_then_upserts_missing_rows() {
    let sql = plan_async_mirror_batch_update(
        "\"koldstore\".\"items__cl\"",
        &["tenant_id", "id"],
        &["uuid".to_string(), "bigint".to_string()],
        "unused",
    )
    .unwrap();

    assert!(sql.contains("UPDATE \"koldstore\".\"items__cl\" AS mirror"));
    assert!(sql.contains("FROM incoming"));
    assert!(sql.contains("RETURNING mirror.\"tenant_id\", mirror.\"id\""));
    assert!(sql.contains("LEFT JOIN updated"));
    assert!(sql.contains("WHERE updated.\"tenant_id\" IS NULL"));
    assert!(sql.contains("ON CONFLICT (\"tenant_id\", \"id\") DO UPDATE"));
    assert!(sql.contains("count(*) FILTER (WHERE NOT inserted)"));
}

#[test]
fn mirror_changes_since_scan_keeps_callers_in_control_of_predicates() {
    let mirror = mirror_relation_for_source(&TableName::parse("app.items").unwrap()).unwrap();
    let scan = plan_select_mirror_rows_after_seq(
        &mirror,
        &["id"],
        1,
        3,
        &["mirror.\"tenant_id\" = $2".to_string()],
    )
    .unwrap();
    let stats = plan_mirror_stats(&mirror);

    assert!(scan
        .sql
        .contains("jsonb_build_object('id', mirror.\"id\") AS pk"));
    assert!(scan.sql.contains("mirror.\"seq\" AS commit_seq"));
    assert!(scan.sql.contains("NULL::jsonb AS row_image"));
    assert!(scan.sql.contains("mirror.\"seq\" > $1::bigint"));
    assert!(scan.sql.contains("mirror.\"tenant_id\" = $2"));
    assert!(scan.sql.contains("LIMIT $3::integer"));
    assert_eq!(
        scan.param_types,
        vec![
            SqlParamType::BigInt,
            SqlParamType::Text,
            SqlParamType::Integer
        ]
    );
    assert!(stats.sql.contains("'row_count', count(*)"));
    assert!(stats.sql.contains("FROM \"koldstore\".\"items__cl\""));
    assert!(stats.param_types.is_empty());
}

#[test]
fn selected_record_columns_match_flush_cleanup_contract() {
    let columns = koldstore_mirror::selected_record_columns(&["id"]).unwrap();
    assert_eq!(columns, "\"id\" text, \"seq\" bigint, \"op\" smallint");
}

#[test]
fn selected_delete_uses_caller_supplied_selected_set() {
    let mirror = mirror_relation_for_source(&TableName::parse("app.items").unwrap()).unwrap();
    let delete = plan_delete_selected_mirror_rows(
        &mirror,
        &["id"],
        "    SELECT * FROM jsonb_to_recordset($1::jsonb) AS selected(\"id\" text, \"seq\" bigint)",
    )
    .unwrap();

    assert!(delete.sql.contains("WITH selected AS"));
    assert!(delete
        .sql
        .contains("DELETE FROM \"koldstore\".\"items__cl\" AS mirror"));
    assert!(delete.sql.contains("mirror.\"id\"::text = selected.\"id\""));
    assert!(delete.sql.contains("mirror.\"seq\" = selected.\"seq\""));
    assert_eq!(delete.param_types, vec![SqlParamType::Jsonb]);
}
