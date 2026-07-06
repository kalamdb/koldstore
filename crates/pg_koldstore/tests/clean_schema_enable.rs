use koldstore_common::{
    PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal, PrimaryKeyColumnShape, PrimaryKeyShape,
};
use koldstore_migrate::{mirror, register, QualifiedTableName};
use koldstore_schema::MirrorInitializationState;
use koldstore_schema::SchemaColumn;
use uuid::Uuid;

fn pk_shape() -> PrimaryKeyShape {
    PrimaryKeyShape::new(vec![PrimaryKeyColumnShape::new(
        PkColumn::new("id").unwrap(),
        PkOrdinal::new(1).unwrap(),
        PgTypeOid::new(20).unwrap(),
        PgTypeName::new("bigint").unwrap(),
        PgTypmod::new(-1),
        None,
        None,
        true,
    )])
    .unwrap()
}

#[test]
fn clean_schema_enablement_plans_no_user_table_system_columns() {
    let source = QualifiedTableName::parse("public.messages").unwrap();
    let plan = mirror::plan_change_log_mirror(&source, &pk_shape()).unwrap();
    let planned_sql = [
        plan.collision_probe.sql.as_str(),
        plan.create_table.sql.as_str(),
        plan.seq_index.sql.as_str(),
        plan.changed_at_index.sql.as_str(),
    ]
    .join("\n");

    assert!(planned_sql.contains("CREATE TABLE IF NOT EXISTS \"koldstore\".\"messages__cl\""));
    assert!(planned_sql.contains("\"id\" bigint NOT NULL"));
    assert!(planned_sql.contains("\"seq\" bigint NOT NULL"));
    assert!(planned_sql.contains("\"op\" smallint NOT NULL"));
    assert!(planned_sql.contains("PRIMARY KEY (\"id\")"));

    for forbidden in [
        "ALTER TABLE ONLY \"public\".\"messages\"",
        "ADD COLUMN IF NOT EXISTS \"_seq\"",
        "ADD COLUMN IF NOT EXISTS \"_commit_seq\"",
        "ADD COLUMN IF NOT EXISTS \"_deleted\"",
        "ADD COLUMN IF NOT EXISTS \"_user_id\"",
    ] {
        assert!(
            !planned_sql.contains(forbidden),
            "clean-schema enablement must not plan user-table internals: {forbidden}"
        );
    }
}

#[test]
fn registry_metadata_records_clean_schema_mirror_without_system_columns() {
    let metadata = register::RegistrationMetadata {
        table_oid: 42,
        table_type: "shared".to_string(),
        storage_id: Uuid::from_u128(7),
        scope_column: None,
        mirror_relation: Some("koldstore.messages__cl".to_string()),
        primary_key_shape: Some(pk_shape()),
        initialization_state: MirrorInitializationState::Complete,
        active: true,
        primary_key: vec!["id".to_string()],
        columns: vec![
            SchemaColumn::app("id", "bigint", false),
            SchemaColumn::app("body", "text", false),
        ],
        indexed_columns: Vec::new(),
        type_matrix: serde_json::Value::Null,
        flush_policy: Some("rows:1000".to_string()),
        options: serde_json::json!({}),
    };

    let plan = register::plan_schema_registry_insert_with_id(&metadata, Uuid::from_u128(99))
        .expect("clean metadata should register");

    assert_eq!(
        plan.metadata.mirror_relation.as_deref(),
        Some("koldstore.messages__cl")
    );
    assert_eq!(plan.metadata.initialization_state, "complete");
    assert_eq!(
        plan.metadata.primary_key_shape,
        serde_json::to_value(pk_shape()).unwrap()
    );

    let columns = plan
        .metadata
        .columns
        .as_array()
        .expect("columns are serialized as an array");
    assert_eq!(columns.len(), 2);
    for forbidden in ["_seq", "_commit_seq", "_deleted", "_user_id"] {
        assert!(
            !columns
                .iter()
                .any(|column| column["name"].as_str() == Some(forbidden)),
            "clean registry metadata must not include {forbidden}"
        );
    }
}
