use koldstore_catalog::SchemaColumn;
use pg_koldstore::migrate::register::{
    plan_schema_registry_insert_with_id, RegistrationMetadata, INITIAL_SCHEMA_VERSION,
};
use pg_koldstore::spi::SpiAccess;
use uuid::Uuid;

fn metadata() -> RegistrationMetadata {
    RegistrationMetadata {
        table_oid: 42,
        table_type: "user".to_string(),
        storage_id: Uuid::from_u128(7),
        scope_column: Some("user_id".to_string()),
        primary_key: vec!["id".to_string()],
        columns: vec![
            SchemaColumn::app("id", "bigint", false),
            SchemaColumn::app("title", "text", false),
            SchemaColumn::app("user_id", "text", false),
            SchemaColumn::system("_seq", "bigint"),
            SchemaColumn::system("_commit_seq", "bigint"),
            SchemaColumn::system("_deleted", "boolean"),
        ],
        indexed_columns: vec!["id".to_string(), "created_at".to_string()],
        type_matrix: serde_json::json!({"postgres": 16}),
        flush_policy: Some("rows:1000,interval:60".to_string()),
        options: serde_json::json!({"compression": "zstd"}),
    }
}

#[test]
fn schema_registry_plan_captures_greenfield_metadata() {
    let plan = plan_schema_registry_insert_with_id(&metadata(), Uuid::from_u128(99)).unwrap();

    assert_eq!(plan.schema_id, Uuid::from_u128(99));
    assert_eq!(plan.metadata.table_oid, 42);
    assert_eq!(plan.metadata.version, INITIAL_SCHEMA_VERSION);
    assert_eq!(plan.metadata.scope_column.as_deref(), Some("user_id"));
    assert_eq!(plan.metadata.primary_key, serde_json::json!(["id"]));
    assert_eq!(
        plan.metadata.indexed_columns,
        serde_json::json!(["id", "created_at"])
    );
    assert_eq!(
        plan.metadata.type_matrix,
        serde_json::json!({"postgres": 16})
    );
    assert_eq!(
        plan.metadata.options,
        serde_json::json!({
            "compression": "zstd",
            "flush_policy": "rows:1000,interval:60",
            "cold_metadata": {
                "stats_columns": ["id", "created_at"],
                "bloom_candidate_columns": ["id", "created_at"]
            }
        })
    );
}

#[test]
fn schema_registry_plan_derives_type_matrix_and_cold_metadata_candidates() {
    let mut metadata = metadata();
    metadata.type_matrix = serde_json::Value::Null;
    metadata.primary_key = vec!["id".to_string()];
    metadata.indexed_columns = vec![
        "created_at".to_string(),
        "title".to_string(),
        "created_at".to_string(),
    ];

    let plan = plan_schema_registry_insert_with_id(&metadata, Uuid::from_u128(99)).unwrap();

    assert_eq!(
        plan.metadata.type_matrix,
        serde_json::json!({
            "version": 1,
            "columns": [
                {"name": "id", "type_name": "bigint", "supported": true},
                {"name": "title", "type_name": "text", "supported": true},
                {"name": "user_id", "type_name": "text", "supported": true},
                {"name": "_seq", "type_name": "bigint", "supported": true},
                {"name": "_commit_seq", "type_name": "bigint", "supported": true},
                {"name": "_deleted", "type_name": "boolean", "supported": true}
            ]
        })
    );
    assert_eq!(
        plan.metadata.options["cold_metadata"],
        serde_json::json!({
            "stats_columns": ["created_at", "title"],
            "bloom_candidate_columns": ["id", "created_at", "title"]
        })
    );
}

#[test]
fn schema_registry_plan_uses_parameterized_upsert_sql() {
    let plan = plan_schema_registry_insert_with_id(&metadata(), Uuid::from_u128(99)).unwrap();

    assert_eq!(plan.statement.operation, "register managed table schema");
    assert_eq!(plan.statement.access, SpiAccess::ReadWrite);
    assert!(plan.statement.sql.contains("INSERT INTO koldstore.schemas"));
    assert!(plan
        .statement
        .sql
        .contains("ON CONFLICT (table_oid, version) DO UPDATE"));
    assert!(plan.statement.sql.contains("RETURNING s.id"));

    for placeholder in [
        "$1", "$2", "$3", "$4", "$5", "$6", "$7", "$8", "$9", "$10", "$11",
    ] {
        assert!(
            plan.statement.sql.contains(placeholder),
            "missing placeholder {placeholder}"
        );
    }
    for literal in ["rows:1000", "created_at", "compression", "user_id"] {
        assert!(
            !plan.statement.sql.contains(literal),
            "registry SQL must keep metadata in bind parameters"
        );
    }
}

#[test]
fn schema_registry_plan_rejects_incomplete_metadata() {
    let mut invalid = metadata();
    invalid.primary_key.clear();
    assert!(plan_schema_registry_insert_with_id(&invalid, Uuid::from_u128(99)).is_err());

    invalid = metadata();
    invalid.table_type = "archive".to_string();
    assert!(plan_schema_registry_insert_with_id(&invalid, Uuid::from_u128(99)).is_err());

    invalid = metadata();
    invalid.scope_column = None;
    assert!(plan_schema_registry_insert_with_id(&invalid, Uuid::from_u128(99)).is_err());

    invalid = metadata();
    invalid.storage_id = Uuid::nil();
    assert!(plan_schema_registry_insert_with_id(&invalid, Uuid::from_u128(99)).is_err());
}
