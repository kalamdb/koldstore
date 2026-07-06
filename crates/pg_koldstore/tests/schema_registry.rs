use koldstore_catalog::{MirrorInitializationState, SchemaColumn};
use koldstore_core::{
    PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal, PrimaryKeyColumnShape, PrimaryKeyShape,
};
use pg_koldstore::migrate::register::{
    cold_metadata_config, plan_schema_registry_insert_with_id, IndexedColumnSource,
    RegistrationMetadata, INITIAL_SCHEMA_VERSION,
};
use pg_koldstore::spi::SpiAccess;
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

fn metadata() -> RegistrationMetadata {
    RegistrationMetadata {
        table_oid: 42,
        table_type: "user".to_string(),
        storage_id: Uuid::from_u128(7),
        scope_column: Some("user_id".to_string()),
        mirror_relation: Some("koldstore.items__cl".to_string()),
        primary_key_shape: Some(pk_shape()),
        initialization_state: MirrorInitializationState::Complete,
        primary_key: vec!["id".to_string()],
        columns: vec![
            SchemaColumn::app("id", "bigint", false),
            SchemaColumn::app("title", "text", false),
            SchemaColumn::app("user_id", "text", false),
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
    assert_eq!(
        plan.metadata.mirror_relation.as_deref(),
        Some("koldstore.items__cl")
    );
    assert_eq!(
        plan.metadata.primary_key_shape,
        serde_json::to_value(pk_shape()).unwrap()
    );
    assert_eq!(plan.metadata.initialization_state, "complete");
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
                "bloom_filter_columns": ["id", "created_at"],
                "bloom_candidate_columns": ["id", "created_at"],
                "indexed_columns": [
                    {
                        "column": "id",
                        "source": "primary_key",
                        "source_name": "primary_key",
                        "ordinal": 1,
                        "unique": true,
                        "primary_key": true,
                        "foreign_key": false,
                        "supports_stats": true,
                        "supports_bloom": true
                    },
                    {
                        "column": "created_at",
                        "source": "secondary_index",
                        "source_name": null,
                        "ordinal": 2,
                        "unique": false,
                        "primary_key": false,
                        "foreign_key": false,
                        "supports_stats": true,
                        "supports_bloom": true
                    }
                ],
                "ordered_indexes": []
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
                {"name": "user_id", "type_name": "text", "supported": true}
            ]
        })
    );
    assert_eq!(
        plan.metadata.options["cold_metadata"],
        serde_json::json!({
            "stats_columns": ["created_at", "title"],
            "bloom_filter_columns": ["id", "created_at", "title"],
            "bloom_candidate_columns": ["id", "created_at", "title"],
            "indexed_columns": [
                {
                    "column": "id",
                    "source": "primary_key",
                    "source_name": "primary_key",
                    "ordinal": 1,
                    "unique": true,
                    "primary_key": true,
                    "foreign_key": false,
                    "supports_stats": true,
                    "supports_bloom": true
                },
                {
                    "column": "created_at",
                    "source": "secondary_index",
                    "source_name": null,
                    "ordinal": 1,
                    "unique": false,
                    "primary_key": false,
                    "foreign_key": false,
                    "supports_stats": true,
                    "supports_bloom": true
                },
                {
                    "column": "title",
                    "source": "secondary_index",
                    "source_name": null,
                    "ordinal": 2,
                    "unique": false,
                    "primary_key": false,
                    "foreign_key": false,
                    "supports_stats": true,
                    "supports_bloom": true
                }
            ],
            "ordered_indexes": []
        })
    );
}

#[test]
fn cold_metadata_config_records_typed_sources_and_bloom_columns() {
    let config = cold_metadata_config(
        &["id".to_string()],
        &["created_at".to_string(), "tenant_id".to_string()],
    );

    assert_eq!(config.stats_columns, vec!["created_at", "tenant_id"]);
    assert_eq!(
        config.bloom_filter_columns,
        vec!["id", "created_at", "tenant_id"]
    );
    assert_eq!(config.bloom_candidate_columns, config.bloom_filter_columns);
    assert_eq!(
        config.indexed_columns[0].source,
        IndexedColumnSource::PrimaryKey
    );
    assert_eq!(
        config.indexed_columns[1].source,
        IndexedColumnSource::SecondaryIndex
    );
    assert!(config.ordered_indexes.is_empty());
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
        "$1", "$2", "$3", "$4", "$5", "$6", "$7", "$8", "$9", "$10", "$11", "$12", "$13", "$14",
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
    invalid.mirror_relation = None;
    assert!(plan_schema_registry_insert_with_id(&invalid, Uuid::from_u128(99)).is_err());

    invalid = metadata();
    invalid.primary_key_shape = None;
    assert!(plan_schema_registry_insert_with_id(&invalid, Uuid::from_u128(99)).is_err());

    invalid = metadata();
    invalid.storage_id = Uuid::nil();
    assert!(plan_schema_registry_insert_with_id(&invalid, Uuid::from_u128(99)).is_err());
}
