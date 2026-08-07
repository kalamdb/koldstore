use koldstore_common::SqlAccess as SpiAccess;
use koldstore_common::{
    ColumnId, ColumnRef, ManageTableOptions, PgTypeName, PgTypeOid, PgTypmod, PkColumn, PkOrdinal,
    PrimaryKeyColumnShape, PrimaryKeyShape, StorageId, TableOid,
};
use koldstore_migrate::register::{
    cold_metadata_config, plan_schema_registry_insert_with_id, IndexedColumnSource,
    RegistrationMetadata, INITIAL_SCHEMA_VERSION,
};
use koldstore_schema::MirrorInitializationState;
use koldstore_schema::SchemaColumn;
use uuid::Uuid;

fn pk_shape() -> PrimaryKeyShape {
    PrimaryKeyShape::new(vec![PrimaryKeyColumnShape::new(
        ColumnId::from_attnum(1),
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
        table_oid: TableOid::from_raw(42),
        table_type: "user".to_string(),
        storage_id: StorageId::new("00000007").unwrap(),
        scope_column: Some("user_id".to_string()),
        mirror_relation: Some("koldstore.items__cl".to_string()),
        primary_key_shape: Some(pk_shape()),
        initialization_state: MirrorInitializationState::Complete,
        active: true,
        primary_key: vec![ColumnRef::new(ColumnId::from_attnum(1), "id")],
        columns: vec![
            SchemaColumn::app(1, "id", "bigint", false),
            SchemaColumn::app(2, "title", "text", false),
            SchemaColumn::app(3, "user_id", "text", false),
        ],
        indexed_columns: vec![
            ColumnRef::new(ColumnId::from_attnum(1), "id"),
            ColumnRef::new(ColumnId::from_attnum(4), "created_at"),
        ],
        type_matrix: serde_json::json!({"postgres": 16}),
        options: ManageTableOptions::default()
            .with_compression(koldstore_common::ParquetCompression::Zstd)
            .with_flush(1000, 1, 1000),
    }
}

#[test]
fn schema_registry_plan_captures_greenfield_metadata() {
    let plan = plan_schema_registry_insert_with_id(&metadata(), Uuid::from_u128(99)).unwrap();

    assert_eq!(plan.schema_id, Uuid::from_u128(99));
    assert_eq!(plan.metadata.table_oid.get(), 42);
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
    assert_eq!(
        plan.metadata.primary_key,
        serde_json::json!([{"column_id": 1, "name": "id"}])
    );
    assert_eq!(
        plan.metadata.indexed_columns,
        serde_json::json!([
            {"column_id": 1, "name": "id"},
            {"column_id": 4, "name": "created_at"}
        ])
    );
    assert_eq!(
        plan.metadata.type_matrix,
        serde_json::json!({"postgres": 16})
    );
    assert_eq!(
        plan.metadata.options,
        serde_json::json!({
            "compression": "zstd",
            "flush_policy": {
                "type": "row_limit",
                "hot_row_limit": 1000,
                "min_flush_rows": 1,
                "max_rows_per_file": 1000,
                "max_rows_per_flush": 10000
            },
            "cold_metadata": {
                "stats_columns": [
                    {"column_id": 1, "name": "id"},
                    {"column_id": 4, "name": "created_at"}
                ],
                "bloom_filter_columns": [
                    {"column_id": 1, "name": "id"},
                    {"column_id": 4, "name": "created_at"}
                ],
                "indexed_columns": [
                    {
                        "column_id": 1,
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
                        "column_id": 4,
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
    metadata.primary_key = vec![ColumnRef::new(ColumnId::from_attnum(1), "id")];
    metadata.indexed_columns = vec![
        ColumnRef::new(ColumnId::from_attnum(4), "created_at"),
        ColumnRef::new(ColumnId::from_attnum(2), "title"),
        ColumnRef::new(ColumnId::from_attnum(4), "created_at"),
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
            "stats_columns": [
                {"column_id": 4, "name": "created_at"},
                {"column_id": 2, "name": "title"}
            ],
            "bloom_filter_columns": [
                {"column_id": 1, "name": "id"},
                {"column_id": 4, "name": "created_at"},
                {"column_id": 2, "name": "title"}
            ],
            "indexed_columns": [
                {
                    "column_id": 1,
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
                    "column_id": 4,
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
                    "column_id": 2,
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
        &[ColumnRef::new(ColumnId::from_attnum(1), "id")],
        &[
            ColumnRef::new(ColumnId::from_attnum(2), "created_at"),
            ColumnRef::new(ColumnId::from_attnum(3), "tenant_id"),
        ],
    );

    assert_eq!(
        config.stats_columns,
        vec![
            ColumnRef::new(ColumnId::from_attnum(2), "created_at"),
            ColumnRef::new(ColumnId::from_attnum(3), "tenant_id"),
        ]
    );
    assert_eq!(
        config.bloom_filter_columns,
        vec![
            ColumnRef::new(ColumnId::from_attnum(1), "id"),
            ColumnRef::new(ColumnId::from_attnum(2), "created_at"),
            ColumnRef::new(ColumnId::from_attnum(3), "tenant_id"),
        ]
    );
    assert!(!config.bloom_filter_columns.is_empty());
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
fn cold_metadata_honors_operator_pruning_and_bloom_overrides() {
    let mut metadata = metadata();
    metadata.columns.push(SchemaColumn::app(4, "created_at", "timestamptz", false));
    metadata.options = metadata
        .options
        .with_pruning_columns(["created_at"])
        .with_bloom_filter_columns(["title"]);

    let plan = plan_schema_registry_insert_with_id(&metadata, Uuid::from_u128(99)).unwrap();
    let cold = &plan.metadata.options["cold_metadata"];

    assert_eq!(
        cold["stats_columns"],
        serde_json::json!([{"column_id": 4, "name": "created_at"}])
    );
    // PK is forced into Bloom even when the operator list only names title.
    assert_eq!(
        cold["bloom_filter_columns"],
        serde_json::json!([
            {"column_id": 1, "name": "id"},
            {"column_id": 2, "name": "title"}
        ])
    );
    assert_eq!(
        plan.metadata.options["pruning_columns"],
        serde_json::json!(["created_at"])
    );
    assert_eq!(
        plan.metadata.options["bloom_filter_columns"],
        serde_json::json!(["title"])
    );
}

#[test]
fn cold_metadata_rejects_unknown_operator_bloom_column() {
    let mut metadata = metadata();
    metadata.options = metadata
        .options
        .with_bloom_filter_columns(["not_a_column"]);

    let err = plan_schema_registry_insert_with_id(&metadata, Uuid::from_u128(99)).unwrap_err();
    assert!(
        err.to_string().contains("bloom_filter_columns"),
        "unexpected error: {err}"
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
        "$1", "$2", "$3", "$4", "$5", "$6", "$7", "$8", "$9", "$10", "$11", "$12", "$13", "$14",
        "$15",
    ] {
        assert!(
            plan.statement.sql.contains(placeholder),
            "missing placeholder {placeholder}"
        );
    }
    for literal in ["hot_row_limit", "created_at", "compression", "user_id"] {
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

    assert!(StorageId::new("").is_err());
}
