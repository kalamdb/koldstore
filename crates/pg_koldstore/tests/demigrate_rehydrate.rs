#[test]
fn sql_exposes_demigrate_table_with_rehydrate_default() {
    let options = pg_koldstore::migrate::rehydrate::DemigrateOptions::default();

    assert!(options.rehydrate);
    assert_eq!(
        options.mode(),
        pg_koldstore::migrate::rehydrate::DemigrationMode::Rehydrate
    );
}

#[test]
fn default_demigration_plan_rehydrates_current_rows_and_retains_cold_artifacts() {
    use pg_koldstore::migrate::rehydrate::{
        plan_demigration, ColdArtifactAction, DemigrateOptions, DemigrationContext, DemigrationMode,
    };
    use pg_koldstore::migrate::QualifiedTableName;

    let plan = plan_demigration(
        DemigrationContext {
            table: QualifiedTableName::parse("app.items").unwrap(),
            table_oid: 42,
            cold_object_prefix: "app/items/".to_string(),
            logical_reader_name: "KoldstoreMergeScan".to_string(),
            mirror_table: Some(QualifiedTableName::parse("koldstore.items__cl").unwrap()),
        },
        DemigrateOptions::default(),
    )
    .unwrap();

    assert_eq!(plan.mode, DemigrationMode::Rehydrate);
    assert_eq!(plan.lock.table_oid, 42);
    assert_eq!(plan.cold_artifact_action, ColdArtifactAction::Retain);
    assert!(plan.warning.is_none());
    assert!(plan
        .statements
        .iter()
        .any(|statement| statement.sql.contains("CREATE TEMP TABLE")));
    assert!(plan
        .statements
        .iter()
        .any(|statement| statement.sql.contains("INSERT INTO \"app\".\"items\"")));
    assert!(plan
        .statements
        .iter()
        .any(|statement| statement.sql.contains("UPDATE koldstore.schemas")));
}

#[test]
fn demigrate_table_request_maps_sql_defaults_to_demigration_options() {
    let request = pg_koldstore::sql::ddl::DemigrateTableRequest {
        table_name: "app.items".to_string(),
        rehydrate: None,
        drop_cold: None,
    };

    let options = request.options();

    assert_eq!(request.table_name, "app.items");
    assert!(options.rehydrate);
    assert!(!options.drop_cold);
}
