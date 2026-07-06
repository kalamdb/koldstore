use pg_koldstore::migrate::{
    rehydrate::{plan_demigration, DemigrateOptions, DemigrationContext},
    QualifiedTableName,
};

fn context() -> DemigrationContext {
    DemigrationContext {
        table: QualifiedTableName::parse("app.items").unwrap(),
        table_oid: 42,
        cold_object_prefix: "app/items/".to_string(),
        logical_reader_name: "koldstore.logical_items_current".to_string(),
        mirror_table: Some(QualifiedTableName::parse("koldstore.items__cl").unwrap()),
    }
}

#[test]
fn clean_schema_demigration_rehydrates_from_logical_reader_without_system_columns() {
    let plan = plan_demigration(context(), DemigrateOptions::default()).unwrap();
    let sql = plan
        .statements
        .iter()
        .map(|statement| statement.sql.as_str())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(sql.contains("CREATE TEMP TABLE pg_koldstore_demigrate_42 AS SELECT * FROM koldstore.logical_items_current"));
    assert!(sql.contains("TRUNCATE TABLE ONLY \"app\".\"items\""));
    assert!(sql.contains("INSERT INTO \"app\".\"items\" SELECT * FROM pg_koldstore_demigrate_42"));
    assert!(sql.contains("DROP TABLE IF EXISTS \"koldstore\".\"items__cl\""));
    assert!(sql.contains("UPDATE koldstore.schemas"));
    assert!(!sql.contains("_deleted"));
    assert!(!sql.contains("_seq"));
    assert!(!sql.contains("DROP COLUMN"));
}

#[test]
fn clean_schema_demigration_drops_capture_before_mirror_and_cancels_jobs() {
    let plan = plan_demigration(context(), DemigrateOptions::default()).unwrap();
    let operations = plan
        .statements
        .iter()
        .map(|statement| statement.operation.as_str())
        .collect::<Vec<_>>();

    let capture_cleanup = operations
        .iter()
        .position(|operation| *operation == "drop change-log mirror insert capture trigger")
        .unwrap();
    let mirror_drop = operations
        .iter()
        .position(|operation| *operation == "demigrate drop change-log mirror")
        .unwrap();
    let catalog_deactivation = operations
        .iter()
        .position(|operation| *operation == "demigrate deactivate catalog metadata")
        .unwrap();
    let flush_cancel = operations
        .iter()
        .position(|operation| *operation == "demigrate cancel flush jobs")
        .unwrap();

    assert!(capture_cleanup < mirror_drop);
    assert!(mirror_drop < catalog_deactivation);
    assert!(catalog_deactivation < flush_cancel);
}
