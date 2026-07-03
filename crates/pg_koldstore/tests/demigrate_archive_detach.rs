#[test]
fn archive_detach_mode_warns_about_cold_only_rows() {
    let options = pg_koldstore::migrate::rehydrate::DemigrateOptions {
        rehydrate: false,
        drop_cold: false,
        drop_system_columns: false,
    };

    assert_eq!(
        options.mode(),
        pg_koldstore::migrate::rehydrate::DemigrationMode::ArchiveDetach
    );
    assert!(!options.requires_successful_rehydrate());
}

#[test]
fn archive_detach_plan_skips_rehydrate_and_warns_about_invisible_cold_rows() {
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
        },
        DemigrateOptions {
            rehydrate: false,
            drop_cold: false,
            drop_system_columns: true,
        },
    )
    .unwrap();

    assert_eq!(plan.mode, DemigrationMode::ArchiveDetach);
    assert_eq!(plan.cold_artifact_action, ColdArtifactAction::Retain);
    assert!(plan
        .warning
        .as_deref()
        .unwrap()
        .contains("cold-only rows will not be visible"));
    assert!(!plan
        .statements
        .iter()
        .any(|statement| statement.sql.contains("CREATE TEMP TABLE")));
    assert!(plan
        .statements
        .iter()
        .any(|statement| statement.sql.contains("DROP COLUMN IF EXISTS \"_seq\"")));
}
