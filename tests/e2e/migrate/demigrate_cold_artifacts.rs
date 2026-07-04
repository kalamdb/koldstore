#[path = "../common/mod.rs"]
mod common;

use pg_koldstore::migrate::rehydrate::{
    plan_demigration, ColdArtifactAction, DemigrateOptions, DemigrationContext,
};
use pg_koldstore::migrate::QualifiedTableName;

#[test]
fn demigrate_cold_artifacts_are_retained_by_default_and_dropped_only_after_rehydrate() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let context = DemigrationContext {
        table: QualifiedTableName::parse("app.items").unwrap(),
        table_oid: 42,
        cold_object_prefix: "app/items/".to_string(),
        logical_reader_name: "KoldstoreMergeScan".to_string(),
    };

    let retain = plan_demigration(context.clone(), DemigrateOptions::default()).unwrap();
    assert_eq!(retain.cold_artifact_action, ColdArtifactAction::Retain);

    let drop_after_rehydrate = plan_demigration(
        context.clone(),
        DemigrateOptions {
            rehydrate: true,
            drop_cold: true,
            drop_system_columns: false,
        },
    )
    .unwrap();
    assert_eq!(
        drop_after_rehydrate.cold_artifact_action,
        ColdArtifactAction::DeleteAfterRehydrate {
            prefix: "app/items/".to_string()
        }
    );

    let invalid = plan_demigration(
        context,
        DemigrateOptions {
            rehydrate: false,
            drop_cold: true,
            drop_system_columns: false,
        },
    )
    .unwrap_err();
    assert_eq!(invalid.to_string(), "drop_cold requires rehydrate => true");
}
