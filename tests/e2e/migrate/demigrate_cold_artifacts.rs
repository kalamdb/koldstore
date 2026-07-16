use crate::common;

use anyhow::Result;
use koldstore_migrate::rehydrate::{
    plan_demigration, ColdArtifactAction, DemigrateOptions, DemigrationContext,
};
use koldstore_migrate::QualifiedTableName;

#[test]
fn demigrate_cold_artifacts_are_retained_by_default_and_dropped_only_after_rehydrate() {
    common::require_pgrx_server_sync()
        .expect("E2E tests require a running pgrx PostgreSQL server with koldstore installed");

    let context = DemigrationContext {
        table: QualifiedTableName::parse("app.items").unwrap(),
        table_oid: 42,
        cold_object_prefix: "app/items/".to_string(),
        logical_reader_name: "KoldMergeScan".to_string(),
        mirror_table: Some(QualifiedTableName::parse("koldstore.items__cl").unwrap()),
    };

    let retain = plan_demigration(context.clone(), DemigrateOptions::default()).unwrap();
    assert_eq!(retain.cold_artifact_action, ColdArtifactAction::Retain);

    let drop_after_rehydrate = plan_demigration(
        context.clone(),
        DemigrateOptions {
            rehydrate: true,
            drop_cold: true,
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
        },
    )
    .unwrap_err();
    assert_eq!(invalid.to_string(), "drop_cold requires rehydrate => true");
}

#[tokio::test]
async fn demigrate_cold_artifact_options_execute_through_pgrx() -> Result<()> {
    for target in common::scenario_pg_matrix() {
        let db = common::TestDb::start(target, "demigrate_cold_artifacts").await?;
        let table = db
            .create_indexed_items_table("demigrate_artifact_items", 8)
            .await?;
        db.manage_shared(&table.relation, "id").await?;
        assert_eq!(db.flush_table(&table.relation).await?, 8);
        common::assert_cold_metadata_present(&db.client, &table.relation).await?;

        let invalid = db
            .client
            .query_one(
                "SELECT koldstore.unmanage_table($1::text::regclass, false, true)",
                &[&table.relation],
            )
            .await
            .unwrap_err();
        let invalid_message = invalid.as_db_error().map_or_else(
            || invalid.to_string(),
            |db_error| db_error.message().to_string(),
        );
        assert!(
            invalid_message.contains("drop_cold requires rehydrate => true"),
            "{invalid_message}"
        );

        let deactivated = db
            .client
            .query_one(
                "SELECT koldstore.unmanage_table($1::text::regclass, true, true)",
                &[&table.relation],
            )
            .await?
            .get::<_, i64>(0);
        assert_eq!(deactivated, 1);

        let active_schema_rows = db
            .client
            .query_one(
                "SELECT count(*) FROM koldstore.schemas WHERE table_oid = $1::text::regclass::oid AND active",
                &[&table.relation],
            )
            .await?
            .get::<_, i64>(0);
        assert_eq!(active_schema_rows, 0);
        assert_eq!(common::row_count(&db.client, &table.relation).await?, 8);
    }

    Ok(())
}
